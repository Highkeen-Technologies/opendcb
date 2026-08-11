# Scale Across Multiple Instances

This tutorial walks through configuring a Spring Boot application so
multiple running copies of it safely share the work of processing events,
using `opendcb-axon-spring-boot-routing`. There's no dedicated multi-instance
example module in the repository yet — this walks through the real
auto-configuration classes so you can set this up on top of your own
application, following the same [Spring Boot Setup](../setup/spring-boot-setup.md)
you'd use for a single instance.

## Step 1: start from a working single-instance app

You need a Spring Boot application already using `opendcb-axon-spring-boot-starter`,
with at least one Axon event processor registered (for example, a
projection built with `@EventHandler` methods on a
`PooledStreamingEventProcessor`). If you don't have this yet, work through
[Spring Boot Setup](../setup/spring-boot-setup.md) first.

## Step 2: add the routing dependency

```xml
<dependency>
    <groupId>com.highkeen.opendcb</groupId>
    <artifactId>opendcb-axon-spring-boot-routing</artifactId>
    <version>1.0.0-axon5.1</version>
</dependency>
```

With this on the classpath, `OpenDcbTokenStoreAutoConfiguration` publishes a
`TokenStore` bean automatically — as long as you haven't already defined
your own, and as long as a `DataSource` bean exists in your application.
By default it points at the same database as your event store (the
`openDcbEventStoreDataSource` bean the starter builds), and creates its own
token table if it doesn't exist yet, controlled by
`opendcb.routing.autoCreateSchema` (`true` by default).

No further code is required — this is the entire setup.

## Step 3: run two instances against the same database

Package your application and run it twice, on two different ports, both
pointed at the same PostgreSQL database:

```bash
java -jar your-app.jar --server.port=8081
java -jar your-app.jar --server.port=8082
```

## Step 4: what you'd observe

Axon splits an event processor's workload into a fixed number of segments.
`JdbcTokenStore` — the mechanism this module wires up — tracks, in the
database, which running instance currently owns which segment, using a
`SELECT ... FOR UPDATE`-style row lock so only one instance can hold a
given segment at a time.

With one instance running, that instance owns every segment. Start the
second instance against the same database, and Axon rebalances: some
segments release from the first instance and the second instance claims
them, so both instances end up sharing the processing workload rather than
both processing every event. You can observe this by watching your
application logs — Axon logs segment claim/release activity — or by adding
logging inside your event handlers and noting which instance's log lines
show up for which events.

If you stop one instance, its segments become unclaimed once its lease
expires, and the remaining instance eventually claims them, taking over the
full workload again.

## Why this doesn't corrupt data

The same `SELECT ... FOR UPDATE` row lock that makes claiming safe also
makes it impossible for two instances to believe they both own the same
segment at once — confirmed directly by `opendcb-axon-spring-boot-routing`'s
own test suite, which forces two independent `JdbcTokenStore` instances to
race a claim on the same segment at the exact same instant and asserts
exactly one succeeds.

## What's next

See [Spring Boot Starter and Scaling](../module-guides/spring-boot-starter-and-scaling.md)
for the full explanation of what this module does and why, including its
configuration options.
