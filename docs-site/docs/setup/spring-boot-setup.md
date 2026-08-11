# Spring Boot Setup

## Add the dependency

```xml
<dependency>
    <groupId>com.highkeen.opendcb</groupId>
    <artifactId>opendcb-axon-spring-boot-starter</artifactId>
    <version>1.0.0-axon5.1</version>
</dependency>
```

## Point it at a database

At minimum, tell it where PostgreSQL lives:

```properties
opendcb.eventstore.datasource.url=jdbc:postgresql://localhost:5432/opendcb
opendcb.eventstore.datasource.username=postgres
opendcb.eventstore.datasource.password=postgres
```

That's it. On startup, Spring Boot auto-configures a working `EventStorageEngine`
bean — the object Axon Framework uses to read and write events. No code
required. Behind the scenes this is done by a class called
`OpenDcbPostgresAutoConfiguration`, which creates the database schema if it
doesn't exist yet and hands you back a ready-to-use engine.

## Reusing your app's existing database, instead

If you don't set `opendcb.eventstore.datasource.url`, OpenDCB doesn't fail —
it reuses your application's primary `DataSource` bean (the one Spring Boot
already builds from your regular `spring.datasource.*` properties) and logs
an `INFO` message telling you it did so. This is the default because many
small applications are happy keeping everything in one database.

## Using a separate database for events

Some teams prefer to keep the event store on its own database, separate from
the rest of the application's tables — for example, to scale or back it up
independently, or to keep a noisy event log from crowding out other
workloads on the same instance. Setting
`opendcb.eventstore.datasource.url` (as shown above) does exactly this: it
builds a dedicated `DataSource` just for events, instead of reusing the
primary one. You can also set `opendcb.eventstore.datasource.use-primary=true`
explicitly if you want to be clear in configuration that you're
intentionally sharing the primary database, rather than relying on the
silent fallback.

## What's next

A single instance works out of the box. If you plan to run more than one
instance of your application against the same database, see the
[Spring Boot Starter and Scaling](../module-guides/spring-boot-starter-and-scaling.md)
guide — it covers `opendcb-axon-spring-boot-routing`, the piece that keeps
multiple instances from stepping on each other's work.
