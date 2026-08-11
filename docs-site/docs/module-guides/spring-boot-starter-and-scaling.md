# Spring Boot Starter and Scaling

## Recap: opendcb-axon-spring-boot-starter

If you followed [Spring Boot Setup](../setup/spring-boot-setup.md), you
already have this working: add the starter, point
`opendcb.eventstore.datasource.*` at PostgreSQL, and Spring Boot gives you a
working `EventStorageEngine` bean with no code. This page assumes that's
already done and focuses on what happens once you run more than one copy of
that application.

## The problem: two copies of your app, one database

Say you deploy your Spring Boot application as three replicas for
availability. All three connect to the same PostgreSQL event store and all
three want to process the same streams of events (via Axon's
`PooledStreamingEventProcessor`, for projections and event handlers). If
nothing coordinates them, all three would process every event — triple
handling, not scaling.

Axon Framework solves this with **segments**: the total workload is split
into a fixed number of segments, and each running instance claims some
subset of them. `JdbcTokenStore` is Axon's own mechanism for tracking, in
the database, which instance currently holds which segment.

Think of it like a stack of numbered tickets at a counter. Each segment is a
ticket. Each application instance walks up and takes an unclaimed ticket —
whichever it grabs first, it owns until it releases it or crashes. No two
instances can hold the same ticket at once, because claiming one requires a
database-level lock that only one instance can hold at a time. If an
instance dies, its tickets eventually become claimable again.

## What opendcb-axon-spring-boot-routing actually does

This module is a Spring Boot auto-configuration
(`OpenDcbTokenStoreAutoConfiguration`) that wires Axon's real `JdbcTokenStore`
against your event store's database, so you don't have to configure it by
hand. Concretely:

- It publishes a bean named `openDcbTokenStore`, only if you haven't already
  defined your own `TokenStore` bean (`@ConditionalOnMissingBean(TokenStore.class)`),
  and only if a `DataSource` bean exists in your application at all.
- It resolves which database to point the token store at by looking up a
  bean named `openDcbEventStoreDataSource` — the same `DataSource` the
  Spring Boot starter builds for your event store. It does this by bean
  *name*, not a compile-time dependency on the starter module, so this
  module never has to know the starter exists. If no bean with that name is
  found, it falls back to your application's primary `DataSource`, logging
  an `INFO` message when it does — worth watching for, since falling back to
  the wrong database would make token coordination meaningless.
- It creates the token store's own table via Axon's `GenericTokenTableFactory`
  if it doesn't already exist. This is controlled by
  `opendcb.routing.autoCreateSchema`, `true` by default — same idea as the
  starter's own `autoCreateSchema` setting for the event tables.

## Configuration

```properties
# Whether to auto-create the token store's table. Default: true.
opendcb.routing.autoCreateSchema=true
```

That's the only property this module defines — see the full list on the
[Configuration Properties](../reference/configuration-properties.md) page.

## What's next

To see this in action — two instances actually splitting the work — walk
through [Scale Across Multiple Instances](../tutorials/scale-across-multiple-instances.md).
