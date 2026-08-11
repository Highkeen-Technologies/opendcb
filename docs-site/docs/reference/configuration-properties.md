# Configuration Properties

Every `opendcb.*` Spring Boot property that actually exists today, pulled
directly from `OpenDcbProperties` (`opendcb-axon-spring-boot-starter`) and
`OpenDcbRoutingProperties` (`opendcb-axon-spring-boot-routing`).

## opendcb.eventstore.* — opendcb-axon-spring-boot-starter

| Property | Default | Description |
|---|---|---|
| `opendcb.eventstore.provider` | `postgres` | Which storage provider to wire up. |
| `opendcb.eventstore.autoCreateSchema` | `true` | Create the `events`/`event_tags` tables on startup if they don't exist. |
| `opendcb.eventstore.datasource.url` | *(none)* | JDBC URL for a dedicated event store database. If unset, falls back to your app's primary `DataSource`. |
| `opendcb.eventstore.datasource.username` | *(none)* | Username for the dedicated event store `DataSource`. |
| `opendcb.eventstore.datasource.password` | *(none)* | Password for the dedicated event store `DataSource`. |
| `opendcb.eventstore.datasource.driverClassName` | *(none)* | JDBC driver class, if it needs to be set explicitly. |
| `opendcb.eventstore.datasource.usePrimary` | `false` | Explicitly reuse the application's primary `DataSource` instead of building a dedicated one. |

Setting `opendcb.eventstore.provider=none` disables auto-configuration
entirely — no `EventStorageEngine` bean is created. If you supply your own
`EventStorageEngine` bean, the starter also backs off automatically,
without attempting to build any `DataSource`.

## opendcb.routing.* — opendcb-axon-spring-boot-routing

| Property | Default | Description |
|---|---|---|
| `opendcb.routing.autoCreateSchema` | `true` | Create the `JdbcTokenStore`'s own table on startup if it doesn't exist. |

This is the only property this module defines. It resolves its
`DataSource` by looking up a bean named `openDcbEventStoreDataSource`
(built by the starter above), falling back to your application's primary
`DataSource` with a logged `INFO` message if that bean isn't found.

## What's next

See [Spring Boot Setup](../setup/spring-boot-setup.md) and
[Spring Boot Starter and Scaling](../module-guides/spring-boot-starter-and-scaling.md)
for how these properties are actually used.
