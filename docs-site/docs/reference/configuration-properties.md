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

## Data protection: not Spring Boot properties

`opendcb-data-protection`, `opendcb-data-protection-vault`, and
`opendcb-data-protection-aws-kms` have no `opendcb.*` Spring Boot
auto-configuration — none of the three ships a `@ConfigurationProperties`
class. Every `MasterKeyProvider` is configured via plain constructor
arguments instead:

| Class | Configuration | Default |
|---|---|---|
| `EnvVarMasterKeyProvider` (`opendcb-data-protection`) | Reads a base64-encoded 256-bit AES key from an environment variable — the *name* is a constructor argument (`new EnvVarMasterKeyProvider("MY_VAR")`), not itself a Spring property. | `OPENDCB_DATA_PROTECTION_MASTER_KEY`, if the no-arg constructor is used |
| `VaultMasterKeyProvider` (`opendcb-data-protection-vault`) | Constructor takes Vault base URL, Vault token, and Transit key name — all three are plain `String` arguments, no defaults. | *(none — all three required)* |
| `AwsKmsMasterKeyProvider` (`opendcb-data-protection-aws-kms`) | Constructor takes an already-configured `KmsClient` and a CMK key ID/ARN — this class never builds its own client, so region/credentials/endpoint are entirely the caller's own AWS SDK configuration, outside OpenDCB's config surface. | *(none)* |

See
[Data Protection and Key Management](../module-guides/data-protection-and-key-management.md)
for real, working construction examples of all three.

## What's next

See [Spring Boot Setup](../setup/spring-boot-setup.md) and
[Spring Boot Starter and Scaling](../module-guides/spring-boot-starter-and-scaling.md)
for how the `opendcb.eventstore.*`/`opendcb.routing.*` properties above are
actually used.
