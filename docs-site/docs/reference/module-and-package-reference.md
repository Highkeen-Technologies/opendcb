# Module and Package Reference

Every module in the repository, its root Java package, what it does, and
its current status. Mirrors `README.md`'s module table and
`docs/ARCHITECTURE.md`'s package table.

| Module | Package | Purpose | Status |
|---|---|---|---|
| `eventstore-core` | `com.highkeen.opendcb.eventstore.core` | Framework-agnostic port: `StoredEvent`, `EventStoreStorage` | Done |
| `eventstore-postgres` | `com.highkeen.opendcb.eventstore.postgres` | PostgreSQL storage provider (plain JDBC) | Done |
| `eventstore-mysql` | `com.highkeen.opendcb.eventstore.mysql` | MySQL storage provider | Template only |
| `eventstore-mongo` | `com.highkeen.opendcb.eventstore.mongo` | MongoDB storage provider | Template only |
| `integrations/eventstore-axon` | `com.highkeen.opendcb.integrations.axon` | Axon 5 `EventStorageEngine` adapter | Done |
| `bootstrap-axon-postgres` | `com.highkeen.opendcb.bootstrap.axon.postgres` | One-call wiring of Axon + Postgres for non-Spring consumers | Done |
| `opendcb-axon-spring-boot-starter` | `com.highkeen.opendcb.springboot.axon` | Spring Boot auto-configuration | Done |
| `opendcb-axon-spring-boot-routing` | `com.highkeen.opendcb.routing.axon.springboot` | Multi-instance segment routing via Axon's `JdbcTokenStore` | Done |
| `opendcb-scheduling-core` | `com.highkeen.opendcb.scheduling.core` | Schedule events for future, DCB-aware delivery | Done |
| `opendcb-conductor-bridge` | `com.highkeen.opendcb.conductor.bridge` | Sagas/process managers via Conductor OSS | Done |
| `opendcb-snapshot-postgres` | `com.highkeen.opendcb.snapshot.postgres` | Postgres-backed implementation of Axon's own, free `SnapshotStore` SPI | Done |
| `outbox-relay-core` | `com.highkeen.opendcb.relay.core` | Tails the event log, publishes to a transport | Done |
| `outbox-relay-rabbitmq` | `com.highkeen.opendcb.relay.rabbitmq` | RabbitMQ publisher for `outbox-relay-core` | Done |
| `outbox-relay-kafka` | `com.highkeen.opendcb.relay.kafka` | Kafka publisher | Not started |
| `outbox-relay-webhook` | `com.highkeen.opendcb.relay.webhook` | HTTP webhook publisher | Not started |
| `opendcb-data-protection` | `com.highkeen.opendcb.dataprotection` | Crypto-shredding for erasure compliance: `@DataSubjectId`/`@PersonalData`, `OpenDcbEncryptingConverter`, `OpenDcbKeyStore` | Done |
| `opendcb-data-protection-vault` | `com.highkeen.opendcb.dataprotection.vault` | `MasterKeyProvider` backed by HashiCorp Vault's Transit secrets engine | Done |
| `opendcb-data-protection-aws-kms` | `com.highkeen.opendcb.dataprotection.awskms` | `MasterKeyProvider` backed by AWS KMS | Implemented and verified (real Testcontainers-backed LocalStack KMS) |

`examples/*` (`plain-java-sample`, `microservices-sample`) are runnable
demonstrations, not published libraries — they have no stable package
convention of their own to document here.

## Source

Every module's source lives under the repository root at
[github.com/Highkeen-Technologies/opendcb](https://github.com/Highkeen-Technologies/opendcb),
one top-level directory per module (or per `integrations/`/`examples/`
subdirectory), matching the paths in this table.

## What's next

See [docs/ARCHITECTURE.md](https://github.com/Highkeen-Technologies/opendcb/blob/main/docs/ARCHITECTURE.md)
for the full module dependency order and design rationale behind this
layout, and [docs/ROADMAP.md](https://github.com/Highkeen-Technologies/opendcb/blob/main/docs/ROADMAP.md)
for detailed status and history per module.
