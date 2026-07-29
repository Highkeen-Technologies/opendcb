# OpenDCB

[![CI](https://github.com/Highkeen-Technologies/opendbc/actions/workflows/ci.yml/badge.svg)](https://github.com/Highkeen-Technologies/opendbc/actions/workflows/ci.yml)

**Maintained by [Highkeen Technologies](https://highkeen.com).**

OpenDCB is an open-source Java toolkit that fills the gap [Axon Framework
5](https://github.com/AxonFramework/AxonFramework) (free) deliberately
leaves to Axoniq Framework (paid): DCB-capable event storage on any backing
store, plus distributed event routing, without requiring Axon Server. It's
not a replacement for Axon Framework — `integrations/eventstore-axon` is a
set of `EventStorageEngine` implementations that plug into it, the same
relationship any community Axon extension has — and it's not trying to
match Axon Server feature-for-feature (no clustering/HA replication, no
Multi-Context). It targets the self-hosted, single-team tier that doesn't
need enterprise HA yet.

The storage layer (`eventstore-core` and every `eventstore-<provider>`
module) has zero dependency on Axon or any other framework by design, so
that if a second event-sourcing framework becomes relevant, supporting it
is "add one `integrations/` module," not a rewrite. See
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the design principle in
full.

License: [Apache 2.0](LICENSE), matching Axon Framework itself.

## Modules

| Module | Purpose | Status |
|---|---|---|
| `eventstore-core` | Framework-agnostic port: `StoredEvent`, `EventStoreStorage` | Done |
| `eventstore-postgres` | PostgreSQL storage provider (plain JDBC) | Done |
| `eventstore-mysql` | MySQL storage provider | Template only, not implemented |
| `eventstore-mongo` | MongoDB storage provider | Template only, not implemented |
| `integrations/eventstore-axon` | Axon 5 `EventStorageEngine` adapter | Done |
| `bootstrap-axon-postgres` | One-call wiring of Axon + Postgres for non-Spring consumers | Done |
| `opendcb-axon-spring-boot-starter` | Spring Boot auto-configuration | Done |
| `opendcb-axon-spring-boot-routing` | Multi-instance segment routing via Axon's `JdbcTokenStore` | Done |
| `opendcb-scheduling-core` | Schedule events for future, DCB-aware delivery | Done |
| `outbox-relay-core` | Tails the event log, publishes to a transport | Done |
| `outbox-relay-rabbitmq` | RabbitMQ publisher for `outbox-relay-core` | Done |
| `outbox-relay-kafka` | Kafka publisher | Not started |
| `outbox-relay-webhook` | HTTP webhook publisher | Not started |

See [docs/ROADMAP.md](docs/ROADMAP.md) for the full module skeleton,
detailed status, and design history, and
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the module dependency
order and package layout.

## Quickstart

### Spring Boot

Add `opendcb-axon-spring-boot-starter` to your dependencies, point it at a
PostgreSQL database, and an `EventStorageEngine` bean appears:

```xml
<dependency>
    <groupId>com.highkeen.opendcb</groupId>
    <artifactId>opendcb-axon-spring-boot-starter</artifactId>
    <version>1.0.0-axon5.1</version>
</dependency>
```

```properties
opendcb.eventstore.datasource.url=jdbc:postgresql://localhost:5432/opendcb
opendcb.eventstore.datasource.username=postgres
opendcb.eventstore.datasource.password=postgres
```

No further wiring code required. See
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#bootstrap-modules-zero-boilerplate-wiring-without-requiring-spring)
for what this does under the hood.

### Plain Java (no Spring)

One factory call, from `bootstrap-axon-postgres`:

```java
DataSource dataSource = new PGSimpleDataSource();
// ... configure dataSource ...

EventStorageEngine engine = OpenDcbAxonPostgres.engine(dataSource);
```

That's a working `EventStorageEngine` — no hand-assembling
`PostgresEventStoreStorage` + `AbstractDcbEventStorageEngine` + a
`Converter` yourself. See
[examples/plain-java-sample](examples/plain-java-sample) for the full,
runnable version, including re-sourcing an entity from a second,
independent configurer to prove durability.

### Event-driven microservices

For a system split across bounded contexts, each service keeps its own
`eventstore-postgres` internally; `outbox-relay-core` plus a transport
module (`outbox-relay-rabbitmq` today) is the only thing that crosses a
service boundary, and it only ever publishes a deliberately-shaped
integration event — never the internal domain event payload. See
[examples/microservices-sample](examples/microservices-sample) for a
complete, runnable two-service example (`orders-service` /
`shipping-service`) with a real anti-corruption-layer translator on the
consuming side and a test proving redelivery-safe idempotency.

## Known limitations

- **No event upcasting.** Axon Framework 5.1.2 ships no released
  upcaster/`IntermediateEventRepresentation` SPI to wire up — the only such
  code lives in Axon's own unreleased, explicitly "not to be released"
  module. `integrations/eventstore-axon` documents this as a deliberate
  simplification, not an oversight; see the open question in
  [docs/ROADMAP.md](docs/ROADMAP.md#open-questions-worth-deciding-before-writing-more-code).
- **`eventstore-mysql` and `eventstore-mongo` are design templates only.**
  Schema/collection shape and locking strategy are documented in
  [docs/PROVIDERS.md](docs/PROVIDERS.md), but neither has an implementation
  yet.
- **`outbox-relay-kafka` and `outbox-relay-webhook` don't exist yet.**
  RabbitMQ (`outbox-relay-rabbitmq`) is the only working transport today.
- **No clustering/HA replication, no Axon Server feature parity.** This is
  a deliberate scope boundary, not a gap — see the Positioning section of
  [docs/ROADMAP.md](docs/ROADMAP.md).

## Contributing

See [CLAUDE.md](CLAUDE.md) for the hard architectural rules (framework
coupling boundaries, licensing constraints), and the `docs/` directory for
full detail:

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — module layout and the
  framework-agnostic storage design.
- [docs/CONVENTIONS.md](docs/CONVENTIONS.md) — coding standards, license
  headers, and the architectural boundary rules CI enforces.
- [docs/PROVIDERS.md](docs/PROVIDERS.md) — adding a new storage provider.
- [docs/TESTING.md](docs/TESTING.md) — the shared contract test suite every
  provider must pass, and what CI blocks on.
- [docs/ROADMAP.md](docs/ROADMAP.md) — module status and priorities.

## Building

```
mvn clean install
```

Runs the full reactor, including the Testcontainers-backed contract and
end-to-end test suites — a local Docker daemon is required.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
