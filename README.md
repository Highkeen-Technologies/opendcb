# OpenDCB

[![CI](https://github.com/Highkeen-Technologies/opendbc/actions/workflows/ci.yml/badge.svg)](https://github.com/Highkeen-Technologies/opendbc/actions/workflows/ci.yml)

Open-source Java toolkit providing DCB-capable event storage on any backing
store, plus event routing, for event-sourcing frameworks — currently
[Axon Framework 5](https://github.com/AxonFramework/AxonFramework), designed
so other frameworks can be added without touching the storage layer.

Fills the gap Axon Framework 5 (free) deliberately leaves to Axoniq
Framework (paid): DCB-capable event storage on any backing store, and
distributed event routing without Axon Server. Not a replacement for Axon
Framework, and not trying to match Axon Server feature-for-feature — this
targets the self-hosted, single-team tier that doesn't need enterprise HA
yet.

License: Apache 2.0, matching Axon Framework itself.

## Documentation

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — module layout and the
  framework-agnostic storage design.
- [docs/CONVENTIONS.md](docs/CONVENTIONS.md) — coding standards, license
  headers, and the architectural boundary rules CI enforces.
- [docs/PROVIDERS.md](docs/PROVIDERS.md) — adding a new storage provider.
- [docs/TESTING.md](docs/TESTING.md) — the shared contract test suite every
  provider must pass, and what CI blocks on.
- [docs/ROADMAP.md](docs/ROADMAP.md) — module status and priorities.

## Examples

- [examples/plain-java-sample](examples/plain-java-sample) — wiring
  everything by hand with zero Spring, against a single PostgreSQL-backed
  event store.
- [examples/microservices-sample](examples/microservices-sample) — two
  independent services, each with its own database, connected only by a
  deliberately-shaped integration event over RabbitMQ.

## Building

```
mvn clean install
```

Runs the full reactor, including the Testcontainers-backed contract and
end-to-end test suites — a local Docker daemon is required.
