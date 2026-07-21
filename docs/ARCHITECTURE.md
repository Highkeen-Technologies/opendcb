# Architecture

## Design principle: the storage layer must outlive any one framework

`eventstore-core` and every `eventstore-<provider>` module are framework-agnostic
by design — they know nothing about Axon, or any other event-sourcing
framework. All framework-specific translation lives in `integrations/`. This
means adding support for a second framework in the future means writing one
new adapter module — it never touches storage or providers.

## Module dependency order

Each module may only depend on modules above it in this list. No sideways or
upward dependencies — this is what keeps modules independently publishable
and keeps the framework-agnostic layer honestly agnostic.

```
1. eventstore-core                    (no dependency on any framework, or any other
                                        toolkit module)
2. eventstore-postgres                 depends on: eventstore-core
   eventstore-mysql                    depends on: eventstore-core
   eventstore-mongo                    depends on: eventstore-core
3. integrations/eventstore-axon        depends on: eventstore-core + org.axonframework
   integrations/eventstore-<other>     depends on: eventstore-core + that framework
                                        (hypothetical — added only if/when needed)
4. routing-spring-boot-axon            depends on: eventstore-core, integrations/eventstore-axon
                                        (Axon-specific: token store / segment routing concepts
                                        don't generalize across frameworks — a different
                                        framework would need its own routing module)
5. outbox-relay-core                   depends on: eventstore-core only
                                        (framework-agnostic: it just tails
                                        EventStoreStorage.readRange)
6. outbox-relay-kafka                  depends on: outbox-relay-core
   outbox-relay-rabbitmq               depends on: outbox-relay-core
   outbox-relay-webhook                depends on: outbox-relay-core
7. bootstrap-axon-postgres             depends on: integrations/eventstore-axon, eventstore-postgres
   bootstrap-axon-<provider>           depends on: integrations/eventstore-axon, eventstore-<provider>
                                        (one bootstrap module per provider — see
                                        "Bootstrap modules" section below)
8. spring-boot-starter-axon            depends on: bootstrap-axon-postgres (or whichever
                                        bootstrap module matches its default provider) +
                                        routing-spring-boot-axon
                                        (a future spring-boot-starter-<framework> would be
                                        its own module, not a branch inside this one)
9. examples/*                          depends on: whatever the example demonstrates
```

`eventstore-postgres`, `eventstore-mysql`, and `eventstore-mongo` must never
depend on each other, or on any `integrations/*` module. `integrations/eventstore-axon`
must never depend on a specific provider — it only knows `EventStoreStorage`.

## eventstore-core: the pattern every provider follows

Two things live here, and nothing else:

- `StoredEvent` — a plain DTO (position, payload, tags, metadata, timestamp).
  No framework type anywhere in its signature.
- `EventStoreStorage` — the port a provider implements. Five methods:
  `appendAtomically`, `readRange`, `maxPosition`, `minPosition`,
  `positionAtOrAfter`. The conflict predicate passed into `appendAtomically`
  is `java.util.function.Predicate<StoredEvent>` — plain Java, not an Axon type.

## integrations/eventstore-axon: where Axon coupling lives

`AbstractDcbEventStorageEngine` implements Axon's `EventStorageEngine` SPI and
translates Axon's types (`TaggedEventMessage`, `AppendCondition`,
`ConsistencyMarker`, `MessageStream`, `EventCriteria`) to and from
`StoredEvent`/`EventStoreStorage`. This is the *only* place in the whole
toolkit that imports `org.axonframework`. If a change to support a new
provider requires touching this class, the port is leaking abstraction — fix
`EventStoreStorage`, not this adapter.

A second framework showing up in the market means adding
`integrations/eventstore-<framework>` alongside this one, implementing that
framework's own storage SPI the same way. `eventstore-postgres` (and any
other provider) needs zero changes to support it.

Note: as of the current build, `eventstore-axon` and `eventstore-postgres`
have each been verified independently (unit tests against an in-memory
double; the Postgres contract suite against a real container) but nothing
yet wires the two together end-to-end. `bootstrap-axon-postgres` (below) is
the first place that actually happens — worth treating its build as an
integration checkpoint, not just a convenience module.

## What does NOT generalize across frameworks

- **Read-side routing** (`routing-spring-boot-axon`, built on Axon's own
  `JdbcTokenStore`) is inherently Axon-specific — token store and segment
  claiming are Axon Framework concepts. A different framework needs its own
  routing module; there is no shared abstraction possible here.
- Everything under `outbox-relay-*` stays framework-agnostic regardless,
  since it only reads from `EventStoreStorage` directly.

## Bootstrap modules: zero-boilerplate wiring without requiring Spring

`integrations/eventstore-axon` deliberately never depends on a specific
provider, and `eventstore-postgres` (etc.) never depends on Axon — that's
what keeps each layer honestly reusable. But that split means a plain-Java
(or Quarkus/Micronaut) consumer would otherwise have to hand-assemble
`PostgresEventStoreStorage` + `AbstractDcbEventStorageEngine` + an
`ObjectMapper` themselves, every time, in every project.

**`bootstrap-axon-postgres`** exists to remove that repetition, without
violating the dependency rules above — it's the one place allowed to depend
on *both* a specific `integrations/eventstore-<framework>` module *and* a
specific `eventstore-<provider>` module, precisely because its only job is
gluing exactly those two together behind a single factory call:

```java
EventStorageEngine engine = OpenDcbAxonPostgres.engine(dataSource);
```

Rules for this tier:

- A bootstrap module may depend on exactly one `integrations/eventstore-<framework>`
  module and exactly one `eventstore-<provider>` module — never more than one
  of each, and never depended upon by `eventstore-core`, any provider, or
  any `integrations/*` module (dependency flows one direction only: into
  the bootstrap module, never out of it).
- `spring-boot-starter-axon` must call into `bootstrap-axon-postgres`
  internally rather than re-implementing the same wiring — one source of
  truth for "how do you correctly assemble Axon + Postgres," shared by both
  the Spring and non-Spring paths.
- A new provider reaching parity gets its own `bootstrap-axon-<provider>`
  module (e.g. `bootstrap-axon-mysql`) — same one-class pattern, no new
  design decisions required each time.

## Two supported deployment shapes

**Monolithic / single bounded context**: `eventstore-postgres` (or another
provider) + `integrations/eventstore-axon` + `routing-spring-boot-axon`.
Multiple instances scale via the shared `JdbcTokenStore` — no broker.

**Event-driven microservices**: each service still uses its own
`eventstore-*` internally. `outbox-relay-core` + one transport module is the
only thing crossing a bounded-context boundary, and only ever publishes a
deliberately-shaped integration event — never the internal domain event
payload directly. See @docs/PROVIDERS.md for adapter-specific detail and
@docs/ROADMAP.md for current module status.

## Package convention

Root package: `com.highkeen.opendcb`.

| Module | Package |
|---|---|
| `eventstore-core` | `com.highkeen.opendcb.eventstore.core` |
| `eventstore-postgres` | `com.highkeen.opendcb.eventstore.postgres` |
| `eventstore-mysql` | `com.highkeen.opendcb.eventstore.mysql` |
| `eventstore-mongo` | `com.highkeen.opendcb.eventstore.mongo` |
| `integrations/eventstore-axon` | `com.highkeen.opendcb.integrations.axon` |
| `routing-spring-boot-axon` | `com.highkeen.opendcb.routing.axon.springboot` |
| `outbox-relay-core` | `com.highkeen.opendcb.relay.core` |
| `outbox-relay-kafka` | `com.highkeen.opendcb.relay.kafka` |
| `outbox-relay-rabbitmq` | `com.highkeen.opendcb.relay.rabbitmq` |
| `outbox-relay-webhook` | `com.highkeen.opendcb.relay.webhook` |
| `bootstrap-axon-postgres` | `com.highkeen.opendcb.bootstrap.axon.postgres` |
| `spring-boot-starter-axon` | `com.highkeen.opendcb.springboot.axon` |

Maven `groupId` for every module: `com.highkeen.opendcb`.
