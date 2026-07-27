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
1. eventstore-core                          (no dependency on any framework, or any other
                                              toolkit module)
2. eventstore-postgres                       depends on: eventstore-core
   eventstore-mysql                          depends on: eventstore-core
   eventstore-mongo                          depends on: eventstore-core
3. integrations/eventstore-axon              depends on: eventstore-core + org.axonframework
   integrations/eventstore-<other>           depends on: eventstore-core + that framework
                                              (hypothetical — added only if/when needed)
3b. opendcb-axon-scheduling                  depends on: org.axonframework only (CommandGateway)
                                              + JDBC (own scheduled_command table — independent
                                              of eventstore-core/EventStoreStorage entirely; see
                                              "opendcb-axon-scheduling" section below for why)
4. opendcb-axon-spring-boot-routing          depends on: eventstore-core, integrations/eventstore-axon
                                              (Axon-specific: token store / segment routing concepts
                                              don't generalize across frameworks — a different
                                              framework would need its own routing module)
5. outbox-relay-core                         depends on: eventstore-core only
                                              (framework-agnostic: it just tails
                                              EventStoreStorage.readRange)
6. outbox-relay-kafka                        depends on: outbox-relay-core
   outbox-relay-rabbitmq                     depends on: outbox-relay-core
   outbox-relay-webhook                      depends on: outbox-relay-core
7. bootstrap-axon-postgres                   depends on: integrations/eventstore-axon, eventstore-postgres
   bootstrap-axon-<provider>                 depends on: integrations/eventstore-axon, eventstore-<provider>
                                              (one bootstrap module per provider — see
                                              "Bootstrap modules" section below)
8. opendcb-axon-spring-boot-starter          depends on: bootstrap-axon-postgres (or whichever
                                              bootstrap module matches its default provider) +
                                              opendcb-axon-spring-boot-routing
                                              (a future opendcb-<framework>-spring-boot-starter
                                              would be its own module, not a branch inside this one)
9. examples/*                                depends on: whatever the example demonstrates
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

Note: `eventstore-axon` and `eventstore-postgres` were originally verified
independently (unit tests against an in-memory double; the Postgres contract
suite against a real container). `bootstrap-axon-postgres` closed that gap —
it's the first module that proves the two work together end-to-end against
a real Postgres instance, not just in isolation.

## What does NOT generalize across frameworks

- **Read-side routing** (`opendcb-axon-spring-boot-routing`, built on Axon's
  own `JdbcTokenStore`) is inherently Axon-specific — token store and
  segment claiming are Axon Framework concepts. A different framework needs
  its own routing module; there is no shared abstraction possible here.
- Everything under `outbox-relay-*` stays framework-agnostic regardless,
  since it only reads from `EventStoreStorage` directly.

## opendcb-axon-scheduling: OpenDCB's own abstraction, not an Axon SPI

Axon 4 Community shipped `DeadlineManager`/`EventScheduler` for free — "run
this command at a future time," "detect a deadline was missed." In Axon 5,
per AxonIQ's own published feature comparison, both are Axoniq Framework
(paid) only. Verified directly against the released source (not just the
marketing page): neither the interfaces nor any implementation
(`SimpleDeadlineManager`, `QuartzDeadlineManager`, `SimpleEventScheduler`,
etc.) exist in any published `org.axonframework` artifact — they live only
in Axon's own internal `axon-todo` module, explicitly marked by Axon's
maintainers as "not to be released code."

This is a materially different situation from the upcaster gap
(@docs/ROADMAP.md's open questions): there, an Axon SPI genuinely exists
but is unreleased, so building against it would mean guessing at a moving
target. Here, there is no SPI at all to target — free or paid, released or
not. That means `opendcb-axon-scheduling` is **not** an implementation of
any Axon interface, and must never be named or documented as if it were.
It's OpenDCB's own abstraction (its own class names — e.g.
`ScheduledCommandStore`, `ScheduledCommandDispatcher`, never anything
implying it's Axon's `DeadlineManager`/`EventScheduler`), solving the same
class of problem, integrating with Axon only at the one point where it
dispatches a due command via Axon's real, free `CommandGateway` — core
message-dispatch functionality, not part of what's paywalled.

Because it never touches `EventStoreStorage` or any provider — it owns a
separate `scheduled_command` table entirely independent of the event
store — it doesn't belong under `eventstore-*` or `integrations/*` at all.
It sits beside `integrations/eventstore-axon` in the dependency order,
depending only on `org.axonframework` (for `CommandGateway`) and plain JDBC.

If Axon ever publishes a real, released interface for this, revisit
whether `opendcb-axon-scheduling` should adapt to it — but do not
preemptively shape this module's API to guess what that interface might
look like.

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
- `opendcb-axon-spring-boot-starter` must call into `bootstrap-axon-postgres`
  internally rather than re-implementing the same wiring — one source of
  truth for "how do you correctly assemble Axon + Postgres," shared by both
  the Spring and non-Spring paths.
- A new provider reaching parity gets its own `bootstrap-axon-<provider>`
  module (e.g. `bootstrap-axon-mysql`) — same one-class pattern, no new
  design decisions required each time.

## Two supported deployment shapes

**Monolithic / single bounded context**: `eventstore-postgres` (or another
provider) + `integrations/eventstore-axon` + `opendcb-axon-spring-boot-routing`.
Multiple instances scale via the shared `JdbcTokenStore` — no broker.

**Event-driven microservices**: each service still uses its own
`eventstore-*` internally. `outbox-relay-core` + one transport module is the
only thing crossing a bounded-context boundary, and only ever publishes a
deliberately-shaped integration event — never the internal domain event
payload directly. `OutboxRelay`'s optional `Predicate<StoredEvent>` filter is
the mechanism that enforces this: a service supplies a filter matching only
its public/integration event types, and everything else — including its rich
internal domain events — simply never matches, so it's skipped (position
still advances past it) rather than published. Internal events stay internal
by construction, with no separate mechanism needed. See @docs/PROVIDERS.md
for adapter-specific detail and @docs/ROADMAP.md for current module status.

## Package convention

Root package: `com.highkeen.opendcb`.

Naming note: `opendcb-axon-spring-boot-starter` and
`opendcb-axon-spring-boot-routing` deliberately do NOT start with
`spring-boot` — Spring Boot's own convention for third-party starters
(docs.spring.io, "Creating Your Own Starter" → "Naming") explicitly says not
to, since that implies official Spring support. The project name comes
first, `-spring-boot-...` is the suffix.

| Module | Package |
|---|---|
| `eventstore-core` | `com.highkeen.opendcb.eventstore.core` |
| `eventstore-postgres` | `com.highkeen.opendcb.eventstore.postgres` |
| `eventstore-mysql` | `com.highkeen.opendcb.eventstore.mysql` |
| `eventstore-mongo` | `com.highkeen.opendcb.eventstore.mongo` |
| `integrations/eventstore-axon` | `com.highkeen.opendcb.integrations.axon` |
| `opendcb-axon-scheduling` | `com.highkeen.opendcb.scheduling.axon` |
| `opendcb-axon-spring-boot-routing` | `com.highkeen.opendcb.routing.axon.springboot` |
| `outbox-relay-core` | `com.highkeen.opendcb.relay.core` |
| `outbox-relay-kafka` | `com.highkeen.opendcb.relay.kafka` |
| `outbox-relay-rabbitmq` | `com.highkeen.opendcb.relay.rabbitmq` |
| `outbox-relay-webhook` | `com.highkeen.opendcb.relay.webhook` |
| `bootstrap-axon-postgres` | `com.highkeen.opendcb.bootstrap.axon.postgres` |
| `opendcb-axon-spring-boot-starter` | `com.highkeen.opendcb.springboot.axon` |

Maven `groupId` for every module: `com.highkeen.opendcb`.
