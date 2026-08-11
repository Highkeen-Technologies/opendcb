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
4. opendcb-axon-spring-boot-routing          depends on: eventstore-core, integrations/eventstore-axon
                                              (Axon-specific: token store / segment routing concepts
                                              don't generalize across frameworks — a different
                                              framework would need its own routing module)
5. outbox-relay-core                         depends on: eventstore-core only
                                              (framework-agnostic: it just tails
                                              EventStoreStorage.readRange)
5b. opendcb-scheduling-core                  depends on: eventstore-core only
                                              (framework-agnostic, same shape as outbox-relay-core —
                                              see "opendcb-scheduling-core" section below; NOT
                                              Axon-specific despite earlier drafts of this module,
                                              since firing a scheduled event is just appending via
                                              EventStoreStorage — no Axon dependency needed at all)
5c. opendcb-conductor-bridge                 depends on: org.axonframework (CommandGateway only) +
                                              Conductor's Java SDK + JDBC (own saga_correlation
                                              table). Genuinely Axon-coupled, unlike scheduling-core
                                              — see "opendcb-conductor-bridge" section below for why.
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
- `opendcb-scheduling-core` is likewise framework-agnostic — see below.

## opendcb-scheduling-core: schedules events, not commands — matching Axon's own design, and framework-agnostic as a result

**Design history worth keeping, since it explains a real pivot:** the first
draft of this module scheduled *commands* (`ScheduledCommandStore`,
dispatched via Axon's `CommandGateway`), reasoning that re-validating
business rules at fire time is safer in a DCB system than committing to a
decision far in advance. That reasoning wasn't wrong, but it diverged from
what Axon itself has always done — verified directly against two
independent sources: Axon 4's `EventScheduler` (legacy reference-guide,
superseded Oct 2024) explicitly schedules a raw event (its own example:
"schedule an `InvoicePaymentDeadlineExpiredEvent` to be published in 30
days"), and Axon 5's real, current `DcbEventChannel.scheduleEvent(Instant,
Event)` (in the free `io.axoniq:axonserver-connector-java`, confirmed via
source) takes an `Event` parameter too. Same model, both versions — a
deliberate, consistent design choice by AxonIQ, not an artifact of an older
API. That consistency is a strong enough signal to follow their pattern
rather than diverge from it.

**The pivot turned out to simplify the architecture, not just rename it.**
Firing a scheduled event means appending it to the log — and appending is
exactly what `EventStoreStorage` (`eventstore-core`) already does. There's
no need to route through Axon's command handling at all. So this module
needs **zero dependency on `org.axonframework`** — it depends only on
`eventstore-core`, the same shape as `outbox-relay-core`. Renamed
accordingly: `opendcb-scheduling-core`, not `opendcb-axon-scheduling` —
dropping "-axon" because it genuinely isn't Axon-specific anymore.

**What replaces the re-validation-at-fire-time safety this pivot gives
up:** nothing stops a scheduled event's downstream event handler from
checking "is this still relevant?" before acting — that check simply moves
to the *consumer* side instead of blocking the append. Arguably this is
more aligned with event-sourcing philosophy anyway: the event ("a deadline
passed") is a fact that occurred; what to *do* about it is a separate
decision made by whoever handles it, not baked into whether the fact gets
recorded at all.

**An additional, DCB-native safety net worth building in (optional, not
mandatory):** since `EventStoreStorage.appendAtomically` already takes a
conflict predicate, `opendcb-scheduling-core` can let the caller supply
one at schedule time — e.g. "don't append `InvoicePaymentDeadlineExpiredEvent`
if an `InvoicePaidEvent` for this invoice already exists." If the predicate
matches, the scheduled append is skipped rather than forced through. This
gives DCB-aware cancellation *in addition to* the explicit
`cancel(scheduleId)` call, for cases where "this is no longer valid" is
easier to express as "check the log" than "remember to call cancel."
Optional because not every scheduled event needs this — a plain reminder
notification has no real conflict to check for.

Components, own table (`scheduled_event`, independent of the event log's
own tables):

- `ScheduledEventStore` — owns the table: `schedule(...)`, `cancel(...)`,
  and `claimDue(...)` (lease-based, same cross-JVM-safe `SELECT ... FOR
  UPDATE SKIP LOCKED` + expiring-lease-reclaim design as before — see
  @docs/ROADMAP.md for the exact schema/columns).
- `ScheduledEventDispatcher` — the poller: claims due rows, builds a
  `StoredEvent` from the stored payload/tags, calls
  `EventStoreStorage.appendAtomically(...)` (optionally with the
  caller-supplied conflict predicate above), marks the row complete on
  success.

If Axon ever publishes a real, released interface for event scheduling in
`org.axonframework` itself (as opposed to only inside Axon Server), revisit
whether to adapt `opendcb-scheduling-core` to it — but don't preemptively
shape its API to guess what that interface might look like.

### Known alternative: `io.axoniq:axonserver-connector-java` (not adopted here, documented for context)

Verified directly against the actual repo (github.com/AxonIQ/axonserver-connector-java):
its `DcbEventChannel` interface has real, working `scheduleEvent(Instant, Event)`,
`cancelSchedule(String)`, and `reschedule(String, Instant, Event)` methods —
genuine, free, Apache 2.0 Java client code, under the groupId `io.axoniq`
(**not** `io.axoniq.framework`, the paid tier this project excludes
everywhere else per @docs/CONVENTIONS.md).

This is not a contradiction of the reasoning above — it's a different
axis entirely. These methods work by calling **Axon Server's** own
`DcbEventScheduler` gRPC service. The connector client is free; **Axon
Server itself — the server being connected to — is the thing with
licensing tiers** (SE free/single-node vs. EE paid), the same pattern
already confirmed for `JdbcTokenStore`. Adopting this connector would mean
running Axon Server specifically for scheduling, while everything else in
a monolithic/microservices OpenDCB deployment stays on a self-hosted
Postgres-backed `eventstore-postgres` — an awkward two-backend
architecture, not a simplification, and it reintroduces exactly the Axon
Server dependency `eventstore-postgres` exists to avoid.

**For the record, not a recommendation to build against:** if a team's
deployment already runs Axon Server (having made that separate licensing
decision on its own merits), `DcbEventChannel.scheduleEvent(...)` gives
them real, free-client scheduling against it today, with no need for
`opendcb-scheduling-core` at all. `opendcb-scheduling-core` is the
equivalent capability specifically for teams on OpenDCB's self-hosted,
no-Axon-Server stack — and, being framework-agnostic, works for any
future `integrations/eventstore-<framework>` too, not just Axon.

## opendcb-conductor-bridge: sagas via Conductor OSS, not a hand-built engine

**Why not build our own saga engine, the way we built our own scheduling
mechanism:** a saga/process-manager needs the same order of correctness
rigor as everything else in this toolkit (crash recovery, exactly-once
step execution, compensation) — but proving that rigor ourselves, to the
standard every other module here has been held to, is materially more
work than scheduling was. [Conductor OSS](https://github.com/conductor-oss/conductor)
(Apache 2.0, actively maintained by Orkes + community, in production at
Netflix/Tesla/LinkedIn/J.P. Morgan scale) already provides durable,
replayable workflow execution with genuine saga/compensation support,
verified directly against its source and docs rather than assumed:

- **`postgres-persistence` is a first-class module** in Conductor's own
  repo (real Flyway migrations, Testcontainers-backed tests) — fits
  OpenDCB's self-hosted-Postgres philosophy without forcing Cassandra,
  Elasticsearch, or Redis just to run it. Use a separate database (or at
  minimum a separate schema) from `eventstore-postgres`'s own tables —
  Conductor owns and migrates its own schema independently.
- **Compensation is via a `failureWorkflow`**, not fully-automatic
  per-task undo — worth being precise here since marketing copy can
  overstate this: Conductor triggers your designated failure workflow on
  main-workflow failure, and passes it structured context (`reason`,
  `workflowId`, `failureTaskId`, the full failed workflow's execution
  JSON) — but you still write the actual reverse-order compensation logic
  yourself, as tasks in that failure workflow. The orchestration/triggering
  is automatic; the undo logic is not generated for you.

**Honest trade-off, stated plainly:** Conductor runs as its own server —
same category of thing as Axon Server — a genuine departure from every
other module in this toolkit, which has deliberately avoided requiring
any extra service beyond Postgres + your own JVM. Unlike Axon Server,
there's no paid-tier tension (Conductor is free at every level), so the
cost here is purely operational (one more thing to run), not licensing.

### How this integrates — reusing what's already built, not a new engine

**Starting a saga reactively** needs no new OpenDCB code at all. Conductor
natively supports AMQP as an event source, and `outbox-relay-rabbitmq`
already publishes OpenDCB events to RabbitMQ — Conductor's own
`EventHandler` (registered via its REST API, a JSON definition, not
Java code) can consume from that same exchange directly and auto-start a
workflow when a matching event arrives. This is the direct analog of
Axon 4's `@StartSaga`.

**Reacting to a later correlated event** — the analog of Axon 4's
`@SagaEventHandler(associationProperty = ...)` — works differently in
Conductor and needs one small piece of glue we don't get for free.
Axon maintained its own internal index (event property value → saga
instance). Conductor's `complete_task` action (used to resume a workflow
paused on a `WAIT` task) requires the **workflow's own internal ID** to be
known at the point of completion — it has no equivalent automatic
property-based lookup. `opendcb-conductor-bridge` owns exactly this
missing piece: a `saga_correlation(correlation_key, conductor_workflow_id,
created_at)` table, populated when `start_workflow`'s response is
captured, consulted whenever a later event needs to route a
`complete_task` call to the right running instance. Same shape as every
other small lookup table in this toolkit (`scheduled_event`,
`relay_position`) — not a new engine, just the missing mapping.

**A saga step actually doing something in our domain** — the analog of a
`@SagaEventHandler` method dispatching a command — is a Conductor task
worker (using Conductor's Java SDK, on Maven Central) that, when polled
for work, dispatches an Axon command via `CommandGateway`. This is the
same integration point `opendcb-scheduling-core` already uses to invoke
Axon from outside its own command-handling path, and it's the reason this
module — unlike `opendcb-scheduling-core` — genuinely needs a dependency
on `org.axonframework`.

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
| `opendcb-axon-spring-boot-routing` | `com.highkeen.opendcb.routing.axon.springboot` |
| `outbox-relay-core` | `com.highkeen.opendcb.relay.core` |
| `opendcb-scheduling-core` | `com.highkeen.opendcb.scheduling.core` |
| `opendcb-conductor-bridge` | `com.highkeen.opendcb.conductor.bridge` |
| `outbox-relay-kafka` | `com.highkeen.opendcb.relay.kafka` |
| `outbox-relay-rabbitmq` | `com.highkeen.opendcb.relay.rabbitmq` |
| `outbox-relay-webhook` | `com.highkeen.opendcb.relay.webhook` |
| `bootstrap-axon-postgres` | `com.highkeen.opendcb.bootstrap.axon.postgres` |
| `opendcb-axon-spring-boot-starter` | `com.highkeen.opendcb.springboot.axon` |

Maven `groupId` for every module: `com.highkeen.opendcb`.