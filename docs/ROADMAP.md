# OpenDCB — Roadmap

An open-source toolkit that fills the gap Axon Framework 5 (free) deliberately
leaves to Axoniq Framework (paid): DCB-capable event storage on any backing
store, and distributed event routing without Axon Server. Designed so the
storage layer can outlive Axon specifically, in case another event-sourcing
framework worth supporting shows up later.

## Positioning

- **Not a replacement for Axon Framework.** `integrations/eventstore-axon` is
  a set of `EventStorageEngine` implementations that plug into it — same
  relationship any community Axon extension has.
- **Not trying to match Axon Server feature-for-feature.** No clustering/HA
  replication, no server-managed segment push, no Multi-Context. Those stay
  genuine reasons to eventually pay for Axon Server at scale. This toolkit
  targets the "self-hosted, single-team, don't need enterprise HA yet" tier.
- **Built framework-agnostic from the storage layer down**, specifically so
  that if a second event-sourcing framework becomes relevant, supporting it
  is "add one integrations/ module," not a rewrite. See
  @docs/ARCHITECTURE.md's design principle section.
- **License:** Apache 2.0, matching Axon Framework itself.
- **Maintained by Highkeen Technologies** — groupId `com.highkeen.opendcb`,
  published under a Highkeen GitHub org.

## Module skeleton

```
opendcb/
│
├── eventstore-core/
│   Framework-agnostic. StoredEvent, EventStoreStorage (port). Zero
│   dependency on Axon or any other framework — this is the layer meant to
│   survive a framework change.
│   Status: DONE — extracted from the sample project, package renamed to
│   com.highkeen.opendcb.eventstore.core.
│   Depends on: nothing but the JDK + Jackson (for StoredEvent's payload field).
│
├── eventstore-postgres/
│   Status: DONE — PostgresEventStoreStorage, working JDBC implementation.
│   Depends on: eventstore-core, postgresql driver (provided scope).
│
├── eventstore-mysql/
│   Status: TEMPLATE ONLY — schema + GET_LOCK strategy documented, not implemented.
│
├── eventstore-mongo/
│   Status: TEMPLATE ONLY — document shape + transaction strategy documented.
│
├── integrations/
│   ├── eventstore-axon/
│   │   AbstractDcbEventStorageEngine — implements Axon's real
│   │   EventStorageEngine SPI, translating to/from StoredEvent/EventStoreStorage.
│   │   The ONLY module that imports org.axonframework.
│   │   Status: DONE — extracted from the sample project (was originally
│   │   bundled into eventstore-core before the framework-agnostic split;
│   │   moved here).
│   │   Depends on: eventstore-core, org.axonframework.
│   │
│   └── eventstore-<future-framework>/
│       Placeholder pattern for if/when a second framework becomes relevant.
│       Same shape as eventstore-axon. eventstore-postgres/mysql/mongo need
│       zero changes to support it.
│
├── routing-spring-boot-axon/
│   Wires Axon's own free JdbcTokenStore against whichever eventstore-*
│   datasource is active, so PooledStreamingEventProcessor segments can be
│   claimed/rebalanced across multiple JVM instances. Axon-specific by
│   nature (token stores are an Axon concept) — a future framework would
│   need its own routing-<mechanism>-<framework> module.
│   Status: NOT STARTED.
│   Depends on: eventstore-core, integrations/eventstore-axon.
│
├── outbox-relay-core/
│   Generic relay: tails an eventstore-* log from a persisted "last relayed
│   position", exposes a Publisher SPI. Framework-agnostic — reads directly
│   from EventStoreStorage, so it works regardless of which framework(s)
│   are producing events.
│   Status: NOT STARTED. Design is settled — implementation is the polling
│   loop + Publisher interface + its own tiny position-tracking table.
│
├── outbox-relay-kafka/       (Publisher impl, plain Spring Kafka)
├── outbox-relay-rabbitmq/    (Publisher impl, plain Spring AMQP)
├── outbox-relay-webhook/     (Publisher impl, simple HTTP callbacks)
│   Status: NOT STARTED.
│
├── spring-boot-starter-axon/
│   Packages eventstore-postgres (or whichever provider) +
│   integrations/eventstore-axon + routing-spring-boot-axon + outbox-relay-*
│   as a real Spring Boot starter with proper
│   META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports.
│   Named "-axon" specifically so a future spring-boot-starter-<framework>
│   is its own module, not a branch inside this one.
│   Status: IN PROGRESS — OpenDcbPostgresAutoConfiguration +
│   OpenDcbProperties scaffolded; needs the routing/relay pieces once those
│   modules exist.
│   Depends on: integrations/eventstore-axon, a provider, routing-spring-boot-axon.
│
└── examples/
    ├── monolith-sample/       (DONE, as axon5-sample — needs package rename)
    └── microservices-sample/  (NOT STARTED)
```

## The two usage patterns this needs to support

**Monolithic / single-bounded-context deployment**
`eventstore-postgres` + `integrations/eventstore-axon` + `routing-spring-boot-axon`
only. Multiple pods of the same app scale horizontally via the shared
`JdbcTokenStore` — no broker, no relay. Default recommendation for most
single-team deployments unless there's a concrete reason to split services.

**Event-driven microservices across bounded contexts**
Add `outbox-relay-core` + one transport module. Each service still uses
`eventstore-postgres` internally for its own event log; the relay is the
only thing that crosses the boundary, and only publishes what's been
deliberately exposed (public/integration event shape, not raw internal
events).

## Suggested build order

1. ~~`eventstore-core` + `eventstore-postgres`~~ — DONE, extracted with
   package rename to `com.highkeen.opendcb.*`.
2. ~~`integrations/eventstore-axon`~~ — DONE, split out of what was
   originally bundled with eventstore-core.
3. `spring-boot-starter-axon` — IN PROGRESS. Finish once routing exists.
4. `routing-spring-boot-axon` — small, high value, unblocks multi-instance scaling.
5. `outbox-relay-core` + `outbox-relay-kafka` — unlocks the microservices story.
6. `examples/microservices-sample` — proves 1–5 actually compose correctly.
7. `eventstore-mysql`, `eventstore-mongo`, `outbox-relay-rabbitmq`,
   `outbox-relay-webhook` — fill in once the pattern is validated once.
8. `integrations/eventstore-<future-framework>` — only if/when a second
   framework actually becomes relevant. Not speculative work until then.

## Open questions worth deciding before writing more code

- **Publishing target:** GitHub under a Highkeen org + Maven Central via
  Sonatype, or internal to Highkeen first, open-sourced once proven on real
  client engagements?
- **Versioning:** tie module versions to the Axon Framework version they
  were built against (e.g. `1.0.0-axon5.1`)? Framework-agnostic modules
  (`eventstore-core`, providers) could version independently from
  `integrations/eventstore-axon`, since they don't actually depend on Axon's
  release cadence.
- **Schema evolution:** `integrations/eventstore-axon` currently uses
  Jackson directly rather than Axon's `Converter`/upcaster SPI (documented
  as a deliberate simplification). Worth deciding now whether the toolkit
  commits to wiring the real `Converter` SPI before v1.0.
- **Testing strategy:** the shared `EventStoreStorageContractTest` suite
  (@docs/TESTING.md) should be built alongside `eventstore-postgres`, not
  after — retrofitting it once three providers exist independently is more
  work and more likely to surface a provider that's subtly non-compliant.
