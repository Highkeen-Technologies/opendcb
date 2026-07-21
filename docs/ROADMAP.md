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
│   Framework-agnostic. StoredEvent (record, with nested StoredTag) and
│   EventStoreStorage (port: appendAtomically, readRange, maxPosition,
│   minPosition, positionAtOrAfter, ConcurrentAppendConflictException).
│   Zero dependency on Axon or any other framework — this is the layer
│   meant to survive a framework change.
│   Status: DONE — port defined and unit tested.
│   Depends on: nothing but the JDK (StoredEvent carries payload as a
│   pre-serialized JSON string, so no Jackson dependency here).
│
├── eventstore-postgres/
│   PostgresEventStoreStorage — plain JDBC, no ORM. appendAtomically uses a
│   transaction-scoped pg_advisory_xact_lock to serialize concurrent appends
│   across JVMs, then conflict-checks the tail in-transaction before insert.
│   Status: DONE — passes the full EventStoreStorageContractTest suite
│   (@docs/TESTING.md) against a real PostgreSQL 16 Testcontainers instance,
│   including the concurrent cross-JVM conflict test (two EventStoreStorage
│   instances racing an overlapping-predicate append; verified the advisory
│   lock is load-bearing, not just present, by rerunning that test in
│   isolation and by tracing the check-then-act race that appears if the
│   lock is removed).
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
│   │   Status: DONE — implemented and unit tested against an in-memory
│   │   EventStoreStorage test double. Now also proven wired up against a
│   │   real provider: bootstrap-axon-postgres exercises eventstore-axon +
│   │   eventstore-postgres together end-to-end against a real Postgres 16
│   │   Testcontainers instance — that integration gap is closed.
│   │   Depends on: eventstore-core, org.axonframework.
│   │
│   └── eventstore-<future-framework>/
│       Placeholder pattern for if/when a second framework becomes relevant.
│       Same shape as eventstore-axon. eventstore-postgres/mysql/mongo need
│       zero changes to support it.
│
├── opendcb-axon-spring-boot-routing/
│   Wires Axon's own free JdbcTokenStore against whichever eventstore-*
│   datasource is active, so PooledStreamingEventProcessor segments can be
│   claimed/rebalanced across multiple JVM instances. Axon-specific by
│   nature (token stores are an Axon concept) — a future framework would
│   need its own routing-<mechanism>-<framework> module.
│   Status: DONE — OpenDcbRoutingProperties + OpenDcbTokenStoreAutoConfiguration
│   implemented against the real Axon 5.1.2 JdbcTokenStore API (grounded by
│   cloning AxonFramework/AxonFramework at the axon-5.1.2 tag, not assumed
│   from 4.x knowledge). Resolves the event store's own DataSource by
│   looking up a bean named "openDcbEventStoreDataSource" via BeanFactory —
│   never a compile-time dependency on opendcb-axon-spring-boot-starter —
│   falling back to the application's sole DataSource bean with an explicit
│   INFO warning if the name isn't found, since that fallback may silently
│   point token coordination at the wrong database. Uses JacksonConverter
│   (transitively available via axon-messaging -> axon-conversion, no new
│   dependency) rather than Axon's Spring-aware GeneralConverter, since the
│   Spring-specific converter/connection-provider classes live in Axon's
│   own extensions/spring modules, off-limits per this module's dependency
│   rules. Schema creation uses GenericTokenTableFactory rather than
│   PostgresTokenTableFactory — a deliberate tradeoff, not an oversight:
│   the resolved DataSource may not even be the event store's own database
│   (the primary-DataSource fallback path), so the "compatible with most
│   databases" factory is the safer default; a Postgres-specific factory
│   (bytea token column instead of BLOB) remains a possible future
│   optimization once/if a provider-aware variant is worth the complexity.
│   Verified beyond bean-wiring: a dedicated cross-JVM test constructs two
│   independent JdbcTokenStore instances (distinct nodeId each, standing in
│   for two JVMs) against one shared H2 database — the second instance's
│   claim attempt on a segment the first still holds throws
│   UnableToClaimTokenException, and succeeds only after the first releases
│   it. This is the guarantee the whole module exists for; bean-resolution
│   tests alone don't demonstrate it. A second test goes further and proves
│   the SELECT ... FOR UPDATE row lock is load-bearing under true
│   concurrency, not just sequential ordering: a CountDownLatch pair forces
│   both nodes' fetchToken calls into their FOR UPDATE at the same instant
│   (mirroring eventstore-postgres's advisory-lock race test), asserting
│   succeededA ^ succeededB — exactly one of the two simultaneous claims
│   succeeds.
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
├── bootstrap-axon-postgres/
│   One factory class (OpenDcbAxonPostgres.engine(dataSource)) gluing
│   integrations/eventstore-axon + eventstore-postgres together, so a plain
│   Java/Quarkus/Micronaut consumer gets a working EventStorageEngine in one
│   call instead of hand-assembling it every project. Also the first place
│   in the repo where eventstore-axon and eventstore-postgres are actually
│   exercised together — see the note on that gap above. See
│   @docs/ARCHITECTURE.md's "Bootstrap modules" section for the dependency
│   rules this tier follows.
│   Status: DONE — OpenDcbAxonPostgres.engine(DataSource[, autoCreateSchema])
│   implemented; proven end-to-end with a real Postgres 16 Testcontainers
│   instance, a trivial @EventSourcedEntity, and a command dispatched
│   against one EventSourcingConfigurer then durably re-sourced from a
│   second, brand-new EventSourcingConfigurer against the same database.
│   Depends on: integrations/eventstore-axon, eventstore-postgres.
│
├── opendcb-axon-spring-boot-starter/
│   OpenDcbPostgresAutoConfiguration — a Spring Boot 4 @AutoConfiguration
│   that resolves an openDcbEventStoreDataSource bean (reuse the app's
│   primary DataSource via opendcb.eventstore.datasource.use-primary, build
│   a dedicated one from opendcb.eventstore.datasource.url, or fall back to
│   the primary DataSource with a logged INFO message) and delegates to
│   bootstrap-axon-postgres's OpenDcbAxonPostgres.engine(...) for the
│   EventStorageEngine bean. Named opendcb-axon-... (not spring-boot-...)
│   per Spring Boot's own starter-naming convention — third-party starters
│   must not be prefixed spring-boot, since that implies official Spring
│   support. A future opendcb-<framework>-spring-boot-starter would be its
│   own module, not a branch inside this one.
│   Status: DONE — OpenDcbProperties + OpenDcbPostgresAutoConfiguration
│   implemented (Spring Boot 4.1.0 / Spring Framework 7), backing off
│   correctly both when opendcb.eventstore.provider=none and when a
│   user-supplied EventStorageEngine bean already exists (in which case
│   neither openDcbEventStoreDataSource nor any dedicated DataSource gets
│   constructed either — verified via ApplicationContextRunner with an
│   unreachable datasource.url, proving no connection is ever attempted).
│   Six ApplicationContextRunner tests cover the fallback/use-primary/
│   explicit-url DataSource resolution paths and both back-off cases.
│   Does not yet package opendcb-axon-spring-boot-routing or outbox-relay-*
│   (those modules don't exist yet) — this covers the eventstore-postgres +
│   integrations/eventstore-axon wiring only, via bootstrap-axon-postgres.
│   Depends on: bootstrap-axon-postgres.
│
└── examples/
    ├── monolith-sample/       (NOT STARTED)
    ├── plain-java-sample/     Status: DONE — a plain public static void main(String[])
    │   wires everything by hand (PGSimpleDataSource, OpenDcbAxonPostgres.engine(dataSource),
    │   an EventSourcingConfigurer) with zero Spring anywhere in the module (verified via
    │   grep -r "org.springframework" examples/plain-java-sample/ returning nothing but its
    │   own README's instructions). Domain: AccountEntity (state only) +
    │   AccountCommandHandlers, a separate Stateful Command Handler class using @InjectEntity
    │   for the instance command (DepositMoney) alongside a creational command (OpenAccount),
    │   per docs/CONVENTIONS.md. Also serves as the first true end-to-end smoke test of the
    │   whole stack beyond isolated unit/contract tests: run against a real Postgres 16
    │   container, it dispatches OpenAccount + two DepositMoney commands through one
    │   EventSourcingConfigurer, then shuts it down and builds a second, fully independent
    │   EventSourcingConfigurer (autoCreateSchema=false) against the same database — the
    │   re-sourced entity's state matched the write-side state exactly (balance=150.00 both
    │   times), proving durability rather than in-memory carryover between configurers.
    │   Depends on: bootstrap-axon-postgres, postgresql driver (compile scope — this is a
    │   standalone app, not a library, so unlike eventstore-postgres the driver isn't provided).
    └── microservices-sample/  (NOT STARTED)
```

## The two usage patterns this needs to support

**Monolithic / single-bounded-context deployment**
`eventstore-postgres` + `integrations/eventstore-axon` + `opendcb-axon-spring-boot-routing`
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

1. ~~`eventstore-core`~~ — DONE (port only: `StoredEvent`, `EventStoreStorage`).
2. ~~`eventstore-postgres`~~ — DONE (`PostgresEventStoreStorage`, plus the
   `EventStoreStorageContractTest` suite from @docs/TESTING.md, now living
   in `eventstore-core`'s test-jar and passing against a real Postgres 16
   Testcontainers instance).
3. ~~`integrations/eventstore-axon`~~ — DONE (adapter implemented and unit
   tested against an in-memory `EventStoreStorage` double; wiring against a
   real `eventstore-postgres` instance end-to-end is item 4, below).
4. ~~`bootstrap-axon-postgres`~~ — DONE. First thing that actually proves
   eventstore-axon + eventstore-postgres work together, not just
   independently, via a real Postgres-backed integration test.
5. ~~`opendcb-axon-spring-boot-starter`~~ — DONE. Delegates to
   bootstrap-axon-postgres rather than duplicating its wiring; does not yet
   package opendcb-axon-spring-boot-routing or outbox-relay-*, since neither
   exists yet (item 6, below).
6. ~~`opendcb-axon-spring-boot-routing`~~ — DONE. Unblocks multi-instance
   scaling; the cross-JVM claim-conflict test proves the actual guarantee,
   not just bean wiring.
7. ~~`examples/plain-java-sample`~~ — DONE. Proves the no-Spring path via
   bootstrap-axon-postgres directly; also doubles as the first true
   end-to-end smoke test of the whole stack (dispatch via commands, read
   back via a second, independent EventSourcingConfigurer) rather than
   isolated unit/contract tests.
8. `outbox-relay-core` + `outbox-relay-kafka` — unlocks the microservices story.
9. `examples/microservices-sample` — proves the full stack composes correctly
   across bounded contexts.
10. `eventstore-mysql`, `eventstore-mongo`, `outbox-relay-rabbitmq`,
    `outbox-relay-webhook`, `bootstrap-axon-mysql`, `bootstrap-axon-mongo` —
    fill in once the pattern is validated once.
11. `integrations/eventstore-<future-framework>` — only if/when a second
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
