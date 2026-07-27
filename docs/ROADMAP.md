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
│   │   Testcontainers instance — that integration gap is closed. Payload
│   │   (de)serialization uses Axon's real Converter SPI (JacksonConverter)
│   │   rather than an ad-hoc ObjectMapper; no upcaster is applied, since
│   │   Axon 5.1.2 has no released upcaster SPI to wire up (see the open
│   │   questions section below).
│   │   Depends on: eventstore-core, org.axonframework.
│   │
│   └── eventstore-<future-framework>/
│       Placeholder pattern for if/when a second framework becomes relevant.
│       Same shape as eventstore-axon. eventstore-postgres/mysql/mongo need
│       zero changes to support it.
│
├── opendcb-axon-scheduling/
│   OpenDCB's OWN abstraction for scheduled/deferred command dispatch and
│   deadline detection — NOT an implementation of Axon's DeadlineManager/
│   EventScheduler, since neither the interface nor any implementation is
│   published in any org.axonframework artifact (verified: both exist only
│   in Axon's own internal, explicitly-not-to-be-released axon-todo module —
│   same unreleased status as the upcaster SPI, but with no free interface
│   to target at all, unlike upcasting). See docs/ARCHITECTURE.md's
│   "opendcb-axon-scheduling" section for the full rationale on why this is
│   a new abstraction rather than an Axon SPI implementation.
│   A scheduled_command table (own schema, independent of EventStoreStorage
│   entirely) + a poller (structurally similar to OutboxRelay, but
│   dispatching due commands via Axon's real, free CommandGateway instead of
│   publishing to a transport).
│   Status: NOT STARTED. Depends on: org.axonframework (CommandGateway only)
│   + JDBC — deliberately NOT on eventstore-core, since it has no need to
│   read/write the event log itself.
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
│   Licensing check: verified JdbcTokenStore resolves exclusively to
│   org.axonframework, Apache 2.0, confirmed via dependency tree + JAR
│   manifest + GitHub source + Maven Central search for io.axoniq.framework
│   (zero results) — no rework needed, no licensing conflict.
│   Depends on: eventstore-core, integrations/eventstore-axon.
│
├── outbox-relay-core/
│   Generic relay: tails an eventstore-* log from a persisted "last relayed
│   position", exposes a Publisher SPI. Framework-agnostic — reads directly
│   from EventStoreStorage, so it works regardless of which framework(s)
│   are producing events.
│   Status: DONE — OutboxRelay.runOnce() reads the last relayed position via
│   RelayPositionStore, calls EventStoreStorage.readRange, and publishes each
│   event in order, persisting the new position immediately after each one
│   (not batched at the end) so a crash mid-batch resumes from the correct
│   event. PublishException is sealed to exactly RetryablePublishException
│   (stop the batch, don't advance past the failed event, so the next
│   runOnce() retries it) and NonRetryablePublishException (hand the event to
│   DeadLetterSink, advance past it, keep going) — the sealed hierarchy
│   forces every Publisher implementation to commit to one or the other, per
│   docs/CONVENTIONS.md's retryable-vs-non-retryable error-handling guidance.
│   LoggingDeadLetterSink (java.lang.System.Logger, no logging-facade
│   dependency) is the only DeadLetterSink this module provides; a real sink
│   (dead-letter table, separate topic) is a transport's concern.
│   start(pollInterval)/stop() run runOnce() on a daemon
│   ScheduledExecutorService thread, catching and logging any RuntimeException
│   per poll so one bad cycle doesn't silently kill all future polling (a
│   scheduled task that throws suppresses its own future executions
│   otherwise). JdbcRelayPositionStore is plain, generic ANSI SQL — legitimately
│   provider-agnostic unlike eventstore-postgres, since it's a trivial
│   single-row-per-relay-name table (update-then-insert instead of a
│   vendor-specific upsert, since MERGE/ON CONFLICT syntax isn't portable).
│   Tested against an in-memory EventStoreStorage/Publisher double (ordering,
│   retryable-stops-and-retries, non-retryable-dead-letters-and-advances) plus
│   a dedicated durability test: one OutboxRelay/JdbcRelayPositionStore runs
│   and stops, then a second, fully independent OutboxRelay + a brand-new
│   JdbcRelayPositionStore instance against the same H2 DataSource resumes
│   from the correct position — proving the position genuinely persisted to
│   the database rather than living in the first relay's Java object.
│   Depends on: eventstore-core only.
│
├── outbox-relay-rabbitmq/
│   RabbitMqPublisher implements Publisher using the plain com.rabbitmq:amqp-client
│   library directly (no Spring AMQP), built ahead of outbox-relay-kafka since
│   it fits Highkeen's typical single-team/self-hosted deployment targets
│   better and outbox-relay-core's Publisher SPI is fully transport-agnostic,
│   so there's no dependency forcing a specific transport order.
│   Status: DONE — publish() serializes the StoredEvent record directly to
│   JSON (its own payloadJson/messageType/tags/metadata fields travel as-is,
│   never derived from an Axon type), enables publisher confirms
│   (confirmSelect()) and the mandatory flag with a ReturnListener, then maps
│   failures per docs/CONVENTIONS.md's retryable-vs-non-retryable split:
│   IOException/ShutdownSignalException from opening a channel or from
│   basicPublish itself -> RetryablePublishException (connection/channel
│   unavailable); TimeoutException or a ShutdownSignalException from
│   waitForConfirmsOrDie -> RetryablePublishException; an IOException
│   specifically from waitForConfirmsOrDie -> NonRetryablePublishException
│   (a broker nack); the ReturnListener flag being set after a successful
│   confirm -> NonRetryablePublishException (unroutable message). Every one
│   of these signatures was grounded by reading the real amqp-client source
│   (ChannelN.waitForConfirmsOrDie, AMQChannel.ensureIsOpen) rather than
│   assumed — including a real bug this caught: AlreadyClosedException and
│   the getCloseReason()-triggered ShutdownSignalException are unchecked
│   RuntimeExceptions thrown from methods declared to only throw IOException,
│   so they had to be caught explicitly or they'd have escaped as uncaught
│   errors instead of RetryablePublishException. Takes a Connection rather
│   than a Channel in its constructor — a deliberate choice, documented in
│   the class Javadoc: the real client closes the channel itself on both a
│   nack and a confirm timeout, so a long-lived Channel would become
│   permanently unusable after the first failure; RabbitMqPublisher instead
│   lazily opens a fresh Channel whenever the current one is missing or
│   closed. Tested with a real RabbitMQ Testcontainers instance: a published
│   event is received intact by a real consumer bound to the exchange/
│   routing key; closing the connection before publishing throws
│   RetryablePublishException; publishing to a routing key with no bound
│   queue throws NonRetryablePublishException. A further end-to-end test
│   wires a real OutboxRelay against a real PostgreSQL EventStoreStorage and
│   this Publisher against a real RabbitMQ broker — events appended to
│   Postgres are relayed and received by a test consumer in order — the
│   first true proof of the whole cross-bounded-context flow, not just
│   eventstore-postgres and outbox-relay-rabbitmq verified independently.
│   Depends on: outbox-relay-core, com.rabbitmq:amqp-client.
│
├── outbox-relay-kafka/       (Publisher impl, plain Kafka client)
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
    └── microservices-sample/  Two modules, orders-service and shipping-service, each with
        its own eventstore-postgres database — outbox-relay-rabbitmq is the only thing
        crossing the bounded-context boundary. orders-service's PlaceOrder command appends
        both OrderPlaced (internal) and OrderPlacedIntegrationEvent (public) atomically;
        OrdersService.integrationRelay's Predicate<StoredEvent> only matches the integration
        event type, so OrderPlaced is skipped by the relay rather than published.
        shipping-service's IntegrationEventConsumer reads off a real RabbitMQ queue and
        hands the raw envelope to OrderPlacedIntegrationEventTranslator, an anti-corruption
        layer that dispatches a local CreateShipmentCommand rather than treating the
        incoming payload as its own domain event.
        Status: DONE — root pom.xml wires in both orders-service and shipping-service as
        modules; `mvn clean install` from repo root builds all 12 modules and passes every
        test, including both of shipping-service's:
        - `OrderPlacedIntegrationEventTranslatorConcurrentRedeliveryTest.concurrentRedeliveryOfTheSameIntegrationEventProducesExactlyOneShipment`
          proves the redelivery dedup is race-proof, not just sequentially correct: two
          independent OrderPlacedIntegrationEventTranslator instances (standing in for two
          consumer threads/JVMs) translate the identical envelope at the same instant via a
          CountDownLatch start gate — the same pattern as eventstore-postgres's advisory-lock
          conflict test and JdbcTokenStoreClaimConflictTest. The translator has no
          check-then-act: `tryMarkProcessed` attempts `INSERT INTO
          processed_integration_events (event_id)` directly and treats a `23505`
          (unique-violation) SQLState as "already processed" instead of a separate `SELECT`
          first, so Postgres's own primary-key constraint serializes the race — the test
          asserts exactly 1 `ShipmentCreated` event resulted, not 0 or 2.
        - `OrdersToShippingEndToEndTest.orderPlacedInOrdersServiceResultsInExactlyOneShipmentInShippingServiceAndNeverRelaysTheInternalEvent`
          runs against two real Postgres 16 Testcontainers instances (one per service) plus a
          real RabbitMQ 3.13 Testcontainers instance — no in-memory doubles. Dispatches
          PlaceOrder via orders-service's own CommandGateway, asserts its store holds exactly
          2 events (OrderPlaced + OrderPlacedIntegrationEvent), runs OutboxRelay.runOnce()
          once against the real RabbitMqPublisher, and asserts a test subscriber bound to the
          same exchange/routing key as shipping-service's own consumer receives exactly one
          message whose `payloadClass` is `OrderPlacedIntegrationEvent` — then asserts
          polling again for 1s returns null, proving the internal `OrderPlaced` event never
          crosses the boundary. Confirms exactly one `ShipmentCreated` lands in
          shipping-service's own database, re-sources the shipment from a second, brand-new
          AxonConfiguration/StateManager against that same database (orderId/customerId
          match, proving durability rather than in-memory carryover), then republishes the
          identical envelope bytes a second time to simulate broker redelivery and asserts
          the shipment count never exceeds 1 over a 3-second window.
        Depends on: eventstore-postgres, integrations/eventstore-axon,
        bootstrap-axon-postgres, outbox-relay-core, outbox-relay-rabbitmq (shipping-service
        consumes with the plain com.rabbitmq:amqp-client directly, matching
        outbox-relay-rabbitmq's own no-Spring-AMQP convention).
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
8. ~~`outbox-relay-core`~~ — DONE (polling engine, Publisher/DeadLetterSink SPIs,
   JdbcRelayPositionStore).
9. ~~`outbox-relay-rabbitmq`~~ — DONE, built ahead of `outbox-relay-kafka`:
   simpler ops (no cluster) fits Highkeen's typical self-hosted/single-team
   deployment targets better, and there's no architectural dependency
   forcing a specific transport order since `outbox-relay-core`'s Publisher
   SPI is fully transport-agnostic. First proof the microservices story
   works end-to-end (Postgres -> OutboxRelay -> RabbitMQ -> a real
   consumer), not just each piece in isolation. `outbox-relay-kafka` —
   NOT STARTED.
10. ~~`examples/microservices-sample`~~ — DONE. Proves the full cross-bounded-context
    story from end to end with real infrastructure (two Postgres containers, one
    RabbitMQ container, no in-memory doubles): orders-service places an order,
    outbox-relay-rabbitmq relays only the public integration event (never the
    internal domain event), and shipping-service's translator dispatches a local
    command idempotently, including under true concurrent redelivery.
11. `opendcb-axon-scheduling` — new module, not yet started. Solves
    scheduled/deferred command dispatch and deadline detection as OpenDCB's
    own abstraction, since neither Axon's DeadlineManager/EventScheduler
    interfaces nor any implementation are published in any org.axonframework
    artifact (confirmed via direct source/Maven Central verification — see
    docs/ARCHITECTURE.md). Structurally similar to outbox-relay-core (own
    table + poller) but dispatches via CommandGateway instead of publishing
    to a transport, and has no dependency on eventstore-core.
12. `eventstore-mysql`, `eventstore-mongo`, `outbox-relay-kafka`,
    `outbox-relay-webhook`, `bootstrap-axon-mysql`, `bootstrap-axon-mongo` —
    fill in once the pattern is validated once.
13. `integrations/eventstore-<future-framework>` — only if/when a second
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
- **Schema evolution:** RESOLVED (partially) — `integrations/eventstore-axon`
  now uses Axon's real `Converter` SPI (`JacksonConverter`) for payload
  (de)serialization instead of an ad-hoc `ObjectMapper`. Upcasting remains
  unresolved and blocked: Axon Framework 5.1.2 ships no released upcaster/
  `IntermediateEventRepresentation` SPI — the only such code lives in its
  own unreleased `axon-todo` module, explicitly documented by Axon's
  maintainers as "not to be released code." Revisit once Axon ships a real,
  released transformation/upcaster mechanism.
