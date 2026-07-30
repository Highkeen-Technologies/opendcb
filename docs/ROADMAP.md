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
├── opendcb-scheduling-core/
│   OpenDCB's OWN abstraction for scheduling events — NOT an implementation
│   of any Axon interface (none exists, released or otherwise — see
│   docs/ARCHITECTURE.md's "opendcb-scheduling-core" section for the full
│   pivot history: this module originally scheduled *commands* via Axon's
│   CommandGateway, then was redesigned to schedule *events* directly,
│   matching Axon's own EventScheduler/DcbEventChannel.scheduleEvent
│   precedent). Firing a scheduled event is just an
│   EventStoreStorage.appendAtomically call, so this module has ZERO
│   dependency on org.axonframework — only on eventstore-core, the same
│   shape as outbox-relay-core.
│   Status: DONE (core scheduling mechanism — schedule/cancel/lease-based
│   claim-reclaim/dispatch-by-append). Two things are deliberately deferred
│   as documented future enhancements, not oversights — see below.
│   Own scheduled_event table (independent of the event log's own tables):
│   id (UUID PK), scheduled_time, event_id (unique), message_type,
│   payload_class, payload_json, metadata_json, tags_json, scope_name,
│   status (PENDING/IN_PROGRESS/COMPLETED/CANCELLED), claimed_at, worker_id,
│   created_at.
│   ScheduledEventStore.claimDue(now, batchSize, workerId, leaseDuration)
│   claims due rows cross-JVM-safely with a single
│   SELECT ... FOR UPDATE SKIP LOCKED WHERE (status = 'PENDING' AND
│   scheduled_time <= ?) OR (status = 'IN_PROGRESS' AND claimed_at <= ?)
│   (i.e. lease-expired), then a batched UPDATE to IN_PROGRESS in the same
│   transaction — grounded in real Postgres MVCC/locking semantics (a
│   locked row only counts as a SKIP LOCKED candidate if it still matches
│   WHERE at lock-attempt time; a row another worker already claimed and
│   committed simply stops matching and is never a candidate), not assumed.
│   cancel(scheduleId) is a safe no-op unless the row is still PENDING (an
│   UPDATE ... WHERE status = 'PENDING' guard) — cancelling something
│   already claimed is a normal race against a concurrent claimDue, not an
│   error. markCompleted(scheduleId, workerId) guards on
│   WHERE status = 'IN_PROGRESS' AND worker_id = ? rather than status alone
│   — a status-only guard would let a worker whose lease already expired and
│   was reclaimed by another worker overwrite that other worker's in-flight
│   claim with a stale completion, since the row is IN_PROGRESS again just
│   under new ownership; fencing on worker_id closes that gap.
│   ScheduledEventDispatcher polls claimDue, reconstructs a StoredEvent
│   (position = -1) from each due row, and appends it via
│   EventStoreStorage.appendAtomically, marking the row COMPLETED only on a
│   successful append; a failed append is logged and the row left
│   IN_PROGRESS for a later lease-expiry reclaim, rather than reverted,
│   unless that reclaim would exceed the row's own attempt budget (see the
│   retry-cap/dead-letter paragraph below). start(Duration pollInterval)/
│   stop() use the same daemon ScheduledExecutorService shape as OutboxRelay.
│   Retry cap and dead-letter handling: scheduled_event carries its own
│   attempt_count INT NOT NULL DEFAULT 0 and max_attempts INT NOT NULL
│   columns (max_attempts set at schedule() time via a DEFAULT_MAX_ATTEMPTS
│   = 5 overload — enough claim attempts to ride out a handful of transient
│   failures without retrying a permanently-broken schedule forever), plus a
│   new terminal DEAD_LETTERED status alongside PENDING/IN_PROGRESS/
│   COMPLETED/CANCELLED. Inside the same locked transaction that identifies
│   candidate rows, claimDue now branches each row BEFORE claiming: if
│   claiming it would push attempt_count past max_attempts, it transitions
│   straight to DEAD_LETTERED (a separate batched UPDATE from the ordinary
│   IN_PROGRESS one) and is never returned as claimable again; otherwise it
│   is claimed as before with attempt_count incremented. claimDue returns a
│   ClaimBatch(claimed, deadLettered) record bundling both outcomes rather
│   than two separate calls, since the branch has to happen inside that one
│   transaction. DeadLetterSink (onDeadLetter(ScheduledEventRecord, String
│   reason)) plus its default LoggingDeadLetterSink (System.Logger at
│   ERROR) are a locally-defined mirror of outbox-relay-core's
│   DeadLetterSink pattern — same shape, independent implementation, since
│   this module still depends only on eventstore-core, not
│   outbox-relay-core (same relationship ScheduledEventStore already has to
│   JdbcRelayPositionStore). ScheduledEventDispatcher's constructor takes an
│   optional DeadLetterSink (defaulting to LoggingDeadLetterSink); runOnce()
│   forwards every row a poll's claimDue dead-lettered to the sink with a
│   reason of "exceeded max_attempts (N)" before dispatching the claimed
│   batch. Tested (real PostgreSQL 16 Testcontainers): a row scheduled with
│   max_attempts=2, claimed and abandoned twice (each past its lease), is
│   dead-lettered on the third claimDue call rather than returned claimable,
│   with ClaimBatch.deadLettered() containing exactly that row; a
│   dead-lettered row is confirmed genuinely terminal by querying its status
│   directly and by further claimDue calls returning it in neither list;  a
│   row that succeeds within its budget (completes on attempt 2 of
│   max_attempts=5) never reaches DEAD_LETTERED; and a dedicated fencing
│   test confirms markCompleted's WHERE status = 'IN_PROGRESS' AND
│   worker_id = ? guard still correctly no-ops when a stale worker's late
│   completion targets a row a different worker's claimDue call
│   dead-lettered out from under it in the interim, rather than resurrecting
│   it. A dispatcher-level end-to-end test confirms runOnce() invokes a
│   capturing DeadLetterSink exactly once with the expected reason string
│   and never appends the dead-lettered event to a real
│   PostgresEventStoreStorage.
│   Tested against a real PostgreSQL 16 Testcontainers instance (via
│   eventstore-postgres's PostgresEventStoreStorage as the EventStoreStorage
│   under test): claimDue returns a row only at/after its scheduled_time,
│   never before; cancel on a still-PENDING row prevents it from ever being
│   claimed; cancel on an IN_PROGRESS row is a safe no-op (proven by a
│   subsequent markCompleted still succeeding); a dedicated lease-expiry
│   test has worker A claim a row with a short lease and never complete it,
│   sleeps past the lease, then has a second, fully independent
│   ScheduledEventStore (worker B) reclaim the same row via claimDue, and
│   confirms worker A's late markCompleted call afterward does not clobber
│   worker B's active claim; a CountDownLatch ready/go race test (mirroring
│   eventstore-postgres's and opendcb-axon-spring-boot-routing's concurrent
│   claim tests) has two independent ScheduledEventStore instances call
│   claimDue at the same instant against six overlapping due rows plus one
│   row already claimed with an unexpired lease, asserting zero overlap
│   between what each instance claims, that the two claimed sets union to
│   exactly the six due rows, and that neither instance claims the
│   unexpired-lease row; and an end-to-end ScheduledEventDispatcher test
│   schedules an event, runs the dispatcher once against a real
│   PostgresEventStoreStorage, and confirms the event is genuinely readable
│   back via storage.readRange afterward.
│   DCB-native conflict-predicate safety net (the optional feature deferred
│   from the initial pass, see docs/ARCHITECTURE.md's
│   "opendcb-scheduling-core" section) is now DONE too: since a persisted
│   scheduled_event row can't hold a real java.util.function.Predicate, the
│   check is instead modeled as ConflictCriteria — a serializable record
│   (Set<StoredEvent.StoredTag> requiredTags, Set<String> messageTypes,
│   empty = match any type) stored as a nullable conflict_criteria_json
│   column (null = no check, fully backward compatible with every existing
│   schedule(...) overload and pre-existing row). Same spirit as Axon's own
│   EventCriteria (tags + message types), expressed in this module's own
│   framework-agnostic terms, zero Axon dependency. schedule(...) gained a
│   5-arg overload accepting an optional ConflictCriteria; the existing
│   3-arg and 4-arg overloads delegate down to it with conflictCriteria =
│   null, unchanged in their own public signature. A new terminal
│   SKIPPED_CONFLICT status sits alongside PENDING/IN_PROGRESS/COMPLETED/
│   CANCELLED/DEAD_LETTERED. For each row claimDue returns as claimed,
│   ScheduledEventDispatcher.runOnce() checks record.conflictCriteria() != 
│   null before building/appending the event: findConflictingEvent(criteria)
│   does a paginated, full-scan-plus-in-memory-predicate read via
│   storage.readRange (batches of 500, same deliberate v1 simplicity
│   docs/PROVIDERS.md already establishes for provider-side tag filtering —
│   not a shortcut specific to this feature). If a match is found, the row
│   is marked SKIPPED_CONFLICT instead of appended and a ConflictSkipSink
│   is notified with the conflicting event; if none is found, the row fires
│   exactly as before. ConflictSkipSink (onConflictSkip(ScheduledEventRecord,
│   StoredEvent conflictingEvent)) plus its default LoggingConflictSkipSink
│   are a locally-defined mirror of DeadLetterSink's shape but semantically
│   distinct — LoggingConflictSkipSink logs at INFO, not ERROR, since a
│   conflict skip is a deliberate by-design outcome, not a failure.
│   markSkippedConflict(UUID, String workerId) was added to
│   ScheduledEventStore, sharing its WHERE status = 'IN_PROGRESS' AND
│   worker_id = ? fencing guard with markCompleted via a common private
│   updateIfInProgressAndOwned(...) helper — same worker-fencing reasoning
│   as before: a stale worker's late skip-or-complete decision must not
│   clobber a row a different worker has since reclaimed. Tested (real
│   PostgreSQL 16 Testcontainers): a row with conflict criteria and no
│   matching event in the log fires normally and reaches COMPLETED; a row
│   with conflict criteria matching an event already in the log is never
│   appended (confirmed via storage.readRange afterward), reaches
│   SKIPPED_CONFLICT, and invokes a capturing ConflictSkipSink exactly once
│   with the correct conflicting event; a row scheduled via the pre-existing
│   3-arg overload (null conflictCriteria) fires identically to before this
│   feature existed, confirming no regression; and a dedicated fencing test
│   mirrors the dead-letter fencing test's pattern — worker A claims a row,
│   its lease expires before it can finish the conflict check, worker B
│   reclaims it, and worker A's late markSkippedConflict call afterward is a
│   no-op, proven by worker B's own subsequent markCompleted still
│   succeeding.
│   Depends on: eventstore-core only.
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
11. ~~`opendcb-scheduling-core`~~ — DONE. Solves scheduled/deferred event
    firing as OpenDCB's own abstraction, since no Axon interface for this
    exists, released or otherwise (confirmed via direct source/Maven
    Central verification — see docs/ARCHITECTURE.md). Structurally similar
    to `outbox-relay-core` (own table + poller), but fires by *appending*
    via `EventStoreStorage.appendAtomically` rather than publishing to a
    transport — so, unlike the original command-dispatch design it
    replaced, it has zero dependency on `org.axonframework` and depends
    only on `eventstore-core`. Both of the enhancements originally deferred
    from the first pass — the retry-cap/dead-letter mechanism and the
    DCB-native conflict-predicate safety net (`ConflictCriteria`,
    `SKIPPED_CONFLICT`, `ConflictSkipSink`) — are now DONE too (see the
    module's own status entry above).
12. `eventstore-mysql`, `eventstore-mongo`, `outbox-relay-kafka`,
    `outbox-relay-webhook`, `bootstrap-axon-mysql`, `bootstrap-axon-mongo` —
    fill in once the pattern is validated once.
13. `integrations/eventstore-<future-framework>` — only if/when a second
    framework actually becomes relevant. Not speculative work until then.

## Open questions worth deciding before writing more code

- **Publishing target:** RESOLVED — GitHub under the Highkeen-Technologies
  org (already done; see the repo's own remote) + Maven Central, via the
  current (2025+) Central Publisher Portal, not the old OSSRH/Nexus staging
  workflow (shut down 2025-06-30). POM preparation for the 9 publishable
  modules (`eventstore-core`, `eventstore-postgres`,
  `integrations/eventstore-axon`, `bootstrap-axon-postgres`,
  `opendcb-axon-spring-boot-starter`, `opendcb-axon-spring-boot-routing`,
  `outbox-relay-core`, `outbox-relay-rabbitmq`, `opendcb-scheduling-core` —
  `examples/*` deliberately excluded, since those aren't meant to be
  depended on externally) is DONE: `<name>`, `<description>`, `<url>`,
  `<scm>` on every one of the 9 (verified against Maven's own inheritance
  rules that `<licenses>`/`<developers>`/`<description>` inherit as-is from
  the root `pom.xml` so they're declared there once, while `<name>` never
  inherits and `<url>`/`<scm>` inherit but with the child's artifactId
  auto-appended to the parent's value — which would corrupt this mono-repo's
  single GitHub/git URLs — so those three are repeated explicitly, and
  identically, in each of the 9 modules instead of relying on inheritance);
  `maven-source-plugin` (3.4.0, the latest stable — not the `4.0.0-beta-1`
  Maven Central currently lists as `<release>`) and `maven-javadoc-plugin`
  (3.12.0) wired to attach `-sources.jar`/`-javadoc.jar`;
  `central-publishing-maven-plugin` (`org.sonatype.central`, 0.11.0 — looked
  up from Maven Central directly, not assumed) with
  `<extensions>true</extensions>` and `publishingServerId=central`,
  deliberately without `autoPublish=true` yet, so the first release lands as
  a manual-publish deployment reviewable at central.sonatype.com before
  going live (Central is immutable — a published version can never be
  removed or modified); `maven-gpg-plugin` (3.2.8) wired to sign every
  artifact at deploy time with no passphrase in this repo — it relies on the
  plugin's own current, non-deprecated `useAgent`/`passphraseEnvName`
  (`MAVEN_GPG_PASSPHRASE`) mechanisms instead of its deprecated
  `passphrase`/`passphraseServerId` parameters. All four plugins are
  declared once in the root `pom.xml`'s `pluginManagement` and opted into
  per-module via a minimal `<build><plugins>` stub in each of the 9 — so
  `examples/*` never triggers them, without needing an explicit `<skip>` on
  each example module. `maven-deploy-plugin`'s own default `deploy` binding
  is separate from all four of the above, though: it runs for every module
  in the reactor regardless of what `central-publishing-maven-plugin` does
  elsewhere, so each of the 3 `examples/*` modules additionally declares
  `maven-deploy-plugin` (3.1.4, the latest stable — not the `4.0.0-beta-2`
  Maven Central currently lists as `<release>`/`<latest>`) directly in its
  own `<build><plugins>` with `<skip>true</skip>`, so `mvn deploy`/`mvn
  clean deploy` never attempts to publish them. Binding `gpg:sign` to the
  `verify` phase (which plain `mvn install` also runs) broke local builds
  with no configured GPG key, fixed via a `gpg.skip` property (root
  `pom.xml`, defaults `true`) referenced in the gpg-plugin's managed
  `<configuration><skip>${gpg.skip}</skip></configuration>`, flipped to
  `false` only inside a `release` Maven profile (`mvn clean deploy -P
  release`) — so CI's actual publish step must pass `-P release` or signing
  silently no-ops.

  GitHub Actions automation is now DONE too:
  `.github/workflows/release.yml` runs on `workflow_dispatch` (manual) or a
  pushed `v*.*.*` tag — deliberately no automatic trigger on an ordinary
  push to main, since publishing must always be a deliberate human action.
  It checks out, sets up JDK 21, imports the GPG key via
  `crazy-max/ghaction-import-gpg` (current, actively maintained; also seeds
  `gpg-agent`'s passphrase cache for `maven-gpg-plugin`'s `useAgent`),
  writes `~/.m2/settings.xml` containing only the literal placeholder text
  `${env.CENTRAL_USERNAME}`/`${env.CENTRAL_PASSWORD}` (Maven resolves these
  from the environment at build time — the real values are never written to
  a file or echoed to a log), then runs `mvn --batch-mode clean deploy
  -DskipTests=false -P release` with `CENTRAL_USERNAME`, `CENTRAL_PASSWORD`,
  and `MAVEN_GPG_PASSPHRASE` set from the `CENTRAL_USERNAME`,
  `CENTRAL_PASSWORD`, and `GPG_PASSPHRASE` GitHub repo secrets respectively
  (`GPG_PRIVATE_KEY` feeds the GPG-import step). All four secrets are
  referenced by name only via `${{ secrets.* }}` — no credential value is
  ever hardcoded anywhere in this repo. Confirmed GitHub-hosted
  `ubuntu-latest` runners ship Docker preinstalled, so the Testcontainers-
  based test suite (required to pass before anything publishes, per
  `-DskipTests=false`) needs no extra runner setup. As with the plugin
  config above, `autoPublish` is deliberately never set in this workflow:
  `central-publishing-maven-plugin` uploads and validates a deployment
  bundle, but a human must still sign in to central.sonatype.com and click
  "Publish" after reviewing it, since Central is immutable — switching to
  `autoPublish=true` once the manual-review step has proven itself over a
  few releases is a deliberate future decision to revisit, not an
  oversight.

  What this preparation does NOT yet cover, and needs doing outside any
  coding session before the real first release-tag push: a Sonatype Central
  account + verified `com.highkeen.opendcb` namespace, a generated Central
  Portal user token, and a real GPG keypair (public key published to a
  keyserver) — these must be provisioned and then stored as the
  `CENTRAL_USERNAME`, `CENTRAL_PASSWORD`, `GPG_PRIVATE_KEY`, and
  `GPG_PASSPHRASE` GitHub repo secrets that `release.yml` already references
  by name.
- **Versioning:** RESOLVED — modules split into two independently-versioned
  groups, since framework-agnostic modules don't share Axon's release
  cadence, and versioning is now fully tag-driven end to end: no commit or PR
  ever exists solely to bump a version number in a `pom.xml`.

  Root aggregator `pom.xml` defines two properties: `opendcb.version` for the
  framework-agnostic group (`eventstore-core`, `eventstore-postgres`/`-mysql`/
  `-mongo`, `outbox-relay-core`/`-kafka`/`-rabbitmq`/`-webhook`,
  `opendcb-scheduling-core`), and `opendcb-axon.version` for the Axon-tied
  group (`integrations/eventstore-axon`, `bootstrap-axon-postgres`,
  `opendcb-axon-spring-boot-starter`, `opendcb-axon-spring-boot-routing`).
  Each member of a group declares its own explicit `<version>` (e.g.
  `<version>${opendcb.version}</version>`), overriding the version it would
  otherwise inherit from the parent — Maven only inherits a child's
  `<version>` when the child does not declare its own (confirmed against the
  Maven POM reference's "Inheritance" section). Every inter-module
  `<dependency>` that crosses a version boundary references
  `${opendcb.version}` or `${opendcb-axon.version}` explicitly instead of
  `${project.version}`, since that property only ever resolves to the
  version of the POM being built, not of whatever it depends on — it would
  silently point at the wrong version for any dependency once the two
  groups diverge. `examples/*` modules keep no `<version>` override at all
  and simply inherit the aggregator's version unchanged, since they're not
  meant to be published or depended on externally; each carries a one-line
  comment explaining why.

  The committed values of `opendcb.version` (`1.0.0`) and
  `opendcb-axon.version` (`1.0.0-axon5.1`) are LOCAL-DEV DEFAULTS ONLY, not
  "the current release version" — a plain local `mvn install` with no `-D`
  override just uses whatever's committed, which is fine since that build is
  never published. The real, tag-driven scheme:

  1. A release is cut by pushing a `vX.Y.Z` tag (e.g. `v1.2.3`), or by
     running `.github/workflows/release.yml` manually via
     `workflow_dispatch`, which requires an explicit `version` input
     (`required: true`) since a manual run has no tag to derive one from —
     never a guessed or defaulted version.
  2. The workflow's "Resolve and validate release version" step extracts the
     version from `${GITHUB_REF_NAME}` (tag push, stripping the leading `v`)
     or from the `version` input (`workflow_dispatch`), then validates it
     against a semver-shaped pattern (`X.Y.Z`, optionally `-qualifier`) and
     fails the run immediately with `::error::` if it doesn't match, rather
     than letting a malformed tag silently reach `mvn deploy`.
  3. A workflow-level `AXON_VERSION_QUALIFIER` constant near the top of
     `release.yml` (currently `axon5.1`) is appended to the resolved version
     to build `opendcb-axon.version` (e.g. `1.2.3-axon5.1`). This constant is
     bumped manually and deliberately only when the project upgrades to a new
     Axon Framework release — it is never derived from the release tag/
     version, since the two version lines are independent: a `1.2.3` release
     tag says nothing about which Axon Framework version the Axon-coupled
     modules were built against.
  4. The `mvn --batch-mode clean deploy -P release` step passes both resolved
     values on the command line as `-Dopendcb.version=...` and
     `-Dopendcb-axon.version=...`, overriding the committed local-dev
     defaults for that build only.
  5. `flatten-maven-plugin` (`org.codehaus.mojo`, `1.8.0` — looked up from
     Maven Central directly, not assumed) resolves those now-overridden
     properties into a generated, literal-valued flattened POM that
     transparently replaces the original `pom.xml` for install/deploy
     purposes, so the POM actually uploaded to Maven Central contains a real
     version number, never the unresolved text `${opendcb.version}`.
     `flattenMode=ossrh` was chosen deliberately over the plugin's own
     `resolveCiFriendliesOnly` mode — verified against the plugin's
     `flatten-mojo.html`/`usage.html` docs, not assumed: `resolveCiFriendliesOnly`
     only resolves the three specific, plugin-reserved property names
     `revision`/`sha1`/`changelist` (Maven's "CI Friendly Versions"
     convention), and this project's own custom property names
     (`opendcb.version`/`opendcb-axon.version`) don't match those, so that
     mode would leave them unresolved — silently defeating the purpose.
     `ossrh` mode instead computes the full effective POM (resolving every
     property placeholder, custom-named or not) while keeping exactly the
     optional POM elements Sonatype's OSS Repository-Hosting requirements
     need (name, description, url, licenses, developers, scm) and stripping
     the rest. Bound via the plugin's own documented execution pattern: the
     `flatten` goal in the `process-resources` phase (early enough that
     compile/test/package/install/deploy all see the flattened POM), and the
     `clean` goal (its own default binding to the `clean` lifecycle) to
     delete the generated `.flattened-pom.xml` so it never lingers as an
     untracked file. Declared once in the root `pom.xml`'s `pluginManagement`
     and opted into per-module via the same minimal `<build><plugins>` stub
     pattern as the other Central-publishing plugins, in each of the 9
     publishable modules (`examples/*` never opts in, matching the existing
     pattern).

  Verified end-to-end pre-`flatten-maven-plugin` (still holds with it added,
  confirmed via a fresh `mvn clean install`): `mvn clean install` from a
  `.m2` cache with the prior `0.1.0-SNAPSHOT` artifacts deliberately removed
  first (so a stale cache couldn't mask a misconfiguration) produces
  `BUILD SUCCESS` across all 13 modules, and `mvn dependency:tree` on
  `bootstrap-axon-postgres`, `opendcb-axon-spring-boot-starter`, and
  `shipping-service` each confirm the resolved coordinates land at the
  correct explicit version per module (e.g. `bootstrap-axon-postgres`
  resolves `eventstore-axon:1.0.0-axon5.1` and `eventstore-postgres:1.0.0`
  side by side, not a shared version). The previously-noted "'version'
  contains an expression but should be a constant" Maven warning is now a
  non-issue for anything published externally: `flatten-maven-plugin`
  resolves it away in the POM that actually ships to Maven Central; it's
  still visible during a plain local reactor build, which is harmless.
  Deploying to Maven Central itself (real secrets, a real pushed tag) is
  outside what a coding session can verify locally — see the "does NOT yet
  cover" note above.
- **Schema evolution:** RESOLVED (partially) — `integrations/eventstore-axon`
  now uses Axon's real `Converter` SPI (`JacksonConverter`) for payload
  (de)serialization instead of an ad-hoc `ObjectMapper`. Upcasting remains
  unresolved and blocked: Axon Framework 5.1.2 ships no released upcaster/
  `IntermediateEventRepresentation` SPI — the only such code lives in its
  own unreleased `axon-todo` module, explicitly documented by Axon's
  maintainers as "not to be released code." Revisit once Axon ships a real,
  released transformation/upcaster mechanism.
