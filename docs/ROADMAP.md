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
├── opendcb-conductor-bridge/
│   Saga/process-manager support via Conductor OSS
│   (github.com/conductor-oss/conductor, Apache 2.0, actively maintained by
│   Orkes + community, in production at Netflix/Tesla/LinkedIn/J.P. Morgan
│   scale — verified directly against its source and docs, not assumed),
│   not a hand-built saga engine. See docs/ARCHITECTURE.md's
│   "opendcb-conductor-bridge" section for the full rationale: a hand-built
│   saga engine needs the same order of crash-recovery/exactly-once rigor
│   every other correctness-critical module here has been held to, and
│   that's materially more surface area than opendcb-scheduling-core was —
│   adopting a battle-tested engine here is the pragmatic call, not a
│   shortcut.
│   Confirmed via Conductor's own repo: `postgres-persistence` is a
│   first-class module (real Flyway migrations, Testcontainers-backed
│   tests) — fits OpenDCB's self-hosted-Postgres philosophy without forcing
│   Cassandra/Elasticsearch/Redis just to run it. Use a separate database
│   (or at minimum a separate schema) from eventstore-postgres's own
│   tables — Conductor owns and migrates its own schema independently.
│   Honest trade-off, not glossed over: Conductor runs as its own server —
│   same category of thing as Axon Server, and a genuine departure from
│   every other module in this toolkit, which has deliberately avoided
│   requiring any extra service beyond Postgres + your own JVM. Unlike
│   Axon Server there's no paid-tier tension (Conductor is free at every
│   level), so the cost here is purely operational, not licensing.
│   Compensation is via a `failureWorkflow` you write yourself, NOT
│   automatic per-task undo — worth stating precisely since marketing copy
│   can overstate this: Conductor triggers your designated failure
│   workflow on main-workflow failure and passes it structured context
│   (reason, workflowId, failureTaskId, the full failed workflow's
│   execution JSON), but the actual reverse-order compensation logic is
│   tasks you write in that failure workflow. The orchestration/triggering
│   is automatic; the undo logic is not generated for you.
│   Reactive saga-starting needs NO new OpenDCB code: Conductor natively
│   consumes AMQP as an event source, and outbox-relay-rabbitmq already
│   publishes OpenDCB events to RabbitMQ — Conductor's own EventHandler (a
│   JSON definition registered via its REST API, not Java code) subscribes
│   to that same exchange directly. Direct analog of Axon 4's @StartSaga.
│   What this module actually contains is the one piece of glue Axon's own
│   association-property index gave us for free and Conductor doesn't:
│   Conductor's complete_task action (used to resume a workflow paused on
│   a WAIT task — the analog of @SagaEventHandler reacting to a later
│   correlated event) requires the workflow's own internal ID to be known
│   at the point of completion, with no automatic property-based lookup
│   the way Axon's associationProperty had. saga_correlation
│   (correlation_key, conductor_workflow_id, created_at) is that missing
│   mapping — populated when start_workflow's response is captured,
│   consulted whenever a later event needs to route a complete_task call
│   to the right running instance. Same shape as every other small lookup
│   table in this toolkit (scheduled_event, relay_position) — not a new
│   engine, just the missing plumbing.
│   Also contains a thin base for Conductor task workers (using
│   Conductor's Java SDK, on Maven Central) that, when polled for work,
│   dispatch an Axon command via CommandGateway — the same integration
│   point opendcb-scheduling-core already uses to invoke Axon from outside
│   its own command-handling path, and the reason this module — unlike
│   opendcb-scheduling-core — genuinely needs a dependency on
│   org.axonframework.
│   Status: DONE. Implemented against Conductor's real, published Java SDK
│   coordinates — `io.orkes.conductor:conductor-client:3.9.31-orkes`, NOT
│   `com.netflix.conductor:conductor-client` — verified by resolving against
│   Maven Central and cross-checking with `javap` against the resolved
│   jar's real bytecode, since conductor-oss/java-sdk's GitHub HEAD (tag
│   v6.0.0, internally versioned 5.1.0-SNAPSHOT) is ahead of what's
│   actually published; 3.9.31-orkes is the latest version Central actually
│   resolves. That version gap has one concrete consequence: no direct
│   "complete task by workflowId + taskRefName" convenience method exists
│   in 3.9.31-orkes (unlike GitHub HEAD), so `signalSaga` instead fetches
│   the workflow via `WorkflowClient.getWorkflow` and looks up the task by
│   reference name via `Workflow.getTaskByRefName` before calling
│   `TaskClient.updateTask(TaskResult)` — the only update method the
│   published API actually has.
│   `saga_correlation` deviates from the literal spec in two documented
│   ways: `conductor_workflow_id` is nullable, not `NOT NULL` — a reserving
│   thread inserts its row with a null workflow ID first (an insert-and-let-
│   the-database-reject race-breaker on a unique `correlation_key`,
│   matching examples/microservices-sample's own idempotency pattern), only
│   the winner actually calls Conductor's `startWorkflow`, then fills the ID
│   in afterward — so any losing concurrent caller has something to poll
│   for instead of getting no row at all; and the timestamp column is a
│   portable `TIMESTAMP`, not `TIMESTAMPTZ`, matching outbox-relay-core's
│   own JdbcRelayPositionStore precedent for staying plain ANSI SQL.
│   SagaCorrelationStore (recordCorrelation/findWorkflowId/updateWorkflowId)
│   and ConductorSagaBridge (startSagaIfNotAlreadyRunning/signalSaga) are
│   both plain JDBC + the Conductor client — zero framework dependency.
│   ConductorCommandTaskWorker implements Conductor's real `Worker`
│   interface, deserializes a claimed task's input into a caller-supplied
│   command type via Jackson, and dispatches it through `CommandGateway`
│   (the one class in this module that touches org.axonframework).
│   Tested against real infrastructure, no mocks: a real Conductor OSS
│   server (`conductoross/conductor:next`, Postgres-backed persistence per
│   its own `config-postgres.properties`) plus real PostgreSQL 16
│   Testcontainers instances. `SagaCorrelationStoreTest` (4 tests) covers
│   the JDBC layer alone. `ConductorSagaBridgeIntegrationTest` proves, with
│   a real Conductor server: a `CountDownLatch`-gated true-concurrency race
│   of two `startSagaIfNotAlreadyRunning` calls for the same correlation key
│   results in exactly one Conductor workflow (both callers return the same
│   workflow ID, and `getWorkflows` confirms only one exists); and an
│   end-to-end `signalSaga` call genuinely completes a real WAIT task and
│   progresses the workflow to COMPLETED. `ConductorCommandTaskWorkerTest`
│   proves a real `TaskRunnerConfigurer` polling a real Conductor server
│   hands a claimed task to the worker under test, which deserializes its
│   input and dispatches it through a hand-rolled `StubCommandGateway` test
│   double (`CommandGateway` has two abstract members — `send` and, via
│   `DescribableComponent`, `describeTo` — so it isn't a functional
│   interface and can't be a lambda), matching docs/TESTING.md's framework-
│   adapter-test philosophy of isolating translation-layer bugs from a real
│   CommandBus stack.
│   Build/dependency issues found and fixed along the way, worth recording
│   since at least one is a genuine Maven gotcha specific to this SDK:
│   `conductor-client` declares `conductor-common` transitively at runtime
│   scope only, so the com.netflix.conductor.common.* types this module's
│   source references needed an explicit direct compile-scope dependency;
│   and `conductor-common` itself directly depends on an old
│   `jackson-core:2.13.2`, which was winning Maven's dependency-mediation
│   tie-break over the newer 2.22.1 pulled transitively via
│   jackson-databind (both at equal depth, and "nearest wins" falls back to
│   declaration order on a tie) — this silently left a jackson-core too old
│   to contain `StreamReadConstraints` (added in 2.15) on the runtime
│   classpath, causing a `NoClassDefFoundError` inside the task worker at
│   the exact moment it tried to deserialize a task's input, not at
│   compile time. Fixed by adding an explicit direct dependency on
│   `jackson-core:${jackson.version}`, which always wins depth-based
│   mediation regardless of tie-breaking rules.
│   Depends on: org.axonframework (axon-messaging, CommandGateway only),
│   Conductor's Java SDK (io.orkes.conductor:conductor-client +
│   conductor-common, 3.9.31-orkes), JDBC.
│
├── opendcb-data-protection/
│   Crypto-shredding for erasure compliance (GDPR-style right to be
│   forgotten, India's DPDP Act 2023, RBI cybersecurity/BFSI expectations,
│   PCI-DSS where card data is involved) — motivated by planned BFSI use.
│   NOT an implementation of Axon's Data Protection module, since that's
│   fully paid (io.axoniq.framework:axoniq-data-protection — verified
│   absent, annotations included, from the released org.axonframework
│   source; unlike EventStorageEngine, no free interface exists here at
│   all). See docs/ARCHITECTURE.md's "opendcb-data-protection" section for
│   the full rationale.
│   Own annotations (@DataSubjectId, @PersonalData), an
│   OpenDcbEncryptingConverter wrapping any other Converter (hooks into
│   the same free org.axonframework Converter interface
│   AbstractDcbEventStorageEngine already uses), a per-data-subject key
│   store (own JDBC table — erasure destroys the key material, not the
│   row; an erased_at timestamp stays as an audit record that erasure
│   happened, when, and for whom), an audit log of encrypt/decrypt/erase
│   operations, and a pluggable MasterKeyProvider for envelope encryption
│   (env-var implementation shipped first; KMS/Vault providers are
│   separate, optional modules — see below).
│   AesGcm (AES-256-GCM, JDK-only, no external crypto library) does the
│   actual field encryption; OpenDcbKeyStore owns the per-subject key table
│   (create-on-first-use, INSERT-and-catch-unique-violation to converge
│   concurrent first-use races on one key rather than a separate SELECT
│   first — same check-then-act-avoidance discipline as
│   opendcb-conductor-bridge's saga_correlation table); EnvVarMasterKeyProvider
│   is the shipped default MasterKeyProvider (reads a 32-byte key from a
│   configured env var, Base64-decoded, and never falls back to any default
│   key if it's missing/invalid).
│   Status: DONE. EnvVarMasterKeyProviderTest covers missing/blank/
│   non-Base64/wrong-length env values all failing fast, never falling back,
│   plus a real wrap/unwrap round trip and confirmation that wrapping the
│   same key twice produces different ciphertext (GCM's random nonce).
│   OpenDcbEncryptingConverterIntegrationTest runs against a real PostgreSQL
│   16 Testcontainers instance (own key-store + audit-log tables, no
│   mocking): a full round trip leaves @PersonalData fields encrypted at
│   rest and decrypted correctly on read; both record and non-record
│   (direct-field-mutation) payload shapes are covered; erasing a data
│   subject makes previously-encrypted ciphertext permanently undecryptable
│   without touching the event log; a field that was never encrypted
│   decrypts to null without throwing; concurrent first-use key creation for
│   the same subject converges on exactly one key row; and the audit log is
│   confirmed to never contain plaintext personal data.
│   Depends on: org.axonframework (Converter only), JDBC.
│
├── opendcb-data-protection-vault/
│   MasterKeyProvider implementation against HashiCorp Vault's Transit
│   secrets engine (wrap/unwrap map directly onto Vault's encrypt/decrypt
│   Transit endpoints). Self-hosted, no cloud vendor dependency — same
│   philosophy as eventstore-postgres over Axon Server.
│   Status: DONE. Implemented against the real, grounded jvault-connector
│   API (de.stklcode.jvault:jvault-connector:1.5.5, Apache 2.0 — chosen over
│   Spring Vault, which would violate this module's no-Spring rule, and over
│   BetterCloud's older, unmaintained driver): HTTPVaultConnector.builder(...)
│   .withToken(...).buildAndAuth() — buildAndAuth(), not build(), since only
│   buildAndAuth() performs a real network authentication call, which is
│   what makes fail-fast-on-unreachable-Vault actually work at construction
│   time, not just on first use. wrapKey/unwrapKey call TransitClient's real
│   encrypt/decrypt methods; Vault's own "vault:v1:..." ciphertext string is
│   stored as-is (UTF-8 bytes), no re-encoding. VaultMasterKeyProviderTest
│   runs against a real HashiCorp Vault 1.18.1 Testcontainers instance (no
│   mocking) with the Transit engine enabled: a wrap/unwrap round trip
│   recovers the original key; wrapping the same key twice produces
│   different ciphertext (Transit's non-deterministic AEAD default) and both
│   ciphertexts still unwrap correctly (safe given OpenDcbKeyStore only
│   calls wrapKey once per subject key); the constructor fails fast and
│   clearly when Vault is unreachable; and wrapKey fails clearly when the
│   configured Transit key doesn't exist.
│   Depends on: opendcb-data-protection, HashiCorp Vault's Java client
│   (de.stklcode.jvault:jvault-connector).
│
├── opendcb-data-protection-aws-kms/
│   MasterKeyProvider implementation against AWS KMS (Encrypt/Decrypt
│   operations — deliberately not GenerateDataKey, which has KMS mint a
│   brand-new key server-side rather than wrap a caller-supplied one, so it
│   cannot implement this class's fixed wrapKey(byte[] existingKey)
│   contract; earlier drafts of this doc describing GenerateDataKey/Decrypt
│   were a documentation error, corrected alongside the implementation).
│   Managed, has a Mumbai (ap-south-1) region for RBI-style data
│   localization without cross-border key material transfer.
│   Status: DONE, including real verification. Implemented against AWS SDK v2
│   (software.amazon.awssdk:kms:2.53.1, confirmed current via Central's own
│   maven-metadata.xml), grounded via javap against the real jar rather than
│   assumed: KmsClient.encrypt(EncryptRequest)/.decrypt(DecryptRequest); the
│   constructor takes an already-configured KmsClient + key ID/ARN (this
│   class never builds its own client, so region/credentials/endpoint
│   override are entirely the caller's concern). Every exception these
│   operations can throw is unchecked and shares one common ancestor,
│   SdkException (confirmed via javap against sdk-core/aws-core), so a
│   single catch clause is sufficient and exhaustive for the
│   fail-fast-and-clearly requirement.
│   AwsKmsMasterKeyProviderTest runs unconditionally against a real
│   Testcontainers-backed LocalStack (localstack/localstack:4.9) with KMS
│   enabled: a genuine CreateKey/Encrypt/Decrypt round trip, a clear failure
│   when the configured CMK doesn't exist, and a clear failure on garbage
│   ciphertext. An earlier @EnabledIfEnvironmentVariable(named =
│   "LOCALSTACK_AUTH_TOKEN", matches = ".+") gate was removed after this was
│   directly re-investigated and empirically checked, not re-asserted: the
│   gate's premise (that LocalStack's unified image, since March 2026,
│   requires an account + auth token even for free/non-commercial use) is
│   correct for localstack/localstack:latest and newer releases going
│   forward, but does NOT apply retroactively to already-pinned older tags —
│   and localstack/localstack:4.9 (LocalStack 4.9.2, built 2025-10-06) is
│   one such tag, five months older than the 2026-03-23 cutover. Verified
│   two independent ways: (1) a manual `docker run` of that exact pinned
│   image plus `aws --endpoint-url kms create-key`/`encrypt`/`decrypt` calls
│   against it, with zero LOCALSTACK_AUTH_TOKEN and zero account, succeeded;
│   (2) the real AwsKmsMasterKeyProviderTest suite itself, run via
│   `mvn -pl opendcb-data-protection-aws-kms test
│   -Dtest=AwsKmsMasterKeyProviderTest` with LOCALSTACK_AUTH_TOKEN confirmed
│   unset in the environment, produced `Tests run: 3, Failures: 0, Errors:
│   0, Skipped: 0` / BUILD SUCCESS — not merely compiling or being gated
│   skippable, but actually executing and passing. The
│   `.withEnv("LOCALSTACK_AUTH_TOKEN", ...)` call that previously injected
│   that (frequently unset, so effectively "null") env var into the
│   container was also removed as a latent bug alongside the gate. Community/
│   free-tier LocalStack has always fully emulated KMS's symmetric
│   CreateKey/Encrypt/Decrypt operations — this provider's only use case;
│   known emulation gaps (asymmetric keys, custom key material, plaintext-
│   size validation) don't affect it. No LocalStack account, auth token, or
│   real AWS account is required to run this suite — same as every other
│   Testcontainers-backed suite in this repo.
│   Depends on: opendcb-data-protection, AWS SDK for KMS
│   (software.amazon.awssdk:kms).
│
├── opendcb-snapshot-postgres/
│   A Postgres-backed implementation of Axon's own, genuinely free
│   SnapshotStore interface — NOT OpenDCB's own abstraction, unlike
│   scheduling/sagas/data-protection. Verified directly against the real
│   AxonFramework 5.1.2 source: SnapshotStore, Snapshot, and
│   SnapshotCapableEventStorageEngine all live in org.axonframework,
│   released and free. See docs/ARCHITECTURE.md's
│   "opendcb-snapshot-postgres" section for the full mechanism (Axon's
│   own @Snapshotting annotation drives the write side entirely; this
│   module only implements the two-method SnapshotStore interface —
│   store(QualifiedName, Object identifier, Snapshot) and
│   load(QualifiedName, Object identifier), no ProcessingContext parameter
│   on either, confirmed from the real interface rather than assumed by
│   analogy with EventStorageEngine).
│   Wiring is auto-decoration, not manual construction: registering a
│   PostgresSnapshotStore instance as the SnapshotStore component on an
│   EventSourcingConfigurer is enough — Axon's own default
│   ConfigurationEnhancer (EventSourcingConfigurationDefaults) decorates
│   the registered EventStorageEngine with SnapshotCapableEventStorageEngine
│   automatically. SnapshotCapableEventStorageEngine.decorate(...), a
│   convenience static factory, does not exist at all at this project's
│   pinned 5.1.2 (it's @since 5.3.0) — confirmed by reading the actual
│   5.1.2 class, not by version-number comparison alone. The constructor
│   (@since 5.1.0) does exist but this module never calls it directly,
│   since the auto-decoration path makes manual construction unnecessary —
│   no project-wide or module-local Axon version bump was needed.
│   Smallest new module in the toolkit: one table, two async methods, no
│   relay/correlation/lease machinery needed. Own snapshot table
│   (independent of the event log's own tables): qualified_name,
│   identifier, position, version, payload_class, payload_json,
│   metadata_json, occurred_at, PRIMARY KEY (qualified_name, identifier) —
│   store() upserts via INSERT ... ON CONFLICT (qualified_name, identifier)
│   DO UPDATE, matching SnapshotStore.store's own documented
│   replace-not-append contract. SnapshotStore, Snapshot, and
│   SnapshotCapableEventStorageEngine are all marked @Internal in Axon's
│   own source (may break in minor/patch Axon releases) — pinned tightly,
│   re-verify against source on every Axon version bump.
│   Status: DONE. PostgresSnapshotStoreTest (3 tests, real PostgreSQL 16
│   Testcontainers): store() then load() round-trips the exact payload,
│   position, version, metadata, and timestamp; a second store() for the
│   same qualified name + identifier replaces the snapshot (load() returns
│   the new one, and a direct query confirms exactly one row exists, not
│   two); load() for a never-stored identifier completes with null, not an
│   exception. PostgresSnapshotStoreEndToEndTest (1 test) is the one that
│   actually proves the feature works end-to-end, not just the storage
│   primitive: wraps a real eventstore-postgres-backed EventStorageEngine
│   (via integrations/eventstore-axon's AbstractDcbEventStorageEngine) with
│   this module's PostgresSnapshotStore (registered as the SnapshotStore
│   component, letting Axon's own ConfigurationEnhancer auto-decorate);
│   registers a CounterEntity annotated @Snapshotting(afterEvents = 2)
│   whose @EventSourcingHandler increments an int on each CounterIncremented
│   event; dispatches CreateCounter + 5x IncrementCounter (6 events total)
│   through a real CommandGateway, each IncrementCounter forcing a genuine
│   load via @InjectEntity — per SnapshotPolicy.afterEvents' real semantics
│   (confirmed from source: the threshold is evaluated per individual
│   sourcing/load operation via a local, non-cumulative event counter, not
│   as a running total across the entity's whole history), the third
│   IncrementCounter's load applies 3 events (> 2) and triggers a snapshot
│   store; confirms via direct SQL that a snapshot row now exists; then
│   builds a SECOND, independent EventSourcingConfigurer (same
│   brand-new-configurer-proves-durability pattern as
│   OpenDcbAxonPostgresTest) wired to a test-local CountingEventStoreStorage
│   decorator wrapping a fresh PostgresEventStoreStorage against the same
│   database, and dispatches one more IncrementCounter; asserts the number
│   of events that decorator's readRange actually returned is strictly
│   fewer than the 6 events genuinely appended — direct, observable proof
│   sourcing used the snapshot rather than replaying the full log. All 4
│   tests pass against real PostgreSQL, no mocking.
│   mvn dependency:tree confirms org.axonframework:axon-eventsourcing and
│   org.axonframework:axon-messaging both resolve as direct (depth-1)
│   compile-scope dependencies of this module — grounding its placement in
│   the opendcb-axon.version group, same dependency:tree-based reasoning
│   already applied to opendcb-conductor-bridge and opendcb-data-protection
│   (QualifiedName lives in axon-messaging's org.axonframework.messaging.core
│   package, needed at compile scope since it's a direct parameter type on
│   SnapshotStore's own methods — axon-messaging was added as an explicit
│   direct dependency for this reason, not left transitive-only).
│   Depends on: org.axonframework (axon-eventsourcing — SnapshotStore,
│   Snapshot, SnapshotCapableEventStorageEngine, GlobalIndexPosition/Position;
│   axon-messaging — QualifiedName), JDBC, jackson-databind. Does NOT
│   depend on eventstore-core or eventstore-postgres in main code — both
│   are test-scope-only dependencies, used solely by the end-to-end test.
│
│   **Investigated CI-cost concern, root-caused, not a code defect (2026-08-21):**
│   the first full-reactor `mvn clean install` after this module was added
│   showed `PostgresSnapshotStoreTest` (the plain store/load suite, not the
│   end-to-end test) taking 755.9s in-reactor vs. 1.27s standalone — a
│   16-minute-class outlier for three tests that are individually
│   millisecond-fast. This was followed up rather than dropped, per this
│   project's own standard for anything that could become a recurring CI
│   cost:
│   - **Isolated re-runs (3x, `mvn -pl opendcb-snapshot-postgres test
│     -Dtest=PostgresSnapshotStoreTest`, no `-am`) confirmed the module
│     itself is fine on its own**: 3.14s, 21.74s, 3.58s — normal
│     JVM/Testcontainers-startup jitter (a single ~7x outlier among three
│     runs), nowhere near 755s. The module and its tests are not the
│     problem.
│   - **A second full-reactor `mvn clean install` reproduced the underlying
│     phenomenon, but not pinned to this module** — `bootstrap-axon-postgres`
│     took 8:31 min (vs. 4.2s the first time) and `outbox-relay-rabbitmq`
│     (built earlier in the reactor than this module) outright **failed**
│     after a 1041s (17+ min) `ContainerLaunchException`: Testcontainers'
│     `LogMessageWaitStrategy` timed out waiting for RabbitMQ's own
│     `.*Server startup complete.*` log line. The build never reached
│     `opendcb-snapshot-postgres` this run (later modules show `SKIPPED`),
│     so this specific module's reproduction is inconclusive in isolation —
│     but the class of failure (an arbitrary Testcontainers-backed module,
│     10-20x+ slower than normal, sometimes to outright failure) clearly
│     reproduced, just at a different point in the reactor.
│   - **Root cause, confirmed via `docker ps -a` sampled throughout the run
│     plus host-level diagnostics, is host/Docker-Desktop resource
│     contention on the local dev machine — NOT a Testcontainers
│     container-reuse/reaping leak.** `docker ps -a` never showed a stale
│     container from an earlier module lingering alongside the current
│     one (only the in-flight container plus the long-lived Ryuk reaper
│     were ever present, and Ryuk cleaned up the failed RabbitMQ container
│     promptly once its owning JVM exited) — every module's containers were
│     torn down correctly, ruling out the "Conductor OSS/Vault container
│     still running from an earlier module" hypothesis directly (those two
│     modules were never even reached this run). Instead: a `docker
│     stats`/`docker ps` polling loop sampled every 15s stalled completely
│     for ~15 minutes (13:05:10 → 13:20:36) — i.e. the Docker daemon itself
│     was unresponsive for a sustained stretch, exactly overlapping the
│     `bootstrap-axon-postgres`/`outbox-relay-rabbitmq` window — and host
│     diagnostics taken during that stretch showed a system load average of
│     ~190 and Docker Desktop's own VM process
│     (`com.apple.Virtualization.VirtualMachine`) pegged at ~192% CPU.
│     `docker info` confirms this machine's Docker Desktop VM is capped at
│     10 CPUs / 7.65GB RAM total — small for running Postgres/RabbitMQ (and,
│     in a full run, Conductor OSS/Vault/LocalStack) back-to-back inside one
│     long-lived `mvn clean install`, especially alongside ~57GB of cached
│     images and ~40GB of reclaimable volumes from unrelated projects
│     sharing the same Docker Desktop instance on this developer's machine.
│   - **CI relevance:** this is a local-machine resource-contention finding,
│     not evidence of a bug that would recur identically on a dedicated CI
│     runner (GitHub Actions runners aren't shared with an IDE, browser, and
│     other Docker projects the way this dev machine is). It IS relevant to
│     CI in one respect worth carrying forward: this reactor build is
│     sensitive enough to Docker/CPU/memory contention that a resource-
│     starved runner could see the same class of Testcontainers
│     `LogMessageWaitStrategy` timeout (an outright failure, not just
│     slowness) on an arbitrary module — worth keeping in mind if CI ever
│     shows a flaky, unrelated-looking Testcontainers timeout rather than
│     assuming it's a real regression in whichever module happened to fail.
│     No code or configuration change was made as a result of this
│     investigation — findings only, per the scope of this pass.
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
12. ~~`opendcb-conductor-bridge`~~ — DONE. Saga/process-manager support via
    Conductor OSS rather than a hand-built engine — see the module's own
    entry above and docs/ARCHITECTURE.md's "opendcb-conductor-bridge"
    section for the full rationale, including the honest server-dependency
    trade-off this module accepts that every other module here has
    deliberately avoided. Tested against a real Conductor OSS server plus
    real PostgreSQL, including a true-concurrency race test for the
    correlation table and an end-to-end task-worker test dispatching a real
    Axon command.
13. ~~`opendcb-data-protection`~~ — DONE. Crypto-shredding for erasure
    compliance (GDPR/DPDP Act 2023/RBI/PCI-DSS), motivated by planned BFSI
    use — see the module's own entry above and docs/ARCHITECTURE.md's
    "opendcb-data-protection" section for the full rationale, including
    why this is our own implementation rather than Axon's fully-paid Data
    Protection module. Master key management ships with a pluggable
    MasterKeyProvider interface + an env-var-based implementation first.
14. ~~`opendcb-data-protection-vault`~~ — DONE, including real verification
    (real HashiCorp Vault Testcontainers coverage, actually run and
    passing). ~~`opendcb-data-protection-aws-kms`~~ — DONE too, including
    real verification: its real-backend test suite runs unconditionally
    against a Testcontainers-backed LocalStack (localstack/localstack:4.9,
    pre-dating LocalStack's 2026-03-23 unified-image auth-token
    requirement) and actually passes (3/3 tests) — no LocalStack account
    token or real AWS account needed; see the module's own entry above for
    the full verification detail. Real
    KMS-backed MasterKeyProvider implementations, built immediately after
    opendcb-data-protection itself rather than deferred, per an explicit
    decision to support both a self-hosted (Vault) and a managed-cloud (AWS
    KMS) path from the start — see docs/ARCHITECTURE.md's "Master key
    provider modules" section. Both depend only on opendcb-data-protection's
    MasterKeyProvider interface; neither changes opendcb-data-protection
    itself.
15. ~~`opendcb-snapshot-postgres`~~ — DONE. A Postgres-backed
    implementation of Axon's own free SnapshotStore, not OpenDCB's own
    abstraction — see the module's own entry above and
    docs/ARCHITECTURE.md's "opendcb-snapshot-postgres" section. Smallest
    module in the toolkit (one table, two async methods). Wired via
    auto-decoration (registering the SnapshotStore component is enough —
    Axon's own ConfigurationEnhancer wraps the EventStorageEngine
    automatically), so the 5.3.0-only decorate() factory vs. the 5.1.0
    constructor question turned out moot: this module calls neither
    directly. End-to-end tested against real PostgreSQL, including direct,
    observable proof (a counting EventStoreStorage decorator) that sourcing
    after a snapshot reads fewer events than were actually appended.
16. `eventstore-mysql`, `eventstore-mongo`, `outbox-relay-kafka`,
    `outbox-relay-webhook`, `bootstrap-axon-mysql`, `bootstrap-axon-mongo` —
    fill in once the pattern is validated once.
17. `integrations/eventstore-<future-framework>` — only if/when a second
    framework actually becomes relevant. Not speculative work until then.

## Open questions worth deciding before writing more code

- **Publishing target:** RESOLVED — GitHub under the Highkeen-Technologies
  org (already done; see the repo's own remote) + Maven Central, via the
  current (2025+) Central Publisher Portal, not the old OSSRH/Nexus staging
  workflow (shut down 2025-06-30). POM preparation for the 13 publishable
  modules (`eventstore-core`, `eventstore-postgres`,
  `integrations/eventstore-axon`, `bootstrap-axon-postgres`,
  `opendcb-axon-spring-boot-starter`, `opendcb-axon-spring-boot-routing`,
  `outbox-relay-core`, `outbox-relay-rabbitmq`, `opendcb-scheduling-core`,
  `opendcb-conductor-bridge`, `opendcb-data-protection`,
  `opendcb-data-protection-vault`, `opendcb-data-protection-aws-kms` —
  `examples/*` deliberately excluded, since those aren't meant to be
  depended on externally) is DONE: `<name>`,
  `<description>`, `<url>`, `<scm>` on every one of the 13 (verified against Maven's own inheritance
  rules that `<licenses>`/`<developers>`/`<description>` inherit as-is from
  the root `pom.xml` so they're declared there once, while `<name>` never
  inherits and `<url>`/`<scm>` inherit but with the child's artifactId
  auto-appended to the parent's value — which would corrupt this mono-repo's
  single GitHub/git URLs — so those three are repeated explicitly, and
  identically, in each of the 13 modules instead of relying on inheritance);
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
  per-module via a minimal `<build><plugins>` stub in each of the 13 — so
  `examples/*` never triggers them, without needing an explicit `<skip>` on
  each example module. `maven-deploy-plugin`'s own default `deploy` binding
  is separate from all four of the above, though: it runs for every module
  in the reactor regardless of what `central-publishing-maven-plugin` does
  elsewhere, so each of the 3 `examples/*` modules additionally declares
  `maven-deploy-plugin` (3.1.4, the latest stable — not the `4.0.0-beta-2`
  Maven Central currently lists as `<release>`/`<latest>`) directly in its
  own `<build><plugins>` with `<skip>true</skip>`, so `mvn deploy`/`mvn
  clean deploy` never attempts to publish them.

  **`opendcb-data-protection`, `opendcb-data-protection-vault`, and
  `opendcb-data-protection-aws-kms` wired in after the fact** (this
  Publishing-target write-up originally covered 10 modules, before these
  three existed): version-group placement was determined by
  `mvn dependency:tree` on each module, not assumed — `opendcb-data-protection`
  resolves `org.axonframework:axon-messaging` as a **direct** dependency
  (its `OpenDcbEncryptingConverter` implements Axon's real `Converter` SPI),
  so it versions under `opendcb-axon.version`, the same group as
  `opendcb-conductor-bridge`. `opendcb-data-protection-vault` and
  `opendcb-data-protection-aws-kms` each resolve `axon-messaging` only as a
  **transitive** dependency, pulled in solely through their own dependency
  on `opendcb-data-protection` — `mvn dependency:tree`'s indentation shows
  it nested under that dependency, never at the module's own top level —
  confirming neither has any direct `org.axonframework` import of its own
  (`VaultMasterKeyProvider`/`AwsKmsMasterKeyProvider` implement only
  `com.highkeen.opendcb.dataprotection.MasterKeyProvider`, whose two-method
  signature is plain `byte[]`, no framework type anywhere). Depending on an
  Axon-tied module doesn't itself make a module Axon-tied — the same
  relationship `eventstore-postgres` has to `eventstore-core` — so both
  version under `opendcb.version` instead, each declaring an explicit
  `${opendcb-axon.version}` reference on their own `opendcb-data-protection`
  dependency (a version-boundary crossing, same pattern
  `opendcb-data-protection-vault`'s own pom.xml comment documents). All
  three already carried the full source/javadoc/GPG/central-publishing/
  flatten-maven-plugin stub plus `maven.deploy.skip=false` when this was
  verified — no plugin wiring was missing, only this document's own module
  count and list were stale.

  **Bug found by the first real `workflow_dispatch` run (2026-07-30), fixed
  same day:** that per-module `<skip>true</skip>` stub was only ever added to
  `examples/*`, not to the root aggregator `pom.xml` itself — but the
  aggregator (`packaging=pom`, artifactId `opendcb`) is also a reactor
  project and goes through the `deploy` phase like everything else. It has
  no `central-publishing-maven-plugin` (only the 9 publishable modules
  declare that) and no deploy-skip, so `mvn clean deploy -P release` failed
  immediately with `Deployment failed: repository element was not specified
  in the POM inside distributionManagement element or in
  -DaltDeploymentRepository=...` — before the build even reached a real
  module. (The version-resolution/validation step that ran just before it
  worked correctly: a manual run with input `1.0.0-rc.1` correctly produced
  `opendcb.version=1.0.0-rc.1` and `opendcb-axon.version=1.0.0-rc.1-axon5.1`
  — this was purely a deploy-skip gap, not a versioning bug.) Fixed with a
  `maven.deploy.skip` property instead of another per-module plugin stub:
  confirmed via `mvn maven-deploy-plugin:3.1.4:help -Dgoal=deploy
  -Ddetail=true` that the plugin's own `skip` parameter's built-in default
  expression already is `${maven.deploy.skip}` (same mechanism as
  `maven.test.skip`), so no explicit
  `<configuration><skip>${maven.deploy.skip}</skip></configuration>` needed
  to be added to the plugin's `pluginManagement` entry at all — setting the
  property anywhere in the effective POM is enough. Root `pom.xml` sets
  `maven.deploy.skip` to `true` in its own `<properties>` (covering the
  aggregator itself and, redundantly-but-harmlessly, `examples/*`, which
  keep their pre-existing explicit stub too); each of the 9 publishable
  modules overrides it back to `false` in its own `<properties>` — the same
  override-in-the-child shape already used for `opendcb.version`/
  `opendcb-axon.version`. Verified empirically before trusting it, per this
  project's own "never guess a framework's API" convention applied to Maven
  itself: `mvn help:effective-pom` on `eventstore-core` confirmed a
  plain `<build><plugins>` binding at the root (the first fix attempted)
  *does* inherit into every child's effective POM and would have wrongly
  re-skipped deploy for all 9 publishable modules too — that approach was
  reverted in favor of the property override, and `mvn help:effective-pom`
  was re-run to confirm the root resolves `maven.deploy.skip=true` while all
  9 publishable modules resolve `false`. `mvn -N deploy` at the root (no
  altDeploymentRepository, no credentials) now exits `0` (skipped) instead
  of reproducing the original error; a full `mvn clean install` across all
  13 modules still passes with the property added.

  Binding `gpg:sign` to the
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
  `opendcb-scheduling-core`, `opendcb-data-protection-vault`,
  `opendcb-data-protection-aws-kms` — the latter two despite depending on the
  Axon-tied `opendcb-data-protection`, since neither has a direct
  `org.axonframework` dependency of its own, confirmed via
  `mvn dependency:tree`; see the "Publishing target" entry above), and
  `opendcb-axon.version` for the Axon-tied group (`integrations/eventstore-axon`,
  `bootstrap-axon-postgres`, `opendcb-axon-spring-boot-starter`,
  `opendcb-axon-spring-boot-routing`, `opendcb-conductor-bridge`,
  `opendcb-data-protection`).
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

  The committed values of `opendcb.version` (`1.1.1-SNAPSHOT`) and
  `opendcb-axon.version` (`1.1.1-axon5.1-SNAPSHOT`) are LOCAL-DEV DEFAULTS
  ONLY, not "the current release version" — a plain local `mvn install`
  with no `-D` override just uses whatever's committed, which is fine since
  that build is never published. These carry a standard Maven `-SNAPSHOT`
  suffix — a deliberate policy adoption of Maven's own dev-version
  convention, not the scheme's original design — with `-SNAPSHOT` always
  the trailing segment, confirmed empirically (not assumed) via a scratch
  `mvn install` comparison: `1.1.1-axon5.1-SNAPSHOT` gets recognized as a
  snapshot by Maven's tooling (a version-level `maven-metadata-local.xml`
  is generated on install), while `1.1.1-SNAPSHOT-axon5.1` is silently
  treated as an ordinary release version instead — so the qualifier must
  always precede `-SNAPSHOT`, never follow it. Every child module's own
  `<parent><version>` is a hardcoded literal (Maven doesn't allow a
  property expression there, since the parent POM is resolved before
  properties are interpolated) and must independently track the root
  aggregator's `<version>` — verified the hard way: letting these drift out
  of sync doesn't fail the build, it silently resolves `${opendcb.version}`/
  `${opendcb-axon.version}` from whatever matching parent version happens
  to already be cached in `~/.m2` instead of the reactor's own `pom.xml`.
  The real, tag-driven scheme:

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
     pattern as the other Central-publishing plugins, in each of the 10
     publishable modules (`examples/*` never opts in, matching the existing
     pattern).
  6. Immediately after a successful deploy, `release.yml`'s final step
     bumps the committed `pom.xml` values forward — not back to the
     just-released version, but to that version's *next patch*, with
     `-SNAPSHOT` appended: releasing `1.1.0` bumps `main`'s
     `opendcb.version` to `1.1.1-SNAPSHOT` and `opendcb-axon.version` to
     `1.1.1-axon5.1-SNAPSHOT` (only the release's `X.Y.Z` prefix is used
     for this computation — a pre-release qualifier like `-rc.1` on the
     released version is ignored). Patch, not minor, is the deliberate
     default bump size: it costs nothing to under-guess, since this
     committed value is never what actually gets published — the next real
     release still resolves its own version from its own tag/input via the
     `-D` overrides in step 4 above, entirely independent of whatever patch
     number happens to be sitting in `main`. This step also patches every
     child module's own hardcoded `<parent><version>` literal to match, in
     the same commit — required so the reactor stays internally consistent
     (see the stale-parent-version failure mode noted above). Same
     protected-`main`-bypass mechanism as before (the `opendcb-release-bot`
     GitHub App, not `GITHUB_TOKEN`) — see
     `docs-site/docs/contributing.md`'s "Workflow and branch protection"
     section.

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

  **Next-patch-`-SNAPSHOT` bump (step 6 above) verified separately,
  end-to-end, when it was adopted:** the bump script's version arithmetic
  was run inside a real `ubuntu:24.04` Docker container (not the local
  macOS/zsh shell, which was confirmed to silently mis-execute it —
  `BASH_REMATCH` is unset under zsh, and BSD `sed` doesn't support GNU
  sed's `"0,/pattern/"` first-match address the same way), matching the
  real `ubuntu-latest` Actions runner's bash + GNU sed. Confirmed correct
  output across `1.1.0` -> `1.1.1-SNAPSHOT` / `1.1.1-axon5.1-SNAPSHOT`,
  `1.2.3-rc.1` -> `1.2.4-SNAPSHOT` (pre-release qualifier correctly
  ignored), and `2.0.9` -> `2.0.10-SNAPSHOT` (double-digit patch handled
  correctly, no leading-zero or truncation bug). Separately, a real `mvn
  clean install -DskipTests` against the SNAPSHOT-baseline `pom.xml` (no
  `-D` overrides) exited `0` and produced `.flattened-pom.xml` files
  correctly showing the plain SNAPSHOT values; a second, targeted `mvn
  clean install -DskipTests -pl <module> -am
  -Dopendcb.version=9.9.9 -Dopendcb-axon.version=9.9.9-axon5.1` run
  confirmed the override mechanism works identically regardless of whether
  the committed baseline is a plain release-shaped string or a `-SNAPSHOT`
  string. Both runs' exit codes were checked directly against a redirected
  log file rather than through a piped `tail`, since `mvn ... | tail -N;
  echo $?` reports `tail`'s exit code, not `mvn`'s, without `pipefail`.
- **Schema evolution:** RESOLVED (partially) — `integrations/eventstore-axon`
  now uses Axon's real `Converter` SPI (`JacksonConverter`) for payload
  (de)serialization instead of an ad-hoc `ObjectMapper`. Upcasting remains
  unresolved and blocked: Axon Framework 5.1.2 ships no released upcaster/
  `IntermediateEventRepresentation` SPI — the only such code lives in its
  own unreleased `axon-todo` module, explicitly documented by Axon's
  maintainers as "not to be released code." Revisit once Axon ships a real,
  released transformation/upcaster mechanism.
