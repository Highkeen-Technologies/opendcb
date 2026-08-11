# Testing

## EventStoreStorage contract suite

Every provider (`eventstore-postgres`, `eventstore-mysql`, `eventstore-mongo`,
future ones) must pass a shared abstract test class —
`EventStoreStorageContractTest` (`com.highkeen.opendcb.eventstore.core`) —
that each provider module extends with its own setup (e.g. Testcontainers
instance). This lives in `eventstore-core`'s test-jar so providers can
depend on it without duplicating test code, and without pulling in any
framework dependency to test a framework-agnostic module.

The suite must cover, at minimum:

- **Append + read round-trip**: appended events come back in order via
  `readRange`, with tags and metadata intact.
- **Conflict detection**: two appends whose predicates both match the same
  prior event — the second must throw `ConcurrentAppendConflictException`
  and leave no partial write.
- **Concurrent cross-JVM safety**: simulate two "JVMs" (two separate
  `EventStoreStorage` instances against the same backing store) appending
  concurrently with overlapping conflict predicates — exactly one must
  succeed.
- **Position semantics**: `firstToken`/`minPosition`, `latestToken`/
  `maxPosition`, and `positionAtOrAfter` return correct values on an empty
  store, a single-event store, and a multi-event store.
- **Ordering under concurrent, non-conflicting appends**: events from
  different logical streams (non-overlapping tags) interleave correctly by
  position — no store may silently reorder them.

## Framework adapter tests (integrations/eventstore-axon and future adapters)

Each `integrations/eventstore-<framework>` module tests its adapter class
(e.g. `AbstractDcbEventStorageEngine`) against a simple in-memory
`EventStoreStorage` test double — not a real database — since the goal is to
isolate translation-layer bugs (Axon/framework type <-> `StoredEvent`) from
storage-layer bugs, which the contract suite above already covers
separately.

## Entity/command-handling tests (Axon-specific)

Use `AxonTestFixture` (`org.axonframework.test.fixture`, given/when/state,
exception assertions) for anything exercising Axon entity command handling.
No dedicated AxonTestFixture-based entity command-handling test currently
exists in this repo — `examples/plain-java-sample`'s `AccountEntity`/
`AccountCommandHandlers` demonstrate the pattern structurally, but without
a corresponding test. Writing one is a reasonable future contribution;
until then, follow `AxonTestFixture`'s own documentation directly
(`org.axonframework.test.fixture`) for the given/when/then pattern.

## Server-integration tests (real external server, not just a database)

`opendcb-conductor-bridge` is the current example of a module that wraps a
real external server (Conductor OSS) rather than just a database. Its
concurrency-critical paths — the `saga_correlation` start-race and the
signal/complete-task flow — are tested against a real Conductor OSS server
plus real PostgreSQL persistence, both via Testcontainers
(`conductoross/conductor:next`, `CONFIG_PROP=config-postgres.properties`,
health-checked on `/health`), never a mocked Conductor client. This
matches the no-mocking-on-concurrency-critical-paths standard the
`EventStoreStorageContractTest` suite above and
`opendcb-scheduling-core`'s own claim/lease tests were already held to —
see `ConductorSagaBridgeIntegrationTest` (the `CountDownLatch`-gated
two-thread race for `startSagaIfNotAlreadyRunning`, and the end-to-end
`signalSaga` → `WAIT` task → `COMPLETED` flow) and
`ConductorCommandTaskWorkerTest` (a real `TaskRunnerConfigurer` polling a
real server and dispatching a claimed task through to a command gateway
test double — the gateway itself is stubbed here, per "Framework adapter
tests" above, since the goal is isolating the Conductor-to-command
translation layer, not exercising a real `CommandBus`).

## What CI must block on

`.github/workflows/ci.yml` currently runs two jobs: `boundary-check`
(three grep-based checks, below) and `build` (`mvn -B clean install` for
the full reactor, gated on `boundary-check` passing — this is what
actually enforces "every provider's contract suite is green" and "every
module's own tests pass," since there is no separate per-provider CI
check beyond the reactor build itself).

- Any `eventstore-core`, `eventstore-<provider>`, or `opendcb-scheduling-core`
  module importing `org.axonframework` — see @docs/CONVENTIONS.md. Enforced
  today by the "No org.axonframework inside eventstore-core,
  eventstore-<provider>, or opendcb-scheduling-core" step. `opendcb-conductor-bridge`
  is deliberately excluded from this step — its `org.axonframework`
  dependency (`CommandGateway` only) is the one documented exception, see
  @docs/ARCHITECTURE.md's "opendcb-conductor-bridge" section — but it is
  still covered by the two checks below, since neither exception applies
  to `io.axoniq.framework` or `org.springframework`.
- Any module importing `io.axoniq.framework`. Enforced today by the "No
  io.axoniq.framework anywhere in the repo" step (repo-wide — covers every
  module, including `opendcb-scheduling-core` and `opendcb-conductor-bridge`,
  by construction, not because either is named explicitly).
- Any module outside `eventstore-*`, `integrations/*`, `bootstrap-*`,
  `outbox-relay-core`, `opendcb-scheduling-core`, or `opendcb-conductor-bridge`
  importing `org.springframework` — see @docs/CONVENTIONS.md. Enforced
  today by the "No org.springframework outside the Spring-specific
  modules" step, which now also covers `opendcb-scheduling-core` and
  `opendcb-conductor-bridge` (added alongside the `org.axonframework` fix
  above — neither module should ever depend on Spring).

Both the `org.axonframework` and `org.springframework` checks match only
actual `import` statements (`^import org\.axonframework\.` /
`^import org\.springframework\.`), not any textual mention of those
package names — a plain substring grep originally produced a false
positive against `opendcb-scheduling-core`'s own Javadoc (which correctly
states, in prose, that the class "has no dependency on `org.axonframework`"),
which would have failed CI on a fully compliant module.
