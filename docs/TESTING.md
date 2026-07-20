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

Use `AxonTestFixture` (given/when/state, exception assertions) — see
`axon5-sample`'s `AccountEntityTest` for the pattern, including how to wire
an entity and a separate stateful command handler module together for
testing.

## What CI must block on

- Any provider missing a passing contract suite run.
- Any `eventstore-core` or `eventstore-<provider>` module importing a
  framework type (Axon or otherwise) — see @docs/CONVENTIONS.md. This
  should be a CI grep/dependency-analysis check, not just a code review
  reminder.
- Any module importing `io.axoniq.framework`.
