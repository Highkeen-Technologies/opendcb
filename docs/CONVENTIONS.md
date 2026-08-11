# Conventions

## License, groupId, and dependencies

- Apache 2.0 license header on every source file, attributed to "the OpenDCB
  contributors".
- Maven `groupId`: `com.highkeen.opendcb` for every module.
- Package root: `com.highkeen.opendcb` — see @docs/ARCHITECTURE.md's table
  for the per-module package.
- `eventstore-core`, every `eventstore-<provider>` module, and
  `opendcb-scheduling-core` must never import any event-sourcing framework
  type (Axon or otherwise). If a class in one of these modules needs to
  import something from `org.axonframework`, that class belongs in
  `integrations/eventstore-axon` instead — this is a hard architectural
  boundary, not a style preference. CI enforces this for actual `import`
  statements (not for a class's own Javadoc *describing* the absence of
  such a dependency) — see @docs/TESTING.md's "What CI must block on".
- Never add a dependency under `io.axoniq.framework`, `axon-server-connector`,
  or anything requiring an Axoniq Platform subscription. Grep for
  `io.axoniq.framework` before every commit if unsure.
- `opendcb-conductor-bridge` is the one module allowed to depend on both
  `org.axonframework` (its `CommandGateway` only, for dispatching a saga
  step's command) and a third-party server SDK (Conductor's Java client) —
  a deliberate, documented exception, not a boundary violation; see
  @docs/ARCHITECTURE.md's "opendcb-conductor-bridge" section.

## Dependency mediation

- When adding a dependency on a library that bundles its own transitive
  Jackson (or similarly foundational) pin, check `mvn dependency:tree` for
  a version conflict before trusting the resolved classpath — Maven's
  "nearest wins" mediation falls back to declaration order when two paths
  to the same artifact are at equal depth, which can silently resolve to
  an older transitive version instead of the newer one your own POM
  otherwise implies. `opendcb-conductor-bridge`'s build hit this for real:
  `conductor-common` directly declares `jackson-core:2.13.2` at the same
  depth as the `2.22.1` pulled in transitively via `jackson-databind`, and
  the older one won — silently, with no build warning — leaving a
  `jackson-core` too old to contain `StreamReadConstraints` (added in
  2.15) on the runtime classpath. This surfaced only at runtime, as a
  `NoClassDefFoundError` inside the code path that actually exercised
  Jackson, not at compile time. Fix by adding an explicit direct
  dependency on the version you actually want — a direct (depth-1)
  dependency always wins depth-based mediation regardless of any
  declaration-order tie-break.

## Grounding framework API usage

- Never write code against an assumed framework API signature (Axon or any
  future framework this toolkit supports). Before implementing against any
  `org.axonframework` interface, check the actual source: clone
  `AxonFramework/AxonFramework` from GitHub (shallow, `--depth 1`) and read
  the real interface/class, or check the official Javadoc. Signatures change
  between minor versions — assume nothing from memory or from older Axon 4.x
  knowledge.
- When an implementation makes a deliberate simplification versus what
  "real" framework-integrated code would do (e.g. `integrations/eventstore-axon`
  not applying an upcaster, since Axon Framework 5.1.2 has no released
  upcaster/`IntermediateEventRepresentation` SPI to wire up — see
  @docs/ROADMAP.md's open questions), document it explicitly in the class
  Javadoc, not just in a comment buried in the method body.

## Adding support for a new framework

- Never modify `eventstore-core` or any `eventstore-<provider>` module to
  accommodate a new framework's types. Add
  `integrations/eventstore-<framework>` instead, following the same shape as
  `integrations/eventstore-axon`: one adapter class implementing that
  framework's storage SPI, translating to/from `StoredEvent`/`EventStoreStorage`.
- Read-side routing (segment claiming, token stores, or that framework's
  equivalent) gets its own `routing-<mechanism>-<framework>` module — do not
  try to force a shared abstraction across frameworks for this; see
  @docs/ARCHITECTURE.md's "What does NOT generalize" section for why.

## Error handling

- Storage-layer conflicts throw `EventStoreStorage.ConcurrentAppendConflictException`
  (checked at the port level) — never let a raw SQL/driver exception leak
  through `EventStoreStorage` methods; wrap in `IllegalStateException` with
  the original as cause, following the pattern in `PostgresEventStoreStorage`.
- Relay publishers should distinguish retryable failures (network/broker
  unavailable — retry with backoff) from non-retryable ones (serialization
  failure — dead-letter, don't retry forever).

## Testing

- Every public class needs a unit test.
- Every `EventStoreStorage` provider must pass the shared contract suite —
  see @docs/TESTING.md. Do not merge a provider without it green.
- Prefer `AxonTestFixture` (`org.axonframework.test.fixture`, given/when/then)
  for anything exercising Axon entity command handling. No dedicated
  AxonTestFixture-based entity command-handling test currently exists in
  this repo — `examples/plain-java-sample`'s `AccountEntity`/
  `AccountCommandHandlers` demonstrate the pattern structurally, but
  without a corresponding test. Writing one is a reasonable future
  contribution; until then, follow `AxonTestFixture`'s own documentation
  directly (`org.axonframework.test.fixture`) for the given/when/then
  pattern.
- Modules that wrap a real external server (not just a database) —
  `opendcb-conductor-bridge` is the only current example — must test
  concurrency-critical paths (the correlation race, signal/completion)
  against a real instance of that server via Testcontainers, not a mock.
  Same no-mocking-on-concurrency-critical-paths standard the
  `EventStoreStorage` contract suite and `opendcb-scheduling-core` were
  both already held to — a mocked server can't prove a real race
  condition is actually closed.
