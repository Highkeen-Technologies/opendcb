# Conventions

## License, groupId, and dependencies

- Apache 2.0 license header on every source file, attributed to "the OpenDCB
  contributors".
- Maven `groupId`: `com.highkeen.opendcb` for every module.
- Package root: `com.highkeen.opendcb` — see @docs/ARCHITECTURE.md's table
  for the per-module package.
- `eventstore-core` and every `eventstore-<provider>` module must never
  import any event-sourcing framework type (Axon or otherwise). If a class
  in one of these modules needs to import something from
  `org.axonframework`, that class belongs in `integrations/eventstore-axon`
  instead — this is a hard architectural boundary, not a style preference.
- Never add a dependency under `io.axoniq.framework`, `axon-server-connector`,
  or anything requiring an Axoniq Platform subscription. Grep for
  `io.axoniq.framework` before every commit if unsure.

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
- Prefer `AxonTestFixture` (given/when/then) for anything exercising Axon
  entity command handling, matching the pattern in the axon5-sample
  reference project.
