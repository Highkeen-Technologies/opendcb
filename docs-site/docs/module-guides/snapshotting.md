# Snapshotting (opendcb-snapshot-postgres)

## The problem this solves

An event-sourced entity's state is whatever you get from replaying every
event in its history, in order. That's fine for a handful of events — but
for an entity with a long history (thousands of deposits on one bank
account, say), replaying from the very first event on every single load
gets slow. A **snapshot** is a saved checkpoint of the entity's state at
some point in its history, so a later load can resume from that checkpoint
instead of replaying from the beginning.

`opendcb-snapshot-postgres` is a Postgres-backed place to store those
checkpoints.

## This is free, native Axon functionality — no paywall involved

Unlike `opendcb-scheduling-core`, `opendcb-conductor-bridge`, and
`opendcb-data-protection` — which each build OpenDCB's own abstraction
because the equivalent Axon capability is paid or doesn't exist — this
module is different: `SnapshotStore`, `Snapshot`, and
`SnapshotCapableEventStorageEngine` are real interfaces that ship in the
free, released `org.axonframework` library itself. `opendcb-snapshot-postgres`
is a genuine implementation of Axon's own SPI, not a stand-in for a
missing or paid one. No Axoniq Framework dependency, no licensing
consideration, anywhere in this module.

## Step 1: tell Axon when to snapshot

Snapshotting is driven entirely by Axon's own framework code — nothing in
this toolkit decides *when* a snapshot is taken. You opt an entity in with
`@Snapshotting`:

```java
@EventSourcedEntity
@Snapshotting(afterEvents = 100)   // snapshot after every 100 events applied during a load
public class AccountEntity {
    // ... same AccountEntity as in "Build Your First Event-Sourced Entity" ...
}
```

`afterEvents` and `afterSourcingTime` (an ISO-8601 duration, e.g.
`"PT5S"`) can be combined — either threshold independently triggers a
snapshot. At least one has to resolve to a concrete value. From here,
Axon's own framework code takes care of calling `SnapshotStore.store(...)`
whenever the threshold is crossed during a load — your entity and command
handlers need no changes beyond this one annotation.

## Step 2: register `PostgresSnapshotStore`

```java
DataSource dataSource = ...;

PostgresSnapshotStore snapshotStore = new PostgresSnapshotStore(dataSource);
snapshotStore.ensureSchema();   // creates the `snapshot` table if it doesn't exist yet

AxonConfiguration configuration = EventSourcingConfigurer.create()
        .registerEntity(accountEntity)
        .registerCommandHandlingModule(commandHandlingModule)
        .registerEventStorageEngine(c -> engine)
        .componentRegistry(cr -> cr.registerComponent(
                SnapshotStore.class, c -> snapshotStore))
        .start();
```

That's the entire wiring. You do **not** need to construct
`SnapshotCapableEventStorageEngine` yourself, or wrap `engine` in
anything — registering a `SnapshotStore` component is sufficient.
`EventSourcingConfigurer.create()` always registers Axon's own default
`ConfigurationEnhancer` (`EventSourcingConfigurationDefaults`), and that
enhancer decorates whatever `EventStorageEngine` you registered with
`SnapshotCapableEventStorageEngine` automatically, the moment it sees a
`SnapshotStore` component in the same registry. This applies to any
`EventStorageEngine`, including `AbstractDcbEventStorageEngine`
unmodified — see [Axon Integration](axon-integration.md) for how that
engine itself is built.

## What actually happens underneath

- On write: Axon's framework code decides a load crossed the
  `@Snapshotting` threshold and calls `SnapshotStore.store(qualifiedName,
  identifier, snapshot)`. `PostgresSnapshotStore` upserts one row per
  `(qualified_name, identifier)` pair into its own `snapshot` table —
  independent of the event log's own tables, and this module depends on
  neither `eventstore-core` nor `eventstore-postgres` to do it. A second
  snapshot for the same entity replaces the row; snapshots are a
  checkpoint, not a history.
- On read: the auto-decorating `SnapshotCapableEventStorageEngine` calls
  `SnapshotStore.load(...)` first. If a snapshot exists, it's used as the
  entity's starting point and only events *after* the snapshot's position
  are sourced from the log. If none exists yet, sourcing falls back to
  full replay automatically — nothing special to handle on your end.

## Honest caveat: this targets an `@Internal` Axon API

Worth stating plainly, not burying: `SnapshotStore` (the interface this
module implements) and `SnapshotCapableEventStorageEngine` (the decorator
that makes snapshotting actually take effect) are both marked `@Internal`
in Axon Framework's own source. Axon's own Javadoc for `@Internal` states
this marks code that "may introduce breaking changes within minor and
patch releases" — unlike the stable, public `EventStorageEngine` SPI most
of the rest of this toolkit (`integrations/eventstore-axon`) is built
against. Practically: pin your Axon version tightly if you depend on this
module, and treat an Axon version bump as a reason to re-check this
module's behavior, not something to upgrade blindly.

One consequence of this API surface still moving: `SnapshotCapableEventStorageEngine.decorate(...)`,
a convenience static factory, doesn't exist yet at Axon 5.1.2 — it's
`@since 5.3.0`. This turns out not to matter for you as a consumer of
`opendcb-snapshot-postgres`, since the auto-decoration path above never
calls it (or the constructor) directly — registering the `SnapshotStore`
component is all this module ever asks you to do.

## What's next

Walk through this end to end in
[Enable Snapshotting](../tutorials/enable-snapshotting.md).
