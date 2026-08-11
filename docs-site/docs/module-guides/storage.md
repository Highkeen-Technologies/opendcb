# Storage (eventstore-core and eventstore-postgres)

## What's an event, in OpenDCB?

Every fact your application records — "an order was placed," "money was
deposited" — is stored as a `StoredEvent`: a plain data object with no
framework attached, holding a position in the log, a unique ID, a type
name, the payload (as JSON text), metadata, a timestamp, and a set of
**tags**.

Tags are what make an event queryable without a fixed schema. A tag is just
a key-value pair — `orderId=O-123`, for example — describing what the event
relates to. An event can carry several tags, since a real-world fact often
relates to more than one thing (an order, a customer, a warehouse).

## The EventStoreStorage port

`EventStoreStorage` is the interface every database backing OpenDCB must
implement: append events, read a range of events, and three methods for
finding positions (`minPosition`, `maxPosition`, `positionAtOrAfter`).
Nothing in this interface mentions Axon or any other framework — it's
plain Java. This is also why adding a new database doesn't require
touching anything above the storage layer: as long as it implements this
same interface, everything else in OpenDCB works with it unchanged.

## A real example

```java
StoredEvent orderPlaced = new StoredEvent(
        0L,                                        // ignored on append — assigned by storage
        "evt-1",                                    // unique event ID
        "OrderPlaced",                               // event type
        "com.example.OrderPlaced",                   // payload's Java class name
        "{\"orderId\":\"O-1\"}",                     // payload, as JSON
        Map.of("traceId", "t-1"),                    // metadata
        Set.of(new StoredEvent.StoredTag("orderId", "O-1")),
        Instant.now());

long lastPosition = storage.appendAtomically(
        List.of(orderPlaced),
        storage.maxPosition(),
        event -> false);   // "true" here would mean: reject this append, a conflicting event exists

List<StoredEvent> events = storage.readRange(0L, null, 100);
```

The third argument to `appendAtomically` is a plain
`java.util.function.Predicate<StoredEvent>` — a rule for "does this event
conflict with what I'm about to write?" that OpenDCB checks before
committing.

## PostgresEventStoreStorage: the real, tested implementation

`eventstore-postgres` stores events in two tables — `events` (one row per
event) and `event_tags` (one row per tag). See
[Database Setup](../setup/database-setup.md) for the exact columns.

### What happens when two application instances write at the same time?

Imagine two people at two different bank branches trying to update the same
customer's balance at the exact same moment — without some way of taking
turns, one update could silently overwrite the other. `PostgresEventStoreStorage`
avoids this with a **transaction-scoped advisory lock**
(`pg_advisory_xact_lock`), managed by PostgreSQL itself and released
automatically when the transaction ends. Every append takes this lock
before checking for conflicts and writing. If two application instances —
or two JVMs entirely — try to append at once, one waits for the other, so
the conflict check always sees a complete, up-to-date log, never a
half-written one.

## What's next

To see how this storage layer connects to Axon Framework, read the
[Axon Integration](axon-integration.md) guide.
