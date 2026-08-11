# Schedule a Future Event

A short, focused walkthrough: schedule an event, run the dispatcher, and
confirm it actually fired — using `opendcb-scheduling-core`'s real API.

## Step 1: set up the store and your event store

```java
ScheduledEventStore store = new ScheduledEventStore(dataSource);
store.ensureSchema();

EventStoreStorage storage = new PostgresEventStoreStorage(dataSource);
```

`ScheduledEventStore` owns its own `scheduled_event` table, completely
separate from the event log's own tables.

## Step 2: schedule an event

```java
StoredEvent reminderEvent = new StoredEvent(
        0L,                                          // ignored — assigned at fire time
        UUID.randomUUID().toString(),
        "TrialExpiryReminder",
        "com.example.TrialExpiryReminder",
        "{\"accountId\":\"acc-1\"}",
        Map.of(),
        Set.of(new StoredEvent.StoredTag("accountId", "acc-1")),
        Instant.now());                               // ignored — assigned at fire time

UUID scheduleId = store.schedule(
        Instant.now().plusSeconds(5),                 // fire in 5 seconds, for this test
        reminderEvent,
        "billing");                                    // scopeName
```

This inserts a `PENDING` row. Nothing fires yet.

## Step 3: run the dispatcher

```java
ScheduledEventDispatcher dispatcher =
        new ScheduledEventDispatcher(store, storage, "worker-1");

Thread.sleep(6000);   // wait past the 5-second scheduled time
dispatcher.runOnce();
```

`runOnce()` claims any due row (using `claimDue` under the hood, with a
lease so another instance could safely take over if this one crashed
mid-fire), builds a `StoredEvent` from it, appends it via
`EventStoreStorage.appendAtomically`, and marks the row `COMPLETED`.

For continuous polling instead of a single call, use:

```java
dispatcher.start(Duration.ofSeconds(5));   // polls every 5 seconds
// ... later ...
dispatcher.stop();
```

## Step 4: confirm it fired

Read the event back from the real event store — this is the actual proof,
not just a completed status on the schedule row:

```java
List<StoredEvent> events = storage.readRange(0L, null, 100);
boolean fired = events.stream().anyMatch(e -> "TrialExpiryReminder".equals(e.messageType()));
```

## Cancelling before it fires

```java
UUID anotherScheduleId = store.schedule(Instant.now().plusSeconds(60), reminderEvent, "billing");
store.cancel(anotherScheduleId);
```

Cancelling only works while the row is still `PENDING` — this is a safe
no-op if a `claimDue` call already grabbed it, since that's a normal race,
not an error condition.

## What's next

See [Scheduling Events](../module-guides/scheduling-events.md) for the
lease/reclaim mechanism, retry-cap/dead-lettering, and the optional
conflict-predicate safety net this tutorial didn't cover.
