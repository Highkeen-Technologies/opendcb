# Scheduling Events

## The problem this solves

Sometimes you need to fire an event in the future, not right now — "mark
this invoice overdue in 30 days," "send a reminder in a week." That's easy
if your application never restarts. `opendcb-scheduling-core` makes it
reliable even when it does: the schedule survives an application restart,
and if the instance responsible for firing an event crashes mid-work,
another instance picks it up automatically.

## Why it schedules events, not commands

An earlier design scheduled *commands* — dispatch `MarkInvoiceOverdue` at
the future time, and let a command handler re-validate business rules right
before acting. That's not wrong, but it diverges from what Axon Framework
itself does: both Axon 4's `EventScheduler` and Axon 5's
`DcbEventChannel.scheduleEvent(Instant, Event)` schedule a raw *event*, not
a command. `opendcb-scheduling-core` follows that same precedent — see
[docs/ARCHITECTURE.md](https://github.com/Highkeen-Technologies/opendcb/blob/main/docs/ARCHITECTURE.md#opendcb-scheduling-core-schedules-events-not-commands--matching-axons-own-design-and-framework-agnostic-as-a-result)
for the full reasoning. The practical upshot: firing a scheduled event is
just appending it via `EventStoreStorage.appendAtomically` — so this module
depends only on `eventstore-core`, nothing Axon-specific at all.

## The lease/reclaim mechanism, in plain language

Picture a library book with a due date. You check it out (claim it), and
you have until the due date to return it (complete it). If you never bring
it back, the library doesn't wait forever — once the due date passes, the
book becomes available for someone else to check out.

That's exactly how claiming a scheduled event works. `ScheduledEventStore.claimDue`
lets one instance "check out" a batch of due rows for a limited lease. If
that instance crashes before finishing, another instance's later
`claimDue` call sees the lease has expired and reclaims the row — safely,
even if two instances call `claimDue` at the exact same moment, since the
claim uses `SELECT ... FOR UPDATE SKIP LOCKED` under the hood: a row locked
by one instance simply isn't a candidate for another.

## Scheduling an event

```java
ScheduledEventStore store = new ScheduledEventStore(dataSource);
store.ensureSchema();

UUID scheduleId = store.schedule(
        Instant.now().plus(Duration.ofDays(30)),   // scheduledTime
        invoiceOverdueEvent,                        // a StoredEvent
        "invoicing");                                // scopeName
```

`ScheduledEventDispatcher` is the poller: it claims due rows and appends
each one via a real `EventStoreStorage`, marking it complete on success.

```java
ScheduledEventDispatcher dispatcher =
        new ScheduledEventDispatcher(store, storage, "worker-1");
dispatcher.start(Duration.ofSeconds(5));   // polls every 5 seconds
```

You can cancel a schedule any time before it fires:

```java
store.cancel(scheduleId);
```

Cancelling is a safe no-op if the row has already been claimed — that's a
normal race against a concurrent `claimDue`, not an error.

## What happens if firing keeps failing: retry cap and dead-lettering

Every scheduled row has a `maxAttempts` (5 by default). Each time
`claimDue` claims a row, its attempt count increases; once claiming it
again would exceed `maxAttempts`, the row is marked `DEAD_LETTERED` instead
of claimed again, and a `DeadLetterSink` is notified (a `LoggingDeadLetterSink`
by default, logging at `ERROR`). This exists so a permanently broken
schedule doesn't retry forever with no visibility.

## Optional: skip firing if a conflicting event already happened

Since `EventStoreStorage.appendAtomically` already takes a conflict
predicate, `schedule(...)` has an overload accepting an optional
`ConflictCriteria` — a set of required tags and message types. If the
dispatcher finds a matching event already in the log at fire time, it skips
the append and marks the row `SKIPPED_CONFLICT` instead, notifying a
`ConflictSkipSink` (logged at `INFO`, since this is a deliberate outcome,
not a failure). Example: don't fire `InvoicePaymentDeadlineExpired` if an
`InvoicePaid` event for the same invoice already exists. This is optional —
a plain reminder with nothing to conflict against doesn't need it, and
omitting `ConflictCriteria` (passing `null`, the default for the simpler
overloads) fires the event unconditionally, same as before this feature
existed.

## What's next

Walk through this end to end in
[Schedule a Future Event](../tutorials/schedule-a-future-event.md).
