# What is DCB (Dynamic Consistency Boundary)

## The problem, without any jargon

Imagine an airline seat-booking system. Two passengers, at the exact same
moment, both try to book seat 14A. Whichever request the database sees
first should win; the second one needs to fail cleanly rather than double-book
the seat.

In a traditional database, you'd reach for a row-level lock or a unique
constraint on `(flight_id, seat_number)` — the "seat" is a row, and the
database's own concurrency control protects it.

In an event-sourced system, there's no "seat row" to lock. There's only a
stream of events — `SeatBooked`, `SeatReleased`, and so on. So the question
becomes: **when you're about to append a new event, how do you check "does
anything already in the log conflict with what I'm about to do" — across
possibly many different entities, not just one row — without a race
condition slipping through?**

That check, done safely under concurrency, is what **Dynamic Consistency
Boundary (DCB)** means. "Dynamic" because the set of events you care about
isn't a fixed table or aggregate boundary — it's whatever you say it is for
this particular append, evaluated fresh every time.

## How OpenDCB implements it

`EventStoreStorage`, the port every storage provider implements, has one
method that matters here:

```java
long appendAtomically(
        List<StoredEvent> events,
        long conflictCheckFromPositionExclusive,
        Predicate<StoredEvent> conflictsIfMatched)
        throws ConcurrentAppendConflictException;
```

Appending isn't just "write these events." It's: *starting from
`conflictCheckFromPositionExclusive`, check every event already in the log
against `conflictsIfMatched`. If any of them match, throw
`ConcurrentAppendConflictException` and write nothing at all. Only if none
match, append `events` and return the new position.*

The check and the write happen in the same atomic operation — that's the
part that closes the race. `eventstore-postgres`, for example, takes a
transaction-scoped `pg_advisory_xact_lock` before reading the tail and
inserting, so two concurrent callers can't both pass the conflict check and
then both write.

## Back to the seat-booking example

To book seat 14A, you'd append a `SeatBooked` event with a predicate that
matches any existing `SeatBooked` event for that same flight and seat
number:

```java
Predicate<StoredEvent> seatAlreadyBooked = event ->
        event.messageType().equals("SeatBooked")
        && event.tags().contains(new StoredEvent.StoredTag("seat", "14A"));

storage.appendAtomically(
        List.of(newSeatBookedEvent),
        lastKnownPosition,
        seatAlreadyBooked);
```

If another booking for the same seat was appended after
`lastKnownPosition`, the predicate matches, the append throws
`ConcurrentAppendConflictException`, and nothing is written. The caller
(the command-handling layer) turns that into "sorry, that seat's taken" —
DCB doesn't decide what to do about a conflict, only that one exists.

The `Predicate<StoredEvent>` is plain `java.util.function.Predicate` — no
framework type. `StoredEvent` itself is a plain DTO (`position`, `eventId`,
`messageType`, `payloadJson`, `metadata`, `tags`, `timestamp`). This is
deliberate: the conflict-detection concept doesn't belong to Axon or any
other framework, so it's expressed entirely in `eventstore-core`'s own
terms, and every storage provider has to honor the same contract (see the
shared `EventStoreStorageContractTest` suite in `docs/TESTING.md`).

## Why "dynamic" matters

Notice the predicate above wasn't scoped to one entity's own event stream —
it matched on a tag (`seat=14A`), which could span events from any entity
that happens to carry that tag. That's the actual power of DCB over
traditional per-aggregate locking: the consistency boundary is whatever
predicate you write for *this* append, not a boundary baked into your schema
ahead of time. Axon Framework 5's own `EventCriteria`/`AppendCondition`
types (translated to/from this predicate in `integrations/eventstore-axon`)
are built around the same idea — OpenDCB's job is making that guarantee real
against a self-hosted database, not just an in-memory or Axon-Server-backed
one.
