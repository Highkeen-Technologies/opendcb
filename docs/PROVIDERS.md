# Adding a new storage provider

Use `eventstore-postgres`'s `PostgresEventStoreStorage`
(`com.highkeen.opendcb.eventstore.postgres`) as the reference implementation
— every new provider follows the same shape.

Providers stay framework-agnostic by construction: they implement
`com.highkeen.opendcb.eventstore.core.EventStoreStorage` and never import
anything from `integrations/*` or any framework. If you find yourself
wanting to import an Axon (or other framework) type into a provider, stop —
that logic belongs in the corresponding `integrations/eventstore-<framework>`
module, not here. See @docs/ARCHITECTURE.md.

## Steps

1. **Schema/collection design**: one table/collection for events
   (position, event id, message type, payload class, payload, metadata,
   tags, timestamp), with tags queryable (child table, array field, or
   equivalent for the store).
2. **Position assignment**: must be strictly increasing and gap-tolerant.
   Postgres uses `BIGSERIAL`; a store without native auto-increment needs an
   atomically-incrementing counter (see `eventstore-mongo`'s template for the
   counter-collection pattern).
3. **Cross-JVM append locking**: `appendAtomically` must serialize concurrent
   appends across every JVM connected to the same store, not just threads in
   one process. Identify the store's equivalent of Postgres's
   `pg_advisory_xact_lock` (e.g. MySQL `GET_LOCK`, Mongo multi-document
   transactions) before writing the append logic — this is the step most
   likely to be silently wrong if skipped or approximated.
4. **Conflict check**: within the same locked/transactional scope, read the
   tail of events after the given position and test each with the supplied
   `Predicate<StoredEvent>`. If any matches, roll back and throw
   `ConcurrentAppendConflictException`. Do not weaken this to "check, then
   separately write" outside the lock — that reintroduces the race.
5. **Read path**: `readRange`, `maxPosition`, `minPosition`,
   `positionAtOrAfter` — straightforward range queries. No framework types
   involved, ever.
6. **Contract tests**: implement the shared suite from @docs/TESTING.md
   before considering the provider done.

## What NOT to do

- Do not import or reference any `org.axonframework` type (`AppendCondition`,
  `EventCriteria`, `ConsistencyMarker`, etc.), or any other framework's
  types, inside a provider module. Those stay in the relevant
  `integrations/eventstore-<framework>` module. If a provider seems to need
  one, the port (`EventStoreStorage`) is missing something — raise it as an
  `eventstore-core` change, don't route around it.
- Do not optimize tag filtering into provider-specific queries as a first
  pass. Full-scan-plus-in-memory-predicate (as Postgres does today) is
  correct and simple; query-level tag filtering is a valid later
  optimization once correctness is proven, not a prerequisite.
