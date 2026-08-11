# Choosing a Storage Provider

OpenDCB is designed so any database can back the event store — but today,
one implementation actually exists and is tested.

## eventstore-postgres: the real, working option

`eventstore-postgres` is the only storage provider that's implemented and
passes OpenDCB's full test suite (including tests that verify two
application instances writing at the same time can't corrupt each other's
data). If you're getting started with OpenDCB today, this is what you use —
every setup page and example in this site assumes it.

## eventstore-mysql and eventstore-mongo: not implemented yet

You may see `eventstore-mysql` and `eventstore-mongo` mentioned in the
project's roadmap. These are **design templates only** — the table/document
schema and the locking strategy for each database are documented, but no
working code exists yet. Don't use them, and don't expect them to appear as
usable Maven dependencies today.

## What if I need a different database right now?

OpenDCB's storage layer is built around one interface,
`EventStoreStorage`, that any database can implement — that's the whole
point of separating storage from the rest of the toolkit. If PostgreSQL
isn't an option for you, the fastest path today is implementing that
interface yourself against your database of choice.

The internal `docs/PROVIDERS.md` guide in the repository walks through
this step by step: schema design, how to assign a strictly increasing
position to each event, how to lock across multiple application instances
so they don't step on each other, and the shared test suite every provider
must pass before it's considered done. `PostgresEventStoreStorage` (the
class behind `eventstore-postgres`) is the reference implementation to
copy the shape of.

## What's next

Once you've picked PostgreSQL (or built your own provider), head to
[Database Setup](database-setup.md) to get a database running locally.
