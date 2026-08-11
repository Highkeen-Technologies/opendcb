# Database Setup

OpenDCB's PostgreSQL provider needs a plain PostgreSQL database — no
extensions, no special configuration. This page gets you a local instance
running in under a minute.

## Run PostgreSQL locally with Docker

```bash
docker run --name opendcb-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=opendcb \
  -p 5432:5432 \
  -d postgres:16
```

This starts PostgreSQL 16, listening on the default port 5432, with a
database named `opendcb` ready to use.

## The schema OpenDCB needs

`eventstore-postgres` stores events across two tables:

- **`events`** — one row per event: its position in the log (an
  auto-incrementing number), a unique event ID, the event's type, the
  payload's Java class name, the payload itself (as JSON), metadata (as
  JSON), and when it happened.
- **`event_tags`** — one row per tag on an event (an event can have
  several). Tags are how OpenDCB knows what an event relates to — for
  example, an order ID or a customer ID — without needing a fixed schema
  per event type.

You don't need to create these tables by hand for local development —
see the next section.

## Automatic schema creation

By default, OpenDCB creates the `events` and `event_tags` tables itself the
first time it connects, if they don't already exist. This is controlled by
an `autoCreateSchema` setting:

- With the Spring Boot starter, this is the `opendcb.eventstore.autoCreateSchema`
  property, and it's `true` by default.
- With plain Java (`OpenDcbAxonPostgres.engine(dataSource)`), it's also
  `true` by default; pass `false` as a second argument to disable it.

This is convenient for local development and getting started quickly. In
production, many teams prefer to manage schema changes through a dedicated
migration tool (like Flyway or Liquibase) instead of letting the
application create tables on startup — set `autoCreateSchema` to `false` in
that case, and apply the same `CREATE TABLE` statements
`PostgresEventStoreStorage` uses through your migration tool instead.

## What's next

With a database running, continue to
[Spring Boot Setup](spring-boot-setup.md) or
[Plain Java Setup](plain-java-setup.md), depending on how you're building
your application.
