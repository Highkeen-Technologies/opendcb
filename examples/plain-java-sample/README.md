# plain-java-sample

A standalone Java application with **zero Spring dependency anywhere** in its
classpath — proves OpenDCB works outside Spring Boot entirely. Confirm this
yourself with:

```
grep -r "org.springframework" examples/plain-java-sample/
```

It should return nothing.

## What it does

A minimal `AccountEntity` (holds state only) plus a separate
`AccountCommandHandlers` class (the Stateful Command Handler pattern —
`@InjectEntity` rather than command handlers on the entity itself, per
docs/CONVENTIONS.md) handling one creational command (`OpenAccount`) and one
instance command (`DepositMoney`).

`main()` wires everything by hand — no framework auto-configuration:

1. Builds a plain `org.postgresql.ds.PGSimpleDataSource`.
2. Gets a working `EventStorageEngine` in **one call**:
   `OpenDcbAxonPostgres.engine(dataSource)` — from `bootstrap-axon-postgres`.
   No hand-assembling `PostgresEventStoreStorage` +
   `AbstractDcbEventStorageEngine` + an `ObjectMapper`.
3. Builds an `EventSourcingConfigurer`, dispatches an `OpenAccount` and two
   `DepositMoney` commands via `CommandGateway`, and prints the resulting
   state.
4. Shuts that configurer down, builds a **second, completely independent**
   `EventSourcingConfigurer` against the same database
   (`autoCreateSchema=false` — the schema already exists), and re-sources the
   same account. Both the write-side and re-sourced-read-side state are
   printed so it's visibly obvious they match — proof state is genuinely
   durable in Postgres, not just held in the first configurer's memory.

## Contrast with the Spring Boot starter

`opendcb-axon-spring-boot-starter` gets you the same `EventStorageEngine`
via Spring Boot auto-configuration — no `main()` wiring code at all, just
`opendcb.eventstore.datasource.url` in `application.properties` and an
`EventStorageEngine` bean appears. This example is the other end of that
trade: no Spring, no auto-configuration, but also no framework magic — every
wiring step above is plain, traceable Java you could adapt into a
Quarkus/Micronaut app just as easily.

## Running it

Requires a local PostgreSQL instance. Start one with Docker:

```
docker run --name opendcb-sample-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=opendcb_sample \
  -p 5432:5432 \
  -d postgres:16
```

Then, from the repo root:

```
mvn -pl examples/plain-java-sample -am clean install
mvn -pl examples/plain-java-sample exec:java
```

(`-am` builds `bootstrap-axon-postgres` and its own dependencies first, since
this example depends on them via the reactor.)

Connection settings default to `jdbc:postgresql://localhost:5432/opendcb_sample`
/ `postgres` / `postgres`, matching the Docker command above. Override with
the `OPENDCB_SAMPLE_JDBC_URL`, `OPENDCB_SAMPLE_DB_USER`, and
`OPENDCB_SAMPLE_DB_PASSWORD` environment variables if your instance differs.
