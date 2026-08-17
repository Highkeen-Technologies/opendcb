# Prerequisites

## Java 21

The reactor's root `pom.xml` sets `maven.compiler.release` to `21` — every
module compiles against, and requires, Java 21. Confirm your JDK:

```bash
java -version
```

## Maven

Used to build the reactor and run any module directly (`mvn -pl <module>
...`). Any recent Maven 3.9+ works.

## Docker

Needed to run the databases and services OpenDCB itself doesn't provide:

- **PostgreSQL** — every quickstart and tutorial in this site runs against a
  real `postgres:16` container (`eventstore-postgres` is the only storage
  provider implemented today; see [Choosing a Storage
  Provider](../setup/choosing-a-storage-provider.md)).
- **Conductor OSS** — only if you're following the [Sagas with
  Conductor](../module-guides/sagas-with-conductor.md) guide.
  `opendcb-conductor-bridge` is the one module in this toolkit that requires
  running an external server beyond Postgres + your own JVM.

Nothing else in this toolkit needs Docker — a single-instance, no-relay
deployment is just your app plus Postgres.

## What you don't need to install separately

- **Spring Boot users**: nothing beyond the above.
  `opendcb-axon-spring-boot-starter` pulls in the PostgreSQL JDBC driver and
  everything else it needs transitively.
- **Plain Java users**: `eventstore-postgres` declares the PostgreSQL driver
  at `provided` scope, since it's a library, not an application — your own
  project (like `examples/plain-java-sample`) needs to declare the driver
  dependency itself, at compile scope.

---

New to event sourcing? Start with [Event Sourcing in Plain
English](../core-concepts/event-sourcing-in-plain-english.md). Ready to see
code? Jump to the [Spring Boot Quickstart](spring-boot-quickstart.md) or
[Plain Java Quickstart](plain-java-quickstart.md).
