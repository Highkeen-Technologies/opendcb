# Plain Java Setup

If you're not using Spring Boot — a plain Java application, or a framework
like Quarkus or Micronaut — you can still get a working OpenDCB setup with
one line of code. This page walks through exactly what
`examples/plain-java-sample` in the repository does.

## Build a DataSource

OpenDCB needs a JDBC `DataSource` pointing at your PostgreSQL database. Any
`DataSource` implementation works; the sample uses the plain PostgreSQL
driver's own:

```java
import org.postgresql.ds.PGSimpleDataSource;
import javax.sql.DataSource;

PGSimpleDataSource dataSource = new PGSimpleDataSource();
dataSource.setUrl("jdbc:postgresql://localhost:5432/opendcb_sample");
dataSource.setUser("postgres");
dataSource.setPassword("postgres");
```

## Build the event storage engine

One call, from `bootstrap-axon-postgres`, does the rest:

```java
import com.highkeen.opendcb.bootstrap.axon.postgres.OpenDcbAxonPostgres;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;

EventStorageEngine engine = OpenDcbAxonPostgres.engine(dataSource);
```

By default this also creates the database schema (the `events` and
`event_tags` tables) if it doesn't exist yet. If you're running against a
database whose schema is already set up — say, a second application
instance, or a production database managed by a migration tool — pass
`false` to skip that:

```java
EventStorageEngine engine = OpenDcbAxonPostgres.engine(dataSource, false);
```

## Wire it into Axon

`EventStorageEngine` is Axon Framework's own type for reading and writing
events. Hand it to an `EventSourcingConfigurer` the same way you would with
any other Axon setup:

```java
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.common.configuration.AxonConfiguration;

var stateEntity = EventSourcedEntityModule.autodetected(String.class, AccountEntity.class);
var commandHandlingModule = CommandHandlingModule.named("Account")
        .commandHandlers()
        .autodetectedCommandHandlingComponent(c -> new AccountCommandHandlers());

AxonConfiguration configuration = EventSourcingConfigurer.create()
        .registerEntity(stateEntity)
        .registerCommandHandlingModule(commandHandlingModule)
        .registerEventStorageEngine(c -> engine)
        .start();
```

From here, `configuration.getComponent(CommandGateway.class)` gives you a
working `CommandGateway` to dispatch commands against, exactly like any
other Axon Framework 5 application.

## Compared to Spring Boot

This is exactly what
[the Spring Boot starter](spring-boot-setup.md) does automatically —
`OpenDcbAxonPostgres.engine(dataSource)` is the same call
`OpenDcbPostgresAutoConfiguration` makes internally. The only difference is
who calls it: Spring Boot calls it for you and publishes the result as a
bean; here, you call it yourself.

## Run the real example

```bash
mvn -pl examples/plain-java-sample -am clean install
mvn -pl examples/plain-java-sample exec:java
```

Override the database connection with `OPENDCB_SAMPLE_JDBC_URL`,
`OPENDCB_SAMPLE_DB_USER`, and `OPENDCB_SAMPLE_DB_PASSWORD` environment
variables if your PostgreSQL instance isn't the default local one.
