# Plain Java Quickstart

No Spring, no framework auto-configuration — just plain `main()` code. This
walks through `examples/plain-java-sample`, the repo's own proof that
OpenDCB works standalone: one factory call gets you a working Axon
`EventStorageEngine`, and everything downstream is ordinary Axon Framework 5
code you could just as easily adapt into a Quarkus or Micronaut app.

## 1. Start PostgreSQL

```bash
docker run --name opendcb-sample-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=opendcb_sample \
  -p 5432:5432 \
  -d postgres:16
```

## 2. Add the dependency

```xml
<dependency>
    <groupId>com.highkeen.opendcb</groupId>
    <artifactId>bootstrap-axon-postgres</artifactId>
    <version>1.0.0-axon5.1</version>
</dependency>
```

`bootstrap-axon-postgres` is the one module allowed to depend on both
`integrations/eventstore-axon` and `eventstore-postgres` directly — its only
job is gluing exactly those two together behind a single factory call, so
you don't have to hand-assemble `PostgresEventStoreStorage` +
`AbstractDcbEventStorageEngine` + a `Converter` yourself.

## 3. Get a working `EventStorageEngine` in one call

```java
DataSource dataSource = new PGSimpleDataSource();
dataSource.setUrl("jdbc:postgresql://localhost:5432/opendcb_sample");
dataSource.setUser("postgres");
dataSource.setPassword("postgres");

EventStorageEngine engine = OpenDcbAxonPostgres.engine(dataSource);
```

That's it — schema creation included. `engine` is a real Axon
`EventStorageEngine`, ready to hand to an `EventSourcingConfigurer`.

## 4. Define the entity, commands, and events

Same domain model as the Spring Boot Quickstart — an `AccountEntity` that
holds state only, plus a separate `AccountCommandHandlers` class for the
"Stateful Command Handler" pattern (`@InjectEntity` rather than command
handlers on the entity itself):

```java
@EventSourcedEntity
public class AccountEntity {

    private final String accountId;
    private final String ownerName;
    private BigDecimal balance;

    @EntityCreator
    public AccountEntity(AccountOpened event) {
        this.accountId = event.accountId();
        this.ownerName = event.ownerName();
        this.balance = BigDecimal.ZERO;
    }

    @EventSourcingHandler
    void evolve(MoneyDeposited event) {
        this.balance = this.balance.add(event.amount());
    }

    public String accountId() { return accountId; }
    public String ownerName() { return ownerName; }
    public BigDecimal balance() { return balance; }
}
```

```java
class AccountCommandHandlers {

    @CommandHandler
    void handle(OpenAccount command, EventAppender eventAppender) {
        eventAppender.append(new AccountOpened(command.accountId(), command.ownerName()));
    }

    @CommandHandler
    void handle(DepositMoney command, @InjectEntity AccountEntity account, EventAppender eventAppender) {
        if (command.amount().signum() <= 0) {
            throw new IllegalArgumentException("Deposit amount must be positive, got " + command.amount());
        }
        eventAppender.append(new MoneyDeposited(command.accountId(), command.amount()));
    }
}
```

## 5. Wire the configurer, dispatch commands, read the state back

```java
EventSourcingConfigurer configurer = EventSourcingConfigurer.create()
        .registerEventStorageEngine(c -> engine)
        .registerEntity(EventSourcedEntityModule.autodetected(String.class, AccountEntity.class))
        .registerCommandHandlingModule(
                CommandHandlingModule.named("Account")
                        .commandHandlers(c -> new AccountCommandHandlers()));

AxonConfiguration configuration = configurer.start();
CommandGateway commandGateway = configuration.getComponent(CommandGateway.class);

String accountId = UUID.randomUUID().toString();
commandGateway.sendAndWait(new OpenAccount(accountId, "Ada Lovelace"));
commandGateway.sendAndWait(new DepositMoney(accountId, new BigDecimal("100.00")));

UnitOfWorkFactory unitOfWorkFactory = configuration.getComponent(UnitOfWorkFactory.class);
AccountEntity account = unitOfWorkFactory.create()
        .executeWithResult(ctx -> ctx.component(StateManager.class)
                .repository(AccountEntity.class, String.class)
                .load(accountId, ctx)
                .thenApply(ManagedEntity::entity))
        .join();

System.out.println("balance=" + account.balance()); // balance=100.00
```

The full example goes one step further: it shuts this configurer down,
builds a **second, completely independent** `EventSourcingConfigurer`
against the same database (`OpenDcbAxonPostgres.engine(dataSource, false)` —
`autoCreateSchema=false`, since the schema already exists), and re-sources
the same account from scratch. Both states print identically, which is the
actual proof the balance came from PostgreSQL and not from anything held in
memory.

## Run the real thing

The code above is copied from `examples/plain-java-sample` — run it
directly instead of retyping it:

```bash
mvn -pl examples/plain-java-sample -am clean install
mvn -pl examples/plain-java-sample exec:java
```

(`-am` builds `bootstrap-axon-postgres` and its own dependencies first,
since the example depends on them via the reactor.)

Connection settings default to
`jdbc:postgresql://localhost:5432/opendcb_sample` / `postgres` / `postgres`,
matching the Docker command above — override with the
`OPENDCB_SAMPLE_JDBC_URL`, `OPENDCB_SAMPLE_DB_USER`, and
`OPENDCB_SAMPLE_DB_PASSWORD` environment variables if your instance differs.

You'll see output ending in two matching account states — one from the
configurer that dispatched the commands, one from a brand-new configurer
that re-sourced the account from PostgreSQL alone.

## Next steps

- [What is DCB](../core-concepts/what-is-dcb.md) to understand the conflict
  detection underneath `appendAtomically`.
- [examples/microservices-sample](https://github.com/Highkeen-Technologies/opendcb/tree/main/examples/microservices-sample)
  for the next step up: two services, each with their own event store,
  connected by `outbox-relay-rabbitmq`.
