# Spring Boot Quickstart

This walks through wiring OpenDCB into a Spring Boot application, dispatching
two commands against a real PostgreSQL-backed event store, and reading the
resulting state back — proving events were genuinely appended and re-sourced,
not just held in memory.

## What `opendcb-axon-spring-boot-starter` actually gives you

Add the starter and point it at a database:

```xml
<dependency>
    <groupId>com.highkeen.opendcb</groupId>
    <artifactId>opendcb-axon-spring-boot-starter</artifactId>
    <version>1.0.0-axon5.1</version>
</dependency>
```

```properties
opendcb.eventstore.datasource.url=jdbc:postgresql://localhost:5432/opendcb
opendcb.eventstore.datasource.username=postgres
opendcb.eventstore.datasource.password=postgres
```

That's enough for Spring to auto-configure a working Axon
`EventStorageEngine` bean, backed by `eventstore-postgres`, with the schema
created automatically. **Worth being precise about scope here**: the starter
stops at that one bean (plus the `DataSource` it needs internally). It does
not auto-wire Axon's `EventSourcingConfigurer`, `CommandGateway`, or entity
registration — those are Axon Framework 5's own concepts, and the starter
deliberately doesn't guess how you want to assemble them. You wire them the
same way `examples/plain-java-sample` does, just injecting the
Spring-provided `EventStorageEngine` bean instead of building one by hand.

## 1. Start PostgreSQL

```bash
docker run --name opendcb-quickstart-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=opendcb \
  -p 5432:5432 \
  -d postgres:16
```

## 2. Define an entity and its commands/events

This is the same shape used throughout the repo (see
`examples/plain-java-sample`): the entity holds state and evolves via
`@EventSourcingHandler`, while a separate class holds the `@CommandHandler`
methods (the "Stateful Command Handler" pattern).

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
@Command
public record OpenAccount(@TargetEntityId String accountId, String ownerName) {}

@Command
public record DepositMoney(@TargetEntityId String accountId, BigDecimal amount) {}

@Event
public record AccountOpened(@EventTag(key = "AccountEntity") String accountId, String ownerName) {}

@Event
public record MoneyDeposited(@EventTag(key = "AccountEntity") String accountId, BigDecimal amount) {}
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

## 3. Wire Axon's `EventSourcingConfigurer` against the auto-configured bean

This is a plain `@Configuration` class. The only OpenDCB-specific piece is
the injected `EventStorageEngine` bean — everything else is standard Axon
Framework 5 configuration, the same calls `examples/plain-java-sample` makes
by hand:

```java
@Configuration
class AxonConfig {

    @Bean
    AxonConfiguration axonConfiguration(EventStorageEngine engine) {
        return EventSourcingConfigurer.create()
                .registerEventStorageEngine(c -> engine)
                .registerEntity(EventSourcedEntityModule.autodetected(String.class, AccountEntity.class))
                .registerCommandHandlingModule(
                        CommandHandlingModule.named("Account")
                                .commandHandlers(c -> new AccountCommandHandlers()))
                .start();
    }

    @Bean
    CommandGateway commandGateway(AxonConfiguration axonConfiguration) {
        return axonConfiguration.getComponent(CommandGateway.class);
    }
}
```

## 4. Dispatch a command and read the state back

A `CommandLineRunner` is enough to prove the round trip end to end on
startup:

```java
@Component
class DemoRunner implements CommandLineRunner {

    private final CommandGateway commandGateway;
    private final AxonConfiguration axonConfiguration;

    DemoRunner(CommandGateway commandGateway, AxonConfiguration axonConfiguration) {
        this.commandGateway = commandGateway;
        this.axonConfiguration = axonConfiguration;
    }

    @Override
    public void run(String... args) {
        String accountId = UUID.randomUUID().toString();

        commandGateway.sendAndWait(new OpenAccount(accountId, "Ada Lovelace"));
        commandGateway.sendAndWait(new DepositMoney(accountId, new BigDecimal("100.00")));

        UnitOfWorkFactory unitOfWorkFactory = axonConfiguration.getComponent(UnitOfWorkFactory.class);
        AccountEntity account = unitOfWorkFactory.create()
                .executeWithResult(ctx -> ctx.component(StateManager.class)
                        .repository(AccountEntity.class, String.class)
                        .load(accountId, ctx)
                        .thenApply(ManagedEntity::entity))
                .join();

        System.out.println("Re-sourced account: " + account.ownerName()
                + ", balance=" + account.balance());
    }
}
```

Run the application. On startup you should see:

```
Re-sourced account: Ada Lovelace, balance=100.00
```

That balance was never held anywhere in memory across the call — it was
computed by loading `AccountOpened` and `MoneyDeposited` back from
PostgreSQL and re-applying them through `AccountEntity`'s
`@EventSourcingHandler`. You've appended events and read them back through
a real event-sourced entity.

## Next steps

- [What is DCB](../core-concepts/what-is-dcb.md) to understand the conflict
  detection `EventStoreStorage.appendAtomically` gives you underneath this.
- [Scale Across Multiple Instances](../tutorials/scale-across-multiple-instances.md)
  once you're ready to run more than one instance of this application.
