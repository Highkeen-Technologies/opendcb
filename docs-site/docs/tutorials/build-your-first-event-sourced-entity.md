# Build Your First Event-Sourced Entity

This tutorial walks through `examples/plain-java-sample`'s real
`AccountEntity` step by step: an account that can be opened and receive
deposits. Everything here is Axon Framework's own entity model — OpenDCB
just supplies the storage underneath.

## Step 1: the entity holds state only

`AccountEntity` has no command-handling logic in it at all. It just knows
how to construct itself from its creation event, and how to evolve from
later events:

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

    public BigDecimal balance() {
        return balance;
    }
}
```

`@EntityCreator` marks the constructor Axon calls when the entity's first
event arrives. `@EventSourcingHandler` marks a method that updates state in
response to a later event. This is exactly what "event-sourced" means: the
entity's current state is whatever you get from replaying its events in
order.

## Step 2: the events themselves

Events are plain records, tagged with the entity ID they belong to:

```java
@Event
public record AccountOpened(
        @EventTag(key = "AccountEntity") String accountId,
        String ownerName) {
}

@Event
public record MoneyDeposited(
        @EventTag(key = "AccountEntity") String accountId,
        BigDecimal amount) {
}
```

## Step 3: commands, and the Stateful Command Handler split

Command handling lives in a *separate* class,
`AccountCommandHandlers` — not on the entity itself. This is deliberate:
opening an account is a **creational** command (nothing exists yet to
load), while depositing money is an **instance** command (it needs the
account's current state to validate against).

```java
@Command
public record OpenAccount(@TargetEntityId String accountId, String ownerName) {
}

@Command
public record DepositMoney(@TargetEntityId String accountId, BigDecimal amount) {
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

The creational handler for `OpenAccount` takes no entity — there's nothing
to load yet. The instance handler for `DepositMoney` uses `@InjectEntity`
to get the current `AccountEntity`, so it can validate against real state
(here, just checking the amount is positive) before appending.

## Step 4: wire it up

This is the same wiring [Plain Java Setup](../setup/plain-java-setup.md)
walks through — a `DataSource`, an `EventStorageEngine` from
`OpenDcbAxonPostgres.engine(dataSource)`, and an `EventSourcingConfigurer`:

```java
var accountEntity = EventSourcedEntityModule.autodetected(String.class, AccountEntity.class);
var commandHandlingModule = CommandHandlingModule.named("Account")
        .commandHandlers()
        .autodetectedCommandHandlingComponent(c -> new AccountCommandHandlers());

AxonConfiguration configuration = EventSourcingConfigurer.create()
        .registerEntity(accountEntity)
        .registerCommandHandlingModule(commandHandlingModule)
        .registerEventStorageEngine(c -> engine)
        .start();
```

## Step 5: dispatch commands

```java
CommandGateway commandGateway = configuration.getComponent(CommandGateway.class);

commandGateway.sendAndWait(new OpenAccount("acc-1", "Ada Lovelace"));
commandGateway.sendAndWait(new DepositMoney("acc-1", new BigDecimal("100.00")));
commandGateway.sendAndWait(new DepositMoney("acc-1", new BigDecimal("50.00")));
```

After this, `acc-1`'s balance is `150.00` — sourced entirely from its two
`MoneyDeposited` events plus its `AccountOpened` event, whether you load it
now or after a full application restart.

## Run the real example

```bash
mvn -pl examples/plain-java-sample -am clean install
mvn -pl examples/plain-java-sample exec:java
```

See [examples/plain-java-sample](https://github.com/Highkeen-Technologies/opendcb/tree/main/examples/plain-java-sample)
for the full, runnable version — it also proves durability by re-sourcing
the account from a second, independent configurer against the same
database.
