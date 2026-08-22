# Enable Snapshotting

This tutorial extends `AccountEntity` from
[Build Your First Event-Sourced Entity](build-your-first-event-sourced-entity.md)
with `@Snapshotting`, dispatches enough commands to cross the threshold,
and confirms a snapshot was actually taken — following the same shape as
`opendcb-snapshot-postgres`'s own end-to-end test.

## Step 1: add `@Snapshotting` to the entity

Nothing about `AccountEntity`'s state or event-sourcing handlers changes —
just the one annotation:

```java
@EventSourcedEntity
@Snapshotting(afterEvents = 2)   // low threshold on purpose, so this tutorial triggers fast
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

`afterEvents` is evaluated per individual sourcing/load operation, not as
a running total across the entity's whole history — so a single command
dispatch has to force a load that applies *more than* the threshold's
number of events for a snapshot to actually be stored. `AccountOpened`
plus two `MoneyDeposited` events (three events applied on one load) is
enough to cross `afterEvents = 2`.

## Step 2: register `PostgresSnapshotStore`

```java
DataSource dataSource = ...;

PostgresSnapshotStore snapshotStore = new PostgresSnapshotStore(dataSource);
snapshotStore.ensureSchema();

var accountEntity = EventSourcedEntityModule.autodetected(String.class, AccountEntity.class);
var commandHandlingModule = CommandHandlingModule.named("Account")
        .commandHandlers()
        .autodetectedCommandHandlingComponent(c -> new AccountCommandHandlers());

AxonConfiguration configuration = EventSourcingConfigurer.create()
        .registerEntity(accountEntity)
        .registerCommandHandlingModule(commandHandlingModule)
        .registerEventStorageEngine(c -> OpenDcbAxonPostgres.engine(dataSource))
        .componentRegistry(cr -> cr.registerComponent(
                SnapshotStore.class, c -> snapshotStore))
        .start();
```

Registering `SnapshotStore` as a component is the entire wiring step —
Axon's own `ConfigurationEnhancer` decorates the registered
`EventStorageEngine` with snapshot-aware sourcing automatically. See
[Snapshotting](../module-guides/snapshotting.md) for why no manual
`SnapshotCapableEventStorageEngine` construction is needed.

## Step 3: dispatch enough commands to cross the threshold

```java
CommandGateway commandGateway = configuration.getComponent(CommandGateway.class);

String accountId = "acc-1";
commandGateway.sendAndWait(new OpenAccount(accountId, "Ada Lovelace"));
commandGateway.sendAndWait(new DepositMoney(accountId, new BigDecimal("100.00")));
commandGateway.sendAndWait(new DepositMoney(accountId, new BigDecimal("50.00")));
```

Each `DepositMoney` dispatch forces a real load of `AccountEntity` (via
`@InjectEntity` in `AccountCommandHandlers`, the same instance-command
pattern from the first tutorial). The second `DepositMoney` load applies
three events — `AccountOpened` plus two `MoneyDeposited` — crossing
`afterEvents = 2` and triggering a snapshot store.

## Step 4: confirm a snapshot was actually taken

Don't just trust that it worked — query the `snapshot` table directly,
the same way `opendcb-snapshot-postgres`'s own end-to-end test does:

```java
try (Connection connection = dataSource.getConnection();
     PreparedStatement statement = connection.prepareStatement(
             "SELECT COUNT(*) FROM snapshot WHERE identifier = ?")) {
    statement.setString(1, accountId);
    try (ResultSet resultSet = statement.executeQuery()) {
        resultSet.next();
        boolean snapshotTaken = resultSet.getInt(1) == 1;
    }
}
```

## Step 5 (optional): prove the snapshot is actually used, not just stored

A stored snapshot row isn't proof it's ever read back. The real test is
whether a *later, independent* load reads fewer events than were actually
appended to the log. Build a second, brand-new `EventSourcingConfigurer`
against the same database (a fresh configurer proves durability, not
in-memory state carried over from the first one) and dispatch one more
`DepositMoney` — that load should source starting from the snapshot,
reading only the events appended after it, not the full history from
`AccountOpened`. This is exactly what
`PostgresSnapshotStoreEndToEndTest.sourcingAfterASnapshotReadsFewerEventsThanWereActuallyAppended`
does, using a test-local `EventStoreStorage` decorator that counts every
event `readRange` actually returns — see that test in
[`opendcb-snapshot-postgres`](https://github.com/Highkeen-Technologies/opendcb/tree/main/opendcb-snapshot-postgres)
for the full, runnable version of this proof.

## What's next

See [Snapshotting](../module-guides/snapshotting.md) for how the
`afterEvents`/`afterSourcingTime` policies combine, what
`PostgresSnapshotStore` actually stores, and the `@Internal` Axon API
caveat worth knowing before pinning a version.
