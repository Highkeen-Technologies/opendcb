# Examples

Two runnable examples live under `examples/` in the repo. Both are excluded
from Maven Central publishing deliberately — they're meant to be read and
run, not depended on.

## plain-java-sample

Wires OpenDCB into a standalone `main()` with **zero Spring dependency
anywhere in its classpath** — `OpenDcbAxonPostgres.engine(dataSource)` gets
you a working Axon `EventStorageEngine` in one call, then it's plain Axon
Framework 5 code from there. Dispatches an `OpenAccount` and two
`DepositMoney` commands against a real PostgreSQL-backed store, then builds
a **second, independent** `EventSourcingConfigurer` against the same
database and re-sources the account — proving the state is durable in
Postgres, not just held in memory.

**Takes**: a couple of minutes (start a Postgres container, `mvn install`,
`exec:java`).

- [Repo directory](https://github.com/Highkeen-Technologies/opendcb/tree/main/examples/plain-java-sample)
- [Tutorial: Build Your First Event-Sourced Entity](tutorials/build-your-first-event-sourced-entity.md)

## microservices-sample

Two independent services — `orders-service` and `shipping-service` — each
with their own `eventstore-postgres` database, demonstrating the
event-driven microservices deployment shape. Placing an order appends both
a rich internal `OrderPlaced` event and a small public
`OrderPlacedIntegrationEvent` atomically; `outbox-relay-rabbitmq` relays
**only** the integration event across the boundary, and
`shipping-service`'s translator dispatches a local `CreateShipmentCommand`
idempotently — including under simulated at-least-once broker redelivery.

**Takes**: ~30-60s for the end-to-end test (two real Postgres containers
plus one RabbitMQ container via Testcontainers).

- [Repo directory](https://github.com/Highkeen-Technologies/opendcb/tree/main/examples/microservices-sample)
- [Tutorial: Build a Two-Service System](tutorials/build-a-two-service-system.md)
