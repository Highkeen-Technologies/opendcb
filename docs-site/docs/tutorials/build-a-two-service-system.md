# Build a Two-Service System

This tutorial walks through `examples/microservices-sample`'s real
`orders-service`/`shipping-service` split: placing an order in one service
results in a shipment being created in a completely separate service, with
its own database, connected only by a relay and a translator.

## The shape of the system

- **orders-service** has its own PostgreSQL database and its own event
  store. Placing an order appends two events atomically.
- **shipping-service** has its own, separate PostgreSQL database. It never
  reads orders-service's database directly.
- **RabbitMQ** carries a deliberately narrow "integration event" between
  them — never the rich internal event.

## Step 1: orders-service appends two events per order

`OrderCommandHandlers` handles `PlaceOrder` by appending both the rich
internal `OrderPlaced` and the smaller public `OrderPlacedIntegrationEvent`
in the same atomic append:

```java
@Command
public record PlaceOrder(
        @TargetEntityId String orderId, String customerId, List<String> items, BigDecimal total) {
}

@Event
public record OrderPlaced(
        @EventTag(key = "OrderEntity") String orderId,
        String customerId, List<String> items, BigDecimal total) {
}
```

`OrderPlacedIntegrationEvent` carries only `orderId`, `customerId`, and
`total` — no `items`, no other internal detail. That's the entire public
contract shipping-service ever sees.

## Step 2: only the integration event crosses the boundary

`OrdersService.integrationEventFilter()` is the `Predicate<StoredEvent>`
that `OutboxRelay` uses to decide what to publish:

```java
public Predicate<StoredEvent> integrationEventFilter() {
    return event -> "OrderPlacedIntegrationEvent".equals(event.messageType());
}
```

`OrderPlaced` — the rich internal event — is skipped by the relay (its
position still advances past it) and never reaches RabbitMQ.

## Step 3: shipping-service consumes and translates

`IntegrationEventConsumer` is a plain `amqp-client` consumer bound to
orders-service's exchange/routing key. Every message it receives goes to
`OrderPlacedIntegrationEventTranslator`, which:

1. Checks `processed_integration_events` to guard against redelivery — an
   `INSERT` that relies on a primary-key violation (`23505`) to detect
   "already handled," rather than a separate check-then-act read.
2. Deserializes the envelope and dispatches a **local** command,
   `CreateShipmentCommand` — never the integration event's own shape.

```java
@Command
public record CreateShipmentCommand(@TargetEntityId String orderId, String customerId) {
}
```

## Step 4: shipping-service creates its own event

`ShipmentCommandHandlers` handles `CreateShipmentCommand` by appending
shipping-service's own domain event, `ShipmentCreated` — a type
orders-service has never heard of:

```java
@Event
public record ShipmentCreated(
        @EventTag(key = "ShipmentEntity") String orderId, String customerId) {
}
```

## Step 5: run it

Both services need their own PostgreSQL database and a running RabbitMQ
broker. `ShippingService.start(dataSource)` and the equivalent on
`OrdersService` wire up each service's own `EventStorageEngine`
independently — they never share one.

```bash
mvn -pl examples/microservices-sample -am clean install
```

The example's own tests exercise the full path against real
infrastructure — two real PostgreSQL containers and a real RabbitMQ
container, no in-memory doubles: placing an order results in exactly one
shipment, the internal `OrderPlaced` event never crosses into RabbitMQ, and
redelivering the same integration event twice still produces exactly one
shipment.

## What's next

See [examples/microservices-sample](https://github.com/Highkeen-Technologies/opendcb/tree/main/examples/microservices-sample)
for the full, runnable source, and
[Event Routing and Microservices](../module-guides/event-routing-and-microservices.md)
for the concepts behind this pattern.
