# Event Routing and Microservices

## What outbox-relay-core does

If your system is split into multiple services, each with its own event
store, something needs to carry events across that boundary reliably —
even if the process carrying them crashes partway through. That's
`outbox-relay-core`'s job: `OutboxRelay` tails your event log from a
persisted "last relayed position," publishes each event in order, and
saves the new position immediately after each one — not batched at the
end — so a crash mid-batch resumes from exactly where it left off, rather
than replaying already-published events or skipping unpublished ones.

Publishing itself is pluggable via a `Publisher` interface with one method,
`publish(StoredEvent)`. `outbox-relay-rabbitmq`'s `RabbitMqPublisher` is
the one working transport today, built on the plain `com.rabbitmq:amqp-client`
library (no Spring AMQP). It uses RabbitMQ's publisher confirms and the
`mandatory` flag, so a broker-side rejection is detected rather than
silently dropped.

Failures split into two kinds, and `OutboxRelay` treats them differently:

- **Retryable** (e.g. the broker connection dropped, a confirm timed out) —
  stop the batch without advancing the position, so the next poll retries
  the same event.
- **Non-retryable** (e.g. the broker rejected the message, or no queue was
  bound to receive it) — hand the event to a `DeadLetterSink`, advance past
  it, and keep going.

## Deciding what actually crosses the boundary

Not every event a service produces should be visible to other services —
often a service has rich internal events full of implementation detail
that should stay internal. `OutboxRelay`'s constructor takes an optional
`Predicate<StoredEvent>` filter (default: match everything). `examples/microservices-sample`
shows this in practice: `orders-service`'s `OrderCommandHandlers` appends
*two* events atomically when an order is placed — the rich internal
`OrderPlaced`, and a deliberately smaller `OrderPlacedIntegrationEvent`
carrying only `orderId`, `customerId`, and `total`. `OrdersService.integrationEventFilter()`
is the real `Predicate<StoredEvent>` that matches only the integration
event's type:

```java
public Predicate<StoredEvent> integrationEventFilter() {
    return event -> "OrderPlacedIntegrationEvent".equals(event.messageType());
}
```

Because the relay's position still advances past a filtered-out event, the
internal `OrderPlaced` is simply never published — it stays internal by
construction, with no separate access-control mechanism needed.

## Translating on the receiving side

The service on the other end shouldn't treat an incoming integration event
as its own domain event — that would let another team's event shape leak
into your model. `examples/microservices-sample`'s `shipping-service` shows
the pattern: `IntegrationEventConsumer` is a plain `amqp-client` consumer
bound to the same exchange/routing key `orders-service` publishes to.
Every message it receives is handed to `OrderPlacedIntegrationEventTranslator`
— an anti-corruption layer that deserializes the envelope, then dispatches
a local `CreateShipmentCommand` rather than acting on the incoming payload
directly.

## Idempotency: surviving redelivery

Message brokers can redeliver a message more than once. `OrderPlacedIntegrationEventTranslator`
handles this with a `processed_integration_events(event_id VARCHAR(255) PRIMARY KEY)`
table: it attempts an `INSERT` for the incoming event's ID first, and
treats a unique-constraint violation (SQLState `23505`) as "already
processed" rather than doing a separate `SELECT` check beforehand. This
means the database's own primary-key constraint — not application logic —
is what makes concurrent or repeated delivery of the same event safe,
resulting in exactly one shipment even if the same message arrives twice.

## What's next

Walk through the whole flow — place an order, relay it, translate it,
create a shipment — in
[Build a Two-Service System](../tutorials/build-a-two-service-system.md).
