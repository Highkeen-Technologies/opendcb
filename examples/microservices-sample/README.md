# OpenDCB :: Examples :: Microservices Sample

Two independent services demonstrating the **event-driven microservices**
deployment shape from [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md):
each service owns its own `eventstore-postgres` database, and the only thing
crossing the boundary between them is a deliberately-shaped integration
event, carried by `outbox-relay-rabbitmq`.

The domain is intentionally generic and fictional — placing an order
(`orders-service`) triggers creating a shipment (`shipping-service`) — so the
sample stays a clean illustration of the pattern rather than tied to any
real client's domain.

## Architecture

```
 orders-service (own Postgres)              shipping-service (own Postgres)
┌─────────────────────────────┐            ┌──────────────────────────────┐
│ PlaceOrder command          │            │                              │
│   └─ OrderCommandHandlers   │            │                              │
│        appends TWO events:  │            │                              │
│        • OrderPlaced        │            │                              │
│          (rich, internal)   │            │                              │
│        • OrderPlacedInteg-  │            │                              │
│          rationEvent        │            │                              │
│          (small, public)    │            │                              │
│                              │            │                              │
│ OutboxRelay + filter ────────┼─ RabbitMQ ─┼──▶ IntegrationEventConsumer  │
│ (only OrderPlacedIntegration-│  exchange  │      └─ OrderPlacedInteg-    │
│  Event ever matches; the    │  "orders.  │         rationEventTranslator│
│  rich internal event never  │  integration"│      (anti-corruption layer)│
│  crosses)                   │            │        └─ CreateShipmentCmd  │
│                              │            │             └─ ShipmentEntity│
└─────────────────────────────┘            └──────────────────────────────┘
```

### Public vs. internal event shape

`orders-service`'s `PlaceOrder` command handler appends **two** events
atomically in the same call: the rich `OrderPlaced` (order id, customer id,
line items, total — internal domain detail) and a separate, deliberately
smaller `OrderPlacedIntegrationEvent` (order id, customer id, total only).
This is the concrete instance of the "public vs internal event shape"
principle from `docs/ARCHITECTURE.md`: the internal event is free to evolve
with the domain, while the integration event is the stable public contract
other services can depend on.

`OrdersService.integrationRelay(...)` wires an `OutboxRelay` with a
`Predicate<StoredEvent>` filter that matches only
`OrderPlacedIntegrationEvent` by payload class. Every other event — starting
with `OrderPlaced` itself — simply never matches, so the relay skips it
(advancing its position past it, per the filter semantics added to
`outbox-relay-core`) without ever publishing it. The rich internal event
never leaves `orders-service`'s own database; no separate mechanism is
needed to keep it internal.

### The translator (anti-corruption layer)

`shipping-service` never treats the incoming RabbitMQ message as its own
domain event. `IntegrationEventConsumer` hands the raw message bytes to
`OrderPlacedIntegrationEventTranslator`, which:

1. Parses the published envelope and extracts the integration event's
   `eventId`.
2. Deduplicates using that `eventId` — a unique constraint on
   `processed_integration_events(event_id)` in shipping-service's own
   database. The row is inserted *before* dispatching the local command; a
   duplicate delivery fails that insert and returns without touching the
   command gateway at all.
3. Only then translates the payload into a local `CreateShipmentCommand`,
   dispatched against `shipping-service`'s own `ShipmentEntity` and its own
   event store.

Because relay + broker delivery is at-least-once, this dedup step is what
makes redelivery of the same integration event a no-op instead of a second
shipment — see the end-to-end test below, which proves it by redelivering
the identical message and asserting the shipment count never exceeds one.

## End-to-end test

`OrdersToShippingEndToEndTest` (in `shipping-service`) spins up two separate
`PostgreSQLContainer` instances (one per service — genuinely two databases,
not a shared schema) plus one `RabbitMQContainer`, and proves:

1. Placing an order appends both events to `orders-service`'s own store.
2. Running the relay delivers **only** the integration event to RabbitMQ.
3. `shipping-service` creates a shipment in its **own** database, verified
   by sourcing it from a second, independent `AxonConfiguration` built
   fresh against the same `DataSource` — not a re-read of the same
   in-memory objects that created it.
4. Redelivering the exact same integration event a second time (simulating
   at-least-once broker redelivery) does not create a second shipment.
5. The rich internal `OrderPlaced` event is never observed anywhere on the
   broker — a test subscriber bound to the same exchange/routing key
   receives exactly one message, and it's shaped like the integration
   event, never the internal one.

Run it (starts real containers, takes ~30-60s):

```
mvn -pl examples/microservices-sample/shipping-service -am test
```
