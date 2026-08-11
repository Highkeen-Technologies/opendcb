# Sagas with Conductor

## What's a saga, and why would you need one?

A saga (also called a process manager) coordinates a business process that
spans multiple steps over time, often across different parts of your
system: place an order, charge a payment, ship the goods. Each step can
succeed or fail independently, sometimes minutes or days apart, and if a
later step fails, earlier ones may need to be undone (compensated). A saga
is the thing that remembers "where are we in this process" and reacts when
the next relevant event arrives.

## Why Conductor OSS instead of a built-in engine

Everywhere else in OpenDCB, the answer to "how do we get this right" is to
build it directly on top of `EventStoreStorage`. Sagas are the exception. A
saga engine needs the same order of crash-recovery and exactly-once rigor
every other module here has been held to — but proving that from scratch is
materially more work than, say, scheduling was. [Conductor OSS](https://github.com/conductor-oss/conductor)
(Apache 2.0, in production at Netflix/Tesla/LinkedIn/J.P. Morgan scale)
already provides durable, replayable workflow execution with real
saga/compensation support. `opendcb-conductor-bridge` reuses it rather than
re-proving that rigor from scratch — see
[docs/ARCHITECTURE.md](https://github.com/Highkeen-Technologies/opendcb/blob/main/docs/ARCHITECTURE.md#opendcb-conductor-bridge-sagas-via-conductor-oss-not-a-hand-built-engine)
for the full reasoning.

## The one trade-off worth flagging clearly

Every other OpenDCB module needs nothing beyond PostgreSQL and your own
JVM. This one is different: **Conductor runs as its own server**, the same
category of thing as Axon Server. That's a genuine operational cost — one
more service to run and monitor. There's no licensing tension, though;
Conductor is free at every tier, unlike Axon Server. If your system doesn't
need sagas, skip this module entirely — nothing else in OpenDCB depends on
it.

Conductor's `postgres-persistence` module lets it run against PostgreSQL
(a separate database or schema from your own event store — Conductor
manages its own schema), so you're not forced into Cassandra or
Elasticsearch just to use it.

## The missing piece: saga_correlation

Axon 4's sagas kept an internal index from an event property (like an order
ID) to the running saga instance, so a later event could find the right
saga to signal. Conductor doesn't have an equivalent automatic lookup — its
`complete_task` action (used to resume a workflow paused on a `WAIT` task)
needs to know the workflow's own internal ID.

`SagaCorrelationStore` is the small table that closes that gap:
`saga_correlation(correlation_key, conductor_workflow_id, created_at)`. When
a saga starts, its correlation key (say, an order ID) is mapped to the
Conductor workflow ID that was created for it. When a later event needs to
signal that saga, it looks up the workflow ID by correlation key first.

```java
SagaCorrelationStore correlationStore = new SagaCorrelationStore(dataSource);
correlationStore.ensureSchema();

boolean reserved = correlationStore.recordCorrelation(correlationKey, null, "OrderSaga");
Optional<String> workflowId = correlationStore.findWorkflowId(correlationKey);
correlationStore.updateWorkflowId(correlationKey, workflowId);
```

`recordCorrelation` reserves the row with a `null` workflow ID first,
relying on `correlation_key`'s primary-key constraint to guarantee only one
concurrent caller actually wins and calls Conductor's start-workflow API —
not client-side locking. `ConductorSagaBridge` wraps this pattern for you:

```java
ConductorSagaBridge bridge =
        new ConductorSagaBridge(workflowClient, taskClient, correlationStore);

String workflowId = bridge.startSagaIfNotAlreadyRunning(
        correlationKey, "OrderSaga", "order_saga_workflow", input);

bridge.signalSaga(correlationKey, "await-payment", Map.of("paid", true));
```

`startSagaIfNotAlreadyRunning` starts the workflow only if this correlation
key hasn't already started one, returning the running workflow's ID either
way. `signalSaga` completes a named `WAIT` task in that saga's workflow,
which is how a later, correlated event resumes it — the analog of Axon 4's
`@SagaEventHandler` reacting to a correlated event.

## A saga step calling back into your domain

A saga step that needs to actually *do* something — charge a payment,
create a shipment — is a Conductor task worker. `ConductorCommandTaskWorker<C>`
implements Conductor's `Worker` interface: when polled for work, it
deserializes the claimed task's input into your command type and dispatches
it via `CommandGateway.sendAndWait`.

```java
ConductorCommandTaskWorker<MarkOrderPaidCommand> worker = new ConductorCommandTaskWorker<>(
        "mark_order_paid_task", MarkOrderPaidCommand.class, commandGateway, objectMapper);
```

This is the one class in `opendcb-conductor-bridge` that imports
`org.axonframework` — a deliberate, documented exception to the
framework-agnostic rule everywhere else in this toolkit, since dispatching
into your domain genuinely requires `CommandGateway`.

## Starting a saga reactively — no new code needed

Conductor natively consumes AMQP as an event source. Since
`outbox-relay-rabbitmq` already publishes OpenDCB events to RabbitMQ,
Conductor's own `EventHandler` (a JSON definition registered via its REST
API) can subscribe to that same exchange and auto-start a workflow when a
matching event arrives — the direct analog of Axon 4's `@StartSaga`, with no
OpenDCB code required.

## Compensation is your logic, not automatic

Conductor triggers a `failureWorkflow` you designate when the main workflow
fails, passing it context (reason, workflow ID, the failed execution's
JSON) — but the actual reverse-order undo steps are tasks you write
yourself in that failure workflow. The orchestration is automatic; the undo
logic isn't generated for you.

## What's next

Walk through a real, working workflow definition and task worker in
[Build a Saga](../tutorials/build-a-saga.md).
