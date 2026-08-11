# Build a Saga

This tutorial walks through a real, working saga shape, grounded directly
in `opendcb-conductor-bridge`'s own test suite: a Conductor workflow with a
task a worker dispatches as an Axon command. You need a running Conductor
OSS server (`conductoross/conductor:next`) for any of this to actually run —
see [Sagas with Conductor](../module-guides/sagas-with-conductor.md) for why
that's the one OpenDCB module with this requirement.

## Step 1: define the workflow and its task

A Conductor `WorkflowDef` is a JSON-shaped definition, registered via
`MetadataClient`. This one has a single `SIMPLE` task — the kind that gets
polled for and executed by a worker, as opposed to a `WAIT` task, which
pauses the workflow until something external completes it.

```java
MetadataClient metadataClient = new MetadataClient();
metadataClient.setRootURI(rootUri);

TaskDef taskDef = new TaskDef("opendcb_mark_order_paid_task");
metadataClient.registerTaskDefs(List.of(taskDef));

WorkflowTask simpleTask = new WorkflowTask();
simpleTask.setName("opendcb_mark_order_paid_task");
simpleTask.setTaskReferenceName("mark_order_paid");
simpleTask.setWorkflowTaskType(TaskType.SIMPLE);
simpleTask.setInputParameters(Map.of("orderId", "${workflow.input.orderId}"));

WorkflowDef workflowDef = new WorkflowDef();
workflowDef.setName("mark_order_paid_wf");
workflowDef.setVersion(1);
workflowDef.setSchemaVersion(2);
workflowDef.setOwnerEmail("you@example.com");
workflowDef.setTasks(List.of(simpleTask));

metadataClient.registerWorkflowDef(workflowDef);
```

`setInputParameters` maps the workflow's own input into what the task
receives — here, the task gets `orderId` straight from the workflow's
input.

## Step 2: a task worker that dispatches an Axon command

`ConductorCommandTaskWorker<C>` implements Conductor's `Worker` interface.
When polled and handed a task, it deserializes the task's input into your
command type and dispatches it via `CommandGateway`:

```java
record MarkOrderPaidCommand(String orderId) {
}

ConductorCommandTaskWorker<MarkOrderPaidCommand> worker = new ConductorCommandTaskWorker<>(
        "opendcb_mark_order_paid_task", MarkOrderPaidCommand.class, commandGateway, new ObjectMapper());
```

## Step 3: start polling for work

```java
TaskRunnerConfigurer taskRunnerConfigurer =
        new TaskRunnerConfigurer.Builder(taskClient, List.of(worker))
                .withThreadCount(1)
                .withSleepWhenRetry(100)
                .build();
taskRunnerConfigurer.init();
```

## Step 4: start the workflow

```java
StartWorkflowRequest request = new StartWorkflowRequest()
        .withName("mark_order_paid_wf")
        .withInput(Map.of("orderId", "order-789"));

String workflowId = workflowClient.startWorkflow(request);
```

Once the worker polls, claims the task, and dispatches
`MarkOrderPaidCommand("order-789")` through your `CommandGateway`, the
workflow completes.

## Adding a WAIT step: correlating a later event

If a step in your saga needs to pause until a later, unrelated event
arrives (not just wait for a task worker to finish), use a `WAIT`-type task
instead of `SIMPLE`:

```java
WorkflowTask waitTask = new WorkflowTask();
waitTask.setName("wait_for_signal");
waitTask.setTaskReferenceName("wait_for_signal");
waitTask.setWorkflowTaskType(TaskType.WAIT);
```

Start this saga with `ConductorSagaBridge.startSagaIfNotAlreadyRunning`
instead of calling `WorkflowClient` directly — it records the correlation
key so a later event can find this workflow again:

```java
ConductorSagaBridge bridge = new ConductorSagaBridge(workflowClient, taskClient, correlationStore);

String workflowId = bridge.startSagaIfNotAlreadyRunning(
        "order-123", "OrderFulfillmentSaga", "mark_order_paid_wf", Map.of());
```

When the correlated event later arrives (say, a payment confirmation),
resume the paused workflow:

```java
bridge.signalSaga("order-123", "wait_for_signal", Map.of("paid", true));
```

## What's next

See `opendcb-conductor-bridge`'s own
[`ConductorSagaBridgeIntegrationTest`](https://github.com/Highkeen-Technologies/opendcb/blob/main/opendcb-conductor-bridge/src/test/java/com/highkeen/opendcb/conductor/bridge/ConductorSagaBridgeIntegrationTest.java)
and
[`ConductorCommandTaskWorkerTest`](https://github.com/Highkeen-Technologies/opendcb/blob/main/opendcb-conductor-bridge/src/test/java/com/highkeen/opendcb/conductor/bridge/ConductorCommandTaskWorkerTest.java)
for the complete, real, passing versions of both flows above, including
Testcontainers setup for a real Conductor server. [Sagas with Conductor](../module-guides/sagas-with-conductor.md)
covers the concepts (`saga_correlation`, compensation) behind this code.
