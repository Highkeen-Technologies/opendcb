/*
 * Copyright the OpenDCB contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.highkeen.opendcb.conductor.bridge;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.client.automator.TaskRunnerConfigurer;
import com.netflix.conductor.client.http.MetadataClient;
import com.netflix.conductor.client.http.TaskClient;
import com.netflix.conductor.client.http.WorkflowClient;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.StartWorkflowRequest;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Proves {@link ConductorCommandTaskWorker} against a real Conductor OSS
 * server: a real workflow is started with a {@code SIMPLE} task, a real
 * {@code TaskRunnerConfigurer} polls for and hands the task to the worker
 * under test, and the worker dispatches an Axon command captured by {@link
 * StubCommandGateway} -- proving the actual translation-layer behaviour
 * (task input JSON -&gt; deserialized command -&gt; dispatched via {@code
 * CommandGateway#sendAndWait}), not just that the classes compile together.
 *
 * <p>Does not use a real Axon {@code CommandBus}/{@code CommandGateway} stack
 * -- see {@link StubCommandGateway}'s Javadoc for why a hand-rolled test
 * double is the right isolation boundary here, matching {@code
 * docs/TESTING.md}'s stated philosophy for framework adapter tests.
 */
@Testcontainers
class ConductorCommandTaskWorkerTest {

    private static final Network NETWORK = Network.newNetwork();

    @Container
    private static final GenericContainer<?> CONDUCTOR_POSTGRES = new GenericContainer<>(DockerImageName.parse("postgres:16"))
            .withNetwork(NETWORK)
            .withNetworkAliases("postgresdb")
            .withEnv("POSTGRES_USER", "conductor")
            .withEnv("POSTGRES_PASSWORD", "conductor")
            .withEnv("POSTGRES_DB", "postgres")
            .withExposedPorts(5432)
            .waitingFor(Wait.forListeningPort());

    @Container
    private static final GenericContainer<?> CONDUCTOR_SERVER = new GenericContainer<>(DockerImageName.parse("conductoross/conductor:next"))
            .withNetwork(NETWORK)
            .withEnv("CONFIG_PROP", "config-postgres.properties")
            .withExposedPorts(8080)
            .waitingFor(Wait.forHttp("/health").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(3)))
            .dependsOn(CONDUCTOR_POSTGRES);

    private static final String WORKFLOW_NAME = "opendcb_command_task_worker_test_wf";
    private static final String TASK_DEF_NAME = "opendcb_mark_order_paid_task";
    private static final String TASK_REF_NAME = "mark_order_paid";

    private static WorkflowClient workflowClient;
    private static TaskClient taskClient;

    private TaskRunnerConfigurer taskRunnerConfigurer;

    private record MarkOrderPaidCommand(String orderId) {
    }

    @BeforeAll
    static void startClientsAndRegisterDefinitions() {
        String rootUri = "http://" + CONDUCTOR_SERVER.getHost() + ":" + CONDUCTOR_SERVER.getMappedPort(8080) + "/api/";

        workflowClient = new WorkflowClient();
        workflowClient.setRootURI(rootUri);
        taskClient = new TaskClient();
        taskClient.setRootURI(rootUri);
        MetadataClient metadataClient = new MetadataClient();
        metadataClient.setRootURI(rootUri);

        TaskDef taskDef = new TaskDef(TASK_DEF_NAME);
        metadataClient.registerTaskDefs(List.of(taskDef));

        WorkflowTask simpleTask = new WorkflowTask();
        simpleTask.setName(TASK_DEF_NAME);
        simpleTask.setTaskReferenceName(TASK_REF_NAME);
        simpleTask.setWorkflowTaskType(TaskType.SIMPLE);
        simpleTask.setInputParameters(Map.of("orderId", "${workflow.input.orderId}"));

        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(WORKFLOW_NAME);
        workflowDef.setVersion(1);
        workflowDef.setSchemaVersion(2);
        workflowDef.setOwnerEmail("oss@highkeen.com");
        workflowDef.setTasks(List.of(simpleTask));

        metadataClient.registerWorkflowDef(workflowDef);
    }

    @AfterEach
    void stopWorker() {
        if (taskRunnerConfigurer != null) {
            taskRunnerConfigurer.shutdown();
        }
    }

    @Test
    void aPolledTaskIsDeserializedAndDispatchedAsARealAxonCommand() {
        StubCommandGateway commandGateway = new StubCommandGateway();
        ConductorCommandTaskWorker<MarkOrderPaidCommand> worker = new ConductorCommandTaskWorker<>(
                TASK_DEF_NAME, MarkOrderPaidCommand.class, commandGateway, new ObjectMapper());

        taskRunnerConfigurer = new TaskRunnerConfigurer.Builder(taskClient, List.of(worker))
                .withThreadCount(1)
                .withSleepWhenRetry(100)
                .build();
        taskRunnerConfigurer.init();

        StartWorkflowRequest request = new StartWorkflowRequest()
                .withName(WORKFLOW_NAME)
                .withInput(Map.of("orderId", "order-789"));
        String workflowId = workflowClient.startWorkflow(request);

        awaitCondition(Duration.ofSeconds(15), () -> commandGateway.dispatchedCommands().size() == 1);
        assertEquals(1, commandGateway.dispatchedCommands().size());
        assertEquals(new MarkOrderPaidCommand("order-789"), commandGateway.dispatchedCommands().get(0));

        awaitCondition(
                Duration.ofSeconds(15),
                () -> workflowClient.getWorkflow(workflowId, false).getStatus() == Workflow.WorkflowStatus.COMPLETED);
        assertEquals(Workflow.WorkflowStatus.COMPLETED, workflowClient.getWorkflow(workflowId, false).getStatus());
    }

    private static void awaitCondition(Duration timeout, java.util.function.BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }
}
