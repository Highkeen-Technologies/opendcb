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

import com.netflix.conductor.client.http.MetadataClient;
import com.netflix.conductor.client.http.TaskClient;
import com.netflix.conductor.client.http.WorkflowClient;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves {@link ConductorSagaBridge} against a real Conductor OSS server
 * backed by real PostgreSQL persistence (image {@code
 * conductoross/conductor:next}, {@code CONFIG_PROP=config-postgres.properties})
 * -- not a mocked Conductor API, since the whole point of the concurrency test
 * below is proving the {@code saga_correlation} unique-constraint race-breaker
 * actually prevents a genuine double-start against Conductor's real API, not
 * just against a stand-in.
 *
 * <p>The Conductor server's own persistence Postgres must be reachable from
 * inside its container at the hostname {@code postgresdb} / database {@code
 * postgres}, both hardcoded in the image's baked-in {@code
 * config-postgres.properties} -- confirmed by reading that file directly
 * (conductor-oss/conductor's own repo) rather than assumed, since Conductor's
 * custom properties loader does not honor standard Spring Boot environment
 * variable overrides the way a normal Spring Boot app would.
 */
@Testcontainers
class ConductorSagaBridgeIntegrationTest {

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

    @Container
    private static final org.testcontainers.containers.PostgreSQLContainer<?> CORRELATION_POSTGRES =
            new org.testcontainers.containers.PostgreSQLContainer<>("postgres:16");

    private static final String WAIT_WORKFLOW_NAME = "opendcb_saga_bridge_test_wait_wf";
    private static final String WAIT_TASK_REF_NAME = "wait_for_signal";

    private static WorkflowClient workflowClient;
    private static TaskClient taskClient;
    private static SagaCorrelationStore correlationStore;
    private static ConductorSagaBridge bridge;

    @BeforeAll
    static void startClientsAndRegisterWorkflowDefs() {
        String rootUri = "http://" + CONDUCTOR_SERVER.getHost() + ":" + CONDUCTOR_SERVER.getMappedPort(8080) + "/api/";

        workflowClient = new WorkflowClient();
        workflowClient.setRootURI(rootUri);
        taskClient = new TaskClient();
        taskClient.setRootURI(rootUri);
        MetadataClient metadataClient = new MetadataClient();
        metadataClient.setRootURI(rootUri);

        DataSource dataSource = newCorrelationDataSource();
        correlationStore = new SagaCorrelationStore(dataSource);
        correlationStore.ensureSchema();
        bridge = new ConductorSagaBridge(workflowClient, taskClient, correlationStore);

        WorkflowTask waitTask = new WorkflowTask();
        waitTask.setName(WAIT_TASK_REF_NAME);
        waitTask.setTaskReferenceName(WAIT_TASK_REF_NAME);
        waitTask.setWorkflowTaskType(TaskType.WAIT);

        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(WAIT_WORKFLOW_NAME);
        workflowDef.setVersion(1);
        workflowDef.setSchemaVersion(2);
        workflowDef.setOwnerEmail("oss@highkeen.com");
        workflowDef.setTasks(List.of(waitTask));

        metadataClient.registerWorkflowDef(workflowDef);
    }

    @BeforeEach
    void truncateCorrelationTable() throws SQLException {
        try (Connection connection = newCorrelationDataSource().getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE TABLE saga_correlation");
        }
    }

    @Test
    void twoConcurrentStartSagaCallsForTheSameCorrelationKeyResultInExactlyOneConductorWorkflow()
            throws InterruptedException {
        String correlationKey = "order-concurrent-" + System.nanoTime();
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch go = new CountDownLatch(1);
        AtomicReference<String> resultA = new AtomicReference<>();
        AtomicReference<String> resultB = new AtomicReference<>();

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Runnable taskA = () -> {
                ready.countDown();
                await(go);
                resultA.set(bridge.startSagaIfNotAlreadyRunning(
                        correlationKey, "OrderFulfillmentSaga", WAIT_WORKFLOW_NAME, Map.of()));
            };
            Runnable taskB = () -> {
                ready.countDown();
                await(go);
                resultB.set(bridge.startSagaIfNotAlreadyRunning(
                        correlationKey, "OrderFulfillmentSaga", WAIT_WORKFLOW_NAME, Map.of()));
            };

            executor.submit(taskA);
            executor.submit(taskB);
            assertTrue(ready.await(10, TimeUnit.SECONDS));
            go.countDown();
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        }

        assertEquals(resultA.get(), resultB.get());

        List<Workflow> workflows = workflowClient.getWorkflows(WAIT_WORKFLOW_NAME, correlationKey, true, false);
        assertEquals(1, workflows.size());
        assertEquals(resultA.get(), workflows.get(0).getWorkflowId());
    }

    @Test
    void signalSagaCompletesTheWaitTaskAndProgressesTheWorkflowToCompletion() {
        String correlationKey = "order-e2e-" + System.nanoTime();

        String workflowId = bridge.startSagaIfNotAlreadyRunning(
                correlationKey, "OrderFulfillmentSaga", WAIT_WORKFLOW_NAME, Map.of());

        awaitWorkflowTaskInProgress(workflowId, WAIT_TASK_REF_NAME);

        bridge.signalSaga(correlationKey, WAIT_TASK_REF_NAME, Map.of("paid", true));

        Workflow workflow = awaitWorkflowStatus(workflowId, Workflow.WorkflowStatus.COMPLETED);
        assertEquals(Workflow.WorkflowStatus.COMPLETED, workflow.getStatus());
    }

    private static void awaitWorkflowTaskInProgress(String workflowId, String taskRefName) {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(15).toMillis();
        while (System.currentTimeMillis() < deadline) {
            Workflow workflow = workflowClient.getWorkflow(workflowId, true);
            if (workflow.getTaskByRefName(taskRefName) != null) {
                return;
            }
            sleep(100);
        }
        throw new IllegalStateException("Workflow '" + workflowId + "' never reached task '" + taskRefName + "'");
    }

    private static Workflow awaitWorkflowStatus(String workflowId, Workflow.WorkflowStatus expected) {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(15).toMillis();
        Workflow last = null;
        while (System.currentTimeMillis() < deadline) {
            last = workflowClient.getWorkflow(workflowId, true);
            if (last.getStatus() == expected) {
                return last;
            }
            sleep(100);
        }
        throw new IllegalStateException(
                "Workflow '" + workflowId + "' never reached status " + expected + "; last status was "
                        + (last == null ? "unknown" : last.getStatus()));
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static DataSource newCorrelationDataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(CORRELATION_POSTGRES.getJdbcUrl());
        ds.setUser(CORRELATION_POSTGRES.getUsername());
        ds.setPassword(CORRELATION_POSTGRES.getPassword());
        return ds;
    }
}
