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

import com.netflix.conductor.client.http.TaskClient;
import com.netflix.conductor.client.http.WorkflowClient;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.metadata.workflow.StartWorkflowRequest;
import com.netflix.conductor.common.run.Workflow;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * Starts and signals Conductor OSS sagas, using {@link SagaCorrelationStore}
 * as the missing correlation-key -&gt; workflow-instance index that Axon's own
 * association-property saga index provided for free (see
 * {@code docs/ARCHITECTURE.md}'s "opendcb-conductor-bridge" section).
 *
 * <p><b>Grounded against the actually-published SDK, not GitHub HEAD:</b>
 * {@code conductor-oss/java-sdk}'s GitHub HEAD (tag {@code v6.0.0}) exposes a
 * convenience {@code TaskClient.updateTask(workflowId, taskRefName, status,
 * output)} method, but that version has never been published --
 * {@code io.orkes.conductor:conductor-client:3.9.31-orkes} is the latest
 * version actually resolvable from Maven Central, verified directly against
 * its bytecode. That version's {@link TaskClient} only exposes {@code
 * updateTask(TaskResult)}, which needs a real {@code taskId}, not just a
 * reference name. {@link #signalSaga} therefore fetches the workflow via
 * {@link WorkflowClient#getWorkflow(String, boolean)} and resolves the task ID
 * via {@link Workflow#getTaskByRefName(String)} first -- validated end-to-end
 * against a real running Conductor server before this class was written.
 *
 * <p><b>Why {@link #startSagaIfNotAlreadyRunning} doesn't match a literal
 * reading of "record the correlation, then start":</b> {@code
 * saga_correlation.conductor_workflow_id} can't be known until <i>after</i>
 * Conductor's {@code startWorkflow} call returns, but the insert that breaks
 * the concurrent-start race must happen <i>before</i> that call, or two
 * racing callers would both call {@code startWorkflow} and genuinely create
 * two Conductor workflow instances. This method resolves that by having
 * {@link SagaCorrelationStore#recordCorrelation} reserve the row with a
 * {@code null} workflow ID first -- the unique constraint on {@code
 * correlation_key} is still what breaks the race, exactly as designed -- then
 * only the winner calls {@code startWorkflow} and fills the reservation in
 * via {@link SagaCorrelationStore#updateWorkflowId}. A caller that loses the
 * race never calls Conductor at all; it instead polls {@link
 * SagaCorrelationStore#findWorkflowId} for a short, bounded window until the
 * winner's update lands, since the winner's HTTP round-trip to Conductor is
 * still in flight at that point.
 */
public class ConductorSagaBridge {

    private static final Logger LOG = System.getLogger(ConductorSagaBridge.class.getName());

    private static final Duration AWAIT_WORKFLOW_ID_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration AWAIT_WORKFLOW_ID_POLL_INTERVAL = Duration.ofMillis(25);

    private final WorkflowClient workflowClient;
    private final TaskClient taskClient;
    private final SagaCorrelationStore correlationStore;

    public ConductorSagaBridge(WorkflowClient workflowClient, TaskClient taskClient, SagaCorrelationStore correlationStore) {
        this.workflowClient = workflowClient;
        this.taskClient = taskClient;
        this.correlationStore = correlationStore;
    }

    /**
     * Starts {@code workflowName} in Conductor unless a saga for {@code
     * correlationKey} is already running, returning the running workflow's ID
     * either way. Safe under concurrent callers racing the same {@code
     * correlationKey}: exactly one actually calls Conductor's start-workflow
     * API, enforced by {@code saga_correlation}'s primary key, not by
     * client-side locking.
     */
    public String startSagaIfNotAlreadyRunning(
            String correlationKey, String sagaType, String workflowName, Map<String, Object> input) {
        boolean reserved = correlationStore.recordCorrelation(correlationKey, null, sagaType);
        if (!reserved) {
            return awaitWorkflowId(correlationKey);
        }

        StartWorkflowRequest request =
                new StartWorkflowRequest().withName(workflowName).withCorrelationId(correlationKey).withInput(input);
        String workflowId = workflowClient.startWorkflow(request);
        correlationStore.updateWorkflowId(correlationKey, workflowId);
        return workflowId;
    }

    /**
     * Completes the {@code WAIT} task named {@code taskRefName} in the saga
     * correlated with {@code correlationKey}, resuming it -- the analog of
     * Axon 4's {@code @SagaEventHandler} reacting to a later correlated
     * event. If no correlation is found, or the workflow has no task with
     * that reference name, this is logged clearly rather than silently
     * swallowed: it likely means an event arrived for a saga that was never
     * started, or one that already completed and whose correlation should
     * arguably have been cleaned up -- a known gap, see {@code
     * docs/ROADMAP.md}.
     */
    public void signalSaga(String correlationKey, String taskRefName, Map<String, Object> output) {
        Optional<String> workflowId = correlationStore.findWorkflowId(correlationKey);
        if (workflowId.isEmpty()) {
            LOG.log(
                    Level.WARNING,
                    "signalSaga: no saga_correlation row found for correlation key '{0}' -- "
                            + "either no saga was ever started for it, or its row was never cleaned up "
                            + "after the saga completed",
                    correlationKey);
            return;
        }

        Workflow workflow = workflowClient.getWorkflow(workflowId.get(), true);
        Task task = workflow.getTaskByRefName(taskRefName);
        if (task == null) {
            LOG.log(
                    Level.WARNING,
                    "signalSaga: workflow '{0}' (correlation key '{1}') has no task with reference name '{2}' -- "
                            + "it may not have reached that step yet, or the saga definition changed",
                    workflowId.get(),
                    correlationKey,
                    taskRefName);
            return;
        }

        TaskResult result = new TaskResult();
        result.setWorkflowInstanceId(workflowId.get());
        result.setTaskId(task.getTaskId());
        result.setStatus(TaskResult.Status.COMPLETED);
        result.setOutputData(output);
        taskClient.updateTask(result);
    }

    private String awaitWorkflowId(String correlationKey) {
        Instant deadline = Instant.now().plus(AWAIT_WORKFLOW_ID_TIMEOUT);
        while (Instant.now().isBefore(deadline)) {
            Optional<String> workflowId = correlationStore.findWorkflowId(correlationKey);
            if (workflowId.isPresent()) {
                return workflowId.get();
            }
            try {
                Thread.sleep(AWAIT_WORKFLOW_ID_POLL_INTERVAL.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Interrupted while awaiting concurrent saga start for correlation key '" + correlationKey
                                + "'",
                        e);
            }
        }
        throw new IllegalStateException(
                "Timed out waiting for a concurrent startSagaIfNotAlreadyRunning call to record its workflow id "
                        + "for correlation key '" + correlationKey + "'");
    }
}
