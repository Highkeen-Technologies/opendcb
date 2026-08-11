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
import com.netflix.conductor.client.worker.Worker;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;

/**
 * A Conductor {@link Worker} that, when polled for work, deserializes the
 * task's input into an Axon command and dispatches it via {@link
 * CommandGateway#sendAndWait(Object)} -- the same synchronous dispatch point
 * {@code examples/microservices-sample}'s {@code
 * OrderPlacedIntegrationEventTranslator} already uses to invoke Axon from
 * outside its own command-handling path. This is the analog of a {@code
 * @SagaEventHandler} method dispatching a command, expressed as a Conductor
 * task worker instead.
 *
 * <p>Register instances with a real {@code
 * com.netflix.conductor.client.automator.TaskRunnerConfigurer} to actually
 * start polling; this class only implements the per-task translation, not
 * the polling loop itself, which Conductor's own SDK already provides.
 *
 * @param <C> the Axon command type this worker dispatches
 */
public class ConductorCommandTaskWorker<C> implements Worker {

    private final String taskDefName;
    private final Class<C> commandType;
    private final CommandGateway commandGateway;
    private final ObjectMapper objectMapper;

    public ConductorCommandTaskWorker(
            String taskDefName, Class<C> commandType, CommandGateway commandGateway, ObjectMapper objectMapper) {
        this.taskDefName = taskDefName;
        this.commandType = commandType;
        this.commandGateway = commandGateway;
        this.objectMapper = objectMapper;
    }

    @Override
    public String getTaskDefName() {
        return taskDefName;
    }

    @Override
    public TaskResult execute(Task task) {
        TaskResult result = new TaskResult(task);
        try {
            C command = objectMapper.convertValue(task.getInputData(), commandType);
            commandGateway.sendAndWait(command);
            result.setStatus(TaskResult.Status.COMPLETED);
        } catch (RuntimeException e) {
            result.setStatus(TaskResult.Status.FAILED);
            result.setReasonForIncompletion(e.getMessage());
        }
        return result;
    }
}
