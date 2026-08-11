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

import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.commandhandling.gateway.CommandResult;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;

/**
 * A minimal {@link CommandGateway} test double that captures every dispatched
 * command instead of routing it through a real {@code CommandBus}. {@link
 * CommandGateway} has two abstract members ({@code send(Object, Metadata,
 * ProcessingContext)} and {@code describeTo}, inherited from {@code
 * DescribableComponent}), so it isn't a functional interface and can't be
 * implemented with a lambda -- hence this small named class rather than an
 * inline one, following {@code docs/TESTING.md}'s "simple in-memory test
 * double, not a real CommandBus stack" philosophy applied to {@link
 * ConductorCommandTaskWorkerTest}.
 */
class StubCommandGateway implements CommandGateway {

    private final List<Object> dispatchedCommands = new CopyOnWriteArrayList<>();

    List<Object> dispatchedCommands() {
        return dispatchedCommands;
    }

    @Override
    public CommandResult send(Object command, Metadata metadata, ProcessingContext processingContext) {
        dispatchedCommands.add(command);
        Message resultMessage = new GenericCommandResultMessage(new MessageType("result"), (Object) null);
        CompletableFuture<Message> future = CompletableFuture.completedFuture(resultMessage);
        return () -> future;
    }

    @Override
    public void describeTo(ComponentDescriptor componentDescriptor) {
        // no-op: nothing meaningful to describe for a test double
    }
}
