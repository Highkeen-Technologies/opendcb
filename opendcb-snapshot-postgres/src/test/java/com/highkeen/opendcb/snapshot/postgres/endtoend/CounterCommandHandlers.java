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
package com.highkeen.opendcb.snapshot.postgres.endtoend;

import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.modelling.annotation.InjectEntity;

/**
 * Stateful Command Handler, same split as {@code examples/plain-java-sample}'s
 * {@code AccountCommandHandlers}. The instance handler injects {@link CounterEntity} via
 * {@link InjectEntity}, which is what forces a genuine sourcing/load operation on every dispatch --
 * necessary to exercise {@code @Snapshotting} and, later, the snapshot-read path.
 */
class CounterCommandHandlers {

    @CommandHandler
    void handle(CreateCounter command, EventAppender eventAppender) {
        eventAppender.append(new CounterCreated(command.counterId()));
    }

    @CommandHandler
    void handle(IncrementCounter command, @InjectEntity CounterEntity counter, EventAppender eventAppender) {
        eventAppender.append(new CounterIncremented(command.counterId()));
    }
}
