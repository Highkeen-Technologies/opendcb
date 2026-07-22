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
package com.highkeen.opendcb.examples.microservices.orders;

import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;

/**
 * Stateful Command Handler: a class separate from {@link OrderEntity}, registered independently via
 * {@code CommandHandlingModule} rather than as command handlers declared on the entity itself.
 *
 * <p>Placing an order appends two events atomically in one call to {@link EventAppender#append(Object...)}:
 * the rich internal {@link OrderPlaced} and the deliberately smaller public {@link OrderPlacedIntegrationEvent}
 * — the concrete instance of docs/ARCHITECTURE.md's public-vs-internal event shape principle.
 */
class OrderCommandHandlers {

    @CommandHandler
    void handle(PlaceOrder command, EventAppender eventAppender) {
        eventAppender.append(
                new OrderPlaced(command.orderId(), command.customerId(), command.items(), command.total()),
                new OrderPlacedIntegrationEvent(command.orderId(), command.customerId(), command.total()));
    }
}
