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
package com.highkeen.opendcb.examples.microservices.shipping;

import org.axonframework.eventsourcing.annotation.EventTag;
import org.axonframework.messaging.eventhandling.annotation.Event;

/**
 * shipping-service's own domain event — never orders-service's {@code OrderPlaced} or
 * {@code OrderPlacedIntegrationEvent} directly. {@link OrderPlacedIntegrationEventTranslator} is the
 * anti-corruption layer that produces the {@link CreateShipmentCommand} this event results from.
 */
@Event
public record ShipmentCreated(
        @EventTag(key = "ShipmentEntity")
        String orderId,
        String customerId
) {
}
