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

import org.axonframework.eventsourcing.annotation.EventTag;
import org.axonframework.messaging.eventhandling.annotation.Event;

import java.math.BigDecimal;

/**
 * The public contract for "an order was placed" — deliberately smaller than {@link OrderPlaced}, since
 * this is the only shape another bounded context is ever allowed to see. Appended alongside
 * {@link OrderPlaced} in the same command handler (see {@link OrderCommandHandlers}), and is the only
 * event type {@link OrdersService}'s relay filter lets cross the boundary.
 */
@Event
public record OrderPlacedIntegrationEvent(
        @EventTag(key = "OrderEntity")
        String orderId,
        String customerId,
        BigDecimal total
) {
}
