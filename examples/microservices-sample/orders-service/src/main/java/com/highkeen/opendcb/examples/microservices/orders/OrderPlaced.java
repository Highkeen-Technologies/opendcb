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
import java.util.List;

/**
 * The rich internal domain event for orders-service — full detail, never leaves this service's own
 * event store. {@link OrderPlacedIntegrationEvent} is the deliberately smaller public contract emitted
 * alongside it; see docs/ARCHITECTURE.md's public-vs-internal event shape principle.
 */
@Event
public record OrderPlaced(
        @EventTag(key = "OrderEntity")
        String orderId,
        String customerId,
        List<String> items,
        BigDecimal total
) {
}
