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

import org.axonframework.eventsourcing.annotation.EventSourcedEntity;
import org.axonframework.eventsourcing.annotation.reflection.EntityCreator;

import java.math.BigDecimal;
import java.util.List;

/**
 * Holds state only — no {@code @CommandHandler} methods. All command handling lives in
 * {@link OrderCommandHandlers}, the separate Stateful Command Handler class, per docs/CONVENTIONS.md.
 *
 * <p>{@link OrderPlacedIntegrationEvent} is also appended to this entity's tagged stream (sharing the
 * same {@code orderId} tag) but has no corresponding {@code @EventSourcingHandler} here — Axon simply
 * skips evolving state for event types with no matching handler, so the entity never needs to know
 * about the public contract it's paired with.
 */
@EventSourcedEntity
public class OrderEntity {

    private final String orderId;
    private final String customerId;
    private final List<String> items;
    private final BigDecimal total;

    @EntityCreator
    public OrderEntity(OrderPlaced event) {
        this.orderId = event.orderId();
        this.customerId = event.customerId();
        this.items = event.items();
        this.total = event.total();
    }

    public String orderId() {
        return orderId;
    }

    public String customerId() {
        return customerId;
    }

    public List<String> items() {
        return items;
    }

    public BigDecimal total() {
        return total;
    }
}
