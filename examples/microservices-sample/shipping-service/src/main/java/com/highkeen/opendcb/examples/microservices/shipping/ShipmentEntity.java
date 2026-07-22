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

import org.axonframework.eventsourcing.annotation.EventSourcedEntity;
import org.axonframework.eventsourcing.annotation.reflection.EntityCreator;

/**
 * Holds state only — no {@code @CommandHandler} methods. All command handling lives in
 * {@link ShipmentCommandHandlers}, the separate Stateful Command Handler class, per docs/CONVENTIONS.md.
 * Keyed by {@code orderId} — one shipment per order in this toy domain.
 */
@EventSourcedEntity
public class ShipmentEntity {

    private final String orderId;
    private final String customerId;

    @EntityCreator
    public ShipmentEntity(ShipmentCreated event) {
        this.orderId = event.orderId();
        this.customerId = event.customerId();
    }

    public String orderId() {
        return orderId;
    }

    public String customerId() {
        return customerId;
    }
}
