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

import com.highkeen.opendcb.bootstrap.axon.postgres.OpenDcbAxonPostgres;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;

import javax.sql.DataSource;

/**
 * Wiring for shipping-service: its own {@link EventStorageEngine} (backed by its own PostgreSQL
 * database, entirely separate from orders-service's), and command handling for
 * {@link CreateShipmentCommand} — the local command {@link OrderPlacedIntegrationEventTranslator}
 * produces after translating an incoming integration event.
 */
public final class ShippingService {

    private ShippingService() {
    }

    /** Builds and starts an {@link AxonConfiguration} backed by PostgreSQL, creating schema if needed. */
    public static AxonConfiguration start(DataSource dataSource) {
        EventStorageEngine engine = OpenDcbAxonPostgres.engine(dataSource);
        return configurer().registerEventStorageEngine(c -> engine).start();
    }

    /** The entity + command handling wiring, without an event storage engine — callers supply that. */
    public static EventSourcingConfigurer configurer() {
        EventSourcedEntityModule<String, ShipmentEntity> shipmentEntity =
                EventSourcedEntityModule.autodetected(String.class, ShipmentEntity.class);
        var shipmentCommands = CommandHandlingModule.named("Shipment")
                .commandHandlers()
                .autodetectedCommandHandlingComponent(c -> new ShipmentCommandHandlers());
        return EventSourcingConfigurer.create()
                .registerEntity(shipmentEntity)
                .registerCommandHandlingModule(shipmentCommands);
    }
}
