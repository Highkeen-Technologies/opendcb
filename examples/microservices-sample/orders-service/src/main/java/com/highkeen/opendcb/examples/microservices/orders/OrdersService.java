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

import com.highkeen.opendcb.bootstrap.axon.postgres.OpenDcbAxonPostgres;
import com.highkeen.opendcb.eventstore.core.EventStoreStorage;
import com.highkeen.opendcb.eventstore.core.StoredEvent;
import com.highkeen.opendcb.relay.core.DeadLetterSink;
import com.highkeen.opendcb.relay.core.LoggingDeadLetterSink;
import com.highkeen.opendcb.relay.core.OutboxRelay;
import com.highkeen.opendcb.relay.core.Publisher;
import com.highkeen.opendcb.relay.core.RelayPositionStore;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;

import javax.sql.DataSource;
import java.util.function.Predicate;

/**
 * Wiring for orders-service: its own {@link EventStorageEngine} (backed by its own PostgreSQL database),
 * command handling for {@link PlaceOrder}, and the {@link Predicate} that lets {@link OutboxRelay} relay
 * only {@link OrderPlacedIntegrationEvent}s — this service's public contract — never the rich internal
 * {@link OrderPlaced} event. See docs/ARCHITECTURE.md's "Event-driven microservices" section.
 */
public final class OrdersService {

    /** Exchange this service's integration events are published to. */
    public static final String INTEGRATION_EXCHANGE = "orders.integration";

    /** Routing key representing the "an order was placed" public contract. */
    public static final String INTEGRATION_ROUTING_KEY = "order.placed";

    private static final String RELAY_NAME = "orders-integration-relay";

    private OrdersService() {
    }

    /** Builds and starts an {@link AxonConfiguration} backed by PostgreSQL, creating schema if needed. */
    public static AxonConfiguration start(DataSource dataSource) {
        EventStorageEngine engine = OpenDcbAxonPostgres.engine(dataSource);
        return configurer().registerEventStorageEngine(c -> engine).start();
    }

    /** The entity + command handling wiring, without an event storage engine — callers supply that. */
    public static EventSourcingConfigurer configurer() {
        EventSourcedEntityModule<String, OrderEntity> orderEntity =
                EventSourcedEntityModule.autodetected(String.class, OrderEntity.class);
        var orderCommands = CommandHandlingModule.named("Order")
                .commandHandlers()
                .autodetectedCommandHandlingComponent(c -> new OrderCommandHandlers());
        return EventSourcingConfigurer.create()
                .registerEntity(orderEntity)
                .registerCommandHandlingModule(orderCommands);
    }

    /** Matches only {@link OrderPlacedIntegrationEvent}s — the public contract, never {@link OrderPlaced}. */
    public static Predicate<StoredEvent> integrationEventFilter() {
        return event -> OrderPlacedIntegrationEvent.class.getName().equals(event.payloadClass());
    }

    /** An {@link OutboxRelay} that only ever relays {@link OrderPlacedIntegrationEvent}s to {@code publisher}. */
    public static OutboxRelay integrationRelay(
            EventStoreStorage storage,
            Publisher publisher,
            RelayPositionStore positionStore) {
        DeadLetterSink deadLetterSink = new LoggingDeadLetterSink();
        return new OutboxRelay(storage, publisher, positionStore, deadLetterSink, RELAY_NAME, 100, integrationEventFilter());
    }
}
