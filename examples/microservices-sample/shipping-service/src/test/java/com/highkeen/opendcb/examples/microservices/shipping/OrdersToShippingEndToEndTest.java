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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.highkeen.opendcb.bootstrap.axon.postgres.OpenDcbAxonPostgres;
import com.highkeen.opendcb.eventstore.core.StoredEvent;
import com.highkeen.opendcb.eventstore.postgres.PostgresEventStoreStorage;
import com.highkeen.opendcb.examples.microservices.orders.OrderPlaced;
import com.highkeen.opendcb.examples.microservices.orders.OrderPlacedIntegrationEvent;
import com.highkeen.opendcb.examples.microservices.orders.OrdersService;
import com.highkeen.opendcb.examples.microservices.orders.PlaceOrder;
import com.highkeen.opendcb.relay.core.JdbcRelayPositionStore;
import com.highkeen.opendcb.relay.core.OutboxRelay;
import com.highkeen.opendcb.relay.rabbitmq.RabbitMqPublisher;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.modelling.StateManager;
import org.axonframework.modelling.repository.ManagedEntity;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The end-to-end proof of the whole microservices-sample story: two genuinely separate PostgreSQL
 * databases (one per service) and one shared RabbitMQ broker. Places an order in orders-service,
 * relays only its public {@link OrderPlacedIntegrationEvent} across the boundary, and confirms
 * shipping-service's translator turns it into a shipment in its own, independently-sourced database —
 * while the rich internal {@link OrderPlaced} event never crosses at all, and redelivering the same
 * integration event never creates a second shipment.
 */
@Testcontainers
class OrdersToShippingEndToEndTest {

    @Container
    private static final PostgreSQLContainer<?> ORDERS_POSTGRES = new PostgreSQLContainer<>("postgres:16");

    @Container
    private static final PostgreSQLContainer<?> SHIPPING_POSTGRES = new PostgreSQLContainer<>("postgres:16");

    @Container
    private static final RabbitMQContainer RABBITMQ = new RabbitMQContainer("rabbitmq:3.13-management");

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void orderPlacedInOrdersServiceResultsInExactlyOneShipmentInShippingServiceAndNeverRelaysTheInternalEvent()
            throws Exception {
        DataSource ordersDataSource = postgresDataSource(ORDERS_POSTGRES);
        DataSource shippingDataSource = postgresDataSource(SHIPPING_POSTGRES);

        // --- orders-service: place an order, appending both the internal and public events atomically.
        AxonConfiguration ordersConfig = OrdersService.start(ordersDataSource);
        CommandGateway ordersGateway = ordersConfig.getComponent(CommandGateway.class);

        String orderId = "order-" + UUID.randomUUID();
        ordersGateway.sendAndWait(new PlaceOrder(
                orderId, "customer-1", List.of("widget", "gadget"), new BigDecimal("42.50")));

        PostgresEventStoreStorage ordersStorage = new PostgresEventStoreStorage(ordersDataSource);
        assertEquals(2, ordersStorage.readRange(0, null, 100).size(),
                "orders-service's own store should hold both OrderPlaced and OrderPlacedIntegrationEvent");

        // --- shipping-service: its own, entirely separate database.
        AxonConfiguration shippingConfig = ShippingService.start(shippingDataSource);
        CommandGateway shippingGateway = shippingConfig.getComponent(CommandGateway.class);

        Connection rabbitConnection = newRabbitConnection();
        try {
            // A test subscriber bound to the same exchange/routing key as shipping-service's own
            // consumer -- a direct exchange fans the same message out to every bound queue, so this
            // observes exactly what crossed the boundary, independent of the translator's own handling.
            String monitorQueue = declareAndBindQueue(rabbitConnection, "monitor." + UUID.randomUUID());
            BlockingQueue<byte[]> monitored = subscribe(rabbitConnection, monitorQueue);

            OrderPlacedIntegrationEventTranslator translator =
                    new OrderPlacedIntegrationEventTranslator(shippingGateway, shippingDataSource);
            translator.ensureSchema();
            IntegrationEventConsumer consumer = new IntegrationEventConsumer(
                    rabbitConnection,
                    OrdersService.INTEGRATION_EXCHANGE,
                    OrdersService.INTEGRATION_ROUTING_KEY,
                    "shipping-service." + OrdersService.INTEGRATION_ROUTING_KEY,
                    translator);
            consumer.start();

            RabbitMqPublisher publisher = new RabbitMqPublisher(
                    rabbitConnection, OrdersService.INTEGRATION_EXCHANGE, OrdersService.INTEGRATION_ROUTING_KEY);
            JdbcRelayPositionStore positionStore = new JdbcRelayPositionStore(ordersDataSource);
            positionStore.ensureSchema();
            OutboxRelay relay = OrdersService.integrationRelay(ordersStorage, publisher, positionStore);

            // 1 & 2: run the relay -- the only event that should ever reach RabbitMQ is the integration event.
            relay.runOnce();

            byte[] firstDelivery = monitored.poll(10, TimeUnit.SECONDS);
            assertTrue(firstDelivery != null, "shipping-service's exchange never received the integration event");
            JsonNode envelope = OBJECT_MAPPER.readTree(firstDelivery);
            assertEquals(OrderPlacedIntegrationEvent.class.getName(), envelope.get("payloadClass").asText(),
                    "the only message relayed must be the public integration event");

            // 5: the rich internal OrderPlaced event was never relayed -- nothing else ever arrives.
            assertNull(monitored.poll(1, TimeUnit.SECONDS),
                    "OrderPlaced (the rich internal event) reached the broker -- it must never cross the boundary");

            // 3: a shipment now exists in shipping-service's OWN database.
            PostgresEventStoreStorage shippingStorage = new PostgresEventStoreStorage(shippingDataSource);
            waitUntil(() -> !shippingStorage.readRange(0, null, 100).isEmpty(), Duration.ofSeconds(10),
                    "shipping-service never created a shipment for the relayed integration event");
            assertEquals(1, countShipmentCreated(shippingStorage), "expected exactly one shipment after the first delivery");

            // Sourced independently: a brand-new AxonConfiguration/StateManager against the same database.
            EventStorageEngine secondShippingEngine = OpenDcbAxonPostgres.engine(shippingDataSource, false);
            AxonConfiguration secondShippingConfig =
                    ShippingService.configurer().registerEventStorageEngine(c -> secondShippingEngine).start();
            try {
                ShipmentEntity shipment = load(secondShippingConfig, orderId);
                assertEquals(orderId, shipment.orderId(),
                        "shipment re-sourced from an independent AxonConfiguration has the wrong orderId");
                assertEquals("customer-1", shipment.customerId(),
                        "shipment re-sourced from an independent AxonConfiguration has the wrong customerId");
            } finally {
                secondShippingConfig.shutdown();
            }

            // 4: redeliver the exact same integration event a second time (simulate broker redelivery)
            // by republishing the identical bytes -- the translator must dedupe on the envelope's eventId.
            try (Channel redeliveryChannel = rabbitConnection.createChannel()) {
                redeliveryChannel.basicPublish(
                        OrdersService.INTEGRATION_EXCHANGE, OrdersService.INTEGRATION_ROUTING_KEY, null, firstDelivery);
            }

            assertNeverExceedsOneShipment(shippingStorage, Duration.ofSeconds(3));
        } finally {
            rabbitConnection.close();
        }
    }

    private static void assertNeverExceedsOneShipment(PostgresEventStoreStorage shippingStorage, Duration window)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + window.toMillis();
        while (System.currentTimeMillis() < deadline) {
            int count = countShipmentCreated(shippingStorage);
            assertTrue(count <= 1, "redelivering the same integration event created a second shipment");
            Thread.sleep(100);
        }
    }

    private static int countShipmentCreated(PostgresEventStoreStorage storage) {
        return (int) storage.readRange(0, null, 100).stream()
                .map(StoredEvent::payloadClass)
                .filter(ShipmentCreated.class.getName()::equals)
                .count();
    }

    private static ShipmentEntity load(AxonConfiguration configuration, String orderId) {
        UnitOfWorkFactory unitOfWorkFactory = configuration.getComponent(UnitOfWorkFactory.class);
        return unitOfWorkFactory.create()
                .executeWithResult(ctx -> ctx.component(StateManager.class)
                        .repository(ShipmentEntity.class, String.class)
                        .load(orderId, ctx)
                        .thenApply(ManagedEntity::entity))
                .join();
    }

    private static void waitUntil(BooleanSupplier condition, Duration timeout, String failureMessage)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError(failureMessage);
    }

    private static DataSource postgresDataSource(PostgreSQLContainer<?> container) {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(container.getJdbcUrl());
        dataSource.setUser(container.getUsername());
        dataSource.setPassword(container.getPassword());
        return dataSource;
    }

    private static Connection newRabbitConnection() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBITMQ.getHost());
        factory.setPort(RABBITMQ.getAmqpPort());
        factory.setUsername(RABBITMQ.getAdminUsername());
        factory.setPassword(RABBITMQ.getAdminPassword());
        return factory.newConnection();
    }

    private static String declareAndBindQueue(Connection connection, String queueName) throws Exception {
        try (Channel setup = connection.createChannel()) {
            setup.exchangeDeclare(OrdersService.INTEGRATION_EXCHANGE, BuiltinExchangeType.DIRECT, true);
            setup.queueDeclare(queueName, true, false, false, null);
            setup.queueBind(queueName, OrdersService.INTEGRATION_EXCHANGE, OrdersService.INTEGRATION_ROUTING_KEY);
            return queueName;
        }
    }

    private static BlockingQueue<byte[]> subscribe(Connection connection, String queue) throws Exception {
        Channel consumerChannel = connection.createChannel();
        BlockingQueue<byte[]> received = new ArrayBlockingQueue<>(10);
        consumerChannel.basicConsume(queue, true, (consumerTag, delivery) -> received.add(delivery.getBody()),
                consumerTag -> { });
        return received;
    }
}
