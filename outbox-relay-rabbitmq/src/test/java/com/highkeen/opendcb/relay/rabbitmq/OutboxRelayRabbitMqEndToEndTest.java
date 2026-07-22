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
package com.highkeen.opendcb.relay.rabbitmq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.highkeen.opendcb.eventstore.core.EventStoreStorage;
import com.highkeen.opendcb.eventstore.core.StoredEvent;
import com.highkeen.opendcb.eventstore.postgres.PostgresEventStoreStorage;
import com.highkeen.opendcb.relay.core.JdbcRelayPositionStore;
import com.highkeen.opendcb.relay.core.LoggingDeadLetterSink;
import com.highkeen.opendcb.relay.core.OutboxRelay;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * First true end-to-end proof of the cross-bounded-context flow: events appended to a real
 * PostgreSQL {@link EventStoreStorage}, relayed by a real {@link OutboxRelay}, published to a
 * real RabbitMQ broker via {@link RabbitMqPublisher}, and received in order by a real consumer.
 */
@Testcontainers
class OutboxRelayRabbitMqEndToEndTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    @Container
    private static final RabbitMQContainer RABBITMQ = new RabbitMQContainer("rabbitmq:3.13-management");

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void eventsAppendedToPostgresAreRelayedToRabbitMqInOrder() throws Exception {
        DataSource postgresDataSource = newPostgresDataSource();
        PostgresEventStoreStorage storage = new PostgresEventStoreStorage(postgresDataSource);
        storage.ensureSchema();

        for (int i = 1; i <= 5; i++) {
            storage.appendAtomically(List.of(event(i)), storage.maxPosition(), ignored -> false);
        }

        Connection rabbitConnection = newRabbitConnection();
        try {
            String exchange = "orders-exchange-" + UUID.randomUUID();
            String routingKey = "orders.created";
            String queue = declareExchangeQueueAndBinding(rabbitConnection, exchange, routingKey);
            BlockingQueue<byte[]> received = subscribe(rabbitConnection, queue);

            RabbitMqPublisher publisher = new RabbitMqPublisher(rabbitConnection, exchange, routingKey);
            JdbcRelayPositionStore positionStore = new JdbcRelayPositionStore(h2DataSource());
            positionStore.ensureSchema();

            OutboxRelay relay = new OutboxRelay(
                    storage, publisher, positionStore, new LoggingDeadLetterSink(), "orders-relay", 100);

            relay.runOnce();

            List<String> receivedEventIds = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                byte[] body = received.poll(5, TimeUnit.SECONDS);
                assertTrue(body != null, "did not receive message " + (i + 1) + " of 5 within timeout");
                JsonNode json = OBJECT_MAPPER.readTree(body);
                receivedEventIds.add(json.get("eventId").asText());
            }

            assertEquals(List.of("event-1", "event-2", "event-3", "event-4", "event-5"), receivedEventIds);
            assertEquals(5L, positionStore.getLastRelayedPosition("orders-relay"));
        } finally {
            rabbitConnection.close();
        }
    }

    private static DataSource newPostgresDataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(POSTGRES.getJdbcUrl());
        dataSource.setUser(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        return dataSource;
    }

    private static DataSource h2DataSource() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:relay-e2e-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        dataSource.setPassword("");
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

    private static String declareExchangeQueueAndBinding(Connection connection, String exchange, String routingKey)
            throws Exception {
        try (Channel setup = connection.createChannel()) {
            setup.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT, true);
            String queue = setup.queueDeclare().getQueue();
            setup.queueBind(queue, exchange, routingKey);
            return queue;
        }
    }

    private static BlockingQueue<byte[]> subscribe(Connection connection, String queue) throws Exception {
        Channel consumerChannel = connection.createChannel();
        BlockingQueue<byte[]> received = new ArrayBlockingQueue<>(10);
        consumerChannel.basicConsume(queue, true, (consumerTag, delivery) -> received.add(delivery.getBody()),
                consumerTag -> { });
        return received;
    }

    private static StoredEvent event(long seed) {
        return new StoredEvent(
                0L,
                "event-" + seed,
                "OrderCreated",
                String.class.getName(),
                "{\"orderId\":\"o-" + seed + "\"}",
                Map.of(),
                Set.of(),
                Instant.now());
    }
}
