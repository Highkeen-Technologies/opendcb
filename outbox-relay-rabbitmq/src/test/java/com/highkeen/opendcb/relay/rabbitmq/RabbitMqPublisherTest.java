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
import com.highkeen.opendcb.eventstore.core.StoredEvent;
import com.highkeen.opendcb.relay.core.NonRetryablePublishException;
import com.highkeen.opendcb.relay.core.RetryablePublishException;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class RabbitMqPublisherTest {

    @Container
    private static final RabbitMQContainer RABBITMQ = new RabbitMQContainer("rabbitmq:3.13-management");

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private Connection connection;

    @AfterEach
    void closeConnection() throws Exception {
        if (connection != null && connection.isOpen()) {
            connection.close();
        }
    }

    @Test
    void publishedEventIsReceivedByARealConsumerViaTheBoundQueue() throws Exception {
        connection = newConnection();
        String exchange = "events-exchange-" + UUID.randomUUID();
        String routingKey = "orders.created";
        String queue = declareExchangeQueueAndBinding(connection, exchange, routingKey);

        RabbitMqPublisher publisher = new RabbitMqPublisher(connection, exchange, routingKey);
        StoredEvent event = event(1L, "OrderCreated", "{\"orderId\":\"o-1\"}");

        publisher.publish(event);

        BlockingQueue<byte[]> received = subscribe(connection, queue);
        byte[] body = received.poll(5, TimeUnit.SECONDS);
        assertTrue(body != null, "consumer did not receive the published message");

        JsonNode json = OBJECT_MAPPER.readTree(body);
        assertEquals("event-1", json.get("eventId").asText());
        assertEquals("OrderCreated", json.get("messageType").asText());
        assertEquals("{\"orderId\":\"o-1\"}", json.get("payloadJson").asText());
    }

    @Test
    void brokerUnavailableThrowsRetryablePublishException() throws Exception {
        connection = newConnection();
        // Close the connection ourselves to simulate the broker having become unreachable: any
        // further attempt to open a channel or publish on it must fail as "connection unavailable".
        connection.close();

        RabbitMqPublisher publisher = new RabbitMqPublisher(connection, "any-exchange", "any-key");
        StoredEvent event = event(1L, "OrderCreated", "{}");

        assertThrows(RetryablePublishException.class, () -> publisher.publish(event));
    }

    @Test
    void unroutableMessageThrowsNonRetryablePublishException() throws Exception {
        connection = newConnection();
        String exchange = "events-exchange-" + UUID.randomUUID();
        try (Channel setup = connection.createChannel()) {
            setup.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT, true);
            // Deliberately no queue bound to this exchange/routing key combination.
        }

        RabbitMqPublisher publisher = new RabbitMqPublisher(connection, exchange, "no-queue-bound-to-this-key");
        StoredEvent event = event(1L, "OrderCreated", "{}");

        assertThrows(NonRetryablePublishException.class, () -> publisher.publish(event));
    }

    private static Connection newConnection() throws Exception {
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

    private static StoredEvent event(long position, String messageType, String payloadJson) {
        return new StoredEvent(
                position,
                "event-" + position,
                messageType,
                String.class.getName(),
                payloadJson,
                Map.of("traceId", "trace-" + position),
                Set.of(new StoredEvent.StoredTag("orderId", "o-" + position)),
                Instant.now());
    }
}
