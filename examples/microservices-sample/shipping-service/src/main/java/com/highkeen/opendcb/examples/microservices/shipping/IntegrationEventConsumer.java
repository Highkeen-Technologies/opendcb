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

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;

/**
 * A real RabbitMQ consumer — plain {@code com.rabbitmq:amqp-client}, no Spring AMQP, consistent with
 * {@code outbox-relay-rabbitmq}'s framework-agnostic transport choice. Declares its own durable queue
 * bound to orders-service's integration exchange/routing key, and hands every delivery's raw body to
 * {@link OrderPlacedIntegrationEventTranslator} — this consumer never interprets the message itself,
 * translation is entirely the translator's job.
 */
public final class IntegrationEventConsumer {

    private final Connection connection;
    private final String exchange;
    private final String routingKey;
    private final String queueName;
    private final OrderPlacedIntegrationEventTranslator translator;

    public IntegrationEventConsumer(
            Connection connection,
            String exchange,
            String routingKey,
            String queueName,
            OrderPlacedIntegrationEventTranslator translator) {
        this.connection = connection;
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.queueName = queueName;
        this.translator = translator;
    }

    /** Declares the exchange/queue/binding and starts consuming; returns the consumer tag. */
    public String start() throws IOException {
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT, true);
        channel.queueDeclare(queueName, true, false, false, null);
        channel.queueBind(queueName, exchange, routingKey);
        return channel.basicConsume(queueName, true,
                (consumerTag, delivery) -> translator.handle(delivery.getBody()),
                consumerTag -> { });
    }
}
