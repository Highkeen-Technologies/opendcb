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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.highkeen.opendcb.eventstore.core.StoredEvent;
import com.highkeen.opendcb.relay.core.NonRetryablePublishException;
import com.highkeen.opendcb.relay.core.PublishException;
import com.highkeen.opendcb.relay.core.Publisher;
import com.highkeen.opendcb.relay.core.RetryablePublishException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownSignalException;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Publishes {@link StoredEvent}s to a RabbitMQ exchange using the plain
 * {@code com.rabbitmq:amqp-client} library directly — never Spring AMQP, so
 * this module stays framework-agnostic per {@code docs/ARCHITECTURE.md}.
 *
 * <p>Each event is serialized to JSON from the {@link StoredEvent} record
 * itself (its {@code payloadJson}, {@code messageType}, {@code tags}, and
 * {@code metadata} fields travel as-is) — never derived from any
 * event-sourcing framework's type. Publishing uses publisher confirms
 * ({@link Channel#waitForConfirmsOrDie(long)}) plus the {@code mandatory}
 * flag with a return listener, so an unroutable message is detected rather
 * than silently dropped.
 *
 * <p><b>Deliberate design choice — takes a {@link Connection}, not a
 * {@link Channel}:</b> the real {@code com.rabbitmq.client} implementation
 * ({@code ChannelN.waitForConfirmsOrDie}) closes the channel itself both on a
 * broker nack ({@code close(..., "NACKS RECEIVED", ...)}) and on a confirm
 * timeout ({@code close(PRECONDITION_FAILED, "TIMEOUT WAITING FOR ACK")}) —
 * confirmed by reading that method's source rather than assumed. A
 * long-lived {@link Channel} handed in at construction would therefore
 * become permanently unusable after the very first nack or timeout, turning
 * every subsequent publish into a false "retryable" failure that can never
 * actually succeed. Taking the {@link Connection} instead lets this class
 * lazily open a fresh {@link Channel} whenever the current one is missing or
 * closed, so the relay's retry-on-{@link RetryablePublishException}
 * semantics keep working across broker-side channel closures.
 *
 * <p><b>Not thread-safe beyond the internal channel handling</b> — a
 * RabbitMQ {@link Channel} is documented as usable by only one thread at a
 * time, and {@link #publish} is only ever called sequentially by a single
 * {@code OutboxRelay} poll loop, so no broader synchronization is attempted.
 */
public final class RabbitMqPublisher implements Publisher {

    private static final Duration DEFAULT_CONFIRM_TIMEOUT = Duration.ofSeconds(5);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private final Connection connection;
    private final String exchange;
    private final Function<StoredEvent, String> routingKeyResolver;
    private final long confirmTimeoutMillis;
    private final AtomicBoolean lastPublishReturned = new AtomicBoolean(false);

    private Channel channel;

    /** Publishes every event to a single fixed routing key. */
    public RabbitMqPublisher(Connection connection, String exchange, String routingKey) {
        this(connection, exchange, event -> routingKey, DEFAULT_CONFIRM_TIMEOUT);
    }

    /** Publishes each event to a routing key derived from the event itself (e.g. from its tags). */
    public RabbitMqPublisher(Connection connection, String exchange, Function<StoredEvent, String> routingKeyResolver) {
        this(connection, exchange, routingKeyResolver, DEFAULT_CONFIRM_TIMEOUT);
    }

    public RabbitMqPublisher(
            Connection connection,
            String exchange,
            Function<StoredEvent, String> routingKeyResolver,
            Duration confirmTimeout) {
        this.connection = connection;
        this.exchange = exchange;
        this.routingKeyResolver = routingKeyResolver;
        this.confirmTimeoutMillis = confirmTimeout.toMillis();
    }

    @Override
    public void publish(StoredEvent event) throws PublishException {
        String routingKey = routingKeyResolver.apply(event);
        byte[] body = serialize(event);
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .deliveryMode(2)
                .messageId(event.eventId())
                .type(event.messageType())
                .build();

        Channel ch;
        try {
            ch = channel();
        } catch (IOException | ShutdownSignalException e) {
            throw new RetryablePublishException("Failed to open a RabbitMQ channel", e);
        }

        lastPublishReturned.set(false);
        try {
            ch.basicPublish(exchange, routingKey, true, properties, body);
        } catch (IOException | ShutdownSignalException e) {
            // ShutdownSignalException (e.g. AlreadyClosedException) is an unchecked exception that the
            // real ChannelN implementation throws from ensureIsOpen() when the underlying connection/
            // channel has already gone away — confirmed by reading that method's source — so it must be
            // caught explicitly here even though Channel#basicPublish only declares IOException.
            throw new RetryablePublishException(
                    "Failed to publish event " + event.eventId() + " to RabbitMQ (connection/channel unavailable)", e);
        }

        try {
            ch.waitForConfirmsOrDie(confirmTimeoutMillis);
        } catch (TimeoutException e) {
            throw new RetryablePublishException("Timed out waiting for RabbitMQ to confirm event " + event.eventId(), e);
        } catch (ShutdownSignalException e) {
            // ChannelN#waitForConfirms(long) throws this (unchecked) directly from its wait loop when
            // getCloseReason() is non-null — i.e. the channel/connection went away for a reason other
            // than the nack path below (which throws a checked IOException instead), e.g. a dropped
            // broker connection. That's a connection/channel-unavailable case, not a broker rejection.
            throw new RetryablePublishException(
                    "RabbitMQ channel/connection was closed while waiting to confirm event " + event.eventId(), e);
        } catch (IOException e) {
            // ChannelN#waitForConfirmsOrDie(long) throws IOException("nacks received") specifically
            // when the broker nacks the message, and closes the channel itself when it does. Any
            // connection/channel-level IOException is thrown by basicPublish above instead, so an
            // IOException reaching this catch block unambiguously means the broker rejected the
            // message — a broker-side/config problem, not a transient one.
            throw new NonRetryablePublishException("RabbitMQ broker nack'd event " + event.eventId(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetryablePublishException("Interrupted while waiting for RabbitMQ to confirm event " + event.eventId(), e);
        }

        if (lastPublishReturned.get()) {
            // The broker's basic.return is delivered synchronously, before the corresponding ack, on
            // the same connection thread that just unblocked waitForConfirmsOrDie above — confirmed by
            // reading ChannelN's frame-handling source — so this flag is reliably set by the time we
            // get here. It fires only when no queue is bound to match the routing key: a routing/
            // configuration problem, not a transient one.
            throw new NonRetryablePublishException(
                    "Event " + event.eventId() + " was unroutable: no queue bound to exchange '" + exchange
                            + "' with routing key '" + routingKey + "'");
        }
    }

    private synchronized Channel channel() throws IOException {
        if (channel == null || !channel.isOpen()) {
            channel = connection.createChannel();
            channel.confirmSelect();
            channel.addReturnListener((replyCode, replyText, returnExchange, returnRoutingKey, properties, body) ->
                    lastPublishReturned.set(true));
        }
        return channel;
    }

    private static byte[] serialize(StoredEvent event) {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize StoredEvent " + event.eventId() + " to JSON", e);
        }
    }
}
