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
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * The anti-corruption layer between orders-service and shipping-service: receives the raw published
 * envelope for an {@code OrderPlacedIntegrationEvent} (as JSON bytes off a real RabbitMQ queue, via
 * {@link IntegrationEventConsumer}) and translates it into a local {@link CreateShipmentCommand}
 * dispatched against this service's own entity/event store. It never treats the incoming payload as
 * this service's own domain event — {@link ShipmentCreated} is a distinct type this service defines
 * itself, only ever produced by {@link ShipmentCommandHandlers} in response to that local command.
 *
 * <p>Since relay + broker delivery is at-least-once, redelivery of the exact same integration event
 * must not create a second shipment. This is enforced with a unique constraint on the envelope's
 * {@code eventId} (see {@link #ensureSchema()}) — the row is inserted <em>before</em> the shipment
 * command is dispatched, so a duplicate delivery fails the insert and returns without dispatching
 * anything.
 */
public final class OrderPlacedIntegrationEventTranslator {

    private final CommandGateway commandGateway;
    private final DataSource dataSource;
    private final ObjectMapper objectMapper;

    public OrderPlacedIntegrationEventTranslator(CommandGateway commandGateway, DataSource dataSource) {
        this.commandGateway = commandGateway;
        this.dataSource = dataSource;
        this.objectMapper = new ObjectMapper();
    }

    public void ensureSchema() {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS processed_integration_events (
                        event_id VARCHAR(255) PRIMARY KEY
                    )
                    """);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to create processed_integration_events schema", e);
        }
    }

    /**
     * Translates a raw published integration-event envelope (the JSON body a {@link
     * com.highkeen.opendcb.relay.rabbitmq.RabbitMqPublisher} publishes for a {@code StoredEvent}) into a
     * dispatched {@link CreateShipmentCommand}, deduping on the envelope's {@code eventId} so redelivery
     * of the same message never creates a second shipment.
     */
    public void handle(byte[] envelopeJson) {
        JsonNode envelope;
        try {
            envelope = objectMapper.readTree(envelopeJson);
        } catch (IOException e) {
            throw new IllegalStateException("Malformed integration event envelope", e);
        }

        String eventId = envelope.get("eventId").asText();
        if (!tryMarkProcessed(eventId)) {
            return;
        }

        JsonNode payload;
        try {
            payload = objectMapper.readTree(envelope.get("payloadJson").asText());
        } catch (IOException e) {
            throw new IllegalStateException("Malformed integration event payload", e);
        }

        commandGateway.sendAndWait(
                new CreateShipmentCommand(payload.get("orderId").asText(), payload.get("customerId").asText()));
    }

    /** @return {@code true} if this is the first time {@code eventId} has been seen, {@code false} if it's a duplicate. */
    private boolean tryMarkProcessed(String eventId) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement =
                     connection.prepareStatement("INSERT INTO processed_integration_events (event_id) VALUES (?)")) {
            statement.setString(1, eventId);
            statement.executeUpdate();
            return true;
        } catch (SQLException e) {
            if (isUniqueViolation(e)) {
                return false;
            }
            throw new IllegalStateException("Failed to record processed integration event " + eventId, e);
        }
    }

    private static boolean isUniqueViolation(SQLException e) {
        return "23505".equals(e.getSQLState());
    }
}
