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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.highkeen.opendcb.eventstore.postgres.PostgresEventStoreStorage;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Proves {@link OrderPlacedIntegrationEventTranslator}'s redelivery dedup holds under TRUE
 * concurrency, not just sequential redelivery. Two independent translator instances — standing in
 * for two consumer threads or two JVMs racing a redelivery — attempt to translate the exact same
 * integration event envelope at the same instant, forced into the race by a {@link CountDownLatch}
 * start gate, the same pattern used by eventstore-postgres's advisory-lock conflict test and
 * opendcb-axon-spring-boot-routing's {@code JdbcTokenStoreClaimConflictTest}.
 *
 * <p>This is the case a purely sequential redelivery test cannot rule out: if dedup were a
 * check-then-act (a {@code SELECT} to see whether {@code eventId} was already processed, followed
 * by an {@code INSERT}) instead of a single {@code INSERT} relying on the database's own primary-key
 * constraint, two concurrent calls could both pass the check before either commits, producing two
 * shipments. {@link OrderPlacedIntegrationEventTranslator#handle} never does a separate check — it
 * attempts the {@code INSERT} directly and treats a unique-violation as "already processed", so
 * Postgres itself serializes the race.
 */
@Testcontainers
class OrderPlacedIntegrationEventTranslatorConcurrentRedeliveryTest {

    @Container
    private static final PostgreSQLContainer<?> SHIPPING_POSTGRES = new PostgreSQLContainer<>("postgres:16");

    @Test
    void concurrentRedeliveryOfTheSameIntegrationEventProducesExactlyOneShipment() throws Exception {
        DataSource dataSource = postgresDataSource(SHIPPING_POSTGRES);
        AxonConfiguration shippingConfig = ShippingService.start(dataSource);
        try {
            CommandGateway commandGateway = shippingConfig.getComponent(CommandGateway.class);

            OrderPlacedIntegrationEventTranslator translatorA =
                    new OrderPlacedIntegrationEventTranslator(commandGateway, dataSource);
            OrderPlacedIntegrationEventTranslator translatorB =
                    new OrderPlacedIntegrationEventTranslator(commandGateway, dataSource);
            translatorA.ensureSchema();

            String orderId = "order-" + UUID.randomUUID();
            byte[] envelope = buildEnvelope("event-" + UUID.randomUUID(), orderId, "customer-1");

            CountDownLatch bothReady = new CountDownLatch(2);
            CountDownLatch start = new CountDownLatch(1);

            Thread threadA = new Thread(() -> deliver(translatorA, envelope, bothReady, start), "redelivery-A");
            Thread threadB = new Thread(() -> deliver(translatorB, envelope, bothReady, start), "redelivery-B");
            threadA.start();
            threadB.start();

            bothReady.await();
            start.countDown();

            threadA.join(10_000);
            threadB.join(10_000);
            assertEquals(false, threadA.isAlive(), "redelivery-A did not finish within 10s");
            assertEquals(false, threadB.isAlive(), "redelivery-B did not finish within 10s");

            PostgresEventStoreStorage shippingStorage = new PostgresEventStoreStorage(dataSource);
            long shipmentCount = shippingStorage.readRange(0, null, 100).stream()
                    .filter(event -> ShipmentCreated.class.getName().equals(event.payloadClass()))
                    .count();
            assertEquals(1, shipmentCount,
                    "two truly concurrent deliveries of the same integration event produced more (or fewer) "
                            + "than exactly one shipment -- the dedup is not race-proof");
        } finally {
            shippingConfig.shutdown();
        }
    }

    private static void deliver(
            OrderPlacedIntegrationEventTranslator translator,
            byte[] envelope,
            CountDownLatch bothReady,
            CountDownLatch start) {
        bothReady.countDown();
        try {
            start.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        translator.handle(envelope);
    }

    private static byte[] buildEnvelope(String eventId, String orderId, String customerId) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode payload = mapper.createObjectNode();
        payload.put("orderId", orderId);
        payload.put("customerId", customerId);

        ObjectNode envelope = mapper.createObjectNode();
        envelope.put("eventId", eventId);
        envelope.put("payloadJson", mapper.writeValueAsString(payload));

        return mapper.writeValueAsBytes(envelope);
    }

    private static DataSource postgresDataSource(PostgreSQLContainer<?> container) {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(container.getJdbcUrl());
        dataSource.setUser(container.getUsername());
        dataSource.setPassword(container.getPassword());
        return dataSource;
    }
}
