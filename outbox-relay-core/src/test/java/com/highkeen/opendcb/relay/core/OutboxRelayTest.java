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
package com.highkeen.opendcb.relay.core;

import com.highkeen.opendcb.eventstore.core.StoredEvent;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class OutboxRelayTest {

    private final InMemoryEventStoreStorage storage = new InMemoryEventStoreStorage();
    private final RecordingPublisher publisher = new RecordingPublisher();
    private final RecordingDeadLetterSink deadLetterSink = new RecordingDeadLetterSink();
    private final InMemoryRelayPositionStore positionStore = new InMemoryRelayPositionStore();
    private static final String RELAY_NAME = "test-relay";

    @Test
    void publishesEventsInOrder() {
        appendEvents(5);
        OutboxRelay relay = new OutboxRelay(storage, publisher, positionStore, deadLetterSink, RELAY_NAME, 100);

        relay.runOnce();

        assertEquals(List.of(1L, 2L, 3L, 4L, 5L), positions(publisher.published()));
        assertEquals(5L, positionStore.getLastRelayedPosition(RELAY_NAME));
    }

    @Test
    void resumesFromCorrectPositionAfterRestart() {
        appendEvents(3);
        JdbcDataSource dataSource = h2DataSource();
        JdbcRelayPositionStore firstPositionStore = new JdbcRelayPositionStore(dataSource);
        firstPositionStore.ensureSchema();

        OutboxRelay firstRelay = new OutboxRelay(storage, publisher, firstPositionStore, deadLetterSink, RELAY_NAME, 100);
        firstRelay.runOnce();
        assertEquals(List.of(1L, 2L, 3L), positions(publisher.published()));

        appendEvents(2);
        // A brand-new RelayPositionStore instance against the same DataSource, and a brand-new OutboxRelay —
        // neither holds any in-process memory of the first relay's run. If the position only lived in a Java
        // field rather than the database, this would re-read from -1 and republish events 1-3.
        RecordingPublisher secondPublisher = new RecordingPublisher();
        JdbcRelayPositionStore secondPositionStore = new JdbcRelayPositionStore(dataSource);
        OutboxRelay secondRelay = new OutboxRelay(storage, secondPublisher, secondPositionStore, deadLetterSink, RELAY_NAME, 100);

        secondRelay.runOnce();

        assertEquals(List.of(4L, 5L), positions(secondPublisher.published()));
        assertEquals(5L, secondPositionStore.getLastRelayedPosition(RELAY_NAME));
    }

    @Test
    void retryablePublishExceptionStopsBatchAndRetriesSameEventNext() {
        appendEvents(5);
        RetryablePublishException failure = new RetryablePublishException("transport unavailable");
        publisher.failAt(3, failure);
        OutboxRelay relay = new OutboxRelay(storage, publisher, positionStore, deadLetterSink, RELAY_NAME, 100);

        relay.runOnce();

        assertEquals(List.of(1L, 2L), positions(publisher.published()));
        assertEquals(2L, positionStore.getLastRelayedPosition(RELAY_NAME));
        assertTrue(deadLetterSink.deadLetteredEvents().isEmpty());

        publisher.clearFailure(3);
        relay.runOnce();

        assertEquals(List.of(1L, 2L, 3L, 4L, 5L), positions(publisher.published()));
        assertEquals(5L, positionStore.getLastRelayedPosition(RELAY_NAME));
    }

    @Test
    void nonRetryablePublishExceptionDeadLettersAndAdvancesPastIt() {
        appendEvents(5);
        NonRetryablePublishException failure = new NonRetryablePublishException("serialization failed");
        publisher.failAt(3, failure);
        OutboxRelay relay = new OutboxRelay(storage, publisher, positionStore, deadLetterSink, RELAY_NAME, 100);

        relay.runOnce();

        assertEquals(List.of(1L, 2L, 4L, 5L), positions(publisher.published()));
        assertEquals(1, deadLetterSink.deadLetteredEvents().size());
        assertEquals(3L, deadLetterSink.deadLetteredEvents().get(0).position());
        assertSame(failure, deadLetterSink.causes().get(0));
        assertEquals(5L, positionStore.getLastRelayedPosition(RELAY_NAME));
    }

    @Test
    void filteredOutEventNeverReachesPublisherButPositionAdvancesPastIt() {
        appendEvents(5);
        OutboxRelay relay = new OutboxRelay(
                storage, publisher, positionStore, deadLetterSink, RELAY_NAME, 100, event -> event.position() != 3L);

        relay.runOnce();

        assertEquals(List.of(1L, 2L, 4L, 5L), positions(publisher.published()));
        assertTrue(deadLetterSink.deadLetteredEvents().isEmpty());
        assertEquals(5L, positionStore.getLastRelayedPosition(RELAY_NAME));
    }

    @Test
    void startPollsInBackgroundUntilStopped() throws InterruptedException {
        appendEvents(3);
        OutboxRelay relay = new OutboxRelay(storage, publisher, positionStore, deadLetterSink, RELAY_NAME, 100);

        relay.start(Duration.ofMillis(20));
        try {
            awaitUntil(() -> publisher.published().size() == 3, Duration.ofSeconds(2));
        } finally {
            relay.stop();
        }
        assertEquals(List.of(1L, 2L, 3L), positions(publisher.published()));

        appendEvents(1);
        Thread.sleep(100);
        assertEquals(3, publisher.published().size(), "no further polls should run once stopped");
    }

    private static void awaitUntil(java.util.function.BooleanSupplier condition, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail("condition not met within " + timeout);
            }
            Thread.sleep(10);
        }
    }

    private void appendEvents(int count) {
        for (int i = 0; i < count; i++) {
            storage.appendAtomically(List.of(event()), storage.maxPosition(), ignored -> false);
        }
    }

    private static StoredEvent event() {
        String id = UUID.randomUUID().toString();
        return new StoredEvent(
                0L,
                id,
                "TestEvent",
                String.class.getName(),
                "\"payload-" + id + "\"",
                Map.of(),
                Set.of(),
                Instant.now());
    }

    private static List<Long> positions(List<StoredEvent> events) {
        return events.stream().map(StoredEvent::position).toList();
    }

    private static JdbcDataSource h2DataSource() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:relay-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        dataSource.setPassword("");
        return dataSource;
    }

    /** Plain in-memory {@link RelayPositionStore} test double for tests that don't need to prove durability. */
    private static final class InMemoryRelayPositionStore implements RelayPositionStore {
        private final Map<String, Long> positions = new HashMap<>();

        @Override
        public long getLastRelayedPosition(String relayName) {
            return positions.getOrDefault(relayName, -1L);
        }

        @Override
        public void setLastRelayedPosition(String relayName, long position) {
            positions.put(relayName, position);
        }
    }
}
