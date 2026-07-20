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
package com.highkeen.opendcb.eventstore.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.highkeen.opendcb.eventstore.core.EventStoreStorage.ConcurrentAppendConflictException;
import com.highkeen.opendcb.eventstore.core.StoredEvent.StoredTag;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Shared contract every {@link EventStoreStorage} provider must satisfy before it can be
 * considered merge-ready — see {@code docs/TESTING.md}. Packaged as part of {@code
 * eventstore-core}'s test-jar so provider modules can extend it without duplicating test code or
 * pulling any framework dependency into a framework-agnostic module.
 *
 * <p>Every test method assumes the backing store is empty when it starts. Subclasses are
 * responsible for resetting storage between tests (e.g. truncating tables in a {@code
 * @BeforeEach} method) and for providing a real, provider-specific backing store (e.g. a
 * Testcontainers instance).
 */
public abstract class EventStoreStorageContractTest {

    /** The {@link EventStoreStorage} under test. Must point at an empty backing store at the start of each test. */
    protected abstract EventStoreStorage storage();

    /**
     * A second, independently constructed {@link EventStoreStorage} pointed at the exact same
     * backing store as {@link #storage()} — simulates a second JVM/connection pool talking to the
     * same database, for the concurrent cross-JVM safety test below.
     */
    protected abstract EventStoreStorage anotherStorageInstance();

    @Test
    void appendThenReadRangeReturnsEventsInOrderWithTagsAndMetadataIntact() {
        EventStoreStorage storage = storage();
        StoredEvent first = newEvent(
                "evt-1", Set.of(new StoredTag("orderId", "O-1")), Map.of("traceId", "t-1"),
                Instant.parse("2026-01-01T00:00:00Z"));
        StoredEvent second = newEvent(
                "evt-2", Set.of(new StoredTag("orderId", "O-2")), Map.of("traceId", "t-2"),
                Instant.parse("2026-01-01T00:00:01Z"));

        long lastPosition = storage.appendAtomically(List.of(first, second), storage.maxPosition(), event -> false);

        List<StoredEvent> events = storage.readRange(0L, null, 100);
        assertEquals(2, events.size());
        assertTrue(events.get(0).position() < events.get(1).position());
        assertEquals(lastPosition, events.get(1).position());

        StoredEvent readFirst = events.get(0);
        assertEquals("evt-1", readFirst.eventId());
        assertEquals(Set.of(new StoredTag("orderId", "O-1")), readFirst.tags());
        assertEquals(Map.of("traceId", "t-1"), readFirst.metadata());
        assertEquals(first.messageType(), readFirst.messageType());
        assertEquals(first.payloadClass(), readFirst.payloadClass());
        assertEquals(first.payloadJson(), readFirst.payloadJson());
        assertEquals(first.timestamp(), readFirst.timestamp());

        StoredEvent readSecond = events.get(1);
        assertEquals("evt-2", readSecond.eventId());
        assertEquals(Set.of(new StoredTag("orderId", "O-2")), readSecond.tags());
        assertEquals(Map.of("traceId", "t-2"), readSecond.metadata());
    }

    @Test
    void secondConflictingAppendThrowsAndLeavesNoPartialWrite() {
        EventStoreStorage storage = storage();
        StoredTag tag = new StoredTag("orderId", "O-1");
        StoredEvent first = newEvent("evt-1", Set.of(tag), Map.of(), Instant.parse("2026-01-01T00:00:00Z"));
        storage.appendAtomically(List.of(first), storage.maxPosition(), event -> false);

        StoredEvent conflicting = newEvent("evt-2", Set.of(tag), Map.of(), Instant.parse("2026-01-01T00:00:01Z"));
        assertThrows(
                ConcurrentAppendConflictException.class,
                () -> storage.appendAtomically(List.of(conflicting), 0L, event -> event.tags().contains(tag)));

        List<StoredEvent> events = storage.readRange(0L, null, 100);
        assertEquals(1, events.size());
        assertEquals("evt-1", events.get(0).eventId());
    }

    @Test
    void concurrentAppendsWithOverlappingConflictPredicateAllowExactlyOneSuccess() throws Exception {
        EventStoreStorage storageA = storage();
        EventStoreStorage storageB = anotherStorageInstance();
        StoredTag tag = new StoredTag("orderId", "O-race");

        StoredEvent candidateA = newEvent("evt-a", Set.of(tag), Map.of(), Instant.parse("2026-01-01T00:00:00Z"));
        StoredEvent candidateB = newEvent("evt-b", Set.of(tag), Map.of(), Instant.parse("2026-01-01T00:00:00Z"));

        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        boolean succeededA;
        boolean succeededB;
        try {
            Future<Boolean> futureA = executor.submit(raceTask(storageA, candidateA, tag, ready, go));
            Future<Boolean> futureB = executor.submit(raceTask(storageB, candidateB, tag, ready, go));

            assertTrue(ready.await(10, TimeUnit.SECONDS), "both racing threads should reach the start line");
            go.countDown();

            succeededA = futureA.get(30, TimeUnit.SECONDS);
            succeededB = futureB.get(30, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        assertTrue(succeededA ^ succeededB, "exactly one of the two racing appends must succeed");

        List<StoredEvent> events = storage().readRange(0L, null, 100);
        long taggedCount = events.stream().filter(event -> event.tags().contains(tag)).count();
        assertEquals(1, taggedCount, "only the winning append's event should be persisted");
    }

    private static Callable<Boolean> raceTask(
            EventStoreStorage storage, StoredEvent candidate, StoredTag tag, CountDownLatch ready, CountDownLatch go) {
        return () -> {
            ready.countDown();
            go.await();
            try {
                storage.appendAtomically(List.of(candidate), 0L, event -> event.tags().contains(tag));
                return true;
            } catch (ConcurrentAppendConflictException e) {
                return false;
            }
        };
    }

    @Test
    void positionMethodsOnEmptyStore() {
        EventStoreStorage storage = storage();
        assertEquals(0L, storage.minPosition());
        assertEquals(0L, storage.maxPosition());
        assertTrue(storage.positionAtOrAfter(Instant.EPOCH).isEmpty());
    }

    @Test
    void positionMethodsOnSingleEventStore() {
        EventStoreStorage storage = storage();
        Instant timestamp = Instant.parse("2026-01-01T00:00:00Z");
        StoredEvent event = newEvent("evt-1", Set.of(), Map.of(), timestamp);
        long position = storage.appendAtomically(List.of(event), storage.maxPosition(), candidate -> false);

        assertEquals(position, storage.minPosition());
        assertEquals(position, storage.maxPosition());
        assertEquals(Optional.of(position), storage.positionAtOrAfter(timestamp));
        assertEquals(Optional.of(position), storage.positionAtOrAfter(timestamp.minusSeconds(1)));
        assertTrue(storage.positionAtOrAfter(timestamp.plusSeconds(1)).isEmpty());
    }

    @Test
    void positionMethodsOnMultiEventStore() {
        EventStoreStorage storage = storage();
        Instant t1 = Instant.parse("2026-01-01T00:00:00Z");
        Instant t2 = Instant.parse("2026-01-01T00:00:10Z");
        Instant t3 = Instant.parse("2026-01-01T00:00:20Z");

        long p1 = storage.appendAtomically(
                List.of(newEvent("evt-1", Set.of(), Map.of(), t1)), storage.maxPosition(), candidate -> false);
        long p2 = storage.appendAtomically(
                List.of(newEvent("evt-2", Set.of(), Map.of(), t2)), storage.maxPosition(), candidate -> false);
        long p3 = storage.appendAtomically(
                List.of(newEvent("evt-3", Set.of(), Map.of(), t3)), storage.maxPosition(), candidate -> false);

        assertEquals(p1, storage.minPosition());
        assertEquals(p3, storage.maxPosition());
        assertEquals(Optional.of(p1), storage.positionAtOrAfter(t1.minusSeconds(1)));
        assertEquals(Optional.of(p2), storage.positionAtOrAfter(t2));
        assertEquals(Optional.of(p3), storage.positionAtOrAfter(t2.plusSeconds(1)));
        assertTrue(storage.positionAtOrAfter(t3.plusSeconds(1)).isEmpty());
    }

    @Test
    void concurrentNonConflictingAppendsFromDifferentStreamsInterleaveByPositionWithoutReordering() throws Exception {
        EventStoreStorage storageA = storage();
        EventStoreStorage storageB = anotherStorageInstance();
        int eventsPerStream = 8;

        ExecutorService executor = Executors.newFixedThreadPool(2);
        List<Long> positionsA;
        List<Long> positionsB;
        try {
            Future<List<Long>> futureA = executor.submit(appendSequentially(storageA, "stream-a", eventsPerStream));
            Future<List<Long>> futureB = executor.submit(appendSequentially(storageB, "stream-b", eventsPerStream));

            positionsA = futureA.get(30, TimeUnit.SECONDS);
            positionsB = futureB.get(30, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        assertStrictlyIncreasing(positionsA);
        assertStrictlyIncreasing(positionsB);

        List<StoredEvent> events = storage().readRange(0L, null, 100);
        assertEquals(eventsPerStream * 2, events.size());
        assertStrictlyIncreasing(events.stream().map(StoredEvent::position).toList());

        List<String> orderedA = events.stream()
                .filter(event -> event.tags().contains(new StoredTag("streamId", "stream-a")))
                .map(StoredEvent::eventId)
                .toList();
        List<String> orderedB = events.stream()
                .filter(event -> event.tags().contains(new StoredTag("streamId", "stream-b")))
                .map(StoredEvent::eventId)
                .toList();
        assertEquals(expectedEventIds("stream-a", eventsPerStream), orderedA);
        assertEquals(expectedEventIds("stream-b", eventsPerStream), orderedB);
    }

    private static Callable<List<Long>> appendSequentially(EventStoreStorage storage, String streamId, int count) {
        return () -> {
            List<Long> positions = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                StoredEvent event = newEvent(
                        streamId + "-" + i,
                        Set.of(new StoredTag("streamId", streamId)),
                        Map.of(),
                        Instant.parse("2026-01-01T00:00:00Z").plusSeconds(i));
                positions.add(storage.appendAtomically(List.of(event), storage.maxPosition(), candidate -> false));
            }
            return positions;
        };
    }

    private static List<String> expectedEventIds(String streamId, int count) {
        List<String> ids = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ids.add(streamId + "-" + i);
        }
        return ids;
    }

    private static void assertStrictlyIncreasing(List<Long> values) {
        for (int i = 1; i < values.size(); i++) {
            assertTrue(values.get(i) > values.get(i - 1), "positions must be strictly increasing: " + values);
        }
    }

    private static StoredEvent newEvent(
            String eventId, Set<StoredTag> tags, Map<String, String> metadata, Instant timestamp) {
        return new StoredEvent(
                0L,
                eventId,
                "TestEvent",
                "com.highkeen.opendcb.eventstore.core.EventStoreStorageContractTest$TestPayload",
                "{}",
                metadata,
                tags,
                timestamp);
    }
}
