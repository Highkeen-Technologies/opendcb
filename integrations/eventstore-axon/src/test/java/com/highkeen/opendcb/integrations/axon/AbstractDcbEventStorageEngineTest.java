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
package com.highkeen.opendcb.integrations.axon;

import com.highkeen.opendcb.eventstore.core.EventStoreStorage;
import com.highkeen.opendcb.eventstore.core.StoredEvent;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.AppendEventsTransactionRejectedException;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine.AppendTransaction;
import org.axonframework.eventsourcing.eventstore.GlobalIndexConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AbstractDcbEventStorageEngineTest {

    private final InMemoryEventStoreStorage storage = new InMemoryEventStoreStorage();
    private final AbstractDcbEventStorageEngine engine =
            new AbstractDcbEventStorageEngine(storage, new JacksonConverter());

    @Test
    void appendEvents_persistsThroughStorage_andYieldsConsistencyMarker() throws Exception {
        TaggedEventMessage<EventMessage> tagged = taggedEvent("SomeEvent", "orderId", "order-1");

        AppendTransaction<?> transaction = engine.appendEvents(AppendCondition.none(), null, List.of(tagged)).get();
        Object commitResult = transaction.commit().get();
        ConsistencyMarker marker = transaction.afterCommit(uncheckedCast(commitResult)).get();

        assertEquals(1L, GlobalIndexConsistencyMarker.position(marker));
        assertEquals(1, storage.events.size());
        assertEquals("SomeEvent", MessageType.fromString(storage.events.get(0).messageType()).qualifiedName().name());
    }

    @Test
    void appendEvents_conflict_rejectsTransaction() {
        storage.events.add(storedEvent(1, "SomeEvent", "orderId", "order-1"));
        AppendCondition condition = AppendCondition.withCriteria(EventCriteria.havingTags(Tag.of("orderId", "order-1")));
        TaggedEventMessage<EventMessage> tagged = taggedEvent("AnotherEvent", "orderId", "order-1");

        ExecutionException executionException = assertThrows(ExecutionException.class, () -> {
            AppendTransaction<?> transaction = engine.appendEvents(condition, null, List.of(tagged)).get();
            transaction.commit().get();
        });

        assertInstanceOf(AppendEventsTransactionRejectedException.class, executionException.getCause());
        assertTrue(storage.events.size() == 1, "conflicting append must not have written a second event");
    }

    @Test
    void source_returnsMatchingEventsInOrder_terminatedByConsistencyMarker() {
        storage.events.add(storedEvent(1, "OrderPlaced", "orderId", "order-1"));
        storage.events.add(storedEvent(2, "OrderPlaced", "orderId", "order-2"));
        storage.events.add(storedEvent(3, "OrderShipped", "orderId", "order-1"));

        SourcingCondition condition =
                SourcingCondition.conditionFor(EventCriteria.havingTags(Tag.of("orderId", "order-1")));
        MessageStream<EventMessage> stream = engine.source(condition);

        List<String> types = new ArrayList<>();
        Optional<MessageStream.Entry<EventMessage>> entry;
        ConsistencyMarker terminalMarker = null;
        while ((entry = stream.next()).isPresent()) {
            EventMessage message = entry.get().message();
            if (message.payload() == null) {
                terminalMarker = entry.get().getResource(ConsistencyMarker.RESOURCE_KEY);
                break;
            }
            types.add(message.type().qualifiedName().name());
        }

        assertEquals(List.of("OrderPlaced", "OrderShipped"), types);
        assertEquals(3L, GlobalIndexConsistencyMarker.position(terminalMarker));
    }

    @Test
    void stream_pollsStorageForNewlyAppendedEvents() {
        storage.events.add(storedEvent(1, "OrderPlaced", "orderId", "order-1"));

        StreamingCondition condition = StreamingCondition.startingFrom(new GlobalSequenceTrackingToken(0));
        MessageStream<EventMessage> stream = engine.stream(condition);

        assertTrue(stream.hasNextAvailable());
        assertTrue(stream.next().isPresent());
        assertFalse(stream.hasNextAvailable());

        storage.events.add(storedEvent(2, "OrderShipped", "orderId", "order-1"));

        assertTrue(stream.hasNextAvailable());
        Optional<MessageStream.Entry<EventMessage>> next = stream.next();
        assertTrue(next.isPresent());
        assertEquals("OrderShipped", next.get().message().type().qualifiedName().name());
    }

    @Test
    void tokens_delegateToStoragePositions() throws Exception {
        storage.events.add(storedEvent(1, "OrderPlaced", "orderId", "order-1"));
        storage.events.add(storedEvent(2, "OrderShipped", "orderId", "order-1"));

        assertEquals(2L, engine.latestToken().get().position().orElseThrow());
        assertTrue(engine.firstToken().get().position().orElseThrow() < 1L);

        Instant afterFirstEvent = storage.events.get(0).timestamp().plusMillis(1);
        assertEquals(1L, engine.tokenAt(afterFirstEvent).get().position().orElseThrow());
    }

    @SuppressWarnings("unchecked")
    private static <R> R uncheckedCast(Object value) {
        return (R) value;
    }

    private static TaggedEventMessage<EventMessage> taggedEvent(String type, String tagKey, String tagValue) {
        EventMessage message = new GenericEventMessage(
                new MessageType(type), "payload-" + type, Map.of("some-key", "some-value"));
        Set<Tag> tags = Set.of(Tag.of(tagKey, tagValue));
        return new TaggedEventMessage<>() {
            @Override
            public EventMessage event() {
                return message;
            }

            @Override
            public Set<Tag> tags() {
                return tags;
            }

            @Override
            public TaggedEventMessage<EventMessage> updateTags(Function<Set<Tag>, Set<Tag>> updater) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static StoredEvent storedEvent(long position, String type, String tagKey, String tagValue) {
        return new StoredEvent(
                position,
                "event-" + position,
                new MessageType(type).toString(),
                String.class.getName(),
                "\"payload-" + type + "\"",
                new HashMap<>(),
                Set.of(new StoredEvent.StoredTag(tagKey, tagValue)),
                Instant.ofEpochMilli(position * 1000));
    }

    private static final class InMemoryEventStoreStorage implements EventStoreStorage {

        private final List<StoredEvent> events = new ArrayList<>();

        @Override
        public synchronized long appendAtomically(List<StoredEvent> newEvents, long conflictCheckFromPositionExclusive,
                                                    Predicate<StoredEvent> conflictsIfMatched) {
            for (StoredEvent existing : events) {
                if (existing.position() > conflictCheckFromPositionExclusive && conflictsIfMatched.test(existing)) {
                    throw new ConcurrentAppendConflictException("conflicting event at position " + existing.position());
                }
            }
            long position = events.size();
            for (StoredEvent event : newEvents) {
                position++;
                events.add(new StoredEvent(position, event.eventId(), event.messageType(), event.payloadClass(),
                        event.payloadJson(), event.metadata(), event.tags(), event.timestamp()));
            }
            return position;
        }

        @Override
        public synchronized List<StoredEvent> readRange(long fromPositionExclusive, Long toPositionInclusiveOrNull,
                                                          int maxBatchSize) {
            List<StoredEvent> result = new ArrayList<>();
            for (StoredEvent event : events) {
                if (event.position() <= fromPositionExclusive) {
                    continue;
                }
                if (toPositionInclusiveOrNull != null && event.position() > toPositionInclusiveOrNull) {
                    break;
                }
                result.add(event);
                if (result.size() >= maxBatchSize) {
                    break;
                }
            }
            return result;
        }

        @Override
        public synchronized long maxPosition() {
            return events.isEmpty() ? 0 : events.get(events.size() - 1).position();
        }

        @Override
        public synchronized long minPosition() {
            return events.isEmpty() ? 0 : events.get(0).position();
        }

        @Override
        public synchronized Optional<Long> positionAtOrAfter(Instant at) {
            return events.stream()
                    .filter(event -> !event.timestamp().isBefore(at))
                    .map(StoredEvent::position)
                    .findFirst();
        }
    }
}
