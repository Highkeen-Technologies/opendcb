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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.highkeen.opendcb.eventstore.core.EventStoreStorage;
import com.highkeen.opendcb.eventstore.core.StoredEvent;
import com.highkeen.opendcb.eventstore.core.StoredEvent.StoredTag;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.AppendEventsTransactionRejectedException;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.GlobalIndexConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.Position;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.SourcingStrategy;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.TerminalEventMessage;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.SimpleEntry;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.EventsCondition;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Adapts a framework-agnostic {@link EventStoreStorage} (from {@code eventstore-core}) to Axon
 * Framework 5's {@link EventStorageEngine} SPI.
 *
 * <p>This is the only class in the OpenDCB toolkit that imports {@code org.axonframework} — see
 * {@code docs/ARCHITECTURE.md}. All persistence is delegated to the {@link EventStoreStorage}
 * given at construction; this class only translates between Axon's types ({@link
 * TaggedEventMessage}, {@link AppendCondition}, {@link ConsistencyMarker}, {@link MessageStream},
 * {@link EventsCondition}) and {@link StoredEvent}.
 *
 * <h2>Deliberate simplification: payload (de)serialization</h2>
 *
 * <p>Payloads are (de)serialized directly with Jackson's {@link ObjectMapper}, keyed by the
 * payload's fully-qualified class name (stored as {@link StoredEvent#payloadClass()}). Axon's own
 * {@code Converter}/upcaster SPI is deliberately bypassed for now. This means schema evolution
 * (renaming a payload class, restructuring its fields) is not supported out of the box — a
 * production deployment that needs upcasting should wire Axon's real {@code Converter} instead of
 * relying on this class's Jackson usage.
 *
 * <h2>Deliberate simplification: position/marker numbering</h2>
 *
 * <p>{@link StoredEvent#position()} values are used directly as Axon's {@link GlobalIndexPosition}
 * / {@link GlobalSequenceTrackingToken} index values and as {@link GlobalIndexConsistencyMarker}
 * values, with no offset translation between the two spaces. Both are treated purely as opaque,
 * monotonically increasing {@code long}s used for pagination and conflict-checking, and {@code
 * EventStoreStorage} providers are expected to hand out strictly increasing positions — so no
 * translation is required for this to be internally consistent.
 *
 * <h2>Deliberate simplification: streaming is poll-based, not push-based</h2>
 *
 * <p>{@link #stream(StreamingCondition)} returns a {@link MessageStream} that re-queries {@link
 * EventStoreStorage#readRange} on every {@code next()}/{@code peek()}/{@code hasNextAvailable()}
 * call rather than being notified by the storage layer when new events arrive. {@link
 * MessageStream#setCallback(Runnable)} only fires immediately if a matching event is already
 * available at registration time — there is no background notification when new events are
 * appended later. Callers must keep polling (e.g. via a scheduled {@code hasNextAvailable()}
 * check) to observe new events.
 */
public class AbstractDcbEventStorageEngine implements EventStorageEngine {

    private static final int SOURCING_BATCH_SIZE = Integer.MAX_VALUE;
    private static final int STREAMING_BATCH_SIZE = 256;

    private final EventStoreStorage storage;
    private final ObjectMapper objectMapper;

    public AbstractDcbEventStorageEngine(EventStoreStorage storage, ObjectMapper objectMapper) {
        this.storage = Objects.requireNonNull(storage, "storage must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    }

    @Override
    public CompletableFuture<AppendTransaction<?>> appendEvents(AppendCondition condition,
                                                                 @Nullable ProcessingContext context,
                                                                 List<TaggedEventMessage<?>> events) {
        List<StoredEvent> toAppend = events.stream().map(this::toStoredEvent).toList();
        long conflictCheckFromPositionExclusive = GlobalIndexConsistencyMarker.position(condition.consistencyMarker());

        return CompletableFuture.completedFuture(new AppendTransaction<Long>() {

            private final AtomicBoolean committed = new AtomicBoolean(false);

            @Override
            public CompletableFuture<Long> commit() {
                if (committed.getAndSet(true)) {
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("This append transaction was already committed or rolled back."));
                }
                if (toAppend.isEmpty()) {
                    return CompletableFuture.completedFuture(null);
                }
                try {
                    long lastAppendedPosition = storage.appendAtomically(
                            toAppend,
                            conflictCheckFromPositionExclusive,
                            storedEvent -> matches(storedEvent, condition));
                    return CompletableFuture.completedFuture(lastAppendedPosition);
                } catch (EventStoreStorage.ConcurrentAppendConflictException e) {
                    return CompletableFuture.failedFuture(
                            AppendEventsTransactionRejectedException.conflictingEventsDetected(condition.consistencyMarker()));
                }
            }

            @Override
            public void rollback() {
                if (committed.getAndSet(true)) {
                    throw new UnsupportedOperationException(
                            "Cannot roll back an append transaction that has already been committed — "
                                    + "events are durably persisted once appendAtomically() returns.");
                }
            }

            @Override
            public CompletableFuture<ConsistencyMarker> afterCommit(Long commitResult) {
                ConsistencyMarker marker = commitResult == null
                        ? ConsistencyMarker.ORIGIN
                        : new GlobalIndexConsistencyMarker(commitResult);
                return CompletableFuture.completedFuture(marker);
            }
        });
    }

    @Override
    public MessageStream<EventMessage> source(SourcingCondition condition) {
        long startPositionExclusive = switch (condition.strategy()) {
            case SourcingStrategy.Absolute(Position position) -> Math.max(0, GlobalIndexPosition.toIndex(position));
            default -> throw new UnsupportedOperationException(
                    "Unsupported sourcing strategy: " + condition.strategy());
        };
        long endPositionInclusive = storage.maxPosition();

        List<StoredEvent> matched = startPositionExclusive >= endPositionInclusive
                ? List.of()
                : storage.readRange(startPositionExclusive, endPositionInclusive, SOURCING_BATCH_SIZE)
                        .stream()
                        .filter(storedEvent -> matches(storedEvent, condition))
                        .toList();

        List<EventMessage> messages = new ArrayList<>(matched.size() + 1);
        for (StoredEvent storedEvent : matched) {
            messages.add(toEventMessage(storedEvent));
        }
        messages.add(TerminalEventMessage.INSTANCE);

        ConsistencyMarker terminalMarker = new GlobalIndexConsistencyMarker(endPositionInclusive);
        return MessageStream.fromIterable(messages, message -> message == TerminalEventMessage.INSTANCE
                ? ConsistencyMarker.addToContext(Context.empty(), terminalMarker)
                : Context.empty());
    }

    @Override
    public MessageStream<EventMessage> stream(StreamingCondition condition) {
        long startPositionExclusive = Math.max(-1, condition.position().position().orElse(-1));
        return new PollingEventMessageStream(startPositionExclusive, condition);
    }

    @Override
    public CompletableFuture<TrackingToken> firstToken() {
        return CompletableFuture.completedFuture(new GlobalSequenceTrackingToken(storage.minPosition() - 1));
    }

    @Override
    public CompletableFuture<TrackingToken> latestToken() {
        return CompletableFuture.completedFuture(new GlobalSequenceTrackingToken(storage.maxPosition()));
    }

    @Override
    public CompletableFuture<TrackingToken> tokenAt(Instant at) {
        return storage.positionAtOrAfter(at)
                .map(position -> (TrackingToken) new GlobalSequenceTrackingToken(position - 1))
                .map(CompletableFuture::completedFuture)
                .orElseGet(this::latestToken);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("storage", storage.getClass().getName());
    }

    private static boolean matches(StoredEvent storedEvent, EventsCondition condition) {
        QualifiedName type = MessageType.fromString(storedEvent.messageType()).qualifiedName();
        Set<Tag> tags = storedEvent.tags().stream()
                .map(storedTag -> new Tag(storedTag.key(), storedTag.value()))
                .collect(Collectors.toUnmodifiableSet());
        return condition.matches(type, tags);
    }

    private StoredEvent toStoredEvent(TaggedEventMessage<?> tagged) {
        EventMessage event = tagged.event();
        Object payload = event.payload();
        String payloadClass = payload != null ? payload.getClass().getName() : null;
        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(
                    "Failed to serialize payload of event [" + event.identifier() + "] to JSON.", e);
        }
        Set<StoredTag> tags = tagged.tags().stream()
                .map(tag -> new StoredTag(tag.key(), tag.value()))
                .collect(Collectors.toUnmodifiableSet());
        return new StoredEvent(
                0L,
                event.identifier(),
                event.type().toString(),
                payloadClass,
                payloadJson,
                new HashMap<>(event.metadata()),
                tags,
                event.timestamp());
    }

    private EventMessage toEventMessage(StoredEvent storedEvent) {
        MessageType type = MessageType.fromString(storedEvent.messageType());
        return new GenericEventMessage(
                storedEvent.eventId(),
                type,
                deserializePayload(storedEvent),
                storedEvent.metadata(),
                storedEvent.timestamp());
    }

    private @Nullable Object deserializePayload(StoredEvent storedEvent) {
        if (storedEvent.payloadClass() == null) {
            return null;
        }
        try {
            Class<?> payloadType = Class.forName(storedEvent.payloadClass());
            return objectMapper.readValue(storedEvent.payloadJson(), payloadType);
        } catch (ClassNotFoundException | JsonProcessingException e) {
            throw new IllegalStateException(
                    "Failed to deserialize payload of event [" + storedEvent.eventId() + "] with declared class ["
                            + storedEvent.payloadClass() + "].", e);
        }
    }

    /**
     * Infinite, poll-based {@link MessageStream} backed by repeated {@link EventStoreStorage#readRange}
     * calls — see this class's Javadoc for why this is a deliberate simplification versus a
     * push-notified stream.
     */
    private final class PollingEventMessageStream implements MessageStream<EventMessage> {

        private final EventsCondition condition;
        private final Deque<StoredEvent> buffer = new ArrayDeque<>();
        private long rawCursor;
        private volatile boolean closed = false;

        private PollingEventMessageStream(long startPositionExclusive, EventsCondition condition) {
            this.rawCursor = startPositionExclusive;
            this.condition = condition;
        }

        private synchronized void fillBufferIfNeeded() {
            if (closed || !buffer.isEmpty()) {
                return;
            }
            List<StoredEvent> batch = storage.readRange(rawCursor, null, STREAMING_BATCH_SIZE);
            for (StoredEvent storedEvent : batch) {
                rawCursor = storedEvent.position();
                if (matches(storedEvent, condition)) {
                    buffer.addLast(storedEvent);
                }
            }
        }

        @Override
        public synchronized Optional<Entry<EventMessage>> next() {
            if (closed) {
                return Optional.empty();
            }
            fillBufferIfNeeded();
            StoredEvent storedEvent = buffer.pollFirst();
            return storedEvent == null ? Optional.empty() : Optional.of(toEntry(storedEvent));
        }

        @Override
        public synchronized Optional<Entry<EventMessage>> peek() {
            if (closed) {
                return Optional.empty();
            }
            fillBufferIfNeeded();
            return buffer.isEmpty() ? Optional.empty() : Optional.of(toEntry(buffer.peekFirst()));
        }

        @Override
        public void setCallback(Runnable callback) {
            if (hasNextAvailable()) {
                callback.run();
            }
        }

        @Override
        public Optional<Throwable> error() {
            return Optional.empty();
        }

        @Override
        public boolean isCompleted() {
            return closed;
        }

        @Override
        public synchronized boolean hasNextAvailable() {
            if (closed) {
                return false;
            }
            fillBufferIfNeeded();
            return !buffer.isEmpty();
        }

        @Override
        public synchronized void close() {
            closed = true;
            buffer.clear();
        }

        private Entry<EventMessage> toEntry(StoredEvent storedEvent) {
            EventMessage message = toEventMessage(storedEvent);
            Context context = TrackingToken.addToContext(
                    Context.empty(), new GlobalSequenceTrackingToken(storedEvent.position()));
            return new SimpleEntry<>(message, context);
        }
    }
}
