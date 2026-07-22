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

import com.highkeen.opendcb.eventstore.core.EventStoreStorage;
import com.highkeen.opendcb.eventstore.core.StoredEvent;

import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Tails an {@link EventStoreStorage} log from a persisted "last relayed position"
 * and hands each event to a {@link Publisher}, in order, advancing the position as
 * it goes. Framework-agnostic — reads directly from {@link EventStoreStorage}, so
 * it works regardless of which framework(s) produced the events.
 *
 * <p>An optional {@link Predicate} filter decides which events actually reach the
 * {@link Publisher}. This is how a bounded context controls what crosses its
 * boundary: a service exposes only its deliberately-shaped integration events by
 * supplying a filter that matches those and rejects everything else — internal
 * domain events simply never match, with no separate mechanism required.
 */
public class OutboxRelay {

    private static final System.Logger LOG = System.getLogger(OutboxRelay.class.getName());

    private final EventStoreStorage storage;
    private final Publisher publisher;
    private final RelayPositionStore positionStore;
    private final DeadLetterSink deadLetterSink;
    private final String relayName;
    private final int batchSize;
    private final Predicate<StoredEvent> filter;

    private ScheduledExecutorService executor;

    public OutboxRelay(
            EventStoreStorage storage,
            Publisher publisher,
            RelayPositionStore positionStore,
            DeadLetterSink deadLetterSink,
            String relayName,
            int batchSize) {
        this(storage, publisher, positionStore, deadLetterSink, relayName, batchSize, event -> true);
    }

    /**
     * @param filter decides which events reach the {@link Publisher}; an event rejected by the
     *               filter is neither published nor dead-lettered — it's a deliberate policy
     *               decision, not a failure — and the relayed position simply advances past it.
     */
    public OutboxRelay(
            EventStoreStorage storage,
            Publisher publisher,
            RelayPositionStore positionStore,
            DeadLetterSink deadLetterSink,
            String relayName,
            int batchSize,
            Predicate<StoredEvent> filter) {
        this.storage = storage;
        this.publisher = publisher;
        this.positionStore = positionStore;
        this.deadLetterSink = deadLetterSink;
        this.relayName = relayName;
        this.batchSize = batchSize;
        this.filter = filter;
    }

    /**
     * Reads up to {@code batchSize} events after the last relayed position and
     * publishes them in order. The relayed position is persisted immediately after
     * each successful (or dead-lettered) publish — not batched at the end — so a
     * crash mid-batch resumes from the correct event, not from the batch start.
     *
     * <p>A {@link RetryablePublishException} stops the batch entirely without
     * advancing past the failed event, so the next call retries it first. A
     * {@link NonRetryablePublishException} hands the event to the
     * {@link DeadLetterSink} and advances past it, continuing the batch.
     */
    public void runOnce() {
        long lastPosition = positionStore.getLastRelayedPosition(relayName);
        List<StoredEvent> batch = storage.readRange(lastPosition, null, batchSize);

        for (StoredEvent event : batch) {
            if (!filter.test(event)) {
                positionStore.setLastRelayedPosition(relayName, event.position());
                continue;
            }
            try {
                publisher.publish(event);
            } catch (RetryablePublishException e) {
                return;
            } catch (NonRetryablePublishException e) {
                deadLetterSink.onDeadLetter(event, e);
                positionStore.setLastRelayedPosition(relayName, event.position());
                continue;
            } catch (PublishException e) {
                // Unreachable: PublishException is sealed to exactly the two subtypes caught above.
                throw new AssertionError("Unknown PublishException subtype: " + e.getClass(), e);
            }
            positionStore.setLastRelayedPosition(relayName, event.position());
        }
    }

    /** Runs {@link #runOnce()} repeatedly on a background thread, waiting {@code pollInterval} between calls. */
    public synchronized void start(Duration pollInterval) {
        if (executor != null) {
            throw new IllegalStateException("Relay '" + relayName + "' is already started");
        }
        executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "outbox-relay-" + relayName);
            thread.setDaemon(true);
            return thread;
        });
        executor.scheduleWithFixedDelay(this::runOnceSafely, 0, pollInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    public synchronized void stop() {
        if (executor == null) {
            return;
        }
        executor.shutdownNow();
        executor = null;
    }

    private void runOnceSafely() {
        try {
            runOnce();
        } catch (RuntimeException e) {
            // A scheduled task that throws suppresses all future executions — log and keep polling instead.
            LOG.log(Level.ERROR, "Relay '" + relayName + "' failed during a poll; will retry next interval", e);
        }
    }
}
