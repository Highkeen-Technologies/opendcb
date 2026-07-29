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
package com.highkeen.opendcb.scheduling.core;

import com.highkeen.opendcb.eventstore.core.EventStoreStorage;
import com.highkeen.opendcb.eventstore.core.StoredEvent;

import java.lang.System.Logger.Level;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Polls {@link ScheduledEventStore} for due rows and fires each by appending it as a real event via
 * {@link EventStoreStorage#appendAtomically}. Structurally similar to {@code OutboxRelay} (claim/read
 * a batch, act on each, mark progress), but dispatches by <em>appending</em> rather than publishing to
 * a transport.
 *
 * <p><b>No DCB conflict-predicate safety net is applied here.</b> {@code
 * EventStoreStorage.appendAtomically} takes a conflict predicate that could, in principle, let a
 * caller skip firing a scheduled event if a conflicting event already occurred (e.g. don't fire
 * {@code InvoicePaymentDeadlineExpiredEvent} if {@code InvoicePaidEvent} already exists) -- that is
 * deliberately out of scope for this build (see {@code docs/ROADMAP.md}'s follow-up note) and is left
 * as a documented future enhancement. Every append here always succeeds unless the underlying store
 * itself fails: {@code conflictCheckFromPositionExclusive = 0} with a predicate that never matches.
 *
 * <p><b>Retry cap and dead-letter handling.</b> Each row carries its own {@code max_attempts} budget
 * (set at schedule time, see {@link ScheduledEventStore#schedule(Instant, StoredEvent, String, int)}).
 * On append failure the row is left {@code IN_PROGRESS}, exactly as before, and becomes reclaimable
 * once its lease expires -- but {@link ScheduledEventStore#claimDue} now refuses to reclaim a row past
 * its attempt budget, dead-lettering it instead. This dispatcher forwards every newly dead-lettered
 * record from each poll cycle to a {@link DeadLetterSink} (defaulting to {@link LoggingDeadLetterSink}
 * if none is supplied), closing what was previously an indefinite retry loop.
 */
public class ScheduledEventDispatcher {

    private static final System.Logger LOG = System.getLogger(ScheduledEventDispatcher.class.getName());

    private final ScheduledEventStore scheduledEventStore;
    private final EventStoreStorage storage;
    private final int batchSize;
    private final String workerId;
    private final Duration leaseDuration;
    private final DeadLetterSink deadLetterSink;

    private ScheduledExecutorService executor;

    public ScheduledEventDispatcher(
            ScheduledEventStore scheduledEventStore,
            EventStoreStorage storage,
            int batchSize,
            String workerId,
            Duration leaseDuration) {
        this(scheduledEventStore, storage, batchSize, workerId, leaseDuration, new LoggingDeadLetterSink());
    }

    public ScheduledEventDispatcher(
            ScheduledEventStore scheduledEventStore,
            EventStoreStorage storage,
            int batchSize,
            String workerId,
            Duration leaseDuration,
            DeadLetterSink deadLetterSink) {
        this.scheduledEventStore = scheduledEventStore;
        this.storage = storage;
        this.batchSize = batchSize;
        this.workerId = workerId;
        this.leaseDuration = leaseDuration;
        this.deadLetterSink = deadLetterSink;
    }

    /**
     * Claims due rows and fires each in turn: appends the reconstructed event, then marks the row
     * completed only on a successful append. A failed append is logged and skipped, leaving the row
     * {@code IN_PROGRESS} for a later lease-expiry reclaim (see the class Javadoc). Rows this poll's
     * {@link ScheduledEventStore#claimDue} dead-lettered (exceeded {@code max_attempts}) are handed to
     * {@link #deadLetterSink} instead -- they are never dispatched.
     */
    public void runOnce() {
        Instant now = Instant.now();
        ScheduledEventStore.ClaimBatch batch = scheduledEventStore.claimDue(now, batchSize, workerId, leaseDuration);
        for (ScheduledEventRecord record : batch.deadLettered()) {
            deadLetterSink.onDeadLetter(record, "exceeded max_attempts (" + record.maxAttempts() + ")");
        }
        for (ScheduledEventRecord record : batch.claimed()) {
            try {
                storage.appendAtomically(List.of(record.toStoredEvent(Instant.now())), 0L, event -> false);
            } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "Failed to fire scheduled event " + record.eventId()
                        + "; leaving IN_PROGRESS for lease-expiry reclaim", e);
                continue;
            }
            scheduledEventStore.markCompleted(record.id(), workerId);
        }
    }

    /** Runs {@link #runOnce()} repeatedly on a background thread, waiting {@code pollInterval} between calls. */
    public synchronized void start(Duration pollInterval) {
        if (executor != null) {
            throw new IllegalStateException("ScheduledEventDispatcher '" + workerId + "' is already started");
        }
        executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "opendcb-scheduling-" + workerId);
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
            // A scheduled task that throws suppresses all future executions -- log and keep polling instead.
            LOG.log(Level.ERROR, "ScheduledEventDispatcher '" + workerId
                    + "' failed during a poll; will retry next interval", e);
        }
    }
}
