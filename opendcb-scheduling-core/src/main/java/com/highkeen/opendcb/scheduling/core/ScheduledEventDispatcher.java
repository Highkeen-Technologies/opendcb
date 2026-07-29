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
 * <p><b>DCB conflict-predicate safety net.</b> A row scheduled with a non-null {@link
 * ConflictCriteria} (see {@link ScheduledEventStore#schedule(Instant, StoredEvent, String, int,
 * ConflictCriteria)}) is checked against the log immediately before firing: {@link
 * #findConflictingEvent} does a paginated, full-scan-plus-in-memory-predicate read via {@link
 * EventStoreStorage#readRange} (the same deliberate v1 simplicity already established in {@code
 * docs/PROVIDERS.md} for provider-side tag filtering, not a shortcut specific to this feature). If a
 * match is found, the row is transitioned to {@code SKIPPED_CONFLICT} instead of appended, and {@link
 * #conflictSkipSink} (defaulting to {@link LoggingConflictSkipSink} if none is supplied) is notified --
 * a deliberate by-design skip, not a failure. A row with a {@code null} conflictCriteria (the common
 * case, and the only case before this feature existed) skips this check entirely and fires exactly as
 * before: {@code conflictCheckFromPositionExclusive = 0} with a predicate that never matches.
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

    /** Batch size for {@link #findConflictingEvent}'s paginated scan over the log. */
    private static final int CONFLICT_SCAN_BATCH_SIZE = 500;

    private final ScheduledEventStore scheduledEventStore;
    private final EventStoreStorage storage;
    private final int batchSize;
    private final String workerId;
    private final Duration leaseDuration;
    private final DeadLetterSink deadLetterSink;
    private final ConflictSkipSink conflictSkipSink;

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
        this(scheduledEventStore, storage, batchSize, workerId, leaseDuration, deadLetterSink,
                new LoggingConflictSkipSink());
    }

    public ScheduledEventDispatcher(
            ScheduledEventStore scheduledEventStore,
            EventStoreStorage storage,
            int batchSize,
            String workerId,
            Duration leaseDuration,
            DeadLetterSink deadLetterSink,
            ConflictSkipSink conflictSkipSink) {
        this.scheduledEventStore = scheduledEventStore;
        this.storage = storage;
        this.batchSize = batchSize;
        this.workerId = workerId;
        this.leaseDuration = leaseDuration;
        this.deadLetterSink = deadLetterSink;
        this.conflictSkipSink = conflictSkipSink;
    }

    /**
     * Claims due rows and fires each in turn: appends the reconstructed event, then marks the row
     * completed only on a successful append. A failed append is logged and skipped, leaving the row
     * {@code IN_PROGRESS} for a later lease-expiry reclaim (see the class Javadoc). Rows this poll's
     * {@link ScheduledEventStore#claimDue} dead-lettered (exceeded {@code max_attempts}) are handed to
     * {@link #deadLetterSink} instead -- they are never dispatched. A claimed row carrying a non-null
     * {@link ScheduledEventRecord#conflictCriteria()} is checked against the log first; a match skips
     * the append, marks the row {@code SKIPPED_CONFLICT}, and notifies {@link #conflictSkipSink}.
     */
    public void runOnce() {
        Instant now = Instant.now();
        ScheduledEventStore.ClaimBatch batch = scheduledEventStore.claimDue(now, batchSize, workerId, leaseDuration);
        for (ScheduledEventRecord record : batch.deadLettered()) {
            deadLetterSink.onDeadLetter(record, "exceeded max_attempts (" + record.maxAttempts() + ")");
        }
        for (ScheduledEventRecord record : batch.claimed()) {
            if (record.conflictCriteria() != null) {
                StoredEvent conflictingEvent = findConflictingEvent(record.conflictCriteria());
                if (conflictingEvent != null) {
                    scheduledEventStore.markSkippedConflict(record.id(), workerId);
                    conflictSkipSink.onConflictSkip(record, conflictingEvent);
                    continue;
                }
            }
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

    /**
     * Paginated full-scan of the log via {@link EventStoreStorage#readRange}, returning the first
     * {@link StoredEvent} matching {@code criteria}, or {@code null} if none is found. Full-scan-plus-
     * in-memory-predicate, matching the deliberate v1 simplicity {@code docs/PROVIDERS.md} already
     * establishes for provider-side tag filtering.
     */
    private StoredEvent findConflictingEvent(ConflictCriteria criteria) {
        long fromPositionExclusive = -1;
        while (true) {
            List<StoredEvent> batch = storage.readRange(fromPositionExclusive, null, CONFLICT_SCAN_BATCH_SIZE);
            if (batch.isEmpty()) {
                return null;
            }
            for (StoredEvent event : batch) {
                if (criteria.matches(event)) {
                    return event;
                }
            }
            fromPositionExclusive = batch.get(batch.size() - 1).position();
            if (batch.size() < CONFLICT_SCAN_BATCH_SIZE) {
                return null;
            }
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
