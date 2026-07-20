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

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * The port a storage provider (e.g. {@code eventstore-postgres}) implements.
 *
 * <p>Framework-agnostic by design — see {@code docs/ARCHITECTURE.md}. No
 * method here may accept or return a framework-specific type.
 */
public interface EventStoreStorage {

    /**
     * Appends {@code events} atomically, after checking every event at or after
     * {@code conflictCheckFromPositionExclusive} against {@code conflictsIfMatched}.
     * If any matches, no event is written and
     * {@link ConcurrentAppendConflictException} is thrown.
     *
     * @return the position of the last appended event
     */
    long appendAtomically(
            List<StoredEvent> events,
            long conflictCheckFromPositionExclusive,
            Predicate<StoredEvent> conflictsIfMatched)
            throws ConcurrentAppendConflictException;

    /**
     * Reads events in position order, starting after {@code fromPositionExclusive}
     * up to and including {@code toPositionInclusiveOrNull} (or unbounded if
     * {@code null}), returning at most {@code maxBatchSize} events.
     */
    List<StoredEvent> readRange(long fromPositionExclusive, Long toPositionInclusiveOrNull, int maxBatchSize);

    /** The highest position currently stored. */
    long maxPosition();

    /** The lowest position currently stored. */
    long minPosition();

    /** The position of the first event at or after {@code at}, if any. */
    Optional<Long> positionAtOrAfter(Instant at);

    /** Thrown by {@link #appendAtomically} when the conflict predicate matches an existing event. */
    class ConcurrentAppendConflictException extends RuntimeException {

        public ConcurrentAppendConflictException(String message) {
            super(message);
        }

        public ConcurrentAppendConflictException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
