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
package com.highkeen.opendcb.snapshot.postgres.endtoend;

import com.highkeen.opendcb.eventstore.core.EventStoreStorage;
import com.highkeen.opendcb.eventstore.core.StoredEvent;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * Test-only decorator counting exactly how many {@link StoredEvent}s {@link #readRange} returns
 * across its lifetime -- used by {@code PostgresSnapshotStoreEndToEndTest} as direct, observable
 * evidence of how many events a given {@code EventStorageEngine} instance actually read from the
 * underlying store, to prove a sourcing operation used a snapshot (fewer events read than were
 * ever appended) rather than replaying the full log.
 */
final class CountingEventStoreStorage implements EventStoreStorage {

    private final EventStoreStorage delegate;
    private final AtomicInteger eventsRead = new AtomicInteger();

    CountingEventStoreStorage(EventStoreStorage delegate) {
        this.delegate = delegate;
    }

    int eventsRead() {
        return eventsRead.get();
    }

    @Override
    public long appendAtomically(
            List<StoredEvent> events,
            long conflictCheckFromPositionExclusive,
            Predicate<StoredEvent> conflictsIfMatched) {
        return delegate.appendAtomically(events, conflictCheckFromPositionExclusive, conflictsIfMatched);
    }

    @Override
    public List<StoredEvent> readRange(long fromPositionExclusive, Long toPositionInclusiveOrNull, int maxBatchSize) {
        List<StoredEvent> events = delegate.readRange(fromPositionExclusive, toPositionInclusiveOrNull, maxBatchSize);
        eventsRead.addAndGet(events.size());
        return events;
    }

    @Override
    public long maxPosition() {
        return delegate.maxPosition();
    }

    @Override
    public long minPosition() {
        return delegate.minPosition();
    }

    @Override
    public Optional<Long> positionAtOrAfter(Instant at) {
        return delegate.positionAtOrAfter(at);
    }
}
