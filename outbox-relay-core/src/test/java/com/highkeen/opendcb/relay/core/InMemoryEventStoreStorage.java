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

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/** Minimal in-memory {@link EventStoreStorage} test double — not a real database, per docs/TESTING.md. */
final class InMemoryEventStoreStorage implements EventStoreStorage {

    private final List<StoredEvent> events = new ArrayList<>();

    @Override
    public synchronized long appendAtomically(
            List<StoredEvent> newEvents, long conflictCheckFromPositionExclusive, Predicate<StoredEvent> conflictsIfMatched) {
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
    public synchronized List<StoredEvent> readRange(long fromPositionExclusive, Long toPositionInclusiveOrNull, int maxBatchSize) {
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
