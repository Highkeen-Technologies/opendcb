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

import com.highkeen.opendcb.eventstore.core.StoredEvent;
import com.highkeen.opendcb.eventstore.core.StoredEvent.StoredTag;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** One {@code scheduled_event} row, as returned by {@link ScheduledEventStore#claimDue}. */
public record ScheduledEventRecord(
        UUID id,
        Instant scheduledTime,
        String eventId,
        String messageType,
        String payloadClass,
        String payloadJson,
        Map<String, String> metadata,
        Set<StoredTag> tags,
        String scopeName,
        ScheduledEventStatus status,
        Instant claimedAt,
        String workerId,
        Instant createdAt) {

    /**
     * Reconstructs the {@link StoredEvent} to fire, with {@code position = -1} (not yet assigned —
     * the same convention {@code AbstractDcbEventStorageEngine} uses for not-yet-appended events) and
     * {@code timestamp = firedAt}, the moment the append is actually attempted, not {@link #scheduledTime}.
     */
    public StoredEvent toStoredEvent(Instant firedAt) {
        return new StoredEvent(-1, eventId, messageType, payloadClass, payloadJson, metadata, tags, firedAt);
    }
}
