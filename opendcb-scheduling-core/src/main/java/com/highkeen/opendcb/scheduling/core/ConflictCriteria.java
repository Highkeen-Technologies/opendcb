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
import java.util.Set;

/**
 * A DCB-native conflict check a caller can attach to a scheduled event at schedule time (see {@link
 * ScheduledEventStore#schedule(Instant, StoredEvent, String, int, ConflictCriteria)}): if a {@link
 * StoredEvent} matching this criteria already exists in the log by the time {@link
 * ScheduledEventDispatcher} would otherwise fire the scheduled event, the fire is skipped instead --
 * e.g. don't fire {@code InvoicePaymentDeadlineExpiredEvent} if an {@code InvoicePaidEvent} for the
 * same invoice already exists (see {@code docs/ARCHITECTURE.md}'s "opendcb-scheduling-core" section).
 *
 * <p>Persisted as JSON on the {@code scheduled_event} row -- a plain in-memory {@code
 * java.util.function.Predicate} can't survive a restart -- so this is expressed as a serializable
 * tag/type structure instead. Same spirit as Axon's own {@code EventCriteria} (tags + message types),
 * but in this module's own framework-agnostic terms ({@link StoredTag}, plain {@link String}s).
 *
 * @param requiredTags a {@link StoredEvent} must carry every one of these tags to match
 * @param messageTypes if non-empty, a {@link StoredEvent} must also have one of these message types to
 *                      match; an empty set matches any message type
 */
public record ConflictCriteria(Set<StoredTag> requiredTags, Set<String> messageTypes) {

    /**
     * True if {@code event} carries every one of {@link #requiredTags} and, when {@link #messageTypes}
     * is non-empty, has one of those message types.
     */
    public boolean matches(StoredEvent event) {
        return event.tags().containsAll(requiredTags)
                && (messageTypes.isEmpty() || messageTypes.contains(event.messageType()));
    }
}
