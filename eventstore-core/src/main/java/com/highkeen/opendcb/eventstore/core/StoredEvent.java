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
import java.util.Map;
import java.util.Set;

/**
 * A single event as persisted by an {@link EventStoreStorage} implementation.
 *
 * <p>Plain DTO with no dependency on any event-sourcing framework's types —
 * framework-specific translation happens in the corresponding
 * {@code integrations/eventstore-<framework>} module.
 */
public record StoredEvent(
        long position,
        String eventId,
        String messageType,
        String payloadClass,
        String payloadJson,
        Map<String, String> metadata,
        Set<StoredTag> tags,
        Instant timestamp) {

    /** A single tag associated with a {@link StoredEvent}, used for DCB conflict predicates. */
    public record StoredTag(String key, String value) {}
}
