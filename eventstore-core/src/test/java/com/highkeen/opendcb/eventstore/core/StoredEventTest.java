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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.highkeen.opendcb.eventstore.core.StoredEvent.StoredTag;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class StoredEventTest {

    @Test
    void constructsWithAllFieldsAccessible() {
        Instant timestamp = Instant.parse("2026-07-20T12:00:00Z");
        StoredEvent event = new StoredEvent(
                1L,
                "event-1",
                "AccountOpened",
                "com.example.AccountOpened",
                "{\"accountId\":\"acc-1\"}",
                Map.of("traceId", "trace-1"),
                Set.of(new StoredTag("accountId", "acc-1")),
                timestamp);

        assertEquals(1L, event.position());
        assertEquals("event-1", event.eventId());
        assertEquals("AccountOpened", event.messageType());
        assertEquals("com.example.AccountOpened", event.payloadClass());
        assertEquals("{\"accountId\":\"acc-1\"}", event.payloadJson());
        assertEquals(Map.of("traceId", "trace-1"), event.metadata());
        assertEquals(Set.of(new StoredTag("accountId", "acc-1")), event.tags());
        assertEquals(timestamp, event.timestamp());
    }

    @Test
    void recordsWithSameFieldsAreEqual() {
        Instant timestamp = Instant.parse("2026-07-20T12:00:00Z");
        StoredEvent first = new StoredEvent(
                1L, "event-1", "AccountOpened", "com.example.AccountOpened", "{}",
                Map.of(), Set.of(new StoredTag("accountId", "acc-1")), timestamp);
        StoredEvent second = new StoredEvent(
                1L, "event-1", "AccountOpened", "com.example.AccountOpened", "{}",
                Map.of(), Set.of(new StoredTag("accountId", "acc-1")), timestamp);

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
    }

    @Test
    void recordsWithDifferentPositionAreNotEqual() {
        Instant timestamp = Instant.parse("2026-07-20T12:00:00Z");
        StoredEvent first = new StoredEvent(
                1L, "event-1", "AccountOpened", "com.example.AccountOpened", "{}",
                Map.of(), Set.of(), timestamp);
        StoredEvent second = new StoredEvent(
                2L, "event-1", "AccountOpened", "com.example.AccountOpened", "{}",
                Map.of(), Set.of(), timestamp);

        assertNotEquals(first, second);
    }

    @Test
    void storedTagRecordsWithSameFieldsAreEqual() {
        assertEquals(new StoredTag("accountId", "acc-1"), new StoredTag("accountId", "acc-1"));
    }
}
