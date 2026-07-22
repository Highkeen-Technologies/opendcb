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

import com.highkeen.opendcb.eventstore.core.StoredEvent;

import java.util.ArrayList;
import java.util.List;

/** In-memory {@link DeadLetterSink} test double: records every dead-lettered event and its cause. */
final class RecordingDeadLetterSink implements DeadLetterSink {

    private final List<StoredEvent> deadLetteredEvents = new ArrayList<>();
    private final List<Throwable> causes = new ArrayList<>();

    @Override
    public void onDeadLetter(StoredEvent event, Throwable cause) {
        deadLetteredEvents.add(event);
        causes.add(cause);
    }

    List<StoredEvent> deadLetteredEvents() {
        return deadLetteredEvents;
    }

    List<Throwable> causes() {
        return causes;
    }
}
