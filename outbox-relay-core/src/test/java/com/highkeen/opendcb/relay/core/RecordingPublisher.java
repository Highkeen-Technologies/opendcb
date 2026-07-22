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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** In-memory {@link Publisher} test double: records what was published, and can be told to fail at a given position. */
final class RecordingPublisher implements Publisher {

    private final List<StoredEvent> published = new ArrayList<>();
    private final Map<Long, PublishException> failuresByPosition = new HashMap<>();

    void failAt(long position, PublishException exception) {
        failuresByPosition.put(position, exception);
    }

    void clearFailure(long position) {
        failuresByPosition.remove(position);
    }

    @Override
    public void publish(StoredEvent event) throws PublishException {
        PublishException failure = failuresByPosition.get(event.position());
        if (failure != null) {
            throw failure;
        }
        published.add(event);
    }

    List<StoredEvent> published() {
        return published;
    }
}
