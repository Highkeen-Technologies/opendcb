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

import java.lang.System.Logger.Level;

/**
 * Default {@link ConflictSkipSink}: logs at {@link Level#INFO} -- not {@link Level#ERROR}, since a
 * conflict skip is a deliberate by-design outcome, not a failure -- and does nothing else.
 */
public class LoggingConflictSkipSink implements ConflictSkipSink {

    private static final System.Logger LOG = System.getLogger(LoggingConflictSkipSink.class.getName());

    @Override
    public void onConflictSkip(ScheduledEventRecord record, StoredEvent conflictingEvent) {
        LOG.log(Level.INFO, "Skipping scheduled event " + record.id() + " (eventId=" + record.eventId()
                + "): conflicting event " + conflictingEvent.eventId() + " (messageType="
                + conflictingEvent.messageType() + ") already exists in the log");
    }
}
