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

/**
 * Receives scheduled events {@link ScheduledEventDispatcher} deliberately skipped firing because a
 * {@link ConflictCriteria} matched an event already in the log. Same shape as {@link DeadLetterSink}
 * but semantically distinct: a conflict skip is a deliberate by-design outcome, not a failure -- see
 * {@link LoggingConflictSkipSink}, which logs at INFO rather than ERROR for exactly that reason.
 */
public interface ConflictSkipSink {
    void onConflictSkip(ScheduledEventRecord record, StoredEvent conflictingEvent);
}
