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

/**
 * Receives scheduled events {@link ScheduledEventStore#claimDue} gave up retrying after exceeding
 * their {@code max_attempts} budget. This mirrors {@code outbox-relay-core}'s {@code DeadLetterSink}
 * pattern (same shape: a narrow SPI plus a default logging implementation) as an independent,
 * locally-defined type rather than a shared dependency -- this module deliberately depends only on
 * {@code eventstore-core}, not on {@code outbox-relay-core} (see {@code docs/ARCHITECTURE.md}'s module
 * dependency order), the same way {@link ScheduledEventStore} already parallels {@code
 * JdbcRelayPositionStore} without depending on it.
 */
public interface DeadLetterSink {
    void onDeadLetter(ScheduledEventRecord record, String reason);
}
