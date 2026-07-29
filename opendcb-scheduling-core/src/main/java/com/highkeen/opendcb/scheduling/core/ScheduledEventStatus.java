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

/** Lifecycle of one {@code scheduled_event} row, as owned by {@link ScheduledEventStore}. */
public enum ScheduledEventStatus {
    PENDING,
    IN_PROGRESS,
    COMPLETED,
    CANCELLED,
    /** Terminal: {@link ScheduledEventStore#claimDue} gave up after {@code max_attempts} claims. */
    DEAD_LETTERED
}
