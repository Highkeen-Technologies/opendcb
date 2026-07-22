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

/**
 * Receives events {@link OutboxRelay} gave up retrying after a
 * {@link NonRetryablePublishException}. What actually happens to a dead-lettered
 * event (a dedicated table, a separate topic, ...) is a transport's concern, not
 * core's — see {@link LoggingDeadLetterSink} for the only implementation this
 * module provides.
 */
public interface DeadLetterSink {

    void onDeadLetter(StoredEvent event, Throwable cause);
}
