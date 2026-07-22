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

/**
 * Thrown when publishing this specific event can never succeed (e.g. serialization
 * failure) — retrying it would loop forever. {@link OutboxRelay#runOnce()} hands the
 * event to its {@link DeadLetterSink} and advances the relayed position past it,
 * continuing the batch.
 */
public final class NonRetryablePublishException extends PublishException {

    public NonRetryablePublishException(String message) {
        super(message);
    }

    public NonRetryablePublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
