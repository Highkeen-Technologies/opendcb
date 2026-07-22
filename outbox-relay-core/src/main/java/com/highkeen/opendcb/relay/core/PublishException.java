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
 * Base type for {@link Publisher#publish} failures. Abstract, and deliberately not
 * throwable directly — a {@link Publisher} must commit to one of
 * {@link RetryablePublishException} or {@link NonRetryablePublishException} so
 * {@link OutboxRelay#runOnce()} has an exhaustive, unambiguous way to decide whether
 * to retry the event or dead-letter it, per docs/CONVENTIONS.md's error-handling
 * guidance.
 */
public abstract sealed class PublishException extends Exception
        permits RetryablePublishException, NonRetryablePublishException {

    protected PublishException(String message) {
        super(message);
    }

    protected PublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
