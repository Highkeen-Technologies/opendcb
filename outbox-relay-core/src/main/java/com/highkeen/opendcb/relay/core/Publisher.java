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
 * The SPI a transport ({@code outbox-relay-kafka}, {@code outbox-relay-rabbitmq},
 * {@code outbox-relay-webhook}) implements to receive events tailed off an
 * {@code EventStoreStorage} log by {@link OutboxRelay}.
 */
public interface Publisher {

    /**
     * Publishes {@code event} to the transport. Must throw either
     * {@link RetryablePublishException} or {@link NonRetryablePublishException} on
     * failure — {@link OutboxRelay} relies on which subtype is thrown to decide
     * whether to retry this event or dead-letter it and move on.
     */
    void publish(StoredEvent event) throws PublishException;
}
