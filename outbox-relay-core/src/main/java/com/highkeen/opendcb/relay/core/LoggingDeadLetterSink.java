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

import java.lang.System.Logger.Level;

/**
 * Default {@link DeadLetterSink}: logs at {@link Level#ERROR} and does nothing
 * else. Uses {@link System.Logger} (JEP 264) rather than a logging facade
 * dependency, since this module depends only on {@code eventstore-core}.
 */
public class LoggingDeadLetterSink implements DeadLetterSink {

    private static final System.Logger LOG = System.getLogger(LoggingDeadLetterSink.class.getName());

    @Override
    public void onDeadLetter(StoredEvent event, Throwable cause) {
        LOG.log(Level.ERROR,
                "Dead-lettering event at position " + event.position() + " (eventId=" + event.eventId() + ")",
                cause);
    }
}
