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

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JdbcRelayPositionStoreTest {

    private JdbcRelayPositionStore store;

    @BeforeEach
    void setUp() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:relay-position-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        dataSource.setPassword("");
        store = new JdbcRelayPositionStore(dataSource);
        store.ensureSchema();
    }

    @Test
    void returnsMinusOneWhenNeverRelayed() {
        assertEquals(-1L, store.getLastRelayedPosition("never-seen-relay"));
    }

    @Test
    void setThenGetRoundTrips() {
        store.setLastRelayedPosition("relay-a", 42L);

        assertEquals(42L, store.getLastRelayedPosition("relay-a"));
    }

    @Test
    void settingAgainUpdatesRatherThanDuplicating() {
        store.setLastRelayedPosition("relay-a", 1L);
        store.setLastRelayedPosition("relay-a", 2L);
        store.setLastRelayedPosition("relay-a", 3L);

        assertEquals(3L, store.getLastRelayedPosition("relay-a"));
    }

    @Test
    void tracksMultipleRelaysIndependently() {
        store.setLastRelayedPosition("relay-a", 10L);
        store.setLastRelayedPosition("relay-b", 20L);

        assertEquals(10L, store.getLastRelayedPosition("relay-a"));
        assertEquals(20L, store.getLastRelayedPosition("relay-b"));
    }

    @Test
    void ensureSchemaIsIdempotent() {
        store.ensureSchema();
        store.ensureSchema();

        assertEquals(-1L, store.getLastRelayedPosition("relay-a"));
    }
}
