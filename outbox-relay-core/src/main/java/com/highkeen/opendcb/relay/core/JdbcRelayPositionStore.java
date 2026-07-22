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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Plain-JDBC {@link RelayPositionStore}, generic ANSI SQL — unlike a storage
 * provider, this is legitimately provider-agnostic: a single-row-per-relay-name
 * table with no advanced locking needs. {@link #setLastRelayedPosition} does an
 * update-then-insert instead of a vendor-specific upsert, since {@code MERGE}/
 * {@code ON CONFLICT}/{@code ON DUPLICATE KEY} syntax isn't portable across
 * databases.
 */
public class JdbcRelayPositionStore implements RelayPositionStore {

    private static final String TABLE_DDL = """
            CREATE TABLE IF NOT EXISTS relay_position (
                relay_name VARCHAR(255) PRIMARY KEY,
                last_relayed_position BIGINT NOT NULL
            )""";

    private final DataSource dataSource;

    public JdbcRelayPositionStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** Creates the {@code relay_position} table if it doesn't already exist. */
    public void ensureSchema() {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(TABLE_DDL);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to ensure relay_position schema", e);
        }
    }

    @Override
    public long getLastRelayedPosition(String relayName) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT last_relayed_position FROM relay_position WHERE relay_name = ?")) {
            statement.setString(1, relayName);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? resultSet.getLong(1) : -1L;
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to read last relayed position for relay '" + relayName + "'", e);
        }
    }

    @Override
    public void setLastRelayedPosition(String relayName, long position) {
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement update = connection.prepareStatement(
                    "UPDATE relay_position SET last_relayed_position = ? WHERE relay_name = ?")) {
                update.setLong(1, position);
                update.setString(2, relayName);
                if (update.executeUpdate() > 0) {
                    return;
                }
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO relay_position (relay_name, last_relayed_position) VALUES (?, ?)")) {
                insert.setString(1, relayName);
                insert.setLong(2, position);
                insert.executeUpdate();
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to persist last relayed position for relay '" + relayName + "'", e);
        }
    }
}
