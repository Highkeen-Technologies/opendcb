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
package com.highkeen.opendcb.snapshot.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.Position;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.QualifiedName;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Postgres-backed implementation of Axon Framework 5's real, free {@link SnapshotStore} SPI --
 * a genuine implementation of an existing {@code org.axonframework} interface, unlike this
 * toolkit's other Axon-coupled modules (which each invent their own abstraction standing in for
 * a capability Axon only offers in a paid tier). Own {@code snapshot} table, independent of the
 * event log's own tables; this module depends on neither {@code eventstore-core} nor
 * {@code eventstore-postgres}.
 *
 * <p>{@code (qualified_name, identifier)} is the table's composite primary key, matching {@link
 * SnapshotStore#store}'s own documented contract that a store call "replaces any previous
 * snapshot(s) with the same name and identifier" -- {@link #store} is therefore an {@code INSERT
 * ... ON CONFLICT ... DO UPDATE} upsert, never a plain insert.
 *
 * <p>The payload is serialized with Jackson directly (not Axon's {@code Converter} SPI): this
 * module lives outside {@code integrations/eventstore-axon}, so it has no existing {@code
 * Converter} to reuse, and a direct Jackson dependency is the same pragmatic pattern already used
 * elsewhere in this toolkit for classes that aren't part of that adapter.
 */
public class PostgresSnapshotStore implements SnapshotStore {

    private static final String SNAPSHOT_TABLE_DDL = """
            CREATE TABLE IF NOT EXISTS snapshot (
                qualified_name TEXT NOT NULL,
                identifier TEXT NOT NULL,
                position BIGINT NOT NULL,
                version TEXT NOT NULL,
                payload_class TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                occurred_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (qualified_name, identifier)
            )""";

    private static final String UPSERT_SQL = """
            INSERT INTO snapshot
                (qualified_name, identifier, position, version, payload_class, payload_json, metadata_json, occurred_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (qualified_name, identifier) DO UPDATE SET
                position = EXCLUDED.position,
                version = EXCLUDED.version,
                payload_class = EXCLUDED.payload_class,
                payload_json = EXCLUDED.payload_json,
                metadata_json = EXCLUDED.metadata_json,
                occurred_at = EXCLUDED.occurred_at
            """;

    private static final String SELECT_SQL = """
            SELECT position, version, payload_class, payload_json, metadata_json, occurred_at
            FROM snapshot
            WHERE qualified_name = ? AND identifier = ?
            """;

    private final DataSource dataSource;
    private final ObjectMapper objectMapper;

    public PostgresSnapshotStore(DataSource dataSource) {
        this(dataSource, new ObjectMapper());
    }

    public PostgresSnapshotStore(DataSource dataSource, ObjectMapper objectMapper) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    }

    /** Creates the {@code snapshot} table if it does not already exist. Safe to call repeatedly. */
    public void ensureSchema() {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(SNAPSHOT_TABLE_DDL);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to create opendcb-snapshot-postgres schema", e);
        }
    }

    @Override
    public CompletableFuture<Void> store(QualifiedName qualifiedName, Object identifier, Snapshot snapshot) {
        Objects.requireNonNull(qualifiedName, "qualifiedName must not be null");
        Objects.requireNonNull(identifier, "identifier must not be null");
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        return CompletableFuture.runAsync(() -> storeBlocking(qualifiedName, identifier, snapshot));
    }

    private void storeBlocking(QualifiedName qualifiedName, Object identifier, Snapshot snapshot) {
        long position = GlobalIndexPosition.toIndex(snapshot.position());
        String payloadClass = snapshot.payload().getClass().getName();
        String payloadJson = writeJson(snapshot.payload());
        String metadataJson = writeJson(snapshot.metadata());
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(UPSERT_SQL)) {
            statement.setString(1, qualifiedName.name());
            statement.setString(2, identifier.toString());
            statement.setLong(3, position);
            statement.setString(4, snapshot.version());
            statement.setString(5, payloadClass);
            statement.setString(6, payloadJson);
            statement.setString(7, metadataJson);
            statement.setTimestamp(8, Timestamp.from(snapshot.timestamp()));
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to store snapshot for '" + qualifiedName.name() + "'/'" + identifier + "'", e);
        }
    }

    @Override
    public CompletableFuture<Snapshot> load(QualifiedName qualifiedName, Object identifier) {
        Objects.requireNonNull(qualifiedName, "qualifiedName must not be null");
        Objects.requireNonNull(identifier, "identifier must not be null");
        return CompletableFuture.supplyAsync(() -> loadBlocking(qualifiedName, identifier));
    }

    private Snapshot loadBlocking(QualifiedName qualifiedName, Object identifier) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(SELECT_SQL)) {
            statement.setString(1, qualifiedName.name());
            statement.setString(2, identifier.toString());
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return null;
                }
                Position position = new GlobalIndexPosition(resultSet.getLong("position"));
                String version = resultSet.getString("version");
                String payloadClass = resultSet.getString("payload_class");
                String payloadJson = resultSet.getString("payload_json");
                String metadataJson = resultSet.getString("metadata_json");
                Instant timestamp = resultSet.getTimestamp("occurred_at").toInstant();

                Object payload = readJson(payloadJson, payloadClass);
                Map<String, String> metadata = readMetadata(metadataJson);

                return new Snapshot(position, version, payload, timestamp, metadata);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to load snapshot for '" + qualifiedName.name() + "'/'" + identifier + "'", e);
        }
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize snapshot value of type " + value.getClass(), e);
        }
    }

    private Object readJson(String json, String payloadClassName) {
        try {
            Class<?> payloadClass = Class.forName(payloadClassName);
            return objectMapper.readValue(json, payloadClass);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to deserialize snapshot payload of type " + payloadClassName, e);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> readMetadata(String json) {
        try {
            return objectMapper.readValue(json, LinkedHashMap.class);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to deserialize snapshot metadata", e);
        }
    }
}
