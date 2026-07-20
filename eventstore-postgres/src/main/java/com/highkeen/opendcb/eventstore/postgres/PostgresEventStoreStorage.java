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
package com.highkeen.opendcb.eventstore.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.highkeen.opendcb.eventstore.core.EventStoreStorage;
import com.highkeen.opendcb.eventstore.core.StoredEvent;
import com.highkeen.opendcb.eventstore.core.StoredEvent.StoredTag;

import javax.sql.DataSource;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

/**
 * PostgreSQL {@link EventStoreStorage} implementation using plain JDBC — no ORM, no framework
 * dependency. See {@code docs/PROVIDERS.md} for the shape every provider follows.
 *
 * <p>Cross-JVM append serialization is achieved with a transaction-scoped Postgres advisory lock
 * ({@code pg_advisory_xact_lock}), taken before the conflict check and released automatically on
 * commit/rollback. This serializes {@link #appendAtomically} calls across every JVM connected to
 * the same database, not just threads within one process.
 *
 * <p>Callers must invoke {@link #ensureSchema()} once at startup before using this class.
 */
public class PostgresEventStoreStorage implements EventStoreStorage {

    /**
     * Fixed key for the transaction-scoped advisory lock guarding {@link #appendAtomically}.
     * Arbitrary but stable — every JVM using this storage must take the same lock key to
     * actually serialize against each other.
     */
    private static final long APPEND_LOCK_KEY = 8_872_034_927_412L;

    private final DataSource dataSource;
    private final ObjectMapper objectMapper;

    public PostgresEventStoreStorage(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.objectMapper = new ObjectMapper();
    }

    /** Creates the {@code events}/{@code event_tags} tables and indexes if they don't already exist. */
    public void ensureSchema() {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS events (
                        position BIGSERIAL PRIMARY KEY,
                        event_id TEXT UNIQUE NOT NULL,
                        message_type TEXT NOT NULL,
                        payload_class TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        metadata_json TEXT NOT NULL,
                        occurred_at TIMESTAMPTZ NOT NULL
                    )
                    """);
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS event_tags (
                        position BIGINT NOT NULL REFERENCES events(position),
                        tag_key TEXT NOT NULL,
                        tag_value TEXT NOT NULL
                    )
                    """);
            statement.execute(
                    "CREATE INDEX IF NOT EXISTS idx_event_tags_key_value ON event_tags (tag_key, tag_value)");
            statement.execute("CREATE INDEX IF NOT EXISTS idx_event_tags_position ON event_tags (position)");
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to ensure eventstore-postgres schema.", e);
        }
    }

    @Override
    public long appendAtomically(
            List<StoredEvent> events,
            long conflictCheckFromPositionExclusive,
            Predicate<StoredEvent> conflictsIfMatched)
            throws ConcurrentAppendConflictException {
        if (events.isEmpty()) {
            throw new IllegalArgumentException("events must not be empty");
        }
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                takeAdvisoryLock(connection);
                List<StoredEvent> tail =
                        readEvents(connection, conflictCheckFromPositionExclusive, null, Integer.MAX_VALUE);
                for (StoredEvent candidate : tail) {
                    if (conflictsIfMatched.test(candidate)) {
                        connection.rollback();
                        throw new ConcurrentAppendConflictException(
                                "Conflicting event found at position " + candidate.position()
                                        + " during append.");
                    }
                }
                long lastPosition = 0L;
                for (StoredEvent event : events) {
                    lastPosition = insertEvent(connection, event);
                }
                connection.commit();
                return lastPosition;
            } catch (ConcurrentAppendConflictException e) {
                throw e;
            } catch (SQLException e) {
                connection.rollback();
                throw new IllegalStateException("Failed to append events atomically.", e);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to obtain database connection for append.", e);
        }
    }

    @Override
    public List<StoredEvent> readRange(
            long fromPositionExclusive, Long toPositionInclusiveOrNull, int maxBatchSize) {
        try (Connection connection = dataSource.getConnection()) {
            return readEvents(connection, fromPositionExclusive, toPositionInclusiveOrNull, maxBatchSize);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to read event range.", e);
        }
    }

    @Override
    public long maxPosition() {
        return singleLongQuery("SELECT COALESCE(MAX(position), 0) FROM events");
    }

    @Override
    public long minPosition() {
        return singleLongQuery("SELECT COALESCE(MIN(position), 0) FROM events");
    }

    @Override
    public Optional<Long> positionAtOrAfter(Instant at) {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement =
                        connection.prepareStatement("SELECT MIN(position) FROM events WHERE occurred_at >= ?")) {
            statement.setTimestamp(1, Timestamp.from(at));
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                long position = resultSet.getLong(1);
                return resultSet.wasNull() ? Optional.empty() : Optional.of(position);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to query positionAtOrAfter.", e);
        }
    }

    private void takeAdvisoryLock(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT pg_advisory_xact_lock(?)")) {
            statement.setLong(1, APPEND_LOCK_KEY);
            statement.execute();
        }
    }

    private long insertEvent(Connection connection, StoredEvent event) throws SQLException {
        long position;
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO events (event_id, message_type, payload_class, payload_json, metadata_json, "
                        + "occurred_at) VALUES (?, ?, ?, ?, ?, ?) RETURNING position")) {
            statement.setString(1, event.eventId());
            statement.setString(2, event.messageType());
            statement.setString(3, event.payloadClass());
            statement.setString(4, event.payloadJson());
            statement.setString(5, writeMetadataJson(event.metadata()));
            statement.setTimestamp(6, Timestamp.from(event.timestamp()));
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                position = resultSet.getLong(1);
            }
        }
        if (!event.tags().isEmpty()) {
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO event_tags (position, tag_key, tag_value) VALUES (?, ?, ?)")) {
                for (StoredTag tag : event.tags()) {
                    statement.setLong(1, position);
                    statement.setString(2, tag.key());
                    statement.setString(3, tag.value());
                    statement.addBatch();
                }
                statement.executeBatch();
            }
        }
        return position;
    }

    private List<StoredEvent> readEvents(
            Connection connection, long fromPositionExclusive, Long toPositionInclusiveOrNull, int maxBatchSize)
            throws SQLException {
        StringBuilder sql = new StringBuilder(
                "SELECT position, event_id, message_type, payload_class, payload_json, metadata_json, "
                        + "occurred_at FROM events WHERE position > ?");
        if (toPositionInclusiveOrNull != null) {
            sql.append(" AND position <= ?");
        }
        sql.append(" ORDER BY position LIMIT ?");

        Map<Long, EventRow> rowsByPosition = new LinkedHashMap<>();
        try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            int index = 1;
            statement.setLong(index++, fromPositionExclusive);
            if (toPositionInclusiveOrNull != null) {
                statement.setLong(index++, toPositionInclusiveOrNull);
            }
            statement.setInt(index, maxBatchSize);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    long position = resultSet.getLong("position");
                    rowsByPosition.put(
                            position,
                            new EventRow(
                                    position,
                                    resultSet.getString("event_id"),
                                    resultSet.getString("message_type"),
                                    resultSet.getString("payload_class"),
                                    resultSet.getString("payload_json"),
                                    readMetadataJson(resultSet.getString("metadata_json")),
                                    resultSet.getTimestamp("occurred_at").toInstant()));
                }
            }
        }
        if (rowsByPosition.isEmpty()) {
            return List.of();
        }

        Map<Long, Set<StoredTag>> tagsByPosition = readTags(connection, rowsByPosition.keySet());
        List<StoredEvent> events = new ArrayList<>(rowsByPosition.size());
        for (EventRow row : rowsByPosition.values()) {
            events.add(new StoredEvent(
                    row.position(),
                    row.eventId(),
                    row.messageType(),
                    row.payloadClass(),
                    row.payloadJson(),
                    row.metadata(),
                    tagsByPosition.getOrDefault(row.position(), Set.of()),
                    row.timestamp()));
        }
        return events;
    }

    private Map<Long, Set<StoredTag>> readTags(Connection connection, Collection<Long> positions)
            throws SQLException {
        Map<Long, Set<StoredTag>> result = new LinkedHashMap<>();
        Array positionArray = connection.createArrayOf("bigint", positions.toArray());
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT position, tag_key, tag_value FROM event_tags WHERE position = ANY(?) ORDER BY position")) {
            statement.setArray(1, positionArray);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    long position = resultSet.getLong("position");
                    result.computeIfAbsent(position, unused -> new LinkedHashSet<>())
                            .add(new StoredTag(resultSet.getString("tag_key"), resultSet.getString("tag_value")));
                }
            }
        }
        return result;
    }

    private long singleLongQuery(String sql) {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            return resultSet.getLong(1);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to query position from eventstore-postgres.", e);
        }
    }

    private String writeMetadataJson(Map<String, String> metadata) {
        try {
            return objectMapper.writeValueAsString(metadata);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize event metadata to JSON.", e);
        }
    }

    private Map<String, String> readMetadataJson(String json) {
        try {
            return objectMapper.readValue(json, new TypeReference<Map<String, String>>() {});
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize event metadata from JSON.", e);
        }
    }

    /** Raw column data for one {@code events} row, before its tags have been attached. */
    private record EventRow(
            long position,
            String eventId,
            String messageType,
            String payloadClass,
            String payloadJson,
            Map<String, String> metadata,
            Instant timestamp) {}
}
