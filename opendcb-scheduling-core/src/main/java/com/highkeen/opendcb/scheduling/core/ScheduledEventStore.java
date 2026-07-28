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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.highkeen.opendcb.eventstore.core.StoredEvent;
import com.highkeen.opendcb.eventstore.core.StoredEvent.StoredTag;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Owns the {@code scheduled_event} table -- its own schema, fully independent of the event log's own
 * tables ({@code events}/{@code event_tags} in {@code eventstore-postgres}). Framework-agnostic: this
 * class has no dependency on {@code org.axonframework} or any {@code EventStoreStorage} provider; it
 * only depends on a plain {@link DataSource}.
 *
 * <p>Cross-JVM claim safety ({@link #claimDue}) is achieved with {@code SELECT ... FOR UPDATE SKIP
 * LOCKED}: rows matching the due-or-lease-expired predicate that are already locked by a concurrent
 * transaction are silently skipped rather than blocked on, so multiple JVMs polling concurrently never
 * double-claim a row and never queue up waiting on each other. This is verified directly against real
 * Postgres locking semantics (not assumed): {@code SKIP LOCKED} applies to whichever rows the {@code
 * WHERE} clause matches regardless of how complex that clause is (an {@code OR} across the PENDING-due
 * and IN_PROGRESS-lease-expired conditions here is no different from a single simple condition), and a
 * locked row is only considered for skipping in the first place if it currently matches {@code WHERE}
 * -- Postgres re-checks the row's latest committed version when attempting to lock it, so a row another
 * worker already claimed and committed simply stops matching and is never even a candidate.
 *
 * <p>Callers must invoke {@link #ensureSchema()} once at startup before using this class.
 */
public class ScheduledEventStore {

    private static final String TABLE_DDL = """
            CREATE TABLE IF NOT EXISTS scheduled_event (
                id UUID PRIMARY KEY,
                scheduled_time TIMESTAMPTZ NOT NULL,
                event_id TEXT NOT NULL UNIQUE,
                message_type TEXT NOT NULL,
                payload_class TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                tags_json TEXT NOT NULL,
                scope_name TEXT NOT NULL,
                status TEXT NOT NULL,
                claimed_at TIMESTAMPTZ,
                worker_id TEXT,
                created_at TIMESTAMPTZ NOT NULL
            )""";

    private final DataSource dataSource;
    private final ObjectMapper objectMapper;

    public ScheduledEventStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.objectMapper = new ObjectMapper();
    }

    /** Creates the {@code scheduled_event} table if it doesn't already exist. */
    public void ensureSchema() {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(TABLE_DDL);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to ensure scheduled_event schema", e);
        }
    }

    /**
     * Inserts a {@code PENDING} row for {@code eventToFire} to be fired at {@code scheduledTime}.
     * Only {@code eventId}/{@code messageType}/{@code payloadClass}/{@code payloadJson}/{@code
     * metadata}/{@code tags} are stored -- {@code eventToFire.position()} and {@code
     * eventToFire.timestamp()} are ignored, since the fired event's real position and timestamp are
     * assigned at fire time by {@link ScheduledEventDispatcher}, not at schedule time.
     */
    public UUID schedule(Instant scheduledTime, StoredEvent eventToFire, String scopeName) {
        UUID id = UUID.randomUUID();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement("""
                        INSERT INTO scheduled_event
                            (id, scheduled_time, event_id, message_type, payload_class, payload_json,
                             metadata_json, tags_json, scope_name, status, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?)
                        """)) {
            statement.setObject(1, id);
            statement.setTimestamp(2, Timestamp.from(scheduledTime));
            statement.setString(3, eventToFire.eventId());
            statement.setString(4, eventToFire.messageType());
            statement.setString(5, eventToFire.payloadClass());
            statement.setString(6, eventToFire.payloadJson());
            statement.setString(7, writeJson(eventToFire.metadata()));
            statement.setString(8, writeJson(eventToFire.tags()));
            statement.setString(9, scopeName);
            statement.setTimestamp(10, Timestamp.from(Instant.now()));
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to schedule event " + eventToFire.eventId(), e);
        }
        return id;
    }

    /**
     * Marks {@code scheduleId} {@code CANCELLED}, but only if it is still {@code PENDING} (the {@code
     * UPDATE ... WHERE status = 'PENDING'} guard). If it has already been claimed ({@code
     * IN_PROGRESS}), completed, or cancelled, this is a safe no-op rather than an exception: a worker
     * may already be mid-append for it, so cancelling something already claimed is a normal race
     * against a concurrent {@link #claimDue}, not a bug to surface to the caller. It is deliberately
     * not cancellable once {@code IN_PROGRESS} for the same reason.
     */
    public void cancel(UUID scheduleId) {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "UPDATE scheduled_event SET status = 'CANCELLED' WHERE id = ? AND status = 'PENDING'")) {
            statement.setObject(1, scheduleId);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to cancel scheduled event " + scheduleId, e);
        }
    }

    /**
     * Atomically claims up to {@code batchSize} rows that are either {@code PENDING} and due ({@code
     * scheduled_time <= now}), or {@code IN_PROGRESS} with an expired lease ({@code claimed_at +
     * leaseDuration <= now} -- a crashed or stalled worker's stale claim), marking them {@code
     * IN_PROGRESS} with {@code claimed_at = now} and {@code worker_id = workerId}. Safe for multiple
     * concurrent JVMs: see the class Javadoc for why {@code SELECT ... FOR UPDATE SKIP LOCKED} makes
     * this safe without double-claiming or blocking on rows this call isn't claiming.
     *
     * <p>The returned records reflect the post-claim state ({@code status = IN_PROGRESS}, {@code
     * claimedAt = now}, {@code workerId = workerId}) -- the state actually committed, not whatever the
     * row held before this call.
     */
    public List<ScheduledEventRecord> claimDue(Instant now, int batchSize, String workerId, Duration leaseDuration) {
        Instant leaseExpiredBefore = now.minus(leaseDuration);
        List<ScheduledEventRecord> claimed = new ArrayList<>();
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                try (PreparedStatement select = connection.prepareStatement("""
                        SELECT id, scheduled_time, event_id, message_type, payload_class, payload_json,
                               metadata_json, tags_json, scope_name, created_at
                        FROM scheduled_event
                        WHERE (status = 'PENDING' AND scheduled_time <= ?)
                           OR (status = 'IN_PROGRESS' AND claimed_at <= ?)
                        ORDER BY scheduled_time
                        LIMIT ?
                        FOR UPDATE SKIP LOCKED
                        """)) {
                    select.setTimestamp(1, Timestamp.from(now));
                    select.setTimestamp(2, Timestamp.from(leaseExpiredBefore));
                    select.setInt(3, batchSize);
                    try (ResultSet resultSet = select.executeQuery()) {
                        while (resultSet.next()) {
                            claimed.add(readClaimedRow(resultSet, now, workerId));
                        }
                    }
                }
                if (!claimed.isEmpty()) {
                    try (PreparedStatement update = connection.prepareStatement(
                            "UPDATE scheduled_event SET status = 'IN_PROGRESS', claimed_at = ?, "
                                    + "worker_id = ? WHERE id = ?")) {
                        for (ScheduledEventRecord record : claimed) {
                            update.setTimestamp(1, Timestamp.from(now));
                            update.setString(2, workerId);
                            update.setObject(3, record.id());
                            update.addBatch();
                        }
                        update.executeBatch();
                    }
                }
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw new IllegalStateException("Failed to claim due scheduled events", e);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to obtain database connection for claimDue", e);
        }
        return claimed;
    }

    /**
     * Marks {@code scheduleId} {@code COMPLETED}, but only if it is still {@code IN_PROGRESS} and still
     * claimed by {@code workerId} -- the {@code UPDATE ... WHERE status = 'IN_PROGRESS' AND worker_id =
     * ?} guard.
     *
     * <p>Guarding on {@code status} alone is not enough: if this worker's lease expired and {@link
     * #claimDue} let a different worker reclaim the row, the row is {@code IN_PROGRESS} again --
     * just owned by that other worker. A status-only guard would still match and let this worker's
     * late completion call overwrite the reclaiming worker's in-flight claim. Fencing on {@code
     * worker_id} too closes that gap: once reclaimed, this worker's {@code workerId} no longer matches,
     * so its late call is a genuine no-op rather than silently corrupting the new claim.
     */
    public void markCompleted(UUID scheduleId, String workerId) {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "UPDATE scheduled_event SET status = 'COMPLETED' "
                                + "WHERE id = ? AND status = 'IN_PROGRESS' AND worker_id = ?")) {
            statement.setObject(1, scheduleId);
            statement.setString(2, workerId);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to mark scheduled event " + scheduleId + " completed", e);
        }
    }

    private ScheduledEventRecord readClaimedRow(ResultSet resultSet, Instant claimedAt, String workerId)
            throws SQLException {
        return new ScheduledEventRecord(
                (UUID) resultSet.getObject("id"),
                resultSet.getTimestamp("scheduled_time").toInstant(),
                resultSet.getString("event_id"),
                resultSet.getString("message_type"),
                resultSet.getString("payload_class"),
                resultSet.getString("payload_json"),
                readJsonMap(resultSet.getString("metadata_json")),
                readJsonTags(resultSet.getString("tags_json")),
                resultSet.getString("scope_name"),
                ScheduledEventStatus.IN_PROGRESS,
                claimedAt,
                workerId,
                resultSet.getTimestamp("created_at").toInstant());
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize scheduled event field to JSON", e);
        }
    }

    private Map<String, String> readJsonMap(String json) {
        try {
            return objectMapper.readValue(json, new TypeReference<Map<String, String>>() {});
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize scheduled event metadata JSON", e);
        }
    }

    private Set<StoredTag> readJsonTags(String json) {
        try {
            return objectMapper.readValue(json, new TypeReference<Set<StoredTag>>() {});
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize scheduled event tags JSON", e);
        }
    }
}
