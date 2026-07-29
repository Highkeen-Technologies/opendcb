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
 * <p>Each row carries its own {@code attempt_count}/{@code max_attempts} retry budget: {@link
 * #claimDue} dead-letters a row (see {@link ClaimBatch}) rather than claiming it once claiming would
 * exceed that budget, terminating what would otherwise be an indefinite lease-expiry retry loop. This
 * is a {@code DeadLetterSink} mechanism independent of {@code outbox-relay-core}'s, mirroring its shape
 * without depending on it -- see {@link DeadLetterSink}.
 *
 * <p>Each row may optionally carry a {@link ConflictCriteria}, persisted as {@code
 * conflict_criteria_json} (nullable -- {@code null} means no check, preserving prior behavior
 * exactly): {@link ScheduledEventDispatcher} checks it against the log immediately before firing
 * and, on a match, transitions the row straight to {@code SKIPPED_CONFLICT} instead of appending.
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
                created_at TIMESTAMPTZ NOT NULL,
                attempt_count INT NOT NULL DEFAULT 0,
                max_attempts INT NOT NULL,
                conflict_criteria_json TEXT
            )""";

    /**
     * Default {@code maxAttempts} for {@link #schedule(Instant, StoredEvent, String)}: enough claim
     * attempts to ride out a handful of transient failures (e.g. a brief store outage spanning a few
     * lease-expiry cycles) without letting a permanently-broken schedule retry forever with no
     * dead-letter signal.
     */
    public static final int DEFAULT_MAX_ATTEMPTS = 5;

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
     * Equivalent to {@link #schedule(Instant, StoredEvent, String, int)} with {@link
     * #DEFAULT_MAX_ATTEMPTS}.
     */
    public UUID schedule(Instant scheduledTime, StoredEvent eventToFire, String scopeName) {
        return schedule(scheduledTime, eventToFire, scopeName, DEFAULT_MAX_ATTEMPTS);
    }

    /** Equivalent to {@link #schedule(Instant, StoredEvent, String, int, ConflictCriteria)} with no conflict check. */
    public UUID schedule(Instant scheduledTime, StoredEvent eventToFire, String scopeName, int maxAttempts) {
        return schedule(scheduledTime, eventToFire, scopeName, maxAttempts, null);
    }

    /**
     * Inserts a {@code PENDING} row for {@code eventToFire} to be fired at {@code scheduledTime}.
     * Only {@code eventId}/{@code messageType}/{@code payloadClass}/{@code payloadJson}/{@code
     * metadata}/{@code tags} are stored -- {@code eventToFire.position()} and {@code
     * eventToFire.timestamp()} are ignored, since the fired event's real position and timestamp are
     * assigned at fire time by {@link ScheduledEventDispatcher}, not at schedule time.
     *
     * @param maxAttempts the number of times {@link #claimDue} may claim this row before dead-lettering
     *                     it instead (see the class Javadoc's dead-letter section)
     * @param conflictCriteria if non-null, {@link ScheduledEventDispatcher} skips firing this event
     *                          (transitioning it to {@code SKIPPED_CONFLICT} instead) if a matching
     *                          event already exists in the log at fire time; {@code null} means no
     *                          conflict check, the same behavior as before this parameter existed
     */
    public UUID schedule(Instant scheduledTime, StoredEvent eventToFire, String scopeName, int maxAttempts,
            ConflictCriteria conflictCriteria) {
        UUID id = UUID.randomUUID();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement("""
                        INSERT INTO scheduled_event
                            (id, scheduled_time, event_id, message_type, payload_class, payload_json,
                             metadata_json, tags_json, scope_name, status, max_attempts,
                             conflict_criteria_json, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, ?)
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
            statement.setInt(10, maxAttempts);
            statement.setString(11, conflictCriteria == null ? null : writeJson(conflictCriteria));
            statement.setTimestamp(12, Timestamp.from(Instant.now()));
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
     * The result of {@link #claimDue}: rows claimed for dispatch, and rows that instead exceeded their
     * {@code max_attempts} budget this call and were transitioned straight to {@code DEAD_LETTERED}
     * (never {@code IN_PROGRESS}, never returned by a future {@code claimDue} call).
     */
    public record ClaimBatch(List<ScheduledEventRecord> claimed, List<ScheduledEventRecord> deadLettered) {}

    /**
     * Atomically claims up to {@code batchSize} rows that are either {@code PENDING} and due ({@code
     * scheduled_time <= now}), or {@code IN_PROGRESS} with an expired lease ({@code claimed_at +
     * leaseDuration <= now} -- a crashed or stalled worker's stale claim). Safe for multiple concurrent
     * JVMs: see the class Javadoc for why {@code SELECT ... FOR UPDATE SKIP LOCKED} makes this safe
     * without double-claiming or blocking on rows this call isn't claiming.
     *
     * <p>Each candidate row is branched, inside the same locked transaction that identified it, before
     * any claim is committed: if claiming it would push {@code attempt_count} past {@code max_attempts},
     * it is transitioned straight to {@code DEAD_LETTERED} instead of {@code IN_PROGRESS} and is
     * returned in {@link ClaimBatch#deadLettered()}, never {@link ClaimBatch#claimed()} -- it will never
     * be a candidate again. Otherwise it is claimed as before: {@code IN_PROGRESS}, {@code
     * attempt_count + 1}, {@code claimed_at = now}, {@code worker_id = workerId}, returned in {@link
     * ClaimBatch#claimed()}. Branching before claiming (rather than claiming and separately checking)
     * keeps the whole decision inside the one transaction the {@code SKIP LOCKED} guarantee already
     * covers.
     *
     * <p>Claimed records reflect the post-claim state actually committed ({@code status = IN_PROGRESS},
     * {@code claimedAt = now}, {@code workerId = workerId}, incremented {@code attemptCount}), not
     * whatever the row held before this call.
     */
    public ClaimBatch claimDue(Instant now, int batchSize, String workerId, Duration leaseDuration) {
        Instant leaseExpiredBefore = now.minus(leaseDuration);
        List<ScheduledEventRecord> claimed = new ArrayList<>();
        List<ScheduledEventRecord> deadLettered = new ArrayList<>();
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                try (PreparedStatement select = connection.prepareStatement("""
                        SELECT id, scheduled_time, event_id, message_type, payload_class, payload_json,
                               metadata_json, tags_json, scope_name, created_at, claimed_at, worker_id,
                               attempt_count, max_attempts, conflict_criteria_json
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
                            int attemptCount = resultSet.getInt("attempt_count");
                            int maxAttempts = resultSet.getInt("max_attempts");
                            if (attemptCount + 1 > maxAttempts) {
                                deadLettered.add(readRow(resultSet, ScheduledEventStatus.DEAD_LETTERED,
                                        nullableInstant(resultSet, "claimed_at"), resultSet.getString("worker_id"),
                                        attemptCount, maxAttempts));
                            } else {
                                claimed.add(readRow(resultSet, ScheduledEventStatus.IN_PROGRESS, now, workerId,
                                        attemptCount + 1, maxAttempts));
                            }
                        }
                    }
                }
                if (!claimed.isEmpty()) {
                    try (PreparedStatement update = connection.prepareStatement(
                            "UPDATE scheduled_event SET status = 'IN_PROGRESS', claimed_at = ?, "
                                    + "worker_id = ?, attempt_count = ? WHERE id = ?")) {
                        for (ScheduledEventRecord record : claimed) {
                            update.setTimestamp(1, Timestamp.from(now));
                            update.setString(2, workerId);
                            update.setInt(3, record.attemptCount());
                            update.setObject(4, record.id());
                            update.addBatch();
                        }
                        update.executeBatch();
                    }
                }
                if (!deadLettered.isEmpty()) {
                    try (PreparedStatement update = connection.prepareStatement(
                            "UPDATE scheduled_event SET status = 'DEAD_LETTERED' WHERE id = ?")) {
                        for (ScheduledEventRecord record : deadLettered) {
                            update.setObject(1, record.id());
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
        return new ClaimBatch(claimed, deadLettered);
    }

    /**
     * Marks {@code scheduleId} {@code COMPLETED}, but only if it is still {@code IN_PROGRESS} and still
     * claimed by {@code workerId} -- see {@link #updateIfInProgressAndOwned}'s Javadoc for why the
     * {@code worker_id} fencing is necessary, not just the status check.
     */
    public void markCompleted(UUID scheduleId, String workerId) {
        updateIfInProgressAndOwned(scheduleId, workerId, "COMPLETED", "completed");
    }

    /**
     * Marks {@code scheduleId} {@code SKIPPED_CONFLICT} -- {@link ScheduledEventDispatcher} found an
     * event matching the row's {@link ConflictCriteria} already in the log and deliberately chose not
     * to fire it. Same {@code worker_id} fencing as {@link #markCompleted}, via {@link
     * #updateIfInProgressAndOwned}.
     */
    public void markSkippedConflict(UUID scheduleId, String workerId) {
        updateIfInProgressAndOwned(scheduleId, workerId, "SKIPPED_CONFLICT", "skipped (conflict)");
    }

    /**
     * Shared fencing guard behind {@link #markCompleted} and {@link #markSkippedConflict}: {@code
     * UPDATE ... WHERE status = 'IN_PROGRESS' AND worker_id = ?}.
     *
     * <p>Guarding on {@code status} alone is not enough: if this worker's lease expired and {@link
     * #claimDue} let a different worker reclaim the row (or dead-letter it), the row's status has
     * since moved on from underneath this worker -- reclaimed rows are {@code IN_PROGRESS} again, just
     * owned by someone else. A status-only guard would still match a reclaimed row and let this
     * worker's late call overwrite the reclaiming worker's in-flight outcome. Fencing on {@code
     * worker_id} too closes that gap: once reclaimed (or dead-lettered, where {@code worker_id} is left
     * as whichever worker last held it, but {@code status} no longer matches), this worker's late call
     * is a genuine no-op rather than silently corrupting the new state.
     */
    private void updateIfInProgressAndOwned(UUID scheduleId, String workerId, String newStatus, String verbPastTense) {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "UPDATE scheduled_event SET status = ? "
                                + "WHERE id = ? AND status = 'IN_PROGRESS' AND worker_id = ?")) {
            statement.setString(1, newStatus);
            statement.setObject(2, scheduleId);
            statement.setString(3, workerId);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to mark scheduled event " + scheduleId + " " + verbPastTense, e);
        }
    }

    private ScheduledEventRecord readRow(ResultSet resultSet, ScheduledEventStatus status, Instant claimedAt,
            String workerId, int attemptCount, int maxAttempts) throws SQLException {
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
                status,
                claimedAt,
                workerId,
                resultSet.getTimestamp("created_at").toInstant(),
                attemptCount,
                maxAttempts,
                readConflictCriteria(resultSet.getString("conflict_criteria_json")));
    }

    private static Instant nullableInstant(ResultSet resultSet, String column) throws SQLException {
        Timestamp timestamp = resultSet.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
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

    private ConflictCriteria readConflictCriteria(String json) {
        if (json == null) {
            return null;
        }
        try {
            return objectMapper.readValue(json, ConflictCriteria.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize scheduled event conflict criteria JSON", e);
        }
    }
}
