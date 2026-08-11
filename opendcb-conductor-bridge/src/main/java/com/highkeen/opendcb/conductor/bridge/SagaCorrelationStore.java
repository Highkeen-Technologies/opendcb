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
package com.highkeen.opendcb.conductor.bridge;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;

/**
 * Plain-JDBC, provider-agnostic ANSI SQL store for {@code saga_correlation} --
 * the mapping from a domain correlation key to the Conductor workflow instance
 * it started, standing in for the automatic association-property index Axon's
 * own saga support provides for free (see {@link ConductorSagaBridge}'s class
 * Javadoc). Same shape as {@code outbox-relay-core}'s {@code
 * JdbcRelayPositionStore}: no vendor-specific SQL, no upsert syntax.
 *
 * <p>{@link #recordCorrelation} relies on the {@code correlation_key} primary
 * key to serialize concurrent "start this saga" attempts across JVMs: it
 * always attempts a plain {@code INSERT} and treats the database rejecting a
 * duplicate key as the signal that another caller already won, rather than
 * checking first and inserting second -- the same insert-and-let-the-database-
 * reject pattern {@code examples/microservices-sample}'s {@code
 * processed_integration_events} table uses for redelivery dedup.
 *
 * <p>{@code conductor_workflow_id} is nullable, not {@code NOT NULL} --
 * deliberately, not an oversight. The real Conductor workflow ID isn't known
 * until <i>after</i> the winning caller's {@code startWorkflow} call returns,
 * but the row that breaks the concurrent-start race must be inserted
 * <i>before</i> that call happens, or the race isn't actually prevented (see
 * {@link ConductorSagaBridge}'s class Javadoc). {@link #recordCorrelation} is
 * therefore called first with a {@code null} workflow ID to reserve the row,
 * and {@link #updateWorkflowId} fills it in afterward -- a plain, un-raced
 * update, since only the caller whose insert won ever calls it.
 */
public class SagaCorrelationStore {

    private static final String UNIQUE_VIOLATION_SQL_STATE = "23505";

    private static final String TABLE_DDL = """
            CREATE TABLE IF NOT EXISTS saga_correlation (
                correlation_key VARCHAR(255) PRIMARY KEY,
                conductor_workflow_id VARCHAR(255),
                saga_type VARCHAR(255) NOT NULL,
                created_at TIMESTAMP NOT NULL
            )""";

    private final DataSource dataSource;

    public SagaCorrelationStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** Creates the {@code saga_correlation} table if it doesn't already exist. */
    public void ensureSchema() {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(TABLE_DDL);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to ensure saga_correlation schema", e);
        }
    }

    /**
     * Attempts to record that {@code correlationKey} started {@code
     * conductorWorkflowId}. Returns {@code true} only if this call's own
     * insert succeeded -- {@code false} means the correlation key already
     * existed (some other caller, possibly concurrently, already started
     * this saga), which callers should treat as "already started," not an
     * error.
     */
    public boolean recordCorrelation(String correlationKey, String conductorWorkflowId, String sagaType) {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO saga_correlation "
                                + "(correlation_key, conductor_workflow_id, saga_type, created_at) "
                                + "VALUES (?, ?, ?, ?)")) {
            statement.setString(1, correlationKey);
            statement.setString(2, conductorWorkflowId);
            statement.setString(3, sagaType);
            statement.setTimestamp(4, Timestamp.from(Instant.now()));
            statement.executeUpdate();
            return true;
        } catch (SQLException e) {
            if (UNIQUE_VIOLATION_SQL_STATE.equals(e.getSQLState())) {
                return false;
            }
            throw new IllegalStateException("Failed to record saga correlation for key '" + correlationKey + "'", e);
        }
    }

    /**
     * Looks up the Conductor workflow ID a correlation key was mapped to, for
     * routing a later correlated event's signal to the right running
     * instance. Returns empty both when no row exists for {@code
     * correlationKey}, and when a row exists but its reservation hasn't been
     * filled in yet (see this class's Javadoc) -- callers that need to
     * distinguish "never started" from "starting right now" should not need
     * to in practice, since the latter is a narrow, transient window.
     */
    public Optional<String> findWorkflowId(String correlationKey) {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT conductor_workflow_id FROM saga_correlation WHERE correlation_key = ?")) {
            statement.setString(1, correlationKey);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }
                return Optional.ofNullable(resultSet.getString(1));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to look up saga correlation for key '" + correlationKey + "'", e);
        }
    }

    /**
     * Fills in the real Conductor workflow ID for a row previously reserved
     * by {@link #recordCorrelation} with a {@code null} workflow ID. Only the
     * caller whose {@code recordCorrelation} insert won the race should ever
     * call this -- it is a plain, un-raced update.
     */
    public void updateWorkflowId(String correlationKey, String conductorWorkflowId) {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "UPDATE saga_correlation SET conductor_workflow_id = ? WHERE correlation_key = ?")) {
            statement.setString(1, conductorWorkflowId);
            statement.setString(2, correlationKey);
            int updated = statement.executeUpdate();
            if (updated == 0) {
                throw new IllegalStateException(
                        "No saga_correlation row reserved for key '" + correlationKey + "' to update");
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to update saga correlation workflow id for key '" + correlationKey + "'", e);
        }
    }
}
