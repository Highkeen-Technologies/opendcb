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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers {@link SagaCorrelationStore} against a real PostgreSQL 16
 * Testcontainers instance -- no Conductor server needed for these cases,
 * since they only exercise the JDBC layer. The true-concurrency race test for
 * {@link ConductorSagaBridge#startSagaIfNotAlreadyRunning} lives in {@link
 * ConductorSagaBridgeIntegrationTest} instead, since proving "exactly one
 * Conductor workflow started" requires a real Conductor server, not just this
 * table.
 */
@Testcontainers
class SagaCorrelationStoreTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    private SagaCorrelationStore store;

    @BeforeEach
    void setUp() throws SQLException {
        DataSource dataSource = newDataSource();
        store = new SagaCorrelationStore(dataSource);
        store.ensureSchema();
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE TABLE saga_correlation");
        }
    }

    @Test
    void firstRecordCorrelationForANewKeySucceeds() {
        assertTrue(store.recordCorrelation("order-1", null, "OrderFulfillmentSaga"));
        assertEquals(Optional.empty(), store.findWorkflowId("order-1"));
    }

    @Test
    void secondRecordCorrelationForTheSameKeyReturnsFalseAndDoesNotOverwriteTheExistingRow() {
        assertTrue(store.recordCorrelation("order-1", "workflow-a", "OrderFulfillmentSaga"));
        assertFalse(store.recordCorrelation("order-1", "workflow-b", "OrderFulfillmentSaga"));

        assertEquals(Optional.of("workflow-a"), store.findWorkflowId("order-1"));
    }

    @Test
    void updateWorkflowIdFillsInAPreviouslyReservedRow() {
        store.recordCorrelation("order-1", null, "OrderFulfillmentSaga");
        assertEquals(Optional.empty(), store.findWorkflowId("order-1"));

        store.updateWorkflowId("order-1", "workflow-a");

        assertEquals(Optional.of("workflow-a"), store.findWorkflowId("order-1"));
    }

    @Test
    void findWorkflowIdForAnUnknownKeyIsEmpty() {
        assertEquals(Optional.empty(), store.findWorkflowId("does-not-exist"));
    }

    private static DataSource newDataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(POSTGRES.getJdbcUrl());
        ds.setUser(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        return ds;
    }
}
