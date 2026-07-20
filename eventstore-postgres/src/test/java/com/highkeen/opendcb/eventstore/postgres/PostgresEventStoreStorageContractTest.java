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

import com.highkeen.opendcb.eventstore.core.EventStoreStorage;
import com.highkeen.opendcb.eventstore.core.EventStoreStorageContractTest;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Runs the shared {@link EventStoreStorageContractTest} suite against a real PostgreSQL 16
 * instance. One container is shared across every test method in this class (started once via
 * {@code @Testcontainers}/{@code @Container}); each test truncates the tables beforehand so the
 * contract suite's "backing store is empty at the start of each test" assumption holds.
 */
@Testcontainers
class PostgresEventStoreStorageContractTest extends EventStoreStorageContractTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    private static PostgresEventStoreStorage storage;
    private static PostgresEventStoreStorage anotherStorage;

    @BeforeAll
    static void setUpSchema() {
        storage = new PostgresEventStoreStorage(newDataSource());
        anotherStorage = new PostgresEventStoreStorage(newDataSource());
        storage.ensureSchema();
    }

    @BeforeEach
    void resetTables() throws SQLException {
        try (Connection connection = newDataSource().getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE TABLE event_tags, events RESTART IDENTITY CASCADE");
        }
    }

    @Override
    protected EventStoreStorage storage() {
        return storage;
    }

    @Override
    protected EventStoreStorage anotherStorageInstance() {
        return anotherStorage;
    }

    private static PGSimpleDataSource newDataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(POSTGRES.getJdbcUrl());
        dataSource.setUser(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        return dataSource;
    }
}
