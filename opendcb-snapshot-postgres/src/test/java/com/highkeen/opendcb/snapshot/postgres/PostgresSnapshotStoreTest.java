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

import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Real PostgreSQL 16 (Testcontainers, no mocking), matching this repo's no-mocking-on-storage-
 * layer standard.
 */
@Testcontainers
class PostgresSnapshotStoreTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    private PostgresSnapshotStore store;
    private DataSource dataSource;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = newDataSource();
        store = new PostgresSnapshotStore(dataSource);
        store.ensureSchema();
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE TABLE snapshot");
        }
    }

    @Test
    void storeThenLoadRoundTripsTheExactSnapshot() {
        QualifiedName qualifiedName = new QualifiedName("com.example.Account");
        String identifier = "acct-1";
        Instant timestamp = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        Snapshot original = new Snapshot(
                new GlobalIndexPosition(42L),
                "1.0",
                new AccountSnapshotPayload("acct-1", "Ada Lovelace", 150L),
                timestamp,
                Map.of("source", "test")
        );

        store.store(qualifiedName, identifier, original).join();
        Snapshot loaded = store.load(qualifiedName, identifier).join();

        assertEquals(GlobalIndexPosition.toIndex(original.position()), GlobalIndexPosition.toIndex(loaded.position()));
        assertEquals(original.version(), loaded.version());
        assertEquals(original.payload(), loaded.payload());
        assertEquals(original.timestamp(), loaded.timestamp());
        assertEquals(original.metadata(), loaded.metadata());
    }

    @Test
    void secondStoreReplacesTheFirstAndLeavesExactlyOneRow() throws Exception {
        QualifiedName qualifiedName = new QualifiedName("com.example.Account");
        String identifier = "acct-2";
        Snapshot first = new Snapshot(
                new GlobalIndexPosition(1L), "1.0",
                new AccountSnapshotPayload("acct-2", "Grace Hopper", 10L),
                Instant.now().truncatedTo(ChronoUnit.MILLIS), Map.of());
        Snapshot second = new Snapshot(
                new GlobalIndexPosition(99L), "1.0",
                new AccountSnapshotPayload("acct-2", "Grace Hopper", 999L),
                Instant.now().truncatedTo(ChronoUnit.MILLIS), Map.of());

        store.store(qualifiedName, identifier, first).join();
        store.store(qualifiedName, identifier, second).join();

        Snapshot loaded = store.load(qualifiedName, identifier).join();
        assertEquals(second.payload(), loaded.payload());
        assertEquals(GlobalIndexPosition.toIndex(second.position()), GlobalIndexPosition.toIndex(loaded.position()));

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT COUNT(*) FROM snapshot WHERE qualified_name = ? AND identifier = ?")) {
            statement.setString(1, qualifiedName.name());
            statement.setString(2, identifier);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                assertEquals(1, resultSet.getInt(1));
            }
        }
    }

    @Test
    void loadForNeverStoredIdentifierCompletesWithNull() {
        Snapshot loaded = store.load(new QualifiedName("com.example.Account"), "never-stored").join();
        assertNull(loaded);
    }

    private static DataSource newDataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(POSTGRES.getJdbcUrl());
        dataSource.setUser(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        return dataSource;
    }

    public record AccountSnapshotPayload(String accountId, String ownerName, long balance) {
    }
}
