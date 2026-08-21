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
package com.highkeen.opendcb.snapshot.postgres.endtoend;

import com.highkeen.opendcb.eventstore.postgres.PostgresEventStoreStorage;
import com.highkeen.opendcb.integrations.axon.AbstractDcbEventStorageEngine;
import com.highkeen.opendcb.snapshot.postgres.PostgresSnapshotStore;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The test that actually proves the feature works, not just the storage primitive: a real
 * {@code eventstore-postgres}-backed {@link EventStorageEngine} (via {@code
 * integrations/eventstore-axon}'s {@code AbstractDcbEventStorageEngine}) is combined with this
 * module's {@link PostgresSnapshotStore} by registering it as a {@code SnapshotStore} component on
 * an {@link EventSourcingConfigurer} -- letting Axon's own {@code EventSourcingConfigurationDefaults}
 * auto-decorate the engine with {@code SnapshotCapableEventStorageEngine}, the idiomatic path
 * (confirmed from real 5.1.2 source) rather than manually constructing the decorator.
 *
 * <p>{@link CounterEntity} is annotated {@code @Snapshotting(afterEvents = 2)}. Per {@code
 * SnapshotPolicy.afterEvents}'s real semantics (also confirmed from source), that threshold is
 * evaluated per sourcing/load operation, not as a cumulative count across the whole log -- so this
 * test dispatches several {@link IncrementCounter} commands (each forcing a real load via {@code
 * @InjectEntity}) against a single configurer until one load applies more than 2 events, which
 * triggers a snapshot store.
 *
 * <p>Real PostgreSQL 16 (Testcontainers, no mocking), matching this repo's no-mocking-on-
 * concurrency/correctness-critical-paths standard.
 */
@Testcontainers
class PostgresSnapshotStoreEndToEndTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    @Test
    void sourcingAfterASnapshotReadsFewerEventsThanWereActuallyAppended() throws Exception {
        DataSource dataSource = newDataSource();
        String counterId = UUID.randomUUID().toString();

        new PostgresSnapshotStore(dataSource).ensureSchema();

        PostgresEventStoreStorage firstStorage = new PostgresEventStoreStorage(dataSource);
        firstStorage.ensureSchema();
        EventStorageEngine firstEngine = new AbstractDcbEventStorageEngine(firstStorage, new JacksonConverter());

        int totalEventsAppended;
        AxonConfiguration firstConfiguration = counterConfigurer(dataSource)
                .registerEventStorageEngine(c -> firstEngine)
                .start();
        try {
            CommandGateway commandGateway = firstConfiguration.getComponent(CommandGateway.class);
            commandGateway.sendAndWait(new CreateCounter(counterId));
            totalEventsAppended = 1;

            // Loads #1 and #2 apply 1 and 2 events respectively (not > 2) -- no snapshot yet.
            // Load #3 applies 3 events (CounterCreated + 2 x CounterIncremented, > 2) -- snapshot
            // triggered. Two further increments run past that point too, so the log ends up
            // materially longer than what a snapshot-based load below should need to read.
            int increments = 5;
            for (int i = 0; i < increments; i++) {
                commandGateway.sendAndWait(new IncrementCounter(counterId));
                totalEventsAppended++;
            }
        } finally {
            firstConfiguration.shutdown();
        }

        assertTrue(snapshotRowExists(dataSource, counterId), "Expected a snapshot row to exist after crossing afterEvents=2");

        // Second, independent configurer against the SAME Postgres database -- same
        // "brand-new configurer proves durability" pattern as OpenDcbAxonPostgresTest -- wired
        // with a counting decorator so we can directly observe how many events THIS engine reads
        // while sourcing the entity.
        CountingEventStoreStorage countingStorage =
                new CountingEventStoreStorage(new PostgresEventStoreStorage(dataSource));
        EventStorageEngine secondEngine = new AbstractDcbEventStorageEngine(countingStorage, new JacksonConverter());

        AxonConfiguration secondConfiguration = counterConfigurer(dataSource)
                .registerEventStorageEngine(c -> secondEngine)
                .start();
        try {
            CommandGateway commandGateway = secondConfiguration.getComponent(CommandGateway.class);
            // @InjectEntity forces a genuine sourcing/load -- this is the operation under test.
            commandGateway.sendAndWait(new IncrementCounter(counterId));
        } finally {
            secondConfiguration.shutdown();
        }

        assertTrue(
                countingStorage.eventsRead() < totalEventsAppended,
                "Expected sourcing via the snapshot to read fewer events (" + countingStorage.eventsRead()
                        + ") than were actually appended (" + totalEventsAppended
                        + ") to the log -- otherwise the snapshot was never used, and the entity was "
                        + "sourced from scratch instead.");
    }

    private static EventSourcingConfigurer counterConfigurer(DataSource dataSource) {
        var stateEntity = EventSourcedEntityModule.autodetected(String.class, CounterEntity.class);
        var commandHandlingModule = CommandHandlingModule.named("Counter")
                .commandHandlers()
                .autodetectedCommandHandlingComponent(c -> new CounterCommandHandlers());
        return EventSourcingConfigurer.create()
                .registerEntity(stateEntity)
                .registerCommandHandlingModule(commandHandlingModule)
                .componentRegistry(cr -> cr.registerComponent(
                        SnapshotStore.class, c -> new PostgresSnapshotStore(dataSource)));
    }

    private static boolean snapshotRowExists(DataSource dataSource, String counterId) throws Exception {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT COUNT(*) FROM snapshot WHERE identifier = ?")) {
            statement.setString(1, counterId);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getInt(1) == 1;
            }
        }
    }

    private static DataSource newDataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(POSTGRES.getJdbcUrl());
        dataSource.setUser(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        return dataSource;
    }
}
