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
package com.highkeen.opendcb.bootstrap.axon.postgres;

import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.modelling.StateManager;
import org.axonframework.modelling.repository.ManagedEntity;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Proves {@link OpenDcbAxonPostgres#engine} wires {@code eventstore-axon} and {@code eventstore-postgres} together
 * for real: a command dispatched against one {@code EventSourcingConfigurer} is durably readable by sourcing the
 * same entity from a second, brand-new {@code EventSourcingConfigurer} backed by the same PostgreSQL database —
 * proof that persistence happened via Postgres, not just in-memory within one configurer.
 */
@Testcontainers
class OpenDcbAxonPostgresTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    @Test
    void commandDispatchedAgainstOneConfigurerIsDurablyReadableFromABrandNewConfigurer() {
        DataSource dataSource = newDataSource();
        String accountId = "acct-1";

        EventStorageEngine firstEngine = OpenDcbAxonPostgres.engine(dataSource);
        AxonConfiguration firstConfiguration =
                AccountEntity.configurer().registerEventStorageEngine(c -> firstEngine).start();
        try {
            CommandGateway commandGateway = firstConfiguration.getComponent(CommandGateway.class);
            commandGateway.sendAndWait(new OpenAccount(accountId, "Ada Lovelace"));
        } finally {
            firstConfiguration.shutdown();
        }

        // Schema already exists by now; also exercises the autoCreateSchema=false path.
        EventStorageEngine secondEngine = OpenDcbAxonPostgres.engine(dataSource, false);
        AxonConfiguration secondConfiguration =
                AccountEntity.configurer().registerEventStorageEngine(c -> secondEngine).start();
        try {
            UnitOfWorkFactory unitOfWorkFactory = secondConfiguration.getComponent(UnitOfWorkFactory.class);
            AccountEntity loaded = unitOfWorkFactory.create()
                    .executeWithResult(ctx -> ctx.component(StateManager.class)
                            .repository(AccountEntity.class, String.class)
                            .load(accountId, ctx)
                            .thenApply(ManagedEntity::entity))
                    .join();

            assertEquals(accountId, loaded.accountId());
            assertEquals("Ada Lovelace", loaded.ownerName());
        } finally {
            secondConfiguration.shutdown();
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
