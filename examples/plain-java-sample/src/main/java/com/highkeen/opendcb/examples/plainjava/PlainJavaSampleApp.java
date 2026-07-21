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
package com.highkeen.opendcb.examples.plainjava;

import com.highkeen.opendcb.bootstrap.axon.postgres.OpenDcbAxonPostgres;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.modelling.StateManager;
import org.axonframework.modelling.repository.ManagedEntity;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.util.UUID;

/**
 * Standalone Java application with no Spring dependency anywhere in its classpath — proves OpenDCB works
 * outside Spring Boot entirely. Everything Spring's {@code opendcb-axon-spring-boot-starter} would normally
 * auto-configure is wired by hand here, in plain {@code main()} code, using nothing but
 * {@code bootstrap-axon-postgres}'s single {@link OpenDcbAxonPostgres#engine} factory call plus Axon's own
 * {@link EventSourcingConfigurer} API.
 *
 * <p>Also doubles as the first true end-to-end smoke test of the whole stack: it dispatches commands
 * against one {@link AxonConfiguration}, then builds a second, completely independent one against the same
 * database to prove the resulting state is durable — read back from Postgres, not held in memory by the
 * first configurer.
 */
public final class PlainJavaSampleApp {

    public static void main(String[] args) {
        DataSource dataSource = buildDataSource();
        String accountId = "acct-" + UUID.randomUUID();

        System.out.println("=== OpenDCB plain-java-sample (no Spring) ===");
        System.out.println("accountId = " + accountId);
        System.out.println();

        EventStorageEngine firstEngine = OpenDcbAxonPostgres.engine(dataSource);
        AxonConfiguration firstConfiguration = accountConfigurer().registerEventStorageEngine(c -> firstEngine).start();
        try {
            CommandGateway commandGateway = firstConfiguration.getComponent(CommandGateway.class);

            System.out.println("Dispatching OpenAccount(" + accountId + ", \"Ada Lovelace\")");
            commandGateway.sendAndWait(new OpenAccount(accountId, "Ada Lovelace"));

            System.out.println("Dispatching DepositMoney(" + accountId + ", 100.00)");
            commandGateway.sendAndWait(new DepositMoney(accountId, new BigDecimal("100.00")));

            System.out.println("Dispatching DepositMoney(" + accountId + ", 50.00)");
            commandGateway.sendAndWait(new DepositMoney(accountId, new BigDecimal("50.00")));
            System.out.println();

            AccountEntity writeSideView = load(firstConfiguration, accountId);
            System.out.println("Write-side state (same configurer that dispatched the commands):");
            System.out.println("  " + describe(writeSideView));
        } finally {
            firstConfiguration.shutdown();
        }

        System.out.println();
        System.out.println("Shutting down the first configurer, then building a brand-new one against the "
                + "same DataSource (autoCreateSchema=false — the schema already exists) to prove durability.");
        System.out.println();

        EventStorageEngine secondEngine = OpenDcbAxonPostgres.engine(dataSource, false);
        AxonConfiguration secondConfiguration = accountConfigurer().registerEventStorageEngine(c -> secondEngine).start();
        try {
            AccountEntity resourcedView = load(secondConfiguration, accountId);
            System.out.println("Re-sourced state (brand-new configurer, same Postgres database):");
            System.out.println("  " + describe(resourcedView));
        } finally {
            secondConfiguration.shutdown();
        }
    }

    private static AccountEntity load(AxonConfiguration configuration, String accountId) {
        UnitOfWorkFactory unitOfWorkFactory = configuration.getComponent(UnitOfWorkFactory.class);
        return unitOfWorkFactory.create()
                .executeWithResult(ctx -> ctx.component(StateManager.class)
                        .repository(AccountEntity.class, String.class)
                        .load(accountId, ctx)
                        .thenApply(ManagedEntity::entity))
                .join();
    }

    private static String describe(AccountEntity account) {
        return "accountId=%s, ownerName=%s, balance=%s".formatted(
                account.accountId(), account.ownerName(), account.balance());
    }

    private static EventSourcingConfigurer accountConfigurer() {
        var stateEntity = EventSourcedEntityModule.autodetected(String.class, AccountEntity.class);
        var commandHandlingModule = CommandHandlingModule.named("Account")
                .commandHandlers()
                .autodetectedCommandHandlingComponent(c -> new AccountCommandHandlers());
        return EventSourcingConfigurer.create()
                .registerEntity(stateEntity)
                .registerCommandHandlingModule(commandHandlingModule);
    }

    private static DataSource buildDataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(env("OPENDCB_SAMPLE_JDBC_URL", "jdbc:postgresql://localhost:5432/opendcb_sample"));
        dataSource.setUser(env("OPENDCB_SAMPLE_DB_USER", "postgres"));
        dataSource.setPassword(env("OPENDCB_SAMPLE_DB_PASSWORD", "postgres"));
        return dataSource;
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value != null ? value : defaultValue;
    }

    private PlainJavaSampleApp() {
    }
}
