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
package com.highkeen.opendcb.springboot.axon;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.highkeen.opendcb.bootstrap.axon.postgres.OpenDcbAxonPostgres;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wiring tests for {@link OpenDcbPostgresAutoConfiguration} against a stub {@link DataSource} — these
 * verify bean resolution/conditionals only, not real PostgreSQL connectivity (which
 * {@code bootstrap-axon-postgres}'s own Testcontainers-backed test already covers).
 *
 * <p>{@code opendcb.eventstore.auto-create-schema=false} is set throughout so that no test ever
 * triggers a real database connection: {@code PostgresEventStoreStorage} only opens a connection from
 * {@code ensureSchema()}, never from its constructor.
 */
class OpenDcbPostgresAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(OpenDcbPostgresAutoConfiguration.class))
            .withPropertyValues("opendcb.eventstore.auto-create-schema=false");

    @Test
    void fallsBackToPrimaryDataSourceWhenEventStoreDatasourceNotConfigured() {
        Logger logger = (Logger) LoggerFactory.getLogger(OpenDcbPostgresAutoConfiguration.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            contextRunner
                    .withBean("dataSource", DataSource.class, OpenDcbPostgresAutoConfigurationTest::stubDataSource)
                    .run(context -> {
                        DataSource primary = context.getBean("dataSource", DataSource.class);
                        DataSource resolved = context.getBean("openDcbEventStoreDataSource", DataSource.class);
                        assertSame(primary, resolved);
                        assertNotNull(context.getBean(EventStorageEngine.class));
                    });
        } finally {
            logger.detachAppender(appender);
        }
        assertTrue(appender.list.stream()
                .anyMatch(event -> event.getFormattedMessage()
                        .contains("reusing the application's primary DataSource")));
    }

    @Test
    void explicitUsePrimaryReusesPrimaryDataSource() {
        contextRunner
                .withPropertyValues("opendcb.eventstore.datasource.use-primary=true")
                .withBean("dataSource", DataSource.class, OpenDcbPostgresAutoConfigurationTest::stubDataSource)
                .run(context -> {
                    DataSource primary = context.getBean("dataSource", DataSource.class);
                    DataSource resolved = context.getBean("openDcbEventStoreDataSource", DataSource.class);
                    assertSame(primary, resolved);
                    assertNotNull(context.getBean(EventStorageEngine.class));
                });
    }

    @Test
    void explicitDatasourceUrlBuildsADedicatedDataSource() {
        contextRunner
                .withPropertyValues(
                        "opendcb.eventstore.datasource.url=jdbc:postgresql://localhost:5432/eventstore",
                        "opendcb.eventstore.datasource.username=events",
                        "opendcb.eventstore.datasource.password=secret")
                .withBean("dataSource", DataSource.class, OpenDcbPostgresAutoConfigurationTest::stubDataSource)
                .run(context -> {
                    DataSource primary = context.getBean("dataSource", DataSource.class);
                    DataSource resolved = context.getBean("openDcbEventStoreDataSource", DataSource.class);
                    assertNotSame(primary, resolved);
                    assertNotNull(context.getBean(EventStorageEngine.class));
                });
    }

    @Test
    void providerNoneDisablesAutoConfigurationEntirely() {
        contextRunner
                .withPropertyValues("opendcb.eventstore.provider=none")
                .withBean("dataSource", DataSource.class, OpenDcbPostgresAutoConfigurationTest::stubDataSource)
                .run(context -> {
                    assertThrows(NoSuchBeanDefinitionException.class,
                            () -> context.getBean(EventStorageEngine.class));
                    assertThrows(NoSuchBeanDefinitionException.class,
                            () -> context.getBean("openDcbEventStoreDataSource", DataSource.class));
                });
    }

    @Test
    void backsOffWhenEventStorageEngineBeanAlreadyExists() {
        EventStorageEngine userSuppliedEngine = OpenDcbAxonPostgres.engine(stubDataSource(), false);
        contextRunner
                .withBean("dataSource", DataSource.class, OpenDcbPostgresAutoConfigurationTest::stubDataSource)
                .withBean(EventStorageEngine.class, () -> userSuppliedEngine)
                .run(context -> assertSame(userSuppliedEngine, context.getBean(EventStorageEngine.class)));
    }

    @Test
    void neitherDataSourceNorEngineBeanIsConstructedWhenEventStorageEngineBeanAlreadyExists() {
        // Unreachable on purpose: if openDcbEventStoreDataSource were built anyway, DataSourceBuilder
        // itself wouldn't fail (pool creation is lazy), but its presence would prove the resolution
        // logic ran when it shouldn't have. Asserting a clean startup rules out any eager connection
        // attempt along that path too.
        EventStorageEngine userSuppliedEngine = OpenDcbAxonPostgres.engine(stubDataSource(), false);
        contextRunner
                .withPropertyValues("opendcb.eventstore.datasource.url=jdbc:postgresql://unreachable-host:5432/eventstore")
                .withBean(EventStorageEngine.class, () -> userSuppliedEngine)
                .run(context -> {
                    assertNull(context.getStartupFailure());
                    assertSame(userSuppliedEngine, context.getBean(EventStorageEngine.class));
                    assertThrows(NoSuchBeanDefinitionException.class,
                            () -> context.getBean("openDcbEventStoreDataSource", DataSource.class));
                    assertThrows(NoSuchBeanDefinitionException.class, () -> context.getBean(DataSource.class));
                });
    }

    private static DataSource stubDataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl("jdbc:postgresql://localhost:5432/unused");
        return dataSource;
    }
}
