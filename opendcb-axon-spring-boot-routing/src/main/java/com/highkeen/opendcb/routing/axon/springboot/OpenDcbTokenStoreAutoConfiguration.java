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
package com.highkeen.opendcb.routing.axon.springboot;

import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.GenericTokenTableFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.JdbcTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.JdbcTokenStoreConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;

/**
 * Wires Axon Framework 5's own free {@link JdbcTokenStore} against whichever {@link DataSource} the
 * event store is actually using, so {@code PooledStreamingEventProcessor} segments can be
 * claimed/rebalanced across multiple JVM instances without Axon Server.
 *
 * <p>This module deliberately depends only on {@code eventstore-core} and
 * {@code integrations/eventstore-axon} — never on {@code opendcb-axon-spring-boot-starter} or
 * {@code bootstrap-axon-postgres} (see {@code docs/ARCHITECTURE.md}'s module dependency order).
 * The {@code openDcbEventStoreDataSource} bean that the starter module creates is therefore looked
 * up by bean name at runtime via {@link BeanFactory}, never referenced as a Java type.
 *
 * <p>Simplification versus what a "real" Axon-Spring-integrated token store would do: the
 * {@link org.axonframework.conversion.Converter} used to de-/serialize tokens is a plain
 * {@link JacksonConverter} with a default {@link tools.jackson.databind.ObjectMapper}, not Axon's
 * Spring-aware {@code GeneralConverter}/{@code SpringDataSourceConnectionProvider} combination —
 * those live in Axon's own {@code extensions/spring} modules, which are off-limits here for the
 * same dependency-boundary reason. {@link JacksonConverter} is transitively available via
 * {@code axon-messaging}'s compile dependency on {@code axon-conversion}, so this requires no
 * additional dependency.
 *
 * <p>The token table is created (when {@link OpenDcbRoutingProperties#isAutoCreateSchema()} is
 * {@code true}) via {@link GenericTokenTableFactory}, not a provider-specific factory such as
 * {@code PostgresTokenTableFactory} — the resolved {@link DataSource} may not even be the event
 * store's own database (see the fallback-to-primary-DataSource path below), so a factory
 * "compatible with most databases" is the safer default.
 */
@AutoConfiguration(
        after = DataSourceAutoConfiguration.class,
        afterName = "com.highkeen.opendcb.springboot.axon.OpenDcbPostgresAutoConfiguration"
)
@EnableConfigurationProperties(OpenDcbRoutingProperties.class)
public class OpenDcbTokenStoreAutoConfiguration {

    /**
     * Name of the {@link DataSource} bean {@code opendcb-axon-spring-boot-starter} creates for the
     * event store. Looked up by name only — this module must not take a compile-time dependency on
     * the module that defines it.
     */
    static final String EVENT_STORE_DATASOURCE_BEAN_NAME = "openDcbEventStoreDataSource";

    private static final Logger log = LoggerFactory.getLogger(OpenDcbTokenStoreAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean(TokenStore.class)
    @ConditionalOnBean(DataSource.class)
    public TokenStore openDcbTokenStore(BeanFactory beanFactory, OpenDcbRoutingProperties properties) {
        DataSource dataSource = resolveDataSource(beanFactory);

        JdbcTokenStore tokenStore = new JdbcTokenStore(
                new JdbcTransactionalExecutorProvider(dataSource),
                new JacksonConverter(),
                JdbcTokenStoreConfiguration.DEFAULT);

        if (properties.isAutoCreateSchema()) {
            tokenStore.createSchema(GenericTokenTableFactory.INSTANCE);
        }
        return tokenStore;
    }

    private static DataSource resolveDataSource(BeanFactory beanFactory) {
        if (beanFactory.containsBean(EVENT_STORE_DATASOURCE_BEAN_NAME)) {
            log.info("Sharing the event store's DataSource ('{}' bean) for the Axon JdbcTokenStore.",
                    EVENT_STORE_DATASOURCE_BEAN_NAME);
            return beanFactory.getBean(EVENT_STORE_DATASOURCE_BEAN_NAME, DataSource.class);
        }

        log.info("No '{}' bean found; using the application's primary DataSource for the Axon "
                + "JdbcTokenStore. If this is not the same database as your event store, token "
                + "coordination will silently produce meaningless results — configure a dedicated "
                + "event store DataSource named '{}' instead.",
                EVENT_STORE_DATASOURCE_BEAN_NAME, EVENT_STORE_DATASOURCE_BEAN_NAME);
        return beanFactory.getBeanProvider(DataSource.class).getObject();
    }
}
