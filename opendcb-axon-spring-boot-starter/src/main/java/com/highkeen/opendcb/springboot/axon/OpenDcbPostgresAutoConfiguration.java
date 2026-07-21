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

import com.highkeen.opendcb.bootstrap.axon.postgres.OpenDcbAxonPostgres;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;

/**
 * Auto-configures an {@link EventStorageEngine} backed by PostgreSQL, delegating the actual wiring
 * to {@link OpenDcbAxonPostgres} (from {@code bootstrap-axon-postgres}) — this class only resolves
 * which {@link DataSource} the event store should use and adapts Spring Boot properties to that
 * factory call. It never re-implements {@code PostgresEventStoreStorage}/
 * {@code AbstractDcbEventStorageEngine} construction itself.
 *
 * <p>The event store's {@link DataSource} is resolved independently of the application's own
 * primary one, since the two often need separate databases:
 * <ol>
 *     <li>{@code opendcb.eventstore.datasource.use-primary=true} — reuse the application's primary
 *     {@code DataSource} bean.</li>
 *     <li>{@code opendcb.eventstore.datasource.url} set — build a dedicated {@code DataSource} from
 *     {@code opendcb.eventstore.datasource.*}.</li>
 *     <li>Otherwise — fall back to the application's primary {@code DataSource}, logging an INFO
 *     message so the reuse is not silent.</li>
 * </ol>
 */
@AutoConfiguration
@ConditionalOnProperty(name = "opendcb.eventstore.provider", havingValue = "postgres", matchIfMissing = true)
@EnableConfigurationProperties(OpenDcbProperties.class)
@AutoConfigureAfter(DataSourceAutoConfiguration.class)
public class OpenDcbPostgresAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(OpenDcbPostgresAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean(name = "openDcbEventStoreDataSource", value = EventStorageEngine.class)
    public DataSource openDcbEventStoreDataSource(OpenDcbProperties properties, ObjectProvider<DataSource> primaryDataSource) {
        OpenDcbProperties.DataSourceProperties dataSourceProperties = properties.getDatasource();

        if (dataSourceProperties.isUsePrimary()) {
            return requirePrimaryDataSource(primaryDataSource,
                    "opendcb.eventstore.datasource.use-primary=true but no unique primary DataSource bean was found");
        }

        if (dataSourceProperties.getUrl() != null) {
            DataSourceBuilder<?> builder = DataSourceBuilder.create()
                    .url(dataSourceProperties.getUrl())
                    .username(dataSourceProperties.getUsername())
                    .password(dataSourceProperties.getPassword());
            if (dataSourceProperties.getDriverClassName() != null) {
                builder.driverClassName(dataSourceProperties.getDriverClassName());
            }
            return builder.build();
        }

        DataSource primary = requirePrimaryDataSource(primaryDataSource,
                "opendcb.eventstore.datasource is not configured and no unique primary DataSource bean was found "
                        + "to fall back to");
        log.info("opendcb.eventstore.datasource not configured; reusing the application's primary DataSource for "
                + "the event store. Set opendcb.eventstore.datasource.url to use a separate database.");
        return primary;
    }

    @Bean
    @ConditionalOnMissingBean(EventStorageEngine.class)
    public EventStorageEngine openDcbEventStorageEngine(
            @Qualifier("openDcbEventStoreDataSource") DataSource dataSource, OpenDcbProperties properties) {
        return OpenDcbAxonPostgres.engine(dataSource, properties.isAutoCreateSchema());
    }

    private static DataSource requirePrimaryDataSource(ObjectProvider<DataSource> primaryDataSource, String message) {
        DataSource primary = primaryDataSource.getIfUnique();
        if (primary == null) {
            throw new IllegalStateException(message);
        }
        return primary;
    }
}
