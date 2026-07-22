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

import com.highkeen.opendcb.eventstore.postgres.PostgresEventStoreStorage;
import com.highkeen.opendcb.integrations.axon.AbstractDcbEventStorageEngine;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;

import javax.sql.DataSource;

/**
 * Zero-boilerplate factory gluing {@code integrations/eventstore-axon} and {@code eventstore-postgres} together, so
 * a plain Java/Quarkus/Micronaut consumer gets a working {@link EventStorageEngine} in one call instead of
 * hand-assembling {@link PostgresEventStoreStorage} and {@link AbstractDcbEventStorageEngine} every project.
 *
 * <p>Per docs/ARCHITECTURE.md's "Bootstrap modules" section, this is the one module in the toolkit allowed to depend
 * on both an {@code integrations/eventstore-<framework>} module and a specific {@code eventstore-<provider>}
 * module.
 */
public final class OpenDcbAxonPostgres {

    private OpenDcbAxonPostgres() {
    }

    /**
     * Builds an {@link EventStorageEngine} backed by PostgreSQL, creating the schema if it does not already exist.
     *
     * @param dataSource the {@link DataSource} to connect to PostgreSQL with.
     * @return a ready-to-use {@link EventStorageEngine}.
     */
    public static EventStorageEngine engine(DataSource dataSource) {
        return engine(dataSource, true);
    }

    /**
     * Builds an {@link EventStorageEngine} backed by PostgreSQL.
     *
     * @param dataSource      the {@link DataSource} to connect to PostgreSQL with.
     * @param autoCreateSchema whether to create the {@code events}/{@code event_tags} schema if it does not already
     *                         exist. Set to {@code false} when schema management is handled externally (e.g. via a
     *                         migration tool).
     * @return a ready-to-use {@link EventStorageEngine}.
     */
    public static EventStorageEngine engine(DataSource dataSource, boolean autoCreateSchema) {
        return engine(dataSource, autoCreateSchema, new JacksonConverter());
    }

    /**
     * Builds an {@link EventStorageEngine} backed by PostgreSQL, using the given {@link Converter} for payload
     * (de)serialization instead of the default {@link JacksonConverter}.
     *
     * @param dataSource      the {@link DataSource} to connect to PostgreSQL with.
     * @param autoCreateSchema whether to create the {@code events}/{@code event_tags} schema if it does not already
     *                         exist. Set to {@code false} when schema management is handled externally (e.g. via a
     *                         migration tool).
     * @param converter        the {@link Converter} used to (de)serialize event payloads.
     * @return a ready-to-use {@link EventStorageEngine}.
     */
    public static EventStorageEngine engine(DataSource dataSource, boolean autoCreateSchema, Converter converter) {
        PostgresEventStoreStorage storage = new PostgresEventStoreStorage(dataSource);
        if (autoCreateSchema) {
            storage.ensureSchema();
        }
        return new AbstractDcbEventStorageEngine(storage, converter);
    }
}
