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

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binds {@code opendcb.eventstore.*} properties for {@link OpenDcbPostgresAutoConfiguration}.
 */
@ConfigurationProperties(prefix = "opendcb.eventstore")
public class OpenDcbProperties {

    /**
     * Which storage provider to auto-configure. {@code "postgres"} (default) wires up
     * {@code eventstore-postgres} via {@code bootstrap-axon-postgres}; {@code "none"} disables this
     * starter's auto-configuration entirely so the application can supply its own
     * {@code EventStorageEngine} bean.
     */
    private String provider = "postgres";

    /**
     * Whether to create the {@code events}/{@code event_tags} schema if it does not already exist.
     */
    private boolean autoCreateSchema = true;

    private final DataSourceProperties datasource = new DataSourceProperties();

    public String getProvider() {
        return provider;
    }

    public void setProvider(String provider) {
        this.provider = provider;
    }

    public boolean isAutoCreateSchema() {
        return autoCreateSchema;
    }

    public void setAutoCreateSchema(boolean autoCreateSchema) {
        this.autoCreateSchema = autoCreateSchema;
    }

    public DataSourceProperties getDatasource() {
        return datasource;
    }

    /**
     * Configures the {@link javax.sql.DataSource} used for the event store, independently of the
     * application's own primary {@code DataSource} — the two often need separate databases (different
     * schema, different access controls, different scaling).
     */
    public static class DataSourceProperties {

        private String url;
        private String username;
        private String password;
        private String driverClassName;

        /**
         * If {@code true}, reuse the application's primary {@code DataSource} bean for the event store
         * instead of building a dedicated one.
         */
        private boolean usePrimary = false;

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getDriverClassName() {
            return driverClassName;
        }

        public void setDriverClassName(String driverClassName) {
            this.driverClassName = driverClassName;
        }

        public boolean isUsePrimary() {
            return usePrimary;
        }

        public void setUsePrimary(boolean usePrimary) {
            this.usePrimary = usePrimary;
        }
    }
}
