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

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binds {@code opendcb.routing.*} properties for {@link OpenDcbTokenStoreAutoConfiguration}.
 */
@ConfigurationProperties(prefix = "opendcb.routing")
public class OpenDcbRoutingProperties {

    /**
     * Whether to create the JDBC token store's table if it does not already exist.
     */
    private boolean autoCreateSchema = true;

    public boolean isAutoCreateSchema() {
        return autoCreateSchema;
    }

    public void setAutoCreateSchema(boolean autoCreateSchema) {
        this.autoCreateSchema = autoCreateSchema;
    }
}
