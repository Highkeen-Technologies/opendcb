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

import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.JdbcTokenStore;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Wiring tests for {@link OpenDcbTokenStoreAutoConfiguration}, using real in-memory H2 {@link DataSource}s
 * so that {@code createSchema} actually runs — not just stubs that prove wiring without proving the
 * schema-creation call is correct.
 */
class OpenDcbTokenStoreAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(OpenDcbTokenStoreAutoConfiguration.class));

    @Test
    void tokenStoreBeanCreatedWhenExactlyOneDataSourceBeanPresent() {
        contextRunner
                .withBean("dataSource", DataSource.class, () -> h2DataSource("single"))
                .run(context -> {
                    assertThat(context).hasSingleBean(TokenStore.class);
                    assertThat(context.getBean(TokenStore.class)).isInstanceOf(JdbcTokenStore.class);
                });
    }

    @Test
    void usesNamedEventStoreDataSourceWhenPresentAlongsideAnotherDataSource() throws Exception {
        DataSource eventStoreDataSource = h2DataSource("named-event-store");
        DataSource otherDataSource = h2DataSource("named-other");

        contextRunner
                .withBean("openDcbEventStoreDataSource", DataSource.class, () -> eventStoreDataSource)
                .withBean("someOtherDataSource", DataSource.class, () -> otherDataSource)
                .run(context -> {
                    assertThat(context).hasSingleBean(TokenStore.class);
                    // Proof the NAMED DataSource was actually used, not just "a" DataSource: the token
                    // table must exist in the event store's own H2 instance and nowhere else.
                    assertThat(tokenTableExists(eventStoreDataSource)).isTrue();
                    assertThat(tokenTableExists(otherDataSource)).isFalse();
                });
    }

    @Test
    void backsOffWhenTokenStoreBeanAlreadyExists() {
        TokenStore userSupplied = new NoOpTokenStore();
        contextRunner
                .withBean("dataSource", DataSource.class, () -> h2DataSource("backs-off-token-store"))
                .withBean(TokenStore.class, () -> userSupplied)
                .run(context -> assertThat(context.getBean(TokenStore.class)).isSameAs(userSupplied));
    }

    @Test
    void backsOffCleanlyWhenNoDataSourceBeanPresent() {
        // @ConditionalOnBean(DataSource.class) is documented to prevent bean creation entirely when no
        // DataSource bean exists — confirmed here rather than assumed: the context must start cleanly
        // (no startup failure) with the TokenStore bean simply absent, since a token store with nothing
        // to route processor claims against isn't a configuration error, just an incomplete one that a
        // later-added DataSource bean (or this module simply not being needed yet) resolves.
        contextRunner.run(context -> {
            assertThat(context.getStartupFailure()).isNull();
            assertThat(context).doesNotHaveBean(TokenStore.class);
        });
    }

    private static boolean tokenTableExists(DataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            try (ResultSet tables = connection.getMetaData().getTables(null, null, "TOKENENTRY", null)) {
                return tables.next();
            }
        }
    }

    private static DataSource h2DataSource(String name) {
        return DataSourceBuilder.create()
                .driverClassName("org.h2.Driver")
                .url("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1")
                .username("sa")
                .password("")
                .build();
    }

    private static final class NoOpTokenStore implements TokenStore {
        @Override
        public CompletableFuture<List<Segment>> initializeTokenSegments(String processorName, int segmentCount,
                                                                         TrackingToken initialToken, ProcessingContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> storeToken(TrackingToken token, String processorName, int segmentId,
                                                   ProcessingContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<TrackingToken> fetchToken(String processorName, int segmentId, ProcessingContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> releaseClaim(String processorName, int segmentId, ProcessingContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> initializeSegment(TrackingToken token, String processorName, Segment segment,
                                                          ProcessingContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> deleteToken(String processorName, int segmentId, ProcessingContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Segment> fetchSegment(String processorName, int segmentId, ProcessingContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<List<Segment>> fetchSegments(String processorName, ProcessingContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<List<Segment>> fetchAvailableSegments(String processorName, ProcessingContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<String> retrieveStorageIdentifier(ProcessingContext context) {
            throw new UnsupportedOperationException();
        }
    }
}
