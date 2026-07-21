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
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.UnableToClaimTokenException;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.GenericTokenTableFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.JdbcTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.JdbcTokenStoreConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.jdbc.DataSourceBuilder;

import javax.sql.DataSource;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves the actual guarantee {@code opendcb-axon-spring-boot-routing} exists for: two JVM instances
 * sharing one {@link JdbcTokenStore}-backed database must not both believe they own the same processor
 * segment at once.
 *
 * <p>Mirrors {@code eventstore-postgres}'s cross-JVM contract test shape — two independent
 * {@link JdbcTokenStore} instances, never sharing in-process state, pointed at one underlying
 * {@link DataSource} — constructed exactly as {@link OpenDcbTokenStoreAutoConfiguration#openDcbTokenStore}
 * constructs its bean, except each instance is given a distinct {@code nodeId} to stand in for two
 * different JVMs. Using the configuration's default {@code nodeId} (the running JVM's
 * {@code ManagementFactory} runtime name) for both instances would make them indistinguishable to
 * {@code JdbcTokenStore}'s claim logic, since both objects would then run inside this single test JVM —
 * that would defeat the entire point of this test, so the distinct node IDs here are load-bearing, not
 * decoration.
 */
class JdbcTokenStoreClaimConflictTest {

    private static final String PROCESSOR_NAME = "test-processor";
    private static final int SEGMENT_ID = 0;

    @Test
    void secondNodeCannotClaimASegmentAlreadyHeldByTheFirst() throws Exception {
        DataSource dataSource = h2DataSource("claim-conflict");
        JdbcTokenStore jvmA = tokenStore(dataSource, "jvm-a");
        JdbcTokenStore jvmB = tokenStore(dataSource, "jvm-b");

        jvmA.createSchema(GenericTokenTableFactory.INSTANCE);
        jvmA.initializeTokenSegments(PROCESSOR_NAME, 1, null, null).get();

        // JVM A claims the segment.
        jvmA.fetchToken(PROCESSOR_NAME, SEGMENT_ID, null).get();

        // JVM B tries to claim the very same segment while JVM A still holds it — must fail.
        assertThatThrownBy(() -> jvmB.fetchToken(PROCESSOR_NAME, SEGMENT_ID, null).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(UnableToClaimTokenException.class);

        // JVM A releases its claim.
        jvmA.releaseClaim(PROCESSOR_NAME, SEGMENT_ID, null).get();

        // Now JVM B can claim it.
        assertThat(jvmB.fetchToken(PROCESSOR_NAME, SEGMENT_ID, null).get()).isNull();
    }

    /**
     * The sequential test above proves the ownership invariant, but never puts two transactions inside
     * their {@code SELECT ... FOR UPDATE} at the same instant — it proves "a held claim blocks a later
     * claim attempt," not "the row lock actually serializes simultaneous attempts." This test forces that:
     * two threads are released past a shared {@link CountDownLatch} start line together, so both threads'
     * {@code fetchToken} calls enter {@code JdbcTokenStore}'s {@code SELECT ... FOR UPDATE} at the same
     * moment rather than one-after-another. Mirrors {@code eventstore-postgres}'s
     * {@code concurrentAppendsWithOverlappingConflictPredicateAllowExactlyOneSuccess} race test shape
     * exactly: a {@code ready}/{@code go} latch pair plus a {@code succeededA ^ succeededB} XOR assertion.
     */
    @Test
    void exactlyOneOfTwoSimultaneousClaimAttemptsSucceeds() throws Exception {
        DataSource dataSource = h2DataSource("claim-race");
        JdbcTokenStore jvmA = tokenStore(dataSource, "jvm-a");
        JdbcTokenStore jvmB = tokenStore(dataSource, "jvm-b");

        jvmA.createSchema(GenericTokenTableFactory.INSTANCE);
        jvmA.initializeTokenSegments(PROCESSOR_NAME, 1, null, null).get();

        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        boolean succeededA;
        boolean succeededB;
        try {
            Future<Boolean> futureA = executor.submit(claimRaceTask(jvmA, ready, go));
            Future<Boolean> futureB = executor.submit(claimRaceTask(jvmB, ready, go));

            assertTrue(ready.await(10, TimeUnit.SECONDS), "both racing threads should reach the start line");
            go.countDown();

            succeededA = futureA.get(30, TimeUnit.SECONDS);
            succeededB = futureB.get(30, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        assertTrue(succeededA ^ succeededB, "exactly one of the two racing claim attempts must succeed");
    }

    private static Callable<Boolean> claimRaceTask(JdbcTokenStore tokenStore, CountDownLatch ready, CountDownLatch go) {
        return () -> {
            ready.countDown();
            go.await();
            try {
                tokenStore.fetchToken(PROCESSOR_NAME, SEGMENT_ID, null).get();
                return true;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnableToClaimTokenException) {
                    return false;
                }
                throw e;
            }
        };
    }

    private static JdbcTokenStore tokenStore(DataSource dataSource, String nodeId) {
        return new JdbcTokenStore(
                new JdbcTransactionalExecutorProvider(dataSource),
                new JacksonConverter(),
                JdbcTokenStoreConfiguration.DEFAULT.nodeId(nodeId));
    }

    private static DataSource h2DataSource(String name) {
        return DataSourceBuilder.create()
                .driverClassName("org.h2.Driver")
                .url("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1")
                .username("sa")
                .password("")
                .build();
    }
}
