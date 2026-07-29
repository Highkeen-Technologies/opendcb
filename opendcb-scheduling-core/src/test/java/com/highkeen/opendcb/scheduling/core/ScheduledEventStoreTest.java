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
package com.highkeen.opendcb.scheduling.core;

import com.highkeen.opendcb.eventstore.core.StoredEvent;
import com.highkeen.opendcb.eventstore.core.StoredEvent.StoredTag;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs {@link ScheduledEventStore} against a real PostgreSQL 16 instance, covering: schedule/claim
 * due-time semantics, cancel's safe-no-op guards, lease-expiry reclaim by a second independent worker,
 * true concurrent claiming (via a {@link CountDownLatch} start line, mirroring {@code
 * eventstore-postgres}'s and {@code opendcb-axon-spring-boot-routing}'s cross-JVM race tests), and the
 * {@code attempt_count}/{@code max_attempts} dead-letter transition.
 */
@Testcontainers
class ScheduledEventStoreTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    private static DataSource dataSource;

    @BeforeAll
    static void setUpSchema() {
        dataSource = newDataSource();
        new ScheduledEventStore(dataSource).ensureSchema();
    }

    @BeforeEach
    void truncateTable() throws SQLException {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE TABLE scheduled_event");
        }
    }

    @Test
    void claimDueReturnsRowOnlyAtOrAfterScheduledTime() {
        ScheduledEventStore store = new ScheduledEventStore(dataSource);
        Instant scheduledTime = Instant.now().plusSeconds(3600);
        UUID id = store.schedule(scheduledTime, testEvent("evt-timing"), "scope-a");

        List<ScheduledEventRecord> before = store.claimDue(
                scheduledTime.minusSeconds(1), 10, "worker-1", Duration.ofMinutes(1)).claimed();
        assertTrue(before.isEmpty(), "must not be claimable before its scheduled time");

        List<ScheduledEventRecord> after = store.claimDue(
                scheduledTime.plusSeconds(1), 10, "worker-1", Duration.ofMinutes(1)).claimed();
        assertEquals(1, after.size());
        assertEquals(id, after.get(0).id());
        assertEquals("evt-timing", after.get(0).eventId());
        assertEquals(ScheduledEventStatus.IN_PROGRESS, after.get(0).status());
        assertEquals("worker-1", after.get(0).workerId());
        assertEquals(1, after.get(0).attemptCount());
    }

    @Test
    void cancelOnPendingRowPreventsItFromEverBeingClaimed() {
        ScheduledEventStore store = new ScheduledEventStore(dataSource);
        UUID id = store.schedule(Instant.now().minusSeconds(1), testEvent("evt-cancel-pending"), "scope-a");

        store.cancel(id);

        List<ScheduledEventRecord> claimed =
                store.claimDue(Instant.now(), 10, "worker-1", Duration.ofMinutes(1)).claimed();
        assertTrue(claimed.stream().noneMatch(record -> record.id().equals(id)),
                "a cancelled-while-pending row must never be claimable");
    }

    @Test
    void cancelOnInProgressRowIsASafeNoOp() {
        ScheduledEventStore store = new ScheduledEventStore(dataSource);
        UUID id = store.schedule(Instant.now().minusSeconds(1), testEvent("evt-cancel-in-progress"), "scope-a");
        List<ScheduledEventRecord> claimed =
                store.claimDue(Instant.now(), 10, "worker-1", Duration.ofMinutes(1)).claimed();
        assertEquals(1, claimed.size());

        assertDoesNotThrow(() -> store.cancel(id));

        // If cancel() had actually flipped status away from IN_PROGRESS, this guarded completion
        // (WHERE status = 'IN_PROGRESS' AND worker_id = ?) would silently fail to apply — proving
        // cancel() left the row alone.
        store.markCompleted(id, "worker-1");
        assertEquals(0, store.claimDue(Instant.now(), 10, "worker-1", Duration.ofMinutes(1)).claimed().size());
    }

    @Test
    void expiredLeaseIsReclaimedByAnotherWorkerAndTheStaleWorkersLateCompletionIsANoOp() throws InterruptedException {
        ScheduledEventStore storeA = new ScheduledEventStore(dataSource);
        ScheduledEventStore storeB = new ScheduledEventStore(dataSource);
        Duration shortLease = Duration.ofMillis(300);

        UUID id = storeA.schedule(Instant.now().minusSeconds(1), testEvent("evt-lease-expiry"), "scope-a");

        List<ScheduledEventRecord> claimedByA = storeA.claimDue(Instant.now(), 10, "worker-A", shortLease).claimed();
        assertEquals(1, claimedByA.size());
        assertEquals(1, claimedByA.get(0).attemptCount(), "first claim increments attempt_count to 1");

        // Worker A never calls markCompleted -- simulate it crashing/hanging past its lease.
        Thread.sleep(shortLease.plusMillis(300).toMillis());

        List<ScheduledEventRecord> claimedByB = storeB.claimDue(Instant.now(), 10, "worker-B", shortLease).claimed();
        assertEquals(1, claimedByB.size(), "worker B must reclaim the row once worker A's lease expired");
        assertEquals(id, claimedByB.get(0).id());
        assertEquals("worker-B", claimedByB.get(0).workerId());
        assertEquals(2, claimedByB.get(0).attemptCount(), "reclaim is a second claim, so attempt_count becomes 2");

        // Worker A's late completion call must not clobber worker B's active claim.
        storeA.markCompleted(id, "worker-A");

        // Proven by worker B still being able to legitimately complete its own claim.
        assertDoesNotThrow(() -> storeB.markCompleted(id, "worker-B"));
    }

    @Test
    void concurrentClaimAttemptsAcrossTwoWorkersNeverOverlapAndLeaveUnexpiredLeasesUntouched() throws Exception {
        ScheduledEventStore storeA = new ScheduledEventStore(dataSource);
        ScheduledEventStore storeB = new ScheduledEventStore(dataSource);
        Duration lease = Duration.ofMinutes(10);

        Instant due = Instant.now().minusSeconds(1);
        Set<UUID> dueIds = new HashSet<>();
        for (int i = 0; i < 6; i++) {
            dueIds.add(storeA.schedule(due, testEvent("evt-race-" + i), "scope-a"));
        }

        // A row that is IN_PROGRESS with a fresh (unexpired) lease must not be claimed by either racer.
        // Scheduled strictly earlier than the others so ORDER BY scheduled_time LIMIT 1 deterministically
        // selects it, rather than relying on tie-breaking among rows sharing the same "due" instant.
        UUID freshlyClaimedId = storeA.schedule(due.minusSeconds(10), testEvent("evt-race-fresh-lease"), "scope-a");
        List<ScheduledEventRecord> preClaimed = storeA.claimDue(Instant.now(), 1, "worker-pre", lease).claimed();
        assertEquals(1, preClaimed.size());
        assertEquals(freshlyClaimedId, preClaimed.get(0).id());

        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        List<ScheduledEventRecord> claimedByA;
        List<ScheduledEventRecord> claimedByB;
        try {
            Future<List<ScheduledEventRecord>> futureA =
                    executor.submit(claimRaceTask(storeA, "worker-A", lease, ready, go));
            Future<List<ScheduledEventRecord>> futureB =
                    executor.submit(claimRaceTask(storeB, "worker-B", lease, ready, go));

            assertTrue(ready.await(10, TimeUnit.SECONDS), "both racing threads should reach the start line");
            go.countDown();

            claimedByA = futureA.get(30, TimeUnit.SECONDS);
            claimedByB = futureB.get(30, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        Set<UUID> idsClaimedByA = claimedByA.stream().map(ScheduledEventRecord::id).collect(Collectors.toSet());
        Set<UUID> idsClaimedByB = claimedByB.stream().map(ScheduledEventRecord::id).collect(Collectors.toSet());

        assertTrue(idsClaimedByA.stream().noneMatch(idsClaimedByB::contains),
                "the two racing claims must never overlap");
        assertEquals(dueIds, union(idsClaimedByA, idsClaimedByB), "every due row must be claimed exactly once");
        assertTrue(!idsClaimedByA.contains(freshlyClaimedId) && !idsClaimedByB.contains(freshlyClaimedId),
                "a row with an unexpired lease must not be claimed by either racer");
    }

    @Test
    void thirdClaimAttemptPastMaxAttemptsDeadLettersTheRowInsteadOfReturningItClaimable()
            throws InterruptedException, SQLException {
        ScheduledEventStore store = new ScheduledEventStore(dataSource);
        Duration shortLease = Duration.ofMillis(300);
        UUID id = store.schedule(Instant.now().minusSeconds(1), testEvent("evt-dead-letter"), "scope-a", 2);

        // Attempt 1: claimed, never completed.
        ScheduledEventStore.ClaimBatch first = store.claimDue(Instant.now(), 10, "worker-1", shortLease);
        assertEquals(1, first.claimed().size());
        assertTrue(first.deadLettered().isEmpty());
        assertEquals(1, first.claimed().get(0).attemptCount());
        Thread.sleep(shortLease.plusMillis(300).toMillis());

        // Attempt 2: still within max_attempts=2, claimed again, never completed.
        ScheduledEventStore.ClaimBatch second = store.claimDue(Instant.now(), 10, "worker-1", shortLease);
        assertEquals(1, second.claimed().size());
        assertTrue(second.deadLettered().isEmpty());
        assertEquals(2, second.claimed().get(0).attemptCount());
        Thread.sleep(shortLease.plusMillis(300).toMillis());

        // Attempt 3 would push attempt_count to 3 > max_attempts=2 -- must dead-letter, not claim.
        ScheduledEventStore.ClaimBatch third = store.claimDue(Instant.now(), 10, "worker-1", shortLease);
        assertTrue(third.claimed().isEmpty(), "a row past its attempt budget must never be returned as claimable");
        assertEquals(1, third.deadLettered().size());
        assertEquals(id, third.deadLettered().get(0).id());
        assertEquals(ScheduledEventStatus.DEAD_LETTERED, third.deadLettered().get(0).status());
        assertEquals("DEAD_LETTERED", fetchStatus(id));
    }

    @Test
    void deadLetteredRowIsNeverReturnedByClaimDueAgainOnSubsequentCalls() throws InterruptedException, SQLException {
        ScheduledEventStore store = new ScheduledEventStore(dataSource);
        Duration shortLease = Duration.ofMillis(300);
        UUID id = store.schedule(Instant.now().minusSeconds(1), testEvent("evt-terminal"), "scope-a", 1);

        ScheduledEventStore.ClaimBatch first = store.claimDue(Instant.now(), 10, "worker-1", shortLease);
        assertEquals(1, first.claimed().size());
        Thread.sleep(shortLease.plusMillis(300).toMillis());

        ScheduledEventStore.ClaimBatch second = store.claimDue(Instant.now(), 10, "worker-1", shortLease);
        assertEquals(1, second.deadLettered().size());
        assertEquals(id, second.deadLettered().get(0).id());

        for (int i = 0; i < 3; i++) {
            ScheduledEventStore.ClaimBatch subsequent =
                    store.claimDue(Instant.now().plusSeconds(3600), 10, "worker-" + i, Duration.ofMillis(1));
            assertTrue(subsequent.claimed().isEmpty(), "a dead-lettered row must never become claimable again");
            assertTrue(subsequent.deadLettered().isEmpty(), "a dead-lettered row must not be dead-lettered again");
        }
        assertEquals("DEAD_LETTERED", fetchStatus(id));
    }

    @Test
    void successWithinAttemptBudgetNeverReachesDeadLettered() throws InterruptedException, SQLException {
        ScheduledEventStore store = new ScheduledEventStore(dataSource);
        Duration shortLease = Duration.ofMillis(300);
        UUID id = store.schedule(Instant.now().minusSeconds(1), testEvent("evt-success-within-budget"), "scope-a", 5);

        // Attempt 1: claimed, abandoned (simulating a transient failure).
        ScheduledEventStore.ClaimBatch first = store.claimDue(Instant.now(), 10, "worker-1", shortLease);
        assertEquals(1, first.claimed().size());
        assertEquals(1, first.claimed().get(0).attemptCount());
        Thread.sleep(shortLease.plusMillis(300).toMillis());

        // Attempt 2: claimed again and this time completes successfully, well within max_attempts=5.
        ScheduledEventStore.ClaimBatch second = store.claimDue(Instant.now(), 10, "worker-2", shortLease);
        assertEquals(1, second.claimed().size());
        assertEquals(2, second.claimed().get(0).attemptCount());
        store.markCompleted(id, "worker-2");

        assertEquals("COMPLETED", fetchStatus(id));
        ScheduledEventStore.ClaimBatch after = store.claimDue(Instant.now().plusSeconds(3600), 10, "worker-3", shortLease);
        assertTrue(after.claimed().isEmpty());
        assertTrue(after.deadLettered().isEmpty(), "a row that succeeded within its budget must never dead-letter");
    }

    @Test
    void staleWorkersLateCompletionIsANoOpAgainstARowDeadLetteredByTheSameClaimDueThatReclaimedIt()
            throws InterruptedException, SQLException {
        ScheduledEventStore storeA = new ScheduledEventStore(dataSource);
        ScheduledEventStore storeB = new ScheduledEventStore(dataSource);
        Duration shortLease = Duration.ofMillis(300);
        UUID id = storeA.schedule(Instant.now().minusSeconds(1), testEvent("evt-fencing-vs-dead-letter"),
                "scope-a", 1);

        List<ScheduledEventRecord> claimedByA = storeA.claimDue(Instant.now(), 10, "worker-A", shortLease).claimed();
        assertEquals(1, claimedByA.size());

        // Worker A never completes -- simulate it crashing past its lease.
        Thread.sleep(shortLease.plusMillis(300).toMillis());

        // max_attempts=1 means this reclaim attempt (the 2nd claim) exceeds the budget: worker B's
        // claimDue call dead-letters the row instead of claiming it for itself.
        ScheduledEventStore.ClaimBatch reclaim = storeB.claimDue(Instant.now(), 10, "worker-B", shortLease);
        assertTrue(reclaim.claimed().isEmpty());
        assertEquals(1, reclaim.deadLettered().size());
        assertEquals(id, reclaim.deadLettered().get(0).id());

        // Worker A's late completion targets a row that is now DEAD_LETTERED, owned by nobody -- the
        // status/worker_id fencing guard on markCompleted must leave it alone rather than resurrecting it.
        assertDoesNotThrow(() -> storeA.markCompleted(id, "worker-A"));
        assertEquals("DEAD_LETTERED", fetchStatus(id));
    }

    @Test
    void staleWorkersLateMarkSkippedConflictIsANoOpAfterAnotherWorkerReclaimsTheRow() throws InterruptedException,
            SQLException {
        ScheduledEventStore storeA = new ScheduledEventStore(dataSource);
        ScheduledEventStore storeB = new ScheduledEventStore(dataSource);
        Duration shortLease = Duration.ofMillis(300);
        UUID id = storeA.schedule(Instant.now().minusSeconds(1), testEvent("evt-conflict-fencing"), "scope-a");

        List<ScheduledEventRecord> claimedByA = storeA.claimDue(Instant.now(), 10, "worker-A", shortLease).claimed();
        assertEquals(1, claimedByA.size());

        // Worker A's lease expires before it finishes its conflict check.
        Thread.sleep(shortLease.plusMillis(300).toMillis());

        List<ScheduledEventRecord> claimedByB = storeB.claimDue(Instant.now(), 10, "worker-B", shortLease).claimed();
        assertEquals(1, claimedByB.size(), "worker B must reclaim the row once worker A's lease expired");

        // Worker A's late conflict-skip decision must not clobber worker B's active claim.
        storeA.markSkippedConflict(id, "worker-A");

        // Proven by worker B still being able to legitimately complete its own claim afterward.
        assertDoesNotThrow(() -> storeB.markCompleted(id, "worker-B"));
        assertEquals("COMPLETED", fetchStatus(id));
    }

    private static Callable<List<ScheduledEventRecord>> claimRaceTask(
            ScheduledEventStore store, String workerId, Duration lease, CountDownLatch ready, CountDownLatch go) {
        return () -> {
            ready.countDown();
            go.await();
            return store.claimDue(Instant.now(), 10, workerId, lease).claimed();
        };
    }

    private static Set<UUID> union(Set<UUID> a, Set<UUID> b) {
        Set<UUID> result = new HashSet<>(a);
        result.addAll(b);
        return result;
    }

    private static String fetchStatus(UUID id) throws SQLException {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement =
                        connection.prepareStatement("SELECT status FROM scheduled_event WHERE id = ?")) {
            statement.setObject(1, id);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getString("status");
            }
        }
    }

    private static StoredEvent testEvent(String eventId) {
        return new StoredEvent(
                -1,
                eventId,
                "TestEvent",
                "com.example.TestPayload",
                "{\"foo\":\"bar\"}",
                Map.of("k", "v"),
                Set.of(new StoredTag("tagKey", "tagValue")),
                Instant.now());
    }

    private static DataSource newDataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(POSTGRES.getJdbcUrl());
        ds.setUser(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        return ds;
    }
}
