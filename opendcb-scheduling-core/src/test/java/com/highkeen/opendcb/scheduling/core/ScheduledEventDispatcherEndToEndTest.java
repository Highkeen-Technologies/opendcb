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
import com.highkeen.opendcb.eventstore.postgres.PostgresEventStoreStorage;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end proof that {@link ScheduledEventDispatcher} genuinely fires a scheduled event: schedules
 * one row, runs the dispatcher once against a real {@link PostgresEventStoreStorage}, then reads the
 * event back via {@code storage.readRange} -- not just asserting the row's status flipped to
 * {@code COMPLETED} in {@code scheduled_event}, which alone wouldn't prove the append actually happened.
 * Also covers dead-letter forwarding and the {@link ConflictCriteria} safety net: firing normally when
 * no conflicting event exists, skipping (and notifying a {@link ConflictSkipSink}) when one does, and
 * confirming a {@code null} conflictCriteria behaves identically to before that feature existed.
 */
@Testcontainers
class ScheduledEventDispatcherEndToEndTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    private DataSource dataSource;
    private ScheduledEventStore scheduledEventStore;
    private PostgresEventStoreStorage storage;

    @BeforeEach
    void setUp() throws SQLException {
        dataSource = newDataSource();
        scheduledEventStore = new ScheduledEventStore(dataSource);
        storage = new PostgresEventStoreStorage(dataSource);
        scheduledEventStore.ensureSchema();
        storage.ensureSchema();
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE TABLE scheduled_event");
            statement.execute("TRUNCATE TABLE event_tags, events RESTART IDENTITY CASCADE");
        }
    }

    @Test
    void dispatcherFiresADueScheduledEventAsARealAppendedEvent() {
        StoredEvent eventToFire = new StoredEvent(
                -1,
                "evt-dispatch-e2e",
                "OrderReminderDue",
                "com.example.OrderReminderDuePayload",
                "{\"orderId\":\"order-1\"}",
                Map.of("source", "scheduler"),
                Set.of(new StoredTag("orderId", "order-1")),
                Instant.now());
        scheduledEventStore.schedule(Instant.now().minusSeconds(1), eventToFire, "scope-a");

        ScheduledEventDispatcher dispatcher = new ScheduledEventDispatcher(
                scheduledEventStore, storage, 10, "worker-1", Duration.ofMinutes(1));
        dispatcher.runOnce();

        List<StoredEvent> appended = storage.readRange(0, null, 100);
        assertEquals(1, appended.size());
        assertEquals("evt-dispatch-e2e", appended.get(0).eventId());
        assertEquals("OrderReminderDue", appended.get(0).messageType());
        assertEquals("{\"orderId\":\"order-1\"}", appended.get(0).payloadJson());
        assertEquals(Set.of(new StoredTag("orderId", "order-1")), appended.get(0).tags());

        // The row must now be COMPLETED, and therefore never claimable again.
        assertEquals(0,
                scheduledEventStore.claimDue(Instant.now(), 10, "worker-1", Duration.ofMinutes(1)).claimed().size());
    }

    @Test
    void runOnceForwardsRowsThatExceededMaxAttemptsToTheDeadLetterSinkAndNeverAppendsThem() throws InterruptedException {
        Duration shortLease = Duration.ofMillis(300);
        StoredEvent eventToFire = new StoredEvent(
                -1,
                "evt-dispatch-dead-letter",
                "OrderReminderDue",
                "com.example.OrderReminderDuePayload",
                "{\"orderId\":\"order-2\"}",
                Map.of("source", "scheduler"),
                Set.of(new StoredTag("orderId", "order-2")),
                Instant.now());
        scheduledEventStore.schedule(Instant.now().minusSeconds(1), eventToFire, "scope-a", 2);

        // Attempts 1 and 2 are simulated as abandoned worker claims directly against the store (not
        // through the dispatcher), the same way ScheduledEventStoreTest simulates a crashed worker --
        // this keeps the scenario focused on the dispatcher's handling of the eventual dead-letter,
        // without needing to force a real append failure to keep the row IN_PROGRESS.
        assertEquals(1, scheduledEventStore.claimDue(Instant.now(), 10, "worker-1", shortLease).claimed().size());
        Thread.sleep(shortLease.plusMillis(300).toMillis());
        assertEquals(1, scheduledEventStore.claimDue(Instant.now(), 10, "worker-1", shortLease).claimed().size());
        Thread.sleep(shortLease.plusMillis(300).toMillis());

        CapturingDeadLetterSink deadLetterSink = new CapturingDeadLetterSink();
        ScheduledEventDispatcher dispatcher = new ScheduledEventDispatcher(
                scheduledEventStore, storage, 10, "worker-1", shortLease, deadLetterSink);

        // Third claim attempt: exceeds max_attempts=2, so this poll dead-letters it instead of firing it.
        dispatcher.runOnce();

        assertEquals(1, deadLetterSink.records.size(), "the dead-lettered row must reach the sink exactly once");
        assertEquals("evt-dispatch-dead-letter", deadLetterSink.records.get(0).eventId());
        assertEquals("exceeded max_attempts (2)", deadLetterSink.reasons.get(0));

        List<StoredEvent> appended = storage.readRange(0, null, 100);
        assertTrue(appended.stream().noneMatch(event -> event.eventId().equals("evt-dispatch-dead-letter")),
                "a dead-lettered scheduled event must never be appended");
    }

    @Test
    void conflictCriteriaWithNoMatchingEventInTheLogFiresNormally() throws SQLException {
        StoredEvent eventToFire = new StoredEvent(
                -1,
                "evt-conflict-no-match",
                "InvoicePaymentDeadlineExpired",
                "com.example.DeadlinePayload",
                "{\"invoiceId\":\"inv-1\"}",
                Map.of(),
                Set.of(new StoredTag("invoiceId", "inv-1")),
                Instant.now());
        ConflictCriteria criteria =
                new ConflictCriteria(Set.of(new StoredTag("invoiceId", "inv-1")), Set.of("InvoicePaid"));
        UUID id = scheduledEventStore.schedule(Instant.now().minusSeconds(1), eventToFire, "scope-a",
                ScheduledEventStore.DEFAULT_MAX_ATTEMPTS, criteria);

        ScheduledEventDispatcher dispatcher = new ScheduledEventDispatcher(
                scheduledEventStore, storage, 10, "worker-1", Duration.ofMinutes(1));
        dispatcher.runOnce();

        List<StoredEvent> appended = storage.readRange(0, null, 100);
        assertTrue(appended.stream().anyMatch(event -> event.eventId().equals("evt-conflict-no-match")),
                "no conflicting event exists yet, so the scheduled event must fire normally");
        assertEquals("COMPLETED", fetchStatus(id));
    }

    @Test
    void conflictCriteriaWithAMatchingEventAlreadyInTheLogSkipsFiringAndNotifiesTheSink() throws SQLException {
        StoredEvent alreadyPaid = new StoredEvent(
                -1,
                "evt-invoice-paid",
                "InvoicePaid",
                "com.example.InvoicePaidPayload",
                "{\"invoiceId\":\"inv-2\"}",
                Map.of(),
                Set.of(new StoredTag("invoiceId", "inv-2")),
                Instant.now());
        storage.appendAtomically(List.of(alreadyPaid), 0L, event -> false);

        StoredEvent deadlineEvent = new StoredEvent(
                -1,
                "evt-conflict-match",
                "InvoicePaymentDeadlineExpired",
                "com.example.DeadlinePayload",
                "{\"invoiceId\":\"inv-2\"}",
                Map.of(),
                Set.of(new StoredTag("invoiceId", "inv-2")),
                Instant.now());
        ConflictCriteria criteria =
                new ConflictCriteria(Set.of(new StoredTag("invoiceId", "inv-2")), Set.of("InvoicePaid"));
        UUID id = scheduledEventStore.schedule(Instant.now().minusSeconds(1), deadlineEvent, "scope-a",
                ScheduledEventStore.DEFAULT_MAX_ATTEMPTS, criteria);

        CapturingConflictSkipSink conflictSkipSink = new CapturingConflictSkipSink();
        ScheduledEventDispatcher dispatcher = new ScheduledEventDispatcher(
                scheduledEventStore, storage, 10, "worker-1", Duration.ofMinutes(1),
                new LoggingDeadLetterSink(), conflictSkipSink);
        dispatcher.runOnce();

        List<StoredEvent> appended = storage.readRange(0, null, 100);
        assertTrue(appended.stream().noneMatch(event -> event.eventId().equals("evt-conflict-match")),
                "a conflicting event already exists, so the scheduled event must never be appended");

        assertEquals(1, conflictSkipSink.records.size(), "the conflict skip must reach the sink exactly once");
        assertEquals("evt-conflict-match", conflictSkipSink.records.get(0).eventId());
        assertEquals("evt-invoice-paid", conflictSkipSink.conflictingEvents.get(0).eventId());

        assertEquals("SKIPPED_CONFLICT", fetchStatus(id));
    }

    @Test
    void nullConflictCriteriaBehavesIdenticallyToBeforeThisFeatureExisted() throws SQLException {
        StoredEvent eventToFire = new StoredEvent(
                -1,
                "evt-backward-compat",
                "OrderReminderDue",
                "com.example.OrderReminderDuePayload",
                "{\"orderId\":\"order-9\"}",
                Map.of(),
                Set.of(new StoredTag("orderId", "order-9")),
                Instant.now());
        UUID id = scheduledEventStore.schedule(Instant.now().minusSeconds(1), eventToFire, "scope-a");

        ScheduledEventDispatcher dispatcher = new ScheduledEventDispatcher(
                scheduledEventStore, storage, 10, "worker-1", Duration.ofMinutes(1));
        dispatcher.runOnce();

        List<StoredEvent> appended = storage.readRange(0, null, 100);
        assertTrue(appended.stream().anyMatch(event -> event.eventId().equals("evt-backward-compat")),
                "a null conflictCriteria must not change firing behavior at all");
        assertEquals("COMPLETED", fetchStatus(id));
    }

    private String fetchStatus(UUID id) throws SQLException {
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

    private static class CapturingDeadLetterSink implements DeadLetterSink {
        private final List<ScheduledEventRecord> records = new ArrayList<>();
        private final List<String> reasons = new ArrayList<>();

        @Override
        public void onDeadLetter(ScheduledEventRecord record, String reason) {
            records.add(record);
            reasons.add(reason);
        }
    }

    private static class CapturingConflictSkipSink implements ConflictSkipSink {
        private final List<ScheduledEventRecord> records = new ArrayList<>();
        private final List<StoredEvent> conflictingEvents = new ArrayList<>();

        @Override
        public void onConflictSkip(ScheduledEventRecord record, StoredEvent conflictingEvent) {
            records.add(record);
            conflictingEvents.add(conflictingEvent);
        }
    }

    private static DataSource newDataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(POSTGRES.getJdbcUrl());
        ds.setUser(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        return ds;
    }
}
