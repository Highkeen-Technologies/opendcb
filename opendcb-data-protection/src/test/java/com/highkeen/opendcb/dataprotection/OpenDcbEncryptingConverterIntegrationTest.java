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
package com.highkeen.opendcb.dataprotection;

import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@link OpenDcbEncryptingConverter} and {@link OpenDcbKeyStore} together against a real
 * PostgreSQL instance (Testcontainers) -- no mocking of the crypto or the database, matching this
 * repo's standard for concurrency- and correctness-critical modules (see docs/TESTING.md).
 *
 * <p>The delegate {@link Converter} throughout is a real {@code JacksonConverter}
 * ({@code org.axonframework.conversion.jackson}), confirmed via {@code javap} against the actual
 * {@code axon-conversion-5.1.2.jar} to have a working no-arg constructor -- so no
 * {@code tools.jackson.databind.ObjectMapper} needs to be constructed by hand here.
 */
@Testcontainers
class OpenDcbEncryptingConverterIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    /**
     * Matches a JSON field's still-encrypted base64 value directly out of the raw JSON text, deliberately
     * NOT via any decrypt path (not even the plain delegate's own JSON-to-Map conversion), so the
     * captured ciphertext used by {@link #erasureMakesCiphertextPermanentlyUndecryptable()} is
     * independent of every code path under test.
     */
    private static final Pattern EMAIL_FIELD_PATTERN = Pattern.compile("\"email\"\\s*:\\s*\"([^\"]*)\"");

    private static DataSource dataSource;

    @BeforeAll
    static void setUpDatabase() {
        dataSource = newDataSource();
        new OpenDcbKeyStore(dataSource, newMasterKeyProvider()).ensureSchema();
    }

    @BeforeEach
    void truncateTables() throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE TABLE data_protection_key, data_protection_audit_log");
        }
    }

    @Test
    void roundTripEncryptsAndDecryptsPersonalDataFieldsExactly() {
        OpenDcbKeyStore keyStore = new OpenDcbKeyStore(dataSource, newMasterKeyProvider());
        Converter converter = new OpenDcbEncryptingConverter(new JacksonConverter(), keyStore);
        String subjectId = "round-trip-" + UUID.randomUUID();
        CustomerRegistered original = new CustomerRegistered(subjectId, "alice@example.com", "+1-555-0100", "gold");

        String json = converter.convert(original, String.class);
        assertFalse(json.contains("alice@example.com"), "serialized JSON must not contain plaintext email");
        assertFalse(json.contains("555-0100"), "serialized JSON must not contain plaintext phone number");

        CustomerRegistered decoded = converter.convert(json, CustomerRegistered.class);
        assertEquals(original, decoded);
    }

    @Test
    void nonRecordPayloadIsHandledViaDirectFieldMutation() {
        // Exercises OpenDcbEncryptingConverter's non-record fallback path (mutateInPlace), which the
        // round-trip test above does not reach since CustomerRegistered is a record.
        OpenDcbKeyStore keyStore = new OpenDcbKeyStore(dataSource, newMasterKeyProvider());
        Converter converter = new OpenDcbEncryptingConverter(new JacksonConverter(), keyStore);
        String subjectId = "mutable-" + UUID.randomUUID();
        MutableCustomerNote original = new MutableCustomerNote(subjectId, "a private note");

        String json = converter.convert(original, String.class);
        assertFalse(json.contains("a private note"));

        MutableCustomerNote decoded = converter.convert(json, MutableCustomerNote.class);
        assertEquals(subjectId, decoded.getSubjectId());
        assertEquals("a private note", decoded.getNote());
    }

    @Test
    void erasureMakesCiphertextPermanentlyUndecryptable() throws Exception {
        OpenDcbKeyStore keyStore = new OpenDcbKeyStore(dataSource, newMasterKeyProvider());
        Converter converter = new OpenDcbEncryptingConverter(new JacksonConverter(), keyStore);

        String subjectId = "erasure-subject-" + UUID.randomUUID();
        String otherSubjectId = "other-subject-" + UUID.randomUUID();
        CustomerRegistered original = new CustomerRegistered(subjectId, "bob@example.com", "+1-555-0199", "silver");
        CustomerRegistered other = new CustomerRegistered(otherSubjectId, "carol@example.com", "+1-555-0200", "silver");

        String json = converter.convert(original, String.class);
        converter.convert(other, String.class); // establishes otherSubjectId's own, distinct key

        byte[] capturedCiphertext = Base64.getDecoder().decode(extractEmailFieldValue(json));
        assertNotEquals("bob@example.com",
                new String(capturedCiphertext, StandardCharsets.UTF_8), "captured value must be genuine ciphertext");

        keyStore.eraseDataSubject(subjectId);

        // (a) wrapped_key is NULL, confirmed via a direct query against the real table.
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT wrapped_key, erased_at FROM data_protection_key WHERE data_subject_id = ?")) {
            statement.setString(1, subjectId);
            try (ResultSet resultSet = statement.executeQuery()) {
                assertTrue(resultSet.next());
                assertNull(resultSet.getBytes("wrapped_key"));
                assertNotNull(resultSet.getTimestamp("erased_at"));
            }
        }

        // (b) the full decrypt path via the converter returns null, not the original plaintext -- and
        // non-personal-data fields survive untouched.
        CustomerRegistered decoded = converter.convert(json, CustomerRegistered.class);
        assertNull(decoded.email());
        assertNull(decoded.phoneNumber());
        assertEquals(subjectId, decoded.customerId());
        assertEquals("silver", decoded.plan());

        // (c) the captured raw ciphertext cannot be decrypted with any other key: every other
        // subject's key, and a freshly-generated random key -- proving the ciphertext was
        // cryptographically bound to the specific, now-destroyed key, not merely that a database flag
        // changed state.
        byte[] otherSubjectKey = keyStore.getDataKeyIfNotErased(otherSubjectId).orElseThrow();
        assertThrows(IllegalStateException.class, () -> AesGcm.decrypt(otherSubjectKey, capturedCiphertext));

        byte[] randomKey = new byte[32];
        new SecureRandom().nextBytes(randomKey);
        assertThrows(IllegalStateException.class, () -> AesGcm.decrypt(randomKey, capturedCiphertext));
    }

    @Test
    void neverEncryptedFieldDecryptsToNullWithoutThrowing() {
        OpenDcbKeyStore keyStore = new OpenDcbKeyStore(dataSource, newMasterKeyProvider());
        Converter converter = new OpenDcbEncryptingConverter(new JacksonConverter(), keyStore);
        String subjectId = "never-had-pii-" + UUID.randomUUID();

        // Build a JSON payload by hand as if it were legacy data whose email field was never actually
        // encrypted for this subject -- routed through the PLAIN delegate converter only, so no key is
        // ever created as a side effect (which would defeat the point of this test).
        Converter plainJackson = new JacksonConverter();
        CustomerRegistered legacyShapedPayload =
                new CustomerRegistered(subjectId, "not-genuinely-ciphertext", null, "bronze");
        String json = plainJackson.convert(legacyShapedPayload, String.class);

        CustomerRegistered decoded = converter.convert(json, CustomerRegistered.class);
        assertNull(decoded.email());
        assertNull(decoded.phoneNumber());
        assertEquals(subjectId, decoded.customerId());
        assertEquals("bronze", decoded.plan());

        // Distinguish this from the erasure case: no row exists at all (never erased_at set, never
        // created), not merely wrapped_key = NULL after an erasure.
        Optional<byte[]> key = keyStore.getDataKeyIfNotErased(subjectId);
        assertTrue(key.isEmpty());
        assertEquals(0, countKeyRows(subjectId));
    }

    @Test
    void concurrentFirstUseKeyCreationForTheSameSubjectConvergesOnOneKey() throws Exception {
        OpenDcbKeyStore keyStore = new OpenDcbKeyStore(dataSource, newMasterKeyProvider());
        String subjectId = "concurrent-subject-" + UUID.randomUUID();
        int threadCount = 8;

        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<byte[]> results;
        try {
            List<Future<byte[]>> futures = new ArrayList<>();
            for (int i = 0; i < threadCount; i++) {
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    go.await();
                    return keyStore.getOrCreateDataKey(subjectId);
                }));
            }
            assertTrue(ready.await(10, TimeUnit.SECONDS));
            go.countDown();

            results = new ArrayList<>();
            for (Future<byte[]> future : futures) {
                results.add(future.get(30, TimeUnit.SECONDS));
            }
        } finally {
            executor.shutdownNow();
        }

        byte[] first = results.get(0);
        for (byte[] result : results) {
            assertArrayEquals(first, result, "every concurrent caller must converge on the identical key");
        }
        assertEquals(1, countKeyRows(subjectId), "exactly one row must exist after the race");
    }

    @Test
    void auditLogNeverContainsPlaintextPersonalData() {
        OpenDcbKeyStore keyStore = new OpenDcbKeyStore(dataSource, newMasterKeyProvider());
        Converter converter = new OpenDcbEncryptingConverter(new JacksonConverter(), keyStore);
        String subjectId = "audit-subject-" + UUID.randomUUID();
        String secretEmail = "super-secret-plaintext@example.com";
        CustomerRegistered original = new CustomerRegistered(subjectId, secretEmail, "+1-555-0300", "platinum");

        String json = converter.convert(original, String.class); // ENCRYPT
        converter.convert(json, CustomerRegistered.class); // DECRYPT
        keyStore.eraseDataSubject(subjectId); // ERASE
        converter.convert(json, CustomerRegistered.class); // DECRYPT_AFTER_ERASURE

        List<String> operations = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT operation, data_subject_id FROM data_protection_audit_log "
                             + "WHERE data_subject_id = ? ORDER BY occurred_at")) {
            statement.setString(1, subjectId);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String operation = resultSet.getString("operation");
                    String storedSubjectId = resultSet.getString("data_subject_id");
                    assertFalse(operation.contains(secretEmail));
                    assertFalse(storedSubjectId.contains(secretEmail));
                    assertEquals(subjectId, storedSubjectId);
                    operations.add(operation);
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }

        assertEquals(List.of("ENCRYPT", "DECRYPT", "ERASE", "DECRYPT_AFTER_ERASURE"), operations);
    }

    private static String extractEmailFieldValue(String json) {
        Matcher matcher = EMAIL_FIELD_PATTERN.matcher(json);
        assertTrue(matcher.find(), "expected an \"email\" field in: " + json);
        return matcher.group(1);
    }

    private static int countKeyRows(String subjectId) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT COUNT(*) FROM data_protection_key WHERE data_subject_id = ?")) {
            statement.setString(1, subjectId);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getInt(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static MasterKeyProvider newMasterKeyProvider() {
        byte[] key = new byte[32];
        new SecureRandom().nextBytes(key);
        return new EnvVarMasterKeyProvider("UNUSED_TEST_VAR", Base64.getEncoder().encodeToString(key));
    }

    private static DataSource newDataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(POSTGRES.getJdbcUrl());
        dataSource.setUser(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        return dataSource;
    }

    /** Record payload fixture: exercises {@code OpenDcbEncryptingConverter}'s record-reconstruction path. */
    record CustomerRegistered(
            @DataSubjectId String customerId,
            @PersonalData String email,
            @PersonalData String phoneNumber,
            String plan) {
    }

    /** Plain mutable class fixture: exercises {@code OpenDcbEncryptingConverter}'s direct-field-mutation path. */
    static class MutableCustomerNote {

        @DataSubjectId
        private String subjectId;

        @PersonalData
        private String note;

        MutableCustomerNote() {
        }

        MutableCustomerNote(String subjectId, String note) {
            this.subjectId = subjectId;
            this.note = note;
        }

        public String getSubjectId() {
            return subjectId;
        }

        public void setSubjectId(String subjectId) {
            this.subjectId = subjectId;
        }

        public String getNote() {
            return note;
        }

        public void setNote(String note) {
            this.note = note;
        }
    }
}
