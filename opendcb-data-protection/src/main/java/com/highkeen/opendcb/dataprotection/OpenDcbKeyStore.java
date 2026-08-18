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

import javax.crypto.KeyGenerator;
import javax.sql.DataSource;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Owns two tables, both fully independent of the event log's own tables: {@code data_protection_key}
 * (one row per data subject, holding that subject's envelope-wrapped AES-256 data encryption key)
 * and {@code data_protection_audit_log} (append-only record of encrypt/decrypt/erase operations,
 * never the personal data itself).
 *
 * <p>Crypto-shredding is the whole point of this class: {@link #eraseDataSubject} destroys a
 * subject's key rather than touching any event payload. Every payload already encrypted under that
 * key becomes permanently undecryptable the instant the key is gone -- the ciphertext bytes remain
 * in the (immutable) event log exactly as they always were, but AES-256-GCM's authentication
 * guarantee means no key other than the exact one that encrypted a given ciphertext can ever decrypt
 * it. See {@link #eraseDataSubject}'s own Javadoc for the honest limits of what this guarantees at
 * the database-storage level, as distinct from the cryptographic guarantee itself.
 *
 * <p>{@code data_subject_id} is declared {@code PRIMARY KEY} deliberately, not incidentally: {@link
 * #getOrCreateDataKey} relies on the database's own uniqueness enforcement to make concurrent
 * first-use key creation for the same subject race-safe, the same insert-and-let-the-database-reject
 * pattern {@code opendcb-conductor-bridge}'s {@code SagaCorrelationStore} already uses for {@code
 * saga_correlation} -- not a separate check-then-act, which would reintroduce the race.
 */
public class OpenDcbKeyStore {

    private static final String UNIQUE_VIOLATION_SQL_STATE = "23505";
    private static final String KEY_ALGORITHM_LABEL = "AES-256-GCM";
    private static final int DEK_LENGTH_BITS = 256;

    private static final String KEY_TABLE_DDL = """
            CREATE TABLE IF NOT EXISTS data_protection_key (
                data_subject_id TEXT PRIMARY KEY,
                wrapped_key BYTEA,
                key_algorithm TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                erased_at TIMESTAMPTZ
            )""";

    private static final String AUDIT_TABLE_DDL = """
            CREATE TABLE IF NOT EXISTS data_protection_audit_log (
                id UUID PRIMARY KEY,
                operation TEXT NOT NULL,
                data_subject_id TEXT NOT NULL,
                occurred_at TIMESTAMPTZ NOT NULL
            )""";

    private final DataSource dataSource;
    private final MasterKeyProvider masterKeyProvider;

    public OpenDcbKeyStore(DataSource dataSource, MasterKeyProvider masterKeyProvider) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.masterKeyProvider = Objects.requireNonNull(masterKeyProvider, "masterKeyProvider must not be null");
    }

    /** Creates both tables if they do not already exist. Safe to call repeatedly. */
    public void ensureSchema() {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(KEY_TABLE_DDL);
            statement.execute(AUDIT_TABLE_DDL);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to create opendcb-data-protection schema", e);
        }
    }

    /**
     * Returns the data subject's AES-256 data encryption key (unwrapped), generating and persisting
     * a new one on first use.
     *
     * <p>Safe under true concurrent first-use from multiple threads or JVMs for the <em>same</em>
     * subject id: every caller generates its own candidate key and attempts an {@code INSERT}
     * first, rather than checking for an existing row before deciding whether to create one. Exactly
     * one insert can ever succeed (the {@code PRIMARY KEY} constraint enforces this at the database
     * level); every other concurrent caller's insert fails with a {@code 23505} unique-violation, at
     * which point it re-reads the winner's row and returns that key instead of its own discarded
     * candidate -- so two threads racing for the same new subject always converge on one identical
     * key, never two different ones.
     *
     * @throws IllegalStateException if the subject was erased in the narrow window between this
     *     call losing the creation race and re-reading the winning row -- creating a key for an
     *     already-erased subject is a contradiction this method refuses to paper over
     */
    public byte[] getOrCreateDataKey(String dataSubjectId) {
        Objects.requireNonNull(dataSubjectId, "dataSubjectId must not be null");
        byte[] candidateDek = generateAes256Key();
        byte[] wrappedCandidate = masterKeyProvider.wrapKey(candidateDek);
        String insertSql = "INSERT INTO data_protection_key (data_subject_id, wrapped_key, key_algorithm, created_at) "
                + "VALUES (?, ?, ?, now())";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(insertSql)) {
            statement.setString(1, dataSubjectId);
            statement.setBytes(2, wrappedCandidate);
            statement.setString(3, KEY_ALGORITHM_LABEL);
            statement.executeUpdate();
            return candidateDek;
        } catch (SQLException e) {
            if (UNIQUE_VIOLATION_SQL_STATE.equals(e.getSQLState())) {
                return readExistingKeyAfterLostRace(dataSubjectId);
            }
            throw new IllegalStateException(
                    "Failed to create data protection key for subject '" + dataSubjectId + "'", e);
        }
    }

    private byte[] readExistingKeyAfterLostRace(String dataSubjectId) {
        String selectSql = "SELECT wrapped_key FROM data_protection_key WHERE data_subject_id = ?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(selectSql)) {
            statement.setString(1, dataSubjectId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new IllegalStateException(
                            "Concurrent key creation for subject '" + dataSubjectId + "' reported a unique-"
                                    + "violation conflict, but no row was found on re-read; this should be "
                                    + "impossible under a PRIMARY KEY race.");
                }
                byte[] wrapped = resultSet.getBytes("wrapped_key");
                if (wrapped == null) {
                    throw new IllegalStateException(
                            "Subject '" + dataSubjectId + "' was erased concurrently with its own first-use "
                                    + "key creation; refusing to fabricate a key for an already-erased subject.");
                }
                return masterKeyProvider.unwrapKey(wrapped);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to re-read data protection key for subject '" + dataSubjectId
                            + "' after losing a concurrent-creation race", e);
        }
    }

    /**
     * Returns the data subject's unwrapped data encryption key, or {@link Optional#empty()} if the
     * subject has been {@link #eraseDataSubject erased} or has never had a key created (no {@link
     * PersonalData @PersonalData} field has ever been encrypted for this subject). Both cases are
     * indistinguishable from this method alone by design -- callers needing to tell them apart
     * should check {@code erased_at} directly if that distinction ever matters to them.
     */
    public Optional<byte[]> getDataKeyIfNotErased(String dataSubjectId) {
        Objects.requireNonNull(dataSubjectId, "dataSubjectId must not be null");
        String selectSql = "SELECT wrapped_key FROM data_protection_key WHERE data_subject_id = ?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(selectSql)) {
            statement.setString(1, dataSubjectId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }
                byte[] wrapped = resultSet.getBytes("wrapped_key");
                if (wrapped == null) {
                    return Optional.empty();
                }
                return Optional.of(masterKeyProvider.unwrapKey(wrapped));
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to read data protection key for subject '" + dataSubjectId + "'", e);
        }
    }

    /**
     * Erases a data subject: sets {@code wrapped_key = NULL} and {@code erased_at = now()} in one
     * {@code UPDATE}. A no-op (still records the audit entry, but changes no row) if the subject has
     * no row at all.
     *
     * <p><b>What this does and does not guarantee, stated plainly for compliance sign-off.</b> Once
     * this returns, the wrapped key is gone from this table, which means the corresponding data
     * encryption key is unrecoverable <em>through this store</em> -- every {@link
     * PersonalData @PersonalData} value ever encrypted under it becomes permanently undecryptable
     * through the normal read path ({@link OpenDcbEncryptingConverter}). That is a genuine
     * cryptographic guarantee: AES-256-GCM's authentication tag means no other key can substitute
     * for the destroyed one.
     *
     * <p>What it does <em>not</em> guarantee is that the overwritten {@code wrapped_key} bytes are
     * physically unrecoverable from the database's own storage layer. A determined actor with direct
     * database access could, in some configurations, recover an already-overwritten value from dead
     * tuples before autovacuum reclaims them, or from WAL segments / physical backups taken before
     * the erasure. This is a known, standard limitation of "erase by overwrite" in any MVCC database
     * and is not specific to this implementation. Recommended mitigations, to document alongside
     * this limitation for compliance sign-off rather than treating it as solved: routine {@code
     * VACUUM} hygiene, encrypting backups and WAL archives at rest, and enforcing a bounded backup
     * retention window so an erased key cannot resurface indefinitely from an old backup.
     */
    public void eraseDataSubject(String dataSubjectId) {
        Objects.requireNonNull(dataSubjectId, "dataSubjectId must not be null");
        String updateSql = "UPDATE data_protection_key SET wrapped_key = NULL, erased_at = now() "
                + "WHERE data_subject_id = ?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(updateSql)) {
            statement.setString(1, dataSubjectId);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to erase data subject '" + dataSubjectId + "'", e);
        }
        recordAudit(AuditOperation.ERASE, dataSubjectId);
    }

    /**
     * Appends one row to {@code data_protection_audit_log}. Public so {@link
     * OpenDcbEncryptingConverter} can record {@code ENCRYPT}/{@code DECRYPT}/{@code
     * DECRYPT_AFTER_ERASURE} entries through the same store that owns the audit table, without a
     * second table-owning class. Never pass personal data as any argument here -- only {@code
     * operation} and {@code dataSubjectId} are ever persisted.
     */
    public void recordAudit(AuditOperation operation, String dataSubjectId) {
        Objects.requireNonNull(operation, "operation must not be null");
        Objects.requireNonNull(dataSubjectId, "dataSubjectId must not be null");
        String insertSql = "INSERT INTO data_protection_audit_log (id, operation, data_subject_id, occurred_at) "
                + "VALUES (?, ?, ?, now())";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(insertSql)) {
            statement.setObject(1, UUID.randomUUID());
            statement.setString(2, operation.name());
            statement.setString(3, dataSubjectId);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to record data protection audit entry (" + operation + ") for subject '"
                            + dataSubjectId + "'", e);
        }
    }

    private static byte[] generateAes256Key() {
        try {
            KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
            keyGenerator.init(DEK_LENGTH_BITS);
            return keyGenerator.generateKey().getEncoded();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("AES-256 key generation is not available on this JVM", e);
        }
    }
}
