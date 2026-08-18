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
package com.highkeen.opendcb.dataprotection.awskms;

import com.highkeen.opendcb.dataprotection.MasterKeyProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.EncryptRequest;
import software.amazon.awssdk.services.kms.model.EncryptResponse;

import java.util.Objects;

/**
 * {@link MasterKeyProvider} backed by AWS KMS.
 *
 * <p>{@link #wrapKey(byte[])} calls KMS's {@code Encrypt} operation with the data encryption key as
 * plaintext under a configured customer master key (CMK); {@link #unwrapKey(byte[])} calls
 * {@code Decrypt} to recover it. Deliberately {@code Encrypt}/{@code Decrypt}, not
 * {@code GenerateDataKey} -- {@code GenerateDataKey} has KMS generate a brand-new key server-side,
 * which cannot implement this class's fixed {@code wrapKey(byte[] existingKey)} contract (wrapping a
 * caller-supplied plaintext, not minting a new one). {@code Encrypt}/{@code Decrypt} is the correct,
 * and only, pairing for that contract; earlier docs describing this module as
 * {@code GenerateDataKey}/{@code Decrypt} were a documentation error, corrected alongside this class.
 *
 * <p><b>Fails fast, deliberately, matching {@code EnvVarMasterKeyProvider}/{@code
 * VaultMasterKeyProvider}'s discipline.</b> Any AWS SDK failure -- access denied, key not found,
 * key disabled, network/client error -- is wrapped in an {@link IllegalStateException} with a clear,
 * specific message rather than silently falling back to any weaker behavior. Every exception the real
 * {@link KmsClient#encrypt(EncryptRequest)}/{@link KmsClient#decrypt(DecryptRequest)} operations can
 * throw is unchecked and shares a common ancestor, {@link SdkException} (confirmed via the real
 * published SDK's class hierarchy, not assumed), so a single catch clause is sufficient and exhaustive
 * -- there is no unchecked-exception category this provider could silently let escape unwrapped.
 */
public class AwsKmsMasterKeyProvider implements MasterKeyProvider {

    private final KmsClient kmsClient;
    private final String keyId;

    /**
     * @param kmsClient an already-configured KMS client (region, credentials, endpoint override for
     *                  tests, etc. are entirely the caller's concern -- this class never constructs
     *                  its own client)
     * @param keyId     the CMK's key ID or ARN to encrypt/decrypt under
     */
    public AwsKmsMasterKeyProvider(KmsClient kmsClient, String keyId) {
        this.kmsClient = Objects.requireNonNull(kmsClient, "kmsClient must not be null");
        this.keyId = Objects.requireNonNull(keyId, "keyId must not be null");
    }

    @Override
    public byte[] wrapKey(byte[] dataEncryptionKey) {
        try {
            EncryptResponse response = kmsClient.encrypt(
                    EncryptRequest.builder()
                            .keyId(keyId)
                            .plaintext(SdkBytes.fromByteArray(dataEncryptionKey))
                            .build());
            return response.ciphertextBlob().asByteArray();
        } catch (SdkException e) {
            throw new IllegalStateException(
                    "AWS KMS Encrypt failed under key '" + keyId + "'.", e);
        }
    }

    @Override
    public byte[] unwrapKey(byte[] wrappedKey) {
        try {
            DecryptResponse response = kmsClient.decrypt(
                    DecryptRequest.builder()
                            .keyId(keyId)
                            .ciphertextBlob(SdkBytes.fromByteArray(wrappedKey))
                            .build());
            return response.plaintext().asByteArray();
        } catch (SdkException e) {
            throw new IllegalStateException(
                    "AWS KMS Decrypt failed under key '" + keyId + "'.", e);
        }
    }
}
