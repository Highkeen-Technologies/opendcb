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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.CreateKeyResponse;

import java.net.URI;
import java.security.SecureRandom;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.KMS;

/**
 * Exercises {@link AwsKmsMasterKeyProvider} against a real LocalStack instance (Testcontainers) with
 * KMS enabled -- no mocking of the AWS SDK client, matching this project's
 * no-mocking-on-correctness-critical-paths standard.
 *
 * <p><b>Why this runs unconditionally against community LocalStack, with no auth token:</b> LocalStack's
 * {@code localstack/localstack:latest} tag started requiring a {@code LOCALSTACK_AUTH_TOKEN} on
 * 2026-03-23, but that requirement is tied to the image tag, not to KMS itself -- it does not apply
 * retroactively to older pinned tags. This module pins {@code localstack/localstack:4.9}
 * (LocalStack 4.9.2, built 2025-10-06, predating the 2026-03-23 cutover by five months), and that
 * pinned image was verified directly (a manual {@code docker run} + {@code aws --endpoint-url}
 * {@code kms create-key}/{@code encrypt}/{@code decrypt} round trip, with zero
 * {@code LOCALSTACK_AUTH_TOKEN} and zero account) to start and serve KMS with no token at all -- the
 * account-requirement gate this test previously carried was based on an unverified assumption about
 * the unified image in general, not on anything actually tested against the specific pinned tag this
 * class uses. Community/free-tier LocalStack has always emulated KMS's symmetric CreateKey/Encrypt/
 * Decrypt operations (this provider's only use case); the known emulation gaps (asymmetric keys,
 * custom key material, plaintext-size validation) don't affect this module. No special environment
 * setup is required to run this test, matching every other Testcontainers-backed suite in this repo.
 */
@Testcontainers
class AwsKmsMasterKeyProviderTest {

    @Container
    static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
                    DockerImageName.parse("localstack/localstack:4.9"))
            .withServices(KMS);

    private static KmsClient kmsClient;
    private static String keyId;

    @BeforeAll
    static void createKmsClientAndKey() {
        kmsClient = KmsClient.builder()
                .endpointOverride(LOCALSTACK.getEndpointOverride(KMS))
                .region(Region.of(LOCALSTACK.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
                .build();

        CreateKeyResponse createKeyResponse = kmsClient.createKey();
        keyId = createKeyResponse.keyMetadata().keyId();
    }

    private AwsKmsMasterKeyProvider provider() {
        return new AwsKmsMasterKeyProvider(kmsClient, keyId);
    }

    @Test
    void wrapThenUnwrapRoundTripsTheOriginalKey() {
        byte[] dataEncryptionKey = randomKey();
        AwsKmsMasterKeyProvider provider = provider();

        byte[] wrapped = provider.wrapKey(dataEncryptionKey);
        byte[] unwrapped = provider.unwrapKey(wrapped);

        assertArrayEquals(dataEncryptionKey, unwrapped);
    }

    @Test
    void wrapKeyFailsClearlyWhenTheConfiguredCmkDoesNotExist() {
        AwsKmsMasterKeyProvider provider = new AwsKmsMasterKeyProvider(
                kmsClient, "00000000-0000-0000-0000-000000000000");

        assertThrows(IllegalStateException.class, () -> provider.wrapKey(randomKey()));
    }

    @Test
    void unwrapKeyFailsClearlyOnGarbageCiphertext() {
        AwsKmsMasterKeyProvider provider = provider();

        assertThrows(IllegalStateException.class, () -> provider.unwrapKey(randomKey()));
    }

    @Test
    void wrapKeyFailsClearlyWhenKmsIsUnreachable() {
        // Unlike VaultMasterKeyProvider, this class's constructor never connects eagerly -- it just
        // stores an already-configured KmsClient (see the class Javadoc). So the equivalent of
        // Vault's "constructor fails fast when unreachable" test has to trigger the failure on first
        // real call instead: a KmsClient pointed at a port nothing is listening on.
        KmsClient unreachableClient = KmsClient.builder()
                .endpointOverride(URI.create("http://127.0.0.1:1"))
                .region(Region.of(LOCALSTACK.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
                .build();
        AwsKmsMasterKeyProvider provider = new AwsKmsMasterKeyProvider(unreachableClient, keyId);

        IllegalStateException e = assertThrows(
                IllegalStateException.class, () -> provider.wrapKey(randomKey()));

        assertTrue(
                e.getMessage().contains("AWS KMS Encrypt failed"),
                "Expected a clear unreachable-KMS message, got: " + e.getMessage());
    }

    private static byte[] randomKey() {
        byte[] key = new byte[32];
        new SecureRandom().nextBytes(key);
        return key;
    }
}
