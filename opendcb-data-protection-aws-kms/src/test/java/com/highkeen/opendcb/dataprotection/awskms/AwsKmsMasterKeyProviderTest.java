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
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.CreateKeyResponse;

import java.security.SecureRandom;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.KMS;

/**
 * Exercises {@link AwsKmsMasterKeyProvider} against a real LocalStack instance (Testcontainers) with
 * KMS enabled -- no mocking of the AWS SDK client, matching this project's
 * no-mocking-on-correctness-critical-paths standard.
 *
 * <p><b>Why this whole class is conditionally skipped, not unconditionally run:</b> since March 2026,
 * LocalStack's unified Docker image requires a LocalStack account and a
 * {@code LOCALSTACK_AUTH_TOKEN} even for free/non-commercial use (its GitHub repo predating this
 * change is now archived/frozen) -- this is the first test suite in this repo with an external-account
 * dependency, a deliberate, discussed trade-off (see {@code docs/ROADMAP.md}), not an oversight. KMS's
 * symmetric Encrypt/Decrypt operations (this provider's only use case) are fully emulated even on
 * LocalStack's free "Hobby" tier; the known emulation gaps (asymmetric keys, custom key material,
 * plaintext-size validation) don't affect this module. This class is skipped, not failed, when no
 * token is configured in the environment/CI secrets -- honest "not exercised here" rather than a
 * false pass from a stub.
 */
@Testcontainers
@EnabledIfEnvironmentVariable(named = "LOCALSTACK_AUTH_TOKEN", matches = ".+")
class AwsKmsMasterKeyProviderTest {

    @Container
    static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
                    DockerImageName.parse("localstack/localstack:4.9"))
            .withServices(KMS)
            .withEnv("LOCALSTACK_AUTH_TOKEN", System.getenv("LOCALSTACK_AUTH_TOKEN"));

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

    private static byte[] randomKey() {
        byte[] key = new byte[32];
        new SecureRandom().nextBytes(key);
        return key;
    }
}
