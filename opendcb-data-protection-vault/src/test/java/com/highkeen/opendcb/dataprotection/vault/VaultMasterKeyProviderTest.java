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
package com.highkeen.opendcb.dataprotection.vault;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.vault.VaultContainer;

import java.security.SecureRandom;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@link VaultMasterKeyProvider} against a real HashiCorp Vault instance (Testcontainers,
 * dev mode) with the Transit secrets engine enabled -- no mocking of the Vault client, matching this
 * project's no-mocking-on-correctness-critical-paths standard.
 */
@Testcontainers
class VaultMasterKeyProviderTest {

    private static final String TRANSIT_KEY_NAME = "opendcb-test-key";
    private static final String ROOT_TOKEN = "opendcb-test-root-token";

    @Container
    static final VaultContainer<?> VAULT = new VaultContainer<>(DockerImageName.parse("hashicorp/vault:1.18.1"))
            .withVaultToken(ROOT_TOKEN)
            .withInitCommand("secrets enable transit", "write -f transit/keys/" + TRANSIT_KEY_NAME);

    private VaultMasterKeyProvider provider() {
        return new VaultMasterKeyProvider(VAULT.getHttpHostAddress(), ROOT_TOKEN, TRANSIT_KEY_NAME);
    }

    @Test
    void wrapThenUnwrapRoundTripsTheOriginalKey() {
        byte[] dataEncryptionKey = randomKey();
        VaultMasterKeyProvider provider = provider();

        byte[] wrapped = provider.wrapKey(dataEncryptionKey);
        byte[] unwrapped = provider.unwrapKey(wrapped);

        assertArrayEquals(dataEncryptionKey, unwrapped);
    }

    @Test
    void wrappingTheSameKeyTwiceProducesDifferentCiphertextButBothStillUnwrapCorrectly() {
        byte[] dataEncryptionKey = randomKey();
        VaultMasterKeyProvider provider = provider();

        byte[] wrappedFirst = provider.wrapKey(dataEncryptionKey);
        byte[] wrappedSecond = provider.wrapKey(dataEncryptionKey);

        assertFalse(
                Arrays.equals(wrappedFirst, wrappedSecond),
                "Vault Transit encrypt is non-deterministic by default (random nonce per call); "
                        + "two wraps of the same plaintext must not produce identical ciphertext.");
        // Non-determinism is safe here only because OpenDcbKeyStore calls wrapKey exactly once per
        // data-subject key, at creation time -- confirm both results still independently decrypt back
        // to the original key, rather than only one of them being valid.
        assertArrayEquals(dataEncryptionKey, provider.unwrapKey(wrappedFirst));
        assertArrayEquals(dataEncryptionKey, provider.unwrapKey(wrappedSecond));
    }

    @Test
    void constructorFailsFastAndClearlyWhenVaultIsUnreachable() {
        IllegalStateException e = assertThrows(
                IllegalStateException.class,
                () -> new VaultMasterKeyProvider("http://127.0.0.1:1", ROOT_TOKEN, TRANSIT_KEY_NAME));

        assertTrue(
                e.getMessage().contains("Failed to connect"),
                "Expected a clear unreachable-Vault message, got: " + e.getMessage());
    }

    @Test
    void wrapKeyAutoCreatesTheTransitKeyWhenItDoesNotAlreadyExist() {
        // Real, documented Vault Transit behavior (not an OpenDCB design choice): the encrypt endpoint
        // auto-vivifies the named key on first use rather than failing. Asserted here so a future Vault
        // upgrade that changed this behavior would be caught by a test, not just by this comment.
        VaultMasterKeyProvider provider = new VaultMasterKeyProvider(
                VAULT.getHttpHostAddress(), ROOT_TOKEN, "auto-created-key");

        byte[] dataEncryptionKey = randomKey();
        byte[] wrapped = provider.wrapKey(dataEncryptionKey);

        assertArrayEquals(dataEncryptionKey, provider.unwrapKey(wrapped));
    }

    @Test
    void unwrapKeyFailsClearlyWhenTheConfiguredTransitKeyDoesNotExist() {
        // Unlike encrypt, decrypt cannot auto-create a key -- there is no key material to derive
        // plaintext from -- so this is the real "the Transit key doesn't exist" failure path.
        VaultMasterKeyProvider provider = new VaultMasterKeyProvider(
                VAULT.getHttpHostAddress(), ROOT_TOKEN, "does-not-exist");

        IllegalStateException e = assertThrows(
                IllegalStateException.class,
                () -> provider.unwrapKey("vault:v1:irrelevant-ciphertext".getBytes()));

        assertTrue(
                e.getMessage().contains("does-not-exist"),
                "Expected the failure message to name the missing Transit key, got: " + e.getMessage());
    }

    private static byte[] randomKey() {
        byte[] key = new byte[32];
        new SecureRandom().nextBytes(key);
        return key;
    }
}
