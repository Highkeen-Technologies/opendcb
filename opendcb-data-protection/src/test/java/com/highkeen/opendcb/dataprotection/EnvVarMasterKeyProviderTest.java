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

import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EnvVarMasterKeyProviderTest {

    private static final String VAR_NAME = "OPENDCB_DATA_PROTECTION_MASTER_KEY_TEST";

    @Test
    void throwsWhenEnvVarIsMissing() {
        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> new EnvVarMasterKeyProvider(VAR_NAME, null));
        assertContainsVarName(exception);
    }

    @Test
    void throwsWhenEnvVarIsBlank() {
        assertThrows(IllegalStateException.class, () -> new EnvVarMasterKeyProvider(VAR_NAME, "   "));
    }

    @Test
    void throwsWhenEnvVarIsNotValidBase64() {
        assertThrows(IllegalStateException.class,
                () -> new EnvVarMasterKeyProvider(VAR_NAME, "not-valid-base64!!!"));
    }

    @Test
    void throwsWhenDecodedKeyIsWrongLength() {
        byte[] tooShort = new byte[16];
        String base64 = Base64.getEncoder().encodeToString(tooShort);
        assertThrows(IllegalStateException.class, () -> new EnvVarMasterKeyProvider(VAR_NAME, base64));
    }

    @Test
    void neverFallsBackToADefaultKey() {
        // Two independently-missing-env-var providers must both refuse to start, never silently
        // agreeing on some default/hardcoded key.
        assertThrows(IllegalStateException.class, () -> new EnvVarMasterKeyProvider(VAR_NAME, null));
        assertThrows(IllegalStateException.class, () -> new EnvVarMasterKeyProvider(VAR_NAME, ""));
    }

    @Test
    void acceptsAValid32ByteBase64Key() {
        String base64 = randomBase64Key();
        assertDoesNotThrow(() -> new EnvVarMasterKeyProvider(VAR_NAME, base64));
    }

    @Test
    void wrapThenUnwrapRecoversTheOriginalDataEncryptionKey() {
        EnvVarMasterKeyProvider provider = new EnvVarMasterKeyProvider(VAR_NAME, randomBase64Key());
        byte[] dek = new byte[32];
        new SecureRandom().nextBytes(dek);

        byte[] wrapped = provider.wrapKey(dek);
        assertArrayEquals(dek, provider.unwrapKey(wrapped));
    }

    @Test
    void wrappingTheSameKeyTwiceProducesDifferentCiphertextEachTime() {
        // A fresh random IV per call means the same (key, master key) pair must never produce
        // identical wrapped output twice -- this is GCM's own hard requirement, verified here.
        EnvVarMasterKeyProvider provider = new EnvVarMasterKeyProvider(VAR_NAME, randomBase64Key());
        byte[] dek = new byte[32];
        new SecureRandom().nextBytes(dek);

        byte[] wrapped1 = provider.wrapKey(dek);
        byte[] wrapped2 = provider.wrapKey(dek);
        org.junit.jupiter.api.Assertions.assertNotEquals(
                Base64.getEncoder().encodeToString(wrapped1), Base64.getEncoder().encodeToString(wrapped2));
    }

    private static void assertContainsVarName(Exception exception) {
        org.junit.jupiter.api.Assertions.assertTrue(exception.getMessage().contains(VAR_NAME));
    }

    private static String randomBase64Key() {
        byte[] key = new byte[32];
        new SecureRandom().nextBytes(key);
        return Base64.getEncoder().encodeToString(key);
    }
}
