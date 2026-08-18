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

import java.util.Base64;

/**
 * {@link MasterKeyProvider} backed by a single base64-encoded, 256-bit AES key read from an
 * environment variable ({@value #DEFAULT_ENV_VAR_NAME} by default).
 *
 * <p><b>Fails fast, deliberately.</b> The constructor reads and validates the environment variable
 * immediately, and throws {@link IllegalStateException} with a clear, specific message if it is
 * missing, not valid base64, or does not decode to exactly 32 bytes. There is no fallback to a
 * default or hardcoded key under any circumstance -- an application that cannot obtain a real master
 * key must fail to start, not silently run with weaker (or no) protection for personal data.
 *
 * <p><b>NOT sufficient, on its own, for a production BFSI deployment.</b> This implementation is a
 * genuinely useful default for local development, testing, and low-stakes deployments, but stating
 * plainly what it does not provide, rather than leaving it implicit: the master key lives in process
 * environment/configuration, with no hardware isolation, no access audit trail of its own, no
 * automated rotation, and no separation of duties between whoever can read the environment and
 * whoever is authorized to decrypt personal data. A real BFSI production deployment is expected to
 * supply its own {@link MasterKeyProvider} backed by a KMS or HSM (e.g. AWS KMS, Google Cloud KMS,
 * Azure Key Vault, HashiCorp Vault, or an on-prem HSM) instead -- this is deferred future work
 * tracked in {@code docs/ROADMAP.md}, not a gap this class quietly works around.
 */
public class EnvVarMasterKeyProvider implements MasterKeyProvider {

    public static final String DEFAULT_ENV_VAR_NAME = "OPENDCB_DATA_PROTECTION_MASTER_KEY";

    private static final int REQUIRED_KEY_LENGTH_BYTES = 32;

    private final byte[] masterKey;

    /** Reads the master key from {@value #DEFAULT_ENV_VAR_NAME}. */
    public EnvVarMasterKeyProvider() {
        this(DEFAULT_ENV_VAR_NAME);
    }

    /** Reads the master key from the given environment variable name instead of the default. */
    public EnvVarMasterKeyProvider(String envVarName) {
        this(envVarName, System.getenv(envVarName));
    }

    /**
     * Test-only entry point that bypasses {@link System#getenv}, since the real environment cannot
     * be modified portably from within a test. Package-private on purpose -- not part of the public
     * API.
     */
    EnvVarMasterKeyProvider(String envVarName, String base64Value) {
        if (base64Value == null || base64Value.isBlank()) {
            throw new IllegalStateException(
                    "Master key environment variable '" + envVarName + "' is not set. "
                            + "opendcb-data-protection requires a base64-encoded 256-bit (32-byte) AES key "
                            + "to start -- refusing to fall back to a default or hardcoded key.");
        }
        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(base64Value.strip());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(
                    "Master key environment variable '" + envVarName + "' is not valid base64.", e);
        }
        if (decoded.length != REQUIRED_KEY_LENGTH_BYTES) {
            throw new IllegalStateException(
                    "Master key environment variable '" + envVarName + "' must decode to exactly "
                            + REQUIRED_KEY_LENGTH_BYTES + " bytes (256 bits) for AES-256; got "
                            + decoded.length + " bytes.");
        }
        this.masterKey = decoded;
    }

    @Override
    public byte[] wrapKey(byte[] dataEncryptionKey) {
        return AesGcm.encrypt(masterKey, dataEncryptionKey);
    }

    @Override
    public byte[] unwrapKey(byte[] wrappedKey) {
        return AesGcm.decrypt(masterKey, wrappedKey);
    }
}
