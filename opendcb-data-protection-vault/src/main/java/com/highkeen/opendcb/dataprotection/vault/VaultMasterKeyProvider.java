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

import com.highkeen.opendcb.dataprotection.MasterKeyProvider;
import de.stklcode.jvault.connector.HTTPVaultConnector;
import de.stklcode.jvault.connector.VaultConnector;
import de.stklcode.jvault.connector.exception.ConnectionException;
import de.stklcode.jvault.connector.exception.InvalidResponseException;
import de.stklcode.jvault.connector.exception.VaultConnectorException;
import de.stklcode.jvault.connector.model.response.TransitResponse;

import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * {@link MasterKeyProvider} backed by HashiCorp Vault's Transit secrets engine.
 *
 * <p>{@link #wrapKey(byte[])} calls Transit's {@code encrypt} endpoint with the data encryption key
 * as plaintext under a configured Transit key name; {@link #unwrapKey(byte[])} calls {@code decrypt}
 * to recover it. Vault's own ciphertext format ({@code "vault:v1:..."}, an opaque, versioned,
 * ASCII-safe string -- confirmed against Transit's real HTTP API, not assumed) is stored as-is,
 * UTF-8 encoded, as this provider's wrapped-key bytes; there is no need to re-encode it, since it is
 * already Vault's own durable on-the-wire representation.
 *
 * <p><b>Fails fast, deliberately, matching {@code EnvVarMasterKeyProvider}'s discipline.</b> Every
 * Vault call failure -- unreachable server, authentication failure, a Transit key that doesn't
 * exist -- is wrapped in an {@link IllegalStateException} with a clear, specific message rather
 * than silently falling back to any weaker behavior. One caveat worth stating plainly, since it's
 * real Vault Transit behavior rather than an OpenDCB design choice: {@link #wrapKey(byte[])}'s
 * underlying {@code encrypt} endpoint auto-creates the named Transit key on first use if it doesn't
 * already exist yet, rather than failing -- a misconfigured/typo'd key name is silently
 * self-healing on the encrypt path (confirmed against a real Vault instance, not assumed). A
 * missing key only surfaces as a real, catchable failure on {@link #unwrapKey(byte[])}'s
 * {@code decrypt} path, which has no key material to auto-create from. Operators who want a typo'd
 * key name to fail loudly on first {@code wrapKey} rather than silently mint a new key should
 * restrict the Vault token's policy to deny {@code create} capability on {@code transit/keys/*}
 * while still allowing {@code update} on {@code transit/encrypt/*} -- a deployment-time policy
 * decision, not something this class enforces itself.
 *
 * <p>Vault's Transit {@code encrypt} operation is non-deterministic by default (no
 * {@code convergent_encryption} configured on the key): encrypting the same plaintext twice produces
 * different ciphertext each time, standard AEAD random-nonce behavior. This is harmless for how
 * {@code opendcb-data-protection}'s {@code OpenDcbKeyStore} actually uses a {@link MasterKeyProvider}
 * -- {@code wrapKey} is called exactly once per data-subject key, at first-use creation time, and the
 * resulting ciphertext is stored as-is; nothing ever compares two independently-produced wrap results
 * for equality.
 */
public class VaultMasterKeyProvider implements MasterKeyProvider {

    private final VaultConnector connector;
    private final String transitKeyName;

    /**
     * Connects to Vault at the given base URL, authenticating with the given token, and wraps/unwraps
     * keys under the given Transit key name.
     *
     * @param vaultBaseUrl   Vault's base URL, e.g. {@code "http://127.0.0.1:8200"}
     * @param vaultToken     a Vault token authorized for {@code transit/encrypt/<transitKeyName>} and
     *                       {@code transit/decrypt/<transitKeyName>}
     * @param transitKeyName the name of an existing Transit key (Transit must already be enabled and
     *                       this key already created -- this provider never creates either)
     */
    public VaultMasterKeyProvider(String vaultBaseUrl, String vaultToken, String transitKeyName) {
        this(connect(vaultBaseUrl, vaultToken), transitKeyName);
    }

    /**
     * Test-only entry point taking an already-built connector, since a real integration test needs to
     * point at a Testcontainers-managed Vault instance without duplicating this class's own connection
     * logic. Package-private on purpose -- not part of the public API.
     */
    VaultMasterKeyProvider(HTTPVaultConnector connector, String transitKeyName) {
        this.connector = connector;
        this.transitKeyName = transitKeyName;
    }

    private static HTTPVaultConnector connect(String vaultBaseUrl, String vaultToken) {
        try {
            return HTTPVaultConnector.builder(vaultBaseUrl).withToken(vaultToken).buildAndAuth();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Vault base URL '" + vaultBaseUrl + "' is not a valid URI.", e);
        } catch (VaultConnectorException e) {
            throw new IllegalStateException(
                    "Failed to connect to or authenticate against Vault at '" + vaultBaseUrl + "'.", e);
        }
    }

    @Override
    public byte[] wrapKey(byte[] dataEncryptionKey) {
        try {
            TransitResponse response = connector.transitEncrypt(transitKeyName, dataEncryptionKey);
            String ciphertext = response.getCiphertext();
            if (ciphertext == null) {
                throw new IllegalStateException(
                        "Vault Transit encrypt under key '" + transitKeyName + "' returned no ciphertext.");
            }
            return ciphertext.getBytes(StandardCharsets.UTF_8);
        } catch (VaultConnectorException e) {
            throw mapFailure("encrypt", e);
        }
    }

    @Override
    public byte[] unwrapKey(byte[] wrappedKey) {
        String ciphertext = new String(wrappedKey, StandardCharsets.UTF_8);
        try {
            TransitResponse response = connector.transitDecrypt(transitKeyName, ciphertext);
            String plaintextBase64 = response.getPlaintext();
            if (plaintextBase64 == null) {
                throw new IllegalStateException(
                        "Vault Transit decrypt under key '" + transitKeyName + "' returned no plaintext.");
            }
            return Base64.getDecoder().decode(plaintextBase64);
        } catch (VaultConnectorException e) {
            throw mapFailure("decrypt", e);
        }
    }

    private IllegalStateException mapFailure(String operation, VaultConnectorException e) {
        if (e instanceof ConnectionException) {
            return new IllegalStateException(
                    "Vault is unreachable while attempting transit " + operation
                            + " under key '" + transitKeyName + "'.", e);
        }
        if (e instanceof InvalidResponseException invalidResponse) {
            return new IllegalStateException(
                    "Vault returned an invalid response (HTTP status " + invalidResponse.getStatusCode()
                            + ") while attempting transit " + operation + " under key '" + transitKeyName
                            + "' -- if the status is 404, the Transit key does not exist.", e);
        }
        return new IllegalStateException(
                "Vault Transit " + operation + " failed under key '" + transitKeyName + "'.", e);
    }
}
