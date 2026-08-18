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

/**
 * The key-encryption-key (KEK) side of envelope encryption: wraps and unwraps the per-data-subject
 * AES-256 data encryption keys (DEKs) that {@link OpenDcbKeyStore} persists. Only the wrapped form
 * of a DEK is ever written to {@code data_protection_key.wrapped_key} -- the master key itself never
 * touches the database.
 *
 * <p>Implementations must be safe to call concurrently from multiple threads (and, since {@link
 * OpenDcbKeyStore} is used across JVMs against a shared database, from multiple independent JVM
 * instances too) -- {@code wrapKey}/{@code unwrapKey} are stateless operations, so this is
 * straightforward for any implementation that doesn't hold mutable shared state, but it is a real
 * requirement, not an incidental one, since {@link OpenDcbKeyStore#getOrCreateDataKey} can call
 * {@code wrapKey} from concurrent first-use races.
 *
 * @see EnvVarMasterKeyProvider the only implementation this module ships
 */
public interface MasterKeyProvider {

    /**
     * Wraps (encrypts) a raw, unwrapped 32-byte AES-256 data encryption key so it is safe to persist.
     *
     * @param dataEncryptionKey the raw DEK bytes; never logged, never returned as-is
     * @return the wrapped form, opaque to the caller -- passed back to {@link #unwrapKey} unchanged
     */
    byte[] wrapKey(byte[] dataEncryptionKey);

    /**
     * Unwraps (decrypts) a previously {@link #wrapKey wrapped} data encryption key.
     *
     * @param wrappedKey the wrapped bytes, exactly as previously returned by {@link #wrapKey}
     * @return the raw, unwrapped DEK bytes
     * @throws RuntimeException if {@code wrappedKey} was not produced by this same master key (e.g.
     *     tampered, corrupted, or wrapped under a different/rotated master key this provider does
     *     not hold)
     */
    byte[] unwrapKey(byte[] wrappedKey);
}
