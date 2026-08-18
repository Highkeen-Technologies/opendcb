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

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;

/**
 * AES-256-GCM authenticated encryption, used both for wrapping/unwrapping per-subject data
 * encryption keys ({@link MasterKeyProvider}) and for encrypting/decrypting {@link
 * PersonalData @PersonalData} field values ({@link OpenDcbEncryptingConverter}). Not a public API
 * of this module -- an internal primitive shared by both.
 *
 * <p>Algorithm/parameter choices, grounded rather than assumed:
 *
 * <ul>
 *   <li><b>{@code AES/GCM/NoPadding}</b> via {@link Cipher}: the standard JCE transformation string
 *       for AES-GCM, backed by the JDK's built-in SunJCE provider on every JDK 21 installation --
 *       no external crypto library needed.
 *   <li><b>96-bit (12-byte) IV</b>: NIST SP 800-38D (the GCM specification) recommends the 96-bit IV
 *       length specifically because it is the length GCM's own IV-to-counter construction handles
 *       most efficiently and without an extra internal hashing step that longer or shorter IVs
 *       require (SP 800-38D section 8.2's "the length of 96 bits ... enjoys certain performance
 *       advantages"). Every other explicit or well-known reference implementation (e.g. Java's own
 *       {@link GCMParameterSpec} usage examples in the JCE reference guide) uses 12 bytes for this
 *       same reason. This class hard-codes 12 bytes accordingly rather than accepting a
 *       caller-supplied length.
 *   <li><b>128-bit (16-byte) authentication tag</b>: the maximum, and NIST-recommended, GCM tag
 *       length -- weaker tag lengths trade authentication strength for a few saved bytes, which is
 *       never worth it for personal data.
 *   <li><b>A fresh, {@link SecureRandom} IV on every single encryption call</b>: GCM's
 *       confidentiality and authenticity guarantees both collapse if the same (key, IV) pair is ever
 *       reused -- this is the one hard rule of GCM usage, so IV generation is not optional,
 *       cacheable, or derivable from anything predictable here.
 * </ul>
 *
 * <p>Wire format for both {@link #encrypt} output and {@link #decrypt} input: the 12-byte IV,
 * immediately followed by the ciphertext with the 16-byte authentication tag appended (this is
 * {@link Cipher}'s own default GCM output layout -- {@code doFinal} appends the tag to the
 * ciphertext automatically, it is not handled separately by this class).
 */
final class AesGcm {

    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final String KEY_ALGORITHM = "AES";
    private static final int IV_LENGTH_BYTES = 12;
    private static final int TAG_LENGTH_BITS = 128;
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private AesGcm() {
    }

    /**
     * Encrypts {@code plaintext} with {@code key} under a freshly generated random IV. Returns
     * {@code iv || ciphertext+tag} concatenated, matching what {@link #decrypt} expects back.
     */
    static byte[] encrypt(byte[] key, byte[] plaintext) {
        byte[] iv = new byte[IV_LENGTH_BYTES];
        SECURE_RANDOM.nextBytes(iv);
        try {
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, KEY_ALGORITHM),
                    new GCMParameterSpec(TAG_LENGTH_BITS, iv));
            byte[] ciphertext = cipher.doFinal(plaintext);
            byte[] result = new byte[IV_LENGTH_BYTES + ciphertext.length];
            System.arraycopy(iv, 0, result, 0, IV_LENGTH_BYTES);
            System.arraycopy(ciphertext, 0, result, IV_LENGTH_BYTES, ciphertext.length);
            return result;
        } catch (GeneralSecurityException e) {
            // Never include plaintext/key material in an exception message -- see the module's
            // "never log plaintext personal data anywhere" rule.
            throw new IllegalStateException("AES-256-GCM encryption failed", e);
        }
    }

    /**
     * Decrypts {@code ivAndCiphertext} (the {@code iv || ciphertext+tag} format {@link #encrypt}
     * produces) with {@code key}. Throws if the authentication tag does not verify -- a wrong key,
     * a truncated/corrupted/tampered ciphertext, or (deliberately, by design) a key that has since
     * been erased and regenerated/replaced all surface as the same failure, since GCM's
     * authentication check cannot and must not distinguish "wrong key" from "tampered data".
     */
    static byte[] decrypt(byte[] key, byte[] ivAndCiphertext) {
        if (ivAndCiphertext.length < IV_LENGTH_BYTES) {
            throw new IllegalArgumentException("Ciphertext too short to contain a 12-byte GCM IV");
        }
        byte[] iv = Arrays.copyOfRange(ivAndCiphertext, 0, IV_LENGTH_BYTES);
        byte[] ciphertext = Arrays.copyOfRange(ivAndCiphertext, IV_LENGTH_BYTES, ivAndCiphertext.length);
        try {
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, KEY_ALGORITHM),
                    new GCMParameterSpec(TAG_LENGTH_BITS, iv));
            return cipher.doFinal(ciphertext);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("AES-256-GCM decryption failed (bad key or corrupted/tampered ciphertext)", e);
        }
    }
}
