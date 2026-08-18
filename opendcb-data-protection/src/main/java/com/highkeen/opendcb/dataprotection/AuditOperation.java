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
 * The {@code operation} values recorded in {@code data_protection_audit_log}. Every audit entry
 * records that an operation of one of these kinds happened for a given data subject, and when --
 * never the personal data itself.
 */
public enum AuditOperation {

    /** A {@link PersonalData @PersonalData} field was encrypted (converter serialize path). */
    ENCRYPT,

    /**
     * A {@link PersonalData @PersonalData} field was successfully decrypted (converter deserialize
     * path, subject not erased).
     */
    DECRYPT,

    /** {@link OpenDcbKeyStore#eraseDataSubject} was called for a data subject. */
    ERASE,

    /**
     * A {@link PersonalData @PersonalData} field could not be decrypted because the subject's key
     * has been erased (or never existed) -- the converter set the field to {@code null} instead of
     * throwing.
     */
    DECRYPT_AFTER_ERASURE
}
