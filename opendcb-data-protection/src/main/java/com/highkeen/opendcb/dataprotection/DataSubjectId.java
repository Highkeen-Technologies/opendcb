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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks the field on an event payload class that identifies the data subject (e.g. a customer or
 * account id) whose {@link PersonalData @PersonalData} fields on that same payload are encrypted
 * and decrypted against. {@link OpenDcbEncryptingConverter} looks this field up reflectively to
 * derive which per-subject data-encryption key ({@link OpenDcbKeyStore}) applies.
 *
 * <p>Must be a {@code String} field: {@code data_protection_key.data_subject_id} is a {@code TEXT}
 * primary key, so the value is used as-is as a lookup key, not converted via {@code toString()}.
 *
 * <p>Exactly one field per payload class should carry this annotation whenever that class also has
 * one or more {@link PersonalData @PersonalData} fields; a class with {@code @PersonalData} fields
 * but no {@code @DataSubjectId} field is a configuration error that {@link OpenDcbEncryptingConverter}
 * rejects eagerly (fail fast, not a silent no-op) the first time that class is encountered.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface DataSubjectId {
}
