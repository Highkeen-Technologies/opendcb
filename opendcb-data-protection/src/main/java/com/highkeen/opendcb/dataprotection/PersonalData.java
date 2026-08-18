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
 * Marks a field on an event payload class as personal data: {@link OpenDcbEncryptingConverter}
 * encrypts its value with the owning data subject's key (see {@link DataSubjectId @DataSubjectId})
 * before delegating serialization, and decrypts it back after delegating deserialization.
 *
 * <p><b>v1 limitation, deliberate:</b> only {@code String}-typed fields are supported. A ciphertext
 * (IV + AES-GCM output + auth tag) is fundamentally binary; representing it as a base64 string back
 * into the same field is straightforward for a {@code String} field but does not generalize cleanly
 * to numeric, boolean, or nested-object fields without a materially more complex design (a wrapper
 * type, a parallel ciphertext field, etc.). Rather than guess at that design prematurely,
 * {@code @PersonalData} is scoped to {@code String} fields only for now; {@link
 * OpenDcbEncryptingConverter} throws eagerly, at first-use of a payload class, if this annotation is
 * found on a non-{@code String} field, rather than silently skipping it.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface PersonalData {
}
