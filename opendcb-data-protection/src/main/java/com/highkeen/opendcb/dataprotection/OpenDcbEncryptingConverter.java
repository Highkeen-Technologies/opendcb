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

import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.conversion.Converter;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link Converter} decorator implementing OpenDCB's own crypto-shredding solution for
 * "right to be forgotten" erasure -- <b>not</b> an implementation of Axon's separately-licensed,
 * paid {@code axoniq-data-protection} module, which does not exist anywhere in the free {@code
 * org.axonframework} tree. This is the one class in {@code opendcb-data-protection} that depends on
 * {@code org.axonframework} (for the real {@link Converter} SPI it wraps), matching this toolkit's
 * usual pattern of confining framework coupling to a single, clearly-documented adapter.
 *
 * <p>Wraps a delegate {@link Converter} (e.g. Axon's own {@code JacksonConverter}) and adds one
 * responsibility on top of it: transparently encrypting/decrypting {@link
 * PersonalData @PersonalData}-annotated {@code String} fields on an event payload, immediately
 * before/after delegating the actual JSON (de)serialization.
 *
 * <h2>Serialize path (payload object &rarr; JSON string)</h2>
 *
 * Before delegating, reflectively locates the payload's {@link DataSubjectId @DataSubjectId} field
 * and every {@link PersonalData @PersonalData} field, calls {@link
 * OpenDcbKeyStore#getOrCreateDataKey} for the subject, encrypts each personal-data value with
 * AES-256-GCM (via {@link AesGcm}), base64-encodes the result back into that same field, records one
 * {@link AuditOperation#ENCRYPT} audit entry, then delegates to the wrapped converter.
 *
 * <h2>Deserialize path (JSON string &rarr; payload object)</h2>
 *
 * Delegates first, then reflectively finds the same fields on the resulting object and decrypts
 * each personal-data value using {@link OpenDcbKeyStore#getDataKeyIfNotErased}. If the subject's key
 * is present, decryption succeeds and an {@link AuditOperation#DECRYPT} entry is recorded. If the
 * key is absent -- the subject was erased, or a key was simply never created for it -- the field is
 * set to {@code null} and an {@link AuditOperation#DECRYPT_AFTER_ERASURE} entry is recorded instead;
 * this method never throws in that case, since a real event replay of an erased subject's stream
 * must still succeed for every field <em>other</em> than the erased personal data.
 *
 * <h2>Record payloads: reconstruction, not in-place mutation</h2>
 *
 * Every payload type in this toolkit's own examples is a Java {@code record}, and record fields
 * cannot be mutated via reflection even with {@code setAccessible(true)} -- {@code Field.set()} on a
 * record's backing field throws {@link IllegalAccessException} unconditionally (JEP 359's
 * immutability guarantee, confirmed empirically against JDK 21, not assumed). This class handles
 * that by branching on {@link Class#isRecord()}: for a record, it builds a fresh instance via the
 * canonical constructor, substituting encrypted/decrypted values for the affected components while
 * copying every other component through unchanged (record component declaration order is guaranteed
 * by the JLS to match the canonical constructor's parameter order, so {@link
 * Class#getRecordComponents()} can be used directly to build the constructor argument list); for an
 * ordinary mutable class, it falls back to direct {@code Field.set()}, which does work for
 * non-record final fields. {@link DataSubjectId @DataSubjectId} and {@link
 * PersonalData @PersonalData} both use {@code @Target(ElementType.FIELD)}, and a field-targeted
 * annotation on a record component does propagate to the component's backing field (also confirmed
 * empirically), so annotation discovery via {@code getDeclaredField(...).isAnnotationPresent(...)}
 * works identically for both record and non-record payload classes -- only the write-back mechanism
 * differs.
 */
public class OpenDcbEncryptingConverter implements Converter {

    private final Converter delegate;
    private final OpenDcbKeyStore keyStore;
    private final Map<Class<?>, ClassMetadata> metadataCache = new ConcurrentHashMap<>();

    public OpenDcbEncryptingConverter(Converter delegate, OpenDcbKeyStore keyStore) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        this.keyStore = Objects.requireNonNull(keyStore, "keyStore must not be null");
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T convert(Object input, Type targetType) {
        if (input != null && targetType == String.class && !(input instanceof String)
                && hasPersonalData(input.getClass())) {
            Object transformed = encryptPersonalDataFields(input);
            return delegate.convert(transformed, targetType);
        }
        if (input instanceof String json && targetType instanceof Class<?> payloadType
                && hasPersonalData(payloadType)) {
            Object deserialized = delegate.convert(json, targetType);
            return (T) decryptPersonalDataFields(deserialized, payloadType);
        }
        return delegate.convert(input, targetType);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("delegate", delegate.getClass().getName());
        descriptor.describeProperty("keyStore", keyStore.getClass().getName());
    }

    private boolean hasPersonalData(Class<?> type) {
        return !metadataFor(type).personalDataFields().isEmpty();
    }

    private ClassMetadata metadataFor(Class<?> type) {
        return metadataCache.computeIfAbsent(type, OpenDcbEncryptingConverter::buildMetadata);
    }

    private static ClassMetadata buildMetadata(Class<?> type) {
        Field dataSubjectIdField = null;
        List<Field> personalDataFields = new ArrayList<>();
        for (Field field : type.getDeclaredFields()) {
            if (field.isAnnotationPresent(DataSubjectId.class)) {
                if (dataSubjectIdField != null) {
                    throw new IllegalStateException(
                            "Class " + type.getName() + " has more than one @DataSubjectId field; "
                                    + "exactly one is required.");
                }
                if (field.getType() != String.class) {
                    throw new IllegalArgumentException(
                            "@DataSubjectId field '" + field.getName() + "' on " + type.getName()
                                    + " must be of type String.");
                }
                field.setAccessible(true);
                dataSubjectIdField = field;
            }
            if (field.isAnnotationPresent(PersonalData.class)) {
                if (field.getType() != String.class) {
                    throw new IllegalArgumentException(
                            "@PersonalData field '" + field.getName() + "' on " + type.getName()
                                    + " is of type " + field.getType().getName()
                                    + "; only String fields are supported in v1.");
                }
                field.setAccessible(true);
                personalDataFields.add(field);
            }
        }
        if (!personalDataFields.isEmpty() && dataSubjectIdField == null) {
            throw new IllegalStateException(
                    "Class " + type.getName() + " has @PersonalData field(s) but no @DataSubjectId "
                            + "field; annotate the data subject identifier field with @DataSubjectId.");
        }
        return new ClassMetadata(dataSubjectIdField, List.copyOf(personalDataFields));
    }

    private Object encryptPersonalDataFields(Object payload) {
        Class<?> type = payload.getClass();
        ClassMetadata metadata = metadataFor(type);
        String dataSubjectId = readStringField(metadata.dataSubjectIdField(), payload);
        byte[] dataKey = keyStore.getOrCreateDataKey(dataSubjectId);
        Map<String, Object> overrides = new HashMap<>();
        for (Field field : metadata.personalDataFields()) {
            String plaintext = readStringField(field, payload);
            if (plaintext != null) {
                byte[] ciphertext = AesGcm.encrypt(dataKey, plaintext.getBytes(StandardCharsets.UTF_8));
                overrides.put(field.getName(), Base64.getEncoder().encodeToString(ciphertext));
            }
        }
        keyStore.recordAudit(AuditOperation.ENCRYPT, dataSubjectId);
        return rebuildWithOverrides(payload, type, metadata.personalDataFields(), overrides);
    }

    private Object decryptPersonalDataFields(Object deserialized, Class<?> type) {
        if (deserialized == null) {
            return null;
        }
        ClassMetadata metadata = metadataFor(type);
        String dataSubjectId = readStringField(metadata.dataSubjectIdField(), deserialized);
        Optional<byte[]> dataKey = keyStore.getDataKeyIfNotErased(dataSubjectId);
        Map<String, Object> overrides = new HashMap<>();
        if (dataKey.isPresent()) {
            byte[] key = dataKey.get();
            for (Field field : metadata.personalDataFields()) {
                String ciphertextB64 = readStringField(field, deserialized);
                if (ciphertextB64 == null) {
                    overrides.put(field.getName(), null);
                } else {
                    byte[] ciphertext = Base64.getDecoder().decode(ciphertextB64);
                    byte[] plaintext = AesGcm.decrypt(key, ciphertext);
                    overrides.put(field.getName(), new String(plaintext, StandardCharsets.UTF_8));
                }
            }
            keyStore.recordAudit(AuditOperation.DECRYPT, dataSubjectId);
        } else {
            for (Field field : metadata.personalDataFields()) {
                overrides.put(field.getName(), null);
            }
            keyStore.recordAudit(AuditOperation.DECRYPT_AFTER_ERASURE, dataSubjectId);
        }
        return rebuildWithOverrides(deserialized, type, metadata.personalDataFields(), overrides);
    }

    private static String readStringField(Field field, Object instance) {
        try {
            return (String) field.get(instance);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(
                    "Failed to read field '" + field.getName() + "' on " + instance.getClass().getName(), e);
        }
    }

    private static Object rebuildWithOverrides(Object instance, Class<?> type, List<Field> personalDataFields,
                                                Map<String, Object> overrides) {
        if (overrides.isEmpty()) {
            return instance;
        }
        if (type.isRecord()) {
            return rebuildRecord(instance, type, overrides);
        }
        return mutateInPlace(instance, personalDataFields, overrides);
    }

    private static Object rebuildRecord(Object instance, Class<?> type, Map<String, Object> overrides) {
        RecordComponent[] components = type.getRecordComponents();
        Class<?>[] paramTypes = new Class<?>[components.length];
        Object[] args = new Object[components.length];
        try {
            for (int i = 0; i < components.length; i++) {
                RecordComponent component = components[i];
                paramTypes[i] = component.getType();
                args[i] = overrides.containsKey(component.getName())
                        ? overrides.get(component.getName())
                        : component.getAccessor().invoke(instance);
            }
            Constructor<?> canonicalConstructor = type.getDeclaredConstructor(paramTypes);
            canonicalConstructor.setAccessible(true);
            return canonicalConstructor.newInstance(args);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "Failed to reconstruct record " + type.getName()
                            + " with encrypted/decrypted personal data field value(s)", e);
        }
    }

    private static Object mutateInPlace(Object instance, List<Field> personalDataFields,
                                         Map<String, Object> overrides) {
        try {
            for (Field field : personalDataFields) {
                if (overrides.containsKey(field.getName())) {
                    field.set(instance, overrides.get(field.getName()));
                }
            }
            return instance;
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(
                    "Failed to set encrypted/decrypted personal data field value(s) on "
                            + instance.getClass().getName(), e);
        }
    }

    private record ClassMetadata(Field dataSubjectIdField, List<Field> personalDataFields) {
    }
}
