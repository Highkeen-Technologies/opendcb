# Protect and Erase Personal Data

A short, focused walkthrough of crypto-shredding: define an event with
personal data, append it, read it back decrypted, erase the subject, and
read it back again — using `opendcb-data-protection`'s real API, grounded in
its own `OpenDcbEncryptingConverterIntegrationTest`.

## Step 1: define the event payload

Mark the data subject's identifying field with `@DataSubjectId`, and every
field to encrypt with `@PersonalData`:

```java
record CustomerRegistered(
        @DataSubjectId String customerId,
        @PersonalData String email,
        @PersonalData String phoneNumber,
        String plan) {
}
```

`plan` isn't personal data, so it's never touched.

## Step 2: set up the key store and converter

```java
DataSource dataSource = new PGSimpleDataSource();
// ... configure dataSource ...

MasterKeyProvider masterKeyProvider = new EnvVarMasterKeyProvider(); // OPENDCB_DATA_PROTECTION_MASTER_KEY

OpenDcbKeyStore keyStore = new OpenDcbKeyStore(dataSource, masterKeyProvider);
keyStore.ensureSchema();

Converter converter = new OpenDcbEncryptingConverter(new JacksonConverter(), keyStore);
```

`ensureSchema()` creates `data_protection_key` and `data_protection_audit_log`
if they don't already exist — both independent of the event log's own
tables.

## Step 3: append — the personal data is encrypted before it's ever written

```java
CustomerRegistered original =
        new CustomerRegistered("cust-42", "alice@example.com", "+1-555-0100", "gold");

String json = converter.convert(original, String.class);
```

`json` is what actually gets written to the event log. Confirm the personal
data never appears in it:

```java
assert !json.contains("alice@example.com");
assert !json.contains("555-0100");
```

`converter` is a drop-in Axon `Converter` — in a real setup, this is exactly
what you hand to `AbstractDcbEventStorageEngine` (or wire in via
`bootstrap-axon-postgres`/the Spring Boot starter) instead of a plain
`JacksonConverter`, so this encryption happens transparently on every
append, not just when you call `convert` by hand as this tutorial does for
clarity.

## Step 4: read it back — decrypted

```java
CustomerRegistered decoded = converter.convert(json, CustomerRegistered.class);

assert decoded.equals(original);
assert decoded.email().equals("alice@example.com");
```

Decryption is transparent too — the reader doesn't need to know encryption
happened at all, as long as it's going through the same wrapped
`Converter`.

## Step 5: erase the data subject

```java
keyStore.eraseDataSubject("cust-42");
```

This destroys `cust-42`'s key — `wrapped_key` becomes `NULL` and
`erased_at` is set on that one row in `data_protection_key`. Nothing else
changes: not the event log, not any other subject's key, not the
`json` string from step 3, which still contains the exact same ciphertext
it always did.

## Step 6: read it back again — personal data is gone, everything else survives

```java
CustomerRegistered afterErasure = converter.convert(json, CustomerRegistered.class);

afterErasure.email();        // null
afterErasure.phoneNumber();  // null
afterErasure.customerId();   // "cust-42" — unchanged, it's not @PersonalData
afterErasure.plan();         // "gold" — unchanged, it's not @PersonalData either
```

This doesn't throw. That's deliberate: a real event replay of an erased
subject's stream must still succeed for every field other than the erased
personal data — a stream that throws on replay because one subject was
erased would be a much bigger problem than the erasure itself.

The ciphertext bytes in `json` are cryptographically, not just logically,
dead at this point: no other subject's key, and no freshly-generated random
key, can decrypt them either — AES-256-GCM's authentication tag is bound to
the exact key that produced a given ciphertext, and that key is gone.

## What's next

See
[`OpenDcbEncryptingConverterIntegrationTest`](https://github.com/Highkeen-Technologies/opendcb/blob/main/opendcb-data-protection/src/test/java/com/highkeen/opendcb/dataprotection/OpenDcbEncryptingConverterIntegrationTest.java)
for the complete, real, passing version of this flow against a real
PostgreSQL Testcontainers instance — including concurrent first-use key
creation, the audit log, and a non-record payload class. See
[Data Protection and Key Management](../module-guides/data-protection-and-key-management.md)
for choosing a `MasterKeyProvider` (env var vs. Vault vs. AWS KMS) and the
honest status of each.
