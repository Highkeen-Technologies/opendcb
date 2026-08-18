# Data Protection and Key Management

## The problem this solves

Event sourcing's core guarantee — an immutable, append-only log — conflicts
directly with "right to be forgotten" erasure requirements (GDPR, India's
DPDP Act 2023, RBI/BFSI expectations, PCI-DSS). You can't delete a
historical event without breaking replay. `opendcb-data-protection` solves
this with **crypto-shredding**: personal-data fields on your event payloads
are encrypted with a key tied to the data subject, that key is stored
*outside* the event log, and "erasing" a subject means destroying their key.
The log itself never changes — the data just becomes permanently
unrecoverable.

This is OpenDCB's own implementation, not Axon's Data Protection module —
that module is entirely paid (`io.axoniq.framework:axoniq-data-protection`)
and has no free surface to build against. See
[docs/ARCHITECTURE.md](https://github.com/Highkeen-Technologies/opendcb/blob/main/docs/ARCHITECTURE.md#opendcb-data-protection-crypto-shredding-for-erasure-not-a-hand-built-encryption-scheme)
for the full rationale, including why this doesn't imply any Axon SPI
compatibility.

## Step 1: mark your payload class

Two annotations, both field-targeted:

- `@DataSubjectId` — exactly one `String` field identifying whose data this
  is (a customer id, an account id).
- `@PersonalData` — any number of `String` fields to encrypt. (v1 only
  supports `String` fields; a ciphertext is fundamentally binary, and
  base64-into-a-`String` is the simplest representation that doesn't
  generalize badly.)

```java
record CustomerRegistered(
        @DataSubjectId String customerId,
        @PersonalData String email,
        @PersonalData String phoneNumber,
        String plan) {
}
```

Records work — `OpenDcbEncryptingConverter` reconstructs a fresh instance
through the canonical constructor, since record fields can't be mutated via
reflection. Plain mutable classes work too, via direct field mutation.

A class with `@PersonalData` fields but no `@DataSubjectId` field is a
configuration error, and fails fast the first time that class is
encountered — not a silent no-op.

## Step 2: wrap your Converter

`OpenDcbEncryptingConverter` decorates any other Axon `Converter` — normally
`JacksonConverter`, the same one `integrations/eventstore-axon` already
uses:

```java
OpenDcbKeyStore keyStore = new OpenDcbKeyStore(dataSource, masterKeyProvider);
keyStore.ensureSchema();

Converter converter = new OpenDcbEncryptingConverter(new JacksonConverter(), keyStore);
```

`OpenDcbKeyStore` owns two tables, independent of the event log's own
tables: `data_protection_key` (one row per data subject, holding that
subject's envelope-wrapped AES-256 key) and `data_protection_audit_log`
(append-only record of encrypt/decrypt/erase operations — never the
personal data itself). `ensureSchema()` creates both if they don't already
exist.

From here, `converter` is a drop-in `Converter` — hand it to
`AbstractDcbEventStorageEngine` the same way you'd hand it a plain
`JacksonConverter`. Every write of a `@PersonalData`-annotated payload gets
its personal-data fields encrypted before serialization; every read
decrypts them back, transparently.

```java
String json = converter.convert(original, String.class);
// json's "email" and "phoneNumber" fields are ciphertext, not plaintext

CustomerRegistered decoded = converter.convert(json, CustomerRegistered.class);
// decoded equals the original — decryption happened transparently
```

## Step 3: erase a data subject

```java
keyStore.eraseDataSubject(customerId);
```

This is the whole point. `eraseDataSubject` doesn't touch a single event —
it sets `wrapped_key = NULL` and `erased_at = now()` on that subject's one
row in `data_protection_key`. The event log is completely untouched: same
events, same ciphertext bytes, forever.

**What happens on the next replay.** When `OpenDcbEncryptingConverter`
deserializes an event for an erased subject, it looks up the subject's key,
finds none, and sets every `@PersonalData` field to `null` instead of
throwing — a real replay of an erased subject's stream must still succeed
for every field *other* than the erased personal data:

```java
CustomerRegistered decoded = converter.convert(json, CustomerRegistered.class);
decoded.email();       // null
decoded.phoneNumber();// null
decoded.customerId();  // unchanged — customerId itself isn't @PersonalData
decoded.plan();         // unchanged — "plan" was never annotated either
```

This is a genuine cryptographic guarantee, not a soft deletion: AES-256-GCM's
authentication tag means no key other than the exact one that encrypted a
given ciphertext can ever decrypt it, and that key is gone. It is *not* a
guarantee that the overwritten database bytes are physically unrecoverable
from the storage engine itself (dead tuples before `VACUUM`, WAL segments,
old backups) — a known, standard limitation of "erase by overwrite" in any
MVCC database, not specific to this implementation. Mitigate with routine
`VACUUM` hygiene, encrypting backups/WAL archives at rest, and a bounded
backup retention window.

Every encrypt, decrypt, and erase is recorded in `data_protection_audit_log`
— operation and subject id only, never the personal data itself.

## Choosing a MasterKeyProvider

Every per-subject key is itself protected by envelope encryption: a
pluggable `MasterKeyProvider` wraps (encrypts) each per-subject key before
`OpenDcbKeyStore` persists it, rather than storing per-subject keys in
plaintext. Three implementations exist today:

| Implementation | Module | When to use |
|---|---|---|
| `EnvVarMasterKeyProvider` | `opendcb-data-protection` (ships by default) | Local development, testing, low-stakes deployments. Reads a base64-encoded 256-bit key from an environment variable. |
| `VaultMasterKeyProvider` | `opendcb-data-protection-vault` | Self-hosted deployments — no cloud vendor dependency, philosophically consistent with `eventstore-postgres` over Axon Server. |
| `AwsKmsMasterKeyProvider` | `opendcb-data-protection-aws-kms` | Managed-cloud deployments — lower operational burden than self-hosting Vault; has a Mumbai (`ap-south-1`) region for RBI-style data-localization without cross-border key transfer. |

See
[docs/ARCHITECTURE.md's "Master key provider modules"](https://github.com/Highkeen-Technologies/opendcb/blob/main/docs/ARCHITECTURE.md#master-key-provider-modules-pluggable-separate-opt-in)
for why these are separate, optional modules rather than bundled
dependencies — the same reasoning as `eventstore-postgres`/`eventstore-mysql`
being separate modules. Both real-backend implementations are equally
"correct"; which to use is a deployment decision, not a capability
difference.

`EnvVarMasterKeyProvider` fails fast if its environment variable is missing,
not valid base64, or doesn't decode to exactly 32 bytes — there is no
fallback to a default or hardcoded key under any circumstance:

```java
MasterKeyProvider masterKeyProvider = new EnvVarMasterKeyProvider(); // reads OPENDCB_DATA_PROTECTION_MASTER_KEY
```

### Vault

```java
MasterKeyProvider masterKeyProvider = new VaultMasterKeyProvider(
        "https://vault.internal:8200",
        vaultToken,
        "opendcb-master-key");   // an existing Transit key name — this provider never creates one
```

`wrapKey` calls Transit's `encrypt` endpoint; `unwrapKey` calls `decrypt`.
Every Vault call failure — unreachable server, authentication failure — is
wrapped in a clear `IllegalStateException`. One exception to that fail-fast
discipline is a real security consideration worth understanding *before*
production: see
[FAQ and Troubleshooting](../faq-and-troubleshooting.md#why-did-vault-silently-create-a-new-transit-key-instead-of-failing)
for what happens when the configured Transit key name doesn't exist, and
the ACL policy that fixes it.

### AWS KMS

```java
KmsClient kmsClient = KmsClient.builder().region(Region.AP_SOUTH_1).build();
MasterKeyProvider masterKeyProvider = new AwsKmsMasterKeyProvider(kmsClient, cmkKeyIdOrArn);
```

`wrapKey`/`unwrapKey` call KMS's `Encrypt`/`Decrypt` operations against the
given customer master key (CMK) — deliberately not `GenerateDataKey`, which
mints a brand-new key server-side rather than wrapping a caller-supplied
one. This class never builds its own `KmsClient` — region, credentials, and
any endpoint override are entirely your concern, same as any other AWS SDK
v2 usage.

## Honest status

All three data-protection modules are verified against real
infrastructure, no mocking of crypto or the database:

- `opendcb-data-protection` and `opendcb-data-protection-vault` run against
  PostgreSQL and HashiCorp Vault, respectively, both via Testcontainers —
  round trips, concurrent first-use key creation, erasure, and (for Vault)
  unreachable-server and missing-key failure modes all actually run and
  pass.
- `opendcb-data-protection-aws-kms` runs `AwsKmsMasterKeyProviderTest`
  unconditionally against a real community-edition LocalStack (Testcontainers,
  `localstack/localstack:4.9`) with KMS enabled — a genuine
  `CreateKey`/`Encrypt`/`Decrypt` round trip, plus clear-failure tests for a
  nonexistent CMK and garbage ciphertext. No `LOCALSTACK_AUTH_TOKEN` or
  LocalStack account is required: that pinned image tag (built 2025-10-06)
  predates LocalStack's 2026-03-23 unified-image auth-token requirement, and
  community/free-tier LocalStack has always emulated KMS's symmetric
  `CreateKey`/`Encrypt`/`Decrypt` operations, which is all this provider
  uses. An earlier `@EnabledIfEnvironmentVariable(LOCALSTACK_AUTH_TOKEN)`
  gate on this test was removed after empirically confirming — via both a
  manual `docker run` + `aws kms` round trip and the real JUnit suite — that
  it was based on an overgeneralized reading of LocalStack's policy change,
  not on anything actually tested against this repo's specific pinned tag.

## What's next

See
[Protect and Erase Personal Data](../tutorials/protect-and-erase-personal-data.md)
for a full step-by-step walkthrough, and
[FAQ and Troubleshooting](../faq-and-troubleshooting.md#why-did-vault-silently-create-a-new-transit-key-instead-of-failing)
for the Vault auto-create-key security consideration.
