# Data Protection and Key Management

## The problem this solves

Event sourcing's core guarantee — an immutable, append-only log — conflicts
directly with "right to be forgotten" erasure requirements (GDPR, India's
DPDP Act 2023, RBI/BFSI expectations, PCI-DSS). You can't delete a
historical event without breaking replay. `opendcb-data-protection` solves
this with crypto-shredding: personal-data fields on your event payloads are
encrypted with a key tied to the data subject, that key is stored *outside*
the event log, and "erasing" a subject means destroying their key. The log
itself never changes — the data just becomes permanently unrecoverable.

Every per-subject key is itself protected by envelope encryption: a
pluggable `MasterKeyProvider` wraps (encrypts) each per-subject key before
it's stored, rather than storing per-subject keys in plaintext. Two
optional, real backends implement this interface — `opendcb-data-protection-vault`
(HashiCorp Vault's Transit secrets engine) and `opendcb-data-protection-aws-kms`
(AWS KMS). Neither is required; `opendcb-data-protection` ships a simpler
`EnvVarMasterKeyProvider` by default for development and smaller
deployments.

## Using the Vault-backed provider

```java
MasterKeyProvider masterKeyProvider = new VaultMasterKeyProvider(
        "https://vault.internal:8200",
        vaultToken,
        "opendcb-master-key");   // the Transit key name
```

`wrapKey` calls Transit's `encrypt` endpoint; `unwrapKey` calls `decrypt`.
Every Vault call failure — unreachable server, authentication failure — is
wrapped in a clear `IllegalStateException` rather than silently falling
back to weaker behavior. There is one important exception to that
fail-fast discipline, covered next, and it's a real security consideration
you should address **before** taking this to production.

## Security consideration: Vault auto-creates a missing Transit key on encrypt

This is genuine, documented HashiCorp Vault Transit behavior — not an
OpenDCB bug, and not something `VaultMasterKeyProvider` can opt out of on
its own. Vault's Transit `encrypt` endpoint auto-creates ("auto-vivifies")
the named key on first use if it doesn't already exist, rather than
rejecting the request.

That matters because of what it means for a **misconfigured Transit key
name**. If your application config has a typo — `opendcb-master-key` meant,
`opendcb-mastr-key` actually deployed — you would reasonably expect
`wrapKey` to fail loudly, the same way it fails loudly when Vault is
unreachable. It doesn't. Vault silently mints a brand-new key under the
typo'd name and happily encrypts under it. The data-subject key gets
wrapped, `wrapKey` returns normally, and everything *looks* correct — but
you now have per-subject keys wrapped under a key nobody intended to create,
separate from whatever key rotation/backup/access-review process governs
your real `opendcb-master-key`. This only surfaces as a real, catchable
failure later, on the `unwrapKey`/`decrypt` path, if that path ever targets
the *correct* key name and finds nothing there to unwrap what was actually
wrapped elsewhere — by which point you may have production ciphertext
scattered across an unintended key.

**Recommended mitigation: restrict the Vault token's ACL policy so it
cannot create Transit keys, only use an existing one.** Grant `update` on
the `encrypt`/`decrypt` paths for your specific key, and explicitly deny
`create` on `transit/keys/*`:

```hcl
# opendcb-master-key-policy.hcl
path "transit/encrypt/opendcb-master-key" {
  capabilities = ["update"]
}

path "transit/decrypt/opendcb-master-key" {
  capabilities = ["update"]
}

path "transit/keys/*" {
  capabilities = ["deny"]
}
```

With this policy in place, a typo'd or otherwise wrong key name in your
application config fails immediately and loudly on the very first
`wrapKey` call — Vault rejects the implicit key-creation attempt instead of
silently succeeding against a key nobody intended to create — which is the
behavior most teams actually want from a security-critical dependency.
Apply this policy as part of provisioning the Vault token OpenDCB uses,
before the application ever runs against it in production.

## What's next

See
[FAQ and Troubleshooting](../faq-and-troubleshooting.md#why-did-vault-silently-create-a-new-transit-key-instead-of-failing)
for a quick answer if you've already hit this.
