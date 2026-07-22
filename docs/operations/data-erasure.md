# Data erasure (GDPR right-to-erasure)

↑ [operations](README.md)

Mycelium cannot *physically* delete a data subject's bytes from every node, WAL, snapshot, and
backup within a request SLA — a gossip+WAL mesh has tombstone windows, anti-entropy resurrection,
and replicated persistence (design: [data-lifecycle-and-erasure](../design/data-lifecycle-and-erasure.md)).
The honest, standards-recognised mechanism is **crypto-shredding**: encrypt each subject's personal
data under a **per-subject key (DEK)**; erase = **destroy the DEK**. Every ciphertext copy becomes
cryptographically dead the instant the key is gone — an O(1) key-destroy, not a byte-hunt.

## The reference helper — `SubjectKeyRegistry`

```rust
use mycelium::SubjectKeyRegistry;   // `tls` feature (AES-256-GCM via ring)

let reg = SubjectKeyRegistry::new();

// Envelope-encrypt PII before storing it (in KV, a DB, anywhere):
let blob = reg.encrypt_for("user-42", b"name, email, ...");
agent.kv().set("pii/user-42", blob.into());

// Read back:
let blob = agent.kv().get("pii/user-42").unwrap();
let pii = reg.decrypt_for("user-42", &blob);      // Some(plaintext)

// Erase (GDPR Art. 17): destroy the DEK — all ciphertext for the subject is now undecryptable.
reg.destroy("user-42");
assert!(reg.decrypt_for("user-42", &blob).is_none());
```

- `encrypt_for` mints the DEK on first use; the blob is `nonce ‖ ciphertext ‖ tag` (AEAD —
  tampering is rejected on decrypt).
- `destroy` zeroizes and drops the DEK. Re-encrypting after erasure mints a **new** DEK; old
  ciphertext never revives.
- `install_key` is the seam for **KMS-backed custody** (below).

## Production custody — use a KMS

The reference holds DEKs in memory (lost on restart). For a durable, provably-destroyable store,
back each DEK with a **KMS/HSM**: generate/wrap the DEK there, unwrap into the registry via
`install_key`, and make `destroy` a **KMS delete-key**. The KMS enforces that destruction survives
restarts and reaches DEK backups — which in-memory custody cannot. This is the recommended path for
regulated deployments; the registry is the envelope layer above it.

Composes with the [`DataAtRestCipher`](crown-jewel.md) disk-boundary cipher (WS3): crypto-shred is
the *per-subject* layer above the KV value; the two are defence-in-depth.

## Honest limits (state these in your DPIA / privacy notice)

- **Plaintext escapes are not erased.** PII placed in the **audit trail** (`detail`), application
  **logs**, or metric **labels** is outside the envelope — DEK destruction does nothing for it.
  Rule: keep subject PII in envelope-encrypted values only; never in audit detail, logs, or labels.
- **A backup holding both ciphertext and the DEK defeats erasure.** Custody must guarantee DEK
  destruction reaches DEK backups (a KMS enforces this; in-memory/local custody requires a
  disciplined backup policy).
- **Pre-adoption plaintext** (data written before you adopted envelope encryption) is not covered —
  migrate (re-encrypt) or document as out of scope.
- **Erasure latency** is the registry/KMS operation, not the mesh GC window: the DEK dies
  immediately; lingering ciphertext is already cryptographically dead.

## Data residency

Residency is a **deployment** property, not code: a Mycelium cluster is isolated by construction
(no cross-cluster data flow), so residency = **node placement**. Run one cluster per
region/jurisdiction; nothing gossips across clusters.

## Shared-responsibility note

Erasure is **Shared**: Mycelium provides the crypto-shred helper + this runbook; you own DEK custody
(KMS), residency (node placement), a backup policy that honours destruction, and keeping PII out of
plaintext channels. See the [shared-responsibility matrix](shared-responsibility-matrix.md).
