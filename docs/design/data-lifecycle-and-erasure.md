# Data lifecycle & erasure (GDPR / right-to-erasure) — design

**Status: DESIGN (2026-07-22).** SOC 2 audit-gap **WS-F** — see
[soc2-audit-gap-closure](../plans/soc2-audit-gap-closure.md). Not SOC 2 core, but every
enterprise/privacy review asks; genuinely absent today. This design lands the mechanism + the honest
limits; a reference helper follows.

## The problem

GDPR Art. 17 (erasure), Art. 5(1)(e) (storage limitation), Art. 44+ (residency) require an operator
to **delete a data subject's personal data on request** and to **control where it lives**. In a
naïve store this is `DELETE`. Mycelium is not a naïve store.

## Why physical deletion is not guaranteeable here

Mycelium is a gossip mesh with LWW+HLC KV. A "delete" is a **tombstone**, and:

- tombstones have a **retention window** (bounded, then GC'd) — during it the value's absence is
  gossiped, not its bytes' destruction;
- **anti-entropy can resurrect** a value if any partition/late node still holds it and the tombstone
  hasn't reached it — deletion is eventually-consistent, not atomic;
- the **WAL and snapshots** persist bytes to disk independent of the live store;
- the value is **replicated to N nodes** and may sit in each one's WAL/snapshot/backup.

So "guarantee every byte is gone from every node, WAL, snapshot, and backup within the request SLA"
is not a property this substrate can honestly offer. Promising it would be the same category error as
promising the mesh is BFT.

## The mechanism: crypto-shredding (crypto-erasure)

The NIST/ISO-recognised answer for distributed/immutable stores: **don't chase the bytes — destroy
the key.** Encrypt each subject's personal data under a **per-subject data-encryption key (DEK)**;
"erase subject S" = **destroy S's DEK**. Every ciphertext copy — live KV, WAL, snapshot, replica,
even an old backup — becomes cryptographically unrecoverable the instant the DEK is gone, without
touching a single distributed byte. Erasure becomes an **O(1) key-destroy**, not an unbounded
byte-hunt.

## Design

```text
                per-subject DEK (AES-256-GCM)
 app write:  plaintext PII --encrypt(DEK_S)--> ciphertext --> kv.set("subject/S/...", ct)
 app read:   kv.get(...) --> ciphertext --decrypt(DEK_S)--> plaintext
 erase(S):   registry.destroy(S)  ⇒ DEK_S gone ⇒ all ciphertext for S is dead
                                     (+ best-effort tombstone the KV keys for hygiene)
```

- **`SubjectKeyRegistry`** — maps subject-id → wrapped DEK. A reference helper provides
  `encrypt_for(subject, plaintext)`, `decrypt_for(subject, ciphertext)`, `destroy(subject)`.
- **DEK custody is the crown jewel and must itself be erasable.** Options, strongest first:
  1. **KMS-managed DEKs** — each DEK wrapped by (or generated in) a KMS; `destroy` = KMS delete-key /
     delete the wrapped DEK. The KMS enforces deletion and its own backup policy — the recommended
     production path (composes with the deployer's existing key custody, see
     [crown-jewel](../operations/crown-jewel.md)).
  2. **Local secure store** for wrapped DEKs, wrapped by a master key held in the existing
     `DataAtRestCipher` custody. `destroy` = delete the wrapped DEK + ensure the deletion is included
     in the DEK-store backup policy.
- **Composition with existing hooks.** `DataAtRestCipher` (WS3) encrypts the *disk* boundary
  (WAL/snapshot) under one node key — it does **not** give per-subject erasure. Crypto-shred is a
  **per-subject layer above the KV value**; the two compose (defence in depth + erasability).
- **Best-effort physical cleanup.** On `destroy`, also tombstone the subject's KV keys so the
  ciphertext evaporates via the normal GC window — but correctness rests on DEK destruction, not on
  the tombstone reaching every node.
- **Residency** is a *deployment* property, not code: a Mycelium cluster is **isolated by
  construction** (no cross-cluster data flow), so residency = **node placement** = deployer-owned.
  Run one cluster per region/jurisdiction; document the boundary. Nothing gossips across clusters.

## Honest limits (these go in the operator doc verbatim)

- **Plaintext escapes are not erased.** PII placed in the **audit trail** (`detail`), application
  **logs**, metrics labels, or any channel outside the crypto-shred envelope is **not** covered — a
  DEK destroy does nothing for it. Rule: never put subject PII in audit `detail`, logs, or labels;
  keep it in envelope-encrypted KV values only.
- **A backup that also holds the DEK defeats erasure.** If a backup captured both the ciphertext and
  the DEK before destruction, restoring it restores the data. Custody therefore must guarantee DEK
  destruction propagates to DEK backups (option 1 KMS enforces this; option 2 requires a disciplined
  DEK-store backup policy).
- **Pre-adoption plaintext isn't covered** — data written to KV in the clear before the app adopted
  envelope encryption. Migration = re-encrypt or accept as out-of-scope, documented.
- **Erasure latency:** DEK destruction is immediate; lingering ciphertext bytes GC on the tombstone
  window but are already cryptographically dead, so the *effective* erasure SLA is the registry
  operation, not the mesh's GC window.

## Shared-responsibility split

- **Mycelium provides:** the crypto-shred reference helper (`SubjectKeyRegistry` + envelope
  encrypt/decrypt/destroy), the pattern, and this limits doc. Flips the erasure matrix cell from
  *absent* → *Mycelium-provides (crypto-shred) + Deployer-owns*.
- **Deployer owns:** DEK custody (KMS), residency (node placement), backup policy that honours
  destruction, and keeping PII out of plaintext channels.

## Next

Reference helper implementation (after this design is accepted), + `docs/operations/data-erasure.md`
operator runbook, + the erasure row in the
[shared-responsibility-matrix](../operations/shared-responsibility-matrix.md) updated from ⚠ to the
crypto-shred cell.
