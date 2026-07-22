# 2026-07-22 — identity-auth Phase 2 shipped (signed proofs → prevention)

SOC 2 WS-E. Closes the *prevention* half of the `sys/identity` poisoning vector for any
connected/established peer (1b was detection only). Design: `docs/design/identity-authentication.md`
§4 Phase 2.

**Mechanism.** A sibling KV key `sys/identity-proof/{V}` = `signer_key(32)‖sig(64)` over the
`sys/identity/{V}` history bytes (chosen over an in-band trailer because the old reader's
`parse_identity_keys` requires `len % 32 == 0` — a 96-byte trailer would break it; the sibling key
is ignored by old readers, so it's fully rollout-compatible). Deliberately **not** a `sys/identity/`
sub-prefix, so the `IDENTITY` prefix scan/watcher never sees proof keys.

- **Publish (self):** `helpers::sign_identity_proof` signs the history with the node's current key,
  written at startup (self-signed → TOFU/anchor) and at rotation **before cutover** (so it signs
  with the *prior* key → peers that trust the prior key chain trust to the new one).
- **Merge (peer):** `helpers::validate_and_merge_identity` replaces the raw `merge_peer_keys` at both
  sites (prewarm + watcher, each now reads the sibling proof). Rule: proof present + signer already
  trusted for V (anchor/prior) + sig valid → **accept**; V unknown → **TOFU** (self-signed first
  entry); proof signed by an **untrusted** key or bad sig → **reject** + count; no proof → rollout
  tolerance (accept + the 1b tripwire — Phase 3 tightens this).

**Why legitimate flow is never rejected (verified against consensus reads):** a legitimate node's
proof is signed by its current/prior key, which peers trust via the CA anchor or the prior entry; a
proof that hasn't gossiped in yet leaves the no-proof path (accept). So only untrusted-signed
overwrites are rejected — `known_verifying_keys` / `consensus::decode_verify` see an unchanged key
set for honest nodes. Full lib suite (incl. consensus, prop) + ws5 rotation stayed green.

**Gates:** `test_identity_proof_rejects_poisoning_accepts_signed` (untrusted-signed overwrite
rejected; prior-key-signed rotation accepted — the two design litmus tests, driven directly against
the merge helper); the 1b end-to-end test now exercises the rejection path (a stale-proof overwrite
fails the sig check → rejected). No wire change.

**Remaining:** Phase 3 (reject *unsigned* entries entirely — closes the "mimic an old node" rollout
residual) is **wire-v13-gated**, a deliberate two-release migration: R1 = write proofs + accept-both
(now shipped), R2 = require proofs.
