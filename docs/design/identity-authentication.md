# Authenticating `sys/identity` — closing the key-poisoning vector

**Status:** Design (2026-07-15). Not yet implemented. Tracks the residual from the
2026-07-15 audit **pass 3** (`docs/analysis/ratings.md`, Calibration Ledger — the
identity-poisoning finding that showed the pass-2 `signer_authorized` bind over-claimed
insider-resistance). This is a **security trust-root** change: it must be reviewed and
landed as a phased workstream, not ad-hoc. This document specifies the fix precisely so it
can be.

## 0. Current state (verified against code, 2026-07-15)

The verifying keys a node trusts for its peers live in `TaskCtx::peer_keys`
(`papaya::HashMap<NodeId, Vec<[u8;32]>>`). They are populated **only** from unauthenticated
Layer-I KV gossip:

- A node publishes its own key history to `sys/identity/{self}` via
  `helpers::encode_identity_history(current, existing)` — a raw concatenation of 32-byte
  keys, **current first, then all retained priors** (WS5 multi-key archival, so historical
  signatures stay verifiable). No signature, no version byte. (`lifecycle.rs:154`, rotation
  at `mod.rs:1030`.)
- Every node mirrors **all** `sys/identity/{node}` entries into `peer_keys` via
  `helpers::parse_identity_keys` + `helpers::merge_peer_keys` — at startup
  (`lifecycle.rs::prewarm_peer_keys`) and on every `sys/identity/` change
  (`lifecycle.rs::start_identity_watcher`).
- `merge_peer_keys` **accumulates** — it unions new keys into the retained set and **never
  drops one** (deliberate: retained keys keep old signatures verifiable across rotations).
- `consensus::decode_verify` verifies a signed consensus message against
  `peer_keys[signer]` (falling back to a fresh `parse_identity_keys` of `sys/identity/{signer}`),
  then binds the vote/propose identity to the signer (`signer_authorized`, pass 2).

**The mTLS handshake validates the peer's CA-signed cert (admission) but never harvests the
Ed25519 key embedded in that cert into `peer_keys`.** So there is, today, **no authenticated
identity source anywhere** — `peer_keys` is entirely TOFU-via-KV-with-accumulate.

## 1. The problem

`sys/identity/{V}` is a plain KV key under the **detection-not-prevention** model: any
admitted node can LWW-overwrite it. Combined with accumulate-forever:

> A compromised admitted node M writes `sys/identity/{V} = V_real ‖ kM_pub`. Every node's
> watcher unions `kM_pub` into `peer_keys[V]` **permanently**. M then signs consensus
> `Vote{voter: V}` messages with `kM_priv`; `decode_verify` finds `kM_pub ∈ peer_keys[V]`,
> the signature verifies, `signer_authorized(voter == signer == V)` passes → **a forged vote
> from V is accepted.** Repeat across N victims → a forged quorum with zero real agreement.

This defeats the pass-2 `signer_authorized` bind: that bind is only as strong as the
`signer → key` map it verifies against, and the map is unauthenticated + append-only.

**Because legitimate rotation and poisoning are byte-identical (both just "a new key appeared
in `sys/identity/{V}`"), no *detection* tripwire can distinguish them without an authenticated
anchor.** Prevention requires authentication; it cannot be bolted on as a counter.

## 2. Threat model — and why close it anyway

Mycelium is **CFT, not BFT** (`framing.rs` signing doc, `philosophy.md`). A compromised
*admitted* node is a Byzantine actor, formally **out of scope**. So this attack is, strictly,
outside the guarantee.

We close it anyway, scoped honestly, because:

1. The `SignedConsensusMsg` layer **exists specifically** to add insider-resistance as
   defense-in-depth *beyond* CFT (its own doc says so). A bind that an insider trivially
   bypasses via a sibling KV write is a weak layer; the audit already corrected the doc to
   stop over-claiming (pass 3), but the mechanism is worth hardening to match its intent.
2. The vector triggers on a **buggy** node too, not only a malicious one: a node that writes a
   wrong `sys/identity` entry (bad rotation, corruption) permanently poisons every peer's key
   set for the victim. That is a robustness problem inside the CFT model.
3. The fix's foundation — an authenticated identity source — is independently valuable
   (removes the startup TOFU race for the common directly-connected case).

**Non-goal:** full BFT consensus. We are authenticating *identity*, not making the protocol
Byzantine-safe end to end (a fully compromised node can still refuse to participate, equivocate
at the transport it controls, etc.). The commit path stays without a quorum certificate.

## 3. Invariants to preserve

- **Retained keys stay verifiable.** Rotation keeps the full history; old signatures must keep
  validating (`merge_peer_keys` accumulate semantics). The fix authenticates *how a key enters*
  the set; it does not drop retained keys.
- **No new blocking / no lock across await** on the hot verify path.
- **Backward compatibility during rollout.** A mixed cluster (old raw-format writers + new
  signed writers) must not partition: old nodes must still read new entries as valid keys, and
  new nodes must still learn old nodes' keys — up to the point the migration deliberately
  tightens (Phase 3).
- **CFT-not-BFT framing intact.** No doc may claim this makes consensus Byzantine-safe.

## 4. The design — three phases

The trust root is the **CA**. Every admitted node holds a CA-signed cert binding its `NodeId`
to its Ed25519 key. That binding is the authenticated anchor the KV layer lacks.

### Phase 1 — Harvest the CA-authenticated key (foundation, no format change)

On a completed mTLS handshake, extract the peer's Ed25519 key from its **validated** cert and
record it in `peer_keys[peer]` as an **anchored** key (a key we obtained from the CA, not from
KV). Concretely:

- `GossipStream::{TlsServer,TlsClient}` expose the rustls connection; `peer_certificates()`
  returns the validated chain (rustls already verified it against the CA via
  `WebPkiClientVerifier`, so no *trust* decision is re-made here — only extraction).
- Parse the leaf cert's `SubjectPublicKeyInfo` for the Ed25519 key (OID `1.3.101.112`). This
  needs a DER/x509 parse — **decision point:** add a small parser dep (`x509-parser` or `der`)
  or extract the fixed-layout 32-byte key from the Ed25519 SPKI with a hardened, length-checked
  helper. (Recommend `x509-parser`: robust, widely used, avoids offset fragility.)
- Store anchored keys distinctly from KV-mirrored keys — either a parallel
  `peer_anchor_keys: papaya::HashMap<NodeId, HashSet<[u8;32]>>`, or tag entries in `peer_keys`.

**What Phase 1 buys, alone:** every *directly-connected* peer now has a CA-authenticated key in
its set. It does **not** yet reject poisoned KV keys (accumulate still unions them), but it
establishes the anchor Phases 2–3 chain to, and it enables an *accurate* conflict tripwire:
warn + counter when a `sys/identity/{V}` entry introduces a key for a `V` whose **anchored** key
is known and differs — a real detection signal (unlike a naive growth counter, which can't tell
rotation from poisoning). Low-risk, additive, no wire change.

### Phase 2 — Signed identity entries + rotation chained to a trusted key

Make `sys/identity/{V}` self-authenticating without breaking old readers:

- **Format (append-only, old-readable):** keep the raw `32·N` key history as the *prefix* so
  `parse_identity_keys` on an old node still reads the real keys; append a trailer
  `‖ signer_key(32) ‖ signature(64)` where `signature = sign(signer_sk, key_history_bytes)`.
  Distinguish via an explicit **version/magic byte** at the *front* of the trailer region plus
  a self-describing length, so a new reader unambiguously splits history from trailer regardless
  of history length (do **not** rely on total-length parity — history is unbounded).
  - Old readers: `parse_identity_keys` currently requires `len % 32 == 0`. The trailer is
    `1 + 32 + 64 = 97` bytes, breaking the multiple-of-32 check → old readers would reject the
    *whole* entry. **Therefore the trailer must be carried in a sibling key**, not appended:
    `sys/identity-proof/{V}` = `{signer_key, signature, covers_hlc}`. Old nodes ignore the
    unknown key and read `sys/identity/{V}` as before; new nodes require a valid matching proof.
    (This is cleaner than in-band framing and fully old-compatible.)
- **Validation on read/merge (new nodes):** accept keys from `sys/identity/{V}` into `peer_keys[V]`
  only if a valid `sys/identity-proof/{V}` exists and its `signer_key` is:
  - the Phase-1 **anchored** (CA-cert) key for V, **or**
  - a key **already trusted** for V from a prior valid entry (rotation chained to a prior key), **or**
  - if V is entirely unknown (no anchor, never connected): **TOFU** — accept the first
    self-signed entry, and record it as provisional (upgradable to anchored when V is later
    connected; conflict → tripwire).
- **Publishing:** a node signs its own entry with its current key on every publish/rotation
  (`kv_persist` identity path). Rotation signs the new history with the **prior** key, so peers
  chain trust.

**What Phase 2 buys:** an attacker's overwrite of `sys/identity/{V}` is rejected unless signed by
a key already trusted for V — which the attacker does not hold. The anytime-overwrite vector is
closed for any V with an anchor or an established set. Residual: the pure-TOFU first-sighting race
for a V never directly connected and not yet established (shrunk from "anytime" to "first-write
only", and eliminated entirely once V is connected once, via the Phase-1 anchor).

### Phase 3 — Tighten (reject unauthenticated), wire-version-gated

Once all nodes write signed proofs (a full rollout, gated like a wire-version bump):

- New nodes **reject** any `sys/identity/{V}` update lacking a valid proof (an unsigned entry can
  no longer *modify* an established set; it may at most match it). This closes the "mimic an old
  node with an unsigned entry" bypass that Phase 2 must tolerate during rollout.
- Sequence as a two-release migration: **R1** = write proofs + accept-both (Phase 2); **R2** =
  require proofs (Phase 3). Document the min-version like `PREV_WIRE_VERSION`.

## 5. Security analysis

| Vector | Today | After P1 | After P2 | After P3 |
|---|---|---|---|---|
| Overwrite `sys/identity/{V}` to inject a key, V connected | ✅ works | detected (tripwire) | **rejected** | rejected |
| …V never connected, set established | ✅ works | ✅ works | **rejected** (no valid prior-key proof) | rejected |
| …V never connected, first sighting (TOFU race) | ✅ works | ✅ works | ⚠ race (first writer wins) | ⚠ race |
| Mimic old node with unsigned entry to modify a set | ✅ works | ✅ works | ✅ works (rollout tolerance) | **rejected** |
| Buggy node writes wrong identity | poisons perm. | detected | rejected (unsigned/foreign) | rejected |

The irreducible residual is the **TOFU first-sighting race for a node never directly
connected** — inherent to any gossip-propagated identity without a pre-shared root beyond the CA.
It is eliminated for any node that is directly connected even once (Phase-1 anchor). A deployment
that wants zero TOFU can require full-mesh connectivity or distribute identities out-of-band; both
are operational choices to document, not code.

## 6. Failure modes & litmus tests

- **Legitimate rotation propagates:** node V rotates; peers that trust V's prior key accept the
  new key (proof signed by prior key). Multi-node test: rotate, assert peers verify a message
  signed by the new key and still verify one signed by the old (retained).
- **Poisoning rejected:** node M writes `sys/identity/{V}` + proof signed by M's key (not in V's
  trusted set); assert peers do **not** add M's key to `peer_keys[V]` and a forged
  `Vote{voter:V}` signed by M is dropped by `decode_verify`.
- **Old-node compat (Phase 2):** an old (unsigned) node's identity still establishes on new
  nodes (TOFU/anchor); a new node's signed entry is still readable by old nodes (sibling proof
  key ignored).
- **Anchor conflict tripwire (Phase 1):** a `sys/identity/{V}` key differing from V's cert-anchored
  key increments a counter and warns.
- **No regression:** existing `chain_spanning_a_key_rotation_verifies_against_the_key_set` and the
  consensus-auth gates stay green.

## 7. Code impact map

- `mycelium-core/src/tls.rs` — extract Ed25519 key from a peer cert DER (new helper; parser-dep
  decision).
- `mycelium-core/src/stream.rs` / `connection.rs` / `writer.rs` — surface the validated peer cert
  post-handshake to the connection setup so the key can be harvested.
- `src/agent/context.rs` (`TaskCtx`) — an anchored-keys map (or a tag on `peer_keys`).
- `src/agent/helpers.rs` — `parse_identity_keys` / `merge_peer_keys` gain proof-validation; a new
  `identity_proof` encode/decode + verify.
- `src/agent/lifecycle.rs` — `prewarm_peer_keys` / `start_identity_watcher` validate proofs;
  publish path signs.
- `src/agent/mod.rs` rotation path — sign the rotated history with the prior key.
- `src/consensus.rs::decode_verify` — already filters revoked keys (pass 3); no change beyond
  reading the now-authenticated `peer_keys`.
- `src/lib.rs` KV-namespace table — add `sys/identity-proof/`.
- Wire-version note for Phase 3.

## 8. Sequencing & effort

- **Phase 1** (anchor + tripwire): ~1 focused change + the parser-dep decision + multi-node test.
  Safe, additive, high foundational value. **Recommended next concrete step.**
- **Phase 2** (signed proofs + rotation chain): the core; needs careful multi-node rotation +
  poisoning tests. Medium.
- **Phase 3** (require proofs): a gated migration release; small code, real rollout coordination.

Each phase ships independently and leaves the system correct. Phase 1 alone materially improves
the posture (authenticated anchor for connected peers + a real detection signal) at low risk;
Phases 2–3 close prevention for the CFT-relevant cases.

## 9. Alternatives considered

- **Detection tripwire only (no auth):** rejected as *primary* — rotation and poisoning are
  byte-identical without an anchor, so a growth counter false-positives on every rotation.
  Becomes accurate *after* Phase 1 (anchor), where it is included.
- **Drop KV identity; use only the mTLS cert key:** breaks mesh-wide verification of forwarded
  consensus messages from peers a verifier never directly connected to.
- **Full quorum-certificate BFT consensus:** out of scope (CFT-not-BFT); far larger; unnecessary
  to close this specific vector.
- **In-band signed format (append to `sys/identity`):** rejected — breaks the old reader's
  `len % 32 == 0` check; the sibling `sys/identity-proof/` key is cleaner and old-compatible.
