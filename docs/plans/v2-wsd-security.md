# Delivery plan — WS-D · Security & trust hardening

**Status:** proposed (2026-06-20). The v2.0 plan's WS-D, executed. Two tracks, both
**coordinator-free** and both homed in the `compliance` Cargo feature (so the default build is
unchanged): a **revocation transparency log** (closes the WS5 compromise gap) and **gossip-level
capability authorization** (M6 — enforce at resolve, detect at write).

**Done when** (from v2.0 §WS-D): capability writes are authz-enforced at the gossip layer, and a
compromised key can be revoked cluster-wide with client-checkable proof — not just rotated.

**The governing posture** (ROADMAP §v2.0 M6, Core Principle 4): **enforce above, detect below.** A
higher-layer law is *never* turned into a Layer I write guard — the same inversion the consensus
commit-conflict and `sys/` tripwires deliberately avoid. So: unauthorized/revoked writes still
propagate per LWW; consumers decline to *act* on them, and a tripwire counter makes the violation
legible.

---

## Track 1 — Revocation transparency log  *(Quilt-DD #2 — the federation keystone, do first)*

WS5's retained-key-set rotation accepts a *retired* key forever (documented caveat: "rotating away
from a compromised key needs explicit revocation on top"). This track delivers that revocation —
gossip-replicated, per-domain, **no central CT operator** — and makes it client-checkable.

### D1 · Revocation log foundation (closes the WS5 caveat)

- A new **owned KV namespace** `sys/revocation/{node}/{seq:016x}` — an append-only, **hash-chained,
  signed** log of revocation events (reusing the WS2 `audit.rs` chain shape: `prev_hash` = SHA-256
  of the predecessor; Ed25519-signed by the node identity). A revocation event names a verifying
  key (`[u8;32]`) the node is retiring-as-compromised.
- `GossipAgent::revoke_identity_key(key)` — append a signed revocation event for `key` (a key this
  node previously published for itself). Detection-not-prevention applies to *forging* a revocation:
  a revocation only counts if signed by an identity that legitimately owns the revoked key (verified
  against `sys/identity/{node}` history).
- **Every retained-key verify path consults the revocation set and excludes revoked keys**: the four
  WS5 sites — `connection` SignedData, `consensus` decode_verify, `rbac::roles_of`, `audit`
  verify_chain (via the key-set). A revoked key, once the event has gossiped, is rejected
  everywhere — "sub-second revocation" bounded by gossip latency.
- **Gate (G-D1):** two `tls`+`compliance` nodes; A rotates, then A `revoke_identity_key(old)`; B,
  after the revocation gossips, **rejects** A's audit chain / role claim / signed data signed by the
  *old* key, while still accepting the *current* key. The pre-revocation WS5 retained-key test
  (`chain_spanning_a_key_rotation_verifies`) stays green for non-revoked rotations.

> Feature gating: all of Track 1 is under `compliance` (= `["gateway","tls"]`). Without it the
> verify paths behave exactly as today (retained-key accepts everything). The revocation *check* is a
> compliance-gated addition inside the tls verify paths.

### D2 · Client-checkable inclusion proofs

- Merkle-ize the revocation log: each node's `sys/revocation/{node}` stream gets a Merkle root over
  its events; an **inclusion proof** (audit path) lets a fetcher verify a specific revocation is in
  the log without trusting the server or replaying the whole chain.
- `GET /gateway/transparency` (scope `transparency:read`, deny-by-default) serves: per-node head
  (root + seq), and an inclusion proof for a queried `(node, key)`.
- A verifier (`verify_revocation_inclusion(proof, root, event)`) is a pure function, unit-tested
  without a live agent. This is the "client-checkable proof" half of the done-when.
- **Gate (G-D2):** a tampered log (drop/alter a revocation) fails inclusion-proof verification at a
  precise index; a valid proof verifies against the published root.

### D3 · Federation tie-in (optional, light)

- Surface the revocation head in the M16 AgentFacts `certification` (the `mycelium-agentfacts`
  edge) so a quilt fetcher can check "is this domain's key set current?" — the ROADMAP's
  "precursor that most strengthens M16's self-certified credibility." Design-note + thin wiring;
  no new authority.

---

## Track 2 — Gossip-level capability authorization (M6)

The v1.x RBAC subset (WS1) enforces at the **gateway** + the `sys/` tripwire. M6 extends enforcement
to **resolve time** on gossiped capabilities — node-local, emergent, no admission coordinator.

### D4 · Resolve-time capability ACL (the enforcement point)

- A capability may carry an `authorized_roles` ACL (advisory `authorized_callers` already exists in
  v1; this is the *advertiser-side* dual: "only advertisers holding role R may provide `ns/name`").
  Encode it on the `CapabilityGroupDef` / a cluster policy entry (see D6).
- `resolve` (and the SkillRunner serve path) **drops** capabilities whose advertiser's **signed
  role** (`rbac::roles_of`, signature-verified) does not satisfy the ACL. A consumer simply never
  sees an unauthorized provider — it routes around it. This is the coordinator-free enforcement.
- **Gate (G-D4):** node A advertises `ns/name` without the required role; consumer B's `resolve`
  returns empty (or excludes A) while an authorized A′ is resolvable.

### D5 · Write-detect tripwire (legibility)

- An unauthorized capability advertisement (advertiser lacks the ACL role) detected at the
  write/forward path → `warn!` + a cumulative `SystemStats::cap_authz_violations` counter on
  `/stats` — the exact tripwire idiom of `commit_conflicts` / `sys_namespace_violations`. **The
  advertisement still propagates per LWW**; consumers decline to resolve it (D4). No forwarding-hop
  block.
- **Gate (G-D5):** the counter increments on an unauthorized advertisement; the entry is still
  present in the store (detection, not prevention).

### D6 · Consensus-distributed role policy

- The cluster-wide capability-authz policy (which roles may provide which `ns/name`) is distributed
  via **consensus** (a `consensus/`-committed policy doc) so every node enforces the *same* policy —
  but **enforcement stays at each resolver** (D4). Consensus carries the policy; it is not an
  admission coordinator.
- **Gate (G-D6):** a policy committed via `system_propose` is read by every node; all resolvers
  enforce it consistently; revoking the policy reopens resolution.

---

## Sequencing & PRs

1. **D1** — revocation foundation (closes the WS5 caveat; highest-value, federation keystone).
2. **D2** — inclusion proofs + `/gateway/transparency` (the "client-checkable" half of done-when).
3. **D4 + D5** — resolve-time capability ACL + write-detect tripwire (M6 core).
4. **D6** — consensus-distributed policy.
5. **D3** — AgentFacts revocation-head tie-in (light, optional).

Each is its own PR. Gates run under `--features compliance` (and `tls`). The default build and the
existing WS1/WS2/WS5 tests stay unchanged — every addition is compliance-gated and
detection-not-prevention, preserving the inverted-dependency invariant.

**Reuse:** the WS2 `audit.rs` hash-chain shape (D1), the WS5 retained-key-set verify sites (D1
hook), the `commit_conflicts` tripwire idiom (D5), the consensus engine (D6), and
`mycelium-agentfacts` (D3). Net-new: the revocation namespace + Merkle inclusion proofs (D2).
