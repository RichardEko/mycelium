# SOC 2 audit-gap closure — plan

**Status: ✅ COMPLETE (2026-07-22).** All workstreams shipped and CI-verified —
WS-0 (matrix) · A (gateway TLS) · B (rotate+revoke) · C (audit sink) · D (checkpointing) ·
E (identity auth 1a/1b/2/3) · F (crypto-shred). The four decisions below were resolved as
recommended, except **Phase 3's mechanism** — implementation showed a `WIRE_VERSION` bump was the
wrong instrument (no frame-format change), so it's gated by the config flag `require_identity_proofs`
with the same two-release rollout discipline. Scope = the five gaps a pentest / auditor control
walkthrough surfaces in an adopter's SOC 2, plus the adopter-facing shared-responsibility matrix
that frames them. Path context: **pure library** (confirmed 2026-07-22 — no managed service; see
[licensing-and-compliance](../wiki/domain/strategy/licensing-and-compliance.md)). Every "fix" here
makes the *library* a cleaner, more auditable component so an adopter can pass *their* audit — none
of it makes Mycelium itself hold a report.

## The matrix is the spine

The [shared-responsibility matrix](#workstream-0--the-shared-responsibility-matrix) is drafted
first and documents current reality — every control, and whether Mycelium provides it, the deployer
owns it, or it is a **gap**. Each workstream below is defined as **flipping specific matrix cells**
from "gap / deployer-owns-alone" to "Mycelium-provides / shared." That gives a measurable,
audit-defensible definition of done, and the matrix becomes the durable adopter deliverable that
outlives this plan.

**Definition of done, every workstream:** (1) the code/design change, (2) tests in the pattern this
repo enforces — deterministic gates, structural readiness (no fixed sleeps), gated in `make check` /
`make check-full` incl. the `compliance` suite (now in CI), (3) an operations runbook page, (4) the
matrix cell flipped with evidence citation.

---

## Workstream 0 — the shared-responsibility matrix — ✅ DRAFTED 2026-07-22

**Delivered:** `docs/operations/shared-responsibility-matrix.md` — the CC1–CC9 +
Confidentiality/Availability control map (M / Shared / Deployer-owns), with the ⚠ gap rows linked to
their workstreams. Living doc: cells flip as each WS lands (WS-A already shown shipped). Linked from
the operations README.

**Objective.** One adopter-facing table: for each SOC 2 Common Criterion (CC1–CC9) + the
Confidentiality/Availability criteria, state **Mycelium-provides / Shared / Deployer-owns**, the
control's config surface, and the evidence pointer. Assembles content today scattered across
`threat-model.md` (the `Residual:`/`Mitigations:` split), `crown-jewel.md`, `audit.md`, and
`security.md`.

**Deliverable.** `docs/operations/shared-responsibility-matrix.md` (operator/adopter-facing) +
a condensed version fit for the compliance-tier evidence package.

**Size: M.** No code. Draft in Phase 1; update one row per workstream as cells flip.

---

## Workstream A — Gateway TLS (native, optional) — ✅ SHIPPED 2026-07-22

**Delivered:** `GossipConfig::gateway_tls` (`GatewayTlsConfig`) — reuse-node-cert (rotates with
identity) or operator-supplied PEM; server-only rustls (`with_no_client_auth`); hand-rolled
`tokio-rustls` + `hyper-util` serve loop (no new compiled crate); plaintext stays the default/opt-out.
Gate: `test_gateway_serves_native_tls`. Runbook: `docs/operations/gateway-tls.md`. Matrix cell
flipped: encryption-in-transit (gateway) → **Shared (native or proxy)**.

**Gap.** The gateway HTTP server is plaintext (`axum::serve` over a plain `TcpListener`,
`src/agent/http.rs:282`); bearer tokens / JWTs traverse cleartext. Sharpest single pentest finding.

**Approach.** Native **server-only** TLS on the gateway, opt-in, plain HTTP as the opt-out.
The gossip mTLS config (`build_rustls_configs`, `mycelium-core/src/tls.rs:312`) **cannot** be reused
— it requires a CA-signed *client* cert (`WebPkiClientVerifier`), which no browser/curl/SDK presents,
and the node cert has only an IP SAN. The gateway needs its own `ServerConfig` built with
`.with_no_client_auth()`.

**Touchpoints.**
- `mycelium-core/src/config.rs:~651` — new `gateway_tls: Option<GatewayTlsConfig>` (None = today's
  plaintext). Variant: reuse-node-identity-cert **or** operator-supplied cert/key paths (the latter
  for a real hostname SAN / browser trust).
- `mycelium-core/src/tls.rs:~312` — new `server_only_config(cert, key) -> ServerConfig`; expose
  `NodeTls::gateway_server_config()`.
- `src/agent/http.rs:282` — branch: if `gateway_tls` set, wrap the listener in a
  `tokio_rustls::TlsAcceptor` serve-loop; else current `axum::serve`. `ctx.tls` already in scope;
  graceful shutdown reuses `shutdown_signal`.
- `tokio-rustls` + `rustls` are **already deps** under `tls` — no new crate required.

**Tests.** Integration: start an agent with `gateway_tls`, hit it with a rustls HTTP client → 200;
assert plain HTTP path unchanged when `None`; assert no client cert is demanded.

**Docs.** New `docs/operations/gateway-tls.md`; update `production-readiness.md`.

**Size: M** (S if `axum-server` is added — a decision, see below). **Flips:** Encryption-in-transit
(gateway) from *Deployer-owns (front with proxy)* → *Shared (native or proxy)*.

---

## Workstream B — Rotation defends a compromised key — ✅ SHIPPED 2026-07-22

**Delivered:** `GossipAgent::rotate_identity_on_compromise` (rotate → revoke the old key with the
new key) + operator route `POST /gateway/identity/revoke` (scope `identity:write`). The revocation
crypto + cluster-wide exclusion (incl. consensus) already existed; this added the missing triggers.
Gate: `test_rotate_on_compromise_revokes_old_key`. Docs: cert-rotation compromise section. Matrix
cell flipped: key-compromise → **Mycelium-provides**. (Open, by design: force-revoking a dead node's
key needs a separate operator authority.)

**Gap.** Rotation is hygiene, not remediation: the retired key stays accepted (retained-set model),
and `rotate_identity` (`src/agent/mod.rs:1010`) publishes `sys/identity` **unsigned** and never
revokes. "Rotate to contain compromise" doesn't hold.

**Good news from scoping.** The remediation primitive is **already built and complete**:
`revoke_identity_key` (`mod.rs:1104` → `revocation.rs:111`) writes a signed `SignedRevocation`;
validation requires the node's *current* key; and **all three verify paths already exclude revoked
keys** — role/audit (`helpers.rs:266`), and crucially **consensus** (`consensus.rs:663`, a pass-3
fix). RFC-6962 transparency over the revocation set is served at `GET /gateway/transparency`. So the
crypto and cluster-wide exclusion are done; the gap is operator glue.

**Approach (glue only).**
1. **Operator-facing revoke trigger** — today revocation is a Rust API only; there is no HTTP write
   route or CLI. Add a scope-gated `POST /gateway/identity/revoke` (`identity:write` or reuse
   `admin`) + a CLI/bin path. *(S–M.)*
2. **Chain into a compromise flow** — add an opt-in "compromise mode" to `rotate_identity` that,
   after cutover, calls `revocation::revoke_key(old_key)` signed by the new current key. *(S.)*
3. **Document the model sharply** — rotation = hygiene, revoke = remediation; and the honest
   coordinator-free limit: **only the node itself (holding its current key) can revoke its own key**
   — a fully-compromised/offline node cannot be force-revoked by a fleet operator without a separate
   authority mechanism (out of scope; flag it).

**Tests.** After rotate+revoke-on-compromise, the old key no longer verifies a consensus `Vote` and
a role claim (extend the existing revocation-exclusion tests with the chained flow + the HTTP route).

**Docs.** Rewrite `cert-rotation.md`'s compromise section; add the revoke route to `operations.md`.

**Size: S–M total.** **Flips:** Key-compromise remediation from *gap* → *Mycelium-provides (revoke)*.

---

## Workstream C — Audit export sink — ✅ SHIPPED 2026-07-22

**Delivered:** `AuditSink` trait + `GossipAgent::with_audit_sink`; `seal_and_write` mirrors each
sealed record into a bounded channel drained to the sink on a background task (off the write path);
drop-on-full is loud and the chain stays authoritative. Gate:
`test_audit_sink_mirrors_sealed_records`. Docs: audit runbook §Export. Matrix cell: audit export →
**Shared (hook provided)**.

**Gap.** No built-in export; the in-cluster hash-chain is a hot window only.

**Approach.** A pluggable sink at the single choke point `seal_and_write` (`src/agent/audit.rs:261`)
through which every record funnels.
- `pub trait AuditSink: Send + Sync { fn export(&self, rec: &SignedAuditRecord); }`
- `audit_sink: Option<Arc<dyn AuditSink>>` on `TaskCtx` (beside `tls`/`audit_chain`).
- House-style (off the hot path): `seal_and_write` pushes the sealed record into a bounded `mpsc`; a
  background task (spawned by `lifecycle.rs:~190`) drains → external SIEM/WORM with retry/backpressure.
  App supplies the `Arc<dyn AuditSink>` at build time.

**Tests.** Sink receives every sealed record; background drain under load; ordering preserved.

**Docs.** `audit.md` retention section — the export half of the story.

**Size: S–M.** **Flips:** Audit export from *Deployer-owns-integration* → *Shared (hook provided)*.

---

## Workstream D — Audit retention / checkpointing — ✅ SHIPPED 2026-07-22

**Delivered:** signed `AuditCheckpoint` under `sys/audit-checkpoint/{node}/{seq}` (separate prefix);
`audit_checkpoint()` seals the current boundary; `audit_prune_to_checkpoint()` tombstones records
below it; `verify_stream` resumes from the newest covering checkpoint (genesis when unpruned). Gate:
`test_audit_checkpoint_prune_and_verify` (checkpoint → prune 0..6 → the remaining stream still
verifies). KV table rows added (`sys/audit/`, `sys/audit-checkpoint/`, `sys/revocation/`,
`sys/capauthz/`). Docs: audit runbook §Retention. Matrix cell: audit retention → **Shared**.

**Process fix (same day):** the guardrails CI job (which builds `mycelium` with `compliance`) caught
a `collapsible_if` that `make check` missed — `make check`'s clippy runs *without* `compliance`, so
compliance-gated code went un-linted locally. Added a compliance clippy to `make check`.

**Gap.** Retention is unbounded; you can't prune, because `verify_stream` verifies **from genesis**
(deleting record 0 → `SequenceGap` + `BrokenLink`, proven by `removing_a_record_breaks_the_chain`).

**Enabler already present.** The lower-level `verify_chain` (`audit.rs:195`) already accepts an
arbitrary start boundary, and `mid_stream_range_verifies_with_known_boundary` proves mid-chain
verification works given a trusted boundary. Pruning only needs a **signed statement of that
boundary.**

**Approach (minimal safe checkpoint).**
1. New signed checkpoint record `{node_id, checkpoint_seq: N, prev_hash_at_seq: P, hlc}`, signed via
   the existing identity-key path — as trustworthy as the records it summarizes.
2. Separate prefix `sys/audit-checkpoint/{node}/{seq}` so pruning `sys/audit/` never touches it.
3. Retention flow: export `[0..N)` via the WS-C sink → delete `sys/audit/{node}/{0..N-1}`.
4. `verify_stream` (`audit.rs:316`) loads the newest checkpoint ≤ the first present seq and verifies
   from there; full genesis verification stays possible offline against the exported archive.

**Tests.** verify-from-checkpoint; prune-then-verify; a tampered checkpoint fails; genesis verify of
the exported archive still holds.

**Docs.** Rewrite the `audit.md` known-limitation into a retention runbook (checkpoint → export →
prune, with the 7-yr HIPAA framing pointing at the external WORM).

**Size: L** (new signed artifact + prefix + issuance + prune + verify-from-checkpoint). **Flips:**
Audit retention from *gap* → *Mycelium-provides (checkpoint) + Deployer-owns (WORM destination)*.

---

## Workstream E — `sys/identity` authentication (the integrity fix)

**Gap.** `peer_keys` is populated only from unauthenticated KV gossip and `merge_peer_keys`
**accumulates, never drops** (`helpers.rs:221`). A compromised **or merely buggy** admitted node can
LWW-union a foreign key into `peer_keys[V]` permanently and forge a `Vote{voter:V}` that
`consensus::decode_verify` accepts. Design exists: `docs/design/identity-authentication.md`.
**Scope honesty:** Mycelium is CFT-not-BFT; this is defense-in-depth for the signed-consensus layer
and a robustness fix against buggy nodes — it does **not** make consensus BFT (a standing invariant).

**Phase 1a — SHIPPED.** `ed25519_key_from_cert_der` (`tls.rs:302`) exists + tested, but is called
**only from its own test module** — unwired.

**Phase 1b — ✅ SHIPPED 2026-07-22 (harvest + anchor + tripwire).** Implementation deviated from the
design's callback-threading (simpler): `peer_keys` is on `CoreCtx`, and `tls: Option<Arc<NodeTls>>`
is already threaded through all 10 `get_or_spawn_writer` sites — so the anchor recorder hangs off
`NodeTls` (`set_anchor_sink`/`record_anchor`), the writer harvests via `GossipStream::peer_ed25519_key`
after the outbound handshake, and the maps (`peer_anchor_keys`, `identity_anchor_conflicts`) live on
`CoreCtx`. **Zero new threading through the writer callers.** The `flag_identity_anchor_conflict`
tripwire fires at both KV-merge sites; counter surfaced on `/stats` + `SystemStats`. Gate:
`test_identity_anchor_recorded_and_conflict_flagged` (2-node TLS: B anchors A's real key; a foreign
KV key trips the counter). No wire change.

**Phase 1b (design's original framing) — harvest wiring + anchor + tripwire. Size M. No wire change.**
On a completed handshake, record the CA-derived key as an **anchored** key in a new
`peer_anchor_keys` on `TaskCtx`, via an `anchor_sink` callback threaded from
`mycelium-core::run_peer_writer` up to the `mycelium`-side merge (cross-crate direction forces the
callback). Add a tripwire: warn + counter when a `sys/identity/{V}` entry introduces a key differing
from V's anchor. Anchors **outbound** peers only (cert SAN is IP-only, so clean NodeId↔cert
correlation exists only when we dialed). Non-fatal (runs after connect). **Buys:** authenticated
anchor for every directly-connected peer + an accurate tripwire (a naive growth counter
false-positives on every legitimate rotation — why detection-only is insufficient).

**Phase 2 — ✅ SHIPPED 2026-07-22 (signed proofs — prevention).** New sibling KV
`sys/identity-proof/{V}` = `signer_key(32)‖sig(64)` over the identity history; a node signs its own
entry on publish/rotation (rotation signs with the *prior* key, pre-cutover, so peers chain trust).
On merge, `validate_and_merge_identity` accepts a key only if the proof is signed by a key already
trusted for V (CA anchor / prior key) or, for an unknown V, TOFU-accepts a self-signed first entry;
a proof signed by an **untrusted** key is **rejected** (poisoning) + counted; no proof falls back to
rollout tolerance (Phase 3 tightens). Gate: `test_identity_proof_rejects_poisoning_accepts_signed`
(untrusted-signed overwrite rejected; prior-key-signed rotation accepted) + the 1b test (stale-proof
overwrite now rejected). No wire change (additive sibling key; old nodes ignore it).

**Phase 2 (design's original framing) — signed identity entries. Size M. The core.**
Proof goes in a **sibling** key `sys/identity-proof/{V}` (not in-band — `parse_identity_keys`
requires `len % 32 == 0`, so a 97-byte trailer would break old readers). New nodes accept a key from
`sys/identity/{V}` only with a valid matching proof signed by (a) V's anchored CA key, (b) a key
already trusted for V (rotation chained to prior key), or (c) TOFU first-sighting if V is unknown.
Touchpoints: `helpers.rs` (proof encode/decode/verify + merge gate), `lifecycle.rs`
(`prewarm_peer_keys:485`, `start_identity_watcher:502` validate + publish signs),
`mod.rs` rotation signs with the **prior** key (fixes WS-B's unsigned-publish note too),
`src/lib.rs` KV table adds `sys/identity-proof/`. **Buys:** an overwrite is rejected unless signed by
an already-trusted key.

**Phase 3 — ✅ SHIPPED 2026-07-22 (reject unauthenticated).** Gated by a **config flag**
`require_identity_proofs` (default false; `GOSSIP_REQUIRE_IDENTITY_PROOFS`), **not a WIRE_VERSION
bump** — implementation confirmed Phase 3 changes no frame format (proofs gossip as ordinary Data
frames), so a v13 bump would gate nothing and spuriously open a rolling-upgrade window. When set, an
identity entry with no valid proof is rejected outright (the "mimic a pre-Phase-2 node" residual).
The identity watcher now also watches the proof prefix so a late-arriving proof re-validates (no
transient partition). Two-release rollout documented like a `PREV_WIRE_VERSION` window
([cert-rotation](../operations/cert-rotation.md)). Gate:
`test_require_identity_proofs_rejects_unsigned` (flag off → accepted; flag on → rejected + counted).

**Phase 3 (design's original framing) — reject unauthenticated. WIRE-VERSION-GATED (v13).**
New nodes reject any `sys/identity/{V}` lacking a valid proof. **Two-release migration**, gated
exactly like `PREV_WIRE_VERSION`: R1 = write proofs + accept-both (Phase 2 ships); R2 = require
proofs (Phase 3), only after the fleet is known to write proofs — else new nodes partition by
rejecting legitimate old unsigned entries. **Must be scheduled as a deliberate wire bump**, not
slipped in.

**Tests.** 1b: anchor recorded for a dialed peer; tripwire fires on conflict. 2: unsigned/wrongly-
signed overwrite rejected; signed rotation chains; TOFU first-sighting. 3: the accept-both→require
migration + a wire-version interop gate (mirrors `decode_wire_v11_*`).

**Flips:** Identity integrity from *gap (TOFU-accumulate)* → *Mycelium-provides (anchored + signed;
enforced after v13)*.

---

## Workstream F — GDPR / erasure / data-residency — ✅ SHIPPED 2026-07-22

**Delivered:** `SubjectKeyRegistry` crypto-shred helper (`mycelium_core::erasure`, `tls` — AES-256-GCM
via ring, no new compiled crate): per-subject DEK, `encrypt_for`/`decrypt_for`/`destroy`, `install_key`
seam for KMS custody. Erase = destroy the DEK → all ciphertext undecryptable. Gates: 4 tests
(round-trip + subject isolation; destroy makes ciphertext unrecoverable and re-encrypt never revives
it; AEAD tamper rejection; KMS-custody seam). Runbook `operations/data-erasure.md` with the honest
limits (plaintext escapes, DEK-in-backup, residency = node placement). Matrix cell → **Shared**.

### (design record, superseded by the above)
🟡 DESIGN LANDED 2026-07-22

**Design:** [`docs/design/data-lifecycle-and-erasure.md`](../design/data-lifecycle-and-erasure.md) —
crypto-shredding (per-subject DEK; erase = destroy the key), why physical deletion isn't guaranteeable
in a gossip+WAL mesh, DEK custody options (KMS-first), composition with `DataAtRestCipher`, residency
as deployer-owned node placement, and the honest limits (plaintext escapes, DEK-in-backup). Reference
helper implementation pending.

**Gap.** No treatment anywhere. Not SOC 2 core, but every enterprise/privacy review asks.

**The hard part.** True physical erasure is not guaranteeable in a gossip mesh: LWW tombstones have
retention windows, anti-entropy can resurrect, and the WAL persists. So the honest mechanism is
**crypto-shredding**: application-level per-subject envelope encryption (each subject's data under a
per-subject key), where "erase" = destroy the key → ciphertext is unrecoverable even if bytes
linger. The existing `DataAtRestCipher` hook covers disk only; erasure needs the per-subject layer
above it. **Residency** is a deployment property — single cluster per region, isolated-by-
construction (`deployment-framing.md`) — so it is largely *documented + deployer-owned*, not code.

**Approach.** Design doc first (`docs/design/data-lifecycle-and-erasure.md`): the crypto-shred
pattern, a reference helper (per-subject key registry + envelope encrypt/decrypt + shred), tombstone
+ no-resurrection semantics, and an **honest limits** statement (what is guaranteed vs. best-effort;
WAL/backup caveats). Implement the reference helper after the design lands.

**Size: L (design-heavy).** Start the **design** in parallel with Phase 1; implement later. **Flips:**
Erasure/residency from *absent* → *Mycelium-provides (crypto-shred reference) + Deployer-owns
(residency, backups)*.

---

## Sequencing

| Phase | Workstreams | Why here |
|---|---|---|
| **1** | WS0 matrix (draft) · **A** gateway TLS · **B** rotation/revocation · *start WS-F design* | Independent, low-risk, highest audit-visibility; B is cheap (crypto done). Matrix frames everything. |
| **2** | **C** export sink → **D** checkpoint/retention | Completes the audit-trail control; C unblocks the retention story before D lands. |
| **3** | **E** Phase 1b → Phase 2 | The identity-integrity fix; no wire change; substantially closes the gap for connected peers + signed rotation. |
| **4** | **E** Phase 3 (v13 wire bump) | The finish line for identity — deliberately a rolling-upgrade window, one release after Phase 2. |
| **5** | **F** implementation | After its design lands (design ran in parallel from Phase 1). |

Matrix updated continuously; each phase flips its cells.

## Decisions — all resolved (2026-07-22)

1. **Gateway TLS (WS-A):** ✅ native HTTPS, hand-rolled `tokio_rustls` + `hyper-util` acceptor
   (no new compiled crate). Shipped.
2. **Identity Phase 3 (WS-E):** ✅ shipped — but **not** a v13 wire bump. Implementation confirmed
   Phase 3 changes no frame format, so the gate is the config flag `require_identity_proofs`
   (default off) with the same two-release rollout discipline (documented like `PREV_WIRE_VERSION`).
3. **GDPR scope (WS-F):** ✅ reference crypto-shred helper (`SubjectKeyRegistry`) + limits doc. Shipped.
4. **Audit export (WS-C):** ✅ sink shipped in-lib (`AuditSink`).

## What stays deployer-owned by design (not gaps to "fix")

TLS termination choice (native **or** proxy) · SIEM/WORM destination · data residency (node
placement) · cluster CA key + at-rest KMS custody · the adopter's **own** SOC 2 on their **own**
system. The matrix states these plainly so "deployer-owned" reads as a deliberate boundary, not an
omission.
