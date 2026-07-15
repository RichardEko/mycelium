# Plan of Action — v1.x Completion (Production Readiness Gap → done)

**Goal.** Close every outstanding v1.x item to a uniform bar — *implemented, fully
tested across the feature matrix, documented (dev + operations + presentation + roadmap),
Gap-Summary status flipped to Complete with a date*. Here "functional" is **the bar itself** — every item ships as a working,
fully-tested capability, not paper — **not** a separate feature bucket. Drafted 2026-06-14.

This plan is the execution counterpart to **ROADMAP.md → Production Readiness Gap** and
the [[project_production_hardening_gate]] / [[project_compliance_strategy]] memory.

---

## Definition of Done (applies to every workstream)

A workstream is *done* only when all of:

1. **Implemented** behind the `compliance` Cargo feature where it is gateway/auth/audit
   surface (today a stub — see `Cargo.toml`); core-substrate pieces (identity, signed
   writes) stay in the default/`tls` build.
2. **Tested** — lib unit tests pass on the full matrix
   (`cargo test --lib --features tls,metrics,a2a,llm,compliance`), `cargo clippy
   --lib --tests … -D warnings` clean, a new integration scenario added under
   `tests/`, and — per the M2 audit ethos — **an executable falsification probe per
   security invariant** kept as a regression test (tamper a sealed audit record → detected;
   unauthorized capability assertion → rejected; forged role write → rejected).
3. **Documented** across all four surfaces (see Workstream 7): dev (`CLAUDE.md`,
   `docs/guide/`, `README.md`), operations (`docs/operations/`, security/threat model),
   presentation (`docs/publications/`, `philosophy.md`, positioning), roadmap
   (flip the Gap Summary row to **Complete (date)**, update the four-sub-gates prose).
4. **Compliance-gated** — verified against the **Core Principles** (no coordinator;
   detection-not-prevention; single substrate; layer discipline) and the **M16
   forward-compatibility acceptance criteria** for the two precursor items.

---

## Guardrails (non-negotiable)

- **Core Principles compliance gate** (ROADMAP). Nothing here may introduce a
  coordinator or teach a lower layer a higher-layer law.
- **M16 / NANDA forward-compat criteria** (ROADMAP → Gap Summary): build to the *stable
  substrate shape*, not NANDA's moving surface; never couple to AgentFacts field names.
  - *Audit:* preserve a **capability-scoped, content-hashed, externally-citable slice**.
  - *RBAC capability-authz:* express "who may assert this capability" **in the signed
    capability entry** (`SignedData` under node Ed25519 identity), not only an HTTP gate.
  - *Edge auth:* keep a **public-readable, signature-verified** path open.
- **Promise-strength / detection-not-prevention.** Audit and RBAC *detect and route
  around* violations (tripwire shape), they do not teach `apply_and_notify` a new law.
- **Lock-order table + lock-free rules** (CLAUDE.md). Any new shared state joins the
  flat lock-order table; any lock-free mutation follows the retry-safe `compute` rules.
- **Test conventions** (CLAUDE.md): full feature matrix locally before push; structural
  polling not fixed sleeps; consensus listeners on every node in multi-node tests.

---

## Workstreams (dependency-ordered)

### WS1 — Identity & RBAC v1.x subset  *(foundation; gap #8)*

**Scope.** Node roles gossiped via `sys/identity/{node}`; capability-assertion
authorization; gateway endpoint ACLs; `sys/`-prefix write guards; **MCP/A2A bridge auth**
(access control on the bridge surfaces, distinct from gateway auth); and the **L1/L2/L3
layer-clearance model** (data-classification-aware roles — an L1 board read ≠ full L3 SPOF
topology). The last two were the AuthN/Z "still to design" items — they are RBAC facets and
live here, not in a separate workstream.

**Key decisions.**
- Roles are **signed claims** under the node's Ed25519 identity (reuse `tls` identity +
  `SignedData`), gossiped under `sys/identity/{node}/role/…`. Promise-strength: a forged
  role write is *detected* (signature/issuer check at the consuming side), not prevented
  at the store.
- **Capability-assertion authz lives in the signed capability entry** (M16 criterion):
  `authorized_callers` / who-may-assert is a field of the signed `cap/` record, enforced
  at resolve/invoke time — not solely a gateway check.
- Gateway endpoint ACLs extend the existing `gateway_auth_token` bearer model with
  role-scoped routes; **keep `/health`, `/ready`, `/metrics`, and the public-verifiable
  descriptor path open** (M16 edge criterion).

**Implementation.** `sys/identity/` role schema + sign/verify; resolve-time authz check
in `capability_ops`/resolve path; gateway ACL middleware (compliance feature);
`sys/` write-guard *tripwire* (detect + `warn!` + stat, mirror the commit-conflict
tripwire — **not** an `apply_and_notify` guard).

**Tests.** Unit: role sign/verify, authz accept/deny, ACL routing. Integration scenario:
multi-node role propagation + unauthorized-assert rejected. **Probe:** forged role write
& unauthorized capability assertion → both detected/rejected (kept as regression).

**Docs.** CLAUDE.md (new "RBAC / identity" architecture-constraint section), README
Security Model, guide chapter, operations RBAC config, ROADMAP Gap row.

### WS2 — Durable tamper-evident audit trail  *(gap #7; M16 keystone precursor)*

**Scope.** Cluster-wide `sys/audit/{hlc}` trail; value-hash tamper-evidence
(hash-chain); `/audit` query endpoint; read-side principal binding; the
**capability-scoped, content-hashed, citable slice** M16 consumes.

**Key decisions.**
- Each audit record is **hash-linked** to its predecessor (per-stream Merkle/hash-chain)
  so an inspector can verify the log was not edited after the fact. Each record carries
  the **principal** (from WS1 identity) for *both* writes and reads.
- Expose a **stable per-claim content hash** so M16 AgentFacts `evaluations` can cite it
  (`auditTrail`-shaped) — *do not* name it after AgentFacts fields.
- Detection-not-prevention: the trail records and proves; it does not block. Cryptographic
  chaining is **in scope** for v1.x (confirmed 2026-06-14, superseding the older
  out-of-scope note) — it is the keystone that backs M16 self-attestation.

**Implementation.** `sys/audit/` writer (HLC-keyed, hash-chained, signed); read-path
principal logging hook; `/audit` endpoint (compliance feature) with verify + range query;
content-hash slice API for the M16 consumer.

**Tests.** Unit: chain build + tamper-detection. Integration: cross-node audit
convergence + late-joiner verify. **Probe:** edit a sealed record → chain verification
fails (regression); confirm read-side principal capture.

**Docs.** CLAUDE.md audit-invariant section, README, guide, operations "audit & evidence"
runbook, ROADMAP Gap row + sub-gate #2 prose, presentation (compliance-evidence story).

### WS3 — Crown-jewel posture  *(new work; sub-gate #3)*

**Scope.** (a) Data-at-rest: optional envelope-encryption hooks for KV/WAL bytes.
(b) Egress boundary policy + config hooks (what the twin may reach outbound).
(c) Blast-radius **threat-model document** cross-linked from `docs/operations/` and the
architecture doc.

**Key decisions.** Encryption **hooks ship in code** (confirmed 2026-06-14, not doc-only):
an **opt-in** envelope-encryption hook where the operator supplies a KMS/keyring adapter —
the substrate stays neutral on key custody. Egress policy is **config + documented
posture**, not a coordinator. Threat model is a doc deliverable + a couple of regression
tests for the egress config gate.

**Tests.** Encrypt/decrypt round-trip on WAL replay; egress-policy denial test.
**Docs (primary deliverable here):** threat-model doc, data-at-rest + egress runbooks,
README security model, ROADMAP sub-gate #3, presentation crown-jewel narrative.

### WS4 — SSO / enterprise IdP  *(gap #9; orthogonal — parallelizable)*

**Scope.** **Generic OIDC** bearer-token validation on the gateway/management surface —
no per-vendor code. Entra/Okta/Auth0/Keycloak are all OIDC-conformant; discover via the
standard `.well-known/openid-configuration` + JWKS, and treat vendor differences as config.
**Human-operator auth — explicitly not agent identity** (orthogonal to NANDA/M16; no
forward-design owed).

**Key decisions.** Validate OIDC JWTs at the gateway (compliance feature); map IdP groups
→ WS1 roles. Test against a **mock OIDC provider** in the integration harness (no live
vendor dependency in CI).

**Tests.** JWT validate/reject (mock issuer, expiry, signature, audience); group→role
mapping. Integration scenario with a containerized mock IdP.

**Docs.** Operations SSO setup (one generic-OIDC runbook + per-vendor config snippets),
README, ROADMAP Gap row.

### WS5 — Hot certificate rotation  *(security gap #3 residual)*

**Scope.** Rotate node TLS/identity certs **without cluster disruption** (the one item
flagged "not yet implemented" in the otherwise-complete security gap).

**Key decisions.** Dual-cert acceptance window (accept old+new during rotation), gossip
the new verifying key to `sys/identity/` before cutover, drain-and-swap per the existing
listener-restart machinery (no full restart). Honors the lock-order table.

**Tests.** Rotation under live traffic: no dropped frames, peers re-verify, zero
connection loss. **Probe:** mid-rotation forged-cert attempt → rejected.

**Docs.** CLAUDE.md TLS section, operations rotation runbook, README, ROADMAP gap #3 prose.

### Functional enhancements — resolved (no separate workstream)

"Functional" is the Definition-of-Done bar (working + tested), not a feature bucket. The
items that looked like enhancements are RBAC facets, now folded into **WS1** (MCP/A2A bridge
auth; L1/L2/L3 layer-clearance). **Explicitly parked, out of scope** for this completion
plan: a `linearizable_get` (ReadIndex) closing the documented `consistent_get`
eventual-consistency gap — a real but optional overlay enhancement to pick up separately,
not a Production-Readiness item.

### WS6 — Documentation alignment  *(cross-cutting; runs with each WS + a final sweep)*

Every WS lands its own doc deltas; this workstream owns the **consistency sweep** so the
four surfaces never drift (the lesson from this session's stale-prose finds):

- **Dev:** `CLAUDE.md` (new architecture-constraint sections per WS; lock-order table
  additions), `docs/guide/` chapters, `README.md` (Security Model, feature matrix,
  `compliance` feature now real not stub).
- **Operations:** `docs/operations/` runbooks (RBAC, audit/evidence, SSO, cert rotation,
  data-at-rest, egress), `tuning.md` if new knobs.
- **Presentation:** `docs/publications/`, `philosophy.md`, the compliance-evidence /
  crown-jewel positioning narratives, [[project_compliance_strategy]] alignment.
- **Roadmap:** flip each **Gap Summary** row Pending → **Complete (date)**; update the
  four-sub-gates prose from "*In flight*"/"*Still to design*" to done; reconcile the
  `compliance` feature description in `Cargo.toml`/CLAUDE.md (stub → implemented).
- **Final sweep:** grep for now-stale "planned / not yet implemented / Pending" prose
  (the §4–6 lesson) and a doc-vs-code cross-check.

---

## Sequencing (phased; each phase ends at a test gate)

```
Phase 1  WS1 Identity & RBAC            ── foundation; audit + SSO depend on it
Phase 2  WS2 Audit trail               ── needs WS1 (principal binding, audit-access ACL)
Phase 3  WS3 Crown-jewel + WS5 cert    ── independent of WS2; can overlap Phase 2
Phase 4  WS4 SSO                       ── parallelizable from Phase 1 (orthogonal)
Phase 5  WS6 final doc sweep + Gap flip ── only after all code+tests land
```

Hard dependency edges: WS2 → WS1 (audit binds principals + is itself access-controlled);
WS4 → WS1 (IdP groups map to roles). WS3/WS5/WS4 are otherwise independent and can run in
parallel given reviewer bandwidth.

## Test program (whole-plan)

- Per-WS: lib unit + clippy `-D warnings` + ≥1 integration scenario + ≥1 falsification
  probe-as-regression.
- Cross-cutting: full feature matrix incl. new `compliance`; the `fuzz/` targets extended
  to the audit/role decoders (and actually *run* in CI — the standing M2 ledger lesson);
  scale/resilience re-run if WS1/WS2 touch the gossip hot path.
- Acceptance: every Definition-of-Done box ticked; Gap Summary all-Complete; an M2
  analysis run (Run N) recording the flips with execution evidence.

## Decisions — all resolved (2026-06-14)

- **Functional enhancements:** the Definition-of-Done bar, not a separate bucket;
  `linearizable_get` parked out of scope.
- **SSO (WS4):** generic OIDC, no per-vendor code; group→role claim path configurable.
- **Crown-jewel data-at-rest (WS3):** encryption **hooks ship in code** (opt-in KMS/keyring
  adapter), not doc-only.
- **Audit cryptographic chaining (WS2):** **in scope** as the keystone — supersedes the
  older out-of-scope note.
- **Support/SLA sub-gate:** **out of engineering scope** — per ROADMAP sub-gate 4 it is
  explicitly *"commercial work, not engineering"* (SLA tiers, named support relationships,
  escalation paths, reference customers). Handled on the commercial track
  ([[project_compliance_strategy]]); represented here only as a non-engineering gate so the
  "v1.x engineering complete" claim stays honest. WS1–WS6 are the engineering scope.
