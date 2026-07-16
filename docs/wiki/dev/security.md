# dev/security — the v1.x security surface (WS1–WS5) + crown-jewel posture

↑ [dev/](dev.md) · canon: `docs/threat-model.md` · runbooks: `docs/operations/{rbac,sso,audit,cert-rotation,crown-jewel}.md`

Everything here obeys the **detection-not-prevention / promise-strength** posture
([runtime-invariants](architecture/runtime-invariants.md)): enforcement happens where a
resource is *served*, never by teaching Layer I a higher-layer law. All shipped
(v1.x engineering complete); plan record: `docs/plans/v1x-completion.md`.

## WS1 — RBAC / identity (`compliance` feature)

Four layers, all additive/opt-in (`src/agent/rbac.rs`, gateway middleware in
`src/agent/http.rs`):
1. **Signed role claims:** `advertise_roles` writes an Ed25519 `SignedRoleClaim` to
   `sys/role/{node}`; `roles_of` returns it only if the signature verifies against the
   node's identity key learned from the cluster — a forged KV write reads back `None`.
2. **Provider-side capability authz:** `caller_authorized` enforces
   `authorized_callers` at the served path (the one place it's genuinely enforceable).
3. **OAuth2 scope gateway ACLs:** `gateway_scoped_tokens` maps bearer→`resource:verb`
   scopes; deny-by-default (unmapped route ⇒ `admin`). `/health|/ready|/stats|/metrics`
   stay public (M16 edge criterion).
4. **`sys/` namespace tripwire (core, feature-free):** inbound writes naming *self* under
   `sys/identity|load|role|tuple/{node}` → `warn!` + `sys_namespace_violations`. Detection
   only — never make it a write guard.

**WS4 OIDC SSO** (`src/agent/oidc.rs`): JWT validated against IdP JWKS, groups→scopes into
the same gate. Alg-confusion-safe (asymmetric-only allowlist *before* key selection);
iss/aud/exp checked; JWKS cached with refresh-on-unknown-kid. Human-operator auth, not agent
identity.

## WS2 — tamper-evident audit (`compliance`)

Per-node hash-chained signed records at `sys/audit/{node}/{seq:016x}` (a global chain would
need a sequencer = coordinator). `SignedAuditRecord` = Ed25519 over canonical bytes;
`verify_chain` returns a precise error naming the offending seq. Sealing holds lock #8 only
for seq/hash/head (~µs); signing and the KV write happen after release. `GET /gateway/audit`
(scope `audit:read`). Records are plain KV — tampering fails verification, is never blocked.

## WS3 — crown-jewel posture (feature-free)

Threat frame: the twin/fleet-state is the concentrated SPOF map; the question regulated
buyers ask is *blast radius*, not "is it secure" (brokered competitors can't answer it —
their broker IS the crown jewel). Two opt-in controls:
- **`DataAtRestCipher`** hook (`src/persistence.rs`) at the four on-disk boundaries (WAL
  append/replay, snapshot write/read). Key custody is the operator's (wrap a KMS); scope is
  disk only.
- **`EgressPolicy { allow_hosts }`** — enforced at every outbound HTTP path the substrate
  chooses (MCP bridge, capability probes, LLM backends, SkillRunner). Fail-closed on
  unparseable hosts.

## WS5 — hot cert/identity rotation (`tls`)

`NodeTls` contents live behind `ArcSwap` (read via accessors per connection — never cache a
config past a rotation; no listener drain-swap needed). `rotate_identity`: generate →
publish `sys/identity/{self}` = `new‖old` → wait → activate.
**Retained-key verification (option B):** `peer_keys` accumulates a per-node key set
(union via `merge_peer_keys` — see [concurrency](concurrency/lock-free-and-atomics.md));
every verify path tries the set. Caveat: a retired key still verifies — compromise needs
explicit **revocation** (WS-D shipped the CT-style revocation log + `/gateway/transparency`
inclusion proofs, PRs #77–#82; revocation is now also applied on the consensus verify path,
audit 2026-07-15 pass 3).

> **`sys/identity` is NOT authenticated (open gap, tracked).** Despite the older "signed by the
> old key" phrasing, `sys/identity/{node}` is a plain Layer-I KV value with **no signature**, and
> `merge_peer_keys` **accumulates** any key that appears there. So a compromised — or merely buggy
> — admitted node can LWW-poison a peer's verifying-key set for a victim, defeating the pass-2
> `signer_authorized` bind. This is a **Byzantine-insider** vector, formally outside CFT-not-BFT,
> but worth closing as defense-in-depth (the signing layer exists precisely to add insider-
> resistance). Full phased fix — CA-cert anchor → signed identity proofs → rotation chained to a
> prior trusted key — designed in
> [`docs/design/identity-authentication.md`](../../design/identity-authentication.md).
