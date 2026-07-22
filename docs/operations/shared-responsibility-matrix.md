# Shared-responsibility matrix (SOC 2 / adopter-facing)

↑ [operations](README.md)

> **What this is.** Mycelium is an **embedded library, not a hosted service** — there is no
> vendor-run control plane. A SOC 2 report is issued to a *service organisation* about an *operated
> system*; the operated system is **your deployment**, so **your** SOC 2 covers your use of Mycelium.
> This table says, per control area, what the **library provides** (a control you inherit and
> configure), what is **shared**, and what **you own** — so "deployer-owned" reads as a deliberate
> boundary, not a gap. It is the evidence artifact your auditor and your customers' security reviews
> ask for on day one.
>
> Mycelium is **CFT, not BFT**: a compromised *admitted* node is formally out of scope (see
> [threat-model](../threat-model.md)). Controls below are "detection-not-prevention" unless noted —
> enforcement happens where a resource is *served*, never as a Layer-I write guard.

Legend: **M** = Mycelium provides (inherit + configure) · **S** = Shared · **D** = Deployer owns ·
⚠ = open gap, tracked in [soc2-audit-gap-closure](../plans/soc2-audit-gap-closure.md).

## The org-level criteria (CC1–CC5, CC9) — not the library's to hold

CC1 Control Environment · CC2 Communication · CC3 Risk Assessment · CC4 Monitoring · CC5 Control
Activities · CC9 Risk Mitigation are **organisational** — your governance, HR, vendor management,
risk register, policies, and the observation-window evidence that they *operated*. **D** across the
board. Mycelium contributes only as a well-documented **vendor** in your CC9 vendor-risk process
(see the vendor-assurance package: a corporate/SDLC SOC 2 + pentest + this matrix).

## CC6 — Logical & physical access controls (the meaty one)

| Control | Who | Mycelium provides | You own / configure |
|---|---|---|---|
| Node authentication | **M** | mTLS mutual admission against a cluster CA; Ed25519 node identity (`tls`) | Distribute + protect the CA; file-protect on-disk keys |
| Peer-identity integrity | **S** ⚠ | Retained-key set; anchor primitive (`ed25519_key_from_cert_der`) | `sys/identity` is not yet authenticated (key-poisoning by a compromised/buggy admitted node) — **WS-E** in progress; until then, treat identity integrity as CFT-bounded |
| Operator authentication (SSO) | **M** | OIDC/JWT validation, alg-confusion-safe (`compliance`) | Wire your IdP; `group_scopes` mapping |
| Authorization (RBAC) | **M** | Signed role claims + L1/L2/L3 clearance; forged KV role reads back `None` | Issue/rotate role claims; set clearances |
| Capability authorization | **M** | `authorized_callers` allowlists; resolve-time `capauthz` (route around unauthorised advertisers) | Define policies; empty = open by design |
| Gateway access control | **M** | OAuth2-style `resource:verb` scopes, **deny-by-default** (unmapped route ⇒ `admin`) | Set `gateway_scoped_tokens` / OIDC scopes; keep `/health /ready /metrics` the only public routes |
| Encryption in transit (gossip) | **M** | mTLS on the gossip TCP transport (`tls`) | Enable `tls` (opt-in) |
| Encryption in transit (gateway) | **S** | **Native gateway TLS** (`gateway_tls`, WS-A ✅) **or** front with a TLS proxy | Pick one — never leave the gateway plaintext on a routable interface. → [gateway-tls](gateway-tls.md) |
| Encryption in transit (SWIM UDP) | **D** | Liveness only (no KV/data); plaintext, unauthenticated | Network-segment it if your policy requires |
| Key management — rotation | **M** | Hot Ed25519 identity/cert rotation, no dropped frames | Operate the rotation cadence |
| Key management — compromise | **M** | Signed revocation consulted on all verify paths incl. consensus; `rotate_identity_on_compromise` + `POST /gateway/identity/revoke` (WS-B ✅) | Trigger it on compromise. Force-revoking a *dead/fully-compromised* node's key needs a separate operator authority (not provided). → [cert-rotation](cert-rotation.md) |
| Key custody (CA, at-rest KMS) | **D** | `DataAtRestCipher` hook (disk boundaries) | CA-key custody; wrap a KMS/HSM for at-rest |
| Encryption at rest | **S** | Optional at-rest cipher hook (WS3) | Supply the cipher + key custody |

## CC7 — System operations (monitoring, detection, response)

| Control | Who | Mycelium provides | You own |
|---|---|---|---|
| Security tripwires | **M** | `sys_namespace_violations`, `cap_authz_violations`, `commit_conflicts`, `schema_mismatch` on `/stats` (feature-free) + Prometheus | Alert on them (esp. namespace-violation > 0) |
| Metrics / observability | **M** | Prometheus `/metrics` (`metrics`); emergent-pathology + view-health gauges | Scrape; Grafana; alert recipes → [diagnostics](diagnostics.md) |
| Coordinator-free diagnosis | **M** | `/gateway/{fleet,explain,diagnose}` — plain-English fleet findings | Consume in your ops flow |
| Audit trail | **M** | Per-node hash-chained, Ed25519-signed records + verification API (`compliance`) | Requires `tls` to seal; **instrument application-level events yourself** |
| Audit export | **S** ⚠ | In-cluster chain is a hot window; **export sink** is **WS-C** (pending) | Stream to your SIEM/WORM; today via `/gateway/audit` polling |
| Audit retention | **S** ⚠ | Unbounded, no pruning yet; **checkpointing** is **WS-D** (pending) | Size the store; export-then-archive for 7-yr-class retention |
| Incident response | **D** | Signals + revocation as building blocks | Your IR runbooks, on-call, drills |

## CC8 — Change management

| Control | Who | Mycelium provides | You own |
|---|---|---|---|
| Wire/protocol change | **M** | `WIRE_VERSION` + documented N→N+1 rolling-upgrade policy, test-gated | Roll upgrades within the window |
| Schema evolution | **M** | Registered migrations (`schema_evolution`), never silent coercion; `schema_mismatch` tripwire | Register your migrations |
| Application change management | **D** | — | Your SDLC, approvals, deploy pipeline |

## Confidentiality & Availability (optional TSC categories)

| Control | Who | Mycelium provides | You own |
|---|---|---|---|
| Data classification | **M** | L1/L2/L3 clearance in RBAC | Classify + assign |
| Data isolation | **S** | Single cluster **isolated by construction** (no cross-cluster feature) | One cluster = one confidentiality/residency boundary; run per-tenant/region as needed |
| Data residency | **D** | Isolation property + docs | **Where nodes run is yours** — placement, borders |
| Right to erasure (GDPR) | **S** ⚠ | Tombstones; at-rest hook | **WS-F** ([design](../design/data-lifecycle-and-erasure.md)): crypto-shred (destroy the per-subject key) — physical erasure isn't guaranteeable in a gossip+WAL mesh; reference helper pending |
| Availability | **S** | AP design (partition-tolerant), epidemic redundancy, anti-entropy repair | Capacity, backups, multi-AZ, DR |
| Egress control | **S** | `EgressPolicy` fail-closed allowlist (per node) | Set `allow_hosts`; network-layer enforcement |

## How to read the ⚠ rows

Each open gap is scoped, sequenced, and sized in
[soc2-audit-gap-closure](../plans/soc2-audit-gap-closure.md). Status at last update
(2026-07-22): **WS-A gateway TLS shipped**; WS-B (revocation glue), WS-C/D (audit export +
retention), WS-E (identity authentication), WS-F (erasure) in progress/pending. This matrix's cells
flip as each lands — treat it as the living definition of done.
