# Production Readiness — the go-live checklist

↑ [Operations](README.md)

A single pre-flight that ties the topic guides together. Mycelium is a **library, not a
platform** — "production" means *your* binary embeds `GossipAgent` and you own the deployment. This
checklist is the sweep to run before that binary carries real traffic. Each item links the deep doc;
this page is the index + the gate.

> **How to use it:** walk top to bottom. Every ☐ is either *done*, *consciously waived* (write down
> why), or *blocking*. Nothing here is Mycelium-specific ceremony — it is the same discipline any
> distributed data-plane deserves.

## 1 · Identity & transport security

- ☐ **mTLS on** — build with `--features tls`; every node has an Ed25519 identity and peer connections
  are mutually authenticated. Without it the mesh is unauthenticated (fine for a trusted LAN, **not**
  for production). → [deployment.md §TLS](deployment.md), [cert-rotation.md](cert-rotation.md)
- ☐ **Identity persisted** — set `auto_cert_dir` so a restarted node keeps its identity (no re-issue,
  no signature churn). → [cert-rotation.md](cert-rotation.md)
- ☐ **Rotation rehearsed** — you have run a hot cert/identity rotation with no disruption at least once
  in staging. → [cert-rotation.md](cert-rotation.md)
- ☐ **KV write signing** decided — Ed25519-signed gossip frames (`SignedData`, wire v10+) if writes
  must be attributable/tamper-evident on the wire.

## 2 · Authorization & the gateway edge

- ☐ **Gateway is not open** — the HTTP gateway has **no auth by default**. Bind it to loopback, or set
  `gateway_auth_token`, or front it with the OIDC/OAuth2 ACLs. `/health` `/ready` `/metrics` are
  intentionally public for probes; everything else must be gated. → [rbac.md](rbac.md),
  [sso.md](sso.md)
- ☐ **RBAC / capability authz** configured if multi-tenant — signed role claims + capability ACLs.
  → [rbac.md](rbac.md)
- ☐ **Egress allowlist** set if nodes reach external tool/LLM/MCP servers (WS3 egress gate). →
  [crown-jewel.md](crown-jewel.md)
- ☐ **Data-at-rest** cipher hook wired if the KV/WAL holds sensitive state. → [crown-jewel.md](crown-jewel.md)
- ☐ **Audit trail** enabled if you need a tamper-evident record (`--features compliance`, hash-chained).
  → [audit.md](audit.md)

## 3 · Persistence & restart

- ☐ **Persistence enabled** with a `sync_mode` matched to your durability need; consensus committed
  slots are always fsynced regardless. → [deployment.md §Restart behaviour](deployment.md)
- ☐ **Restart rehearsed** — single-node WAL replay **and** full-cluster cold restart (anti-entropy
  recovery) both verified in staging. A restarted node re-bootstraps with no rejoin ceremony.
- ☐ **Snapshot cadence** (`snapshot_interval_secs`) tuned so replay time is bounded.

## 4 · Sizing & back-pressure (the scale sweep)

- ☐ **Cluster-size knobs set** — pick the profile for your node count (`max_forwarding_peers`,
  `epidemic_extra_peers`, `gossip_shards`, `writer_channel_depth`, intervals). The full sizing tables
  by cluster size (≤20 / 20–100 / 100–1 000 / >1 000) live in
  [tuning.md §Scaling guidelines](tuning.md). Most defaults auto-derive from cluster size (WS-C).
- ☐ **SWIM on** (default) — the UDP failure detector is required past ~100 nodes; legacy TCP-ping
  saturates the connection table well below that. → [tuning.md §Gossip transport modes](tuning.md)
- ☐ **Connection cap** — set `GOSSIP_MAX_ACTIVE_CONNECTIONS` (partial mesh) for large clusters to avoid
  the O(N²) ceiling. **Known ceiling:** the 100-node `make test-scale` formation-within-240s target
  shows Docker-bridge iptables variance (8–94/100 across identical runs) — *environmental*, not a code
  regression; identical gossip/anti-entropy code converges cleanly at 20/30/50 nodes. On real
  hosts/overlay networks this ceiling does not apply, but **rehearse formation at your target scale on
  your real network** before go-live. → [tuning.md](tuning.md)
- ☐ **Ingress rate limit** (`max_inbound_frames_per_sec`) and **channel depths** sized for your burst
  fan-in. → [tuning.md §Hard invariants](tuning.md)
- ☐ **Back-pressure understood** — the `sys/load/` opacity mechanism sheds admission under load; you
  have decided what "overloaded" means for your kinds.

## 5 · Observability & diagnosis

- ☐ **Metrics scraped** — `--features metrics`, Prometheus at `/metrics`, the Grafana dashboard
  imported. → [observability.md](observability.md)
- ☐ **Readiness gate wired** — load balancers probe `GET /ready` (200 only once capabilities gossip +
  peers connect), not just `/health`.
- ☐ **Fleet diagnosis reachable** — `/gateway/explain` + `/gateway/diagnose` (Legible Emergence) are
  enabled and access-gated; an operator can answer "why is the *fleet* in this state" without a central
  collector. → [diagnostics.md](diagnostics.md)
- ☐ **Alerts** — the per-pathology Prometheus alert recipes are loaded. → [diagnostics.md](diagnostics.md)

## 6 · Evolution & supply chain

- ☐ **Wire-version policy understood** — current `WIRE_VERSION = 12`, `PREV_WIRE_VERSION = 11`;
  `read_frame` accepts both for rolling upgrades. A mixed-version cluster during upgrade is supported;
  a two-version jump is not. → `mycelium-core/src/framing.rs` (top).
- ☐ **`cargo audit` clean** — CI gates it (fails on vulnerabilities; unmaintained advisories surface as
  warnings). Re-run against your locked build.
- ☐ **Feature set pinned** — you build exactly the features you run (`tls` / `metrics` / `a2a` / `llm`
  / `compliance` / `gateway`), and `--no-default-features` still builds if you embed the minimal core.

## 7 · The companions you actually use

Each companion crate (`mycelium-tuple-space`, `-blackboard`, `-wiki`, `-wasm-host`, `-agentfacts`) is
built on the public API and has its own gates + worked example. If your deployment uses one:

- ☐ its role/failover model is understood (elected role + capability-ring failover — call `shutdown`
  on teardown for the wiki),
- ☐ its store/persistence dependency is provisioned (the wiki needs a node-independent store — shared
  FS / S3 / doc store — and a membership policy for the access broker),
- ☐ its gateway/SDK surface is access-gated like the core gateway.

→ companion pages under [`docs/wiki/dev/companions/`](../wiki/dev/companions/companions.md).

---

**Next:** for a *first customer engagement* specifically, pair this with the
[customer-pilot.md](customer-pilot.md) checklist — it scopes the engagement and treats the pilot as the
external validation the internal test loop structurally cannot provide.
