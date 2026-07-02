# dev/operations — the diagnostic surface

↑ [dev/](dev.md) · operator runbooks: `docs/operations/` (deployment, observability, tuning, dynamic-scaling…)

## Public HTTP endpoints (`gateway` feature; no auth by design — M16 edge criterion)

| Endpoint | Tells you |
|---|---|
| `GET /health` | process alive |
| `GET /ready` | soft-state advertised + no dead shards |
| `GET /stats` | `node_id`, optional `cluster_name` (`GOSSIP_CLUSTER_NAME` — also a `cluster` global label on `/metrics` and an AgentFacts field), `store_entries`, `dropped_frames`, `task_count`, the tripwire counters (`commit_conflicts`, `sys_namespace_violations`, `cap_authz_violations`, `schema_mismatch`, `rate_limited_senders`), the emergent-detector gauges `governed_group_conflicts` (P1) and `capability_coverage_gaps` (P6), `membership_flaps` (P2), `opacity_oscillations` (P3 — (node,kind) pairs hunting in/out of shed) (Legible-Emergence Phase 1; `0` unless `GOSSIP_EMERGENT_DETECTORS`), and — when detectors are enabled — `opaque_node_pct` (P4 fleet-opacity-storm gauge, 0–100; a raw gauge the operator thresholds) plus a `view_confidence` object (RT1/RT2: a per-node *estimate*, not fleet truth; `peers_heard`/`peers_known`, `max_staleness_ms`, `self_degraded`) |
| `GET /consensus/{slot}` | committed value (lease-aware) + ballot + lease state |
| `GET /metrics` | Prometheus (`metrics` feature). Includes the emergent-detector gauges when `GOSSIP_EMERGENT_DETECTORS` is on: `mycelium_emergent_governed_group_conflicts` (P1), `mycelium_emergent_capability_coverage_gaps` (P6), `mycelium_emergent_membership_flaps` (P2), `mycelium_emergent_opacity_oscillations` (P3), `mycelium_emergent_opaque_node_pct` (P4), and the RT1/RT2 view-health gauges `mycelium_emergent_peers_heard` / `_peers_known` / `_max_staleness_ms` (alert-qualify a diagnostic by the observer's own view: `peers_heard` ≪ `peers_known` ⇒ partial view) |

Diagnostics surface: `GET /gateway/fleet` (scope `fleet:read`) — the Legible-Emergence Phase-2
relational fleet snapshot, computed locally from this node's KV (governed-group status, coverage
gaps, opacity, flap/oscillation counters + the RT1/RT2 `view_confidence` header). Coordinator-free:
any node answers; at convergence the *diagnosis* agrees across nodes.

Governance surface: `POST /gateway/govern/{tuning,membership}` + `GET /gateway/govern`
(deny-by-default scopes, WS2-audited) — see
[management-as-intent](../domain/theory/management-as-intent.md) for the model.

## task_count reference (leak triage)

Steady state after `start()`: 7 core loops (GC, health, anti-entropy, WAL-flush, reorder
buffer, capability heartbeat, group-member sync) + 2 per gossip shard (default 4) + 1
gateway + N per-peer writers + N `bulk_serve` listeners. Typical 3-node baseline **17–20**.
Not tracked: `rpc_call` (no task), `scatter_gather` (local JoinSet), per-request bulk
handlers (semaphore-bounded, visible as `active_bulk_handlers`). Unbounded growth ⇒ suspect
a per-peer writer not exiting on disconnect.

## Feature gates

`cli` (default; tracing-subscriber for binaries) · `gateway` (default; disable for embedded: `default-features = false` — gossip, KV, signals,
consensus, typed handles all remain) · `consensus` (default; drop for minimal embeds — a
consensus-free node still *forwards* PROPOSE/VOTE/COMMIT) · `tls` · `metrics` · `a2a` ·
`llm` · `compliance` (= gateway+tls). `mycelium-core` builds standalone (≈48 deps vs ≈140).

## Framing discipline

Mycelium is a **library, not a platform** — no daemon, no control plane; a cluster is
emergent from network reachability; fleet observability is the operator's stack aggregating
per-node `/metrics` ([deployment-framing](../domain/strategy/deployment-framing.md)).
