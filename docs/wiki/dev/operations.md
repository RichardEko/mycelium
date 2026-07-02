# dev/operations — the diagnostic surface

↑ [dev/](dev.md) · operator runbooks: `docs/operations/` (deployment, observability, tuning, dynamic-scaling…)

## Public HTTP endpoints (`gateway` feature; no auth by design — M16 edge criterion)

| Endpoint | Tells you |
|---|---|
| `GET /health` | process alive |
| `GET /ready` | soft-state advertised + no dead shards |
| `GET /stats` | `node_id`, optional `cluster_name` (`GOSSIP_CLUSTER_NAME` — also a `cluster` global label on `/metrics` and an AgentFacts field), `store_entries`, `dropped_frames`, `task_count`, the tripwire counters (`commit_conflicts`, `sys_namespace_violations`, `cap_authz_violations`, `schema_mismatch`, `rate_limited_senders`), the emergent-detector gauge `governed_group_conflicts` (Legible-Emergence Phase 1; `0` unless `GOSSIP_EMERGENT_DETECTORS`), and — when detectors are enabled — a `view_confidence` object (RT1/RT2: a per-node *estimate*, not fleet truth; `peers_heard`/`peers_known`, `max_staleness_ms`, `self_degraded`) |
| `GET /consensus/{slot}` | committed value (lease-aware) + ballot + lease state |
| `GET /metrics` | Prometheus (`metrics` feature) |

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
