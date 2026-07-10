# dev/operations — the diagnostic surface

↑ [dev/](dev.md) · operator runbooks: `docs/operations/` (deployment, observability, tuning, dynamic-scaling…). Before a go-live, the [`production-readiness.md`](../../operations/production-readiness.md) pre-flight ties the topic runbooks into one sweep; for a first customer engagement, [`customer-pilot.md`](../../operations/customer-pilot.md) scopes it as external validation.

## Public HTTP endpoints (`gateway` feature; no auth by design — M16 edge criterion)

| Endpoint | Tells you |
|---|---|
| `GET /health` | process alive |
| `GET /ready` | soft-state advertised + no dead shards |
| `GET /stats` | `node_id`, optional `cluster_name` (`GOSSIP_CLUSTER_NAME` — also a `cluster` global label on `/metrics` and an AgentFacts field), `store_entries`, `dropped_frames`, `task_count`, the tripwire counters (`commit_conflicts`, `sys_namespace_violations`, `cap_authz_violations`, `schema_mismatch`, `rate_limited_senders`, `individual_flood_fallbacks` — the flood-fallback/topology-pressure signal, remedy `connect_peer`), the liveness fields (`dead_shards`, `gc_alive`, `health_monitor_alive`), the emergent-detector gauges `governed_group_conflicts` (P1) and `capability_coverage_gaps` (P6), `membership_flaps` (P2), `opacity_oscillations` (P3 — (node,kind) pairs hunting in/out of shed) (Legible-Emergence Phase 1; `0` unless `GOSSIP_EMERGENT_DETECTORS`), and — when detectors are enabled — `opaque_node_pct` (P4 fleet-opacity-storm gauge, 0–100; a raw gauge the operator thresholds) plus a `view_confidence` object (RT1/RT2: a per-node *estimate*, not fleet truth; `peers_heard`/`peers_known`, `max_staleness_ms`, `self_degraded`) |
| `GET /consensus/{slot}` | committed value (lease-aware) + ballot + lease state |
| `GET /metrics` | Prometheus (`metrics` feature). Includes the emergent-detector gauges when `GOSSIP_EMERGENT_DETECTORS` is on: `mycelium_emergent_governed_group_conflicts` (P1), `mycelium_emergent_capability_coverage_gaps` (P6), `mycelium_emergent_membership_flaps` (P2), `mycelium_emergent_opacity_oscillations` (P3), `mycelium_emergent_opaque_node_pct` (P4), and the RT1/RT2 view-health gauges `mycelium_emergent_peers_heard` / `_peers_known` / `_max_staleness_ms` (alert-qualify a diagnostic by the observer's own view: `peers_heard` ≪ `peers_known` ⇒ partial view). Plus the **consensus/lock** family (needs `consensus`): `mycelium_consensus_timeouts_total{reason}` (event-emitted at every no-quorum exit — `no_voters`=partition / `quorum_short`=overload / `all_opaque` / `empty_groups`; the one series *not* gated on the detector loop) and `mycelium_consensus_commit_conflicts` / `mycelium_schema_mismatch` (gauges mirroring the `/stats` scalars, set on the detector tick). Locks are consensus slots → no per-lock gauge; inspect one via `GET /consensus/lock/{name}` |

Diagnostics surface (Legible Emergence, all `fleet:read`) — the three-verb operator spine, each
computed locally from this node's gossiped KV (coordinator-free: any node answers; at convergence
the *diagnosis* agrees across nodes, while each keeps its own `view_confidence`):

- **localize** — `GET /gateway/fleet`: the relational snapshot (governed-group status, coverage
  gaps, opacity, throttle graph, cross-node store-convergence, commit-conflict hot slots + the
  RT1/RT2 `view_confidence` header).
- **explain** — `GET /gateway/explain?since=`: the cross-node HLC-ordered event narrative,
  best-effort fan-out (**capped** at `EXPLAIN_MAX_FANOUT` peers so an operator query never becomes an
  O(N) RPC storm) that *names* both the peers that did not answer (`non_responders`) and the count
  skipped by the cap (`not_queried`) rather than silently dropping them (RT3).
- **diagnose** — `GET /gateway/diagnose`: the "why is the fleet in this state" rule engine — a
  most-severe-first list of findings, each naming a pathology in actionable terms, with an RT1/RT2
  `caveat` when the observer's own view is partial.

All three are also programmatic (**diagnostics as data**): `agent.fleet_snapshot()` /
`fleet_diagnosis()`. Operator runbook (one entry per pathology + Prometheus alert recipes):
[operations/diagnostics.md](../../operations/diagnostics.md).

Governance surface: `POST /gateway/govern/{timing,tuning,membership}` + `GET /gateway/govern`
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
