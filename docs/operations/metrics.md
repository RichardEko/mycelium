# Metrics reference

↑ [Operations](README.md)

The canonical, complete list of every Prometheus metric a Mycelium node emits.
[observability.md](observability.md) covers *how* to scrape; this is *what* the
series mean and *what to watch for*.

**What emits them.** All series appear on the node's `GET /metrics` endpoint, which
exists only when the node is built with the **`metrics`** feature (it installs the
Prometheus recorder — without it every emit below is a no-op). Some families come from
companion crates (`mycelium-wasm-host`, `mycelium-guardrails`, `mycelium-reason`) whose
metric calls are always compiled but stay silent until *the node* has that recorder; a
couple additionally need another feature or an env toggle, called out per family. Every
series carries a `cluster="<name>"` label when `GOSSIP_CLUSTER_NAME` is set
([observability.md](observability.md#naming-environments--monitoring-many-clusters)), and
Prometheus adds the usual `instance` label per node — so `sum by (cluster) (…)` aggregates
a fleet and `by (instance)` breaks it down per node.

The families:

| Family | Prefix | Source crate | Needs beyond `metrics` |
|---|---|---|---|
| [Gossip transport](#gossip-transport) | `gossip_*` | `mycelium-core` | — |
| [Emergent / diagnosis](#emergent--diagnosis) | `mycelium_emergent_*` | node (`src/agent/emergent.rs`) | detector loop on |
| [Governor](#governor) | `mycelium_governor_*` | node (`src/agent/tuning_governor.rs`) | — |
| [Artifact library](#artifact-library) | `mycelium_artifact_*` | `mycelium-wasm-host` | — |
| [Guardrails](#guardrails) | `mycelium_guardrails_*` | `mycelium-guardrails` | `compliance` (the Tier-C gate) |
| [Reason routing](#reason-routing) | `mycelium_reason_route_*` | `mycelium-reason` | — |

---

## Gossip transport

The hot-path layer-I/II counters and gauges from `mycelium-core`. The
[Grafana dashboard](../../dashboards/mycelium-grafana.json) visualises this whole family.

| Metric | Type | Labels | Meaning | Watch for / alert |
|---|---|---|---|---|
| `gossip_store_entries` | gauge | — | live KV keys held by this node | unbounded growth = tombstones not GC'd or a writer looping (mirrors `/stats` `store_entries`) |
| `gossip_kv_writes_total` | counter | — | KV writes applied | a sudden spike locates a hot writer |
| `gossip_kv_deletes_total` | counter | — | KV deletes (tombstones) applied | — |
| `gossip_anti_entropy_rounds_total` | counter | — | Merkle anti-entropy reconciliation rounds run | flat-lining while peers are up = anti-entropy stalled |
| `gossip_messages_received_total` | counter | — | gossip frames received from peers | zero on a node that should have peers = isolation/partition |
| `gossip_frames_dropped_total` | counter | — | inbound frames dropped (backpressure, oversize `FrameTooLarge`, or reconnect) | sustained growth = raise `GOSSIP_WRITER_CHANNEL_DEPTH` ([tuning.md](tuning.md)); mirrors `/stats` `dropped_frames` |
| `gossip_signals_emitted_total` | counter | `scope` | signals emitted, by admission scope | — |
| `gossip_signals_delivered_total` | counter | `kind` | signals delivered to a local subscriber, by kind | — |
| `gossip_signals_rejected_total` | counter | — | signals rejected (admission scope / load shedding) | a rising rate = admission or opacity is suppressing signals |
| `gossip_rpc_latency_ms` | histogram | — | request→response RPC latency (ms) | use `histogram_quantile(0.99, …_bucket)`; p99 climbing = peers slow/opaque |

## Emergent / diagnosis

Node-local pathology gauges for the Legible-Emergence diagnosis surface. Each is the
alertable scalar behind a [diagnostics.md](diagnostics.md) pathology; the snapshot
(`/gateway/fleet`) carries the relational detail and `/gateway/diagnose` the action.

**Emitted only when the detector loop runs** — `GossipConfig::emergent_detectors_enabled`
(env `GOSSIP_EMERGENT_DETECTORS=1`), off by default. Without the loop these gauges are
absent (the snapshot/diagnosis still work; see
[diagnostics.md §Turning it on](diagnostics.md#turning-it-on)). All are gauges with no
extra labels.

| Metric | Type | Meaning | Watch for / alert |
|---|---|---|---|
| `mycelium_emergent_governed_group_conflicts` | gauge | groups whose live membership is outside the governor band | → [governed-group conflict / thrash](diagnostics.md#governed-group-conflict--thrash-the-56-pattern) |
| `mycelium_emergent_membership_flaps` | gauge | governed groups whose membership is *flapping* (`>0` with a conflict ⇒ thrash) | → [same recipe](diagnostics.md#governed-group-conflict--thrash-the-56-pattern) — the thrash escalation |
| `mycelium_emergent_opaque_node_pct` | gauge | percent of nodes opaque (overloaded / shedding); ≥ 34 = storm | → [fleet-opacity storm](diagnostics.md#fleet-opacity-storm) |
| `mycelium_emergent_capability_coverage_gaps` | gauge | demands (`req/…`) with no fresh provider visible from here | → [capability-coverage gap](diagnostics.md#capability-coverage-gap) |
| `mycelium_emergent_opacity_oscillations` | gauge | node/kind pairs flipping in/out of overload (unstable back-pressure) | → [opacity oscillation](diagnostics.md#opacity-oscillation) |
| `mycelium_emergent_max_staleness_ms` | gauge | oldest peer view this observer holds (ms) | high = this observer's inputs are stale; qualifies its diagnoses |
| `mycelium_emergent_peers_heard` | gauge | peers this observer is currently hearing | pair with `_peers_known` to qualify a partial view (RT1/RT2) |
| `mycelium_emergent_peers_known` | gauge | peers this observer knows exist | `_peers_heard ≪ _peers_known` = partial view → cross-check from another node |

The full per-pathology PromQL alert recipes (including the `_peers_heard`/`_peers_known`
partial-view qualifier) live in
[diagnostics.md §Prometheus alert recipes](diagnostics.md#prometheus-alert-recipes) — not
duplicated here.

## Governor

Effective per-node state of the WS-C tuning governor (`/gateway/govern`), re-emitted after
every mutation and reconcile so a scrape always reflects the live governor without polling
the HTTP snapshot. See [dynamic-scaling.md](dynamic-scaling.md).

| Metric | Type | Labels | Meaning | Watch for / alert |
|---|---|---|---|---|
| `mycelium_governor_auto_enabled` | gauge | — | `1` if auto-governing is enabled on this node, else `0` | a node reading `0` you expected to auto-scale is pinned off |
| `mycelium_governor_floor` | gauge | `param` | governed floor for the hot param (`0` = no floor) | — |
| `mycelium_governor_ceiling` | gauge | `param` | governed ceiling (`-1` = unbounded) | a low ceiling capping a param under load explains a scaling stall |
| `mycelium_governor_ratchet` | gauge | `param` | ratchet direction/state (encoded `u8`) | — |
| `mycelium_governor_locally_pinned` | gauge | `param` | `1` if this param is locally pinned (a local pin wins over fleet intent) | a pinned param won't follow a `/gateway/govern` intent — expected, but explains "why didn't it move" |

## Artifact library

Install lifecycle + librarian metrics for the durable artifact library, emitted by
`mycelium-wasm-host` on the node that hosts/provisions artifacts. See
[artifacts.md](artifacts.md). All counters.

| Metric | Type | Labels | Meaning | Watch for / alert |
|---|---|---|---|---|
| `mycelium_artifact_installs_started_total` | counter | — | install attempts begun on this node | — |
| `mycelium_artifact_installs_completed_total` | counter | — | installs that finished successfully | started ≫ completed+failed = installs wedged mid-flight |
| `mycelium_artifact_installs_failed_total` | counter | `stage` | installs that errored, labelled by the failing stage | a rising rate at one `stage` pinpoints where provisioning breaks |
| `mycelium_artifact_ineligible_skips_total` | counter | `reason` | eligibility skips: `reason` ∈ `no_runtime` / `budget` / `memory` / `disk` | rising `memory`/`disk` = this node can't host what the fleet is asking it to; `no_runtime` = missing runtime for the artifact kind |
| `mycelium_artifact_librarian_published_total` | counter | — | artifacts the local librarian published into the durable library | — |
| `mycelium_artifact_librarian_tombstoned_total` | counter | — | library entries the librarian tombstoned (retired) | — |
| `mycelium_artifact_probe_withdrawals_total` | counter | — | install probes withdrawn (a candidate backed out before committing) | a steady stream = artifacts repeatedly probing then withdrawing (resource churn) |

## Guardrails

Tier-C invoke-time gate counters from `mycelium-guardrails` (guide
[16 · Guardrails](../guide/16-guardrails.md)). The gate — and its tamper-evident denial
seal — needs the **`compliance`** feature (which pulls `tls`); the metric emit itself is
unconditional but only fires when the gate runs. Both counters.

| Metric | Type | Meaning | Watch for / alert |
|---|---|---|---|
| `mycelium_guardrails_admits_total` | counter | callers the Tier-C gate admitted (authorized invocations) | — (the healthy baseline) |
| `mycelium_guardrails_denials_sealed_total` | counter | unauthorized invocations the gate stopped (and sealed as an `Invoke`/`Denied` audit record) | **a rising value = unauthorized invocations are being stopped fleet-wide** — investigate the caller; each denial is provable via [audit.md §7 · Proving a guardrail stopped an agent](audit.md#7-proving-a-guardrail-stopped-an-agent) |

## Reason routing

Inference-router counters from `mycelium-reason` (`route.rs`) — the call side that picks a
model provider and fails over. All counters.

| Metric | Type | Meaning | Watch for / alert |
|---|---|---|---|
| `mycelium_reason_route_attempts_total` | counter | routing attempts (a provider was selected and tried) | — (the baseline) |
| `mycelium_reason_route_failovers_total` | counter | attempts that failed over from one provider to the next | **a rising value = inference providers dying or going opaque** — the router is working around them; check which providers dropped out |
| `mycelium_reason_route_no_provider_total` | counter | routes that found no eligible provider at all | rising = no provider advertises the requested capability — a coverage gap in inference |
| `mycelium_reason_route_exhausted_total` | counter | routes that tried every provider and still failed | rising = *all* providers for a request are failing — an inference outage, not a single dead node |

---

*Alert recipes for the emergent family: [diagnostics.md](diagnostics.md#prometheus-alert-recipes).
Reading `/stats` tripwires (not Prometheus): [observability.md](observability.md#reading-stats).*
