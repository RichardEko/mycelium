# Observability

What a running Mycelium cluster exposes, and how to see what it's doing — without
modifying it. Everything here needs the gateway on (`http_port` set; see
[deployment.md](deployment.md)).

> Audience: **DevOps**. Developer-side inspection (reading any layer's KV state
> from code) is in the [cookbook](../guide/cookbook.md).

## Public endpoints (no auth)

These are deliberately *outside* the `/gateway` scope wall — always public,
uncredentialed:

| Endpoint | Tells you |
|---|---|
| `GET /health` | `200` = process alive (liveness probe) |
| `GET /ready` | `200` = startup complete → serving KV/signals/membership (readiness probe). Capability discovery gossips independently and does **not** gate readiness — a node advertising no soft state is still ready (changed 2026-07-15) |
| `GET /stats` | `node_id`, `cluster_name`, `store_entries`, `dropped_frames`, `task_count`, and the tripwire counters (`commit_conflicts`, `sys_namespace_violations`, `cap_authz_violations`, `schema_mismatch`, `rate_limited_senders`, `individual_flood_fallbacks`), plus liveness (`dead_shards`, `gc_alive`, `health_monitor_alive`) |
| `GET /metrics` | Prometheus scrape (requires the `metrics` feature). Carries a `cluster` label on every series when `cluster_name` is set |
| `GET /.well-known/agent-facts.json` | this node's self-certified AgentFacts (when the [facts lens](#viewing-agentfacts) is mounted) |
| `GET /consensus/{slot}` | a consensus slot's committed value + ballot + lease state |

### Reading `/stats`

```bash
curl -s http://node:8080/stats | jq
```

- `store_entries` — live KV keys. Steady within an order of magnitude of your
  working set; unbounded growth means tombstones aren't being GC'd or a writer is
  looping.
- `dropped_frames` — backpressure. Non-zero after a burst is informative, not
  fatal; sustained growth means raise `GOSSIP_WRITER_CHANNEL_DEPTH` ([tuning.md](tuning.md)).
- `task_count` — Tokio tasks in the JoinSet (~17–20 on a 3-node cluster).
  Unbounded growth = a task leak (usually a per-peer writer not exiting).
- `individual_flood_fallbacks` — Individual-scoped frames (RPC, votes) that had **no direct
  route** and fell back to flooding. Persistent growth on a stable topology means an
  RPC-heavy pair isn't directly peered — the node also logs
  `Individual-scoped frame has no direct route; flooding via relay (topology pressure …)`.
  Remedy: pin the pair with `GossipAgent::connect_peer` — see
  [tuning §RPC-heavy pairs](tuning.md#rpc-heavy-pairs--the-topology-pressure-warn).
- `commit_conflicts` / `sys_namespace_violations` — the **detection-not-prevention
  tripwires**. A non-zero value means someone wrote to a `consensus/` slot or a
  `sys/{identity,load,role,tuple}/{node}` key they don't own; the write was
  *applied* per LWW but flagged. Investigate the offending node — see
  [00 · Concepts](../guide/00-concepts.md) on promise- vs mechanism-strength.

### Prometheus

With `--features metrics`, `/metrics` exposes the gossip hot-path gauges/counters
(`gossip_store_entries`, `gossip_anti_entropy_rounds_total`,
`gossip_messages_received_total`, `gossip_signals_delivered_total` /
`_rejected_total`, `gossip_rpc_latency_ms`, …) plus the emergent-diagnosis, governor,
artifact-library, guardrails, and reason-routing families. Scrape it like any service.

**Full reference → [metrics.md](metrics.md)** — the canonical, complete list of every
emitted series (type, feature, meaning, and what to watch for), one section per family.
The gossip names above are just a teaser.

### A note on the `system` scope name

Signals and consensus proposals carry a **scope** — `Cluster`, `Group`, or `Individual` (you'll see
it as the `scope` label on `gossip_signals_*` and as the `scope` field on a gateway signal emit).
**`Cluster` was called `System` before 2026-07-10.** The wire is unchanged and the gateway + SDKs
still accept `"system"` as a deprecated alias, so an old dashboard, config, or client that says
`"system"` still works and means `Cluster`. (`system_stats()` is unrelated — that is node-local
runtime state, not a scope.) Full vocabulary:
[guide 13 · Scope vocabulary](../guide/13-cluster-topology.md#scope-vocabulary-cluster--group--individual).

## Naming environments & monitoring many clusters

**A cluster has no built-in name** — a node is identified by its `node_id`
(`host:port`). For org-wide monitoring across several Mycelium environments, give
each cluster a name so one Prometheus / Grafana can tell them apart:

```bash
GOSSIP_CLUSTER_NAME=prod-eu    # or set GossipConfig::cluster_name
```

**`cluster_name` is a label, not a boundary** — it disambiguates dashboards; it does *not* define
membership (that's reachability + the CA). See
[guide 13 · what makes a cluster](../guide/13-cluster-topology.md#what-actually-makes-a-node-part-of-a-cluster).

## Monitoring a group (vs the whole cluster)

Cluster-wide health is `/stats` + `/metrics` + `/gateway/fleet` (above). For a **single group**:

- **Membership** — `agent.mesh().group_members("nlp")` → live members (`Vec<NodeId>`); for a
  capability group, `agent.capabilities().resolve(&filter)` lists matching providers.
- **Size vs. intent** — a **governed** group (under a `MembershipGovernor`) surfaces
  `GroupStatus { min, max, observed, conflict }` on `GET /gateway/fleet` (`governed_groups`) and as
  the `mycelium_emergent_governed_group_conflicts` gauge → alert on it (see
  [diagnostics.md](diagnostics.md#governed-group-conflict--thrash-the-56-pattern)).
- **Ungoverned groups have no per-group gauge yet** — scrape `group_members().len()` via your own
  exporter if you need it, or put the group under a governor. (Tracked as a metrics gap.)

Full how-to (define + monitor): the cookbook recipe
[*"define a group, and monitor who's in it"*](../guide/cookbook.md#how-do-i-define-a-group-and-monitor-whos-in-it).

When set, the name is a **pure operator label** (no effect on gossip, identity, or
membership) and propagates to all three surfaces:

- `GET /stats` → a `cluster_name` field;
- `GET /metrics` → a `cluster="prod-eu"` **label on every series** (so a single
  Prometheus scraping many clusters can `sum by (cluster) (…)` and one Grafana
  dashboard variable selects the environment);
- AgentFacts (`.well-known/agent-facts.json`) → a `cluster` field, so a federation
  fetcher sees which environment a node belongs to.

**Mycelium ships no fleet console — by design** (it is a library, not a platform;
each cluster is self-contained). Fleet-wide aggregation is your monitoring stack's
job: point one Prometheus (or Thanos / a federated Prometheus / Datadog) at every
cluster's `/metrics`, label by `cluster`, and build per-environment dashboards.
Mycelium's responsibility ends at *exposing* the per-node signals with a cluster
label; aggregating them across clusters is the operator's.

## Auth on the monitoring endpoints

`/health`, `/ready`, `/stats`, `/metrics` are **deliberately public and
uncredentialed** — probes and scrapers must work without secrets (the M16 edge
criterion). Mycelium does **not** authenticate them. If your environment requires
auth or network isolation on `/metrics` or `/stats`, put it in front: a reverse
proxy (nginx/Envoy basic-auth or mTLS), a Kubernetes `NetworkPolicy`, or a service
mesh. *Management* surfaces (`/gateway/**` — governance, audit, transparency) are a
different story: those are scope-gated and OIDC-SSO-capable — see [rbac.md](rbac.md)
and [sso.md](sso.md).

## Viewing AgentFacts

AgentFacts is the node's self-certified, federation-facing metadata (see
[00 · Concepts](../guide/00-concepts.md), the developer how-to in
[17 · Federation](../guide/17-federation.md) — serve/verify + the domain board — and the
[`federation_facts`](../../examples/coop/src/bin/federation_facts.rs) demo). Mount
the lens (from the `mycelium-agentfacts` crate) before `start`:

```rust
agent.with_http_routes(mycelium_agentfacts::agent_facts_router(agent.clone(), opts));
```

Then anyone can pull and verify it:

```bash
# this node's signed facts (a NANDA-shaped JSON-LD document)
curl -s http://node:8080/.well-known/agent-facts.json | jq

# the converged, multi-author domain board (every node's verified facts)
curl -s http://node:8080/.well-known/agent-facts/domain.json | jq '.nodes[].node'
```

The document is self-signed (Ed25519) — a fetcher verifies it against the
embedded key; trust is the fetcher's decision (no issuer authority). The
[coop demos](../../examples/coop/) all mount the lens, so while any of them runs
you can `curl` the printed gateway port to inspect it live.

## Dashboards

**A Grafana dashboard ships in-repo:**
[`dashboards/mycelium-grafana.json`](../../dashboards/mycelium-grafana.json). Import it
into Grafana (it prompts for a Prometheus datasource) for KV-store, gossip-transport,
signal-mesh, and RPC-latency panels over the `gossip_*` family. It is a **starting point**,
not the whole picture — it covers the gossip hot path plus a header row of emergent /
guardrails / reason panels; the *full* metric set (governor, artifact library, and the rest)
is in **[metrics.md](metrics.md)**, ready to add as panels. Point its datasource at a
Prometheus scraping every cluster's `/metrics` and use the `cluster` label as a dashboard
variable to switch environments.

For a quick visual without Grafana: the `llm_agent` / `three_node_demo` examples ship a
live management UI (a mesh view + KV inspector) on their gateway port — the quickest visual
into a running cluster. SkillRunner exposes `/mgmt` (the audit + skill dashboard); see
[guide 05 · Skills](../guide/05-skills.md).

## Logs & tracing

Mycelium uses `tracing`. Set `RUST_LOG=mycelium=info` (or `debug`). Build with
`--features otel` and configure `[skill.otel]` for Jaeger/Grafana spans per
invocation. Failure paths log with actionable context — the tripwire warnings
above, `SignedData from unknown signer`, `Individual-scoped frame dropped`, etc.,
each name the cause (and several are [patterns-chapter](../guide/14-patterns-and-pitfalls.md)
entries).
