# Cookbook — "How do I…?"

Task-oriented answers, each pointing at a runnable demo and the chapter that goes
deeper. Read [00 · Concepts](00-concepts.md) first for the vocabulary
(Capability, Skill, Signal, Artifact vs. A2A/MCP/AgentFacts).

> Audience: **Solution/Dev** (embedding the library, building agents). The
> operator's "how do I deploy / observe / scale" answers are in
> [docs/operations/](../operations/) — cross-linked below.

---

### How do I run an example and see it running?

Every coop demo is a single self-contained process — no Docker, no API key:

```bash
cargo run -p mycelium-coop-examples --bin mailbox_llm
```

It prints its gateway port; while it runs you can inspect it live:

```bash
curl -s http://127.0.0.1:<printed-port>/.well-known/agent-facts.json | jq
```

Run the whole suite Docker-free with [`examples/coop/ci_smoke.sh`](../../examples/coop/ci_smoke.sh).
The [example portfolio](README.md#example-portfolio) lists what each one teaches.

### How do I embed Mycelium in my service?

Pick the crate ([guide intro](README.md#which-crate--mycelium-vs-mycelium-core)): `mycelium` (full) or
`mycelium-core` (Layers I+II only). Then:

```rust
let agent = Arc::new(GossipAgent::new(NodeId::new("0.0.0.0", 7946)?, cfg));
agent.start().await?;
// … use agent.kv(), agent.mesh(), agent.capabilities(), agent.consensus(), agent.service() …
agent.shutdown().await;
```

Production concerns (ports, seeds, TLS, restart): [operations/deployment.md](../operations/deployment.md).

### How do I advertise what a node can do, and find providers?

```rust
let _reg = agent.capabilities().advertise_capability(Capability::new("nlp","summarize"), ttl);
let providers = agent.capabilities().resolve(&CapFilter::new("nlp","summarize"));
```

Dropping `_reg` retracts it; it also evaporates if the node dies. Demos:
[`provisioning`](../../examples/coop/src/bin/provisioning.rs),
[`stigmergy`](../../examples/coop/src/bin/stigmergy.rs). Chapter:
[02 · Capabilities](02-capabilities.md).

### How do I build an invokable Skill (LLM agent)?

A Skill is a Capability + a handler. The no-code path is a TOML manifest +
`skillrunner`; the in-process path is `register_prompt_skill`. Live template
updates, scaling, model selection: [05 · Skills](05-skills.md). Demos:
[`mailbox_llm`](../../examples/coop/src/bin/mailbox_llm.rs),
[`llm_council`](../../examples/coop/src/bin/llm_council.rs).

> Pitfall: host a skill on a *different* node from its caller, and gate readiness
> on capability **and** peers — [14 · Patterns & Pitfalls](14-patterns-and-pitfalls.md) §1–2.

### How do I call across nodes — RPC, mailbox, scatter?

```rust
agent.service().rpc_call(target, kind, payload, timeout).await?;          // request/reply
agent.service().deliver_event(&target, "kind", payload);                 // durable mailbox
let (_h, mut rx) = agent.service().open_mailbox("kind", 64);             // drain it (HLC-ordered)
```

When to use which: [00 · Concepts](00-concepts.md) (Mailbox vs Signal vs RPC vs
Bulk vs Scatter). Full API (RPC, bulk, scatter-gather, mailbox):
[the service-layer reference](#reference--the-service-layer-rpc-bulk-scatter-gather-mailbox)
below. Demo: [`mailbox_llm`](../../examples/coop/src/bin/mailbox_llm.rs).

### How do I coordinate work without a dispatcher (pull)?

Use the tuple space: workers `take()` when ready; no one predicts who does what.
Demos: [`llm_pipeline`](../../examples/coop/src/bin/llm_pipeline.rs) (linear),
[`llm_council`](../../examples/coop/src/bin/llm_council.rs) (fan-out → synthesis →
refinement). Chapter: [07 · Pipelines](07-pipelines.md). Need fan-in by
correlation key? `put_keyed` + `take_by_key` (M13).

### How do I let agents compete for shared facts by content (blackboard)?

When *which* agent acts depends on a fact's *content* rather than a known lane,
use [`mycelium-blackboard`](../../mycelium-blackboard/): agents `post` typed
facts, many `read` them non-destructively (`rd`), and a finite fact is consumed
by exactly one agent via `claim(predicate)` (`in`) — competitive, single-owner,
with at-least-once re-queue if a claimer drops mid-work. Predicates are attribute
equality + presence (not unification). Demo:
[`microgrid`](../../mycelium-blackboard/examples/microgrid.rs) (readers share,
storage executors compete for finite surplus). When to use which:
[00 · Concepts](00-concepts.md) ("tuple space vs. blackboard").

### Why (and when) would I create a group within a cluster?

A cluster is *everyone*; a **group is a named subset you address as a unit**. You create one when
"all nodes" is the wrong audience. Four reasons, all on the same substrate:

- **Scope *agreement* to the nodes that should decide.** `cluster_propose` asks the *whole
  cluster* for quorum; `group_propose("compliance", …)` asks only the compliance nodes. Cheaper
  (fewer voters), and *meaningful* — a policy is ratified by the bloc that owns it, not by unrelated
  workers. Same for a **scoped lock / leader**: `elect_leader("shard-0")` or a lock whose name is
  per-subsystem gives you one owner *within* that function, not one across the fleet.
- **Partition the fleet by function.** `nlp-workers`, `render-pool`, `storage` — each an emergent
  group by capability. Then `SignalScope::Group("nlp")` fires an event only nodes in that function
  act on, and `resolve(filter)` finds exactly that function's providers. The cluster stays one mesh;
  the *work* is organised into functional sets.
- **Bound a pool's size** — make the group *governed* (a `MembershipIntent`): "keep 5–10 render
  workers", and the `MembershipGovernor` converges it. (Ungoverned = self-organising, no cap; see
  [00 · Concepts](00-concepts.md#the-other-concept-pairs-people-conflate).)
- **Span failure domains for durability** — `cross_group_propose` makes each availability zone an
  independent voting bloc, so a commit must clear *every* zone (multi-AZ quorum) — see
  [04 · Consensus](04-consensus.md).

Rule of thumb: **cluster-scope for "everyone"; a group when a *subset* is the right audience for a
signal, a vote, a lock, or a functional role.** How to actually make one ↓.

### How do I define a group, and monitor who's in it?

**Scope terms first:** the mesh has three scopes — `Cluster · Group · Individual` (all / subset /
one), with `node ∈ group(s) ⊆ cluster`. See [13 · Cluster Topology](13-cluster-topology.md#scope-vocabulary-cluster--group--individual).

**Define** — two ways, by intent:
- *Plain membership* — `agent.mesh().join_group("nlp")` (and `leave_group`). This node now acts on
  `SignalScope::Group("nlp")` signals and votes in `group_propose("nlp", …)`. Membership gossips to
  Layer I at `grp/nlp/{node}`.
- *Emergent (capability) group* — `agent.capabilities().define_capability_group("nlp",
  CapabilityGroupDef { filter, provides, requires, … }, ttl)`. Every node whose capabilities match
  `filter` **self-joins** — no coordinator, no join call. This is the one you usually want for agent
  fleets. Chapter: [02 · Capabilities](02-capabilities.md#emergent-capability-groups).

**Monitor** — three angles for the three kinds of group:
- *Who's in it* — `agent.mesh().group_members("nlp")` → `Vec<NodeId>` (live members from
  `grp/nlp/`). For a capability group, `agent.capabilities().resolve(&filter)` lists the matching
  providers directly.
- *Is it at its target size* — a **governed** group (one under a `MembershipIntent` /
  `MembershipGovernor`) reports `GroupStatus { group, min, max, observed, conflict }` via
  `agent.fleet_snapshot().governed_groups`, on `GET /gateway/fleet`, and as the
  `mycelium_emergent_governed_group_conflicts` metric. Runbook:
  [diagnostics.md](../operations/diagnostics.md#governed-group-conflict--thrash-the-56-pattern).
- *This node's own groups* — `agent.groups()`.

> **Gap (honest):** an **ungoverned** group has no per-group Prometheus gauge — you have
> `group_members().len()` from the API, but no first-class "size of group X" metric or dashboard
> panel unless it's under a governor. Tracked separately; use `group_members` + your own gauge if
> you need it scraped today.

### How do I give a group a durable, curated knowledge canon (wiki)?

When the group needs **durable, curated** shared knowledge — not transient work —
use [`mycelium-wiki`](../../mycelium-wiki/): agents `propose` edits, a single
elected **curator** reconciles + lint-checks them into a node-independent store,
and every agent **reads directly** (no curator on the read path). Reach it as MCP
tools (`wiki.read`/`query`/`propose`), over the HTTP gateway (`/gateway/wiki/*` +
Python/TS `Wiki` SDKs), or via `Wiki::request_store_access` for a membership-gated
grant. Demo: [`wiki_chat`](../../mycelium-wiki/examples/wiki_chat.rs) — import
documents, then chat grounded in the wiki (one template for both the org-twin and
council use cases). It **composes** with Postgres (metrics) + RAG (background) by a
shared id namespace — it is the authoritative/maintained layer, not a similarity
index.

### How do I make my agents reachable from LangChain / AutoGen (A2A)?

Serve the A2A AgentCard — built automatically from your capabilities at
`/.well-known/agent.json`. An external ReAct agent discovers and calls your
skills as tools. Demo: [`examples/a2a_langchain`](../../examples/a2a_langchain/).
Chapter: [08 · A2A interop](08-a2a-interop.md).

### How do I expose or consume MCP tools?

`agent.mcp().register_mcp_tool(...)` publishes a tool; `connect_mcp_server(url)`
bridges an external MCP server's tools into the mesh. Demo:
[`mcp_toolgrowth`](../../examples/coop/src/bin/mcp_toolgrowth.rs) (an LLM agent
grows the toolset on demand). Chapter: [06 · Tool discovery](06-tool-discovery.md).

> Pitfall: an MCP tool lives in `tools/`, not `cap/` — bridge it by *also*
> advertising a `tool/` capability ([14](14-patterns-and-pitfalls.md) §9).

### How do I federate across domains (publish AgentFacts)?

Mount the facts lens and serve a self-certified document at the edge; a
neighbouring domain pulls and verifies it with no shared CA. Demo:
[`federation_facts`](../../examples/coop/src/bin/federation_facts.rs). Viewing it:
[operations/observability.md](../operations/observability.md#viewing-agentfacts).

### How do I author and ship a deployable artifact (WASM tool)?

Build a WASM component, content-address it (`library.store`), sign it, and
`publish_installable` to the gossip catalogue; another node discovers it
(`InstallableCatalog::from_kv`), pulls it over the mesh (`MeshArtifactSource`), and
provisions it. Full how-to:
[operations/artifacts.md § Solution/Dev](../operations/artifacts.md#solutiondev--authoring--publishing-an-artifact)
(authoring/publishing) and its [§ DevOps](../operations/artifacts.md#devops--operating-the-catalogue)
half (operating the catalogue). Demos: [`catalog`](../../examples/coop/src/bin/catalog.rs) (the
catalogue), [`provisioning`](../../examples/coop/src/bin/provisioning.rs) (autonomic).

### How do I make a decision that must be singular (consensus)?

Use Layer III — `group_propose` / `cross_group_propose` for quorum agreement,
with optional leased (decaying) commitments. Demo:
[`consensus`](../../examples/coop/src/bin/consensus.rs). Chapter:
[04 · Consensus](04-consensus.md). For *soft* state, don't reach for consensus —
LWW is the default ([00 · Concepts](00-concepts.md): consensus vs LWW vs rendezvous).

### How do I scale the cluster up/down dynamically?

Publish a membership or tuning intent over `/gateway/govern`; nodes self-elect.
Operator guide: [operations/dynamic-scaling.md](../operations/dynamic-scaling.md).
Demo: [`elastic_intent`](../../examples/coop/src/bin/elastic_intent.rs).

### How do I shed load / signal "I'm busy"?

Write your own `sys/load/` pheromone (or run an opacity governor); `resolve`
skips opaque providers automatically. Demo:
[`stigmergy`](../../examples/coop/src/bin/stigmergy.rs).

### How do I secure it (mTLS, RBAC, audit)?

mTLS + Ed25519 identity is one config field ([deployment.md](../operations/deployment.md));
RBAC/OAuth2/OIDC and the tamper-evident audit trail are the `compliance` feature.
Chapter: [09 · Security](09-security.md); ops: [rbac.md](../operations/rbac.md),
[audit.md](../operations/audit.md), [sso.md](../operations/sso.md),
[cert-rotation.md](../operations/cert-rotation.md).

---

**Hit a "why won't this converge / why is this flaky" wall?**
[14 · Patterns & Pitfalls](14-patterns-and-pitfalls.md) collects the real ones.

---

## Reference — the service layer (RPC, bulk, scatter-gather, mailbox)

*Moved from the repo README (2026-07-10).*

Layer 3 delivers the service primitives used by the language bridges and the MCP integration.

#### Point-to-Point RPC

```rust
// Caller
let reply = agent.rpc_call(target, "echo", payload, Duration::from_secs(5)).await?;

// Responder
let mut rx = agent.rpc_rx("echo");
while let Some(req) = rx.recv().await {
    agent.rpc_respond(&req, req.payload());
}
```

#### Bulk Payload Transfer

For payloads too large to gossip through every node, `bulk_call` stages the data at a local
HTTP endpoint and sends only a lightweight ticket over the mesh:

```rust
// Set http_port in GossipConfig so the target can fetch the staged bytes
let reply = agent.bulk_call(target, "process", large_bytes, Duration::from_secs(30)).await?;
```

#### Scatter-Gather

Fan out an identical request to multiple targets concurrently; return as soon as `min_ok` replies arrive:

```rust
let results = agent.scatter_gather(targets, "vote", payload, Duration::from_secs(5), 2).await?;
```

#### Actor/Event Mailboxes

KV-backed durable event delivery. Events survive crashes and are delivered in HLC-causal order:

```rust
// Sender (any node)
agent.deliver_event(&target_id, "task.result", result_bytes);

// Receiver — events delivered at-least-once within TTL, tombstoned after delivery
let (handle, mut rx) = agent.open_mailbox("task.result", 64);
while let Some(event) = rx.recv().await {
    process(&event.payload);
}
// drop(handle) to cancel the watcher
```
