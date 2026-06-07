# Mycelium — Developer Guide

Mycelium is a broker-less embedded Rust library. You embed it directly in your
process — there is no daemon, no sidecar, no coordinator to run. Each node is
simultaneously a participant in the mesh and a full peer. The mesh is the
registry, the bus, and the scheduler all at once.

## Design philosophy

Most distributed systems treat consistency as the default and availability as
the thing you sacrifice during a partition. Mycelium inverts this. Eventual
consistency is the default substrate — fast, partition-tolerant, no coordinator
required. Strong consistency is an opt-in overlay you reach for only where your
application actually needs it. You pay for guarantees only where they matter.

This is what [ROADMAP.md](../../ROADMAP.md) calls **The Structural Inversion**:

> _"Rather than building eventual consistency on top of consensus, build
> consensus on top of eventual consistency. The gossip substrate is always
> available; the consistency overlay is available when you need it."_

The second principle is biological. Signals propagate like hormones in a
circulatory system — epidemically, without routing tables, without a
dispatcher. Each node holds a `Boundary` (its receptor set) that decides
whether it *acts* on a signal; forwarding is always unconditional. Opacity,
load-shedding, and demand pressure all emerge from the same mechanism: nodes
write to their own `sys/load/` prefix, and anything scanning that prefix sees
a consistent picture without any coordination. See
[docs/philosophy.html](../philosophy.html) for the full argument.

The third principle is substrate unity: every higher-layer feature — capability
advertisements, consensus ballots, audit records, tool registrations — is
stored as a key in the gossip KV store. There is one substrate, not a stack of
separate systems. This means any node can inspect any layer's state, and the
anti-entropy mechanism that heals KV partitions also heals capability routing
and consensus voting.

---

## Layers

The library is built in three layers, each complete and useful on its own:

```mermaid
graph TD
    A["Layer I — Gossip KV<br/>Eventual-consistent shared state<br/>LWW · HLC · anti-entropy · TTL"] --> B
    B["Layer II — Signal Mesh<br/>Ephemeral scoped events<br/>Admission boundaries · opacity composition"] --> C
    C["Layer III — Consensus<br/>Epidemic group voting<br/>consistent_set · append · distributed_lock"]
    D["Capability System<br/>Broker-less service discovery<br/>advertise · resolve · demand · locality ranking"] --> E
    E["Application Patterns<br/>Skills · MCP Tools · Pipelines · A2A interop"]
    A --> D
    B --> D
    C --> D
    D --> E

    style A fill:#1a2744,color:#93c5fd
    style B fill:#1a2d1a,color:#86efac
    style C fill:#2d1a1a,color:#fca5a5
    style D fill:#2d2414,color:#fcd34d
    style E fill:#1a1a2d,color:#c4b5fd
```

**Layer I** gives every node an eventually-consistent view of the cluster's
key-value state — no Zookeeper, no etcd, no Redis. Any node can write; every
node converges. HLC timestamps preserve causal ordering under clock skew.

**Layer II** adds ephemeral events that propagate epidemically. Each node
declares a `Boundary` — its admission rules — so signals flow where they're
relevant and nowhere else.

**Layer III** adds strong consistency on top of the eventual-consistency
substrate. `consistent_set`, `append`, `distributed_lock`, and `elect_leader`
are opt-in overlays, not the default. Pay for consistency only where you need it.

**The capability system** is the connective tissue: nodes advertise what they
provide (`ns/name` pairs with structured attributes), and any node can resolve
providers at call time — with locality ranking, demand pressure, and emergent
group formation — without knowing addresses in advance.

> **Note on layer numbering.** This guide uses three layers (I, II, III) for
> clarity. [ROADMAP.md](../../ROADMAP.md) describes five numbered layers (1–5)
> plus the opt-in overlay and capability subsystem as distinct sections.
> Layers 3–5 in ROADMAP correspond to the application patterns in chapters
> 05–08 here. Both descriptions are correct; the guide simplifies for
> newcomers.

Four application patterns build on this substrate:

| Pattern | What it does | Guide chapter |
|---------|-------------|---------------|
| Skills | LLM agents as mesh nodes; TOML manifests; skill→skill composition | [05-skills.md](05-skills.md) |
| MCP Tool Discovery | LLM discovers tools dynamically from the KV store; zero-restart addition | [06-tool-discovery.md](06-tool-discovery.md) |
| Fluid Pipelines | Fixed worker pool flows through pipeline stages; KV ring as buffer | [07-pipelines.md](07-pipelines.md) |
| A2A Interop | LangChain / AutoGen agents discover Mycelium skills via `/.well-known/agent.json` | [08-a2a-interop.md](08-a2a-interop.md) |

---

## Chapters

| # | Concept | Runnable example | Time |
|---|---------|-----------------|------|
| [01](01-gossip-kv.md) | Gossip KV — shared state without a broker | `cargo run --example conway` | 30 s |
| [02](02-capabilities.md) | Capability discovery — find nodes by what they do | `cargo run --example llm_agent` | 1 min |
| [03](03-signals.md) | Signal mesh — ephemeral scoped events | `cargo run --example prompt_skill_demo` | 30 s |
| [04](04-consensus.md) | Consensus overlay — strong consistency on demand | overlay scenarios in `three_node_demo` | 2 min |
| [05](05-skills.md) | Skills — LLM agents on the mesh | `cd examples/community && ./demo.sh` | 5 min |
| [06](06-tool-discovery.md) | MCP tool discovery — LLM finds tools dynamically | `./examples/chat/demo.sh` | 5 min |
| [07](07-pipelines.md) | Fluid pipelines — Agentic Flow Networks | `docker compose up --scale worker=10` | 3 min |
| [08](08-a2a-interop.md) | A2A interop — LangChain / AutoGen integration | `python langchain_agent.py` | 3 min |
| [09](09-security.md) | Security — mTLS, Ed25519, signed KV, audit trail | `--features tls` | — |
| [10](10-language-bridges.md) | Language bridges — Python and TypeScript SDKs | `pip install mycelium-py` | 5 min |
| [11](11-semantic-coordination.md) | Semantic coordination — schema versioning, payload schemas, sender auth | `cargo run --example semantic_coordination` | 5 min |
| [12](12-schema-lifecycle.md) | Schema lifecycle — publish, conflict detection, CI gate, versioning | `agent.schemas().publish_schema(...)` | 10 min |
| [13](13-cluster-topology.md) | Cluster topology — seeds, partial mesh, sizing, partition recovery | — | 10 min |
| [Error handling](error-handling.md) | Error type taxonomy, recoverability, propagation strategy | — | — |

Read chapters 01–04 to build the mental model. Jump to 05–08 for the application
pattern that matches your use case. Chapter 09 covers the security and compliance
story; chapter 10 covers using Mycelium from Python or TypeScript.

---

## Quick orientation

```rust
// Every node is a GossipAgent — embed one in any Rust process
let agent = Arc::new(GossipAgent::new(node_id, config));
agent.start().await?;

// Layer I — shared state
agent.kv().set("my/key", Bytes::from("value"));
let val = agent.kv().get("my/key");

// Layer II — events
agent.mesh().signal_rx(signal_kind::INVOKE);  // returns mpsc::Receiver<Signal>
agent.mesh().emit(signal_kind::INVOKE, SignalScope::Group("nlp".into()), payload);

// Layer III — strong consistency (opt-in)
agent.consensus().consistent_set("seq/counter", b"1").await?;

// Capability system — discovery
let cap = agent.capabilities().advertise_capability(Capability::new("llm", "inference"), Duration::from_secs(60));
let providers = agent.capabilities().resolve(&CapFilter::new("llm", "inference"));
```

See the [main README](../../README.md) for the full API surface and
[ROADMAP.md](../../ROADMAP.md) for the architecture rationale and design
decisions.

---

## Glossary

### Groups — two distinct concepts

The word "group" appears in two unrelated APIs. They are independent mechanisms:

| | Signal groups | Capability groups |
|---|---|---|
| **API** | `agent.mesh().join_group(name)` | `agent.capabilities().define_capability_group(name, def, interval)` |
| **Controls** | **Routing** — `SignalScope::Group(name)` delivers only to members | **Discovery** — nodes self-join when their capabilities match the group's filter |
| **Membership** | Explicit: you call `join_group` / `leave_group` | Emergent: each node independently evaluates the `CapabilityGroupDef` filter against its own capabilities |
| **KV namespace** | `grp/{group}/{node}` | `cap-group/{group}` (definition) + `gcap/{group}/...` (projected capabilities) |
| **Use case** | Scoping a signal to a runtime-defined subset of nodes | Making a set of nodes with matching capabilities visible as a wiring target |

A node can be a member of both a signal group and a capability group with the same name — they are stored and evaluated independently.
