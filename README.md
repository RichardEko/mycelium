# Mycelium

A broker-less mesh runtime for AI agent fleets, embedded as a Rust library. Agents discover
each other's capabilities, route tool calls, exchange events, and reach consensus — all without
a coordinator, central registry, or single point of failure.

Built on TCP epidemic propagation with last-write-wins conflict resolution. Layer 1 carries
persistent state; Layer 2 carries ephemeral events. Higher layers provide async RPC, Actor/Event
mailboxes, MCP tool routing, SkillRunner LLM nodes, and a Python bridge — each agent chooses
its own payload serialisation.

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.20665238.svg)](https://doi.org/10.5281/zenodo.20665238)

The architectural argument is published as: R. Nicholson, *"The Coordinator Trap: Structural
Scaling Liabilities in Mediated Multi-Agent Architectures and a Substrate-Based Alternative,"*
Tathata Systems Ltd, 2026 — [doi:10.5281/zenodo.20665238](https://doi.org/10.5281/zenodo.20665238)
(CC BY 4.0; source in [`docs/publications/`](docs/publications/), reproducible at tag
[`paper-submission-v2`](https://github.com/RichardEko/mycelium/tree/paper-submission-v2)).

## Getting Started

Mycelium is three layers: a broker-less gossip KV store (Layer I), an ephemeral
scoped event mesh (Layer II), and an opt-in consensus overlay (Layer III). The
capability system sits across all three layers and provides broker-less service
discovery. Four application patterns build on this substrate: Skills (LLM agents
as mesh nodes), MCP tool discovery (LLM finds tools dynamically from the KV
store), fluid pipelines (Agentic Flow Networks), and A2A interop (LangChain /
AutoGen).

**[→ Developer guide with concept explanations, diagrams, and dev notes for each pattern](docs/guide/README.md)**

---

## Demos

### Mesh Control UI — `llm_agent`

A three-node gossip mesh with a live management UI. No Ollama required for the quick start.

```sh
# Quick start — mock LLM, no Ollama needed
MOCK_LLM=1 cargo run --example llm_agent

# With a real LLM (set OPENAI_BASE_URL / OPENAI_MODEL for non-Ollama endpoints)
cargo run --example llm_agent
```

Open **http://127.0.0.1:8100** in your browser.

| What you get | Detail |
|---|---|
| Three gossip nodes | Ports 56000 – 56002, fully meshed |
| Management UI | http://127.0.0.1:8100 – 8102 |
| Emergent manager election | Lexicographically smallest live node-id becomes manager; browser auto-redirects |
| Simulated failure | n-0 fails at T+35 s; n-1 becomes manager; n-0 recovers at T+50 s |
| Preset gallery | 11 topology presets — click **Apply** and the manifest propagates to all nodes via gossip |
| Manifest upload | Paste or load any TOML manifest; semver-gated, gossip-propagated |
| Soft stop/start | Stop a group or the whole system; capabilities tombstone within one health interval |
| Hybrid tool discovery | MCP tools (`register_mcp_tool`) and SkillRunner skills (`.skill.toml` nodes) are merged automatically — any skill joining the mesh is immediately available to the LLM planner |

**Preset topologies available in the UI:**

| Preset | Description |
|---|---|
| LLM Agent Demo | Real-time data · compute tools · LLM inference |
| MCP Tool Mesh | Tool providers · data sources · LLM reasoning |
| Compute Cluster | Parallel compute workers with real-time data feed |
| Minimal Mesh | Single data node — development and testing |
| Epidemic Ring | 16-node ring split into alpha/beta signal partitions |
| Consensus Cluster | 7 voters + rotating proposers — two-phase ballot |
| Dispatch Pool | Fast/slow worker tiers with adaptive dispatchers |
| Emergent GPU Pool | 20 workers self-assemble; jobs route via `signal_wired_via` |
| Capability Market | 4 capability kinds — compute/gpu, cpu, storage, ai/agent |
| Locality Mesh | East/west providers — `resolve_with_locality` picks nearest |
| Watchdog Cluster | Heartbeat services + `quorum_persistent` circuit breaker |

**Automated Docker test (no Ollama needed):**

```sh
make test-llm-agent   # 11 scenarios: mesh health · tool discovery · planning cycle · spare failover
```

---

### Interactive Chat — `three_node_demo`

Three nodes with distinct roles — two tool providers and one LLM node with a real-time browser chat UI. The LLM plans tool calls across nodes using the gossip mesh for discovery and routing.

| Role | Tools | HTTP port |
|---|---|---|
| `tool-a` | `weather(city)`, `web_fetch(url)` | 8300 |
| `tool-b` | `calculate(expr)`, `wiki(topic)` | 8300 |
| `tool-sf` | `sf_lookup(query)` — SF Encyclopedia scholarly lookup | 8300 |
| `tool-book` | `book_plot(query)` — Wikipedia full article, Plot section | 8300 |
| `llm` | Browser chat UI + LLM planner | 8080 |
| `mgmt` | Management dashboard | 8090 |

**HTTP endpoints on the `llm` node:**

| Endpoint | Description |
|---|---|
| `GET /` | Browser chat UI (HTML) |
| `POST /chat` | Send `{"message":"..."}` — returns 202, planning runs async |
| `GET /stream` | SSE stream: `Thinking`, `ToolCall`, `ToolResult`, `Assistant`, `Idle` events |
| `GET /mesh` | Tool list visible to the planner + current model name |

**Docker (recommended):**

```sh
make test-llm-demo     # interactive — open http://localhost:8080 to chat (requires Ollama)
make test-three-node   # automated test — 4 scenarios, uses real llama3.2 via Ollama
                       # (~2 GB model download on first run; cached in ollama-models volume)
```

**Local quick start (no Docker):**

```sh
# terminal 1
MYCELIUM_ROLE=tool-a MYCELIUM_PEERS=127.0.0.1:57001,127.0.0.1:57002,127.0.0.1:57003,127.0.0.1:57004 \
  MYCELIUM_PORT=57000 cargo run --example three_node_demo

# terminal 2
MYCELIUM_ROLE=tool-b MYCELIUM_PEERS=127.0.0.1:57000,127.0.0.1:57002,127.0.0.1:57003,127.0.0.1:57004 \
  MYCELIUM_PORT=57001 cargo run --example three_node_demo

# terminal 3 — requires Ollama running on localhost:11434 with llama3.2 pulled
MYCELIUM_ROLE=llm MYCELIUM_PEERS=127.0.0.1:57000,127.0.0.1:57001,127.0.0.1:57003,127.0.0.1:57004 \
  MYCELIUM_PORT=57002 OLLAMA_BASE_URL=http://localhost:11434/v1 \
  cargo run --example three_node_demo
# open http://localhost:8080

# terminal 4 — management dashboard (optional)
MYCELIUM_ROLE=mgmt MYCELIUM_PEERS=127.0.0.1:57000,127.0.0.1:57001,127.0.0.1:57002 \
  MYCELIUM_PORT=57003 cargo run --example three_node_demo
# open http://localhost:8090

# terminal 5 — SF Encyclopedia (start any time; llm discovers it live)
MYCELIUM_ROLE=tool-sf MYCELIUM_PEERS=127.0.0.1:57000,127.0.0.1:57001,127.0.0.1:57002 \
  MYCELIUM_PORT=57004 cargo run --example three_node_demo
# ask: "how does Dan Simmons fit into 1990s SF?" → uses sf_lookup

# terminal 6 — book plot tool (start any time; llm discovers it live)
MYCELIUM_ROLE=tool-book MYCELIUM_PEERS=127.0.0.1:57000,127.0.0.1:57001,127.0.0.1:57002 \
  MYCELIUM_PORT=57005 cargo run --example three_node_demo
# ask: "what happens in Hyperion?" → uses book_plot
```

---

### Consistency Overlay Cluster — `three_node_demo` (overlay role)

Three consensus-voting nodes that expose the full overlay REST API. Designed as an
integration test cluster and developer template — the Python scenario scripts are
copy-paste starting points for production patterns.

| Scenario | Pattern |
|---|---|
| S11 Task Auction | Exact-once delivery via `subscribe_log_group` |
| S12 Leader Election | Concurrent `elect_leader` + consensus-durable `consistent_set` |
| S13 Shared Reasoning Log | Multi-writer `append`, HLC ordering, `compact_log` |

```sh
make test-overlay   # Docker cluster — 3 nodes, 3 Python scenarios (~3 min, no Ollama needed)
```

```sh
# Local — 3 terminals, no Docker
MYCELIUM_ROLE=overlay MYCELIUM_PEERS=127.0.0.1:57001,127.0.0.1:57002 \
  MYCELIUM_PORT=57000 MYCELIUM_HTTP_PORT=8300 cargo run --example three_node_demo
# then talk to it: python -c "
#   from mycelium import MyceliumAgent
#   a = MyceliumAgent('127.0.0.1', 8300)
#   a.consistent_set('x', b'hello')
#   print(a.consistent_get('x'))
# "
```

See [`tests/overlay/README.md`](tests/overlay/README.md) for the full developer guide.

---

### Conway's Game of Life

A separate standalone demo that shows the epidemic substrate itself rather than a service topology. 256 gossip agents (one per cell in a 16×16 grid) coordinate cell state via gossip KV; a tick signal drives each generation.

```sh
cargo run --example conway          # CPU renderer (terminal / HTTP canvas)
cargo run --example conway_gpu      # GPU-accelerated renderer (Metal / wgpu)
```

---

## Skills vs MCP Tools — Choosing the Right Primitive

Mycelium supports two ways to extend what an LLM agent can do. They solve
different problems and compose naturally together.

### Mental model

> **MCP tool** = a function in the mesh. The LLM calls it to look something
> up, run a calculation, or fetch data. Written in any language.
>
> **Skill** = an LLM agent in the mesh. It has its own identity, prompt, and
> capability declaration. It can be called by any node — including other skills.

### Comparison

| | MCP Tool | Skill |
|---|---|---|
| What it is | A function registered on a node | An LLM agent node |
| Written in | Any language | TOML manifest — no code |
| Calls an LLM | Optionally | Always |
| Can call other skills | No | Yes — composition |
| Discovered via | `tools/` KV prefix | Capability system (`ns`/`name`) |
| Started with | Any binary / language | `skillrunner --skill manifest.toml` |
| Live chat example | `three_node_demo` — `wiki`, `weather`, `calculate` | `examples/community/` — researcher, writer, orchestrator |
| Guide | [06-tool-discovery.md](docs/guide/06-tool-discovery.md) | [05-skills.md](docs/guide/05-skills.md) |

### When to use each

Use an **MCP tool** when:
- You need to call an external API (weather, Wikipedia, a database)
- You need deterministic computation (arithmetic, format conversion)
- You want to write the tool in Python, TypeScript, Go, or any language
- The operation is stateless and fast

Use a **Skill** when:
- You need an LLM reasoning step in a pipeline
- You want to compose agents — an orchestrator that calls a researcher that calls a writer
- You want a persistent, named agent role that any node on the mesh can discover and invoke
- You want to scale a reasoning step horizontally (run two researchers; the orchestrator uses both)

### They compose naturally

The `three_node_demo` LLM node uses MCP tools for external lookups (`wiki`,
`weather`, `sf_lookup`, `book_plot`). The `examples/community/` orchestrator
uses Skills for LLM reasoning steps (`researcher`, `writer`). There is no
conflict — a single planner can have both in scope simultaneously.

---

## Build

```
cargo build --release
```

### Fuzz harness

`fuzz/` is a standalone cargo-fuzz crate covering the wire-format decoder and
every capability subsystem decoder. Requires nightly and `cargo install
cargo-fuzz`:

```
cargo +nightly fuzz run wire_decode        -- -max_total_time=60
cargo +nightly fuzz run capability_decode  -- -max_total_time=60
```

## Run

Start a bootstrap node:

```
cargo run -- --port 7946
```

Start a second node that joins via the bootstrap node:

```
cargo run -- --port 7947 --peers 127.0.0.1:7946
```

Start in interactive mode to set/get keys:

```
cargo run -- --port 7947 --peers 127.0.0.1:7946 --interactive
```

### Interactive commands

```
set <key> <value>   store a value and gossip it to peers
get <key>           retrieve a value from the local store
delete <key>        remove a value and gossip the tombstone
stats               show peer count, store size, and open connections
exit                shut down the node
```

## Layer I — Gossip KV Transport

```rust
use mycelium::{GossipAgent, GossipConfig, NodeId};
use std::sync::Arc;

let mut config = GossipConfig::default();
config.bootstrap_peers = vec!["127.0.0.1:7947".parse()?];

let agent = Arc::new(GossipAgent::new(NodeId::new("127.0.0.1", 7946)?, config));
agent.start().await?;

// Write — local store always updated; returns false if gossip channel full
agent.kv().set("key", Bytes::from("value"));

// Read
if let Some(bytes) = agent.kv().get("key") { /* ... */ }

// Delete (propagates a tombstone)
agent.kv().delete("key");

// Enumerate live keys
let keys: Vec<Arc<str>> = agent.kv().keys();

// Scan by prefix — capability discovery, pheromone trail reads
let entries = agent.kv().scan_prefix("load/");

// Subscribe — watch::Receiver fires on every change (local or gossiped)
let mut rx = agent.kv().subscribe("load/my-node");
rx.changed().await?;
println!("{:?}", *rx.borrow());  // None = tombstoned; Some(bytes) = current value

agent.shutdown().await;
```

### Layer I Observability

`system_stats()` is the primary diagnostic surface. Poll it periodically or on anomalies:

```rust
let stats = agent.system_stats();

// Propagation health — the most important number.
// dropped_frames > 0 means gossip writes were silently lost.
// Cause: writer_channel_depth or gossip_channel_capacity too small for the burst rate.
// Fix: raise the limiting field (documented sizing formula in GossipConfig).
assert_eq!(stats.dropped_frames, 0);

// Topology
println!("peers: {}", stats.peers);             // live peers in the ping table
println!("store: {}", stats.store_entries);     // live KV entries (tombstones excluded)

// Internal health — alert if false while agent is running
assert!(stats.gc_alive);                        // tombstone expiry and subscription cleanup
assert!(stats.health_monitor_alive);            // peer pings and eviction

// Shard backpressure — non-zero means gossip workers are falling behind
println!("shard depths: {:?}", stats.gossip_shard_queue_depths);
println!("dead shards: {}", stats.dead_shards); // should always be 0
```

**Diagnostic flow when writes stop propagating:**

```
dropped_frames > 0?
  YES → writer_channel_depth or gossip_channel_capacity too small
        Size writer_channel_depth to N_agents × fan_out (default fan_out = 4)
  NO  → peers == 0?  bootstrap_peers misconfigured or all peers unreachable
        health_monitor_alive == false?  internal task failure — restart agent
        shard_queue_depths saturated?   gossip_shards too low for write rate
```

**Topology introspection — is the store visible from the outside?**

`scan_prefix` doubles as a topology query. After the cluster settles, every node's pheromone
trail is visible from every other node via anti-entropy sync:

```rust
// Count live workers in the "nlp" pool
let live_workers: Vec<LoadState> = agent.kv().scan_prefix("load/nlp/")
    .into_iter()
    .filter_map(|(_, b)| decode::<LoadState>(&b))
    .filter(|s| unix_ms_now() - s.written_at_ms < 30_000)  // evaporation window
    .collect();
println!("{} live nlp workers", live_workers.len());
```

---

## Layer II — Signal / Boundary Mesh

Signals are ephemeral events that propagate epidemically to every node in the cluster. Each node
holds a local **boundary** — a set of group memberships — that decides whether it *acts* on an
incoming signal. Forwarding is always unconditional; the boundary only controls local delivery.

```rust
use mycelium::{signal_kind, SignalScope, OpacityHint};
use std::time::Duration;

// ── Group membership ──────────────────────────────────────────────────────
agent.mesh().join_group("nlp");
agent.mesh().leave_group("nlp");
let groups: Vec<Arc<str>> = agent.groups();  // current memberships

// ── Advertise — periodic heartbeat + pheromone trail ─────────────────────
let load_key = format!("load/{}", agent.node_id());
let agent2 = agent.clone();
let _advert = agent.mesh().advertise(
    signal_kind::CONTRACT_AVAILABLE,
    SignalScope::Group("nlp"),
    Duration::from_secs(10),
    move || {
        let state = LoadState { queue_depth: QUEUE.len(), written_at_ms: unix_ms_now() };
        agent2.kv().set(load_key.clone(), encode(&state));  // pheromone trail — persists
        encode(&state)                                  // signal payload — fast delivery
    },
);
// Drop _advert to stop advertising; call agent.kv().delete(&load_key) on graceful shutdown

// ── Receive signals ────────────────────────────────────────────────────────
let mut rx = agent.mesh().signal_rx(signal_kind::INVOKE);
tokio::spawn(async move {
    while let Some(sig) = rx.recv().await {
        // sig.sender, sig.payload, sig.scope, sig.nonce
    }
});
// Channel sizing: the default depth of 256 suits kinds that arrive at a few Hz
// (health probes, contract advertisements). For kinds where N agents all emit
// simultaneously (e.g. INVOKE to a group of N workers), use:
//   agent.mesh().signal_rx_with_capacity(kind, N * expected_burst)
// A full channel logs a warning and drops the signal — there is no retry.

// ── Sender-filtered receive (signal sender authorization) ──────────────────
// Only deliver signals whose sender is in the trusted list. Signals from any
// other node are silently discarded before reaching the channel. Useful for
// LLM-driven agents that process signal payloads as prompts — prevents
// semantic injection from compromised or buggy peers.
// With --features tls, sender identity is backed by an Ed25519 keypair.
let orchestrator: NodeId = "10.0.1.1:7700".parse().unwrap();
let mut rx = agent.mesh().signal_rx_from("task.assign", vec![orchestrator]);

// ── Emit ───────────────────────────────────────────────────────────────────
agent.mesh().emit("invoke", SignalScope::Group("nlp"), payload);       // non-blocking
agent.mesh().emit_async("invoke", SignalScope::Group("nlp"), payload).await; // awaits capacity

// ── One-shot request/response — register BEFORE emitting the request ───────
let reply = agent.mesh().signal_once("invoke.result", Duration::from_secs(5), |s| {
    s.nonce == request_nonce
}).await;  // → Option<Signal>; None on timeout

// ── Scopes ─────────────────────────────────────────────────────────────────
SignalScope::System              // every node acts; shed under load by opacity
SignalScope::Group("name")       // nodes that called join_group("name")
SignalScope::Individual(node_id) // exactly one node; bypasses opacity shedding
```

### Observing the Mesh

Layer 2 provides several complementary lenses into mesh state. They answer different questions:

```rust
// ── When did I last hear a signal of this kind? ────────────────────────────
// Useful for: circuit-breaker logic, retry decisions, fault detection.
// Returns None if the kind has never been delivered to this node.
let age: Option<Duration> = agent.mesh().last_signal(signal_kind::CONTRACT_AVAILABLE)
    .map(|t| t.elapsed());

// ── Watch — fault detection / supervisor pattern ───────────────────────────
// Calls on_stale() when last_signal(kind) has been silent for longer than threshold.
// Checks every threshold/4 (minimum 100ms). Returns WatchHandle; drop to cancel.
let _watcher = agent.mesh().watch(
    signal_kind::CONTRACT_AVAILABLE,
    Duration::from_secs(30),
    move || {
        tracing::warn!("worker heartbeat stale — triggering respawn");
        respawn_worker();
    },
);

// ── Quorum — threshold activation ─────────────────────────────────────────
// Returns true when at least min_senders *distinct* NodeIds have had a signal
// of kind delivered within window. Synchronous — no background task.
// Use for: consensus-adjacent decisions, majority-activated state changes.
if agent.mesh().quorum(signal_kind::CLUSTER_EVENT, 3, Duration::from_secs(10)) {
    // At least 3 distinct nodes checked in within the last 10 seconds
    start_leader_election();
}

// ── Is this node suppressing a kind? ──────────────────────────────────────
let suppressing: bool = agent.mesh().is_suppressed(signal_kind::INVOKE);

// ── Current fill ratio for a kind's handler channel ───────────────────────
// 0.0 = empty (no load); 1.0 = full (completely saturated, opacity = 100%).
// Corresponds to the probability that the next System/Group signal is shed.
let load: f32 = agent.opacity(signal_kind::INVOKE);  // 0.0..=1.0

// ── Proactive opacity notification governor ────────────────────────────────
// Monitors fill ratio and emits boundary.opaque / boundary.transparent to peers.
// Library governs the threshold (default 0.75, hysteresis 0.20, trend-adjusted).
// Application provides a hint; library clamps and adapts it at runtime.
let _governor = agent.manage_opacity(
    signal_kind::BOUNDARY_OPAQUE,
    SignalScope::Group("nlp"),
    OpacityHint::default(),                  // threshold=0.75, hysteresis=0.20
);

// Optional: application gate — veto an opacity transition.
// Gate is re-consulted every check tick; return false to hold the current state.
// Library overrides all vetoes when fill_ratio == 1.0 (channel completely full).
let has_inflight = Arc::new(AtomicBool::new(false));
let inflight = has_inflight.clone();
let _governor = agent.manage_opacity_gated(
    signal_kind::BOUNDARY_OPAQUE,
    SignalScope::Group("nlp"),
    OpacityHint { threshold: 0.80, hysteresis: 0.25, ..Default::default() },
    move |state| {
        // Veto transition if we have in-flight work, unless fill is above 90%
        state.fill_ratio >= 0.9 || !inflight.load(Ordering::Relaxed)
    },
);
// OpacityState passed to the gate: { fill_ratio, effective_threshold, trend, is_opaque }
```

**What each tool answers:**

| Question | Tool |
|---|---|
| Has a worker been seen recently? | `mesh().last_signal` |
| Has a worker gone silent? (trigger action when silent) | `mesh().watch` |
| Have enough distinct nodes checked in? | `mesh().quorum` |
| Have K nodes checked in (survives restart)? | `kv().quorum_persistent` |
| Is this node actively refusing a kind? | `mesh().is_suppressed` |
| How saturated is this node's intake? | `opacity` |
| Are peers aware this node is overloaded? | `manage_opacity` governor |
| Which groups is this node a member of? | `groups()` |
| How many live workers are in the pool? | `kv().scan_prefix("load/")` |
| Which peers are dropping frames? | `peer_drop_counts()` |

### Opacity vs Inhibition — Knowing the Difference

These two mechanisms both reduce signal delivery, but they arise from completely different causes
and serve completely different purposes. Confusing them leads to incorrect diagnostics.

---

#### Opacity — passive, automatic, emergent

Opacity is not a feature you call. It is a property the boundary acquires automatically when
handler channels fill under load.

```
fill_ratio  = 1.0 - (channel_remaining / channel_capacity)
admit_prob  = 1.0 - fill_ratio
```

When `fill_ratio = 0.6`, 60% of incoming `System` and `Group` signals are shed at the boundary
before reaching handlers. The node still **forwards every signal** — the network remains fully
connected — it simply stops *reacting* to new arrivals. This is emergent backpressure: no
coordinator involved, no explicit "I am busy" handshake, no barrier.

`Individual` scope always bypasses opacity. There is no routing alternative for a directed reply.

**What opacity tells you**: the node is receiving signals faster than its handlers are draining
them. The boundary is load-shedding automatically.

**`manage_opacity` and `OpacityHint`**: these do not change the opacity shedding itself — that
is entirely automatic. They add a governor task that watches the fill ratio and *tells peers*
via a `boundary.opaque` signal when this node is becoming saturated. The application can suggest
a threshold hint; the library adapts it based on the trend (rising fill → lower threshold, falling
fill → relax). This is about *notification to peers*, not about controlling admission.

```
Opacity shedding: automatic, probabilistic, local
manage_opacity:   proactive peer notification — "I am entering overload"
```

---

#### Inhibition — active, deterministic, application-controlled

`suppress(kind, duration)` is called deliberately by your code. For the duration, **no signals
of that kind are delivered** — 100% blocked, not probabilistic. The node keeps forwarding them
and keeps updating `last_signal` timestamps; only handler delivery is blocked.

```rust
// ── Refractory period after handling ──────────────────────────────────────
// After accepting a work item, block the next invocation for 500ms.
// Without this, all queued invocations pile into the handler concurrently.
let mut invoke_rx = agent.mesh().signal_rx(signal_kind::INVOKE);
tokio::spawn(async move {
    while let Some(sig) = invoke_rx.recv().await {
        agent.mesh().suppress(signal_kind::INVOKE, Duration::from_millis(500));
        handle_invocation(sig).await;
    }
});

// ── Rate limiting ──────────────────────────────────────────────────────────
// Suppress "data.sync" for 5s after processing one — prevents sync storms.
agent.on_signal(signal_kind::DATA_SYNC, move |_sig| {
    agent.mesh().suppress(signal_kind::DATA_SYNC, Duration::from_secs(5));
    trigger_sync();
});

// ── Lift early if needed ───────────────────────────────────────────────────
agent.mesh().unsuppress(signal_kind::INVOKE);

// ── Check state for diagnostics ───────────────────────────────────────────
if agent.mesh().is_suppressed(signal_kind::INVOKE) {
    tracing::debug!("invoke suppressed — in refractory period");
}
```

**What inhibition tells you**: the application has deliberately chosen not to handle a kind right
now. It is a programmatic gate, not a load indicator.

---

#### Side-by-side comparison

| Property | Opacity | Inhibition (`suppress`) |
|---|---|---|
| **Triggered by** | Channel fill (automatic) | Application call (`suppress(kind, duration)`) |
| **Effect** | Probabilistic shedding (fill_ratio %) | 100% block — deterministic |
| **Forwarding** | Unaffected | Unaffected |
| **`last_signal` updated?** | Yes | Yes |
| **Reversible** | Auto — drains as channel empties | Auto after `duration`; or explicit `unsuppress` |
| **`Individual` scope** | Always bypassed | Blocked like any other scope |
| **Use for** | Self-protection under load | Refractory period, rate limiting, idempotency window |
| **Diagnostic question** | "Is this node overloaded?" | "Did this node choose to block X?" |

---

#### Combined scenario: worker under heavy load

```rust
// Worker node — three mechanisms working together:

// 1. Opacity (automatic): as invoke_rx fills, boundary sheds incoming invocations
//    probabilistically. No code needed — just keep the channel sized appropriately.

// 2. Inhibition (active): after accepting one invocation, block the next for 500ms.
//    This prevents pile-up even if the channel is large enough to buffer many.
let mut invoke_rx = agent.mesh().signal_rx_with_capacity(signal_kind::INVOKE, 64);
tokio::spawn(async move {
    while let Some(sig) = invoke_rx.recv().await {
        agent.mesh().suppress(signal_kind::INVOKE, Duration::from_millis(500));
        handle_invocation(sig).await;
    }
});

// 3. manage_opacity (proactive): emit boundary.opaque to peers when fill rises above
//    threshold so they route new work elsewhere before the channel fully saturates.
let _governor = agent.manage_opacity(
    signal_kind::BOUNDARY_OPAQUE,
    SignalScope::Group("nlp"),
    OpacityHint::default(),
);
```

The three mechanisms are complementary, not redundant. Opacity protects the node at the boundary.
Inhibition controls the refractory rhythm inside the handler. The governor informs peers in advance.

---

### Signals vs Pheromone Trails

Not all signal kinds need a KV trail. With pheromone trails in the store, some signals are
redundant for *discovery* — the trail is the authoritative record. Others are irreplaceable.

| Signal kind | Role | Covered by pheromone? |
|---|---|---|
| `invoke` | Work request — must reach a worker now | No |
| `invoke.result` | Targeted reply — ephemeral by nature | No |
| `invoke.bulk` | Layer 3 bulk transfer ticket | No |
| `boundary.opaque` | Immediate overload notification | No — fast-path complement to the trail |
| `boundary.transparent` | Recovery notification | No — fast-path complement |
| `contract.available` | Worker availability | **Yes** — `load/<node_id>` trail is authoritative |
| `contract.withdrawn` | Worker gone | **Yes** — tombstone / trail evaporation |
| `cluster.event` | Join/leave events | **Yes** — `grp/<name>/<node_id>` entries |

For routing decisions, always read the store (`kv().scan_prefix("load/")`), not signal history. The
store is visible to late joiners and survives missed signals. Signal history (`mesh().last_signal`,
`mesh().quorum`) is the right tool for liveness and fault detection, not routing.

See [ROADMAP.md](ROADMAP.md) for architecture, design rationale, and Layer 3/4 plans.

---

## Performance Baselines

Measured on the development machine, release build (`cargo bench`). Local hot-path only — no network I/O. Run `cargo bench` to regenerate on target hardware.

| Benchmark | Median | Notes |
|---|---|---|
| `kv/set` | 151 ns | Local store write + gossip channel dispatch |
| `kv/get` hit | 16 ns | Lock-free papaya read |
| `kv/get` miss | 13 ns | Same path, no allocation |
| `scan_prefix` 100 entries | 332 ns | Typical pheromone-trail store size |
| `scan_prefix` 1,000 entries | 2.7 µs | |
| `scan_prefix` 10,000 entries | 41 µs | |
| `scan_prefix` 100,000 entries | 622 µs | **~1 ms — monitor if store grows here** |
| `signal_fanout` 1 handler | ~700 ns | emit + boundary check + deliver + drain |
| `signal_fanout` 4 handlers | ~1.0 µs | |
| `signal_fanout` 16 handlers | ~1.4 µs | Very flat — mpsc try_send is cheap |

`scan_prefix` uses a prefix index for a fast O(|segment_keys|) path when the prefix segment is known (e.g. `"load/"`, `"grp/"`, `"svc/"`). Unknown prefixes fall back to an O(store_size) full scan. At typical pheromone-trail sizes (100–1,000 entries per segment) the cost is negligible relative to network latency.

## Security Model

mycelium supports **mutual TLS** (mTLS) on the gossip TCP port via the optional `tls` cargo
feature. When enabled, every peer connection requires a valid cluster-CA-signed Ed25519
certificate. Nodes without the shared CA are rejected at the TLS handshake before any data
is exchanged.

**Enable with:**
```toml
[dependencies]
mycelium = { version = "…", features = ["tls"] }
```
```rust
config.tls = Some(TlsConfig::default()); // auto-generates CA + certs in ./mycelium-tls/
```

**What the `tls` feature provides:**
- **mTLS transport** — `tokio-rustls` mTLS on every gossip TCP connection; plain nodes cannot connect.
- **Ed25519 node identity** — each node's TLS cert key is its identity keypair; the 32-byte
  verifying key is gossiped to `sys/identity/{node}` and cached in `peer_keys`.
- **Signed consensus payloads** — all `Propose`, `Vote`, `Nack`, and `Commit` messages are
  Ed25519-signed by the sender; forged ballots are dropped on receipt.

**Without the `tls` feature** (default), mycelium operates in a **trusted domain** — all nodes
on the gossip mesh are assumed to be cooperative. A connected peer can:
- Send crafted frames to inject arbitrary KV entries (limited by LWW timestamps)
- Claim any `NodeId` in a `StateRequest` (consequence: misdirected `StateResponse`, harmless)
- Poison a nonce in the dedup seen-set (probability: < 1/2⁶⁴ per collision)

**Do not expose gossip ports to untrusted networks** when running without TLS. Use a
network-layer control (firewall rules, WireGuard, VPC security groups) to restrict access
to the gossip port to trusted peers only.

### HTTP Gateway Security

The embedded HTTP gateway (`/health`, `/stats`, `/gateway/*`) has **no authentication by
default**. Treat it the same as the gossip port: bind it to a loopback or private interface,
or restrict access at the network layer.

For deployments where the gateway must be reachable on a shared interface, enable bearer-token
authentication:

```rust
config.gateway_auth_token = Some("your-secret-token".to_string());
// or via environment variable: GOSSIP_GATEWAY_AUTH_TOKEN=your-secret-token
```

When set, every request to the gateway must include `Authorization: Bearer <token>`.
The `/health`, `/ready`, `/stats`, and `/metrics` endpoints remain public intentionally —
they carry no sensitive data and are needed for load-balancer health probes and Prometheus
scraping without credential configuration.

### Role-Based Access Control (`compliance` feature)

The `compliance` feature (`= ["gateway", "tls"]`) layers OAuth2-style authorization on top of
the bearer model and adds signed, verifiable node roles. It is **opt-in and backward-compatible**:
without it the types below compile away; with it but unconfigured, behaviour is unchanged.

**OAuth2 scope-based gateway ACLs.** Map each bearer token to a set of `resource:verb` scopes;
every `/gateway/**` route requires a scope and admits a token only if its grant holds that scope
or the `"*"` wildcard. Deny-by-default — an unmapped route requires `admin`.

```rust
config.gateway_scoped_tokens = vec![
    GatewayToken { token: "orchestrator".into(),
                   scopes: vec!["kv:read".into(), "kv:write".into(), "mesh:write".into()] },
    GatewayToken { token: "readonly".into(),
                   scopes: vec!["kv:read".into()] },
];
```

The legacy `gateway_auth_token` is equivalent to a token holding `["*"]`, so single-token
deployments upgrade with no change. The public edge (`/health`, `/ready`, `/stats`, `/metrics`,
the A2A descriptor) is never scope-gated.

**SSO via generic OIDC.** Set `GossipConfig::oidc = Some(OidcConfig { issuer, audience,
group_claim, group_scopes, .. })` and the gateway also accepts an IdP-issued JWT bearer:
it validates the token (asymmetric-only algorithms — anti alg-confusion — plus `iss`/`aud`/`exp`)
against the IdP's JWKS (standard `.well-known` discovery, cached), then maps the token's
groups to gateway scopes. One code path for Entra / Okta / Auth0 / Keycloak — differences are
config. Human-operator auth, not agent identity. See the [SSO runbook](docs/operations/sso.md).

**Signed node roles + capability authorization.** `agent.advertise_roles(["admin".into()], 3)`
writes an Ed25519-signed claim to `sys/role/{node}`; `agent.roles_of(node)` returns it **only**
if the signature verifies against the node's cluster-learned identity key — a forged role write
reads back as `None`. A capability provider enforces its `authorized_callers` allowlist with
`agent.caller_authorized(req.sender(), &allow)` at the point it *serves* a request (the only
place the allowlist is genuinely enforceable). This is the *detection-not-prevention* posture:
the substrate never blocks a write, it makes an unauthorized one legible — see also the `sys/`
namespace-ownership tripwire (`/stats` → `sys_namespace_violations`).

### Tamper-Evident Audit Trail (`compliance` feature)

Every node keeps its **own** Ed25519-signed, SHA-256 hash-chained audit stream at
`sys/audit/{node}/{seq}`. Each record's `prev_hash` links to its predecessor, so removing,
reordering, or editing a record breaks verification of that stream. The chain is per-node by
necessity — a single global chain would need a sequencer, i.e. a coordinator, which the design
forbids; the cluster trail is the union of independently verifiable streams.

```rust
// Seal an event; returns the stable, citable content hash.
let h = agent.audit(AuditAction::Invoke, caller.to_string(), "orders/place",
                    AuditOutcome::Success, None)?;

// Verify a node's whole stream against its identity key.
agent.audit_verify(&node)?;     // Err names the first bad seq
```

SkillRunner routes every invocation into this trail (the verified caller is the principal).
Query and verify over HTTP with `GET /gateway/audit` (scope `audit:read`), which returns each
stream's `verified` status, chain-tip `head_hash`, and a per-record `content_hash`. Detection,
not prevention: records are plain replicated KV entries; tampering makes verification fail.

### Crown-Jewel Posture — data-at-rest & egress

Two opt-in, feature-free controls for blast-radius containment (see the
[threat model](docs/threat-model.md) and [crown-jewel runbook](docs/operations/crown-jewel.md)):

- **Data-at-rest encryption.** `agent.with_data_at_rest_cipher(Arc::new(my_cipher))` envelope-
  encrypts WAL records and snapshots before they hit disk and decrypts on replay. You implement
  `DataAtRestCipher` over your KMS/keyring — the substrate stays neutral on key custody. Scope is
  on-disk only; the wire is mTLS (`tls`), memory is unencrypted.
- **Outbound egress allowlist.** `GossipConfig::egress = EgressPolicy { allow_hosts }` constrains
  which external hosts the substrate may reach (enforced at the MCP client bridge; empty = allow
  all). A node-local posture, not a coordinator. Other outbound paths (LLM, probes, A2A) are
  restricted at the network layer today — see the runbook for the coverage table.

### Hot Identity Rotation (`tls` feature)

`agent.rotate_identity(propagation).await?` rotates a node's Ed25519 TLS/identity key with **no
cluster disruption**: it publishes `new‖old` to `sys/identity/` (signed by the old key), waits a
gossip window, then atomically swaps the active key + cert. New connections use the new cert;
existing sessions persist (no listener restart). Verification uses a **retained key set** per node
(accumulated across rotations), so historical signatures — the audit chain, committed consensus,
role claims — keep verifying across a rotation. See the
[rotation runbook](docs/operations/cert-rotation.md). (Caveat: a retired key stays accepted for
verification, so compromise response needs explicit revocation, not just rotation.)

## Layer III — Consensus

Lightweight epidemic two-phase agreement built directly on top of the signal mesh — no extra
wire format, no separate consensus port. All consensus messages ride existing `Signal` frames.

### Protocol sketch

```
Propose → (votes from group members) → Commit → KV committed/{slot}
```

Committed values are written to `consensus/committed/{slot}` and anti-entropy-synced to
late joiners automatically via the existing KV mechanism.

### API

```rust
use mycelium::{ConsensusConfig, ConsensusResult};
use bytes::Bytes;

// Every node that should vote calls this once.
let _listener = agent.start_consensus_listener();

// Propose within a group — blocks until quorum or timeout.
let cfg = ConsensusConfig { quorum_size: 0, ..ConsensusConfig::default() };
match agent.group_propose("workers", "coordinator", Bytes::from("node-7"), cfg).await {
    ConsensusResult::Committed { slot, value, ballot } => {
        println!("committed: {} = {:?} @ ballot {}", slot, value, ballot);
    }
    ConsensusResult::Timeout { ballots_tried, votes_last_ballot, quorum_required, .. } => {
        println!("no quorum after {} ballots; last ballot got {}/{} votes",
                 ballots_tried, votes_last_ballot, quorum_required);
    }
    ConsensusResult::Superseded { slot, ballot } => {
        // Another node reached quorum first; read the committed value.
        let v = agent.consensus_get(&slot).unwrap();
        println!("superseded at ballot {}: {:?}", ballot, v);
    }
}

// System-wide proposal (all known peers vote).
let _ = agent.system_propose("global/epoch", Bytes::from("42"), ConsensusConfig::default()).await;

// Subscribe to a slot — fires whenever the slot is committed.
let mut rx = agent.consensus_rx("coordinator");

// Quorum trust slices (SCP §3.1 — optional, stored for future slice-aware extensions).
agent.declare_trust("workers", &[peer_a, peer_b]);
let slices = agent.group_trust("workers");
```

### Key design decisions

| Decision | Rationale |
|---|---|
| Ballot numbering (SCP §6.2) | Monotonic counter at `consensus/ballot/{slot}`; higher ballot supersedes stale commits |
| Group-scoped votes | All members hear all votes → any member reaching quorum can commit; proposer crash does not stall the slot |
| Proposer self-votes | Proposer always counts as one voter; no listener required for single-node quorum |
| LWW commit idempotency | Two simultaneous commits of the same value are safe; higher-ballot commit wins via LWW timestamp |
| No ordering log | Each slot is an independent KV entry (CASPaxos-style); no WAL required |
| Signing | With `tls` feature: all consensus payloads are Ed25519-signed; forged ballots are dropped. Without: trusted-domain only; Byzantine fault tolerance is out of scope |

`quorum_size = 0` uses `floor(N/2) + 1` (simple majority). `max_peers` cap and
`phase1_timeout` are tunable via `ConsensusConfig`.

## Capability Subsystem

First-class capability advertisement, discovery, demand pressure, and locality-aware routing — all
built on the Layer I KV store. Nodes declare what they offer (`advertise_capability`), what they
need (`declare_requirement`), and how much demand exists relative to supply (`demand`). No external
registry; everything lives under the `cap/`, `req/`, and `gcap/` namespaces and anti-entropy-syncs
to late joiners automatically.

**Schema versioning** — capabilities carry an optional `schema_id` (e.g. `"acme-ml/v2"`) that is
gossip-propagated alongside the capability entry. Callers that need a specific contract version use
`CapFilter::with_schema("acme-ml/v2")`; providers without that `schema_id` are silently excluded.
This prevents silent semantic mismatches when multiple teams advertise the same `(namespace, name)`
with incompatible payload shapes. Input and output JSON Schemas are also embeddable directly in the
capability (`with_input_schema` / `with_output_schema`) so callers can inspect contracts from
`resolve()` results without a separate KV lookup.

→ The **Capability Market** preset in the [Mesh Control UI](examples/mesh_control.html) demonstrates
providers, requirers, and per-capability demand-pressure bars across four capability types.

### Advertising and Resolving Capabilities

```rust
use mycelium::{Capability, CapFilter, CapabilityHandle};
use std::time::Duration;

// Advertise — periodically reasserts cap/{node_id}/{ns}/{name} in the KV store.
// Drop the handle to stop advertising; the tombstone propagates automatically.
let handle: CapabilityHandle = agent.advertise_capability(
    Capability::new("compute", "gpu")
        .with_schema_id("acme-ml/v2")                      // optional contract version
        .with_input_schema(r#"{"type":"object"}"#)          // gossip-propagated JSON Schema
        .with_output_schema(r#"{"type":"string"}"#),
    Duration::from_secs(30),  // reassert interval
);

// Resolve — snapshot of every node currently advertising a matching capability.
// Without with_schema: all providers regardless of schema_id.
let filter = CapFilter::new("compute", "gpu");
let matches: Vec<(NodeId, Capability)> = agent.resolve(&filter);

// With schema constraint: only providers advertising schema "acme-ml/v2".
// Providers with no schema_id or a different schema_id are excluded.
let filter_v2 = CapFilter::new("compute", "gpu").with_schema("acme-ml/v2");
let v2_providers = agent.resolve(&filter_v2);

// Inspect the payload contract from the resolved capability.
if let Some((node, cap)) = v2_providers.first() {
    if let Some(schema) = &cap.input_schema {
        // validate your payload against schema before rpc_call
    }
}

// Watch — push-based; fires when the matching set changes.
// Debounced: burst KV writes within 50 ms collapse to one notification.
let mut rx: watch::Receiver<Vec<(NodeId, Capability)>> = agent.watch_capabilities(filter);
rx.changed().await?;
let current = rx.borrow().clone();
```

### Schema Registry — Publish and Govern Contracts

Schemas live in the gossip KV ring under `schemas/{schema_id}`. Any node can read
them; they propagate via anti-entropy like all other KV state. The inline
`input_schema` / `output_schema` fields on each capability are a gossip-propagated
snapshot — callers inspect the contract from `resolve()` without a separate lookup.

```rust
use mycelium::SchemaPublishResult;

// Publish once at startup (or seed a whole directory).
// Returns Published / Unchanged / Conflict — never silently overwrites.
let schema = br#"{"type":"object","required":["prompt"],"properties":{"prompt":{"type":"string"}}}"#;
match agent.schemas().publish_schema("acme/ml-inference/v1", schema).await? {
    SchemaPublishResult::Published          => println!("registered"),
    SchemaPublishResult::Unchanged          => println!("already up to date"),
    SchemaPublishResult::Conflict { existing } => eprintln!("conflict: {:?}", existing),
}

// Seed all *.json files from a directory; schema_id = relative path without extension.
// schemas/acme/ml-inference/v1.json  →  schema_id "acme/ml-inference/v1"
let results = agent.schemas().seed_schemas_from_dir("./schemas").await;

// Look up and enumerate
let bytes  = agent.schemas().get_schema("acme/ml-inference/v1");
let all    = agent.schemas().list_schemas();  // Vec<(schema_id, json_bytes)> sorted by id

// Force-overwrite (development / migration only — never use in production CI)
agent.schemas().force_publish_schema("acme/ml-inference/v1", updated_schema).await?;
```

See [docs/guide/12-schema-lifecycle.md](docs/guide/12-schema-lifecycle.md) for the
full lifecycle guide: naming conventions, CI/CD gate, rollout window, and the
`v1 → v2` migration pattern.

### Requirements — Declare What You Need

```rust
// Declare — periodically writes req/{node_id}/{ns}/{name} to the KV store.
// Visible to demand watchers on any node; used by orchestrators and autoscalers.
let _handle = agent.declare_requirement(
    CapFilter::new("compute", "gpu"),
    Duration::from_secs(30),
);

// Watch requirement status — fires when the provider set changes relative to this
// node's declared need.
let mut rx = agent.watch_requirement(CapFilter::new("compute", "gpu"));
rx.changed().await?;
let status = rx.borrow();
println!("satisfied: {}", status.is_satisfied());
for provider in &status.providers {
    println!("  provider: {}", provider.node_id);
}
```

### Demand Pressure

`demand_pressure` is `demanding_nodes.len() / max(providers.len(), 1)`. Pressure > 1.0 means
demand outstrips supply. The library never auto-responds to high pressure — this is a signal
for orchestrators, autoscalers, and dashboards.

```rust
let filter = CapFilter::new("compute", "gpu");

// Snapshot
let status: DemandStatus = agent.demand(&filter);
println!("{} demanding, {} providing, pressure {:.2}",
    status.demanding_nodes.len(), status.providers.len(), status.demand_pressure);

// Push-based — debounced, fires on req/, cap/, or gcap/ changes matching filter
let mut rx = agent.watch_demand(filter);
rx.changed().await?;
let s = rx.borrow();
if s.demand_pressure > 2.0 {
    eprintln!("demand critical: {:.2}", s.demand_pressure);
}
```

### Emergent Capability Groups

Nodes that share a capability automatically form a named group via `define_capability_group`.
The library projects their collective capability under `gcap/{group}/{ns}/{name}/{contributor}`
and handles group-level requirement wiring. One consolidated task per group keeps task count
O(groups), not O(groups × members).

→ The **Emergent GPU Pool** preset in the [Mesh Control UI](examples/mesh_control.html) shows a
20-node worker pool that assembles dynamically and fans out render jobs to all members.

```rust
use mycelium::{CapabilityGroupDef, CapFilter, Capability};
use std::time::Duration;

// Any node that advertises compute/gpu joins the "gpu-pool" group automatically.
// The library maintains gcap/ projections and group-level wiring.
agent.define_capability_group(
    "gpu-pool",
    CapabilityGroupDef {
        filter:   CapFilter::new("compute", "gpu"),
        provides: vec![Capability::new("compute", "gpu")],
        requires: vec![],
    },
    Duration::from_secs(60),  // reassert interval
);
```

### Inter-Group Wiring

Wiring connects a consumer's declared requirement to provider groups without the consumer needing
to know which nodes are in the group or how many there are. `signal_wired_via` dispatches a signal
to all matching providers in one call.

```rust
use mycelium::CapFilter;

let filter = CapFilter::new("compute", "gpu");

// Snapshot of wiring state — WiringStatus::Wired or WiringStatus::Unwired
let wiring: WiringStatus = agent.resolve_wiring(&filter);

// Push-based wiring watch
let mut rx = agent.watch_wiring(filter.clone());

// Dispatch to all wired providers — returns Emitted{providers} or Unwired{filter}
let outcome = agent.signal_wired_via(&filter, "render-job", payload).await;
```

### Locality-Aware Resolution

Each node declares a `locality_path` in its config (coarse → fine: `["az1", "rack2", "host3"]`).
`resolve_with_locality` sorts providers by shared-prefix depth with the caller — topologically
closest first. `signal_wired_via_locality` combines wiring with locality preference in one call.

→ The **Locality Mesh** preset in the [Mesh Control UI](examples/mesh_control.html) covers 12 nodes
across two availability zones: remove a close provider and the resolver shifts to the next ring.

```rust
// Config — set once before agent.start()
config.locality_path = vec!["az1".to_string(), "rack2".to_string(), "host3".to_string()];

// resolve_with_locality returns (NodeId, Capability, depth) sorted by depth desc.
// depth = length of shared locality prefix between this node and the provider.
let candidates = agent.resolve_with_locality(
    &CapFilter::new("render", "job"),
    LocalityPreference::PreferShared(0),  // prefer closest; fall back to any
);
for (node_id, _cap, depth) in &candidates {
    println!("  {node_id} depth={depth}");
}

// Route to locality-closest provider via wiring
agent.signal_wired_via_locality(
    &CapFilter::new("render", "job"),
    LocalityPreference::PreferShared(0),
    "render-job",
    payload,
).await;
```

`LocalityPreference` variants:
- `Any` — no locality preference; returns all providers
- `PreferShared(min_depth)` — prefer providers at shared depth ≥ min_depth; fall back to any
- `Strict(min_depth)` — only providers at depth ≥ min_depth; empty if none qualify

---

## Service Layer — RPC, Bulk, Scatter-Gather, Mailbox

Layer 3 delivers the service primitives used by the language bridges and the MCP integration.

### Point-to-Point RPC

```rust
// Caller
let reply = agent.rpc_call(target, "echo", payload, Duration::from_secs(5)).await?;

// Responder
let mut rx = agent.rpc_rx("echo");
while let Some(req) = rx.recv().await {
    agent.rpc_respond(&req, req.payload());
}
```

### Bulk Payload Transfer

For payloads too large to gossip through every node, `bulk_call` stages the data at a local
HTTP endpoint and sends only a lightweight ticket over the mesh:

```rust
// Set http_port in GossipConfig so the target can fetch the staged bytes
let reply = agent.bulk_call(target, "process", large_bytes, Duration::from_secs(30)).await?;
```

### Scatter-Gather

Fan out an identical request to multiple targets concurrently; return as soon as `min_ok` replies arrive:

```rust
let results = agent.scatter_gather(targets, "vote", payload, Duration::from_secs(5), 2).await?;
```

### Actor/Event Mailboxes

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

## Opt-In Consistency and Ordering Overlay

Mycelium's thesis is **consistency as a service, not a foundation** — the epidemic substrate
is always fast; stronger guarantees are opt-in per operation. The overlay layer surfaces these
as first-class APIs without touching the gossip core.

### Consensus KV (`consistent_set` / `consistent_get`)

Runs a ballot-voting round before writing. Concurrent writes to the same key are totally
ordered by ballot number; the highest-ballot value is the authoritative committed entry.

`consistent_get` is a **local read** — it returns the latest committed value that has
anti-entropy-propagated to this node, which may lag by up to one gossip round. This is
suitable for leader election and distributed locks where ballot-based fencing tokens protect
against lower-ballot writers; it is not a substitute for linearizable reads.

```rust
// Any node can write — concurrent writers are ordered by ballot number
agent.consensus().consistent_set("config/endpoint", b"https://api.v2/").await?;
let val = agent.consensus().consistent_get("config/endpoint"); // local read, eventually consistent
```

### Distributed Lock (`distributed_lock`)

Consensus-backed named lock. The returned `LockGuard` releases (tombstones the lock key)
on drop. The `token` field is a monotonic fencing token drawn from the ballot number.

```rust
let guard = agent.distributed_lock("job-42", Duration::from_secs(30)).await?;
println!("fencing token: {}", guard.token);
// exclusive work here
drop(guard); // or guard.release()
```

### Leader Election (`elect_leader`)

One-shot election per group. If this node loses it reads the committed winner and returns
that `NodeId` — so all nodes converge on the same answer.

```rust
let leader = agent.elect_leader("shard-0").await?;
if leader == *agent.node_id() {
    // I won — start serving shard-0
}
```

### Ordered Durable Log (`append` / `scan_log` / `subscribe_log`)

HLC-keyed entries written to the gossip KV under `log/{stream}/{hlc:016x}`. Lexicographic
key order equals causal time order.

```rust
// Producer
let cursor = agent.kv().append("events", b"order-placed");

// Consumer — one-shot scan
let entries = agent.kv().scan_log("events", 0, u64::MAX);

// Live subscriber — mpsc channel, new entries arrive on each gossip tick
let mut rx = agent.kv().subscribe_log("events", 0);
while let Some(entry) = rx.recv().await {
    println!("{} {:?}", entry.hlc, entry.value);
}

// Trim old entries
agent.kv().compact_log("events", checkpoint_hlc);
```

### Consumer Groups (`subscribe_log_group`)

At most one consumer per group advances at a time. The offset (`clog/{stream}/{group}/offset`)
is persisted in the gossip KV so any node can take over if the current holder fails.

```rust
let mut rx = agent.kv().subscribe_log_group("events", "workers").await;
while let Some(entry) = rx.recv().await {
    process(&entry);
    // offset committed before next entry is delivered
}
```

### Reliable Delivery (`emit_reliable`)

Send a payload to a specific node and wait for an explicit application-level ACK (the
receiver calls `rpc_respond`). Returns `AckResult::Acknowledged` or `AckResult::Timeout`.

```rust
let ack = agent.emit_reliable(target, "task.assign", payload, Duration::from_secs(5)).await;
```

### Docker integration-test cluster (developer template)

The overlay scenarios in `tests/overlay/` are designed as copy-paste templates:

| Scenario | Pattern demonstrated |
|---|---|
| [`s11_task_auction.py`](tests/overlay/scenarios/s11_task_auction.py) | Exact-once task delivery — coordinator queues work, workers race via `subscribe_log_group` |
| [`s12_leader_election.py`](tests/overlay/scenarios/s12_leader_election.py) | Leader election + consensus-durable config — 3 concurrent `elect_leader` calls must converge, winner writes `consistent_set` |
| [`s13_shared_reasoning_log.py`](tests/overlay/scenarios/s13_shared_reasoning_log.py) | Multi-writer append — 3 nodes each write observations, all verify HLC ordering and gossip convergence |

```sh
make test-overlay   # 3-node Docker cluster, runs all three scenarios (~3 min on warm cache)
```

See [`tests/overlay/README.md`](tests/overlay/README.md) for the full developer guide.

---

## Python Language Bridge (`mycelium-py`)

Python agents connect to a running Mycelium node over loopback HTTP (~1 ms overhead).
No PyO3 FFI — the sidecar pattern works with any language that can speak HTTP.

```sh
cd mycelium-py
pip install -e ".[dev]"
```

```python
from mycelium import MyceliumAgent

agent = MyceliumAgent("127.0.0.1", 8300)

# Capabilities
handle = agent.advertise_capability("compute", "gpu", attributes={"model": "A100"})
providers = agent.resolve_capability("compute", "gpu")

# Signals
agent.emit("render-job", b"payload", scope="system")
async for sig in agent.on_signal("render-job"):
    print(sig.sender, sig.payload)

# RPC
result = agent.rpc_call(target_id, "echo", b"ping")
async for req in agent.rpc_serve("echo"):
    agent.rpc_respond(req, req.payload)

# KV store
agent.set("my/key", b"value")
val = agent.get("my/key")   # bytes | None

# Mailbox
agent.deliver_event(target_id, "task.result", b"done")
async for event in agent.mailbox("task.result"):
    print(event.payload)
```

See [`mycelium-py/README.md`](mycelium-py/README.md) for the full API reference.

---

## TypeScript Language Bridge (`mycelium-ts`)

TypeScript agents connect to a running Mycelium node over loopback HTTP.
No native extension — same sidecar pattern as the Python SDK.

```sh
cd mycelium-ts
npm install
npm run build
```

```typescript
import { MyceliumAgent } from "mycelium-ts";

const agent = new MyceliumAgent("127.0.0.1", 8300);

// Capabilities
const handle = await agent.advertiseCapability("compute", "gpu", {
  attributes: { model: "A100" },
});
const providers = await agent.resolveCapability("compute", "gpu");

// Signals
await agent.emit("render-job", Buffer.from("payload"), { scope: "system" });
for await (const sig of agent.onSignal("render-job")) {
  console.log(sig.sender, sig.payload);
  break;
}

// KV store
await agent.set("my/key", Buffer.from("value"));
const val = await agent.get("my/key");  // Buffer | null

// RPC
const reply = await agent.rpcCall(target, "echo", Buffer.from("hi"));
for await (const req of agent.rpcServe("echo")) {
  await agent.rpcRespond(req, req.payload);
}

await handle.drop();
```

**Requires Node.js ≥ 18.** See [`mycelium-ts/README.md`](mycelium-ts/README.md) for the full API reference.

---

## Observability — Prometheus Metrics

Enable the `metrics` feature to expose a Prometheus scrape endpoint on every node's HTTP gateway port:

```toml
# Cargo.toml
mycelium = { version = "0.1", features = ["metrics"] }
```

```sh
# Build
cargo build --features metrics

# Scrape
curl http://127.0.0.1:8300/metrics
```

### Metrics reference

| Metric | Type | Description |
|--------|------|-------------|
| `gossip_kv_writes_total` | Counter | Local KV writes (all nodes, including `set_with_min_acks`) |
| `gossip_kv_deletes_total` | Counter | Local KV tombstones |
| `gossip_store_entries` | Gauge | Current live KV entry count |
| `gossip_messages_received_total` | Counter | Inbound gossip Data frames applied |
| `gossip_anti_entropy_rounds_total` | Counter | Anti-entropy StateResponse rounds received |
| `gossip_frames_dropped_total` | Counter | Frames dropped during peer reconnect backoff |
| `gossip_signals_emitted_total{scope}` | Counter | Signals emitted, labelled by scope (`system` / `group` / `node`) |
| `gossip_signals_delivered_total{kind}` | Counter | Signals delivered to local handlers, labelled by kind |
| `gossip_signals_rejected_total` | Counter | Signals suppressed by the load-shedding boundary |
| `gossip_rpc_latency_ms` | Histogram | Full round-trip latency for `rpc_call` calls |

### Prometheus scrape config

```yaml
scrape_configs:
  - job_name: mycelium
    static_configs:
      - targets:
          - "node1:8300"
          - "node2:8300"
          - "node3:8300"
    metrics_path: /metrics
```

A pre-built Grafana dashboard is available at [`dashboards/mycelium-grafana.json`](dashboards/mycelium-grafana.json).
Import it via **Dashboards → Import** in the Grafana UI and select your Prometheus datasource.

---

## SkillRunner — LLM Agents as Mesh Nodes

`skillrunner` is a standalone binary that turns a `.skill.toml` manifest into
a live LLM agent node on the mesh. No Rust required. Write a manifest, point it
at any OpenAI-compatible LLM server, and the node self-advertises its capability,
handles invocations via RPC, and writes a signed audit trail — all automatically.

```sh
cargo build --bin skillrunner
./target/debug/skillrunner --skill examples/skills/hello.skill.toml
```

### How a skill works

When a skill node starts:
1. It joins the mesh and **advertises its capability** (`ns`/`name`) into the gossip KV store
2. Any caller that wants `llm/chat` does `resolve("llm", "chat")` — the mesh returns the node's address
3. The caller sends an RPC with the input JSON; the skill runs its LLM prompt with that input and returns the result
4. An audit record (signed with the node's Ed25519 key, HLC-timestamped) is written to the mesh

No service registry. No coordinator for discovery or routing. The mesh *is* the registry.

> **Scope of "no coordinator":** The gossip KV layer and signal mesh are fully
> coordinator-free. The opt-in consistency overlay (`consistent_set`, `distributed_lock`,
> `elect_leader`) uses epidemic Paxos and requires a live majority — those specific
> operations have a proposer and will stall under partition. `bootstrap_peers` acts as a
> soft coordinator for initial cluster discovery; keep 2–3 long-lived seed nodes for
> reliable join behaviour.

### Minimal skill manifest

```toml
[node]
bind_port       = 7947
bootstrap_peers = ["127.0.0.1:7946"]   # address of any existing mesh node

[capability]
ns          = "llm"
name        = "chat"
description = "Responds to any message"

[capability.input]
type = "object"
required = ["message"]
[capability.input.properties]
message = { type = "string", description = "The user's message" }

[capability.output]
type = "object"
[capability.output.properties]
reply = { type = "string" }

[skill]
prompt = "You are a helpful assistant. Return JSON: {\"reply\": \"<response>\"}."
tools  = []

[skill.llm]
endpoint = "http://localhost:11434/v1"   # Ollama or any OpenAI-compatible endpoint
model    = "llama3.2"
```

### Skill composition — skills calling skills

A skill can declare other skills as `tools`. The orchestrator below calls a
researcher and a writer without knowing their addresses:

```toml
[skill]
prompt = "Coordinate llm/researcher and llm/writer to produce an article on the topic."
tools  = ["llm/researcher", "llm/writer"]   # resolved at inference time via gossip
```

At inference time SkillRunner resolves `llm/researcher` against live capability
advertisements in the KV store, dispatches the sub-invocation through the mesh,
and injects the result back into the LLM context. Start a second researcher node
and the orchestrator automatically load-balances across both.

This is the composition story — see [`examples/community/`](examples/community/)
for a full 3-skill walkthrough with live monitoring instructions.

### Calling a skill from any node

**Rust:**
```rust
let (node_id, _) = agent.resolve(&CapFilter::new("llm", "chat"))[0];
let payload = serde_json::to_vec(&json!({"message": "Hello!"}))?;
let result = agent.rpc_call(node_id, "skill.invoke", payload, Duration::from_secs(30)).await?;
```

**Python (`mycelium-py`):**
```python
from mycelium import MyceliumAgent
import json

agent = MyceliumAgent("127.0.0.1", 8300)
providers = agent.resolve_capability("llm", "chat")
result = agent.rpc_call(providers[0].node_id, "skill.invoke",
                        json.dumps({"message": "Hello!"}).encode())
```

### Ready-to-run examples

| Example | What it shows |
|---|---|
| [`examples/skills/hello.skill.toml`](examples/skills/hello.skill.toml) | Minimal single-skill smoke test |
| [`examples/skills/summarizer.skill.toml`](examples/skills/summarizer.skill.toml) | Structured JSON output with input schema |
| [`examples/community/`](examples/community/) | 3-skill composition: orchestrator → researcher → writer |
| [`examples/a2a_langchain/`](examples/a2a_langchain/) | LangChain + AutoGen auto-discovering skills via A2A |

See [`docs/reference/skillrunner.html`](docs/reference/skillrunner.html) for the full manifest
reference, A2A auto-discovery, OTEL integration, concurrency controls, and the audit trail format.

---

## Prompt Skills — LLM-backed capabilities via the KV substrate

The `llm` feature turns any `GossipAgent` into a host for LLM-backed skills. Prompt templates
are stored in the gossip KV store (`prompts/{ns}/{name}`) and replicated cluster-wide — any
other node can call the skill without knowing which node hosts the model.

```toml
mycelium = { version = "…", features = ["llm"] }
```

### Registering a skill

```rust
use mycelium::{GossipAgent, PromptTemplate, OpenAiBackend};

let backend = OpenAiBackend::new(
    "http://localhost:11434/v1",   // any OpenAI-compatible endpoint
    "",                             // API key (empty for Ollama)
    "llama3.2",                    // model baked in at construction
);

let template = PromptTemplate {
    system: "You are a helpful assistant. Reply concisely.".into(),
    user_template: "{{input}}".into(),
    max_tokens: 512,
    temperature: 0.7,
    metadata: Default::default(),
};

// Advertises cap `llm/chat` on the mesh; template stored in KV with TTL=1 week.
let _handle = agent.register_prompt_skill("llm", "chat", template, backend).await?;
// Drop _handle to retract the capability and stop the dispatch loop.
```

### Calling from Rust

```rust
let output = agent
    .call_prompt_skill("llm", "chat", "Hello!", Default::default(), Duration::from_secs(30))
    .await?;
println!("{output}");
```

### HTTP gateway

```sh
# List all templates visible to this node
curl http://localhost:8300/gateway/prompts

# Read a specific template
curl http://localhost:8300/gateway/prompts/llm/chat

# Write / update a template
curl -X PUT http://localhost:8300/gateway/prompts/llm/chat \
     -H 'Content-Type: application/json' \
     -d '{"system":"You are a helpful assistant.","user_template":"{{input}}","max_tokens":512,"temperature":0.7}'

# Invoke (blocking)
curl -X POST http://localhost:8300/gateway/llm/call \
     -H 'Content-Type: application/json' \
     -d '{"ns":"llm","name":"chat","input":"What is 2+2?"}'

# Invoke (SSE stream — v1 emits a single `done` event)
curl -N http://localhost:8300/gateway/llm/stream \
     -H 'Content-Type: application/json' \
     -d '{"ns":"llm","name":"chat","input":"Hello!"}'
```

### Python (`mycelium-py`)

```python
from mycelium.prompt_skill import PromptSkillClient, PromptTemplate

client = PromptSkillClient("http://localhost:8300")

template = PromptTemplate(
    system="You are a helpful assistant.",
    user_template="{{input}}",
    max_tokens=512,
    temperature=0.7,
)
client.register("llm", "chat", template)

result = client.call("llm", "chat", "What is 2+2?")
print(result.output)          # "4" or similar
print(result.model_used)      # "llama3.2"
print(result.tokens_used)     # 12
```

### Template variables

In `user_template`, `{{input}}` is always replaced with the caller's input string.
`{{node_id}}` and `{{skill_name}}` are injected automatically. Additional key-value
pairs can be passed in the `context` map and referenced as `{{key}}`.

### Model placement

The `model` field is **not** in `PromptTemplate` — model availability is node-local knowledge
that the template author cannot predict. Each hosting node bakes the model into its `LlmBackend`
at construction. The `LlmResult.model_used` field reports what was actually used, so callers
have full observability without requiring central coordination.

---

## Configuration

Pass a TOML config file with `-c <path>`. CLI flags override file values.
Environment variables override both — `GOSSIP_<FIELD_NAME>` for every field.

| Field | Default | Description |
|---|---|---|
| `bind_address` | `127.0.0.1` | TCP listen address |
| `bind_port` | `8080` | TCP listen port |
| `bootstrap_peers` | `[]` | Peers to contact on startup |
| `default_ttl` | `5` | Hops before a message expires |
| `health_check_interval_secs` | `10` | Ping interval and peer eviction cadence |
| `propagation_window_secs` | `60` | Tombstone retention window |
| `max_connections` | `1024` | Inbound connection limit |
| `writer_channel_depth` | `1024` | Per-peer outbound channel depth (ring buffer). **Correctness threshold** — frames silently dropped when full. Covers `N × fan_out` up to N = 256 at the default fan-out of 4; size up for larger fleets or bulk-write bursts. A saturation warning fires every 1 000th cumulative dropped frame. |
| `max_forwarding_peers` | unlimited | Cap gossip fan-out targets. Set to `bootstrap_peers.len()` for fixed-topology meshes |
| `max_peers` | unlimited | Cap the peer table. Prevents O(N²) persistent connections when piggybacked peer lists would otherwise expand every node's view of the full cluster. Set to `bootstrap_peers.len()` for grid or ring topologies |
| `gossip_channel_capacity` | `1024` | Per-shard gossip channel depth |
| `gossip_shards` | `min(CPU,16)` | Gossip worker tasks. Set to `1` for demos/debug to cut task count |
| `max_seen_entries` | `100000` | Dedup cache size before eviction |
| `peer_eviction_intervals` | `3` | Missed ping intervals before a peer is evicted |
| `reconnect_backoff_secs` | `5` | Cooldown after a failed connect |
| `epidemic_extra_peers` | `3` | Extra random non-member peers added to Group-scoped signal fan-out when `group_aware_forwarding = true`. Ensures epidemic coverage beyond the group. Raise to 5–7 for clusters > 1 000 nodes. |
| `group_aware_forwarding` | `true` | When true, Group signals are forwarded only to known group members plus `epidemic_extra_peers` random non-members. Set to `false` to revert to pre-v0.2 broadcast forwarding. |
| `writer_idle_timeout_secs` | `0` (disabled) | Seconds of inactivity before a peer writer closes its TCP connection. Reconnects transparently on the next frame. `0` = no timeout. |
| `signal_window_secs` | `600` | Retention window for the in-memory sender log and `quorum_written` rate-limit tracker. |
| `max_store_entries` | `0` (unlimited) | Hard cap on live KV entries. New live writes are silently dropped once reached; tombstones always accepted. |
| `intern_keys` | `true` | Intern received keys in a process-wide pool so all connection handlers share one `Arc<str>` per distinct key. Disable for workloads with unbounded key spaces (e.g. UUID keys). |
| `intern_max_keys` | `0` (unlimited) | Maximum keys in the intern pool. New keys bypass interning once reached. Only meaningful when `intern_keys = true`. |
| `health_check_max_jitter_ms` | `0` | Startup jitter cap (ms) before the first health-check ping. `0` = up to `health_check_interval_secs × 500` ms. Set to a small value (e.g. `50`) in test configs. |

## License

Mycelium is released under the [GNU Affero General Public License v3.0](LICENSE) (AGPL-3.0-only).

**Open use:** Any project distributed under a compatible open-source license may use Mycelium freely under the AGPL terms. Network-deployed applications using Mycelium must make their source available to users of that service.

**Commercial embedding:** Organisations that need to embed Mycelium in a proprietary product without the AGPL copyleft obligation can obtain a commercial license. Contact [tathatasystems@proton.me](mailto:tathatasystems@proton.me) to discuss terms.
