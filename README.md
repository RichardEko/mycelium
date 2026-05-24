# mycelium

An embedded gossip protocol library for adaptive AI agent systems. Agents discover each other's
capabilities through a shared KV medium, signal intent through scope-filtered epidemic events,
and evolve their topology without a coordinator, central registry, or single point of failure.

Built on TCP epidemic propagation with last-write-wins conflict resolution. Layer 1 carries
persistent state; Layer 2 carries ephemeral events. Higher layers build Actor/Event systems,
async RPC, and MCP AI tool routing on top — each agent chooses its own payload serialisation.

## Demo — Mesh Control UI

A three-node gossip mesh with a live management UI. No dependencies beyond Rust.

```sh
# Quick start — no Ollama needed
MOCK_LLM=1 cargo run --example llm_agent

# With a real LLM (Ollama default, or set OPENAI_BASE_URL / OPENAI_MODEL)
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

**Conway's Game of Life** — a separate standalone demo that shows the epidemic substrate itself rather than a service topology. 256 gossip agents (one per cell in a 16×16 grid) coordinate cell state via gossip KV; a tick signal drives each generation.

```sh
cargo run --example conway          # CPU renderer (terminal / HTTP canvas)
cargo run --example conway_gpu      # GPU-accelerated renderer (Metal / wgpu)
```

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
agent.set("key", Bytes::from("value"));

// Read
if let Some(bytes) = agent.get("key") { /* ... */ }

// Delete (propagates a tombstone)
agent.delete("key");

// Enumerate live keys
let keys: Vec<Arc<str>> = agent.keys();

// Scan by prefix — capability discovery, pheromone trail reads
let entries = agent.scan_prefix("load/");

// Subscribe — watch::Receiver fires on every change (local or gossiped)
let mut rx = agent.subscribe("load/my-node");
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
let live_workers: Vec<LoadState> = agent.scan_prefix("load/nlp/")
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
agent.join_group("nlp");
agent.leave_group("nlp");
let groups: Vec<Arc<str>> = agent.groups();  // current memberships

// ── Advertise — periodic heartbeat + pheromone trail ─────────────────────
let load_key = format!("load/{}", agent.node_id());
let agent2 = agent.clone();
let _advert = agent.advertise(
    signal_kind::CONTRACT_AVAILABLE,
    SignalScope::Group("nlp"),
    Duration::from_secs(10),
    move || {
        let state = LoadState { queue_depth: QUEUE.len(), written_at_ms: unix_ms_now() };
        agent2.set(load_key.clone(), encode(&state));  // pheromone trail — persists
        encode(&state)                                  // signal payload — fast delivery
    },
);
// Drop _advert to stop advertising; call agent.delete(&load_key) on graceful shutdown

// ── Receive signals ────────────────────────────────────────────────────────
let mut rx = agent.signal_rx(signal_kind::INVOKE);
tokio::spawn(async move {
    while let Some(sig) = rx.recv().await {
        // sig.sender, sig.payload, sig.scope, sig.nonce
    }
});
// Channel sizing: the default depth of 256 suits kinds that arrive at a few Hz
// (health probes, contract advertisements). For kinds where N agents all emit
// simultaneously (e.g. INVOKE to a group of N workers), use:
//   agent.signal_rx_with_capacity(kind, N * expected_burst)
// A full channel logs a warning and drops the signal — there is no retry.

// ── Emit ───────────────────────────────────────────────────────────────────
agent.emit("invoke", SignalScope::Group("nlp"), payload);       // non-blocking
agent.emit_async("invoke", SignalScope::Group("nlp"), payload).await; // awaits capacity

// ── One-shot request/response — register BEFORE emitting the request ───────
let reply = agent.signal_once("invoke.result", Duration::from_secs(5), |s| {
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
let age: Option<Duration> = agent.last_signal(signal_kind::CONTRACT_AVAILABLE)
    .map(|t| t.elapsed());

// ── Watch — fault detection / supervisor pattern ───────────────────────────
// Calls on_stale() when last_signal(kind) has been silent for longer than threshold.
// Checks every threshold/4 (minimum 100ms). Returns WatchHandle; drop to cancel.
let _watcher = agent.watch(
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
if agent.quorum(signal_kind::CLUSTER_EVENT, 3, Duration::from_secs(10)) {
    // At least 3 distinct nodes checked in within the last 10 seconds
    start_leader_election();
}

// ── Is this node suppressing a kind? ──────────────────────────────────────
let suppressing: bool = agent.is_suppressed(signal_kind::INVOKE);

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
| Has a worker been seen recently? | `last_signal` |
| Has a worker gone silent? (trigger action when silent) | `watch` |
| Have enough distinct nodes checked in? | `quorum` |
| Have K nodes checked in (survives restart)? | `quorum_persistent` |
| Is this node actively refusing a kind? | `is_suppressed` |
| How saturated is this node's intake? | `opacity` |
| Are peers aware this node is overloaded? | `manage_opacity` governor |
| Which groups is this node a member of? | `groups()` |
| How many live workers are in the pool? | `scan_prefix("load/")` |
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
let mut invoke_rx = agent.signal_rx(signal_kind::INVOKE);
tokio::spawn(async move {
    while let Some(sig) = invoke_rx.recv().await {
        agent.suppress(signal_kind::INVOKE, Duration::from_millis(500));
        handle_invocation(sig).await;
    }
});

// ── Rate limiting ──────────────────────────────────────────────────────────
// Suppress "data.sync" for 5s after processing one — prevents sync storms.
agent.on_signal(signal_kind::DATA_SYNC, move |_sig| {
    agent.suppress(signal_kind::DATA_SYNC, Duration::from_secs(5));
    trigger_sync();
});

// ── Lift early if needed ───────────────────────────────────────────────────
agent.unsuppress(signal_kind::INVOKE);

// ── Check state for diagnostics ───────────────────────────────────────────
if agent.is_suppressed(signal_kind::INVOKE) {
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
let mut invoke_rx = agent.signal_rx_with_capacity(signal_kind::INVOKE, 64);
tokio::spawn(async move {
    while let Some(sig) = invoke_rx.recv().await {
        agent.suppress(signal_kind::INVOKE, Duration::from_millis(500));
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

For routing decisions, always read the store (`scan_prefix("load/")`), not signal history. The
store is visible to late joiners and survives missed signals. Signal history (`last_signal`,
`quorum`) is the right tool for liveness and fault detection, not routing.

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

→ The **Capability Market** preset in the [Mesh Control UI](docs/mesh_control.html) demonstrates
providers, requirers, and per-capability demand-pressure bars across four capability types.
(Standalone source archived at [`examples/archived/capability_market.rs`](examples/archived/capability_market.rs).)

### Advertising and Resolving Capabilities

```rust
use mycelium::{Capability, CapFilter, CapabilityHandle};
use std::time::Duration;

// Advertise — periodically reasserts cap/{node_id}/{ns}/{name} in the KV store.
// Drop the handle to stop advertising; the tombstone propagates automatically.
let handle: CapabilityHandle = agent.advertise_capability(
    Capability::new("compute", "gpu"),
    Duration::from_secs(30),  // reassert interval
);

// Resolve — snapshot of every node currently advertising a matching capability.
let filter = CapFilter::new("compute", "gpu");
let matches: Vec<(NodeId, Capability)> = agent.resolve(&filter);

// Watch — push-based; fires when the matching set changes.
// Debounced: burst KV writes within 50 ms collapse to one notification.
let mut rx: watch::Receiver<Vec<(NodeId, Capability)>> = agent.watch_capabilities(filter);
rx.changed().await?;
let current = rx.borrow().clone();
```

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

→ The **Emergent GPU Pool** preset in the [Mesh Control UI](docs/mesh_control.html) shows a
20-node worker pool that assembles dynamically and fans out render jobs to all members.
(Standalone source archived at [`examples/archived/emergent_pool.rs`](examples/archived/emergent_pool.rs).)

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

→ The **Locality Mesh** preset in the [Mesh Control UI](docs/mesh_control.html) covers 12 nodes
across two availability zones: remove a close provider and the resolver shifts to the next ring.
(Standalone source archived at [`examples/archived/locality_wiring.rs`](examples/archived/locality_wiring.rs).)

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
| `writer_channel_depth` | `256` | Per-peer outbound channel depth (ring buffer). **Correctness threshold** — frames silently dropped when full. Size to `N × fan_out`. A saturation warning fires every 1 000th cumulative dropped frame. |
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
