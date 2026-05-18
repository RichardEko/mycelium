# mycelium

An embedded gossip protocol library for adaptive AI agent systems. Agents discover each other's
capabilities through a shared KV medium, signal intent through scope-filtered epidemic events,
and evolve their topology without a coordinator, central registry, or single point of failure.

Built on TCP epidemic propagation with last-write-wins conflict resolution. Layer 1 carries
persistent state; Layer 2 carries ephemeral events. Higher layers build Actor/Event systems,
async RPC, and MCP AI tool routing on top — each agent chooses its own payload serialisation.

## Build

```
cargo build --release
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
use gossip_protocol::{GossipAgent, GossipConfig, NodeId};
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
// Cause: max_concurrent_forwards or gossip_channel_capacity too small for the burst rate.
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
  YES → max_concurrent_forwards or gossip_channel_capacity too small
        Size max_concurrent_forwards to N_agents × fan_out (default fan_out = 4)
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
use gossip_protocol::{signal_kind, SignalScope, OpacityHint};
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
| Is this node actively refusing a kind? | `is_suppressed` |
| How saturated is this node's intake? | `opacity` |
| Are peers aware this node is overloaded? | `manage_opacity` governor |
| Which groups is this node a member of? | `groups()` |
| How many live workers are in the pool? | `scan_prefix("load/")` |

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

`scan_prefix` is O(n) over the full store. At typical pheromone-trail sizes (100–1,000 entries) it is negligible. It crosses 1 ms around 100,000 entries — if Layer 3 activity grows the store that large, introduce a prefix index.

## Security Model

mycelium operates in a **trusted domain** — all nodes on the gossip mesh are assumed to be
cooperative. There is no TLS, no peer authentication, and no payload encryption.

A connected peer can:
- Send crafted frames to inject arbitrary KV entries (limited by LWW timestamps)
- Claim any `NodeId` in a `StateRequest` (consequence: misdirected `StateResponse`, harmless)
- Poison a nonce in the dedup seen-set (probability: < 1/2⁶⁴ per collision)

**Do not expose gossip ports to untrusted networks.** Use a network-layer control (firewall
rules, WireGuard, VPC security groups) to restrict access to the gossip port to trusted peers
only.

TLS and mutual authentication are planned at Layer 3 for external-facing service endpoints.

## Layer III — Bulk Transfer / Eventing (Planned)

Layer 3 introduces HTTP transport for large payloads and a distinct `Event` type for
transport-bound, connection-scoped, ordered events.

**Why a distinct `Event` type rather than reusing `Signal`**: the delivery guarantees differ
fundamentally. A `Signal` is epidemic and best-effort — it can be silently dropped. A Layer 3
`Event` rides an open transport connection and will not be missed while that connection is live.
Sharing a type would obscure this difference.

**Pattern reuse**: the *model* from Layer 2 transfers. Event kinds use the same string constants
(`invoke.result`, `boundary.opaque`). The "emit to scope, receivers with matching state act"
principle applies. Only the delivery substrate changes.

Layer 3 events are used for streaming token responses, upstream cancel signals on active
requests, and heartbeats on long-lived transport connections.

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
| `max_concurrent_forwards` | `64` | Per-peer outbound channel depth. **Correctness threshold** — frames silently dropped when full. Size to `N × fan_out` |
| `max_forwarding_peers` | unlimited | Cap gossip fan-out targets. Set to `bootstrap_peers.len()` for fixed-topology meshes |
| `max_peers` | unlimited | Cap the peer table. Prevents O(N²) persistent connections when piggybacked peer lists would otherwise expand every node's view of the full cluster. Set to `bootstrap_peers.len()` for grid or ring topologies |
| `gossip_channel_capacity` | `1024` | Per-shard gossip channel depth |
| `gossip_shards` | `min(CPU,16)` | Gossip worker tasks. Set to `1` for demos/debug to cut task count |
| `max_seen_entries` | `100000` | Dedup cache size before eviction |
| `peer_eviction_intervals` | `3` | Missed ping intervals before a peer is evicted |
| `reconnect_backoff_secs` | `5` | Cooldown after a failed connect |
