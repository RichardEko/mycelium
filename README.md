# mycelium

A peer-to-peer gossip protocol library and CLI node written in Rust. Nodes exchange key-value updates over persistent TCP connections using TTL-based epidemic propagation, last-write-wins conflict resolution, and tombstone deletes.

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

## Library API

```rust
use gossip_protocol::{GossipAgent, GossipConfig, NodeId};
use std::sync::Arc;

let node_id = NodeId::new("127.0.0.1", 7946)?;
let config  = GossipConfig::default();
let peers   = vec!["127.0.0.1:7947".parse()?];

let agent = Arc::new(GossipAgent::new(node_id, config, peers));
agent.start().await?;

// Write
agent.set("key", b"value".to_vec());

// Read
if let Some(bytes) = agent.get("key") { ... }

// Delete (propagates a tombstone)
agent.delete("key");

// Enumerate live keys
let keys: Vec<String> = agent.keys();

// Subscribe to changes on a key (watch::Receiver fires on every write or delete)
let mut rx = agent.subscribe("key");
rx.changed().await?;
println!("{:?}", *rx.borrow());

agent.shutdown().await;
```

## Layer II — Signal / Boundary Mesh

Signals are ephemeral events that propagate epidemically to every node in the cluster. Each
node holds a local **boundary** — a set of group memberships — that decides whether it *acts*
on an incoming signal. Forwarding is always unconditional; the boundary only controls local
delivery.

```rust
// Join a capability group — start receiving Group("nlp") signals
agent.join_group("nlp");

// Advertise availability and write a pheromone trail to the KV store
// The store entry persists and is anti-entropy synced; the signal is fast but ephemeral
let load_key = format!("load/{}", agent.node_id());
let agent2 = agent.clone();
let _handle = agent.advertise(
    signal_kind::CONTRACT_AVAILABLE,
    SignalScope::Group("nlp"),
    Duration::from_secs(10),
    move || {
        let state = LoadState { queue_depth: QUEUE.len(), written_at_ms: unix_ms_now() };
        agent2.set(load_key.clone(), encode(&state));  // pheromone trail — persists
        encode(&state)                                  // signal payload — fast delivery
    },
);
// Drop _handle to stop advertising; call agent.delete(&load_key) on graceful shutdown

// Register a persistent handler (one receiver per registered kind per task)
let mut rx = agent.signal_rx("invoke");
tokio::spawn(async move {
    while let Some(sig) = rx.recv().await {
        // sig.sender, sig.payload, sig.scope, sig.nonce
    }
});

// Emit a signal — delivers locally if admitted, then floods the cluster
agent.emit("invoke", SignalScope::Group("nlp"), payload);     // non-blocking
agent.emit_async("invoke", SignalScope::Group("nlp"), payload).await; // awaits capacity

// One-shot request/response — register BEFORE emitting the request
let reply = agent.signal_once("invoke.result", Duration::from_secs(5), |s| {
    s.nonce == request_nonce
}).await;  // → Option<Signal>; None on timeout

// Freshness check — scan the pheromone trails in the KV store (authoritative)
let any_live = agent.scan_prefix("load/")
    .iter()
    .filter_map(|(_, b)| decode::<LoadState>(b))
    .any(|s| unix_ms_now() - s.written_at_ms < 30_000);

// last_signal() is still useful for kinds that have no KV equivalent (e.g. "invoke.result")
let age = agent.last_signal("invoke.result").map(|t| t.elapsed());

// Scopes
SignalScope::System              // every node acts
SignalScope::Group("name")       // nodes that called join_group("name")
SignalScope::Individual(node_id) // exactly one node
```

**Load-adaptive opacity**: when handler channels fill, the boundary probabilistically sheds
incoming signals, creating emergent backpressure. A busy node stops consuming work without any
coordination. `Individual` scope always bypasses this.

### Signals vs Pheromone Trails

Not all signal kinds are equal. With pheromone trails in the KV store, some signals are
redundant for *discovery* — the trail is the authoritative record. Others are irreplaceable.

| Signal kind | Role | Covered by pheromone? |
|---|---|---|
| `invoke` | Work request — must reach a worker now | No |
| `invoke.result` | Targeted reply — ephemeral by nature | No |
| `invoke.bulk` | Layer 3 bulk transfer ticket | No |
| `boundary.opaque` | Immediate overload notification — connection teardown | No — fast-path complement to the trail |
| `boundary.transparent` | Recovery notification | No — fast-path complement |
| `contract.available` | Worker availability | **Yes** — `load/<node_id>` trail is authoritative |
| `contract.withdrawn` | Worker gone | **Yes** — tombstone / trail evaporation |
| `cluster.event` | Join/leave events | **Yes** — `grp/<name>/<node_id>` entries |

Emit pheromone-covered signal kinds only if listeners need real-time notification alongside the
durable trail. For routing decisions, always read the store (`scan_prefix("load/")`), not signal
history — the store is visible to late joiners and survives missed signals.

See [ROADMAP.md](ROADMAP.md) for architecture, design rationale, and Layer 3/4 plans.

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
Environment variables `GOSSIP_BIND_ADDRESS` and `GOSSIP_BIND_PORT` override both.

Key config fields (all have defaults):

| Field | Default | Description |
|---|---|---|
| `bind_address` | `127.0.0.1` | TCP listen address |
| `bind_port` | `8080` | TCP listen port |
| `bootstrap_peers` | `[]` | Peers to contact on startup |
| `default_ttl` | `5` | Hops before a message expires |
| `health_check_interval_secs` | `10` | Ping interval and peer eviction cadence |
| `propagation_window_secs` | `60` | Tombstone retention window |
| `max_connections` | `1024` | Inbound connection limit |
| `max_concurrent_forwards` | `64` | Per-peer outbound channel depth |
| `gossip_channel_capacity` | `1024` | Per-shard gossip channel depth |
| `max_seen_entries` | `100000` | Dedup cache size before eviction |
| `peer_eviction_intervals` | `3` | Missed ping intervals before a peer is evicted |
| `reconnect_backoff_secs` | `5` | Cooldown after a failed connect |
