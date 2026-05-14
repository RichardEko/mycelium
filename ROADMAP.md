# Distributed AI Agent Platform — Engineering Roadmap

> **Status:** Layer 1 and Layer 2 complete. Layers 3–4 planned.
> **Last updated:** 2026-05-14

---

## The Vision

A swarm of AI agents that discovers each other's capabilities through a shared medium, signals
intent through receptors that filter by scope, and moves heavy data over HTTP only when the
payload demands it — with no coordinator, no central registry, no single point of failure.

The gossip protocol is not the application. It is the bloodstream the application runs on.

---

## Design Philosophy: Chemical Signalling

The architecture is modelled on biological chemical signalling, not traditional message routing.

In biology, hormones flood the entire bloodstream. Every cell receives every signal. Specificity
comes not from directed routing but from **receptors** — cells that carry the right receptor
respond; cells that don't, let the signal pass. The body doesn't route insulin to muscle cells;
it trusts that muscle cells have insulin receptors and liver cells do not.

This platform works the same way:

- **Signals flood the cluster epidemically.** Every node receives every signal. There are no
  routing tables, no topology maps, no coordinators deciding who gets what.
- **Boundaries are local receptors.** Each agent holds an in-memory set of group memberships.
  When a signal arrives, the boundary check is a single hash lookup. Pass → act. Fail → forward
  and move on.
- **Forwarding and acting are completely decoupled.** A node outside `group::nlp` forwards
  a group-scoped signal at full speed without acting on it. Signal propagation is guaranteed
  regardless of whether any intermediate node is in scope.

This produces a system that is:
- **Resilient** — no routing bottleneck, no single point of failure in signal delivery
- **Elastic** — agents join and leave groups at runtime without any topology update
- **Simple** — one propagation model for all scopes; complexity lives only in the boundary check

---

## Architecture: Four Layers

```
┌────────────────────────────────────────────────────────────────────┐
│  Layer 4: Observability                              [Phase 4]     │
│  Prometheus metrics · latency histograms · signal throughput       │
├────────────────────────────────────────────────────────────────────┤
│  Layer 3: Bulk Transfer / AI Execution               [Phase 3]     │
│  HTTP for large payloads · streaming LLM calls · reqwest           │
│  Signal carries a ticket; HTTP carries the weight                  │
│  Layer 4+: service routing, load balancing, connection management  │
├────────────────────────────────────────────────────────────────────┤
│  Layer 2: Signal / Boundary Mesh                     [COMPLETE]    │
│  WireMessage::Signal · Boundary (local receptor) · opacity         │
│  advertise · signal_once · last_signal                             │
│  System / Group / Individual scopes · mpsc handlers                │
│  Group membership in KV store · heartbeat-driven retry model       │
├────────────────────────────────────────────────────────────────────┤
│  Layer 1: Gossip Transport                           [COMPLETE]    │
│  GossipAgent · LWW KV store · anti-entropy · zero-copy fan-out     │
│  ShardedSeen · papaya lock-free · wire v6 · 88 tests               │
└────────────────────────────────────────────────────────────────────┘
```

**Fundamental separation of concerns:**

| Layer 1 KV store | Layer 2 Signals |
|---|---|
| *State* — what is true right now | *Events* — something happened |
| Last-write-wins, persistent | Ephemeral, TTL-bounded |
| Contract advertisements, group topology | Invocation requests, results, capability announcements |
| Queryable at any time | Fire-and-forget; handled or missed |

---

## Layer 1 — Gossip Transport (Complete)

The substrate. Lock-free epidemic KV propagation. This layer knows nothing about contracts,
agents, signals, or scopes. It is intentionally general — a high-performance replication
primitive any layer can build on. 88 tests.

**Performance characteristics:**
- Lock-free hot path: `papaya::HashMap` for store, peers, subscriptions; no mutex on the
  frame-receive critical path
- Early nonce dedup: nonce read directly from the wire buffer at byte offset 4 before any
  bincode deserialization — eliminates ~80% of decodes under TTL=5
- Zero-copy fan-out: TTL decremented in-place at byte offset 20; `split().freeze()` is O(1)
- Write coalescing: 16 KB `BufWriter` per peer; drains queued frames into a single kernel write
- Configurable sharding: gossip workers default to logical CPU count, capped at 16

**Stable public API:**

```rust
// Lifecycle
GossipAgent::new(node_id: NodeId, config: GossipConfig) -> Self
agent.start() -> Result<(), GossipError>
agent.shutdown() -> ()

// State
agent.set(key, value) -> bool       // local always updated; false = channel full
agent.get(key) -> Option<Bytes>
agent.delete(key) -> bool           // gossips a tombstone
agent.keys() -> Vec<Arc<str>>

// Reactive
agent.subscribe(key) -> watch::Receiver<Option<Bytes>>

// Introspection
agent.system_stats() -> SystemStats
```

---

## Layer 2 — Signal / Boundary Mesh (Complete)

### Overview

Layer 2 adds two primitives on top of the Layer 1 gossip transport: **signals** and
**boundaries**. Signals are ephemeral events that propagate epidemically to the entire cluster.
Boundaries are local receptors that decide whether a node *acts* on a signal — forwarding is
always unconditional and happens first.

All Layer 2 functionality is exposed directly on `GossipAgent`. No separate wrapper type exists.

### Wire Protocol

One new variant in `WireMessage` (`src/framing.rs`):

```rust
Signal {
    ttl:     u8,
    nonce:   u64,
    sender:  NodeId,
    scope:   SignalScope,   // System | Group(name) | Individual(node_id)
    kind:    Arc<str>,
    payload: Bytes,
}
```

Signal frames share the same TTL/nonce/fan-out machinery as `Data` frames. Every node that
receives a signal decrements TTL and forwards it to all peers — regardless of whether the
node acts on it. The boundary check happens *after* forwarding is queued.

```
Signal arrives
  └─ mark nonce seen (dedup, same ShardedSeen as Data)
  └─ forward at TTL-1 to all peers (unconditional)
  └─ boundary.admits(scope)?
       YES → opacity check → deliver to registered handlers
       NO  → discard locally, already forwarded
```

### Scopes

```rust
pub enum SignalScope {
    System,              // every node acts
    Group("nlp"),        // only nodes that have called join_group("nlp")
    Individual(node_id), // exactly one node; bypasses opacity (no routing alternative)
}
```

### Stable Public API

```rust
// Persistent handler — receives every admitted signal of `kind`
agent.signal_rx(kind)                        -> mpsc::Receiver<Signal>
agent.signal_rx_with_capacity(kind, cap)     -> mpsc::Receiver<Signal>

// Emit
agent.emit(kind, scope, payload)             -> bool   // false = shard full
agent.emit_async(kind, scope, payload).await -> bool   // false = shard dead

// Group membership — updates boundary immediately; gossips KV entry at grp/<name>/<node_id>
agent.join_group(name)
agent.leave_group(name)

// One-shot request/response correlation
agent.signal_once(kind, timeout, predicate).await -> Option<Signal>

// Periodic availability heartbeat
agent.advertise(kind, scope, interval, payload_fn) -> AdvertiseHandle

// Freshness query
agent.last_signal(kind) -> Option<Instant>
```

### The Boundary

Each node holds an in-memory `Boundary` — its receptor set. The admission check is a single
hash-set lookup, called on every inbound signal frame.

```
SignalScope::System          → always admits
SignalScope::Group(name)     → admits iff join_group(name) was called
SignalScope::Individual(id)  → admits iff id == this node's NodeId
```

Group membership is also written to the Layer 1 KV store at `grp/<name>/<node_id>` so any node
can observe the cluster's group topology as standard store state, subscribing to changes via
`agent.subscribe()`.

### Variable Opacity — Load-Adaptive Admission

When handler channels start to fill, the boundary probabilistically sheds incoming signals
rather than queuing them. The admission probability falls linearly as channels fill, reaching
zero (fully opaque) when they are completely full.

```
fill_ratio = 1.0 - (channel_remaining / channel_capacity)
admit      = fastrand::f32() >= fill_ratio
```

`Individual` scope always bypasses opacity — there is no routing alternative for a signal
addressed to a specific node.

**Emergent backpressure**: an overloaded node goes opaque and stops consuming work. It
continues to *forward* all signals it receives — the network remains fully connected — but the
node itself no longer reacts. Upstream nodes that see no response naturally retry elsewhere or
backoff. No coordination required.

### Heartbeat-Driven Retry Model

Workers advertise availability on a periodic interval. Invokers track freshness and retry
rather than assuming delivery. This gives at-least-once semantics without a broker.

**Worker side** — call once at startup:

```rust
let _handle = agent.advertise(
    signal_kind::CONTRACT_AVAILABLE,
    SignalScope::Group("nlp"),
    Duration::from_secs(5),
    || encode(WorkerState { queue_depth, accepted_kinds: &["sentiment"] }),
);
// Drop _handle to stop advertising (e.g. on graceful shutdown)
```

**Invoker side** — register before emitting so no reply is missed:

```rust
let result = agent.signal_once(
    signal_kind::INVOKE_RESULT,
    Duration::from_secs(5),
    |s| s.nonce == request_nonce,
).await;

match result {
    Some(sig) => handle_result(sig),
    None => {
        // No response within timeout — check if any worker was recently alive
        if agent.last_signal(signal_kind::CONTRACT_AVAILABLE)
               .map(|t| t.elapsed() < Duration::from_secs(30))
               .unwrap_or(false)
        {
            // Workers exist but are busy — retry after backoff
        } else {
            // No worker heard from recently — escalate or queue
        }
    }
}
```

### Stigmergic Load State — Pheromone Trails in the KV Store

Rather than maintaining local availability caches (which replicate global state at every node
and require explicit stale eviction), workers write their load state directly into the Layer 1
KV store. The store is the shared medium — any node can read it at any time, new nodes receive
it immediately via anti-entropy sync, and no node needs to manage a derived cache.

```rust
// Worker writes its own pheromone trail on each advertise tick
let node_key = format!("load/{}", agent.node_id());
let _handle = agent.advertise(
    signal_kind::CONTRACT_AVAILABLE,
    SignalScope::Group("nlp"),
    Duration::from_secs(10),
    move || {
        let state = LoadState {
            queue_depth:    QUEUE.len(),
            accepted_kinds: &["sentiment"],
            written_at_ms:  unix_ms_now(),  // timestamp embedded for evaporation
        };
        // Write to the shared medium — this IS the pheromone trail
        agent.set(node_key.clone(), encode(&state));
        encode(&state)   // also emit as signal for fast in-flight notification
    },
);
```

Any node reads the current load picture directly from the store — no signal handler, no local
cache, no background task:

```rust
// Read all load entries for the "nlp" group at decision time
let load_picture: Vec<(NodeId, LoadState)> = agent.keys()
    .into_iter()
    .filter(|k| k.starts_with("load/"))
    .filter_map(|k| {
        let state: LoadState = decode(&agent.get(&k)?)?;
        // Evaporation: ignore entries older than 3 × advertise interval
        if unix_ms_now() - state.written_at_ms > 30_000 { return None; }
        let node_id = k.strip_prefix("load/")?.parse().ok()?;
        Some((node_id, state))
    })
    .collect();
```

**Properties this gives for free:**

- **New nodes bootstrap immediately** — anti-entropy sync delivers the full load picture on
  join. No waiting for the first heartbeat round.
- **No local cache to invalidate** — the store is the single source of truth. Every read is
  current.
- **Stale eviction without coordination** — the timestamp embedded in the payload is the
  pheromone evaporation mechanism. Readers discard stale entries. Crashed workers' trails
  decay naturally. Graceful shutdown writes a tombstone (`agent.delete("load/<node_id>")`).
- **Subscribe for push updates** — `agent.subscribe("load/<node_id>")` delivers every change
  to that entry via a `watch::Receiver`. No polling.

**Cost**: identical to the `advertise()` signals already emitted — one epidemic write per
worker per interval. Store size is O(workers), ~200 bytes per entry.

### Competitive Response — Emergent Routing

The CAS-native invocation model emits to Group scope and lets capable workers compete to
respond. No invoker selects a worker. No routing table exists. Load distribution emerges from
the natural interaction of opacity state and processing speed.

```
Invoker emits: SignalScope::Group("nlp") → floods all nlp-group nodes
               Overloaded workers: boundary opaque → signal not admitted → no response
               Available workers: boundary transparent → signal admitted → process → reply

Invoker receives: first Individual reply → done
                  timeout → retry or check pheromone trails
```

```rust
// Register reply handler BEFORE emitting (no reply missed between emit and recv)
let nonce = fastrand::u64(..);
let reply_fut = agent.signal_once(
    signal_kind::INVOKE_RESULT,
    Duration::from_secs(5),
    move |s| s.nonce == nonce,
);

// Emit to the group — routing is emergent, not explicit
agent.emit_async(
    signal_kind::INVOKE,
    SignalScope::Group("nlp"),
    encode(InvokeRequest { nonce, payload: input }),
).await;

match reply_fut.await {
    Some(sig) => decode(&sig.payload),
    None => {
        // No reply — consult the pheromone trails to decide whether to retry
        let any_available = agent.keys().iter()
            .filter(|k| k.starts_with("load/"))
            .filter_map(|k| agent.get(k))
            .filter_map(|b| decode::<LoadState>(&b))
            .any(|s| s.queue_depth < MAX_QUEUE && !s.written_at_ms_is_stale());
        if any_available { retry_with_backoff() } else { Err("no workers") }
    }
}
```

Workers respond with `Individual(sig.sender)` scope — this is the only correct use of
Individual scope at this layer: returning a result to the specific node that asked for it.
The invoker never specifies which worker to use. Selection does not happen.

### Two-Timescale Load Signalling

Fast signals and slow pheromone trails serve different purposes, at different timescales —
the same two-timescale model biological systems use (neural signals vs. hormones).

| | Pheromone trail (KV) | Opacity signal (ephemeral) |
|---|---|---|
| **Timescale** | 5–30 s | Immediate |
| **Persistence** | Durable, anti-entropy synced | Ephemeral — missed if not listening |
| **Purpose** | Steady-state load picture for routing decisions | Acute overload — tear down connections now |
| **Consumer** | Any node, any time, including late joiners | Nodes currently listening |
| **Evaporation** | Timestamp in payload; readers discard stale | N/A — one-shot event |

Use both: pheromone trails for routing, opacity signals for immediate connection management.

### Opacity Transition Signals

A worker can explicitly announce load state changes so upstream nodes can proactively drain
connections rather than waiting for heartbeat silence.

**Worker going opaque** (overloaded, stopping accepting new work):

```rust
agent.emit(
    "boundary.opaque",
    SignalScope::Group("nlp"),
    encode(OpacityEvent { reason: "queue_full", eta_secs: 30 }),
);
```

**Worker clearing** (recovered, accepting again):

```rust
agent.emit("boundary.transparent", SignalScope::Group("nlp"), Bytes::new());
// Next advertise() heartbeat will re-populate the upstream cache with fresh state
```

**Upstream handler**:

```rust
let mut opaque_rx = agent.signal_rx("boundary.opaque");
tokio::spawn(async move {
    while let Some(sig) = opaque_rx.recv().await {
        let event: OpacityEvent = decode(&sig.payload);
        router.suspend_worker(sig.sender, event.reason, event.eta_secs);
        // close HTTP keep-alives, drain service mesh connections to sig.sender
    }
});
```

This is a Layer 4 concern — Layer 2 provides the signal transport; what the upstream node does
with it (HTTP teardown, connection draining, re-routing) is application code.

### Signals vs Pheromone Trails — Division of Labour

After introducing stigmergic pheromone trails, the role of signals is narrower and more precise.
Understanding which mechanism handles which concern prevents redundant design at Layer 3 and 4.

| Concern | Mechanism | Why |
|---|---|---|
| Worker availability | Pheromone trail (`load/<node_id>`) | Durable, anti-entropy synced, readable by late joiners |
| Worker load state | Pheromone trail (payload) | Persistent; no listener required |
| Group membership | KV store (`grp/<name>/<node_id>`) | Gossips automatically via `join_group`/`leave_group` |
| Graceful withdrawal | Tombstone (`agent.delete("load/<node_id>")`) | Immediate evaporation without a signal |
| **Work request** | **Signal (`invoke`)** | Ephemeral; must reach a worker now |
| **Work response** | **Signal (`invoke.result`)** | Targeted reply; ephemeral |
| **Acute overload** | **Signal (`boundary.opaque`)** | Tear down connections *before* the pheromone trail updates |
| **Recovery** | **Signal (`boundary.transparent`)** | Upstream nodes resume sending; pheromone refreshes on next tick |

**Summary**: pheromone trails carry steady-state knowledge into the shared medium; signals carry
events that must be acted on immediately. When both carry the same information (e.g.
`CONTRACT_AVAILABLE` signal alongside the pheromone write), the signal is a fast-path
notification for live listeners — the trail is the authoritative record.

### Well-Known Signal Kinds

```rust
pub mod signal_kind {
    // ── Core invocation — always signals ──────────────────────────────────
    // No KV equivalent: work requests and responses are ephemeral by nature.
    pub const INVOKE:               &str = "invoke";
    pub const INVOKE_RESULT:        &str = "invoke.result";
    pub const INVOKE_BULK:          &str = "invoke.bulk";    // Layer 3 ticket

    // ── Opacity transitions — fast-path complement to pheromone trails ────
    // The pheromone trail reflects the same state at the next advertise tick.
    // These signals handle the gap: immediate notification for connection teardown.
    pub const BOUNDARY_OPAQUE:      &str = "boundary.opaque";
    pub const BOUNDARY_TRANSPARENT: &str = "boundary.transparent";

    // ── Pheromone-covered — redundant for discovery, optional fast-path ───
    // load/<node_id> and grp/<name>/<node_id> carry this state durably.
    // Emit these alongside the pheromone write if listeners need real-time
    // notification; omit if scan_prefix/subscribe is sufficient.
    pub const CONTRACT_AVAILABLE:   &str = "contract.available";
    pub const CONTRACT_WITHDRAWN:   &str = "contract.withdrawn";
    pub const CLUSTER_EVENT:        &str = "cluster.event";
    pub const HEALTH_PROBE:         &str = "health.probe";
    pub const HEALTH_ACK:           &str = "health.ack";
}
```

---

## Layer 3 — Bulk Transfer / AI Execution (Phase 3)

When payloads exceed practical signal size — multi-KB prompts, large model outputs, streaming
token responses — a Layer 2 `invoke.bulk` signal carries a *ticket* (correlation ID + HTTP
endpoint), and Layer 3 handles the actual data transfer over HTTP.

```
Caller  →  Individual signal "invoke.bulk"
           payload: { contract_id, corr_id, input_endpoint }

Target  ←  fetches large input from caller's HTTP endpoint
        →  runs model / calls LLM API
        →  Individual signal "invoke.result"
           payload: { corr_id, result_endpoint OR inline_result }

Caller  ←  fetches large result from target's HTTP endpoint (if referenced)
```

**HTTP is a Layer 3 concern only.** Agents that only handle small payloads need no HTTP server
at all.

```rust
pub struct BulkTransferLayer {
    client:   reqwest::Client,
    endpoint: String,   // this agent's HTTP base URL for bulk I/O
}
```

### Eventing at Layer 3

Layer 3 introduces transport-bound events — ordered, connection-scoped, flow-controlled — that
are conceptually related to Layer 2 signals but have fundamentally different delivery semantics.

| Property | Layer 2 Signal | Layer 3 Event |
|---|---|---|
| Delivery | Epidemic flood — every node | Point-to-point transport (SSE, gRPC stream) |
| Ordering | None | Ordered per connection |
| Reliability | Best-effort; can be missed | At-least-once on open stream; stream failure is explicit |
| Flow control | Probabilistic opacity shedding | Transport-level (HTTP/2 flow control, TCP backpressure) |
| Scope | System / Group / Individual | Connection-scoped by definition |

Layer 3 defines a distinct `Event` type rather than reusing `Signal`. The delivery guarantees
differ enough that sharing a type would obscure that a `Signal` can be silently dropped while an
`Event` on an open stream will not be. Making the distinction explicit in the type system
prevents callers from conflating the two and relying on the wrong guarantee.

**Pattern reuse from Layer 2**: the *mental model* transfers directly. `EventScope` mirrors
`SignalScope` for application-layer filtering; event kind strings reuse the same constants
(`invoke.result`, `boundary.opaque`); the "emit to scope, receivers with matching state act"
principle applies. The difference is only in the delivery substrate, not in how applications
think about targeting and filtering.

Layer 3 events are used for:
- Streaming token responses back to a caller (progress events on an open HTTP SSE stream)
- Cancel signals travelling upstream on an active bulk request
- Heartbeats on a long-lived transport connection (distinct from Layer 1 peer health probes)

**Streaming** (token-by-token LLM responses) rides HTTP chunked transfer or SSE from the model
API. A Layer 2 `invoke.result` signal fires when streaming ends, carrying the final correlation
ID so the invoker can clean up its `signal_once` registration if still pending.

### Dependencies added at Layer 3

```toml
reqwest = { version = "0.12", features = ["json", "stream", "rustls-tls"],
            default-features = false }
```

---

## Layer 4 — Observability (Phase 4)

Prometheus-compatible metrics across all layers via a single scrape endpoint. Uses the
`metrics` facade — zero-cost when no recorder is installed; Layer 1 and 2 emit calls
without a hard runtime dependency on any exporter.

```
gossip_messages_received_total          counter
gossip_messages_deduplicated_total      counter
gossip_store_entries                    gauge
gossip_peers_connected                  gauge

signal_emitted_total{scope,kind}        counter
signal_delivered_total{kind}            counter
signal_boundary_rejected_total          counter   — admitted=false hits
signal_handler_queue_depth{kind}        gauge

contract_advertisements_total           counter
contract_invocations_total{id,result}   counter
contract_invocation_latency_ms{id}      histogram

bulk_transfer_bytes{direction}          counter
bulk_transfer_latency_ms                histogram
```

---

## Phase Timeline

```
Now ──────────────────────────────────────────────────────────────────►
       [Layer 1: DONE]
       [Layer 2: DONE]
                          [──── Phase 3: Bulk / AI ────]
                                            [─ Phase 4: Obs ─]
Weeks:  0         2          4          6          8         10
```

| Phase | Deliverable | Status |
|---|---|---|
| Layer 1 | `gossip_protocol` — transport + KV | **Complete** |
| Layer 2 | Signal/Boundary Mesh, advertise, signal_once, opacity | **Complete** |
| Phase 3 | Bulk transfer, HTTP, streaming, service routing, connection management | Planned |
| Phase 4 | Metrics, Prometheus exporter, admin endpoint | Planned |

---

## Performance Baselines

Measured on the development machine (`cargo bench --bench throughput`), release build, local hot-path only — no network I/O. These are the foundation numbers Layer 3 builds on top of.

### Layer 1 — KV Store

| Benchmark | Median | Notes |
|---|---|---|
| `kv/set` | 151 ns | Local store write + gossip channel dispatch |
| `kv/get` (hit) | 16 ns | Lock-free papaya read |
| `kv/get` (miss) | 13 ns | Same path, no allocation on miss |

### Layer 1 — `scan_prefix` (O(n) characterisation)

| Store size | Matching entries | Median |
|---|---|---|
| 100 | 10 | 332 ns |
| 1,000 | 10 | 2.7 µs |
| 10,000 | 10 | 41 µs |
| 10,000 | 100 | 49 µs |
| 100,000 | 10 | 622 µs |

`scan_prefix` is a full store scan — O(n) with no prefix indexing. At typical pheromone-trail store sizes (100–1,000 entries) the cost is negligible relative to network latency. The 1 ms threshold falls around 100,000 entries. If Layer 3 activity grows the store to that scale, introduce a dedicated prefix index structure.

### Layer 2 — Signal Fan-out

| Handlers registered | Median | Notes |
|---|---|---|
| 1 | ~700 ns | emit + boundary check + deliver + drain |
| 4 | ~1.0 µs | |
| 16 | ~1.4 µs | Very flat — mpsc try_send dominates, scales well |

Signal fan-out is near-linear and cheap. 16 handlers costs less than 2× a single handler. The bottleneck at scale will be gossip forwarding (network), not local delivery.

Run `cargo bench` to regenerate baselines on the target hardware.

---

## What Layers 1 and 2 Look Like in Practice

**Worker node** — writes pheromone trail, advertises, handles invocations:

```rust
let agent = Arc::new(GossipAgent::new(node_id, config));
agent.start().await?;

// Join capability group — gossips KV entry at grp/nlp/<node_id>
agent.join_group("nlp");

// Advertise every 10 s: emits signal (fast delivery) AND writes KV pheromone (persistence)
let load_key = format!("load/{}", agent.node_id());
let agent2 = agent.clone();
let _advert = agent.advertise(
    signal_kind::CONTRACT_AVAILABLE,
    SignalScope::Group("nlp"),
    Duration::from_secs(10),
    move || {
        let state = LoadState { queue_depth: QUEUE.len(), written_at_ms: unix_ms_now() };
        agent2.set(load_key.clone(), encode(&state));  // pheromone trail in the medium
        encode(&state)                                  // also carried in the signal payload
    },
);
// On graceful shutdown: agent.delete(&load_key) — explicit evaporation

// Handle invocations — respond to the specific caller
let mut invoke_rx = agent.signal_rx(signal_kind::INVOKE);
tokio::spawn(async move {
    while let Some(sig) = invoke_rx.recv().await {
        let req: InvokeRequest = decode(&sig.payload);
        let result = run_model(&req.payload).await;
        // Individual scope is correct here: returning a result to a specific
        // caller is directed addressing, not routing selection.
        agent.emit(
            signal_kind::INVOKE_RESULT,
            SignalScope::Individual(sig.sender),
            encode(InvokeResponse { nonce: req.nonce, result }),
        );
    }
});
```

**Invoker node** — emits to the group (no worker selected), reads pheromone on timeout:

```rust
// Register BEFORE emitting so no reply is missed between emit and recv
let nonce = fastrand::u64(..);
let reply_fut = agent.signal_once(
    signal_kind::INVOKE_RESULT,
    Duration::from_secs(5),
    move |s| s.nonce == nonce,
);

// Emit to the group — routing is emergent from opacity, not from explicit selection
agent.emit_async(
    signal_kind::INVOKE,
    SignalScope::Group("nlp"),
    encode(InvokeRequest { nonce, payload: input }),
).await;

match reply_fut.await {
    Some(sig) => decode(&sig.payload),
    None => {
        // Consult the shared medium — no local cache needed
        let any_live = agent.keys().iter()
            .filter(|k| k.starts_with("load/"))
            .filter_map(|k| agent.get(k))
            .filter_map(|b| decode::<LoadState>(&b))
            .any(|s| unix_ms_now() - s.written_at_ms < 30_000);
        if any_live { retry_with_backoff() } else { Err("no workers") }
    }
}
```

**Node reacting to acute overload** — opacity signal for immediate connection teardown:

```rust
// Opacity signal: fast-path notification for time-sensitive teardown.
// Pheromone trail will reflect the same state at the next advertise tick;
// this signal handles the gap before the next KV write propagates.
let mut opaque_rx = agent.signal_rx("boundary.opaque");
tokio::spawn(async move {
    while let Some(sig) = opaque_rx.recv().await {
        let event: OpacityEvent = decode(&sig.payload);
        http_pool.drain_worker(sig.sender, event.reason);
    }
});
```

---

## Legacy Documentation Status

| Document | Status | Action |
|---|---|---|
| `Gossip_Protocol_Locking_Model.docx` | Architecturally obsolete. Describes a mutex-based `LockManager` replaced by lock-free papaya + ShardedSeen. Neither type exists. | Archive. See `src/seen.rs` and `src/store.rs` for the actual concurrency model. |
| `Gossip_Architecture_Guide.pages` | References lock hierarchy and `EnhancedGossipAgent` as existing. Neither does. | Replace with updated architecture guide after Phase 2 ships. |
| `Gossip_Runtime_Guide.pages` | Config section partially accurate; task hierarchy does not match current startup sequence. | Update to reference `GossipConfig` and `SystemStats` directly. |
| `Gossip-AI-Arch.md` | Vision directionally correct. Implementation details stale (DashMap, HTTP-first, no signal layer). | Superseded by this document. |
| `DeepSeek-GossipAIstack.md` | Useful early design sketch. Not a specification. | Archive as design reference with a clear "early sketch" label. |

---

## Design Position

### Prior Art

This architecture is a synthesis of established ideas, each with a long research and production history. Being clear about what is prior art is as important as identifying what is genuinely differentiated.

| Concept | Prior Art |
|---|---|
| Epidemic gossip propagation | Demers et al. 1987; Cassandra, Consul, Redis Cluster |
| Scope-filtered pub/sub | NATS subjects / queue groups; DDS partitions; MQTT topic trees; SIENA content-based routing (1999) |
| Application-layer broadcast filtering | Implicit in all gossip implementations |
| Chemical computing as a design metaphor | Berry & Boudol, *Chemical Abstract Machine*, 1990 |
| Actor-model group routing | Erlang process groups; Akka Cluster distributed pub/sub |

### What Is Genuinely Differentiated

**1. Broker-less scope filtering with epidemic guarantees**

NATS, Kafka, and conventional pub/sub require a broker cluster to route messages by scope. Here, scope is a pure application-layer filter on an epidemic substrate — no routing infrastructure to operate, provision, or fail. A `Group::nlp` signal reaches every node in the cluster; boundary membership determines action, not a routing table.

**2. Group topology as KV state in the same store**

Most architectures use a separate service discovery layer (Consul, etcd, ZooKeeper) for group membership alongside a separate messaging bus. Here, group membership *is* a gossip KV entry — it propagates via the same epidemic mechanism, obeys the same LWW semantics, and is readable by any node as a standard store query. The cluster is self-describing with no additional infrastructure.

**3. State and events unified on a single transport**

One wire format carries both persistent KV state (LWW, queryable, anti-entropy sync'd) and ephemeral signal events (TTL-bounded, fire-and-forget, nonce-dedup'd). Typically these require two different systems — a KV store and a message bus. Unifying them on a single epidemic substrate reduces operational surface area to a single embedded library.

**4. NodeId as the only contract address**

Every mainstream AI agent framework — LangChain, AutoGPT, CrewAI, OpenAI function calling — routes invocations to HTTP endpoints. Callers must know a URL. Here, the contract's address is the publishing node's gossip identity (`NodeId`). There is no endpoint URL to manage, no HTTP port to expose, no separate service registry. The gossip network *is* the addressing layer.

### Closest Comparison: NATS.io

NATS is the nearest existing production system. The gaps:

- **Infrastructure**: NATS requires running a NATS server cluster as external infrastructure. This design is an embedded library — one dependency in `Cargo.toml`, zero servers.
- **State and messaging**: NATS separates KV state (JetStream) from messaging. This design unifies them.
- **Capability advertisement**: NATS has no gossip-based contract / capability discovery model.

### Honest Verdict

This is a well-designed product, not a research contribution. The novelty is the *combination* and the *context*: a single-dependency, embedded, broker-less system that unifies epidemic KV state, ephemeral scoped signals, dynamic group topology, and contract-based capability advertisement — specifically for AI agent swarms where minimising operational overhead matters. None of the individual components is new; the particular assembly, grounded in the biological receptor metaphor as a first-class design principle, is the differentiated position.
