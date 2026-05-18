# Mycelium — Engineering Roadmap

> **Status:** Layer 1 complete. Layer 2 complete. Layers 3–5 planned.
> **Last updated:** 2026-05-14

---

## The Vision

A substrate for **robust adaptive AI systems** — a swarm of agents that discovers each other's
capabilities through a shared medium, signals intent through receptors that filter by scope, and
evolves its topology in response to activity patterns. No coordinator, no central registry, no
single point of failure.

The gossip protocol is not the application. It is the bloodstream the application runs on.

Higher layers build **Actor/Event systems** (Akka-style mailboxes and supervision),
**async Services and RPC** (request-response with emergent load balancing), and
**MCP AI interactions** (Model Context Protocol tool discovery and routing) — or hybrids of
all three. These paradigms share a common substrate: capability advertisement, request routing,
and result correlation, all of which Layer 1 and 2 already provide.

**Serialisation is chosen at need by each agent.** The substrate carries opaque `Bytes` and
routes by `kind` string. An MCP bridge serialises JSON. An internal compute agent uses bincode.
A high-throughput actor mailbox uses a custom flat layout. They coexist on the same mesh without
any of them knowing about each other's payload format. Routing, correlation, and topology are
`kind`-based and fully serialisation-agnostic.

---

## Design Philosophy: Chemical Signalling on an Evolvable Substrate

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
- **Forwarding and acting are completely decoupled.** A node outside `group::nlp` forwards a
  group-scoped signal at full speed without acting on it.

**For adaptive AI systems this matters more than it looks.** Biological systems don't use
barriers or synchronous agreement — they use *threshold activation*. An agent acts when it has
sufficient local information, not when all agents are ready. `last_signal` answers the right
question: "how recently did I hear from my neighbors?" This degrades gracefully rather than
blocking. Barriers are an anti-pattern here.

The topology itself should be adaptive: forward preferentially to recently-active peers, let
inactive connections decay, let fitness-weighted selection emerge over time. `max_forwarding_peers`
is a guardrail on the path to that.

---

## Architecture: Five Layers

```
┌────────────────────────────────────────────────────────────────────┐
│  Layer 5: Observability                              [Phase 5]     │
│  Prometheus metrics · latency histograms · dropped_frames alerts   │
├────────────────────────────────────────────────────────────────────┤
│  Layer 4: AI Integration                             [Phase 4]     │
│  MCP server/client bridge · multi-agent coordination               │
│  Actor supervision trees · conversation-scoped routing             │
│  Serialisation at need: JSON (MCP) / bincode / protobuf per agent  │
├────────────────────────────────────────────────────────────────────┤
│  Layer 3: Service Patterns                           [Phase 3]     │
│  RPC over gossip · Actor/Event mailboxes · bulk HTTP               │
│  Streaming LLM calls · service routing · connection management     │
│  Signal carries a ticket; HTTP carries the weight                  │
├────────────────────────────────────────────────────────────────────┤
│  Layer 2: Signal / Boundary Mesh                     [COMPLETE]    │
│  advertise · signal_once · last_signal · opacity                   │
│  watch · quorum · suppress/unsuppress · manage_opacity             │
│  System / Group / Individual scopes · heartbeat-driven retry       │
├────────────────────────────────────────────────────────────────────┤
│  Layer 1: Gossip Transport                           [COMPLETE]    │
│  GossipAgent · LWW KV · anti-entropy · zero-copy fan-out           │
│  max_forwarding_peers · max_peers · dropped_frames counter         │
└────────────────────────────────────────────────────────────────────┘
```

**Fundamental separation of concerns:**

| Layer 1 KV store | Layer 2 Signals |
|---|---|
| *State* — what is true right now | *Events* — something happened |
| Last-write-wins, persistent, anti-entropy synced | Ephemeral, TTL-bounded, best-effort |
| Capability advertisements, group topology, load state | Invocation requests, results, acute notifications |
| Queryable by any agent at any time | Fire-and-forget; handled or missed |

**Higher layer convergence:**

All three higher-layer paradigms (Actor, RPC, MCP) reduce to the same three substrate operations:
1. *Advertise capability* — `advertise()` + KV write
2. *Route a request* — `emit_async()` to group scope (routing is emergent from opacity)
3. *Return a result* — `signal_once()` with nonce correlation

The substrate doesn't know which paradigm sits above it. Each agent chooses its payload
serialisation independently.

---

## Layer 1 — Gossip Transport (Complete)

The substrate. Lock-free epidemic KV propagation. This layer knows nothing about contracts,
agents, signals, or scopes. It is intentionally general — a high-performance replication
primitive any layer can build on.

**What was hardened in 2026-05-14:**
- `max_forwarding_peers` config field — caps gossip fan-out targets. Set to
  `bootstrap_peers.len()` for fixed-topology meshes to prevent O(N²) forwarding traffic.
- `max_peers` config field — caps the peer *table* (piggybacked peer discovery via Ping). Without
  this cap, every agent in a 256-node cluster eventually learns all 256 others and the health
  monitor opens persistent connections to each, accumulating ~65,000 file descriptors and
  saturating the tokio runtime. Set to `bootstrap_peers.len()` for grid/ring topologies.
- `dropped_frames: u64` in `SystemStats` — cumulative counter of silently-dropped gossip frames
  (channel-full). Previously invisible; now the first diagnostic to check when writes fail to
  propagate. Incremented at both the agent→shard and shard→peer-writer drop sites.
- `writer_channel_depth` doc clarified as a **correctness threshold**, not a performance knob.
  When full, frames are silently dropped. Sizing formula documented on the field.

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
agent.set(key, value) -> bool           // local always updated; false = channel full
agent.set_async(key, value).await -> bool
agent.get(key) -> Option<Bytes>
agent.delete(key) -> bool               // gossips a tombstone
agent.delete_async(key).await -> bool
agent.keys() -> Vec<Arc<str>>
agent.scan_prefix(prefix) -> Vec<(Arc<str>, Bytes)>

// Reactive
agent.subscribe(key) -> watch::Receiver<Option<Bytes>>

// Introspection
agent.system_stats() -> SystemStats     // includes dropped_frames
```

**Key config fields for Layer 1:**

| Field | Default | Purpose |
|---|---|---|
| `max_forwarding_peers` | `i64::MAX` | Cap gossip targets; set to `bootstrap_peers.len()` for fixed-topology meshes |
| `writer_channel_depth` | `64` | Per-peer outbound channel depth (ring buffer). **Correctness threshold** — size to `N × fan_out` |
| `health_check_interval_secs` | `10` | Peer liveness ping interval |
| `default_ttl` | `5` | Hops before a message stops propagating |
| `gossip_shards` | `min(CPU, 16)` | Gossip worker tasks; set to `1` for demo/debug to cut task count |

**Future Layer 1 improvement (not blocking):**
Activity-weighted forwarding — prefer recently-active peers over randomly-discovered ones.
Currently `max_forwarding_peers` caps the target count; a follow-on pass would weight selection
by last-received-from timestamp so the topology self-organises around actual traffic patterns.

---

## Layer 2 — Signal / Boundary Mesh (Complete)

Layer 2 adds ephemeral events and local receptors on top of the Layer 1 gossip transport. See
README.md for the full API reference, observability guide, and opacity/inhibition scenarios.

The complete stable API is documented in the [Complete Layer 2 API](#complete-layer-2-api) section below.

### Complete Layer 2 API

```rust
// ── Group membership ─────────────────────────────────────────────────────
agent.join_group(name)
agent.leave_group(name)
agent.groups() -> Vec<Arc<str>>            // current memberships

// ── Emit / receive ───────────────────────────────────────────────────────
agent.emit(kind, scope, payload)           -> bool   // false = shard full
agent.emit_async(kind, scope, payload).await -> bool // false = shard dead
agent.signal_rx(kind)                      -> mpsc::Receiver<Signal>
agent.signal_rx_with_capacity(kind, cap)   -> mpsc::Receiver<Signal>

// ── One-shot request/response ────────────────────────────────────────────
agent.signal_once(kind, timeout, predicate).await -> Option<Signal>

// ── Periodic heartbeat ───────────────────────────────────────────────────
agent.advertise(kind, scope, interval, payload_fn) -> AdvertiseHandle

// ── Fault detection ───────────────────────────────────────────────────────
agent.last_signal(kind) -> Option<Instant>       // when was kind last delivered here?
agent.watch(kind, threshold, on_stale) -> WatchHandle  // calls on_stale() when silent > threshold

// ── Threshold activation ─────────────────────────────────────────────────
agent.quorum(kind, min_senders, window) -> bool  // ≥ min_senders distinct nodes in window?

// ── Inhibition / refractory period ──────────────────────────────────────
agent.suppress(kind, duration)                   // block kind delivery for duration
agent.unsuppress(kind)                           // lift early
agent.is_suppressed(kind) -> bool                // diagnostic

// ── Opacity — load-adaptive admission ────────────────────────────────────
agent.opacity(kind) -> f32                       // fill ratio for kind's handler channel
agent.manage_opacity(kind, scope, hint)          -> OpacityHandle
agent.manage_opacity_gated(kind, scope, hint, gate) -> OpacityHandle
```

---

### Layer 2 Observability

The mesh is not a black box. Every observable dimension has a dedicated query:

| Observable | API | What it answers |
|---|---|---|
| Was a kind heard recently? | `last_signal(kind)` | Last delivery timestamp |
| Has a kind gone silent? | `watch(kind, threshold, cb)` | Calls `cb` when silent > threshold |
| Have K distinct nodes checked in? | `quorum(kind, K, window)` | Consensus-adjacent readiness |
| Is this node refusing a kind? | `is_suppressed(kind)` | Active inhibition in effect |
| How loaded is this node's intake? | `opacity(kind)` | Fill ratio 0.0–1.0 |
| Are peers notified of overload? | `manage_opacity(...)` | Emits `boundary.opaque` to peers |
| What groups is this node in? | `groups()` | Current boundary memberships |
| How many workers are alive? | `scan_prefix("load/")` | Pheromone trail count (Layer 1) |
| Are gossip writes being lost? | `system_stats().dropped_frames` | Cumulative drop counter |

**Observability scenario — diagnosing a stalled worker pool:**

```rust
// Worker stopped responding. Work through the observability layers:

// 1. Check propagation health first
let stats = supervisor.system_stats();
if stats.dropped_frames > prev_dropped {
    // Gossip is losing frames — fix writer_channel_depth before anything else
}

// 2. Check if any worker has been heard recently
let fresh = supervisor.last_signal(signal_kind::CONTRACT_AVAILABLE)
    .map(|t| t.elapsed() < Duration::from_secs(30))
    .unwrap_or(false);

// 3. Check if enough workers are present (quorum)
let pool_ready = supervisor.quorum(
    signal_kind::CONTRACT_AVAILABLE, 2, Duration::from_secs(30)
);

// 4. Read the authoritative state from the pheromone trails
let live_trails = supervisor.scan_prefix("load/nlp/")
    .into_iter()
    .filter_map(|(_, b)| decode::<LoadState>(&b))
    .filter(|s| unix_ms_now() - s.written_at_ms < 30_000)
    .count();

println!("signals fresh: {fresh}, quorum: {pool_ready}, live trails: {live_trails}");
// Divergence between pheromone trail count and quorum count means a node is
// alive (trail present) but its signal channel is suppressed or saturated.
```

---

### Opacity vs Inhibition — Conceptual Distinction

Layer 2 has two independent mechanisms that reduce signal delivery. They look superficially
similar and are routinely confused. They are not.

#### Opacity — passive, automatic, emergent

Opacity is a *property* the boundary acquires automatically when handler channels fill.

```
fill_ratio = 1.0 - (channel_remaining / channel_capacity)
admit_prob = 1.0 - fill_ratio          (for System and Group scope)
```

No application code activates opacity. When `fill_ratio = 0.6`, 60% of incoming `System` and
`Group` signals are shed at the boundary. The node still **forwards all signals** — epidemic
propagation continues uninterrupted — it simply stops reacting to new arrivals. This is
emergent backpressure with no coordinator.

`Individual` scope bypasses opacity unconditionally — a directed reply must always arrive.

`manage_opacity` adds a *notification layer* on top: a governor task that monitors fill ratio
and emits `boundary.opaque` / `boundary.transparent` signals to peers so they can route new
work elsewhere before the channel fully saturates. The application provides a threshold hint;
the library clamps and adjusts it based on the rate of fill change (rising trend → lower
threshold, stabilising → relax). The gate parameter lets the application veto transitions,
with a library override at `fill_ratio == 1.0`.

```
Opacity:        automatic, probabilistic, local self-protection
manage_opacity: proactive peer notification — "I am entering overload"
```

#### Inhibition — active, deterministic, application-controlled

`suppress(kind, duration)` is a deliberate application decision. For the duration, **zero**
signals of that kind are delivered — deterministic, not probabilistic. The node keeps updating
`last_signal` timestamps and keeps forwarding signals; only local handler delivery is blocked.

Biological analogue: the *refractory period* after a neuron fires — the cell explicitly will
not fire again for a fixed window regardless of how much stimulus arrives.

```
suppress:  deterministic, total, application-initiated
opacity:   probabilistic, load-proportional, automatic
```

#### Choosing the right tool

| Situation | Use |
|---|---|
| Node is overloaded — stop accepting random work | Opacity handles this automatically |
| Notify peers before becoming overloaded | `manage_opacity` |
| I just handled one invocation — block the next for 500ms | `suppress` |
| Prevent sync storms — process one then gate for 5s | `suppress` |
| Idempotency window — deduplicate re-sent requests | `suppress` |
| Diagnose: is this node voluntarily refusing X? | `is_suppressed(kind)` |
| Diagnose: is this node overwhelmed by X? | `opacity(kind)` |

### Wire Protocol

One variant in `WireMessage` (`src/framing.rs`):

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

Signal frames share the TTL/nonce/fan-out machinery as `Data` frames. Every node that receives
a signal decrements TTL and forwards unconditionally — boundary check happens *after* forwarding.

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
    // Best-effort epidemic delivery. Shed under load by the opacity mechanism.
    // Do not use for coordination requiring guaranteed delivery — use local
    // timers + KV state propagation instead.
    System,
    Group("nlp"),        // only nodes that have called join_group("nlp")
    Individual(node_id), // exactly one node; bypasses opacity
}
```

### Variable Opacity — Load-Adaptive Admission

When handler channels fill, the boundary probabilistically sheds incoming signals. The admission
probability falls linearly as channels fill, reaching zero when they are completely full.

```
fill_ratio = 1.0 - (channel_remaining / channel_capacity)
admit      = fastrand::f32() >= fill_ratio
```

`Individual` scope always bypasses opacity — there is no routing alternative for a directed reply.

**Emergent backpressure**: an overloaded node goes opaque and stops consuming work. It continues
to *forward* all signals — the network remains fully connected — but the node itself no longer
reacts. Upstream nodes that see no response naturally retry elsewhere or back off.

### Heartbeat-Driven Retry Model

Workers advertise availability periodically. Invokers track freshness and retry rather than
assuming delivery. This gives at-least-once semantics without a broker.

```rust
// Worker side
let _handle = agent.advertise(
    signal_kind::CONTRACT_AVAILABLE,
    SignalScope::Group("nlp"),
    Duration::from_secs(5),
    || encode(WorkerState { queue_depth, accepted_kinds: &["sentiment"] }),
);

// Invoker side — register BEFORE emitting so no reply is missed
let nonce = fastrand::u64(..);
let reply_fut = agent.signal_once(
    signal_kind::INVOKE_RESULT,
    Duration::from_secs(5),
    move |s| s.nonce == nonce,
);
agent.emit_async(signal_kind::INVOKE, SignalScope::Group("nlp"),
    encode(InvokeRequest { nonce, payload: input })).await;

match reply_fut.await {
    Some(sig) => handle_result(sig),
    None => {
        if agent.last_signal(signal_kind::CONTRACT_AVAILABLE)
               .map(|t| t.elapsed() < Duration::from_secs(30))
               .unwrap_or(false)
        { retry_with_backoff() } else { Err("no workers") }
    }
}
```

### Stigmergic Load State — Pheromone Trails

Workers write load state into the Layer 1 KV store alongside their `advertise()` heartbeat.
The store is the shared medium — new nodes receive the full load picture immediately via
anti-entropy sync; no local cache to invalidate; stale entries decay via embedded timestamps.

```rust
let load_key = format!("load/{}", agent.node_id());
let agent2 = agent.clone();
let _advert = agent.advertise(
    signal_kind::CONTRACT_AVAILABLE,
    SignalScope::Group("nlp"),
    Duration::from_secs(10),
    move || {
        let state = LoadState { queue_depth: QUEUE.len(), written_at_ms: unix_ms_now() };
        agent2.set(load_key.clone(), encode(&state));  // pheromone trail — persistent
        encode(&state)                                  // signal payload — fast delivery
    },
);
// On graceful shutdown: agent.delete(&load_key) — explicit evaporation
```

Routing decisions read the store directly — no signal handler, no local cache:

```rust
let load_picture = agent.scan_prefix("load/")
    .into_iter()
    .filter_map(|(k, b)| {
        let s: LoadState = decode(&b)?;
        if unix_ms_now() - s.written_at_ms > 30_000 { return None; } // evaporation
        Some((k, s))
    })
    .collect::<Vec<_>>();
```

### Competitive Response — Emergent Routing

No invoker selects a worker. Routing emerges from opacity state and processing speed.

```
Invoker emits: SignalScope::Group("nlp") → floods all nlp-group nodes
               Overloaded workers: boundary opaque → signal not admitted → no response
               Available workers: boundary transparent → signal admitted → process → reply
Invoker receives: first Individual reply → done
                  timeout → check pheromone trails → retry or escalate
```

### Well-Known Signal Kinds

```rust
pub mod signal_kind {
    pub const INVOKE:               &str = "invoke";
    pub const INVOKE_RESULT:        &str = "invoke.result";
    pub const INVOKE_BULK:          &str = "invoke.bulk";       // Layer 3 ticket
    pub const BOUNDARY_OPAQUE:      &str = "boundary.opaque";
    pub const BOUNDARY_TRANSPARENT: &str = "boundary.transparent";
    pub const CONTRACT_AVAILABLE:   &str = "contract.available";
    pub const CONTRACT_WITHDRAWN:   &str = "contract.withdrawn";
    pub const CLUSTER_EVENT:        &str = "cluster.event";
    pub const HEALTH_PROBE:         &str = "health.probe";
    pub const HEALTH_ACK:           &str = "health.ack";
}
```

---

## Layer 3 — Service Patterns (Phase 3)

Layer 3 builds idiomatic service patterns on top of the Layer 1 and 2 substrate. Three paradigms
converge here — they share the same underlying operations (advertise / route / correlate) and can
coexist on the same mesh.

### Actor / Event

Actors are gossip agents with a defined key namespace (their mailbox state in the KV store) and
a signal handler (their message loop). Location transparency comes from `NodeId`-based addressing
— callers emit to `Individual(node_id)` without knowing the actor's network location.

```
actor registration: set("actors/<name>/<node_id>", metadata)
message send:       emit("actor.msg", Individual(target_node_id), payload)
reply:              emit("actor.reply", Individual(sender_node_id), result)
state read:         get("actors/<name>/<node_id>/state")
```

Supervision is Layer 2's `watch()` primitive (see Layer 2 completion): a supervisor watches a
worker's heartbeat kind and triggers restart when stale.

### Async Services / RPC

RPC over gossip: invoker registers `signal_once` before emitting; worker handles via `signal_rx`
and responds with `Individual` scope. Load balancing is emergent from opacity. Circuit breaking
is `last_signal` staleness. Retries use the heartbeat-driven model.

Each RPC service chooses its own serialisation — bincode for internal Rust services, JSON for
MCP-compatible interfaces, protobuf for external-facing endpoints. The substrate carries `Bytes`
and routes by `kind` string; payload format is transparent to the gossip layer.

### Bulk Payloads

When payloads exceed practical signal size — multi-KB prompts, large model outputs — a
`invoke.bulk` signal carries a *ticket* (correlation ID + HTTP endpoint):

```
Caller  →  Individual signal "invoke.bulk"
           payload: { contract_id, corr_id, input_endpoint }
Target  ←  fetches large input from caller's HTTP endpoint
        →  runs model
        →  Individual signal "invoke.result"
           payload: { corr_id, result_endpoint OR inline_result }
Caller  ←  fetches result if referenced
```

HTTP is a Layer 3 concern only. Agents handling small payloads need no HTTP server.

### Streaming

Token-by-token LLM responses ride HTTP chunked transfer or SSE. A Layer 2 `invoke.result`
signal fires when streaming ends, carrying the correlation ID for cleanup.

```toml
# Added at Layer 3
reqwest = { version = "0.12", features = ["json", "stream", "rustls-tls"],
            default-features = false }
```

### Layer 3 Events vs Layer 2 Signals

Layer 3 introduces a distinct `Event` type for transport-bound, connection-scoped, ordered
delivery — conceptually related to signals but with fundamentally different guarantees:

| Property | Layer 2 Signal | Layer 3 Event |
|---|---|---|
| Delivery | Epidemic flood | Point-to-point (SSE, gRPC stream) |
| Ordering | None | Ordered per connection |
| Reliability | Best-effort; can be missed | At-least-once on open stream |
| Flow control | Probabilistic opacity shedding | Transport-level (HTTP/2, TCP) |

A `Signal` can be silently dropped. An `Event` on an open stream will not be missed. Sharing a
type would obscure this — `Event` is explicitly distinct.

---

## Layer 4 — AI Integration (Phase 4)

Layer 4 bridges the gossip substrate to the AI ecosystem.

### MCP (Model Context Protocol)

MCP is request-response: tool discovery → tool invocation → structured result. The gossip layer
provides discovery and routing; MCP provides the wire format and schema contract.

```
Tool registry:   set("tools/<tool_name>/<node_id>", tool_schema_json)
Tool discovery:  scan_prefix("tools/") → list available tools + their schemas
Tool invocation: emit("mcp.invoke", Individual(node_id), json_request)
Tool result:     signal_once("mcp.result", timeout, |s| s.nonce == req_nonce)
```

An MCP server running as a gossip agent advertises its tools to the mesh. Clients discover
tools via KV scan. Routing is emergent. The MCP JSON payload is opaque to the gossip layer —
the substrate carries it as `Bytes`, identified by `kind` string.

Multi-agent coordination: an orchestrator agent discovers worker agents via tool scan, routes
MCP requests to appropriate workers, aggregates results. All state flows through the KV store;
conversation context (multi-turn) lives in a KV namespace per conversation ID.

### Supervision

Layer 4 supervision trees use Layer 2's `watch()` primitive to monitor AI agent liveness.
A supervisor watches `contract.available` heartbeats from worker agents; on stale, triggers
respawn, failover, or escalation. No separate monitoring infrastructure needed.

### Serialisation Autonomy

Each agent at Layer 4 picks its serialisation independently:
- MCP bridge agents: JSON (MCP wire format requirement)
- Internal compute agents: bincode (fast, Rust-native, already a dependency)
- External-facing agents: protobuf or JSON-Schema
- Hybrid agents: JSON at the MCP boundary, bincode internally

The substrate routes by `kind` string. Payload format is a contract between the emitter and
its intended receivers, not a substrate concern.

---

## Layer 5 — Observability (Phase 5)

Prometheus-compatible metrics via a single scrape endpoint. Uses the `metrics` facade — zero-cost
when no recorder is installed; Layers 1 and 2 emit calls without a hard runtime dependency.

```
gossip_messages_received_total
gossip_messages_deduplicated_total
gossip_frames_dropped_total          ← backed by dropped_frames counter (already in SystemStats)
gossip_store_entries
gossip_peers_connected

signal_emitted_total{scope,kind}
signal_delivered_total{kind}
signal_boundary_rejected_total
signal_handler_queue_depth{kind}

contract_invocations_total{id,result}
contract_invocation_latency_ms{id}
bulk_transfer_bytes{direction}
```

---

## Phase Timeline

```
Now ──────────────────────────────────────────────────────────────────►
       [Layer 1: DONE]
       [Layer 2: DONE]
                         [──── Phase 3: Service Patterns ────]
                                          [── Phase 4: AI Integration ──]
                                                        [─ Phase 5: Obs ─]
Weeks:  0         2          4          6          8         10        12
```

| Phase | Deliverable | Status |
|---|---|---|
| Layer 1 | Gossip transport + KV, topology controls, diagnostics | **Complete** |
| Layer 2 | Signal/Boundary Mesh, advertise, signal_once, opacity | **Complete** |
| Layer 2 | watch, quorum, suppress/unsuppress, manage_opacity | **Complete** |
| Phase 3 | Actor/Event, RPC, bulk HTTP, streaming | Planned |
| Phase 4 | MCP bridge, AI coordination, supervision trees | Planned |
| Phase 5 | Metrics, Prometheus exporter | Planned |

---

## Performance Baselines

Measured on the development machine (`cargo bench --bench throughput`), release build, local
hot-path only — no network I/O.

### Layer 1 — KV Store

| Benchmark | Median | Notes |
|---|---|---|
| `kv/set` | 151 ns | Local store write + gossip channel dispatch |
| `kv/get` (hit) | 16 ns | Lock-free papaya read |
| `kv/get` (miss) | 13 ns | Same path, no allocation on miss |

### Layer 1 — `scan_prefix` (O(n))

| Store size | Matching entries | Median |
|---|---|---|
| 100 | 10 | 332 ns |
| 1,000 | 10 | 2.7 µs |
| 10,000 | 10 | 41 µs |
| 10,000 | 100 | 49 µs |
| 100,000 | 10 | 622 µs |

`scan_prefix` is a full store scan. At typical pheromone-trail store sizes (100–1,000 entries)
the cost is negligible relative to network latency. The 1 ms threshold falls around 100,000
entries — introduce a prefix index if Layer 3/4 activity grows the store to that scale.

### Layer 2 — Signal Fan-out

| Handlers registered | Median | Notes |
|---|---|---|
| 1 | ~700 ns | emit + boundary check + deliver + drain |
| 4 | ~1.0 µs | |
| 16 | ~1.4 µs | Very flat — mpsc try_send dominates |

Signal fan-out is near-linear and cheap. The bottleneck at scale is gossip forwarding (network),
not local delivery.

Run `cargo bench` to regenerate baselines on the target hardware.

---

## What Layers 1 and 2 Look Like in Practice

**Worker node** — writes pheromone trail, advertises, handles invocations:

```rust
let agent = Arc::new(GossipAgent::new(node_id, config));
agent.start().await?;
agent.join_group("nlp");

let load_key = format!("load/{}", agent.node_id());
let agent2 = agent.clone();
let _advert = agent.advertise(
    signal_kind::CONTRACT_AVAILABLE,
    SignalScope::Group("nlp"),
    Duration::from_secs(10),
    move || {
        let state = LoadState { queue_depth: QUEUE.len(), written_at_ms: unix_ms_now() };
        agent2.set(load_key.clone(), encode(&state));
        encode(&state)
    },
);

let mut invoke_rx = agent.signal_rx(signal_kind::INVOKE);
tokio::spawn(async move {
    while let Some(sig) = invoke_rx.recv().await {
        let req: InvokeRequest = decode(&sig.payload);
        let result = run_model(&req.payload).await;
        agent.emit(
            signal_kind::INVOKE_RESULT,
            SignalScope::Individual(sig.sender),
            encode(InvokeResponse { nonce: req.nonce, result }),
        );
    }
});
```

**Invoker node** — emergent routing, pheromone trail fallback:

```rust
let nonce = fastrand::u64(..);
let reply_fut = agent.signal_once(
    signal_kind::INVOKE_RESULT,
    Duration::from_secs(5),
    move |s| s.nonce == nonce,
);
agent.emit_async(
    signal_kind::INVOKE,
    SignalScope::Group("nlp"),
    encode(InvokeRequest { nonce, payload: input }),
).await;

match reply_fut.await {
    Some(sig) => decode(&sig.payload),
    None => {
        let any_live = agent.scan_prefix("load/")
            .into_iter()
            .filter_map(|(_, b)| decode::<LoadState>(&b))
            .any(|s| unix_ms_now() - s.written_at_ms < 30_000);
        if any_live { retry_with_backoff() } else { Err("no workers") }
    }
}
```

---

## Design Position

### Prior Art

| Concept | Prior Art |
|---|---|
| Epidemic gossip propagation | Demers et al. 1987; Cassandra, Consul, Redis Cluster |
| Scope-filtered pub/sub | NATS subjects / queue groups; DDS partitions; MQTT topic trees; SIENA content-based routing |
| Application-layer broadcast filtering | Implicit in all gossip implementations |
| Chemical computing as design metaphor | Berry & Boudol, *Chemical Abstract Machine*, 1990 |
| Actor-model group routing | Erlang process groups; Akka Cluster distributed pub/sub |
| MCP tool discovery | Anthropic MCP specification, 2024 |

### What Is Genuinely Differentiated

**1. Broker-less scope filtering with epidemic guarantees.** NATS, Kafka, and conventional
pub/sub require a broker cluster. Here, scope is a pure application-layer filter on an epidemic
substrate — no routing infrastructure to operate, provision, or fail.

**2. Group topology as KV state in the same store.** Group membership *is* a gossip KV entry —
propagates via the same mechanism, obeys the same LWW semantics, readable by any node.
No separate service discovery layer (Consul, etcd, ZooKeeper) required.

**3. State and events unified on a single transport.** One wire format carries both persistent
KV state (LWW, queryable, anti-entropy synced) and ephemeral signal events (TTL-bounded,
fire-and-forget). Typically these require two different systems.

**4. Serialisation autonomy.** Each agent picks its payload format independently — JSON for MCP
compatibility, bincode for internal speed, protobuf for external contracts. The substrate routes
by `kind` string and carries opaque `Bytes`. No cluster-wide serialisation migration needed when
one agent upgrades its format.

**5. NodeId as the only contract address.** No HTTP endpoint to manage, no service registry to
run. The gossip identity *is* the address.

### Closest Comparison: NATS.io

NATS is the nearest existing production system. The gaps:
- **Infrastructure**: NATS requires a server cluster. This design is an embedded library — one
  `Cargo.toml` dependency, zero servers.
- **State and messaging**: NATS separates KV state (JetStream) from messaging. This design
  unifies them.
- **Capability advertisement**: NATS has no gossip-based contract / capability discovery model.
- **Serialisation**: NATS is payload-agnostic like this design, but does not offer the adaptive
  topology or biological-metaphor admission control.

### Honest Verdict

This is a well-designed product, not a research contribution. The novelty is the *combination*
and the *context*: a single-dependency, embedded, broker-less system that unifies epidemic KV
state, ephemeral scoped signals, dynamic group topology, adaptive topology control, and
contract-based capability advertisement — specifically targeting adaptive AI agent swarms where
minimising operational overhead and maximising evolvability matter. None of the individual
components is new; the particular assembly, grounded in the biological receptor metaphor as a
first-class design principle, is the differentiated position.

