# Mycelium — Engineering Roadmap

> **Status:** Layer 1 complete. Layer 2 complete. Layer III (Consensus) complete. Capability & Discovery subsystem complete. Layers 3–5 (Service Patterns / AI / Observability) planned.
> **Last updated:** 2026-05-20

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
│  MCP server/client bridge · Python + TypeScript language bridges   │
│  HTTP gateway sidecar · supervision trees · credential context     │
├────────────────────────────────────────────────────────────────────┤
│  Layer 3: Service Patterns                           [Phase 3]     │
│  Embedded HTTP · SSE streaming · rpc_call/rpc_respond              │
│  invoke.bulk ticket · Actor/Event mailboxes · scatter-gather       │
├────────────────────────────────────────────────────────────────────┤
│  Opt-In Consistency & Ordering Overlay               [Planned]     │  ← cross-cutting
│  consistent_set · consistent_get · distributed_lock · elect_leader │
│  append · subscribe_log · scan_log · compact_log (ordered log)     │
│  subscribe_log_group (consumer groups) · emit_reliable             │
│  shard_for · emit_sharded (cluster sharding)                       │
├────────────────────────────────────────────────────────────────────┤
│  Capability & Discovery Subsystem                    [COMPLETE]    │
│  advertise_capability · resolve · watch_capabilities               │
│  declare_requirement · watch_requirement · RequirementStatus       │
│  demand · watch_demand · DemandStatus (pressure surface)           │
│  define_capability_group · gcap/ projections (emergent groups)     │
│  resolve_wiring · watch_wiring · signal_wired_via                  │
│  resolve_with_locality · signal_wired_via_locality                 │
│  LocalityPreference · locality_path config field                   │
├────────────────────────────────────────────────────────────────────┤
│  Layer III: Consensus                                [COMPLETE]    │
│  ConsensusEngine · epidemic two-phase voting · OpaqueRecompute     │
│  group_propose · system_propose · ConsensusResult                  │
│  KV-backed committed slots · ballot loop · opaque-member aware     │
├────────────────────────────────────────────────────────────────────┤
│  Layer 2: Signal / Boundary Mesh                     [COMPLETE]    │
│  advertise · advertise_persistent · signal_once · last_signal      │
│  watch · quorum · quorum_persistent · suppress · manage_opacity    │
│  System / Group / Individual scopes · heartbeat-driven retry       │
│  epidemic_extra_peers · listener auto-restart · peer_drop_counts   │
├────────────────────────────────────────────────────────────────────┤
│  Layer 1: Gossip Transport                           [COMPLETE]    │
│  GossipAgent · LWW KV · anti-entropy · zero-copy fan-out           │
│  max_forwarding_peers · max_peers · dropped_frames counter         │
│  prefix_index · gossip_shard_fill · shutdown-race protection       │
└────────────────────────────────────────────────────────────────────┘
```

**Design principle — consistency and ordering as opt-in layers, not foundations.**
Every operation defaults to epidemic (fast, available, zero coordination overhead). You escalate
to stronger guarantees only for the specific operation that requires them. A node that never calls
`consistent_set` pays zero overhead for its existence. The same cluster simultaneously supports
sub-millisecond epidemic signals, causally-ordered log streams, and linearizable writes — different
operations choosing different guarantees, no separate infrastructure for each tier.

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

**What was hardened through 2026-05-19:**
- `max_forwarding_peers` — caps gossip fan-out targets per shard. Set to `bootstrap_peers.len()`
  for fixed-topology meshes to prevent O(N²) forwarding traffic.
- `max_peers` — caps the peer *table* (piggybacked peer discovery via Ping). Without this cap,
  every agent in a 256-node cluster eventually learns all 256 others and the health monitor opens
  persistent connections to each. Set to `bootstrap_peers.len()` for grid/ring topologies.
- `dropped_frames: u64` in `SystemStats` — cumulative counter of silently-dropped gossip frames.
  Incremented at both the agent→shard and shard→peer-writer drop sites. A saturation warning
  (`WARN`) fires every 1 000th cumulative drop to surface channel backpressure in logs.
- `writer_channel_depth` default raised to `256` and documented as a **correctness threshold**.
  When full, frames are silently dropped. Sizing formula documented on the field.
- `epidemic_extra_peers` — replaces the former hardcoded `EPIDEMIC_K = 3` constant. Configurable
  per-deployment; raise to 5–7 for clusters > 1 000 nodes, lower to 1–2 for small clusters.
- Listener auto-restart with exponential backoff (100 ms → 30 s cap) on fatal TCP accept errors.
  Previously a listener crash left the node unreachable until the process was restarted.
- `get_or_spawn_writer` shutdown race fix: checks `*shutdown_tx.borrow()` before spawning a new
  peer writer, returning a dead sender immediately if shutdown is already active. In-flight
  connection handlers can no longer insert unkillable writer tasks after `shutdown_with_timeout`.
- `peer_drop_counts()` — returns per-peer cumulative frame-drop counters, allowing operators to
  identify which specific peers are slow or unreachable rather than just seeing the global total.
- `quorum_written` in-memory rate-limit on `SignalHandlers` — tracks when each `sys/quorum/` key
  was last written (max once/second), replacing a per-call KV store read with an in-memory check.
  Evicted in `trim_sender_log` when entries age past `signal_window_secs`.

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
| `writer_channel_depth` | `256` | Per-peer outbound channel depth (ring buffer). **Correctness threshold** — size to `N × fan_out` |
| `health_check_interval_secs` | `10` | Peer liveness ping interval |
| `default_ttl` | `5` | Hops before a message stops propagating |
| `gossip_shards` | `min(CPU, 16)` | Gossip worker tasks; set to `1` for demo/debug to cut task count |
| `epidemic_extra_peers` | `3` | Random non-member peers added to Group-scoped signal fan-out. Raise to 5–7 for clusters > 1 000 nodes |
| `group_aware_forwarding` | `true` | Route Group signals to members + `epidemic_extra_peers`. `false` = broadcast all |
| `max_peers` | `i64::MAX` | Cap the peer table; set to `bootstrap_peers.len()` for grid/ring topologies |
| `writer_idle_timeout_secs` | `0` | Close idle peer TCP connections after N seconds (`0` = no timeout) |
| `signal_window_secs` | `600` | Sender-log and `quorum_written` retention window |
| `max_store_entries` | `0` | Hard cap on live KV entries (`0` = unlimited) |

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
// Like advertise, but also writes payload to Layer I (key: svc/{kind}/{node_id}).
// Tombstoned automatically on drop/shutdown. Lets late joiners discover via scan_prefix.
agent.advertise_persistent(kind, scope, interval, payload_fn) -> AdvertiseHandle

// ── Fault detection ───────────────────────────────────────────────────────
agent.last_signal(kind) -> Option<Instant>       // when was kind last delivered here?
agent.watch(kind, threshold, on_stale) -> WatchHandle  // calls on_stale() when silent > threshold

// ── Threshold activation ─────────────────────────────────────────────────
agent.quorum(kind, min_senders, window) -> bool  // ≥ min_senders distinct nodes in window?
// Same as quorum but reads from sys/quorum/ in Layer I — survives process restarts.
agent.quorum_persistent(kind, window) -> usize   // count of distinct senders in window

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
| Have K nodes checked in (restart-safe)? | `quorum_persistent(kind, window)` | Reads `sys/quorum/` from Layer I |
| Is this node refusing a kind? | `is_suppressed(kind)` | Active inhibition in effect |
| How loaded is this node's intake? | `opacity(kind)` | Fill ratio 0.0–1.0 |
| Are peers notified of overload? | `manage_opacity(...)` | Emits `boundary.opaque` to peers |
| What groups is this node in? | `groups()` | Current boundary memberships |
| How many workers are alive? | `scan_prefix("load/")` | Pheromone trail count (Layer 1) |
| Are gossip writes being lost? | `system_stats().dropped_frames` | Cumulative drop counter |
| Which peers are dropping frames? | `peer_drop_counts()` | Per-peer cumulative drop count |

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

## Capability & Discovery Subsystem (Complete)

First-class capability advertisement, discovery, demand pressure, and locality-aware routing,
built entirely on the Layer I KV store. No separate registry, no coordination overhead; all
capability state lives under `cap/`, `req/`, and `gcap/` namespaces and is anti-entropy-synced
to late joiners automatically.

Three browser-visual examples demonstrate the subsystem end-to-end:
- **[`examples/capability_market.rs`](examples/capability_market.rs)** (port 8097) — four
  capability types, providers and requirers, demand-pressure bars, live toggle
- **[`examples/emergent_pool.rs`](examples/emergent_pool.rs)** (port 8098) — 20-node worker
  pool assembling via `define_capability_group`, consumers dispatching via `signal_wired_via`
- **[`examples/locality_wiring.rs`](examples/locality_wiring.rs)** (port 8099) — 12 nodes
  across two AZs, concentric rings showing locality depth, resolver shifting in real time

### Direct Capability (Phases 0–3)

```rust
// Advertise — reasserts cap/{node_id}/{ns}/{name} on an interval; tombstones on drop.
let _handle = agent.advertise_capability(Capability::new("compute", "gpu"), Duration::from_secs(30));

// Resolve — snapshot of all currently-advertising nodes matching the filter.
let matches: Vec<(NodeId, Capability)> = agent.resolve(&CapFilter::new("compute", "gpu"));

// Watch — push-based, debounced to 50 ms idle window before firing.
let mut rx = agent.watch_capabilities(CapFilter::new("compute", "gpu"));
```

### Requirements and Demand Pressure (Phases 4, 9)

```rust
// Declare — writes req/{node_id}/{ns}/{name}; visible to orchestrators on any node.
let _handle = agent.declare_requirement(CapFilter::new("compute", "gpu"), Duration::from_secs(30));

// Watch requirement status — fires when provider set changes relative to declared need.
let mut rx = agent.watch_requirement(CapFilter::new("compute", "gpu"));

// Demand snapshot — pressure = demanding / max(providers, 1). Library never auto-responds.
let status: DemandStatus = agent.demand(&CapFilter::new("compute", "gpu"));
println!("pressure: {:.2}", status.demand_pressure);  // > 1.0 = supply gap

// Push-based demand — debounced, fires on req/, cap/, or gcap/ changes.
let mut rx = agent.watch_demand(CapFilter::new("compute", "gpu"));
```

### Emergent Capability Groups (Phases 3g, 3h)

Nodes that share a capability self-assemble into a named group. The library projects their
collective capability under `gcap/{group}/{ns}/{name}/{contributor}` and handles group-level
requirement wiring. One consolidated `run_group_membership_task` per group (not per member)
keeps the task count O(active groups).

```rust
agent.define_capability_group(
    "gpu-pool",
    CapabilityGroupDef {
        filter:   CapFilter::new("compute", "gpu"),
        provides: vec![Capability::new("compute", "gpu")],
        requires: vec![],
    },
    Duration::from_secs(60),
);
```

### Inter-Group Wiring (Phase 4)

Wiring connects a consumer's declared requirement to provider groups without the consumer needing
to enumerate group members or know their node IDs.

```rust
// Resolve wiring — WiringStatus::Wired{providers} or WiringStatus::Unwired{filter}
let status = agent.resolve_wiring(&CapFilter::new("compute", "gpu"));

// Watch wiring — push-based, fires when provider set changes
let mut rx = agent.watch_wiring(CapFilter::new("compute", "gpu"));

// Signal via wiring — dispatches to all matching providers
let outcome = agent.signal_wired_via(&CapFilter::new("compute", "gpu"), "render-job", payload).await;
```

### Locality-Aware Resolution (Phase 6)

```rust
// Set once before agent.start():
config.locality_path = vec!["az1".to_string(), "rack2".to_string(), "host3".to_string()];

// Returns (NodeId, Capability, depth) sorted by shared-prefix depth descending.
let candidates = agent.resolve_with_locality(
    &CapFilter::new("render", "job"),
    LocalityPreference::PreferShared(0),
);

// Locality-aware wiring dispatch
agent.signal_wired_via_locality(
    &CapFilter::new("render", "job"),
    LocalityPreference::PreferShared(0),
    "render-job",
    payload,
).await;
```

### Watcher Scalability

The capability watchers have three scalability properties built in:

- **Predicate-narrowed subscriptions** (`subscribe_prefix_with_predicate`): each watcher registers
  a closure that the KV store evaluates before waking it. A `watch_capabilities("compute", "gpu")`
  watcher only wakes when a `cap/*/compute/gpu` entry changes — not on every `cap/` write.
- **50 ms debounce window**: all five watcher kinds (capabilities, requirement, wiring, demand,
  group definitions) drain burst writes for 50 ms before recomputing a snapshot, collapsing O(N)
  burst fires into one reconcile.
- **One task per emergent group**: `run_group_membership_task` owns all gcap projection reasserts
  and requirement opacity watchers for a group, so task count scales with active groups, not with
  members × capabilities.

---

## Layer 3 — Service Patterns (Phase 3)

Layer 3 delivers the transport primitives that unblock Layer 4. Three deliverables are **required
before MCP or language bridges can ship**; the Actor/Event and scatter-gather work follows.

### Required for Layer 4 (blocking)

**1. Embedded HTTP server** — both the MCP bridge and the language gateway need an HTTP surface
inside the agent binary. This is the foundation for bulk payloads, SSE streaming, and the Python
sidecar gateway. No external web framework dependency; a minimal `tokio`-based server sufficient
for the bridge use cases.

**2. SSE / streaming** — MCP's primary value for LLM workloads is streaming token responses.
A tool call that returns a token stream via SSE is the default pattern for any non-trivial AI
integration. Without this, MCP is limited to short synchronous tool calls.

**3. Formalised RPC primitive** — `signal_once` + nonce correlation already works as a pattern.
Layer 3 codifies it as a named primitive (`rpc_call` / `rpc_respond`) so the Python SDK and MCP
bridge don't each re-derive it:

```rust
// Layer 3 formalises the pattern that's already implicit in signal_once + nonce
let response = agent.rpc_call(
    target_node_id,
    "mcp.invoke",
    json_request_bytes,
    Duration::from_secs(30),
).await?;  // → Bytes or RpcError::Timeout / RpcError::NodeGone
```

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

### Actor / Event and Scatter-Gather (follow-on)

Actor/Event mailboxes and scatter-gather (parallel sub-task dispatch + collection) are useful
but not blocking for Layer 4. They land after the MCP bridge ships.

---

## Layer 4 — AI Integration (Phase 4)

Layer 4 delivers two concrete systems: an **MCP bridge** and **language bridges** (Python first,
TypeScript next). Graph-based orchestration frameworks (LangGraph, AutoGen, CrewAI) are
explicitly out of scope — their centralized execution model conflicts with Mycelium's epidemic
routing and provides no integration benefit over using MCP at the boundary.

### MCP Bridge

MCP is request-response: tool discovery → tool invocation → structured result. The bridge has
two distinct roles.

**Mycelium as MCP server** — capability providers expose themselves as MCP tools. Any external
MCP client (Claude, a Python agent, a CLI tool) can discover and call Mycelium-hosted tools:

```
Tool registration:  advertise_capability() + set("tools/{name}/{node_id}", json_schema_bytes)
Tool discovery:     scan_prefix("tools/") → tool name + schema + NodeId per provider
Tool invocation:    rpc_call(node_id, "mcp.invoke", json_rpc_request, timeout)
Tool result:        rpc_respond("mcp.result", json_rpc_response)
Streaming result:   SSE over embedded HTTP (requires Layer 3 streaming)
```

**Mycelium as MCP client** — agents call external MCP tool servers (Claude, 3rd-party providers).
The bridge holds the outbound connection; inbound results re-enter the mesh as capability
interactions. The agent sees no difference between a local Mycelium tool and a remote MCP server.

**Credentials architecture** — API keys and OAuth tokens are held by the bridge layer, not the
substrate. Mycelium carries opaque `Bytes`; credentials are a bridge-level concern injected per
call context. Keys must not appear in signal payloads.

**KV namespace conventions for MCP:**

```
tools/{tool_name}/{node_id}     → JSON Schema bytes (tool advertisement)
tools/{tool_name}/{node_id}/loc → locality path (for locality-aware tool routing)
conv/{conv_id}/context          → multi-turn conversation context (per-conversation namespace)
```

### Language Bridges

Python is the priority. TypeScript follows (LLM tooling ecosystem assumption).
LangGraph is not a target — see rationale below.

**Architecture: HTTP gateway sidecar** (not PyO3 FFI). LLM inference runs at hundreds of
milliseconds; a loopback HTTP call adds ~1 ms, which is invisible. PyO3 couples the Python
version to the Rust build and complicates streaming. The gateway pattern is simpler, not
version-coupled, and supports SSE natively.

```
┌─────────────────────────────────────────────────────┐
│  Python / TypeScript agent process                  │
│  (DSPy program, custom agent, AutoGen agent, etc.)  │
│                                                     │
│  mycelium.advertise_capability("compute", "gpu")    │
│  mycelium.on_signal("render-job", handler)          │
│  mycelium.emit("result", scope, payload)            │
└────────────────┬────────────────────────────────────┘
                 │  HTTP + SSE (loopback)
┌────────────────▼────────────────────────────────────┐
│  Mycelium Rust node (embedded HTTP gateway)         │
│  translates HTTP calls ↔ gossip signals             │
└─────────────────────────────────────────────────────┘
```

**Minimum Python SDK surface:**

```python
agent = MyceliumAgent(host="127.0.0.1", port=7946)

agent.advertise_capability("compute", "gpu", interval_secs=30)
agent.declare_requirement("compute", "gpu", interval_secs=30)

@agent.on_signal("render-job")
async def handle(payload: bytes) -> None: ...

agent.emit("render-job", scope="group:gpu-pool", payload=b"...")
status = agent.demand("compute", "gpu")  # → DemandStatus
```

Everything else (`watch_capabilities`, `resolve_with_locality`, wiring) comes in a later pass.

**Why not LangGraph** — LangGraph assumes a central scheduler that directs graph execution:
"call agent B now, wait for result, then call agent C." This is orthogonal to Mycelium's
epidemic model. Integrating the two means one of them is doing the coordination and the other
is just a message bus. Using Mycelium under LangGraph gives you none of the adaptive routing,
demand pressure, or locality-aware dispatch benefits. The clean boundary is MCP: LangGraph
calls into the Mycelium cluster via MCP tool calls; Mycelium handles capability routing,
load balancing, and fault tolerance within the provider tier.

### Supervision

Layer 4 supervision uses Layer 2's `watch()` to monitor AI agent liveness — no separate
monitoring infrastructure. A supervisor watches `contract.available` heartbeats; on stale,
triggers respawn, failover, or escalation. The Python bridge exposes this as `on_stale(kind, threshold, callback)`.

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

## Opt-In Consistency and Ordering Overlay (Planned)

The epidemic substrate is always available and always fast. These APIs escalate to stronger
guarantees only for the specific operation that demands them — nothing in the fast path becomes
slower or more complex because they exist.

This is **CAP theorem applied selectively, not globally.** Traditional systems pick one position
and apply it uniformly. Here you choose per operation. The same cluster, the same embedded
library, with no separate infrastructure.

### Linearizable KV and Coordination — Consul / etcd parity

Built over the existing `ConsensusEngine` (`group_propose`). The gossip KV remains the fast
path; `consistent_*` operations pay consensus latency only when called.

```rust
// Consistent write — ConsensusEngine agrees before gossiping the value
agent.consistent_set("config/feature-flags", value).await?;
agent.consistent_get("config/feature-flags").await?  // reads the committed value

// Distributed lock — mutual exclusion via consensus; releases on drop
let _guard = agent.distributed_lock("migration-lock", Duration::from_secs(30)).await?;

// Leader election — thin wrapper over group_propose with a NodeId payload
let leader: NodeId = agent.elect_leader("worker-group").await?;
```

**Foundation already exists:** `ConsensusEngine`, `group_propose`, `KV-backed committed slots`.
Implementation is primarily clean API wrappers over existing machinery.

### Ordered Durable Log — Kafka parity

Append-only namespace keyed by HLC timestamp. The gossip KV handles replication and anti-entropy
sync to late joiners; the HLC provides causal ordering without a broker.

```rust
// Append — writes log/{stream}/{hlc} to gossip KV; entries never tombstoned
agent.append("events/orders", entry_bytes);

// Subscribe from a position — reactive, ordered by HLC key, fires on new entries
let mut rx: watch::Receiver<Vec<(Hlc, Bytes)>> = agent.subscribe_log("events/orders", since_hlc);

// Range scan — replay a window or from a checkpoint
let entries = agent.scan_log("events/orders", from_hlc, to_hlc);

// Compaction — tombstones entries older than a watermark
agent.compact_log("events/orders", before_hlc);
```

**Consumer groups** — each consumer tracks its position as a KV entry:
`consumer/{group}/{stream}/offset` = last-processed HLC. `subscribe_log_group` delivers each
entry to exactly one member; `distributed_lock` or `elect_leader` coordinates claim when needed.

**Foundation already exists:** HLC (hybrid logical clock), gossip KV, prefix scan, tombstone
mechanism. This is new API surface over existing primitives, not new infrastructure.

### Reliable Delivery — Akka parity

ACK retry over `rpc_call` (Layer 3). The HLC and signal reorder buffer (already designed)
handle causal ordering and dedup on the receiver side.

```rust
// Fire-and-forget with ACK — retries until acknowledged or timeout
let result = agent.emit_reliable(
    "actor.msg",
    SignalScope::Individual(target),
    payload,
    Duration::from_secs(5),
).await?;  // → AckResult::Acknowledged | AckResult::Timeout
```

**Foundation:** `rpc_call` (Layer 3), signal reorder buffer (planned).

### Cluster Sharding — Akka Cluster Sharding parity

Deterministic placement via consistent hash ring over the sorted NodeId space, combined with
`resolve_with_locality` for topology-awareness. No central shard coordinator.

```rust
// Deterministic owner for a shard key — consistent across all nodes seeing the same provider set
let owner: NodeId = agent.shard_for("user-12345", &CapFilter::new("actor", "user"))?;

// Route directly to the consistent-hash owner matching the capability filter
agent.emit_sharded("actor.msg", "user-12345", &CapFilter::new("actor", "user"), payload).await;
```

**Foundation:** `resolve_with_locality`, `NodeId` ordering, capability subsystem.

### What Each Competitor Advantage Maps To

| Competitor | Their advantage | Mycelium equivalent | Foundation |
|---|---|---|---|
| Consul / etcd | Linearizable KV | `consistent_set` / `consistent_get` | ConsensusEngine ✓ |
| Consul | Distributed locks | `distributed_lock` | ConsensusEngine ✓ |
| Consul | Leader election | `elect_leader` | `group_propose` ✓ |
| Kafka | Ordered log | `append` / `subscribe_log` / `scan_log` | HLC + gossip KV ✓ |
| Kafka | Consumer groups | `subscribe_log_group` + offset KV | `consistent_set` + capability groups ✓ |
| Kafka | Log compaction | `compact_log` | tombstone mechanism ✓ |
| Akka | Reliable delivery | `emit_reliable` | `rpc_call` (Layer 3) |
| Akka | Cluster sharding | `shard_for` / `emit_sharded` | `resolve_with_locality` + NodeId ✓ |

The key difference: these are **additive**. A node using only epidemic gossip pays zero overhead
for the existence of these APIs. The consistency and ordering mechanisms are escalation paths
you call when the operation demands it — not the substrate everything else is built on top of.

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
| Layer 2 | watch, quorum, quorum_persistent, suppress/unsuppress, manage_opacity | **Complete** |
| Layer 2 | advertise_persistent, epidemic_extra_peers, listener auto-restart | **Complete** |
| Layer III | ConsensusEngine, epidemic two-phase voting, group_propose | **Complete** |
| Capability | advertise_capability, resolve, watch_capabilities | **Complete** |
| Capability | declare_requirement, watch_requirement, RequirementStatus | **Complete** |
| Capability | define_capability_group, gcap/ projections, emergent groups | **Complete** |
| Capability | resolve_wiring, watch_wiring, signal_wired_via, inter-group wiring | **Complete** |
| Capability | resolve_with_locality, signal_wired_via_locality, locality paths | **Complete** |
| Capability | demand, watch_demand, DemandStatus (demand pressure surface) | **Complete** |
| Capability | Predicate-narrowed watchers, 50 ms debounce, one-task-per-group | **Complete** |
| Layer 3 | Embedded HTTP server, SSE streaming, `rpc_call`/`rpc_respond` primitive | **Blocking for Layer 4** |
| Layer 3 | Bulk payload / `invoke.bulk` ticket, Actor/Event mailboxes, scatter-gather | Planned |
| Layer 4 | MCP bridge: server role (tools/ KV + rpc_call dispatch) | Planned |
| Layer 4 | MCP bridge: client role (outbound to external MCP servers) | Planned |
| Layer 4 | Python language bridge: HTTP gateway + `mycelium` SDK | Planned |
| Layer 4 | TypeScript language bridge | Planned |
| Layer 5 | Metrics, Prometheus exporter | Planned |
| Consistency overlay | `consistent_set`, `consistent_get`, `distributed_lock`, `elect_leader` | Planned |
| Ordering overlay | `append`, `subscribe_log`, `scan_log`, `compact_log` (ordered log) | Planned |
| Ordering overlay | `subscribe_log_group` + consumer group offset tracking | Planned |
| Reliable delivery | `emit_reliable` + ACK retry (requires Layer 3 `rpc_call`) | Planned |
| Cluster sharding | `shard_for`, `emit_sharded` (consistent hash + locality) | Planned |

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

### Layer 1 — `scan_prefix` (prefix-indexed fast path)

| Store size | Matching entries | Median |
|---|---|---|
| 100 | 10 | 332 ns |
| 1,000 | 10 | 2.7 µs |
| 10,000 | 10 | 41 µs |
| 10,000 | 100 | 49 µs |
| 100,000 | 10 | 622 µs |

`scan_prefix` uses a `prefix_index` for an O(|segment_keys|) fast path when the first path
segment is a known prefix (e.g. `"load/"`, `"grp/"`, `"svc/"`, `"sys/"`). Unknown prefixes
fall back to an O(store_size) full scan. At typical pheromone-trail sizes (100–1,000 entries
per segment) the cost is negligible relative to network latency.

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

**6. Consistency and ordering as opt-in layers, not foundations.** Every traditional distributed
framework picks one position on the CAP triangle and applies it uniformly — Consul/etcd pay Raft
latency on every read/write; Kafka pays broker coordination on every publish; Akka pays ack
overhead on every message. Here, the epidemic substrate is the foundation — always available,
always fast — and you escalate to linearizability (`consistent_set`), ordered logging (`append` /
`subscribe_log`), reliable delivery (`emit_reliable`), or cluster sharding (`shard_for`) only for
the specific operations that require it. Most agent-to-agent coordination doesn't need
linearizability; it needs fast and available. The rare operations that do need it call
`consistent_set` and pay consensus latency only for that call. Same cluster, same binary, no
separate infrastructure for each tier.

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

