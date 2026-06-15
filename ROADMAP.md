# Mycelium — Engineering Roadmap

> **Status:** Layer 1 complete. Layer 2 complete. Consensus complete. Capability & Discovery subsystem complete. Agent state machine (Layer V) complete. MCP bridge (server + client) complete. Config-driven capability probing complete. KV persistence (WAL + snapshot, all sync modes) complete. Layer 3 Service Patterns complete (HTTP server, SSE, rpc_call/rpc_respond, invoke.bulk, Actor/Event mailboxes, scatter-gather). Multi-machine integration tests (Docker Compose, 12 unattended scenarios) complete. **mTLS peer connections + Ed25519 node identity + consensus payload signing complete** (`tls` feature). Python language bridge (`mycelium-py`) complete. **SkillRunner** (`.skill.toml` capability-as-skill, OpenAI-compatible LLM driver, HLC audit trail + OTEL) complete. **Opt-In Consistency & Ordering Overlay complete** (`consistent_set/get`, `distributed_lock`, `elect_leader`, `append`/`scan_log`/`compact_log`/`subscribe_log`/`subscribe_log_group`, `emit_reliable` — all exposed via HTTP gateway and Python SDK). **Layer 5 Observability complete** (`metrics` feature — Prometheus scrape endpoint at `/metrics`, 10 counters/gauge/histogram, Grafana dashboard at `dashboards/mycelium-grafana.json`). **TypeScript language bridge complete** (`mycelium-ts` — 28 methods, SSE streaming, all overlay endpoints, mirrors Python SDK). **Cluster Sharding complete** (`shard_for`/`emit_sharded` + HTTP gateway + Python & TS SDKs). **KV Write Signing complete** (Ed25519 `WireMessage::SignedData`, wire v10). **A2A Adapter complete** (`a2a` feature — `/.well-known/agent.json`, `/a2a` JSON-RPC, Python & TS `A2aClient`). **Cross-Group Consensus complete** (`cross_group_propose` + `GroupQuorum` — multi-voting-bloc proposals with independent per-group quorum fractions, `SignalScope::Groups` variant, HTTP gateway + Python & TS SDKs). **Prompt Skills complete** (`llm` feature — `PromptTemplate` stored in KV, `register_prompt_skill`/`call_prompt_skill` on `GossipAgent`, `OpenAiBackend`/`EchoBackend`, HTTP gateway `/gateway/prompts` + `/gateway/llm/call` + `/gateway/llm/stream`, Python `PromptSkillClient`, TS `PromptSkillClient` — 241 tests). **Signal Reorder Buffer complete** (`emit_ordered()`, `hlc_seq` wire field, wire v11, per-`(sender,kind)` min-heap, `GossipConfig::signal_ordered_delivery`). **Watcher C2 complete** (consolidated requirement opacity watcher — one task, one `cap/` subscription for all declared requirements). **Semantic coordination complete** (capability schema versioning — `with_schema_id`/`CapFilter::with_schema`; gossip-propagated skill payload schemas — `with_input_schema`/`with_output_schema`; signal sender authorization — `signal_rx_from`; FIPA-ACL speech act taxonomy in crate doc). **Schema registry complete** (`publish_schema`, `force_publish_schema`, `get_schema`, `list_schemas`, `seed_schemas_from_dir` — `schemas/` KV namespace, conflict detection, JSON validation). **Research paper in progress** — *"The Coordinator Trap: Why Mediated Multi-Agent Architectures Cannot Scale and a Substrate-Based Alternative"* — target AAMAS 2027; first draft + structural revision complete (2026-05-28); §8 evaluation benchmarks pending empirical runs. **v1.x Production Readiness Gap CLOSED (2026-06-14, `v1.2.0`)** — WS1 RBAC, WS2 tamper-evident audit, WS3 crown-jewel (data-at-rest + egress + threat model), WS4 generic-OIDC SSO, WS5 hot cert rotation all shipped behind the `compliance` feature to an implemented-tested-documented bar. Default 318 / tls 323 / compliance 366 lib tests, 13/13 integration scenarios, 100-node scale + 21-node resilience suites green.
> **Last updated:** 2026-06-14

---

## The Vision

A substrate for **robust adaptive AI systems** — a swarm of agents that discovers each other's
capabilities through a shared medium, signals intent through receptors that filter by scope, and
evolves its topology in response to activity patterns. No coordinator, no central registry, no
single point of failure.

> **Scope of "no coordinator":** The gossip KV layer and signal mesh are coordinator-free.
> The opt-in consistency overlay (`consistent_set`, `distributed_lock`, `elect_leader`) uses
> epidemic Paxos and requires a live majority — those specific operations have a proposer and
> will stall under partition. `bootstrap_peers` is a soft coordinator for initial cluster
> discovery; keep 2–3 long-lived seed nodes for reliable join behaviour.

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

## The Structural Inversion: Consistency as a Service, Not a Foundation

This is Mycelium's defining architectural decision, and it is the reverse of nearly every
production distributed system built in the last two decades.

**How Raft-based systems work.** Consistency is the foundation. Every operation — read, write,
membership change — flows through the consensus log. Consul, etcd, CockroachDB, and TiKV share
this model. The benefit is strong guarantees everywhere. The cost is that *everything* pays
consensus latency, including the 95% of operations that don't need it.

**How Kafka works.** The broker log is the foundation. Every event pays broker round-trip and
partition coordination overhead, including ephemeral signals that are immediately processed and
discarded.

**How Akka works.** The actor model is the foundation. Every message flows through a mailbox and
a supervision tree, including fire-and-forget notifications between co-located agents.

**How Mycelium works.** The epidemic gossip substrate is the foundation — always available,
sub-millisecond, zero coordination overhead. Consistency, ordering, and reliable delivery are
*services* built on top of that substrate, invoked only by the operations that need them.

The `ConsensusEngine` itself is proof of this: it is built *over* the gossip KV, not the other
way around. An agent that never calls `consistent_set` pays zero overhead for its existence.

The consequence is **per-operation guarantee selection**:

| Operation | Guarantee | Cost |
|---|---|---|
| `emit(signal)` | Best-effort, epidemic | sub-ms, zero coordination |
| `append("events/orders", bytes)` | Causally ordered, durable | HLC stamp only — no broker |
| `consistent_set("config/x", val)` | Ballot-serialized (consensus-durable) | consensus round-trip for *this call only* |
| `distributed_lock("migration")` | Mutual exclusion | consensus for *this call only* |

The same cluster. The same embedded binary. No separate infrastructure for each tier.

Consul, Kafka, and Akka each pick one position on the consistency/availability tradeoff and
apply it *uniformly across your whole system*. Mycelium picks *per operation*.

---

## Core Principles — Compliance Gate for Future Work

The sections above (*Design Philosophy*, *The Structural Inversion*) describe how the substrate
works. This section makes those descriptions **normative**: every v2.0 milestone below — and any
future engineering work, in this repo or in a companion crate — must comply with the invariants
here. They are not stylistic preferences. Each is load-bearing for the architecture's central
claim, and violating any one reintroduces exactly the cost the design exists to remove.

1. **No coordinator. The substrate provides mechanism, never agency.** No component decides, on a
   node's behalf, what it should run, hold, become, or admit. Placement, provisioning, supervision,
   and admission are *emergent* — they arise from independent local decisions (a node evaluating its
   own capabilities, load, and locality), never from a central authority that lacks that local
   knowledge. This is the spine of Paper 1's argument; a substrate that holds that agency *is* the
   coordinator trap, by definition.
   **Litmus test:** if a proposed feature requires the substrate itself to act for the fleet —
   watch-and-decide, pull-and-run, assign-a-role — it is non-compliant *by construction*. Reframe it
   as an application-layer agent on the public API (the `SkillRunner` / `ClusterTuner` /
   `mycelium-wasm-host` shape), or do not build it.

2. **Consistency as a service, not a foundation.** The epidemic substrate is always the fast path.
   Stronger guarantees (consensus, ordering, reliable delivery, exactly-once-effect) are opt-in,
   per-operation, and built *over* the substrate — never folded into it. A node that never invokes a
   guarantee pays zero overhead for its existence; nothing in the fast path becomes slower or more
   complex because a stronger tier exists.

3. **Layer discipline.** Higher layers own KV prefixes and write through the documented path; a
   lower layer is never taught a higher layer's law (the consensus *commit-conflict tripwire* is the
   reference — detection in Layer III, **not** a `consensus/`-prefix write guard in Layer I's
   `apply_and_notify`). Inverting the layer dependency to make a foundation enforce a higher-layer
   policy is prohibited.

4. **Detection, not prevention.** Namespace ownership is promise-strength: the substrate *detects*
   violations and routes around them (tripwires, evaporation, fencing leases) rather than enforcing
   them — enforcement implies an authority, and an authority is a coordinator. Prefer a fencing token
   to a forcible kill, a tripwire to a write guard.

5. **Emergent, not imposed; threshold activation over agreement.** A node acts when it has
   sufficient *local* information, not when all nodes agree. Synchronous global barriers are an
   anti-pattern; agreement is paid for per-operation, only where correctness genuinely requires it.

6. **Composability on the public API.** New capabilities ship as companion crates or app-layer
   agents built on the public surface (the `mycelium-tuple-space` composability proof), not as core
   bloat. This keeps embeds a single dependency and continuously re-validates that the public API is
   sufficient.

A milestone that cannot be expressed within these invariants is not "hard" — it is *out of scope*.
The correct response is to redesign it until it complies, or to leave it unbuilt.

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
│  Opt-In Consistency & Ordering Overlay               [COMPLETE]    │  ← cross-cutting
│  consistent_set · consistent_get · distributed_lock · elect_leader │
│  append · subscribe_log · scan_log · compact_log (ordered log)     │
│  subscribe_log_group (consumer groups) · emit_reliable             │
│  HTTP gateway + Python SDK (LogEntry, LockGuard dataclasses)       │
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
│  Consensus                                           [COMPLETE]    │
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

**Design principle — consistency as a service, not a foundation.** See *The Structural Inversion*
above. The Opt-In Overlay row in the stack is cross-cutting precisely because it is not a layer
imposed on everything beneath it — it is a set of higher-guarantee entry points that any agent
may call without affecting agents that don't.

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
  (Raised again to `1024` on 2026-06-11 after both scale tests recorded burst drops.)
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
agent.kv().set(key, value) -> bool           // local always updated; false = channel full
agent.kv().set_async(key, value).await -> bool
agent.kv().get(key) -> Option<Bytes>
agent.kv().delete(key) -> bool               // gossips a tombstone
agent.kv().delete_async(key).await -> bool
agent.kv().keys() -> Vec<Arc<str>>
agent.kv().scan_prefix(prefix) -> Vec<(Arc<str>, Bytes)>

// Reactive
agent.kv().subscribe(key) -> watch::Receiver<Option<Bytes>>

// Introspection
agent.system_stats() -> SystemStats     // includes dropped_frames
```

**Key config fields for Layer 1:**

| Field | Default | Purpose |
|---|---|---|
| `max_forwarding_peers` | `i64::MAX` | Cap gossip targets; set to `bootstrap_peers.len()` for fixed-topology meshes |
| `writer_channel_depth` | `1024` | Per-peer outbound channel depth (ring buffer). **Correctness threshold** — covers `N × fan_out` up to N = 256 at fan-out 4 |
| `health_check_interval_secs` | `10` | Peer liveness ping interval |
| `default_ttl` | `5` | Hops before a message stops propagating |
| `gossip_shards` | `min(CPU, 16)` | Gossip worker tasks; set to `1` for demo/debug to cut task count |
| `epidemic_extra_peers` | `3` | Random non-member peers added to Group-scoped signal fan-out. Raise to 5–7 for clusters > 1 000 nodes |
| `group_aware_forwarding` | `true` | Route Group signals to members + `epidemic_extra_peers`. `false` = broadcast all |
| `max_peers` | `i64::MAX` | Cap the peer table; set to `bootstrap_peers.len()` for grid/ring topologies |
| `writer_idle_timeout_secs` | `0` | Close idle peer TCP connections after N seconds (`0` = no timeout) |
| `signal_window_secs` | `600` | Sender-log and `quorum_written` retention window |
| `max_store_entries` | `0` | Hard cap on live KV entries (`0` = unlimited) |

**Future Layer 1 improvements (not blocking):**
Activity-weighted forwarding — prefer recently-active peers over randomly-discovered ones.
Currently `max_forwarding_peers` caps the target count; a follow-on pass would weight selection
by last-received-from timestamp so the topology self-organises around actual traffic patterns.

A second v2 consideration: hybrid TCP/UDP transport (SWIM-style) — UDP for gossip pings and
capability heartbeats (loss-tolerant, no connection state), TCP reserved for anti-entropy data
transfer (reliability required). This eliminates the iptables FORWARD chain saturation problem
structurally rather than via the `GOSSIP_MAX_ACTIVE_CONNECTIONS` connection cap introduced in
v1. See *v2.0 Milestones* below for the full design note.

---

## Layer 1 — KV Persistence (Complete)

Per-node append-only WAL plus periodic snapshot/compaction. Nodes survive process restarts and
full-cluster cold restarts without loss of hard state. Anti-entropy sync remains the replication
mechanism — persistence is purely local recovery.

### Enabling persistence

```rust
use mycelium::{GossipConfig, PersistenceConfig, SyncMode};
use std::path::PathBuf;

let config = GossipConfig {
    persistence: Some(PersistenceConfig {
        base_path:               PathBuf::from("/var/lib/mycelium"),
        sync_mode:               SyncMode::Async,   // default; Flush for hard durability
        snapshot_wal_threshold:  10_000,             // default
        snapshot_interval_secs:  300,                // default
    }),
    ..GossipConfig::default()
};
```

`persistence: None` (the default) preserves the previous fully-in-memory behaviour.

### Directory layout

```
{base_path}/{node_id}/kv/
    wal.bin         append-only WAL  ([u32-LE-length][bincode SyncEntry])
    snapshot.bin    last compacted full-store snapshot
    snapshot.tmp    in-progress write; atomically renamed on completion
```

The `node_id` subdirectory gives each node its own namespace when multiple agents run on the
same machine. The directory is created automatically on first start.

### Sync modes

| Mode | Durability | Cost | When to use |
|---|---|---|---|
| `Flush` | Survives hard crash + power loss | ~0.1–2 ms extra latency per `set_async` write | Production, consensus-heavy workloads |
| `Async` (default) | Survives process crash; may lose last few writes on hard crash | Negligible | Most production deployments |
| `Os` | No explicit syncs — OS decides when to flush | Zero overhead | Development / testing only |

### Durability contract

| Call | Durability |
|---|---|
| `set(key, value)` | Fire-and-forget WAL (best-effort; crash during OS flush may lose it) |
| `delete(key)` | Same as `set` |
| `set_async(key, value).await` | Awaits fsync in `Flush` mode; fire-and-forget in `Async`/`Os` |
| `delete_async(key).await` | Same as `set_async` |
| Consensus committed slot | Always fsynced (`append_sync`) regardless of `sync_mode` |

### Startup replay

On `agent.start()`, before the gossip loop begins:

1. Load `snapshot.bin` if present — applies all entries via `apply_and_notify`
2. Replay `wal.bin` entries with `timestamp > snapshot_hlc`
3. Observe max replayed HLC — ensures post-restart writes strictly dominate persisted state
4. Trigger an immediate post-replay snapshot — bounds the replay window on next restart
5. Spawn WAL writer task; store handle for all subsequent writes

### Snapshot opacity

During the snapshot window the node writes `sys/load/{node_id}/persistence` with
`is_opaque = true` so other nodes route new work elsewhere. The key is tombstoned when the
snapshot completes. This composes automatically with all other opacity causes via the existing
`is_self_opaque()` prefix scan — no new mechanism is required.

Snapshot triggers:
- WAL threshold reached (`snapshot_wal_threshold` entries)
- Periodic timer (`snapshot_interval_secs`; deferred 30 s if already opaque for another reason)
- Graceful shutdown

### What is persisted vs regenerated

| State | Persisted | Why |
|---|---|---|
| Application KV writes (`set`, `set_async`) | Yes | Hard state — must survive restart |
| Received gossip (anti-entropy, Data frames) | Yes | Hard state — needed before anti-entropy round completes |
| Quorum evidence (`sys/quorum/`) | Yes | Restart-safe `quorum_persistent` depends on it |
| Consensus committed slots (`consensus/committed/`) | Yes — always fsynced | Safety: must not re-propose committed slots |
| Opacity keys (`sys/load/*/…`) | No | Regenerated on restart (opacity governor re-advertises) |
| Capability advertisements (`cap/`, `req/`, `gcap/`) | No | Re-advertised by `advertise_capability` handles on restart |
| Group membership (`grp/`) | No | Re-joined via `join_group` and emergent-group watcher |
| Consensus ballots (`consensus/ballot/`) | No | In-flight ballots abandoned on restart; peers time out cleanly |

---

## Layer 2 — Signal / Boundary Mesh (Complete)

Layer 2 adds ephemeral events and local receptors on top of the Layer 1 gossip transport. See
README.md for the full API reference, observability guide, and opacity/inhibition scenarios.

The complete stable API is documented in the [Complete Layer 2 API](#complete-layer-2-api) section below.

### Complete Layer 2 API

```rust
// ── Group membership ─────────────────────────────────────────────────────
agent.mesh().join_group(name)
agent.mesh().leave_group(name)
agent.groups() -> Vec<Arc<str>>            // current memberships

// ── Emit / receive ───────────────────────────────────────────────────────
agent.mesh().emit(kind, scope, payload)           -> bool   // false = shard full
agent.mesh().emit_async(kind, scope, payload).await -> bool // false = shard dead
agent.mesh().signal_rx(kind)                      -> mpsc::Receiver<Signal>
agent.mesh().signal_rx_with_capacity(kind, cap)   -> mpsc::Receiver<Signal>

// ── One-shot request/response ────────────────────────────────────────────
agent.mesh().signal_once(kind, timeout, predicate).await -> Option<Signal>

// ── Periodic heartbeat ───────────────────────────────────────────────────
agent.mesh().advertise(kind, scope, interval, payload_fn) -> AdvertiseHandle
// Like advertise, but also writes payload to Layer I (key: svc/{kind}/{node_id}).
// Tombstoned automatically on drop/shutdown. Lets late joiners discover via scan_prefix.
agent.mesh().advertise_persistent(kind, scope, interval, payload_fn) -> AdvertiseHandle

// ── Fault detection ───────────────────────────────────────────────────────
agent.mesh().last_signal(kind) -> Option<Instant>       // when was kind last delivered here?
agent.mesh().watch(kind, threshold, on_stale) -> WatchHandle  // calls on_stale() when silent > threshold

// ── Threshold activation ─────────────────────────────────────────────────
agent.mesh().quorum(kind, min_senders, window) -> bool  // ≥ min_senders distinct nodes in window?
// quorum_persistent reads from sys/quorum/ in Layer I — survives process restarts.
agent.kv().quorum_persistent(kind, window) -> usize   // count of distinct senders in window

// ── Inhibition / refractory period ──────────────────────────────────────
agent.mesh().suppress(kind, duration)                   // block kind delivery for duration
agent.mesh().unsuppress(kind)                           // lift early
agent.mesh().is_suppressed(kind) -> bool                // diagnostic

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
| Was a kind heard recently? | `mesh().last_signal(kind)` | Last delivery timestamp |
| Has a kind gone silent? | `mesh().watch(kind, threshold, cb)` | Calls `cb` when silent > threshold |
| Have K distinct nodes checked in? | `mesh().quorum(kind, K, window)` | Consensus-adjacent readiness |
| Have K nodes checked in (restart-safe)? | `kv().quorum_persistent(kind, window)` | Reads `sys/quorum/` from Layer I |
| Is this node refusing a kind? | `mesh().is_suppressed(kind)` | Active inhibition in effect |
| How loaded is this node's intake? | `opacity(kind)` | Fill ratio 0.0–1.0 |
| Are peers notified of overload? | `manage_opacity(...)` | Emits `boundary.opaque` to peers |
| What groups is this node in? | `groups()` | Current boundary memberships |
| How many workers are alive? | `kv().scan_prefix("load/")` | Pheromone trail count (Layer 1) |
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
let fresh = supervisor.mesh().last_signal(signal_kind::CONTRACT_AVAILABLE)
    .map(|t| t.elapsed() < Duration::from_secs(30))
    .unwrap_or(false);

// 3. Check if enough workers are present (quorum)
let pool_ready = supervisor.mesh().quorum(
    signal_kind::CONTRACT_AVAILABLE, 2, Duration::from_secs(30)
);

// 4. Read the authoritative state from the pheromone trails
let live_trails = supervisor.kv().scan_prefix("load/nlp/")
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
let _handle = agent.mesh().advertise(
    signal_kind::CONTRACT_AVAILABLE,
    SignalScope::Group("nlp"),
    Duration::from_secs(5),
    || encode(WorkerState { queue_depth, accepted_kinds: &["sentiment"] }),
);

// Invoker side — register BEFORE emitting so no reply is missed
let nonce = fastrand::u64(..);
let reply_fut = agent.mesh().signal_once(
    signal_kind::INVOKE_RESULT,
    Duration::from_secs(5),
    move |s| s.nonce == nonce,
);
agent.mesh().emit_async(signal_kind::INVOKE, SignalScope::Group("nlp"),
    encode(InvokeRequest { nonce, payload: input })).await;

match reply_fut.await {
    Some(sig) => handle_result(sig),
    None => {
        if agent.mesh().last_signal(signal_kind::CONTRACT_AVAILABLE)
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
        agent2.kv().set(load_key.clone(), encode(&state));  // pheromone trail — persistent
        encode(&state)                                  // signal payload — fast delivery
    },
);
// On graceful shutdown: agent.kv().delete(&load_key) — explicit evaporation
```

Routing decisions read the store directly — no signal handler, no local cache:

```rust
let load_picture = agent.kv().scan_prefix("load/")
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

Three browser-visual examples demonstrated the subsystem end-to-end. They were retired
when the standalone demos were unified under the manifest preset gallery (commit
`dd03725`) and are retrievable from git history:
- **`capability_market.rs`** (port 8097) — four capability types, providers and
  requirers, demand-pressure bars, live toggle
- **`emergent_pool.rs`** (port 8098) — 20-node worker pool assembling via
  `define_capability_group`, consumers dispatching via `signal_wired_via`
- **`locality_wiring.rs`** (port 8099) — 12 nodes across two AZs, concentric rings
  showing locality depth, resolver shifting in real time

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
- **C2 — consolidated requirement opacity watcher**: `declare_requirement` previously spawned one
  `run_filter_opacity_watcher` task per call, each with its own `cap/` subscription. A single
  `run_consolidated_opacity_watcher` now handles all declared requirements — one task, one
  subscription, one scan pass per `cap/` change. The `FilterOpacityRegistry` on `TaskCtx` is the
  shared entry list; `OpacityDropGuard` on `RequirementHandle` signals cancellation on retract.

---

## Layer 3 — Service Patterns (Phase 3)

Layer 3 delivers the transport primitives that unblock Layer 4. Three deliverables are **required
before MCP or language bridges can ship**; the Actor/Event and scatter-gather work follows.

### Required for Layer 4 (blocking)

**1. Embedded HTTP server** — both the MCP bridge and the language gateway need an HTTP surface
inside the agent binary. This is the foundation for bulk payloads, SSE streaming, and the Python
sidecar gateway. No external web framework dependency; a minimal `tokio`-based server sufficient
for the bridge use cases.

> **Note from example development:** The library's HTTP server (`src/agent/http.rs`) serves only
> library-level endpoints (`/health`, `/stats`, `/mcp`, `/signals/{kind}`) and is not exposed as
> an application-level HTTP helper. The `llm_agent` example had to build its own raw TCP HTTP
> server to serve its management UI and control endpoints. The single-read body parsing in that
> server could not handle POST bodies that arrived in a separate TCP packet from the headers —
> a class of bug that a proper HTTP library prevents entirely. Exposing the embedded HTTP server
> as an application-level primitive (so examples and bridges can register their own route handlers)
> would eliminate this failure mode. Not a bug; worth tracking for the Layer 3 follow-on pass.

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

**Shipped Python SDK surface (`mycelium-py`):**

```python
from mycelium import MyceliumAgent

agent = MyceliumAgent(host="127.0.0.1", port=7946)

# Capability advertisement & discovery
handle = agent.advertise_capability("compute", "gpu", interval_secs=30,
                                    attributes={"model": "A100"},
                                    authorized_callers=["orchestrator"])
providers = agent.resolve_capability("compute", "gpu", caller_id="orchestrator")
status = agent.demand("compute", "gpu")          # → DemandStatus

# Signal mesh
agent.emit("render-job", b"payload", scope="group:gpu-pool")
async for sig in agent.on_signal("render-job"):  # → Signal
    print(sig.sender, sig.payload)

# RPC — caller side
result = agent.rpc_call(target_node_id, "echo", b"ping", timeout_secs=5)
replies = agent.scatter_gather([n1, n2], "echo", b"ping", min_ok=1)

# RPC — server side (SSE stream of incoming requests)
async for req in agent.rpc_serve("echo"):        # → RpcRequest
    agent.rpc_respond(req, req.payload)

# Gossip KV
agent.set("my/key", b"value")
val  = agent.get("my/key")                       # → bytes | None
agent.delete("my/key")
keys = agent.keys(prefix="my/")                  # → list[str]
data = agent.scan_prefix("my/")                  # → dict[str, bytes]

# Actor/Event mailbox
agent.deliver_event(target_node_id, "task.result", b"payload")
async for event in agent.mailbox("task.result"): # → MailboxEvent
    print(event.sender, event.payload)
```

See [`mycelium-py/README.md`](mycelium-py/README.md) for installation and full API reference.

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

This is the application-facing primitive. *v2.0 Milestones §14* generalizes it into
declarative, coordinator-free supervision — capability-presence invariants maintained by
emergent self-election rather than a supervision tree — honoring the Layer I/II/III
boundaries end to end.

### Skills as Capabilities — SkillRunner (Complete)

The industry term "skill" maps directly onto `advertise_capability` with a richer JSON Schema
attachment in KV. There is no new primitive. A skill is a named, discoverable, invocable unit
of behavior — exactly what a capability already is.

The `skillrunner` binary is shipped. A **Skill Definition File** (`.skill.toml`) declares
everything needed to register a capability and drive an LLM execution node:

```toml
[capability]
ns          = "dev"
name        = "code-review"
description = "Reviews a PR diff and returns structured feedback"
ttl_secs    = 300

[capability.input]
type = "object"
required = ["pr_number"]
[capability.input.properties]
pr_number = { type = "integer" }
focus     = { type = "string", enum = ["security", "performance", "all"] }

[capability.output]
type = "object"
[capability.output.properties]
summary = { type = "string" }
issues  = { type = "array", items = { type = "string" } }
verdict = { type = "string", enum = ["approve", "request-changes", "comment"] }

[capability.policy]
max_concurrent     = 2
authorized_callers = ["orchestrator", "planner"]   # capability authorization scoping

[capability.platform]
requires = []       # e.g. ["gpu", "locality/east-0"] for platform-constrained skills

[skill]
prompt = """
You are reviewing a pull request. Given the PR number and focus area:
1. Fetch the diff via the gh tool
2. Analyse for the specified focus area
3. Return structured JSON matching the output schema
"""
tools = ["gh", "read_file"]   # mesh capabilities this skill may resolve and invoke
```

A **`SkillRunner`** node loads the file at startup:

1. Advertises `capability(ns, name)` with schema pushed to `skills/{ns}/{name}/{node_id}` in KV
2. Runs a `signal_rx` loop — waits for invocations
3. On invocation: deserialises input per schema → runs LLM with skill prompt + input →
   serialises output per schema → responds via nonce RPC
4. Respects `max_concurrent` via the `suppress` primitive

One `SkillRunner` binary hosts any skill. Swap the file, get different mesh-visible behavior.
LLM credentials are held by the runner, not the substrate — consistent with the credentials
architecture above. Multiple `SkillRunner` nodes loading the same skill file are load-balanced
by the mesh automatically via capability resolution.

**Skill composition is emergent.** The `tools` list in the skill file is itself a capability
resolution list. During execution the LLM is handed a tool set derived from `resolve_capability`
at call time, not hardcoded at authoring time. If a `gh` capability is on the mesh, it appears
in the LLM's tool list. Skills can invoke skills; the mesh routes the sub-invocation; the audit
trail captures the causal chain.

**MCP mapping:**

| Concept | Mycelium form |
|---|---|
| MCP tool | skill capability with schema in `skills/{ns}/{name}/{node_id}` |
| MCP tool invocation | `signal_wired_via` + nonce RPC |
| Skill permissions | `[capability.policy].authorized_callers` |
| Skill platform constraints | `[capability.platform].requires` |

### Plans — Not a Primitive

The industry term "plan" is not a Mycelium primitive. **Planning is LLM-internal reasoning.**
Mycelium provides the execution substrate that makes any plan executable: capability discovery,
signal delivery, and causal audit trail. It does not store or schedule plans.

A planning agent emits signals that trigger sequential capability resolutions. The "plan" lives
in the LLM's context window during execution and in the HLC-keyed audit trail after the fact.
Storing a plan as a first-class mesh object reintroduces a central scheduler — the same
architectural problem as LangGraph.

```
LLM reasons → decides to invoke "dev/code-review"
→ resolve_capability("dev", "code-review") → node_id
→ signal_wired_via(filter, "skill.invoke", json_payload)
→ result arrives via Individual scope nonce
→ LLM reasons again with result added to context
→ audit trail captures the full causal chain (HLC-keyed)
```

The plan is the LLM's internal monologue. The audit trail is the post-hoc record.
The mesh is the execution engine for both.

### Layer 4 Security Primitives

Multi-agent MCP environments create new threat models that origin-based security (SOP, CORS)
never covered. The three primitives below address this at the mesh layer, not the transport layer.

**1. Invocation audit trail** ✓ Complete
Append-only causal log of capability resolutions and skill invocations, propagated via gossip
and keyed by HLC. Captures not just "agent X called skill Y" but the full causal chain: which
signal triggered the invocation, which agent emitted that signal. Enables post-hoc detection
of prompt-injection → cross-service pivot patterns. KV namespace: `audit/{hlc}/{node_id}`.

Exports OTEL spans (trace ID = request nonce, parent span = causal predecessor HLC) so
operators can use existing Grafana / Jaeger / Honeycomb stacks without learning Mycelium
internals. OTEL export is gated on the `otel` cargo feature.

**2. Capability authorization scoping**
`resolve_capability` today returns any matching capability on the mesh — any caller, any
context. For skill/tool exposure this is the confused deputy gap: an LLM manipulated via
prompt injection has the same resolution power as legitimate code. Need a per-caller/session
authorization layer at the `resolve_capability` call site. Expressed declaratively in the
skill or manifest:

```toml
[capability.policy]
max_concurrent     = 3
authorized_callers = ["orchestrator", "planner"]
```

This field affects both `advertise_capability` and `resolve_capability` API signatures — design
before finalising either.

**3. Session-scoped mesh views**
When an LLM agent executes a task via a skill, it should see only capabilities authorized for
that task's context — not the full capability space. Prevents cross-session capability leakage.
Mycelium's capability TTL + `advertise_capability` are already the right primitives; what's
missing is the scoping declaration that constrains what `resolve_capability` returns for a
given caller context.

> **Token bloat and security scoping are the same design problem.**
> When a language bridge (Python/TS) or SkillRunner asks the mesh for available tools, a naive
> `scan_prefix("tools/")` dumps every capability schema on the mesh into the LLM's context
> window — burning tokens on irrelevant tools and widening the confused deputy surface at the
> same time. The fix is identical for both concerns: tool discovery for an LLM agent is a
> *filtered* `resolve_capability` scoped to the caller's authorized context, not a full mesh
> scan. Design the language bridge tool-discovery endpoint to accept a caller context and return
> only the capabilities that context is permitted to see. Session-scoped mesh views is the
> security primitive; filtered tool schemas is the UX/token outcome. One implementation, two
> benefits. Do not implement language bridge tool discovery as a raw `scan_prefix` and patch
> scoping in later — the filtering must be first-class from the start.

**Why mesh-level, not transport-level:** Origin isolation (SOP/CORS), OAuth enhancements, and
user confirmation checkpoints are application-layer concerns. The confused deputy problem is
about what an LLM *decides* to do within its legitimate access — that requires a mesh-level
capability gate, not a network boundary.

**Sequencing:** Design alongside the SkillRunner and MCP server role work. The
`[capability.policy]` field in the skill definition is the natural hook point; the authorization
scoping implementation lives at `resolve_capability`. Retrofitting it after those APIs are
finalised is expensive.

### Landscape Survey — What Not to Take, What to Borrow

From surveying agentgateway (Solo.io / Rust MCP proxy), Gloo Mesh (Istio-based), and LiteLLM:

**Centralised proxy / router model** — do not adopt. agentgateway and LiteLLM solve routing
through a single control plane. Applying that to Mycelium reduces it to a fancy HTTP client,
losing adaptive routing, demand pressure, and locality-aware dispatch. Same trap as LangGraph.

**Sidecar injection (Istio style)** — unnecessary. Mycelium is a library; agents don't need a
daemon injected alongside them.

**A2A wire-protocol adapter (post-MCP)** — shipped. See `a2a` cargo feature.

---

### A2A Discovery Ecosystem — Positioning (2026-05-25)

Concrete projects in the A2A discovery space and where Mycelium sits relative to each:

| Project | Model | Mycelium vs. |
|---|---|---|
| **A2A Registry proposal** (community) | Centralised registry service; clients query by skill/tag | Mycelium *is* the registry — `cap/` KV gossips to every node; `/.well-known/agent.json` is built live from it. No registry process. |
| **Gemini Enterprise A2A** | Admin-configured enterprise catalog, Google-hosted | Not competing. Enterprise product plane. Mycelium is the library underneath such a product. |
| **EMQX A2A over MQTT** | MQTT broker indexes Agent Cards; topics = discovery bus | Closest structural analog. Both do distributed discovery + liveness. Key difference: EMQX requires a broker (SPOF); Mycelium is peer-to-peer — no broker. |
| **AgentScope / Nacos** | Runtime plugin publishes cards to Nacos service registry | Centralised registry; same SPOF tradeoff as EMQX. Mycelium gossips directly without Nacos. |
| **python-a2a discovery module** | Python library: AgentRegistry + DiscoveryClient + heartbeat | A client-side API wrapper, not infrastructure. `mycelium-py`'s `A2aClient` covers the same API surface; the Mycelium node is the actual registry. |
| **ANS (IETF Internet-Draft)** | DNS-like, PKI-backed, protocol-agnostic, internet-scale | Complementary. ANS is cross-org/internet-scope; Mycelium is cluster-scope. A Mycelium cluster could register a single endpoint with ANS. |
| **AGNTCY ADS** | DHT content routing, OCI/ORAS storage, OASF records, provenance/attestation | ADS targets static catalogs with content-addressed immutability. Mycelium is mutable LWW with TTL — better for ephemeral, dynamic agents. Complementary for different concerns. |
| **NANDA / AgentFacts** | Three-layer internet-scale index: lean AgentAddr → W3C VC AgentFacts → dynamic resolver. "Quilt" federation of enterprise, gov, Web3, civil-society registries. | Natural federation layer above Mycelium. A single NANDA `AgentAddr` pointing at a cluster's `/.well-known/agent.json` is enough — no code changes. NANDA covers cross-org discovery and VC-signed attestation; Mycelium covers everything inside the cluster. |
| **Microsoft Agent 365 / Entra Agent ID** | Enterprise inventory + governance plane, Azure-integrated | Platform play. Mycelium is the library an operator would embed; Agent 365 is what an enterprise wraps around it. |
| **MCP Registry** | App-store / static catalog for MCP servers | Static and human-curated. Mycelium's `tools/` KV namespace gossips MCP tool availability dynamically — no registration step. |

**What Mycelium does that none of these do together:**

1. **Discovery is a side-effect of membership.** `advertise_capability` writes a TTL'd KV entry
   that propagates epidemically. There is no register/deregister lifecycle — the TTL handles it.
   Every node has a complete, eventually-consistent view of the fleet.

2. **Routing, not just lookup.** `resolve_with_locality`, `shard_for`, `emit_sharded`, demand
   pressure — the routing decision happens at the substrate, not in application code layered on
   top of a registry API.

3. **No coordinator.** EMQX, Nacos, Gemini Enterprise, Agent 365 all have a service you depend
   on. Mycelium's only failure mode is losing all nodes. Partial failures are transparent.

4. **Execution substrate, not just a directory.** The same library that does discovery also does
   RPC, signals, consensus, sharding, and reliable delivery.

**Where Mycelium genuinely doesn't compete:**

- **Internet-scale / cross-org discovery** — ANS and NANDA. Mycelium assumes a cluster you own.
  Cross-org discovery needs PKI, trust anchors, and a public registry.
- **Static provenance and attestation** — AGNTCY ADS. Mycelium's KV is mutable LWW; wrong model
  for "what exact version of this agent was running at 14:32 UTC."
- **Enterprise governance / compliance plane** — Agent 365/Entra. Mycelium has an audit trail
  (HLC causal log + OTEL) but not an admin console or SSO integration.

**The natural stack is two layers: Mycelium for the cluster, NANDA for the internet.**
Mycelium's `/.well-known/agent.json` endpoint is already a conforming A2A server; a single NANDA
`AgentAddr` record pointing at the cluster's A2A gateway is all that's needed to federate with the
broader agent web. No intermediate layer required.

ANS and AGNTCY ADS are niche alternatives, not required steps — and both niches are addressable
inside Mycelium if needed:
- **ANS** — targets DNS/PKI shops that won't adopt W3C VCs. Mycelium already has Ed25519 node
  identity and mTLS; an ANS-compatible naming adapter would be a thin layer over existing
  infrastructure, not a new dependency.
- **AGNTCY ADS** — targets OCI/ORAS immutable artifact storage and OASF schema compliance.
  Mycelium's `tools/` KV namespace already gossips skill availability dynamically; an OCI-backed
  snapshot export (for provenance / audit) could be added as a feature without changing the
  runtime model.

In either case the two-layer stack holds: Mycelium handles it internally, NANDA federates it
externally.

#### NANDA — Paper Analysis (2026-05-25)

Source papers: *"Upgrade or Switch: Do We Need a Next-Gen Trusted Architecture for the Internet
of AI Agents?"* (2506.12003v2) and *"Beyond DNS: Unlocking the Internet of AI Agents via the
NANDA Index and Verified AgentFacts"* (2507.14263v1).

**1. NANDA's "Switch path" table is Mycelium's architecture spec.**
Table 3 of 2506.12003 defines the desired Update/Revocation Latency for the clean-slate path as
*"Gossip-based or CRDT ledger with millisecond write propagation and automatic tombstoning."* That
is the exact description of Mycelium's KV substrate. Wire v10's `WireMessage::SignedData`
(Ed25519-signed KV gossip) is the "trusted" extension NANDA identifies as mandatory for that path.

**2. NANDA formally validates Mycelium's deployment scope.**
Table 4 of 2507.14263 categorises A2A Agent Cards as *"best-fit for stable SaaS agents inside a
single marketplace."* Mycelium is exactly that — a cluster-scoped deployment. The paper is not
calling single-marketplace limited; it is saying that is where A2A Agent Cards belong. Mycelium's
A2A adapter sits squarely in the intended niche.

**3. Zero-change upgrade path from Mycelium A2A to NANDA AgentFacts.**
2507.14263 states explicitly: *"any conforming A2A server can embed its existing card as a `skills`
extension [in AgentFacts], gaining cryptographic attestation, privacy paths, and TTL-based routing
without altering its runtime logic."* Mycelium's `/.well-known/agent.json` endpoint is already a
conforming A2A server. Upgrading to NANDA registration requires no Mycelium code changes — just an
external AgentFacts document pointing at the existing endpoint.

**4. Mycelium as an enterprise "Quilt" shard.**
NANDA's federation model (the "Quilt") explicitly includes enterprise registries as first-class
participants alongside government, Web3, and civil-society registries. A Mycelium cluster can
register as one enterprise shard: a single NANDA `AgentAddr` record pointing at the cluster's A2A
gateway, with the full capability ring queryable by external resolvers. No Mycelium internals need
to be exposed.

**5. The "Capability Threshold" maps cleanly to Mycelium's scope boundary.**
Paper 1 identifies a crossover point where DNS/PKI assumptions break at scale: 24-48 h propagation,
CRL/OCSP revocation lag, missing capability metadata in RDAP/WHOIS. This threshold formalises where
Mycelium's work ends and external infrastructure begins. Mycelium lives entirely below it —
sub-millisecond local writes, no DNS, Ed25519 identity already in place. NANDA addresses what
happens when you need to reach *across* that boundary to another cluster or org. The two projects
are adjacent layers, not competing ones.

**6. Boundary-Aware Naming ≈ Mycelium locality resolution.**
Paper 1's "Configurable Search Paths" proposal (split-horizon DNS for agents — queries resolve
differently depending on whether the caller is inside or outside the enterprise boundary) maps
directly to Mycelium's locality-aware resolution and group-scoped signal boundaries. NANDA is
building at DNS scale what Mycelium already does at cluster scale. If NANDA's Configurable Search
Paths land, a Mycelium cluster could advertise itself as one named search-path scope, making
intra-cluster fast-path resolution transparent to cross-org callers.

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

## Opt-In Consistency and Ordering Overlay (Complete)

The epidemic substrate is always available and always fast. These APIs escalate to stronger
guarantees only for the specific operation that demands them — nothing in the fast path becomes
slower or more complex because they exist.

This is **CAP theorem applied selectively, not globally.** Traditional systems pick one position
and apply it uniformly. Here you choose per operation. The same cluster, the same embedded
library, with no separate infrastructure.

### Consensus KV and Coordination — Consul / etcd parity

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
agent.kv().append("events/orders", entry_bytes);

// Subscribe from a position — reactive, ordered by HLC key, fires on new entries
let mut rx: watch::Receiver<Vec<(Hlc, Bytes)>> = agent.kv().subscribe_log("events/orders", since_hlc);

// Range scan — replay a window or from a checkpoint
let entries = agent.kv().scan_log("events/orders", from_hlc, to_hlc);

// Compaction — tombstones entries older than a watermark
agent.kv().compact_log("events/orders", before_hlc);
```

**Consumer groups** — each consumer tracks its position as a KV entry:
`consumer/{group}/{stream}/offset` = last-processed HLC. `subscribe_log_group` delivers each
entry to exactly one member; `distributed_lock` or `elect_leader` coordinates claim when needed.

**Foundation already exists:** HLC (hybrid logical clock), gossip KV, prefix scan, tombstone
mechanism. This is new API surface over existing primitives, not new infrastructure.

### Reliable Delivery — Akka parity

ACK retry over `rpc_call` (Layer 3). The HLC and signal reorder buffer handle causal ordering
and dedup on the receiver side.

```rust
// Fire-and-forget with ACK — retries until acknowledged or timeout
let result = agent.emit_reliable(
    "actor.msg",
    SignalScope::Individual(target),
    payload,
    Duration::from_secs(5),
).await?;  // → AckResult::Acknowledged | AckResult::Timeout
```

**Foundation:** `rpc_call` (Layer 3), signal reorder buffer (complete).

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
| Consul / etcd | Consensus-durable KV | `consistent_set` / `consistent_get` | ConsensusEngine ✓ |
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
| Consensus | ConsensusEngine, epidemic two-phase voting, group_propose | **Complete** |
| Capability | advertise_capability, resolve, watch_capabilities | **Complete** |
| Capability | declare_requirement, watch_requirement, RequirementStatus | **Complete** |
| Capability | define_capability_group, gcap/ projections, emergent groups | **Complete** |
| Capability | resolve_wiring, watch_wiring, signal_wired_via, inter-group wiring | **Complete** |
| Capability | resolve_with_locality, signal_wired_via_locality, locality paths | **Complete** |
| Capability | demand, watch_demand, DemandStatus (demand pressure surface) | **Complete** |
| Capability | Predicate-narrowed watchers, 50 ms debounce, one-task-per-group | **Complete** |
| Capability | C2: consolidated opacity watcher — one task + one cap/ subscription for all declared requirements | **Complete** |
| Layer 3 | Embedded HTTP server, SSE streaming, `rpc_call`/`rpc_respond` primitive | **Complete** |
| Layer 3 | Bulk payload / `invoke.bulk` ticket, Actor/Event mailboxes, scatter-gather | **Complete** |
| Layer 4 | MCP bridge: server role (tools/ KV + rpc_call dispatch) | **Complete** |
| Layer 4 | MCP bridge: client role (outbound to external MCP servers) | **Complete** |
| Layer 4 | Agent state machine: policy-guarded transitions, turn/call budgets, state_timeouts | **Complete** |
| Layer 4 | `NodeCapabilityConfig`: declarative local capability declaration + probe loop | **Complete** |
| Layer 4 | Python language bridge: HTTP gateway + `mycelium-py` SDK | **Complete** |
| Layer 4 | TypeScript language bridge | **Complete** |
| Layer 4 | `SkillRunner` node + `.skill.toml` capability-as-skill definition format | **Complete** |
| Layer 4 | Invocation audit trail: HLC-keyed causal log + OTEL span export | **Complete** |
| Layer 4 | Capability authorization scoping: `[capability.policy]` in manifest + `resolve_capability` gate | **Complete** |
| Layer 4 | Session-scoped mesh views: per-caller capability slice at `resolve_capability` | **Complete** |
| Layer 4 | A2A wire-protocol adapter (`a2a` feature — `GET /.well-known/agent.json`, `POST /a2a` JSON-RPC) | **Complete** |
| Layer 4 | A2A outbound clients: Python `A2aClient`, TypeScript `A2aClient` | **Complete** |
| Layer 5 | Metrics, Prometheus exporter, Grafana dashboard | **Complete** |
| **Production** | Multi-machine integration tests + Docker Compose reference topology | **Complete** |
| **Production** | KV persistence: WAL + snapshot/replay; consensus committed-slot durability | **Complete** |
| **Production** | Security: mTLS peer connections + NodeId keypair + consensus payload signing | **Complete** |
| **Production** | KV write signing: Ed25519-signed gossip frames (`WireMessage::SignedData`, v10 wire) | **Complete** |
| Layer 2 | Signal reorder buffer: `emit_ordered()`, `hlc_seq` wire field (v11), per-`(sender,kind)` min-heap, watermark dedup, config-driven | **Complete** |
| Capability / Layer 2 | Semantic coordination: capability schema versioning (`with_schema_id`, `CapFilter::with_schema`), gossip-propagated payload schemas (`with_input_schema`, `with_output_schema`), signal sender auth (`signal_rx_from`), FIPA-ACL speech act taxonomy | **Complete** |
| Capability | Schema registry: `publish_schema` / `force_publish_schema` / `get_schema` / `list_schemas` / `seed_schemas_from_dir` — `schemas/` KV namespace, conflict detection, JSON validation | **Complete** |
| Consistency overlay | `consistent_set`, `consistent_get`, `distributed_lock`, `elect_leader` | **Complete** |
| Ordering overlay | `append`, `subscribe_log`, `scan_log`, `compact_log` (ordered log) | **Complete** |
| Ordering overlay | `subscribe_log_group` + consumer group offset tracking | **Complete** |
| Reliable delivery | `emit_reliable` + ACK retry (requires Layer 3 `rpc_call`) | **Complete** |
| Cluster sharding | `shard_for`, `emit_sharded` (consistent hash ring over `NodeId::id_hash()`) | **Complete** |
| Research | AAMAS 2027 paper — *The Coordinator Trap* — first draft + structural revision complete; §8 benchmarks pending | **In Progress** |

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
agent.mesh().join_group("nlp");

let load_key = format!("load/{}", agent.node_id());
let agent2 = agent.clone();
let _advert = agent.mesh().advertise(
    signal_kind::CONTRACT_AVAILABLE,
    SignalScope::Group("nlp"),
    Duration::from_secs(10),
    move || {
        let state = LoadState { queue_depth: QUEUE.len(), written_at_ms: unix_ms_now() };
        agent2.kv().set(load_key.clone(), encode(&state));
        encode(&state)
    },
);

let mut invoke_rx = agent.mesh().signal_rx(signal_kind::INVOKE);
tokio::spawn(async move {
    while let Some(sig) = invoke_rx.recv().await {
        let req: InvokeRequest = decode(&sig.payload);
        let result = run_model(&req.payload).await;
        agent.mesh().emit(
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
let reply_fut = agent.mesh().signal_once(
    signal_kind::INVOKE_RESULT,
    Duration::from_secs(5),
    move |s| s.nonce == nonce,
);
agent.mesh().emit_async(
    signal_kind::INVOKE,
    SignalScope::Group("nlp"),
    encode(InvokeRequest { nonce, payload: input }),
).await;

match reply_fut.await {
    Some(sig) => decode(&sig.payload),
    None => {
        let any_live = agent.kv().scan_prefix("load/")
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

**6. Consistency as a service, not a foundation — the structural inversion.** Raft-based systems
make consistency the foundation and everything else pays that cost uniformly. Mycelium inverts
this: the epidemic substrate is the foundation; consistency, ordering, and reliable delivery are
services layered on top. The `ConsensusEngine` is built *over* the gossip KV, not the other way
around — this is not a theoretical claim, it is the current architecture. An agent that never
calls `consistent_set` pays zero overhead for its existence. The result is per-operation guarantee
selection: epidemic signals (sub-ms), causally-ordered logs (`append`/`subscribe_log`),
consensus-durable writes (`consistent_set`), distributed locks, and leader election all coexist on the
same cluster, the same binary, with no separate infrastructure for each tier. Consul, Kafka, and
Akka each pick one position on the tradeoff and apply it uniformly. This architecture picks per
operation. (See *The Structural Inversion* section above.)

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

This is a well-designed product whose novel combination of ideas is also the subject of a formal academic argument. The novelty is the *combination* and the *context*: a single-dependency, embedded, broker-less system that unifies epidemic KV state, ephemeral scoped signals, dynamic group topology, adaptive topology control, and contract-based capability advertisement — specifically targeting adaptive AI agent swarms where minimising operational overhead and maximising evolvability matter. None of the individual components is new; the particular assembly, grounded in Holland's signal/boundary model as a first-class design principle, is the differentiated position.

The companion paper — *"The Coordinator Trap"* (target: AAMAS 2027) — argues that the coordinator assumption is not an implementation deficiency but a structural failure mode, that Holland's model provides the theoretical basis for its elimination, and that Mycelium is the working implementation demonstrating that each failure mode is structurally impossible. The substrate is architecturally novel and coherent relative to the current AI agent framework landscape. The engineering work is on a sound foundation.

---

## Research Paper — AAMAS 2027 (In Progress)

**Title:** *The Coordinator Trap: Why Mediated Multi-Agent Architectures Cannot Scale and a Substrate-Based Alternative*

**Status:** First draft complete; structural revision done 2026-05-28. §8 evaluation benchmarks are placeholders pending empirical runs.

**Files:** [`docs/publications/paper.md`](docs/publications/paper.md) (Markdown source), [`docs/publications/arxiv/main.tex`](docs/publications/arxiv/main.tex) (LaTeX source). Rendered HTML is derived and not tracked — regenerate from source as needed.

### Core argument

The paper makes four contributions:

1. A historical account of how the coordinator assumption has persisted for fifty years — Blackboard, Actor model, Linda, BDI/FIPA, LLM orchestration — and why no prior system eliminated it.
2. A causal analysis of three failure modes (audit burden, context loss, output format mismatch) traced to the coordinator assumption through an agent-theoretic lens. Key insight (§4.5): components called "agents" in mediated hierarchies are workers in a fanout RPC system — stripped of the intrinsic boundary property that makes genuine agents scalable. This is the *category error* at the root of all three failure modes.
3. Holland's signal/boundary model (§5) as the theoretical foundation: two primitives, coordinator eliminated structurally rather than ameliorated.
4. Mycelium (§7) as a working implementation — each of the three layers makes a mirrored failure mode structurally impossible. §7.5 adds a quadratic cost decomposition argument: M × (k/M)² = k²/M, showing coordinator-free decomposition is structurally cheaper as well as architecturally correct.

Supporting arguments in Discussion: the Hayek epistemic parallel (central coordination fails structurally in any complex adaptive system, not just software), the Beinhocker organisational parallel, and the strip-the-ceremony pattern (§6) showing how Jini, OSGi, and Paremus each had the correct concept but the wrong implementation.

### Remaining work before submission

| Item | Status |
|---|---|
| §8.1 Coordination Convergence Time — Mycelium `group_propose` vs NegMAS SAO negotiation | Placeholder |
| §8.2 Failure Tolerance — coordinator failure vs random node failure in Mycelium | Placeholder |
| §8.3 State Freshness Under Churn — TTL evaporation rate vs knowledge graph drift | Placeholder |
| §8.4 Audit Obligation Under Load — O(matching) vs O(N) artifact production | Placeholder |
| Citation pass — resolve all `CITE-*` placeholders | Pending |
| Author attribution | Pending |

§8.5 (existing integration evidence: 239 tests, 11 scenarios) is already written. The structural argument is complete and does not depend on §8.1–8.4; those benchmarks provide falsifiable empirical grounding for the claims already made.

---

## Production Readiness Gap

> **✅ CLOSED — 2026-06-14 (`v1.2.0`).** Every engineering item below is shipped to an
> implemented-tested-documented bar across five merged PRs: WS1 Identity & RBAC, WS2
> tamper-evident hash-chained audit, WS3 crown-jewel (opt-in data-at-rest cipher + egress
> allowlist + blast-radius threat model), WS4 generic-OIDC SSO, WS5 hot certificate rotation
> (with multi-key archival + full egress coverage). All behind the `compliance` feature;
> default build unchanged. Verification: 318 default / 323 `tls` / 366 `compliance` lib tests,
> clippy `-D warnings` clean, 13/13 Docker integration scenarios, 100-node scale + 21-node
> resilience suites green. Support/SLA (sub-gate 4) remains **commercial-track** (out of
> engineering scope); the next engineering horizon is the **v2.0 Milestones** below.

The following gaps were the difference between what exists today and a system that could be
deployed in a real multi-machine AI fleet. They are ordered by blocking severity.

**Execution plan (completed):** the action plan that closed every engineering item to a uniform
implemented-tested-documented bar is [`docs/plans/v1x-completion.md`](docs/plans/v1x-completion.md)
(WS1–WS6: RBAC/identity, tamper-evident audit, crown-jewel, generic-OIDC SSO, cert rotation,
doc alignment). Support/SLA (sub-gate 4) is tracked separately as commercial work.

### 1. Multi-machine integration tests — Complete (2026-05-23)

A Docker Compose-based integration test suite exercises real TCP connections across containers.
Twelve unattended scenarios run automatically via `make test`:

| # | Scenario | What it covers |
|---|---|---|
| 01 | Mesh convergence | KV write on node-a propagates to node-b via epidemic gossip |
| 02 | Management API + dashboard | `/api/state` JSON validity, HTML dashboard rendered |
| 03 | KV persistence — single restart | WAL replay restores state before anti-entropy kicks in |
| 04 | Full-cluster restart | node-a restores from WAL; node-b recovers via anti-entropy |
| 05 | Anti-entropy late joiner | node-c starts 25 s late; receives all prior keys |
| 06 | Signal propagation | `test.signal` emitted on node-a received by node-b |
| 07 | Capability discovery | mgmt `/api/state` shows all nodes with correct roles |
| 08 | Scatter-gather fan-out | `POST /scatter` fans out to all peers; at least 1 responder required |
| 09 | invoke.bulk large payload | 4 096-byte payload staged over HTTP; echoed back with `ok=true` |
| 10 | Actor/Event mailbox delivery | self-addressed event delivered and counted via open_mailbox watcher |

**LLM demo smoke test** is a manual scenario started with `make test-llm-demo` —
it requires Ollama with `llama3.2` installed locally.

The test infrastructure lives in `tests/integration/`. The `node` role added to
`examples/three_node_demo.rs` provides `/health`, `GET/PUT /kv/*key`, and `POST /emit/:kind`
endpoints — thin wrappers over the library API with no added test-only logic in the library
itself.

Operator sizing guidance for `max_peers` / `max_forwarding_peers` / `epidemic_extra_peers`
at 10 / 100 / 1,000 nodes is deferred to the production deployment guide.

### 2. KV persistence — Complete (2026-05-23)

Per-node WAL + snapshot persistence is implemented. Nodes survive process restarts and
full-cluster cold restarts. Consensus committed slots are always fsynced regardless of
`sync_mode`. See the **Layer 1 — KV Persistence** section above for the full configuration
reference.

### 3. Security layer — Complete (2026-05-24)

mTLS peer connections, Ed25519 node identity keypairs, and signed consensus payloads are
implemented under the optional `tls` cargo feature. Enabling `GossipConfig::tls` is sufficient;
certificates auto-generate on first start.

**What was implemented:**
- **mTLS** — every gossip TCP connection requires a valid cluster CA-signed cert. A node without
  the shared CA cert is rejected at the TLS handshake before any data is exchanged.
- **Node identity keypair** — each node generates an Ed25519 signing key (same key as its TLS
  cert). The 32-byte verifying key is gossiped to `sys/identity/{node}` and cached in
  `peer_keys` so peers can verify signed messages.
- **Consensus payload signing** — all `Propose`, `Vote`, `Nack`, and `Commit` payloads are
  signed by the sender and verified on receipt via `SignedConsensusMsg`. Forged ballots are
  silently dropped with a `warn!` log entry.

**Implemented in v10 wire:**
- **KV write signing** — `WireMessage::SignedData` variant carries an Ed25519 signature over
  hop-invariant fields (nonce, sender, is_tombstone, timestamp, key, value — TTL excluded so
  the signature survives forwarding hops). Active at `set`/`delete`/`set_async`/`delete_async`
  and consensus KV writes when the `tls` feature is enabled. Receivers fail-open on unknown
  signers (key hasn't gossiped yet) and drop + warn on verification failure.

**Hot certificate rotation — Complete (2026-06-14, WS5):**
- `GossipAgent::rotate_identity(propagation)` rotates the node's Ed25519
  identity/TLS key with no cluster disruption: generate a new CA-signed cert
  (reusing the cluster CA), publish `sys/identity/{self}` = `new‖old` signed by
  the still-trusted old key, wait a propagation window, then atomically swap the
  active key + configs (`ArcSwap` — `server_config()`/`client_config()` are read
  per connection, so the cert cutover is live with no listener restart; existing
  connections keep their CA-trusted session).
- **Retained-key verification (option B):** `peer_keys` holds a *set* per node,
  accumulated across rotations, so historical signatures (audit chain, committed
  consensus, role claims) stay verifiable; every verify path tries the set.
  `sys/identity/{node}` stores the **full** key history (`32 × N`, current first),
  so verification survives any number of rotations and restarts (grows 32 B per
  rotation). *Compromise caveat:* a retired key stays accepted for verification, so
  rotating away from a compromised key needs explicit revocation on top.
  Runbook: [`docs/operations/cert-rotation.md`](docs/operations/cert-rotation.md).

### 4. Language bridges — Complete (2026-05-24)

The Python (`mycelium-py/`) and TypeScript (`mycelium-ts/`) bridges are implemented as HTTP
gateway sidecars (~1 ms loopback overhead; no PyO3/native extension — any language that speaks
HTTP works). Each covers the core mesh surface — `advertise_capability`, `declare_requirement`,
`on_signal`, `emit`, `resolve`, `demand`, RPC, KV, and Actor/Event mailbox. See the **Python
Language Bridge** and **TypeScript Language Bridge** sections in `README.md` for the API
reference.

### 5. Write-durability confirmation API — Complete (2026-05-25)

Without confirmation, `set_async` returns as soon as the value is written locally and handed
to the gossip shard — a write-then-stop race (the originating node killed before gossip reaches
a persistent peer) could lose data if no other node held the key. Closed by
`set_with_min_acks(key, value, min_acks, timeout)` (`src/agent/kv_handle.rs`), which:

1. Writes the key locally and to WAL (as `set_async` does today).
2. Subscribes to the KV watcher for the key (already exists).
3. Waits until `min_acks` *distinct* nodes have echoed the key back via
   anti-entropy (observable as a `subscribe` event where the echoed
   timestamp ≥ the local write timestamp and the update's sender is a
   different node).
4. Returns `Ok(n_acks)` on success or `Err(Timeout)` if fewer than
   `min_acks` nodes confirmed within `timeout`.

No new wire messages are required: the existing `StateResponse` path already
delivers the key back to the originator when a peer runs anti-entropy. The
only additions are a per-key in-memory ACK tracker and a waker.

**Interaction with persistence:** callers should pass `min_acks` equal to the
number of persistent nodes they require to hold the key, not the total cluster
size. Non-persistent peers can serve as ACK sources for availability but not
for restart durability.

**Scope note:** this is Layer I only; it does not replace or overlap the
Consensus API which provides total-order agreement. `set_with_min_acks` is
best-effort quorum write — "at least N nodes saw it" — not "all nodes agree
on the same value at the same logical position."

### 6. Observability — Complete (2026-05-25)

Structured metrics export ships behind the optional `metrics` cargo feature: a `metrics` facade
integration (zero-cost when no recorder is installed — `counter!`/`histogram!`/`gauge!` compile
to no-ops) plus a Prometheus `/metrics` endpoint on the gateway, emitting the Layer 5 counter
set (`gossip_frames_dropped_total`, `signal_delivered_total`, `gossip_store_entries`,
`gossip_peers_connected`, `contract_invocations_total`, `contract_invocation_latency_ms`).
`system_stats()`, `dropped_frames`, and `peer_drop_counts()` remain the always-on diagnostic
surface (see **Layer 1 Observability** in `README.md`).

### Production-hardening gate — the four sub-gates

The remaining v1.x security/compliance gaps (#7, #8, #9, plus two new items
identified below) collectively form the production-hardening gate that
regulated buyers (healthcare, finserv, federal) evaluate before procurement.
This section names the four sub-gates explicitly so the gate is a concrete
checklist rather than a vague "harden it" line item. Each sub-gate identifies
what is currently in the substrate, what is in flight as a numbered gap, and
what remains as new work.

**1. AuthN/Z + RBAC.** Different clearance levels for different layers of the
twin — an L1 board read is different from full L3 SPOF topology and should
not require the same authorisation. Standard RBAC is not enough; the model
must be data-classification-aware.

- *Existing substrate:* gateway bearer-token auth
  (`GossipConfig::gateway_auth_token`, **off by default**); `signal_rx_from`
  sender authorisation; capability schema versioning; wire-level Ed25519 mTLS
  (`tls` feature).
- **Shipped — WS1 (RBAC v1.x subset, `compliance` feature).** Signed node role
  claims (`sys/role/{node}`, Ed25519 over `{node,roles,clearance,issued_at}`,
  verified against cluster-learned identity keys — forgery reads back as `None`);
  data-classification `clearance` on the claim (the L1/L2/L3 model);
  provider-side `caller_authorized` enforcing `authorized_callers` where a
  capability is *served* (wired into the SkillRunner serve loop); **OAuth2
  scope-based gateway ACLs** (`gateway_scoped_tokens`, `resource:verb` scopes,
  deny-by-default, public edge paths kept open — the WS4-OIDC forward-compat
  shape); and a core `sys/` namespace-ownership **tripwire**
  (`SystemStats::sys_namespace_violations`, detection-not-prevention). See
  CLAUDE.md §RBAC / identity and [`docs/plans/v1x-completion.md`](docs/plans/v1x-completion.md) §WS1.
- **Shipped — WS4 (SSO / generic OIDC).** OIDC JWT validation at the gateway
  (`compliance`): asymmetric-only alg allowlist (anti alg-confusion), `iss`/`aud`/
  `exp` checks, discovery + cached JWKS, IdP groups → gateway scopes via config.
  One code path for Entra/Okta/Auth0/Keycloak — see [`docs/operations/sso.md`](docs/operations/sso.md).
- *Deferred to WS-followups:* access control on the MCP / A2A bridge surfaces
  (distinct from the primary gateway auth) — the OAuth2 scope model extends to
  them directly.

**2. Audit — complete and tamper-evident.** *Shipped (WS2).*

- *Existing substrate:* HLC-ordered KV writes; WAL durability;
  Ed25519-signed KV writes (`tls` feature, `WireMessage::SignedData`, wire
  v10) so undetected mutation requires compromising the originating node's
  private key.
- **Shipped — WS2 (`compliance` feature).** Per-node hash-chained, Ed25519-signed
  audit stream at `sys/audit/{node}/{seq}`: each record's `prev_hash` is the
  SHA-256 content hash of its predecessor, so an inspector can verify the stream
  was not edited, reordered, or truncated (`audit_verify` / `verify_chain`, with a
  precise `AuditVerifyError`). The chain is per-node by necessity — a global chain
  would need a sequencer (a coordinator), so the cluster trail is the union of
  independently verifiable streams. `GET /gateway/audit` (scope `audit:read`)
  exposes per-stream `verified` + `head_hash` + per-record `content_hash` (the
  stable M16-citable identifier). SkillRunner seals every invocation through it
  with the *verified caller* as principal — the read-side principal binding for
  the served path. Detection-not-prevention: tampering fails verification, never
  blocked at the store.
- *Deferred (later WS):* read-side principal binding for gateway-token reads
  (waits on WS4 OIDC for real user principals); chain retention/compaction with
  checkpoint hashes (pruning a hash chain otherwise breaks genesis verification).

**3. Crown-jewel posture.** *Shipped (WS3).* *This is the sharpest of the four,
and the one that turns a generic "is it secure" conversation into a specific
blast-radius conversation that regulated buyers actually evaluate.* The twin
is the concentrated map of every SPOF and critical path in the deployment;
compromising it gives an attacker the complete dependency graph, the failure
modes, and the escalation paths.

- **Data-at-rest — shipped (WS3).** Opt-in `DataAtRestCipher` hook
  (`GossipAgent::with_data_at_rest_cipher`) envelope-encrypts WAL records and
  snapshots before they hit disk and decrypts them on replay. The substrate stays
  neutral on key custody — the operator supplies a KMS/keyring adapter. Feature-free,
  zero-overhead when unused; scope is on-disk only (in transit = `tls`).
- **Tier-2 egress boundary — shipped (WS3).** `EgressPolicy { allow_hosts }` in
  `GossipConfig` gates every outbound HTTP path the substrate chooses — the MCP
  client bridge, capability probes, and LLM-backend calls (core prompt skills +
  SkillRunner). A node-local posture, not a coordinator. Empty = allow-all (default).
  Intra-cluster bulk fetches and operator-configured OIDC JWKS are intentionally
  not gated; the A2A *client* is SDK-side. Documented in the egress runbook + threat model.
- **Blast-radius-if-compromised — shipped (WS3).** Threat-model document at
  [`docs/threat-model.md`](docs/threat-model.md), cross-linked from
  `docs/operations/` (crown-jewel runbook) and CLAUDE.md: per-trust-boundary
  threats (single node, trusted domain, external egress), the mitigations
  (mTLS + WS1 RBAC + WS2 audit + WS3 at-rest/egress), and residual risks.

**4. Support / SLA — the single-source v1 dependency question.** Regulated
buyers ask: "Who owns Mycelium in production when something fails at
03:00 on a Saturday?" An open-source library with a GitHub URL is not
an answer that satisfies procurement. This sub-gate is **commercial work**,
not engineering, but it gates the same procurement decisions.

- *Existing:* commercial embedding licence available (`Cargo.toml` notes;
  Tathata Systems Ltd is the legal entity); AGPL fallback for
  non-commercial use.
- *Still to design:* SLA tiers with named response-time commitments;
  named support relationships per customer deployment; documented
  escalation paths from customer on-call to Tathata on-call; reference
  customers with paid production deployments and SLAs in force.

**Cross-cutting:** gaps #7, #8, #9 deliver substrate-level primitives. The
four-sub-gate frame is the *evaluation lens* a regulated buyer applies to
those primitives plus the new work above. Aligning the gap-#N work with the
sub-gate frame keeps the engineering output legible to the procurement
audience.

---

### 7. Durable cluster-wide audit trail

The per-node WAL records every local write, but there is no cluster-wide audit namespace.
If a node is lost, its write history goes with it. For regulated deployments (HIPAA, SOC 2)
operators need to answer: *"What did every agent in the fleet do over the last 7 days?"*
without reassembling per-node WAL files manually.

**Design — three additions, all within existing Layer I:**

**a) Structured audit KV namespace.** Every `set`, `delete`, and `consistent_set` call
optionally emits a companion entry to `sys/audit/{node_id}/{hlc_seq}` with a fixed schema:

```
{ ts: u64, actor: NodeId, op: "set"|"delete"|"consistent_set", key: &str, value_hash: [u8;32] }
```

This is gossip-propagated and TTL-controlled (`audit_retention_secs`, default 7 days).
Because it uses the existing KV substrate, every peer holds a full replica of the audit log
automatically — no node is a single point of failure for the record. Value hashes (not
values) are stored so the audit log is tamper-evident without leaking payload data.

**b) WAL replication for audit entries.** Audit namespace writes use `sync_mode = Flush`
so each entry is fsynced before the originating node acknowledges the application write.
Combined with gossip replication, an audit entry survives both the originating node dying
and a peer losing its WAL.

**c) `/audit` HTTP endpoint (gateway feature).** Time-range scan over `sys/audit/`:

```
GET /audit?since=<unix_ts>&until=<unix_ts>&actor=<node_id>&key_prefix=<prefix>
→ JSON array of audit entries, sorted by HLC timestamp
```

The query is a `scan_prefix("sys/audit/")` with client-side filter — no new storage
mechanism, no indexing. At 1 000 agents × 10 writes/s × 7 days ≈ 6 billion entries this
approach breaks down, but at realistic agentic fleet sizes (10–500 agents) it is correct
and sufficient.

**Opt-in flag:** `GossipConfig::audit: bool` (default `false`). When `false`, no audit
entries are written and `sys/audit/` is empty — zero overhead for deployments that do not
need it.

**What is explicitly out of scope for v1.x:** immutable append-only audit log with
cryptographic chaining (Merkle / hash-chain). That is a v2 item if a regulated customer
requires it. The v1 design provides availability and tamper-evidence via value hashing;
it does not provide non-repudiation under an adversarial-insider threat model.

### 8. Role-based access control (v1.x subset)

Regulated-industry deployments (HIPAA, SOC 2) require documented access control and role
separation. The full gossip-level capability authorization layer is deferred to v2 (see
*v2.0 Milestones §6*), but a v1.x subset is sufficient to unblock compliance certification
and is buildable on existing primitives.

**What is needed:**

**a) Node roles.** Three roles annotated in `GossipConfig` and gossiped to
`sys/identity/{node}/role`:

| Role | Permitted operations |
|---|---|
| `operator` | Full read/write; gateway admin endpoints; audit log access |
| `worker` | KV read/write within own capability namespace; signal emit/receive |
| `probe` | Read-only KV; `/health`, `/ready`, `/metrics`; no write or admin |

Roles are enforced at the gateway layer (HTTP request → check caller's node role from
`sys/identity/`) and at `apply_and_notify` for the `sys/` namespace (worker nodes cannot
write to `sys/config/` or `sys/audit/`).

**b) Gateway endpoint ACLs.** Extend the existing bearer-token middleware to carry a
role claim. The token maps to a role; the role gates which endpoint groups are accessible.
No new auth mechanism — the `GOSSIP_GATEWAY_AUTH_TOKEN` path already validates the token;
role checking is a one-line extension of that middleware.

**c) Audit log access control.** The `/audit` endpoint (gap #7) is restricted to
`operator` role by default. Workers and probes cannot read the full audit trail.

**What this is not:** gossip-level write ACLs enforced at every forwarding hop (that is
v2 §6). The v1 enforcement point is the gateway and the `sys/` write guard — sufficient
for SOC 2 Type II controls evidence and HIPAA §164.312(a)(1) access control requirements.

---

### 9. SSO / enterprise IdP integration (Entra, Okta) — **Shipped (WS4)**

Enterprise procurement almost universally requires SSO against the organisation's IdP
before IT security will approve deployment. Shipped as WS4: generic-OIDC JWT validation
at the gateway. `GossipConfig::oidc = Some(OidcConfig { issuer, audience, group_claim,
group_scopes, jwks_uri })`; the gateway validates the bearer JWT (asymmetric-only alg
allowlist, `iss`/`aud`/`exp`, discovery + cached JWKS) and maps IdP groups → gateway
scopes, feeding the existing WS1 scope gate. Static token auth remains the fallback.
**One code path** covers Entra/Okta/Auth0/Keycloak — per-vendor differences (issuer,
group-claim name) are config, documented in [`docs/operations/sso.md`](docs/operations/sso.md)
with a per-vendor table. The original per-design notes below are retained for context.

**What was needed (design, now implemented):**

**a) OIDC token validation middleware.** Replace the static `GOSSIP_GATEWAY_AUTH_TOKEN`
comparison with a JWT validation path:

```
Authorization: Bearer <JWT>  →  validate signature against OIDC JWKS endpoint
                              →  extract role claim (configurable claim key)
                              →  pass to existing gateway ACL logic (gap #8)
```

`GossipConfig::gateway_oidc_issuer: Option<String>` — when set, the gateway fetches
`{issuer}/.well-known/openid-configuration` on startup, caches the JWKS, and validates
every bearer token. Static token auth remains available as a fallback for non-SSO
deployments.

**b) Entra (Azure AD) reference configuration.** A `docs/guide/sso-entra.md` showing the
app registration, role claim mapping, and `GossipConfig` snippet. This is the first IdP
most enterprise prospects will ask about.

**c) Okta reference configuration.** `docs/guide/sso-okta.md` — same pattern, second
most common enterprise IdP.

The JWKS cache refresh interval and token clock-skew tolerance are configurable via
`GossipConfig`. No new dependencies beyond a JWT validation crate (`jsonwebtoken` is
already in the dep graph via the `llm` feature).

---

### Gap Summary

| Gap | Severity | Status |
|-----|----------|--------|
| Multi-machine integration tests + deployment docs (12 scenarios) | **Blocking** | **Complete** 2026-05-23 |
| KV persistence (WAL + snapshot/replay) | **Blocking** | **Complete** 2026-05-23 |
| mTLS + node identity signing + consensus signing | **Blocking** | **Complete** 2026-05-24 |
| Python language bridge (`mycelium-py`) | High | **Complete** 2026-05-24 |
| `SkillRunner` + `.skill.toml` + invocation audit trail + OTEL | High | **Complete** 2026-05-25 |
| Opt-In Consistency & Ordering Overlay | High | **Complete** 2026-05-25 |
| `set_with_min_acks` write-durability confirmation API | Medium | **Complete** 2026-05-25 |
| Prometheus metrics export + dashboards | Medium | **Complete** 2026-05-25 |
| Durable cluster-wide audit trail (`sys/audit/`, `/audit` endpoint) | High | **Complete** 2026-06-14 (WS2 — per-node hash-chained signed trail, `verify_chain`, `/gateway/audit`, SkillRunner integration) |
| Role-based access control — v1.x subset (node roles, gateway ACLs) | High | **Complete** 2026-06-14 (WS1 — signed `sys/role/`, provider authz, OAuth2 gateway ACLs, `sys/` tripwire) |
| SSO / enterprise IdP integration (OIDC, Entra, Okta) | High | **Complete** 2026-06-14 (WS4 — generic-OIDC JWT validation at the gateway, discovery + JWKS, groups→scopes; mock-IdP test) |

None of these require architectural changes. The substrate is sound; these are engineering
completions on top of it.

**v2 (Milestone 16 / NANDA) forward-compatibility — acceptance criteria for the precursor items.**
Two of the Pending items above are precursors to M16 (NANDA AgentFacts). When implementing them,
build to the *stable substrate shape*, not NANDA's moving v0.3 surface — **don't foreclose, don't
pre-build** — and never couple to AgentFacts field/schema names (the spec is churning, incl. a
possible AgentFacts → "Agent Metadata Layer" rename):

- **Audit trail (gap #7):** preserve a **capability-scoped, content-hashed, externally-citable
  slice** (a stable per-claim hash). M16's self-certified AgentFacts cites it as `evaluations`
  provenance — the thing that turns *self-advertised* into *self-attested-with-audit*. Coarse
  who-wrote-which-key logging that can't be sliced per capability forces a v2 rebuild.
- **RBAC capability-authz (gap #8):** express "who may assert this capability" as a property of
  the **signed capability entry** (under the node Ed25519 identity / `SignedData`), not only an
  HTTP gateway gate — so it is already AgentFacts-shaped.
- **Edge / bridge auth (crown-jewel sub-gate):** as bearer-token auth is added, keep a
  **public-readable, signature-verified** path open — AgentFacts is meant to be publicly fetchable
  and *cryptographically* verified, not token-gated.

SSO / enterprise IdP is **orthogonal** (human operator auth, not agent machine identity) — no
forward-design required. Because these are signed KV entries under `sys/` namespaces, they are
forward-compatible by construction: M16 becomes "new keys + a new payload shape," never a schema break.

---

## v2.0 Milestones

These are architectural changes deferred until v1.x has production usage to inform decisions.
None are required for v1.0. **Every milestone here must satisfy the *Core Principles — Compliance
Gate* above**; where a milestone touches that boundary (provisioning, supervision, placement,
admission), the compliant shape is stated inline rather than assumed.

> **Execution plan:** the milestones below are the *canonical design detail*. For the
> workstream grouping, dependency / critical-path graph, completeness matrix, and
> definition-of-done that tie them together, see [`docs/plans/v2.0.md`](docs/plans/v2.0.md).
> This section answers "what is each milestone"; the plan answers "how do they group, in
> what order, and how do we know v2 is done."

1. **Workspace split** — `mycelium-core` extracted from `mycelium` (full substrate).
   Solves the `TaskCtx` God Object and internalises the Layer I/II entanglement.

   > **✅ IMPLEMENTED (2026-06-15, branch `v2/m1-mycelium-core`, pending merge).** The split
   > shipped in six gated stages — execution record + philosophy-compliance sign-off in
   > [`docs/plans/v2-m1-mycelium-core.md`](docs/plans/v2-m1-mycelium-core.md). `mycelium-core`
   > carries the 14 substrate modules + `CoreCtx` and references nothing upper (a compile-time
   > guarantee via the crate boundary); `mycelium` depends on it with an unchanged public API.
   > Dep tree ≈48 vs ≈140 crates. The three core↔upper couplings are mechanism-in-core hooks
   > (`reply_interceptor`, `QuorumObserver`, `SnapshotDeferHook`). This also closes the
   > **Layer I/II entanglement** and **`TaskCtx` God Object** v2 items.

   **Scope decided 2026-06-13: `mycelium-core` = Layers I + II** (gossip transport +
   KV store + signal/boundary mesh), cut at the **II↔III seam**. `mycelium` (full) =
   core + consensus + capabilities + services + schema/llm/mcp + gateway + tls.

   **Why I+II, not Layer I alone.** The "smaller dep tree" payoff comes from
   excluding Layer III, capabilities, services, gateway (axum/reqwest), and tls
   (rustls/rcgen) — *not* from excluding Layer II, which is in-process
   channel/boundary logic adding essentially no external deps over Layer I. So I+II
   captures nearly all the minimalism win while keeping the signal/boundary mesh,
   Mycelium's most differentiated capability. A Layer-I-only core is "just a gossip
   KV library" — and less than one, since it couldn't offer `subscribe` /
   `subscribe_prefix` (implemented through the Layer II bridge). Shipping it would
   require *more* work (severing `apply_and_notify` from the subscription bridge) to
   produce a *worse* artifact; no consumer for KV-without-signals has been identified.

   **The Layer I/II bridge is sanctioned cohesion, not debt.** `KvStore` (pure
   Layer I) holds zero Layer II references; the coupling lives only in `KvState` +
   `apply_and_notify` — the two documented crossing points (see *Layer I/II Bridge
   Invariant*) — and notification is observer fan-out, not the foundation enforcing a
   Layer II law (admission / `Boundary::admits` stays in Layer II). There is no
   dependency inversion and no Core-Principles violation; the entanglement is a
   *cohesion cost* (it is why Modularity rates 8), correctly internalised by drawing
   the crate boundary *around* it at II↔III. This decision therefore **absorbs the
   "Layer I/II entanglement" v2 item** rather than requiring it as a prerequisite.

   **`CoreCtx`** carries the identity, Layer I (KV), Layer II (signal), and
   networking/lifecycle field groups; the full `TaskCtx` keeps the capability,
   service, and security groups — which is where the 22-field God Object naturally
   cleaves.

   **Only if Layer-I-alone ever becomes a goal** (no current consumer): replace the
   concrete `apply_and_notify`→`KvState` coupling with an abstract change-notification
   sink that Layer II registers against, making Layer I genuinely standalone.

   **Trigger to start**: a real embedding use case needing the I+II substrate
   without consensus/capabilities, or sustained friction from the `TaskCtx` coupling
   in handle code. Enables pure-substrate embeds with a much smaller dep tree.
2. **`#[cfg(feature = "consensus")]`** compile-time gate on the epidemic consensus engine. Currently consensus is always compiled; this would let minimal embeds drop the Paxos machinery entirely.

   > **✅ IMPLEMENTED (2026-06-15, branch `v2/m2-consensus-feature`, pending merge).** `consensus`
   > is a default-on feature; `default-features = false` drops the engine **and** the consistency
   > overlay built on it (`consistent_set`/`get`/lock), ~2,200 LOC. Record + the one
   > graceful-degradation point (`suggest_leader` trust-weighting → pure load) in
   > [`docs/plans/v2-m2-consensus-feature.md`](docs/plans/v2-m2-consensus-feature.md). Mixed
   > clusters are fine: a consensus-disabled node still forwards PROPOSE/VOTE/COMMIT (forwarding
   > is in `mycelium-core`), it just never acts. Gate: default 239 + matrix 302 + **no-consensus
   > 196** lib tests, clippy `-D warnings` clean across all combos.
3. **Owned standalone handles**: `KvHandle` / `MeshHandle` / `CapabilitiesHandle` as ownable values that do not require a live `GossipAgent` borrow. Currently handles hold `Arc<TaskCtx>` from a started agent; this would allow passing handles across crate boundaries without exposing `GossipAgent`.
4. **Partial-mesh gossip** — practical cluster ceiling with current design is ~200–400 nodes.

   Today peer-exchange (Pong messages) causes every node to learn about every other node and
   establish a direct TCP connection to each. At N nodes the cluster has O(N²) total TCP
   connections, O(N²) gossip forwarding traffic, and O(N) anti-entropy load per reconnect.
   The 100-node scale test exposed this: seed accumulates ~200 ESTABLISHED connections (99
   inbound from workers + ~99 outbound from peer-exchange) and the Docker bridge iptables
   FORWARD chain — which is also O(N²) in rules — saturates.

   `GOSSIP_PING_PEER_SAMPLE_SIZE` already limits which peers are *pinged* but does not limit
   which peers receive TCP connections. The fix is to make connection maintenance match the
   ping model: each node keeps connections only to a bounded random subset (target fan-out
   `k = O(log N)`) and relies on multi-hop epidemic flooding to propagate writes across the
   rest of the graph. Expected result: O(N·log N) total connections, O(log N) hop diameter,
   bounded per-node memory regardless of cluster size.

   **Trigger to start**: a real workload that needs > 300 nodes, or a benchmark showing
   per-node RSS growing faster than O(1) as cluster size increases.

5. **Hybrid TCP/UDP gossip transport (SWIM-style)** — keep TCP for anti-entropy data
   transfer (where reliability matters); switch gossip pings and capability heartbeats to
   UDP (where loss is tolerable and connection-free is ideal). This is precisely SWIM's
   design: SWIM uses UDP for its periodic direct-ping / indirect-probe cycle and TCP only
   for full state transfer.

   **The structural fix for the iptables problem.** The `GOSSIP_MAX_ACTIVE_CONNECTIONS` cap
   introduced in v1 reduces the O(N²) connection count to O(N×K), which is sufficient for
   ~50–200 node clusters. But the root cause is that *health-check pings* require persistent
   TCP connection state at all. UDP pings carry no connection state; the iptables FORWARD
   chain never needs to track them. Loss on a ping round triggers an indirect probe (ask K
   random peers to ping on your behalf) before marking a node suspect — structurally tolerant
   of a small drop rate, not dependent on zero loss.

   **What changes in the implementation:**
   - `run_health_monitor` sends UDP datagrams to peers in `cached_ping_targets` instead of
     maintaining persistent TCP connections for liveness checks.
   - TCP connections are opened on-demand for anti-entropy (StateRequest/StateResponse) and
     for Data/Signal frame delivery, then closed after the exchange.
   - `get_or_spawn_writer` is retained for bulk data transfer; the health-check path becomes
     fully stateless.

   **Expected outcome:** zero persistent inter-node connections for gossip heartbeats;
   iptables FORWARD chain saturation eliminated at the source; per-node connection-table
   memory drops to O(1). TCP connections during an anti-entropy round: O(N × fanout),
   short-lived, not O(N×K) persistent.

   **Trigger to start**: validated need for > 500-node clusters, or a production deployment
   where `GOSSIP_MAX_ACTIVE_CONNECTIONS` introduces unacceptable topology gaps (e.g. nodes
   with low K miss peers whose keys they need for anti-entropy).

6. **Full gossip-level capability authorization layer** — a v1.x subset of RBAC ships earlier
   (see *Production Readiness Gap §8*): node roles gossiped via `sys/identity/`, gateway endpoint
   ACLs, and the `sys/` namespace-ownership **tripwire** (detection, not a write guard). That
   subset is sufficient for SOC 2 Type II controls evidence and HIPAA §164.312 access control
   requirements.

   The v2 layer goes further — **enforce at resolve/serve, detect at write** (Core Principle 4;
   never a Layer I write guard — that would teach the gossip layer a higher-layer law, the same
   inversion the consensus commit-conflict and `sys/` tripwires deliberately avoid):
   - Gate `resolve_capability` on a cluster-advertised, signed-role ACL (extending WS1's
     verified-roles beyond the per-call `authorized_callers` field in `.skill.toml`, which is
     advisory in v1). This is the **enforcement** point — a consumer ignores capabilities from
     advertisers whose signed role does not satisfy the ACL, emergently and node-locally.
   - **Detect** unauthorized capability advertisements at the write/forwarding path — a `warn!`
     + a cumulative counter on `/stats` (the tripwire idiom), **not** a forwarding-hop block.
     The advertisement still propagates per LWW; consumers simply decline to resolve it. Routing
     around an unauthorized provider is the coordinator-free answer, not preventing its write.
   - Cluster-wide role policy *distributed* via consensus (so policy is consistent), with
     **enforcement remaining at each resolver** — consensus carries the policy, it does not act
     as an admission coordinator.
   The `compliance` Cargo feature is the intended home for this work.
   **Trigger to start**: a regulated-industry deployment where gateway-layer enforcement proves
   insufficient — i.e. agents need resolve-time authz on gossiped capabilities, not just HTTP-level.

7. **Cluster-wide distributed rate-limiting** — `max_inbound_frames_per_sec` applies per-peer
   at each receiver independently; a misbehaving sender can still flood the network by
   connecting to many peers simultaneously. The compliant shape is **shared observation,
   local decision** (Core Principles 4 + 5 — no cluster-wide eviction *verdict*, which would be
   a coordinator enforcing a behavioural judgment):
   - Gossip per-sender frame-rate observations to all nodes via a dedicated `sys/rate/{node}/`
     KV namespace (bounded, short-TTL) — this is *shared evidence*, not a verdict.
   - Each node **independently** tightens its *own* inbound budget for a sender once the
     aggregate observed rate crosses a threshold — emergent backpressure on local evidence, the
     same posture as the existing per-peer limiter, just better-informed. A sustained abuser
     ends up locally throttled by every node it touches, with no global decision round.
   - Disconnection of an abusive peer remains a **node-local** self-defense choice (each node
     drops the connection it sees abused), never a consensus-issued cluster eviction.
   - Expose the rate state via `system_stats()` for operator visibility.
   **Trigger to start**: a confirmed intra-cluster abuse pattern in production (currently
   `max_inbound_frames_per_sec` is sufficient for well-behaved deployments).

8. **Self-tuning metabolism (startup-time auto-derivation)** — all timing and sizing
   parameters in `docs/operations/tuning.md` follow closed-form formulas derived from cluster
   size N. Currently operators must read the tuning guide and set env vars manually.

   The proposal is to add `None` / "auto" sentinels to `GossipConfig` for the formula-driven
   parameters. At `start()`, if a parameter is left as `None`, Mycelium derives the correct
   value from `bootstrap_peers.len()` (a lower-bound estimate of N):

   | Parameter | Auto-derivation |
   |---|---|
   | `default_ttl` | `max(5, ceil(log₂(N + 1)))` |
   | `max_active_connections` | `0` (full mesh) if N ≤ 20, else `max(16, ceil(√N))` |
   | `writer_channel_depth` | `max(1024, N × 4)` |
   | `max_seen_entries` | `max(100_000, N × 1_000)` |
   | `propagation_window_secs` | `max(60, health_check_interval_secs × peer_eviction_intervals × 2)` |

   The hard invariants (`reconnect_backoff < health_check_interval − 2`, propagation ≥ eviction
   window) are enforced during derivation so the resulting config is always valid. Explicit
   operator values override the auto-derived ones without any inference.

   **No consensus needed, no task restarts** — derivation happens once, at `start()`, before
   any background tasks are spawned.

   **Trigger to start**: recurring ops friction from teams deploying clusters of varying size.

9. **Hot-reloadable tuning subset** — a small set of parameters can be changed on a live
   cluster without task restarts because they are sampled on each use rather than at spawn
   time: `max_inbound_frames_per_sec`, `max_concurrent_bulk_handlers`, `writer_channel_depth`.
   Making these `Arc<AtomicU32>` (rather than plain `u32` in `GossipConfig`) allows a
   management agent to gossip recommended values via `sys/config/` and have every node
   self-apply them immediately.

   A `ClusterTuner` agent would be built on existing Mycelium primitives:
   - Observe `peers()` count periodically to track N.
   - Compute recommended parameter values using the same formulas as milestone 8.
   - Write recommendations to `sys/config/{param}` (Layer I KV, short TTL).
   - Each node subscribes to `sys/config/` and applies a recommendation **only if its own local
     policy accepts it** — the tuner *advises*, the node *decides*; application is never
     mandatory. This keeps `ClusterTuner` an advisor, not a config coordinator with standing
     agency over the cluster (Core Principle 1). A node may clamp, ignore, or override any
     recommendation, exactly as explicit operator values override auto-derivation in milestone 8.

   **No new mechanism** — the tuning agent is a regular agent using `kv().subscribe_prefix`
   and atomic config fields. The cluster manages its own metabolism.

   **Trigger to start**: a production deployment where N grows or shrinks significantly
   at runtime (elastic scaling, rolling deploys) and ops confirms milestone 8 is
   insufficient because static startup-time derivation becomes stale.

10. **Full live reconfiguration (coordinated fence)** — parameters that require task restarts
    (`health_check_interval_secs`, `reconnect_backoff_secs`, `peer_eviction_intervals`) need a
    coordinated fence: reach consensus on the new config version, drain in-flight operations,
    restart affected background tasks, confirm all nodes applied the change. This is the full
    "self-tuning metabolism" vision — the cluster responds to topology changes as they happen,
    not just at startup.

    **Complexity is significant:** partial rollout (some nodes on old interval, some on new)
    creates a window where the backoff invariant can be violated; the fence must be atomic
    cluster-wide or rolled back. Defer until there is a validated production need that
    milestones 8 and 9 cannot address.

    **Trigger to start**: a deployment with highly variable cluster size (e.g. 10 → 500 nodes
    in a single session) where startup-time derivation and hot-reloadable parameters are
    demonstrably insufficient.

11. **Wire-codec succession (bincode replacement)** — `bincode` is the serializer behind
    every wire frame and is officially unmaintained (RUSTSEC-2025-0141; surfaced by the
    CI `cargo audit` job as a permanent warning). There is no immediate risk: the crate
    is pure-Rust, `#![deny(unsafe_code)]`-compatible in our usage, pinned by `Cargo.lock`,
    and the wire format is already frozen by the `WIRE_VERSION` policy — but an
    unmaintained codec in the trust base of a security-marketed substrate is a liability
    that compounds (no fixes if an advisory lands; no upgrades as the Rust ecosystem moves).

    **Options evaluated (2026-06-11):**
    - *Stay + vendor on demand*: zero cost today; fork only if an advisory lands. Viable
      short-term posture, which is why this is a v2 item and not a v1.x hotfix.
    - *postcard / borsh / speedy*: maintained codecs, but each changes the byte layout
      (varint vs fixed-int, different enum tagging) — a full wire break for a third-party
      dependency we'd still not control.
    - *Hand-rolled fixed-layout codec* (**recommended**): `WireMessage` is a small, closed
      enum whose layout we already micro-manage (v6 reordered fields specifically to enable
      in-place TTL decrement and zero-copy forwarding; framing already hand-builds the
      header). A ~300-line explicit encoder/decoder eliminates the unmaintained dependency,
      makes the wire layout a first-class artifact instead of an emergent property of a
      codec's settings, and pairs naturally with the existing fuzz targets.

    **Plan**: implement at the next wire-version bump (v12) so the break rides an already-
    open rolling-upgrade window rather than forcing one. Until then the audit-job warning
    is the tracked reminder.

    **Trigger to start**: the next planned WIRE_VERSION bump, or any RUSTSEC advisory
    against bincode 2.x beyond "unmaintained" — whichever comes first.

12. **WASM components as the bundle analogue (runtime code mobility)** — Mycelium kept
    two of the three things OSGi bundled together: continuous R&C *resolution* (the
    resolver re-evaluates on every relevant KV change) and a *dynamic advertised set*
    per node (`advertise_capability` / handle drop / evaporation). The third — code
    mobility, the install/update/uninstall lifecycle inside a running process — was
    deliberately delegated to the orchestrator layer: "install new code" means
    "schedule a new node," and the resolver wires it in on first heartbeat. That is the
    right v1 trade (Rust has no safe in-process loading story; `dlopen` has neither a
    stable ABI nor a sandbox), but it leaves a gap for fleets that need new *logic* —
    not just new prompt templates — without a container redeploy cycle.

    The WASM component model is the classloader analogue Rust natively lacks:
    sandboxed, ABI-stable (WIT interfaces), capability-scoped imports, fuel/memory
    limits. And the substrate hooks already exist — this milestone is plumbing, not
    new mechanism:

    - `cap/{node}/{ns}/installable` → `cap/{node}/{ns}/loading` (progress 0–100) →
      live capability: the provisioning state machine, already gossip-visible.
    - `agent/{node}/provision/{item}/error` for failed installs.
    - Bulk transport for module-byte distribution (content-addressed; the KV carries
      the hash + source reference, not the bytes).
    - Schema registry + `with_schema_id` for typed invocation compatibility.
    - Ed25519 node identity (`tls` feature) extends naturally to signed module
      hashes — no unsigned code on the mesh.

    **Shape**: a `mycelium-wasm-host` companion crate built entirely on the public API
    (the same composability proof as `mycelium-tuple-space`): hosts wasmtime, watches a
    `wasm/skills/{ns}/{name}` KV prefix, pulls + verifies + instantiates the component,
    advertises the capability, and routes invoke RPCs into it. Unload = handle drop =
    tombstone + evaporation. The resolver does not change — a WASM-backed provider is
    indistinguishable from a compiled-in one, which is the point.

    **Trigger to start**: the first real fleet that needs runtime-installable *logic*
    where data-driven skills (prompt templates in KV, MCP tool fronting) are not
    expressive enough — or an embedded deployment with no orchestrator above it to
    delegate code mobility to.

13. **Keyed-exact-match `take` on the tuple space (fan-in joins)** — Paper 1 §9.4
    (DOI [10.5281/zenodo.20665238](https://doi.org/10.5281/zenodo.20665238)) states
    this "is roadmapped"; this entry is the contract behind that sentence.
    `put` gains an optional correlation key and `take_by_key(stage, key)` claims
    the item on `stage` whose key matches, parking a *keyed* waiter when absent —
    the two-stream rendezvous ("an invoice AND its matching purchase order") that
    exactly-named lanes cannot express without degenerating to one lane per
    correlation key. Scope is deliberately **exact-match only**: a hash lookup,
    O(1), with per-lane depth/backpressure accounting intact — not template
    matching, which remains the blackboard companion's territory (Deferred
    Patterns below).

    **Implementation notes** (sized at a focused day, not an afternoon — the WAL
    is the real work): optional key field on the `Put` WAL record ⇒ WAL format
    v2 with v1 replay accepted; per-`StageState` keyed index + keyed-waiter map
    alongside the FIFO; key carried on secondary replication and through
    promotion replay; `complete` accepting a key for the next stage; gateway
    endpoint + py/ts SDK methods; regression tests for the join rendezvous and
    the crash-requeue of a keyed in-flight item.

    **Trigger to start**: the first real fan-in pipeline, or follow-up work
    exercising Paper 1 §9.4's boundary claims empirically.

14. **Substrate-native supervision (capability-presence invariants)** — the v1
    supervision primitive (*Layer 4 — Supervision* above) is the building block:
    Layer 2 `watch()` on `contract.available` heartbeats with an
    `on_stale(kind, threshold, callback)` hook. That gives an *application* a
    callback; it does not give the *mesh* a coordinator-free recovery discipline.
    This milestone generalizes it into declarative supervision that honors the
    Layer I/II/III boundaries end to end.

    **The reframe.** Akka supervises *objects* — a parent actor decides to restart
    a child. That is a coordinator, and a tree of them; it contradicts the
    no-coordinator thesis directly. The substrate-native unit is not an object but
    a **capability-presence invariant**: "role X must always have ≥1 fresh
    provider." A supervisor is then not a node with authority over others — it is a
    *policy* that every eligible peer evaluates independently against shared
    evaporation state, taking over when the invariant is violated. The
    proof-of-concept already ships: the TupleSpace `Secondary` watching the
    `Primary`'s capability key and promoting on evaporation — *the ring IS the
    failure detector*. This milestone lifts that one hardcoded role into a general,
    declarative mechanism.

    **Mechanism mapping** — every Akka supervision concept resolves to an existing
    primitive, no new transport:

    | Akka | Substrate-native | Built on |
    |---|---|---|
    | Failure detection | capability evaporation (stale past 3× `refresh_interval_ms`) | `CapEntry::is_fresh` |
    | The supervisor | emergent self-election + deterministic tie-break | `join_group` self-eval; TupleSpace `Auto` lowest-id |
    | Restart (fresh state) | successor re-acquires the role, starts clean | role/lease key + re-advertise |
    | Resume (keep state) | successor reads durable state from Layer I, drains the durable mailbox | KV snapshot + `mailbox.rs` |
    | Stop | let the capability evaporate, don't re-acquire | absence of a refresh |
    | Escalate | widen scope along the Layer III ladder | `group_propose` → `system_propose` → `cross_group_propose` |
    | Containment scope (the tree) | signal admission boundary / group scope (flat, nested) | `Boundary::admits` |
    | OneForOne / AllForOne | per-role recovery / coordinated restart at an agreed epoch | former free; latter a consensus round |

    **Leased consensus IS the supervision lease.** For singleton roles where
    double-promotion is catastrophic, the successor wins via `group_propose`
    holding a `committed_lease_secs` lease and renews by re-proposing the same
    value while live; if the successor *also* dies the lease evaporates read-side
    and the slot reopens — recovery-of-the-recoverer falls out of a mechanism that
    already exists, with no background task.

    **Layer decomposition:**
    - **Layer I**: durable policy specs, role/lease keys, recovered-state snapshot,
      mailbox backlog — under an owned prefix (e.g. `sup/role/{role}/...`, or folded
      into the `sys/load/` pheromone space). Failure detection is a *read-side
      freshness convention*, exactly like opacity and lease expiry; Layer I learns
      no supervision law — it is "just new keys."
    - **Layer II**: ephemeral coordination — heartbeats, "claiming role X at epoch
      E" announcements, the promotion watch — admission-scoped so only the relevant
      group sees them. (The generalization of the TupleSpace heartbeat Signal.)
    - **Layer III**: contested succession *only* — correctness-critical singletons.
      Stateless/idempotent roles resolve at Layers I+II via self-election and never
      touch consensus.

    **Compliance with the established invariants:**
    - *Detection, not prevention.* No coordinator can forcibly kill a remote agent,
      and the substrate does not enforce prefixes; a "stopped" agent that keeps
      writing is *detected* (a tripwire, like the commit-conflict tripwire) and
      routed around via evaporation — never prevented. House style, preserved.
    - *Lock-free claim discipline.* "Claim the role" is a conditional papaya
      `compute` (claim-by-sentinel, spawn outside the closure — the
      `get_or_spawn_writer` pattern), never check-then-act; split-brain promotion is
      exactly the "lock-free op followed by an unserialised derived effect" race the
      existing rules already close.
    - *Opacity composition.* An overloaded provider already writes `is_opaque`;
      supervision *reads* it — an opaque provider is a takeover candidate. No
      parallel health model ("new causes = new keys").
    - *Emergent groups.* The supervisor set for a role *is* a `CapabilityGroupDef` —
      nodes able to provide X self-join; nobody assigns supervisor duty.

    **Three apparent limits — none are show-stoppers:**
    - *Forcible stop/kill of a remote agent* is not a substrate gap: no system can
      guarantee a remote process stops across a partition (Akka's `context.stop` is a
      *local* guarantee; remote death-watch is best-effort). Local stop is handle drop
      / task cancel; the remote case wants **fencing**, and the leased epoch + tripwire
      already *is* a fencing token — a stale-epoch writer is rejected the same way the
      commit-conflict tripwire rejects a divergent COMMIT. The route-around-on-
      evaporation behaviour is the correct distributed answer, not a concession.
    - *Atomic AllForOne group-restart* (coordinated restart at a shared epoch) is a
      consensus problem by definition; it is expressible via a `group_propose` round
      today, the cost is intrinsic to the semantics rather than a substrate deficiency,
      and it is the rarely-used supervision strategy anyway.
    - *Exactly-once* splits in two. Exactly-once *delivery* on the wire is universally
      impossible (retry ⇒ at-least-once) and nothing needs it. Exactly-once *effects*
      is achievable and **the engine already ships inside the TupleSpace** —
      `tuple/inflight/{ns}/{id}` advisory claim + the indivisible `Complete` WAL record
      (a stage transition can never half-replay) + crash-requeue + the `id → stage`
      dedup map. The work is to *extract* that discipline as a reusable **dedup-ledger
      overlay**: deterministic dedup key → claim-by-sentinel on `dedup/{handler}/{key}`
      (the same `compute` primitive as `get_or_spawn_writer` and the role claim above)
      → idempotent effect → result-under-key; an in-flight claim left by a dead node
      expires read-side and requeues, safe because the indivisible complete means a
      half-applied effect never recorded completion. This upgrades the at-least-once
      *Reliable Delivery* overlay (above) to exactly-once-effect, and it is the same
      primitive the `mycelium-blackboard` Deferred Pattern names as "the tuple space's
      WAL/in-flight exactly-once discipline" — build it once, share it across the
      blackboard, the mailbox, and supervision restart.

    **Minimal first cut** (~1–2 months, policy plumbing only): a
    `SupervisionPolicy` declarative spec attached to a role (shaped like
    `CapabilityGroupDef`); a watcher reusing the existing evaporation read-side
    check; self-election with the `Auto` tie-break for non-singleton roles; leased
    `group_propose` reserved for the singleton case. No new transport, no
    coordinator.

    **Trigger to start**: the first fleet that needs autonomous agent recovery
    beyond the v1 `on_stale` callback — i.e. the mesh must re-provision a failed
    role without an external orchestrator — or follow-up work generalizing the
    TupleSpace Primary/Secondary failover into a reusable pattern.

15. **OBR-style resolve-from-installable-catalog (autonomic provisioning)** —
    milestone 12 adds the *install mechanism* (pull a WASM component by name,
    instantiate, advertise). This milestone adds the *selection* step that OSGi's
    Bundle Repository resolver provides and Mycelium does not have yet: given a
    requirement, match it against a **catalog of installable artifacts** and choose
    which to pull — rather than pulling by hard-coded name.

    **What this closes.** Mycelium kept OSGi's continuous R&C resolution but pointed
    it at the **running** set — `resolve` / `resolve_with_locality` /
    `resolve_filter_against_kv` match a `CapFilter` against *live, advertised* `cap/`
    + `gcap/` entries. OSGi's OBR resolver instead matches a requirement against a
    *repository of not-yet-installed artifacts* annotated with the capabilities they
    would provide, and produces a deployment set. That resolve-from-catalog step is
    the missing fourth piece — after resolution, the dynamic advertised set, and
    milestone 12's code mobility.

    **What already exists (verified in tree):**
    - `cap/{node}/{ns}/installable` and `cap/{node}/{ns}/loading` are documented KV
      namespace conventions (`lib.rs` ownership table), with a concrete user today:
      the LLM provisioning path (`cap/{node}/llm/installable` carries
      model/size_gb/est_mins; `.../loading` carries progress 0–100).
    - `agent/{node}/provision/{item}/error` for failures — but **written by the
      application provisioning handler, not the substrate**. There is no
      substrate-driven `installable → loading → live` state machine; provisioning is
      application-layer by design.
    - `req/{node}/{ns}/{name}` declares a requirement; `demand_snapshot` (`demand.rs`)
      derives demand pressure from `req/` + `cap/` + `gcap/` as a **read-only view**.
      The library *never auto-advertises in response to demand* — that is explicitly
      an application-layer decision (orchestrators, autoscalers).
    - Content-addressed bulk transport for module bytes; `advertise_capability` to go
      live.

    **What's new (the gap):**
    1. **Declared-provide metadata on catalog entries.** An `installable` entry today
       is keyed by the reserved name with detail in attributes — it does not state,
       in a resolver-matchable form, *the capability it would provide once
       installed*. OBR resolve needs each catalog entry to carry its prospective
       `(ns, name)` provide (and ideally its requires). For WASM this is free: a
       component's **WIT exports are its provides and imports are its requires** —
       read them off the artifact (or its Warg / OCI registry metadata) rather than
       hand-authoring.
    2. **A resolve pass over the installable catalog** — the same `CapFilter` matching
       the live resolver already does, scoped to the declared-provides of
       `installable` entries instead of live `cap/` / `gcap/`.
    3. **Install-time dependency resolution — one hop, not a constraint solver
       (design contract).** Separate two things OSGi conflates. *Service/capability
       dependencies* ("skill A calls skill B") are **already resolved at runtime
       across the mesh** by the live resolver, re-resolved on every relevant KV change
       — A emits to whoever provides B, possibly on another node; this is the
       transitive part, and it is *not* frozen into a deployment set. *Install-time
       artifact dependencies* ("what code must this node pull to bring cap A live") are
       the only thing this milestone resolves, and they bottom out fast: a WASM
       component's WIT imports are satisfied either from **host-provided
       capability-scoped imports** (depth 0) or, at most, **one hop** to a directly
       required component. The rule: *install-time resolution stops at the component
       plus its host-satisfied imports; anything that is a call to another capability
       is runtime-mesh-resolved, not install-resolved.* Going deeper into a transitive
       *install* closure is a smell — it link-time-binds what the mesh already resolves
       at call time. This deliberately **declines OSGi's genuinely hard part** (version
       ranges, uses-constraints, NP-hard SAT) because the mesh dissolved it:
       schema-version compatibility is checked per-hop at the capability boundary (the
       existing `with_schema` filter), never solved globally. The shallow matcher is
       not a lesser resolver — it is the architecture refusing to import a problem it
       doesn't have. For any case that genuinely needs deep artifact closure, delegate
       to the WASM toolchain's own resolver (`wkg` / Warg) rather than reimplementing a
       SAT solver.
    4. **A provisioner agent that closes the loop** — watches `req/` /
       `demand_snapshot`, resolves unmet requirements against the catalog, pulls +
       verifies + instantiates via milestone 12, and lets the new capability advertise
       itself. **This is an application-layer agent** (the same shape as milestone 9's
       `ClusterTuner` and milestone 12's `mycelium-wasm-host` — a regular agent on the
       public API), *not* a substrate mechanism: the "library never auto-provisions"
       invariant stays intact, and provisioning is emergent — any provisioner
       independently resolves demand; no coordinator assigns provisioning duty.

    **Invariant (Core Principle 1 — no coordinator).** The provisioner never migrates into the
    substrate. A Layer-I mechanism that watches demand and pulls-and-runs on a node's behalf is a
    coordinator deciding placement *without* the node's local knowledge — it reintroduces the trap
    the architecture refutes and invalidates the coordinator-free claim wholesale. The substrate
    contributes only the primitives (`req/`, `demand_snapshot`, the `installable` convention, bulk
    transport, `advertise_capability`); the agency to pull and run is always the node's own local
    choice, expressed as an app-layer agent — generalizing `demand.rs`'s existing "the library never
    auto-advertises in response to demand" stance.

    **The autonomic loop:** declared requirement (`req/`) → observable demand
    (`demand_snapshot`) → resolve against the `installable` catalog (new) → pull +
    verify + instantiate (milestone 12) → capability advertises (`cap/`) → demand
    relieved. Pairs naturally with milestone 14: a supervised role whose provider
    evaporated is just an unmet requirement the provisioner can re-satisfy from the
    catalog — supervision *restart* and first-time *provisioning* collapse onto the
    same resolve-and-pull path.

    **Dependency**: milestone 12 (the install mechanism) is the layer below; this is
    the selection layer above it. Without 12, resolve-from-catalog has nothing to pull.

    **Trigger to start**: a fleet where capabilities must be provisioned *by need*
    rather than pre-placed — nodes should acquire what unmet requirements call for
    without an operator choosing artifacts by name — or once milestone 12 ships and
    name-addressed pulls prove too manual.

16. **NANDA interop — AgentFacts emission + a CRDT AgentFacts update layer, the
    Mycelium way** — make a Mycelium domain a first-class **sovereign patch** in a
    NANDA-style agent-discovery quilt (the "Internet of AI Agents" federation: NANDA
    index + Verified AgentFacts, arXiv 2507.14263, **v0.3 RFC — treat as aspirational
    and moving**). Two deliverables, one philosophy.

    **Positioning (verified against the paper).** NANDA is a *federated quilt* of
    registries that holds redirects to sovereign patches it does **not** govern
    (*"NANDA need not authenticate, authorize and govern all agents"*). Its trust model
    offers two modes — *issuer-attested* (VC signed by credential authorities + Trust
    Reputation Scores + federated trust zones) and *self-certified via DID*. Mycelium
    adopts **self-certified only**: the issuer/TRS authority is a P-axis chokepoint and
    a coordinator by another name, so it is out of scope by **Core Principle 1**. A
    domain federates *at the edge* and self-elects whether to publish at all (run-dark
    is the default — the scale-invariant boundary again).

    **Deliverable A — AgentFacts emission (edge / inter-domain; PULL).** Emit a
    self-signed AgentFacts document (JSON-LD, Ed25519-signed by the node identity) at
    the gateway edge, as a superset of the A2A Agent Card Mycelium already serves at
    `/.well-known/agent.json` (the paper states AgentFacts *"can be viewed as a superset
    of the Agent Card"*). Most fields already exist in the tree — this is mapping, not
    new mechanism:

    | AgentFacts field | Existing Mycelium source |
    |---|---|
    | `capabilities` / `skills` (+ schemas) | capability `ns`/`name` + `schema_id` + input/output schemas |
    | `endpoints.adaptive_resolver` (geo / load routing) | `resolve_with_locality` + `signal_wired_via_locality` + emergent groups |
    | `jurisdiction` / locality | `locality_path` |
    | `telemetry` / `evaluations` (self-reported) | `/metrics` + `system_stats()` + `sys/load/` opacity pheromones |
    | `certification` (self-certified) | `tls` Ed25519 node identity + `WireMessage::SignedData` |
    | endpoint TTLs | capability refresh interval + evaporation convention |

    Expose a TTL-scoped `facts_url` so the inter-domain quilt **pulls** at the boundary.

    **Deliverable B — the CRDT AgentFacts update layer (intra-domain; PUSH).** The NANDA
    abstract names a *"CRDT-based update protocol"* that the v0.3 body does not deliver —
    it falls back to whole-document VC + host-at-URL + TTL re-fetch, because
    **whole-document VC-signing is in tension with field-level CRDT merge** (you cannot
    merge two independently-signed documents field-by-field and preserve either
    signature). Mycelium's substrate *is* that missing protocol: **LWW + HLC +
    anti-entropy** is a convergent, concurrent-safe, decentralised update mechanism, and
    **per-entry `SignedData`** (not whole-doc) is exactly the precondition that makes
    field-level merge possible — each AgentFacts field is an independently-signed KV
    write that LWW-merges by HLC. So intra-domain, gossip per-field-signed AgentFacts
    into the mesh; concurrent edits converge; late joiners catch up via anti-entropy;
    freshness is the evaporation convention. **Push internally, pull at the edge.** No
    new transport — the existing Layer I substrate carrying a new payload shape; the
    per-entry-signature granularity makes Mycelium *better-suited* to a CRDT AgentFacts
    than NANDA's own whole-doc VC.

    **Compliance (Core Principles).** Self-certified only (no issuer/TRS authority —
    Principle 1). Emission is opt-in and boundary-gated — the domain self-elects
    federation, run-dark by default. The CRDT layer reuses the existing
    LWW/HLC/anti-entropy substrate (no new mechanism; the fast path is untouched). Ships
    as a **companion crate** (`mycelium-agentfacts`) on the public API, not core bloat —
    the same composability proof as `mycelium-tuple-space`.

    **v1.x precursor dependency.** The credibility of self-certified `evaluations`/`telemetry`
    rests on the v1.x tamper-evident **audit trail** (Production Readiness gap #7): it backs
    self-asserted claims with content-hashed provenance — *self-attested-with-audit*, the
    coordinator-free answer to NANDA's "audited not self-advertised." Capability-assertion authz
    (gap #8) hardens *who* may assert a capability into the emitted facts. The forward-compatibility
    acceptance criteria for both live under *Production Readiness Gap → Gap Summary* — build to the
    stable substrate shape, not this milestone's (moving) AgentFacts surface.

    **Out of scope / honest gaps.** *Third-party attestation* (issuer-VC + audited
    `evaluations` + TRS) — deliberately not adopted; it is the coordinator-shaped trust
    mode. *Requester-privacy* (`PrivateFactsURL` via IPFS/Tor that hides *who is asking*)
    — no within-mesh analog; a privacy gateway could add it later, out of scope
    initially. *Agent-to-agent settlement/payment* — absent from NANDA and from this
    milestone (a separate, still-open agent-stack layer). The paper itself floats
    renaming AgentFacts → "Agent Metadata Layer / AgentFacts v2"; build against the
    stable surface (A2A superset, VC signing, TTL) and treat trust-framework specifics
    as moving.

    **Trigger to start**: a real need to make a domain's agents discoverable across a
    NANDA-style quilt (or another agent-internet registry), or interop demand from an
    A2A/NANDA ecosystem partner. Until then the existing A2A edge already covers
    single-marketplace discovery.

### NANDA Registry-Quilt deep dive (2026-06-15) — technique-transfer candidates to weigh before starting M16 / M5

A deeper read of NANDA's *Registry Quilt internals* (Lambe, "Deep Dive Part 4: The
Registry Quilt — Gossip, CRDTs, and Cross-Signing", alongside arXiv 2507.14263 +
enterprise paper 2508.03101) confirmed the quilt-patch positioning **and** surfaced four
concrete techniques worth deciding on *before* M16 (NANDA interop) or M5 (SWIM transport)
begin. NANDA's Quilt is itself a coordinator-free gossip+CRDT substrate (SWIM membership +
delta gossip + Merkle anti-entropy + OR-Map CRDT + Ed25519 cross-signing + CT-style
transparency log) — which both **validates** Mycelium's design convergently (cite as
prior-art corroboration in Paper 2) and offers borrowable mechanism. The discipline stays:
NANDA owns inter-domain discovery/identity; Mycelium owns the intra-domain substrate NANDA
explicitly defers to "layers below" — **never let Mycelium's gossip try to *be* the Quilt**
(different scope: intra-domain trust-homogeneous sub-second vs cross-org Byzantine 60s SLO).

Ranked by relevance:

1. **Merkle-tree anti-entropy (descend only where hashes differ).** *Net-new; feeds M4/M5
   and the scale-entries tail metric.* Verified 2026-06-15: the current sweep is **not**
   Merkle-based — `WireMessage::StateRequest` ships the full `key_timestamps:
   Vec<(Arc<str>, u64)>` index every probe (O(N) keys on the wire per round) on top of the
   v7 whole-store `store_hash` fast-path skip. NANDA does Dynamo-style per-shard Merkle
   reconciliation: exchange Merkle roots, descend only divergent subtrees, fetch only
   missing leaves. Adopting it bounds bytes-on-wire to O(divergence) instead of O(store
   size) and directly improves the `make test-scale-entries` anti-entropy sweep-tail. Rides
   a future `WIRE_VERSION` bump (pairs naturally with the M11 codec succession at v12) since
   `StateRequest`'s shape changes. **Decision to make before M5:** whether the SWIM rework
   and the Merkle digest land together (both touch the anti-entropy path).

2. **CT-style transparency log with inclusion proofs for key events.** *Net-new; the single
   most federation-aligned item; depends on / extends WS5 + WS2.* WS5's retained-key-set
   rotation accepts a retired key until *explicit* revocation (documented caveat), and the
   WS2 audit chain is **per-node**. A CT-style append-only log of key/rotation/revocation
   events with client-checkable inclusion proofs would (a) deliver the "sub-second
   revocation" property NANDA advertises, (b) close the WS5 "compromise needs explicit
   revocation" gap, and (c) make a Mycelium domain a trustworthy quilt patch without
   adopting NANDA's issuer/TRS authority (which Core Principle 1 rules out). Must stay
   coordinator-free: the log is per-domain and gossip-replicated (a new owned KV namespace +
   inclusion-proof verification on read), **not** a central CT operator. Strategically this
   is the precursor that most strengthens M16's self-certified `certification`/`evaluations`
   credibility.

3. **OR-Map (observed-remove) CRDT for the capability/registry projection specifically.**
   *Refines M16 Deliverable B; design note, not a rewrite.* NANDA models each AgentRecord as
   an OR-Map CRDT with vector clocks; Mycelium uses LWW+HLC+tombstones. For `gcap/` /
   capability advertisements — where concurrent add/remove from many nodes is the norm —
   observed-remove semantics handle the add/remove race more cleanly than LWW-with-tombstones
   (no "remove loses to a concurrent re-add it never saw" surprise). **Not** a wholesale
   replacement of the LWW+HLC substrate (M16 Deliverable B already argues per-entry
   `SignedData` + LWW is *better-suited* than NANDA's whole-doc VC for field-level merge);
   this is a scoped evaluation of whether the cap/registry projection alone benefits from
   OR-Map merge semantics layered on top. Capture as a design note before M16, decide during.

4. **AgentFacts emission as the upward contract.** *Already specified — this is M16
   Deliverable A.* Recorded here only to close the loop: the deep dive reconfirmed AgentFacts
   ≈ A2A AgentCard + `facts_url` + DID + signed VC capability assertions, and that Mycelium's
   existing `/.well-known/agent.json` edge is most of the work. No change to M16 Deliverable A;
   the identity gap (cluster-internal Ed25519 `NodeId` → a domain DID) is the bridging glue to
   scope when M16 starts.

### Schema-registry evolution — runtime schema migration, the Mycelium way (v2; pairs with M16)

BRAIN-IoT's Event Bus (Tim Ward's OSGi Type-Safe Events, rfc-0244, contributed to Apache
Aries) offered *"basic schema transformation"* so a publisher and subscriber compiled against
different schema versions still interoperate. Mycelium has the ingredients —
gossip-distributed schemas (`publish_schema` / `get_schema`), cap/skill input/output schemas
(`with_input_schema` / `with_output_schema`), schema-version-aware filtering
(`with_schema_id`) — but **not** the *transformation/migration execution* step on the
signal/RPC delivery path. Scoped in three tiers by cost and house-style fit:

1. **Additive tolerance** (new optional fields ignored, missing fields defaulted) — *largely
   already true* on the JSON payload paths (gateway, A2A, prompt skills) via serde
   defaults / ignore-unknown; wire frames are bincode but skill payloads are JSON.
   **Action: verify + document the property, not a milestone.**
2. **Compatibility detection** — schema mismatch ⇒ `warn!` + a `schema_mismatch` counter on
   `/stats`, exactly the existing tripwire idiom (`commit_conflicts`,
   `sys_namespace_violations`). Detection-not-prevention. Cheap; add whenever schema drift
   first bites.
3. **Registered migrations** (renames, type coercions, cross-version mapping) — the real
   feature, and a **v2 aspiration**. The Mycelium-idiomatic reframe: **explicit, registered,
   gossip-distributed migration functions** (`vN → vN+1` transforms published into the schema
   registry *alongside* the schemas), composed `v1→v2→v3` on the receive path — **never
   silent best-effort coercion**, which would mask real incompatibilities and violate the
   explicit-contract / detection-not-prevention posture. When no migration path exists,
   detect (tier 2), do not guess.

**Placement:** a sub-item of the schema-registry work, riding alongside **M16** — whose
evolvable, semantically-versioned JSON-LD AgentFacts wants exactly this migration machinery —
not a standalone marquee milestone.

**Trigger to start:** first real cross-version schema drift in production, or M16's AgentFacts
evolution needing field-level migration.

---

## Deferred Patterns

These are well-designed ideas that were evaluated and deliberately shelved — not because
they are wrong, but because they should be driven by real production demand rather than
built speculatively. Full design documents are in `docs/plans/`.

| Pattern | File | Trigger to revisit |
|---|---|---|
| ~~`mycelium-tuple-space` companion crate~~ — **shipped 2026-06-11** as workspace member `mycelium-tuple-space/` (all 5 phases: core store + WAL, primary/secondary failover + Auto election, monitoring + backpressure pheromone, HTTP gateway + py/ts SDKs, integration scenario 13). Trigger was re-evaluated: the pull-based model is the load-bearing empirical artifact for Paper 2a's pull-vs-push reframing, not just an AFN throughput escape hatch. | [`docs/plans/mycelium-tuple-space.md`](docs/plans/mycelium-tuple-space.md) | — |
| `mycelium-blackboard` companion crate — opportunistic shared-working-memory coordination (LLM-agent scratchpads = the blackboard reborn) on the public API: facts as KV writes, trigger predicates as boundaries, plus **one** new primitive — competitive destructive claim-by-predicate with the tuple space's WAL/in-flight exactly-once discipline. Covers the blackboard + contract-net rows of the lane model's boundary (Paper 1 §9.4, philosophy.html "Where associative matching earns its keep"); fan-in joins are deliberately *excluded* (a keyed-exact-match `take` on the tuple space covers them). Would extend the Paper 1 §8 constructive proof to associative-claiming workloads on the same no-coordinator terms. | [`docs/plans/mycelium-blackboard.md`](docs/plans/mycelium-blackboard.md) | Real demand for emergent-topology reasoning over shared facts — a team about to build an ad-hoc claim/poll/match store on the KV — or follow-up work extending the §8 proof |

