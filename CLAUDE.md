# CLAUDE.md — Mycelium quick-reference for future code-assistant sessions

This file is a fast on-ramp for code-assistant tools (and humans new
to the repo). It points at the canonical architecture documents
rather than duplicating them.

## What this is

Mycelium is an embedded, broker-less Rust library that provides a
three-layer substrate for AI agent fleets and storage replication:

| Layer | What it does | Where it lives |
|---|---|---|
| **I — KV store** | Last-write-wins state propagation over TCP; anti-entropy synced. Two distinct "TTL"s — don't conflate: wire frames carry a **hop-count TTL** (`u8`, decremented per forward); key **evaporation** is a *read-side convention* (entries carry `refresh_interval_ms`; readers discard entries older than 3× — `CapEntry::is_fresh` — and, symmetrically, entries stamped further than 3× in the *future*, so a writer with a far-ahead clock quarantines itself instead of becoming un-evaporable; `Hlc::observe` additionally clamps remote drift to `max_clock_drift_ms`, default 5 min). The store never time-evicts live keys; only tombstones are GC'd. | `src/store.rs`, `src/connection.rs`, `src/framing.rs`, `src/writer.rs`, `src/seen.rs` |
| **II — Signal mesh** | Ephemeral scoped events with per-node admission boundaries; pheromone-style opacity composition. | `src/signal.rs`, `src/agent/mesh_handle.rs`, `src/agent/opacity.rs` |
| **III — Consensus** | Epidemic group / system / cross-group proposals with optional Hard topology enforcement. `GroupQuorum` + `cross_group_propose` for multi-voting-bloc decisions. | `src/consensus.rs`, `src/agent/consensus_ops.rs` |
| **Security (tls feature)** | mTLS transport, Ed25519 node identity, signed consensus payloads. | `src/tls.rs`, `src/stream.rs` |

Plus a capability / requirement subsystem with emergent groups, inter-group
wiring, locality-aware resolution, ranking, group-level opacity, and demand
pressure — see [`src/capability.rs`](src/capability.rs) and the four
files in `src/agent/`:
[`capability_ops.rs`](src/agent/capability_ops.rs) (node-level cap/req API
+ shared helpers), [`wiring.rs`](src/agent/wiring.rs) (Phase 4/5/6),
[`emergent_groups.rs`](src/agent/emergent_groups.rs) (Phase 3g/3h/7),
[`demand.rs`](src/agent/demand.rs) (Phase 9).

And Hybrid Logical Clocks for causal LWW ordering: [`src/hlc.rs`](src/hlc.rs).

## Where to read what

| For | Read |
|---|---|
| The library's public API + overall pitch | `src/lib.rs` crate doc-comment + [`README.md`](README.md) |
| The KV-namespace ownership table | `src/lib.rs` crate doc-comment (after the Quick Start) |
| The three-layer model and roadmap | [`ROADMAP.md`](ROADMAP.md) |
| Wire format + version negotiation | `src/framing.rs` (`WIRE_VERSION` policy at the top) |
| HLC design + documented limits | `src/hlc.rs` module doc |
| Capability/requirement model | `src/capability.rs` |
| Example guide (concept → run → dev notes) | [`docs/guide/README.md`](docs/guide/README.md) |

## Core design rules to keep in mind

1. **Single KV substrate.** Higher layers own dedicated key prefixes
   and write directly via `make_gossip_update` + `apply_and_notify`
   (see the namespace table in `src/lib.rs`). This is documented; not
   a layer violation.

2. **Opacity composition.** Any reason a node is opaque writes a
   distinct key under `sys/load/{self}/...` with `is_opaque = true`.
   `is_self_opaque()` scans the whole prefix and returns true if
   *any* entry is opaque. Adding new opacity causes doesn't require
   new mechanism — just new keys.

3. **HLC ordering.** Every locally-originated update gets a timestamp
   from `hlc.tick()`. Every received update is observed via
   `hlc.observe(remote_ts)` so any local write after a remote
   observation has a strictly greater timestamp — preserves causal
   happens-before under wall-clock skew. LWW comparison is still
   `>` on the packed `u64`.

4. **Emergent groups.** A `CapabilityGroupDef` defines a filter +
   optional topology policy + `provides` + `requires`. Each node
   independently evaluates whether it should self-join via
   `join_group(name)` based on its own capabilities. No coordinator
   assigns membership. Provides projected as `gcap/{group}/...`;
   unsatisfied requires write `sys/load/{self}/group-req/{group}/{idx}`.

5. **Inter-group wiring is per-emission.** `signal_wired_via(filter)`
   resolves wiring at the moment of the call. There is no stored
   binding; re-wiring is implicit because each call re-resolves.

6. **TLS is opt-in and transport-only.** `GossipConfig::tls = Some(TlsConfig::default())`
   enables mTLS on the gossip TCP port. The same Ed25519 keypair is reused for identity
   (`sys/identity/{node}`) and consensus signing (`SignedConsensusMsg`). Without the `tls`
   feature flag, all TLS code compiles away and behaviour is unchanged. `NodeTls` is always
   defined (zero-size without the feature) so function signatures stay uniform.

## Active follow-up plans (memory)

These are real work items. Anyone resuming should read
[`MEMORY.md`](~/.claude/projects/-Volumes-Scratch-Mycelium/memory/MEMORY.md) for the index.

| Plan | What's pending |
|---|---|
| TupleSpace companion crate | **Shipped** (2026-06-11) as workspace member `mycelium-tuple-space/` — all 5 phases of [`docs/plans/mycelium-tuple-space.md`](docs/plans/mycelium-tuple-space.md). See §TupleSpace companion crate below. |
| Pre-release arch remediation | **Complete.** All 9 steps done — plan at `~/.claude/plans/humble-twirling-comet.md`. |

**Already shipped (removed from list):** fuzz harness (`fuzz/fuzz_targets/`), SignalHandlers split, ConsensusEngine::propose extraction, locality/topology Phases 0–7, cross-group consensus Phase 8 (`cross_group_propose` + `GroupQuorum`), watcher C2 (`run_consolidated_opacity_watcher` + `FilterOpacityRegistry`), signal reorder buffer (`emit_ordered()` + wire v11 `hlc_seq`), semantic coordination (capability schema versioning `with_schema_id`/`CapFilter::with_schema`, gossip-propagated skill payload schemas `with_input_schema`/`with_output_schema`, signal sender authorization `signal_rx_from`, FIPA-ACL speech act taxonomy — `examples/semantic_coordination.rs`), schema registry (`publish_schema`, `force_publish_schema`, `get_schema`, `list_schemas`, `seed_schemas_from_dir` — `src/agent/schema_ops.rs`), **pre-release arch remediation** (sub-handle facade — `KvHandle`, `MeshHandle`, `SchemaHandle`, `ConsensusHandle`, `ServiceHandle`, `CapabilitiesHandle` — plus `gateway` feature gate for Axum).

## Architecture Constraints

### Layer I/II entanglement (known, v2 roadmap item)

`KvState` co-locates KV subscriptions with gossip storage. `apply_and_notify` writes
to both the store and signals `SignalHandlers` on every inbound frame. The signal mesh
cannot be disabled without losing `subscribe` / `subscribe_prefix` functionality.

Users who only need KV semantics can simply never call `MeshHandle` methods — zero
overhead when no signal handlers are registered.

Planned for v2: extract `mycelium-core` crate (gossip transport + KV only) from
`mycelium` (full substrate with signals, consensus, capabilities).

### Entry-volume scale test (orthogonal to node-count)

`make test-scale-entries` (30 nodes by default) validates the *entry-volume*
axis that `make test-scale` does not cover. The 100-node test writes one key
and confirms it gossips; this test writes `ENTRY_COUNT` keys (default 5 000,
configurable via `ENTRY_COUNT` and `ENTRY_BYTES` Makefile overrides) to a
30-node cluster and measures:

1. **Live-gossip fraction** — what percentage of entries are visible on mgmt
   *immediately after* the bulk-write phase ends. Approximates how well live
   propagation keeps up with the write rate.
2. **Anti-entropy sweep tail** — wall-clock seconds from `T_write_end` to
   `T_full_visible_on_mgmt`. Approximates how much closure work anti-entropy
   has to do on top of live gossip.
3. **Stability** — count remains at `ENTRY_COUNT` 15 s after convergence
   (no flapping, no eviction).
4. **Random-sample integrity** — 50 random keys verified for correct payload
   byte count via `kv-scan`.
5. **Backpressure** — `dropped_frames` on seed and mgmt after the bulk burst;
   non-zero is informative not fatal, with a hint to raise
   `GOSSIP_WRITER_CHANNEL_DEPTH`.

Why 30 nodes and not 100: the runner makes new TCP connections throughout
this test (bulk PUTs + convergence polling + sample reads), so we deliberately
stay well below the iptables FORWARD-chain ceiling. The 100-node test works
around chain saturation with conntrack tricks; the entry-volume test cannot
do the same because polling continues across the entire convergence window.

Bumped configuration in `docker-compose.scale-entries.yml`:
- `GOSSIP_WRITER_CHANNEL_DEPTH = 4096` on seed/mgmt (default 1024 drops
  frames at 5 000+ entries written in a short window)
- `GOSSIP_MAX_STORE_ENTRIES = 200000` so the test's synthetic graph fits
  without triggering store eviction

### Scale and resilience tests — Docker bridge iptables constraint

Both `make test-scale` (100 nodes) and `make test-scale-resilience` (20 nodes default)
are subject to the same Docker bridge iptables FORWARD chain limitation, but in
different ways.

**`make test-scale` (100 nodes)** passes reliably. The test validates: cluster
formation, KV write on seed, gossip propagation seed → mgmt, zero dropped frames.
At 100 nodes, peer-exchange creates ~5 000 TCP connections in the Docker bridge
network. The Linux bridge iptables FORWARD chain grows O(N²); after all inter-node
connections form, new TCP connections from the runner to seed time out. The test
works around this by:
1. Verifying the KV key on seed *immediately* after write (before chain saturates).
2. Verifying gossip propagation via mgmt using conntrack entries established
   during Phase 1 polling (not a new connection).

**`make test-scale-resilience` (default 20 nodes)** defaults to 20 workers rather
than 50. The resilience test includes a **Phase 3 late-joiner probe**: a fresh
container started mid-test that must establish a *new* TCP connection to seed and
receive the full KV history via anti-entropy. At 50 workers the iptables FORWARD
chain is already saturated by the time Phase 3 runs, so the probe's TCP SYN to seed
times out at the OS level (errno 110, ~2 min) — the probe never gets the anti-entropy
data. At 20 workers the chain is well within budget, the probe connects immediately,
and all Phase 3 checks (join, anti-entropy inbound, gossip outbound) pass reliably.

If the Phase 3 late-joiner checks fail on a future run, the iptables chain is the
first suspect. Mitigations: switch the Docker network driver to `macvlan`, enable
nftables (hash-table replacement for the linear iptables chain), or keep
`RESILIENCE_WORKERS ≤ 20`.

**Consecutive-run VM fatigue (observed 2026-06-10):** repeated 100-node rounds
in one Docker Desktop session degrade formation monotonically — same code went
PASS → 80/100 → 97/100 (timeout at 240 s) across three same-day rounds, then
PASS 5/5 with 0 dropped frames immediately after a Docker engine restart.
Kernel state in the VM (conntrack/iptables) accumulates across rounds even
though networks are recreated. Before declaring a formation-timeout failure a
regression, restart the Docker engine (`docker desktop restart`) and re-run
once on the fresh VM.

The v1 runtime mitigation is `GOSSIP_MAX_ACTIVE_CONNECTIONS` (caps outbound
TCP connections per node to K random peers, reducing O(N²) → O(N×K)).

**v2 structural fix:** hybrid TCP/UDP transport (SWIM-style) — gossip pings
and capability heartbeats over UDP (no connection state, loss-tolerable),
TCP reserved for anti-entropy data transfer. Eliminates the iptables problem
at the source rather than managing it with a cap. Full design note in
ROADMAP.md *v2.0 Milestones* item 5.

### Layer I/II Bridge Invariant

`src/store.rs` separates pure Layer I storage (`KvStore`) from the Layer II
subscription bridge (`KvState`). `KvState` wraps `KvStore` and adds the
`subscriptions` field; a `Deref<Target=KvStore>` implementation keeps all
existing call sites working without changes.

**Two named Layer I/II crossing points (the only places either layer reaches into the other):**

| Crossing point | File | What it does |
|---|---|---|
| `apply_and_notify` | `src/store.rs` | Writes to `KvStore` (Layer I), then notifies `KvState::subscriptions` watch channels (Layer II). This is the sole write path for both layers. |
| `subscribe` / `subscribe_prefix` | `src/agent/kv_handle.rs`, `helpers.rs` | Creates a `watch::Sender` in `KvState::subscriptions` (Layer II) and reads the current value from `KvStore::store` (Layer I) to initialise the `watch::Receiver`. |

All other code is single-layer: gossip forwarding reads `KvStore` only; signal
mesh reads `KvState::subscriptions` only. New features that touch only one layer
should stay in that layer and not reach into the other.

### Layer III invariant posture — tripwire, leases, listener registration

Three related facts about the consensus layer's relationship to the substrate:

1. **Namespace ownership is promise-strength.** The substrate never enforces
   the `consensus/` prefix; a rogue or buggy writer can clobber
   `consensus/committed/{slot}` and LWW will accept it. The deliberate response
   is detection, not prevention: the consensus listener's **commit-conflict
   tripwire** refuses to endorse (re-write) a COMMIT carrying a different value
   for a live committed slot, logs a `warn!`, and increments
   `SystemStats::commit_conflicts` (also on `/stats`). Do **not** "fix" this by
   adding a `consensus/`-prefix write guard to `apply_and_notify` — that would
   teach Layer I a Layer III law, inverting the dependency that makes Layer I
   the foundation.

2. **Epoch-leased commitments** (`ConsensusConfig::committed_lease_secs`).
   Opt-in; default (`None`) is permanent commit-once. When set, the commit also
   writes `consensus/lease/{slot}` (u64 LE ms) and readers
   (`consensus_get`, `consistent_get`, `GET /consensus/{slot}`) apply the same
   read-side freshness convention as capability entries: expired lease ⇒ reads
   as not-committed ⇒ the slot reopens for re-proposal. Renewal = re-proposing
   the *same* value while live (refreshes the commit timestamp via a fresh
   quorum round); a *different* value while live returns `Superseded`.
   `consensus_rx` is deliberately the raw KV view.

3. **Listener handlers are registered synchronously.**
   `start_consensus_listener` registers the PROPOSE/COMMIT receivers *before*
   spawning the voter task. Registration used to happen inside the task's first
   poll, which silently dropped any proposal racing listener startup (node
   fails to vote; single-node tests commit via self-quorum and never notice).
   Keep it this way when refactoring.

### TaskCtx — the shared infrastructure bundle (known God Object)

`src/agent/mod.rs::TaskCtx` is a 22-field struct held in a single `Arc` and cloned into
every background task, typed handle, and connection handler. It exists to break the
otherwise-circular reference between `GossipAgent` (which holds the task `JoinSet`) and
the tasks themselves.

**Field groups** (section comments are in the struct body):
| Group | Key fields |
|---|---|
| Identity + config | `node_id`, `config`, `default_ttl` |
| Layer I — KV | `seen`, `hlc`, `gossip_txs`, `kv_state`, `wal` |
| Layer II — Signals | `signal_boundary`, `signal_handlers`, `reorder_buf` |
| Capability subsystem | `caps_advertised`, `filter_opacity_registry`, `group_roster_cache` |
| Service layer | `bulk_transport`, `rpc_pending` |
| Security | `tls`, `peer_keys` |
| Networking + Lifecycle | `peers`, `shutdown_tx`, `task_handles` |

The v2 fix is a workspace split: `mycelium-core` (Layers I+II only, with `CoreCtx`) +
`mycelium` (full substrate). Deferred until there is a real embedding use case that
needs the core without consensus/capabilities.

### Individual-scope routing (RPC / votes) — forwarding stays unconditional

`SignalScope::Individual` carries RPC requests, RPC responses, and consensus
votes. The gossip loop sends an Individual frame directly to the target when
it is in the sender's outbound peer list (optimization), and otherwise
**falls back to flooding** — each hop applies the same rule; the seen-set
dedups and the hop-TTL bounds it. Do not "optimize" the fallback away: before
2026-06-12 the frame was silently dropped when the target wasn't directly
peered, which broke RPC and ballot voting in partial meshes
(`GOSSIP_MAX_ACTIVE_CONNECTIONS` / `max_forwarding_peers` topologies) and
contradicted the unconditional-forwarding model (only *admission* is scoped,
via `Boundary::admits`). Regression gate:
`test_individual_signal_reaches_unpeered_target_via_relay`. Direct peering
remains a *latency* optimization for RPC-heavy pairs — the three-arm
experiment harness bootstraps both directions for that reason.

Companion invariant (same day): fan-out activation is **event-driven** — the
connection handler publishes the peer list the moment a new peer is inserted
(Ping receipt), because waiting for the health monitor's next tick left
inbound-only nodes (seeds, tuple primaries) mute for live sends for up to 2×
`health_check_interval`. The health monitor remains the reconciler/evictor.
Topology-class regression gate:
`test_individual_consumers_over_random_partial_meshes` (random partial
meshes; signal + RPC + ballot between non-adjacent pairs).

### Lock-Order Table

All `Mutex` and `RwLock` sites in the codebase. **Invariant: no function acquires more than one lock from this table.** There are no nested acquisitions, so no lock-ordering discipline is required beyond this flat list.

| # | Field | Type | Acquired in | Notes |
|---|-------|------|-------------|-------|
| 1 | `TaskCtx::task_handles` | `Mutex<JoinSet>` | `spawn_task()`, `wait_for_tasks()` | Short-lived; never held across `await` |
| 2 | `TaskCtx::rpc_pending` | `Mutex<HashMap>` | `rpc_call_ctx` (register + remove), incoming `rpc.result` signal handler | `.lock()` recovers from poisoning; never held across `await` |
| 3 | `TaskCtx::reorder_buf` | `Mutex<ReorderBuffer>` | `emit_ordered()`, reorder-buffer flush task | Lock+flush is synchronous; no `await` inside |
| 4 | `TaskCtx::signal_boundary` | `RwLock<Boundary>` | Read: every `emit()` boundary check; Write: `join_group()`, `leave_group()`, `suppress()`, `unsuppress()` | `read()` is the hot path; `write()` is rare |
| 5 | `GossipAgent::gossip_rxs` | `Mutex<Option<Vec<Receiver>>>` | `start()` (consumed once, `take()`d) | Single-use; never held after `start()` returns |
| 6 | `GossipAgent::extra_routes` | `Mutex<Option<Router>>` | `with_http_routes()` (set once), `start()` (consumed, `take()`d) | Gateway feature only; single-use |
| 7 | `KvStore::index_stripes` | `[Mutex<()>; 64]` (striped by key hash) | `apply_and_notify` secondary-structure reconcile | Leaf lock: nothing else from this table is acquired while held; synchronous section (store re-read + index ops), never across `await`. Exists because the store CAS is lock-free — without it, two winning writers to one key could interleave index ops opposite to their CAS order and strand a live key outside `scan_prefix` (M2 Run-18 finding) |

**Note on async contexts:** `Mutex` guards in Tokio async code are `!Send` when held across `await` points — the compiler enforces this. All sites above release the guard before any `await`, which is why `std::sync::Mutex` (not `tokio::sync::Mutex`) is used throughout: `std::sync::Mutex` is cheaper and the compiler will error if a guard is accidentally held across a suspension point.

### Lock-free mutation rules (papaya) — the race family that keeps recurring

Four M2-audit findings (Runs 16–18) plus a same-day sweep all reduced to one
shape: **a lock-free operation followed by an unserialised derived effect.**
Two rules close the whole family; follow them for every new papaya call site:

1. **`compute` closures must be retry-safe.** papaya re-invokes the closure
   when the entry changes concurrently. Never `take()` a single-use value
   inside one (panics on retry — the signal-registration crash); clone per
   invocation, and reset any captured outputs at the top of the closure
   (see `apply_and_notify`'s `old_ts_if_live`).
2. **Never act on a stale read.** A collect-then-`remove()` sweep, a
   check-then-act (`is_empty()` → spawn), or an unconditional remove keyed by
   something another caller may have replaced — all of these must re-validate
   inside a `compute` (conditional remove: tombstone GC, A2A sweep, peer
   eviction, seen-set eviction), behind an atomic `swap` (LLM dispatch spawn),
   by `Arc::ptr_eq` identity (quorum-tracker and prompt-skill removal), or
   under a stripe lock with a re-read (`apply_and_notify` index reconcile).

Correct reference implementations: `get_or_spawn_writer` (claim-by-sentinel,
spawn outside the closure), `ShardedSeen::evict_below` (conditional remove),
`kv_quorum::{install_tracker, remove_tracker}` (copy-on-write list +
identity-checked removal).

### Memory ordering policy for atomics

The codebase uses atomic operations in two categories — follow the same pattern when
adding new ones:

**`Relaxed` — purely diagnostic counters**

`dropped_frames`, `hash_acc`, `listener_count`, and the `AliveGuard` liveness flags
use `Relaxed`. These are read-only by `system_stats()` or health-check logging; no
control-flow decision depends on observing them at a precise point relative to any
other memory write. A brief visibility lag is acceptable.

**`Release` + `Acquire` — generation counters and readiness gates**

`KvState::grp_generation` is bumped with `Release` whenever a `grp/` key is written.
The gossip-loop cache reader loads it with `Acquire`. This guarantees that when the
reader observes the new generation value, all `grp/` KV writes that happened-before
the `Release` store are also visible — the cached roster is never invalidated too late.

`TaskCtx::caps_advertised` is stored with `Release` (first capability advertisement
tick) and loaded with `Acquire` (the `/ready` handler). This makes the readiness gate
correct: when `/ready` sees `true`, the soft-state KV keys that preceded the store are
visible to the same thread.

**`AcqRel` + `Acquire` — agent lifecycle state**

`GossipAgent::state` (an `AtomicU8` in `lifecycle.rs`) uses `AcqRel` on compare-and-
swap transitions and `Acquire` on plain loads. The lifecycle state gates task spawning
and public API calls; AcqRel gives both acquire and release semantics on the CAS.

**Cancelled flags (`AtomicBool`)**

`RegEntry::cancelled` is stored with `Release` (handle drop) and loaded with `Acquire`
(consolidated opacity watcher loop). The Acquire load ensures that all work done by the
caller before dropping the handle is visible to the watcher before it stops processing
that registration.

### Operational diagnostics reference

**Public HTTP endpoints (no auth required, available when `gateway` feature is on):**

| Endpoint | What it tells you |
|---|---|
| `GET /health` | 200 = process alive |
| `GET /ready` | 200 = capabilities advertised + no dead shards |
| `GET /stats` | `node_id`, `store_entries`, `dropped_frames`, `task_count`, `commit_conflicts` |
| `GET /consensus/{slot}` | `committed` (base64 or null, **lease-aware**) + `ballot` (u64) + `lease_ms` + `lease_expired` for a consensus slot |
| `GET /metrics` | Prometheus scrape endpoint (`metrics` feature required) |

**`SystemStats::task_count`** — number of Tokio tasks in the `JoinSet`. Expected
steady-state values after `start()`:

| Source | Count |
|---|---|
| GC, health-monitor, anti-entropy, WAL-flush, signal-reorder-buffer, capability-heartbeat, group-member-sync | 7 |
| Per gossip shard (default 4): writer + listener | +8 |
| Gateway Axum server (`gateway` feature) | +1 |
| Per connected peer: per-peer writer | +N_peers |
| Each active `bulk_serve` call (one background listener, RAII via `BulkServeHandle`) | +N_bulk_servers |

**Not tracked in `task_handles`:**
- `rpc_call` — direct `async fn` await over a oneshot channel, no task spawned.
- `scatter_gather` — uses a local `JoinSet` dropped on function return; never enters `task_handles`.
- `bulk_serve` per-request handlers — one untracked task is spawned per incoming bulk signal, bounded to `GossipConfig::max_concurrent_bulk_handlers` (default 64) via semaphore; not in `task_count` but visible as `system_stats().active_bulk_handlers`.

Typical baseline on a 3-node cluster: **17–20 tasks**. A value growing
unboundedly indicates a task leak (most likely a per-peer writer that is not exiting on disconnect).

### TupleSpace companion crate (`mycelium-tuple-space/`)

Linda-style pull-based pipeline buffer, built **entirely on the public API**
(the crate's single normal dependency on `mycelium` is the composability
proof; the core's dev-dependency back on it — for `examples/three_node_demo`
— is a legal Cargo cycle). Design doc: `docs/plans/mycelium-tuple-space.md`.

Key facts for future sessions:

- **Pattern**: workers `take()` when ready — readiness is self-announcing,
  so the staleness/misroute failure mode of push-predict distribution does
  not exist. The space removes the central *decision-maker*; the data path
  is still a rendezvous point with its own failover (below). This crate is
  the load-bearing artifact for the pull-vs-push reframing of Paper 2a.
- **Lanes, not Linda matching**: "Linda-style" = generative decoupling +
  blocking pull, NOT associative template retrieval. The store is named
  per-stage FIFO lanes (`stages: HashMap<Arc<str>, StageState>`); payloads
  are opaque; an item's pipeline position is the lane it sits in; `complete`
  is an atomic lane-to-lane move. Workers "filter" only by choosing which
  lane to take from (per-lane depth = the pressure signal). Content-style
  routing is encoded in lane names (`stage-b.high`), never payload matching.
  Fan-in joins (two-stream rendezvous by correlation key) are NOT yet
  expressible — keyed-exact-match `take` is ROADMAP v2.0 milestone 13,
  promised by Paper 1 §9.4.
- **Roles** (`TupleRole`): `Primary` serves; `Secondary` mirrors via
  replicate RPCs + heartbeat Signal and promotes when the primary's
  capability evaporates (the ring IS the failure detector); `Auto` elects
  with a lowest-candidate-id tie-break (the plan's bare resolve-then-promote
  races); `Client` never serves.
- **Durability**: single-lock hot path (no TOCTOU between waiter check and
  store); WAL with 4 record types — `Complete` is one indivisible record so
  a stage transition can never half-replay; compaction bumps a WAL *epoch*
  so a secondary's byte-offset replay cursor can't silently dangle.
- **Capability names**: flat `tuple` / `{ns}.primary|secondary|candidate` —
  capability key segments must not contain `/` (`parse_cap_key` rejects
  them), so the plan's `tuple/{ns}/primary` shape was flattened.
- **KV prefixes owned**: `tuple/inflight/{ns}/{id}` (advisory claim keys)
  and `sys/tuple/{node}/{ns}/…` (metrics + backpressure pheromone). The
  pheromone deliberately does NOT use `sys/load/` opacity: the load-state
  encoding is substrate-internal, and hiding the primary from `resolve`
  under load would false-trigger the secondary's promotion watch.
- **Gates**: `cargo test -p mycelium-tuple-space --features gateway` and
  `cargo clippy -p mycelium-tuple-space --features gateway --all-targets -- -D warnings`;
  SDKs in `mycelium-py/src/mycelium/tuple.py` and `mycelium-ts/src/tuple.ts`;
  integration scenario 13.

### Gateway feature gate

The `gateway` feature (on by default) enables the embedded Axum HTTP server. Disable
it for bare-metal / WASM / no-std targets:

```toml
mycelium = { version = "1", default-features = false }
```

Without `gateway`, `with_http_routes`, `with_a2a`, the SSE/WebSocket endpoints, and
the MCP-over-HTTP bridge are all compiled away. The gossip core, KV store, signal mesh,
consensus, and all typed sub-handles (`KvHandle`, `MeshHandle`, etc.) remain available.

### Testing conventions

**Always run the full feature matrix locally before pushing.**
`cargo test --lib` alone misses code inside `#[cfg(feature = "tls")]`, `#[cfg(feature = "a2a")]`, etc.
Use the same set CI uses:

```bash
cargo test --lib --features tls,metrics,a2a,llm
cargo clippy --lib --tests --features tls,metrics,a2a,llm -- -D warnings
```

CI additionally gates: `tsc --noEmit` over mycelium-ts (`sdk-ts` job), the
AFN demo end-to-end in both pull and push modes (`afn-smoke` job,
`examples/fluid_pipeline/ci_smoke.sh`), and time-boxed libFuzzer runs of the
wire/capability decoders (`fuzz` job; skipped on PRs).

**Consensus tests on multi-node setups require `start_consensus_listener` on every node.**
`system_propose` / `consistent_set` compute `quorum = ⌊(peers+1)/2⌋ + 1`. If peer nodes have
no `ConsensusListener`, their votes never arrive and every ballot times out. A test that omits
this only passes when `peers.len() == 0` at call time (accidental single-node quorum) — a timing
race that disappears as soon as cluster formation is polled properly.

```rust
// Required pattern for any multi-node consensus test:
let _l1 = a1.consensus().start_consensus_listener(ConsensusConfig::default());
let _l2 = a2.consensus().start_consensus_listener(ConsensusConfig::default());
// Poll until peers are visible — structural, not a sleep:
for _ in 0..40 {
    if !a1.peers().is_empty() && !a2.peers().is_empty() { break; }
    tokio::time::sleep(Duration::from_millis(50)).await;
}
```

**Use structural polling, not fixed sleeps, to assert cluster state.**
A structural assertion (`!peers().is_empty()`) fails deterministically and points to the root
cause. A fixed `sleep(300ms)` passes by luck on a fast machine and hides the race on a slow one.
A test that passes intermittently is harder to catch than one that reliably fails — the peer-ready
poll converts a timing race into a consistent deterministic failure, which is how latent bugs
get found.

## Working in this repo

- `cargo build --lib`, `cargo test --lib`, `cargo clippy --lib --tests`
- `cargo build --lib --no-default-features` to verify the gateway-free embedded build
- `cargo build --lib --features metrics` to include the Prometheus scrape endpoint
- `cargo build --lib --features a2a` to include the A2A protocol adapter
- `cargo build --lib --features llm` to include the Prompt Skills LLM adapter
- `cargo build --lib --features compliance` to include gateway auth, durable audit, RBAC (planned, not yet implemented)
- 323 lib tests at HEAD (full feature matrix); clippy at 0 warnings (stub removal
  eliminated the prior 61 `field_reassign_with_default` baseline in test code).
- Wire version is currently **v11** (`PREV_WIRE_VERSION = 10` — rolling upgrade window open).
  v11 adds `hlc_seq: Option<u64>` to `WireMessage::Signal` for ordered delivery via `emit_ordered()`.
  v10 adds `WireMessage::SignedData` for Ed25519-signed KV writes under the `tls` feature.
- **Agentic Flow Networks demo**: `examples/fluid_pipeline/` — 10-worker fluid pool,
  4-stage news article pipeline, two modes via `PIPELINE_MODE`: **pull** (default,
  canonical — tuple-space stages, workers take() from the deepest stage; seeder is an
  edge client) and **push** (pre-refinement baseline — coordinator dispatch over the
  KV ring, kept as the comparison case). Run with
  `docker compose up --build --scale worker=10`; `ci_smoke.sh` runs both modes
  Docker-free and is wired into CI as the `afn-smoke` job. See
  [`examples/fluid_pipeline/README.md`](examples/fluid_pipeline/README.md) for the
  concept document, [`flow_networks.html`](examples/fluid_pipeline/flow_networks.html)
  for the AFN concept essay (incl. the push→pull TupleSpace refinement), and
  [`fluid_pipeline_viz.html`](examples/fluid_pipeline/fluid_pipeline_viz.html) for the
  visualisation.
- **A2A LangChain/AutoGen demo**: `examples/a2a_langchain/` — LangChain ReAct agent and
  AutoGen v0.4 agent that auto-discover Mycelium skills via `/.well-known/agent.json` and
  use them as native tools. Requires `cargo build --bin skillrunner --features a2a` then
  `examples/community/start.sh`.
- Integration test count: **13 scenarios** (scenario 11 = AFN pipeline; scenario 12 = Prompt Skills cross-node KV propagation + invocation; scenario 13 = TupleSpace pull pipeline — node-a primary, node-b secondary mirror, driven through the HTTP gateway).
- Scale tests: `make test-scale` (100 nodes), `make test-scale-resilience` (20 nodes default — see §iptables above for why not 50).
