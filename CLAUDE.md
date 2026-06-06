# CLAUDE.md — Mycelium quick-reference for future code-assistant sessions

This file is a fast on-ramp for code-assistant tools (and humans new
to the repo). It points at the canonical architecture documents
rather than duplicating them.

## What this is

Mycelium is an embedded, broker-less Rust library that provides a
three-layer substrate for AI agent fleets and storage replication:

| Layer | What it does | Where it lives |
|---|---|---|
| **I — KV store** | Last-write-wins state propagation over TCP; anti-entropy synced; every key has a TTL. | `src/store.rs`, `src/connection.rs`, `src/framing.rs`, `src/writer.rs`, `src/seen.rs` |
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
| TupleSpace companion crate | Deferred; design at `~/.claude/plans/mycelium-tuple-space.md` |
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

The v1 runtime mitigation is `GOSSIP_MAX_ACTIVE_CONNECTIONS` (caps outbound
TCP connections per node to K random peers, reducing O(N²) → O(N×K)).

**v2 structural fix:** hybrid TCP/UDP transport (SWIM-style) — gossip pings
and capability heartbeats over UDP (no connection state, loss-tolerable),
TCP reserved for anti-entropy data transfer. Eliminates the iptables problem
at the source rather than managing it with a cap. Full design note in
ROADMAP.md *v2.0 Milestones* item 5.

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
- 263 tests at HEAD; clippy at 0 warnings (stub removal eliminated the prior 61
  `field_reassign_with_default` baseline in test code).
- Wire version is currently **v11** (`PREV_WIRE_VERSION = 10` — rolling upgrade window open).
  v11 adds `hlc_seq: Option<u64>` to `WireMessage::Signal` for ordered delivery via `emit_ordered()`.
  v10 adds `WireMessage::SignedData` for Ed25519-signed KV writes under the `tls` feature.
- **Agentic Flow Networks demo**: `examples/fluid_pipeline/` — 10-worker fluid pool,
  KV ring as distributed buffer, 4-stage news article pipeline. Run with
  `docker compose up --build --scale worker=10`. See `docs/flow_networks.html` for the
  concept document and `docs/fluid_pipeline_viz.html` for the visualisation.
- **A2A LangChain/AutoGen demo**: `examples/a2a_langchain/` — LangChain ReAct agent and
  AutoGen v0.4 agent that auto-discover Mycelium skills via `/.well-known/agent.json` and
  use them as native tools. Requires `cargo build --bin skillrunner --features a2a` then
  `examples/community/start.sh`.
- Integration test count: **12 scenarios** (scenario 11 = AFN pipeline; scenario 12 = Prompt Skills cross-node KV propagation + invocation).
- Scale tests: `make test-scale` (100 nodes), `make test-scale-resilience` (20 nodes default — see §iptables above for why not 50).
