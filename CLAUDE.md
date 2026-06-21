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
| **II — Signal mesh** | Ephemeral scoped events with per-node admission boundaries; pheromone-style opacity composition. | `mycelium-core/src/signal.rs`, `mycelium-core/src/mesh_handle.rs`, `src/agent/opacity.rs` |
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
| Which crate to depend on (`mycelium` vs `mycelium-core`) — why/when | [`README.md`](README.md) §"Which crate?" + `src/lib.rs` crate doc-comment §"Crate layout" + `docs/guide/01-gossip-kv.md` Dev Notes (the user-facing front-door framing of the v2.0 M1 split) |
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
| v1.x completion (Production Readiness Gap) | Action plan at [`docs/plans/v1x-completion.md`](docs/plans/v1x-completion.md). **WS1 (Identity & RBAC) shipped** — signed role claims, provider-side capability authz, OAuth2 gateway ACLs, `sys/` namespace tripwire; see §RBAC / identity. **WS2 (tamper-evident audit) shipped** — per-node hash-chained signed `sys/audit/` trail, `/gateway/audit` verify endpoint, SkillRunner integration; see §Audit trail. **WS3 (crown-jewel) shipped** — opt-in data-at-rest cipher hook, egress allowlist, blast-radius threat model; see §Crown-jewel posture. **WS4 (OIDC SSO) shipped** — generic-OIDC JWT validation at the gateway (alg-confusion-safe), discovery + cached JWKS, groups→scopes; see §RBAC / identity (OIDC SSO) + [`docs/operations/sso.md`](docs/operations/sso.md). **WS5 (hot cert rotation) shipped** — swappable `NodeTls` (ArcSwap), `rotate_identity()` with dual-key window, retained-key-set verification (option B); see §Hot cert / identity rotation + [`docs/operations/cert-rotation.md`](docs/operations/cert-rotation.md). **v1.x engineering scope complete** (WS6 = the doc alignment done per-WS). Support/SLA is commercial-track (out of engineering scope). |
| v2.0 consolidated plan | Execution layer for the next major version at [`docs/plans/v2.0.md`](docs/plans/v2.0.md) — groups the 16 ROADMAP v2.0 milestones + the post-M16 design notes (NANDA Registry-Quilt technique transfers, schema-registry evolution) into 7 workstreams (WS-A crate/API, WS-B scale/transport, WS-C metabolism, WS-D security, WS-E code mobility/autonomic provisioning, WS-F federation/interop, WS-G coordination patterns) with a dependency graph, completeness matrix, and demand-driven definition-of-done. ROADMAP §v2.0 Milestones stays the canonical design home; the plan is strategy/sequencing only (no duplication). **WS-A shipped (M1/M2/M3 on `main`).** WS-B has a per-workstream delivery plan at [`docs/plans/v2-wsb-scale-transport.md`](docs/plans/v2-wsb-scale-transport.md) — M4 partial-mesh → M5 SWIM UDP/TCP → v12 bump (M11 codec + Merkle anti-entropy); **DoD retargeted to 100 nodes** (the iptables FORWARD-chain ceiling is observable well below 100, and the sharpest gate is `test-scale-resilience RESILIENCE_WORKERS=50` passing). **M4 + M5 on PR #19** (`v2/wsb-m5-stage4-depin-flatten`): Stage-4 SWIM cutover complete — `swim_failure_detector` now defaults **true**; G1 (`seed_established` flat over Docker: N=50=24, N=100=22) + G3 (50-worker resilience 11/11) both green. The long in-process/Docker divergence was a config bug — the demo never applied `GOSSIP_*` env, so SWIM was silently off in every Docker run. Rolling-upgrade caveat: don't mix SWIM-on/off nodes (flip a cluster together). **M11 + Merkle anti-entropy + v12 bump shipped** (`v2/wsb-m11-merkle-v12`): bincode fully retired (RUSTSEC-2025-0141 — in-tree `codec` + `serde_fixint`, byte-exact, dev-dep oracle only), and `StateRequest` switched to a per-bucket Merkle digest (`O(divergence)` anti-entropy) at `WIRE_VERSION = 12` / `PREV = 11`. **WS-B COMPLETE & SIGNED OFF** (PRs #19 + #21 merged; example-gating nit PR #22). Full-matrix sweep 2026-06-17: all host feature gates (`--lib` default / `tls,metrics,a2a,llm` / `compliance` / `no-default+gateway`) + tuple-space gateway + 3× clippy `-D warnings` + all examples compile + community/AFN smokes green; Docker integration 13/13, overlay, llm-agent, resilience-20, entries-30 (Merkle paths) green. The 100-node `make test-scale` formation-within-240s is the documented Docker-bridge iptables ceiling (8–94/100 variance on identical code incl. fresh-VM restarts) — environmental, not a regression (identical code converges at 20/30/50 nodes). The rest (WS-C…WS-G) remain trigger-gated — **WS-C has a delivery plan** at [`docs/plans/v2-wsc-metabolism.md`](docs/plans/v2-wsc-metabolism.md) (M8 startup auto-derivation first/standalone — closed-form tuning from N, gates G-C1/G-C2/G-C3; M9 hot-reload + `ClusterTuner` and M7 distributed rate-limit parked on their own triggers; M10 fenced reconfig deferred). Demand signal = the WS-B tuning friction (config-bug root cause, hand-written `tuning.md`, manual `writer_channel_depth` bump). **M9 shipped** (PR #26 hot-reload + ClusterTuner; PR #27 tuning governor = first worked instance of *management = intent + local reconcile*). Next: [`docs/plans/elastic-sizing-intent-governed.md`](docs/plans/elastic-sizing-intent-governed.md) — extract `IntentReconciler` (shared transport) → `MembershipGovernor` (min/max/drain + collective self-election; the engine) → operator surface (`/gateway/govern` + audit + Prometheus; orthogonal slice, lands after the engine). Near-term need. |

**Already shipped (removed from list):** fuzz harness (`fuzz/fuzz_targets/`), SignalHandlers split, ConsensusEngine::propose extraction, locality/topology Phases 0–7, cross-group consensus Phase 8 (`cross_group_propose` + `GroupQuorum`), watcher C2 (`run_consolidated_opacity_watcher` + `FilterOpacityRegistry`), signal reorder buffer (`emit_ordered()` + wire v11 `hlc_seq`), semantic coordination (capability schema versioning `with_schema_id`/`CapFilter::with_schema`, gossip-propagated skill payload schemas `with_input_schema`/`with_output_schema`, signal sender authorization `signal_rx_from`, FIPA-ACL speech act taxonomy — `examples/semantic_coordination.rs`), schema registry (`publish_schema`, `force_publish_schema`, `get_schema`, `list_schemas`, `seed_schemas_from_dir` — `src/agent/schema_ops.rs`), **pre-release arch remediation** (sub-handle facade — `KvHandle`, `MeshHandle`, `SchemaHandle`, `ConsensusHandle`, `ServiceHandle`, `CapabilitiesHandle` — plus `gateway` feature gate for Axum).

## Architecture Constraints

### Layer I/II entanglement (resolved by the `mycelium-core` split — v2 M1)

`KvState` co-locates KV subscriptions with gossip storage. `apply_and_notify` writes
to both the store and signals `SignalHandlers` on every inbound frame. The signal mesh
cannot be disabled without losing `subscribe` / `subscribe_prefix` functionality.

Users who only need KV semantics can simply never call `MeshHandle` methods — zero
overhead when no signal handlers are registered.

**Resolved by v2 M1 (`mycelium-core` workspace split):** Layers I + II (gossip transport
+ KV + signal/boundary mesh) now live in the [`mycelium-core`](mycelium-core/) crate, cut at
the II↔III seam; the full `mycelium` crate (consensus, capabilities, services, gateway, tls)
depends on it. The bridge is kept as sanctioned internal cohesion *within core* — `KvStore`
holds no Layer II references; the coupling is only the documented `KvState` /
`apply_and_notify` crossing points — so the split draws the crate boundary *around* the
entanglement rather than severing it. The crate boundary now makes the inverted-dependency
invariant (a substrate that is never aware of the layers above) a **compile-time guarantee**:
`mycelium-core` cannot reference `mycelium` (it would be a Cargo cycle). Execution record:
[`docs/plans/v2-m1-mycelium-core.md`](docs/plans/v2-m1-mycelium-core.md).

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
| `subscribe` / `subscribe_prefix` | `mycelium-core/src/kv_handle.rs`, `mycelium-core/src/ops.rs` | Creates a `watch::Sender` in `KvState::subscriptions` (Layer II) and reads the current value from `KvStore::store` (Layer I) to initialise the `watch::Receiver`. |

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

### TaskCtx / CoreCtx — the shared infrastructure bundle (God Object split — v2 M1)

The former 22-field `TaskCtx` God Object has been split (v2 M1). The Layers I + II
infrastructure now lives in **`mycelium_core::CoreCtx`** (`mycelium-core/src/context.rs`);
`src/agent/mod.rs::TaskCtx` holds `core: Arc<CoreCtx>` plus the Layer III+ fields and
`Deref`s to `CoreCtx`, so the ~380 existing `ctx.<core-field>` access sites are unchanged.
Both are held in a single `Arc`, cloned into every background task, typed handle, and
connection handler — breaking the otherwise-circular reference between `GossipAgent` (which
holds the task `JoinSet`) and the tasks themselves.

**Field split:**
| → `CoreCtx` (Layers I+II, in `mycelium-core`) | → `TaskCtx` (Layer III+, in `mycelium`) |
|---|---|
| `node_id`, `config`, `default_ttl` | `filter_opacity_registry`, `group_roster_cache` |
| `seen`, `hlc`, `gossip_txs`, `kv_state`, `wal` | `llm_skills`, `llm_dispatch_spawned` (cfg `llm`) |
| `signal_boundary`, `signal_handlers`, `reorder_buf`, `reply_interceptor` | `bulk_transport`, `rpc_pending` |
| `tls`, `peer_keys`, `sys_namespace_violations` | `commit_conflicts` |
| `soft_state_advertised`, `peers`, `shutdown_tx`, `task_handles` | `audit_chain` (cfg `compliance`) |

`CoreCtx` also carries `spawn_task` (the substrate task-spawn entry point) — the
`task_handles` JoinSet it drives is a core field, so the v2-M3 handle pushdown
moved `spawn_task` down with it (`TaskCtx` Deref-coerces unchanged). The
`soft_state_advertised` flag (formerly `caps_advertised`) likewise moved to core:
it is flipped by the pure-Layer-I persist loop and read by the gateway `/ready`.

The three core↔upper couplings are mechanism-in-core / agency-above hooks, `None`-safe for
pure-core embeds: `CoreCtx::reply_interceptor` (RPC/bulk reply claim), the `QuorumObserver`
trait (quorum-ack notify), and `persistence`'s `SnapshotDeferHook` (opacity-defer). See
[`docs/plans/v2-m1-mycelium-core.md`](docs/plans/v2-m1-mycelium-core.md).

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
| 8 | `TaskCtx::audit_chain` | `Mutex<AuditChainState>` (`compliance` feature) | `audit()` record sealing | Leaf lock: serialises the per-node hash chain (seq + last_hash advance atomically). The guard is **released before** the KV write, so it never overlaps lock #7; synchronous section (build record + sign + hash), never across `await` |

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
identity-checked removal), `helpers::merge_peer_keys` (retained-key-set union
recomputed inside a `compute` closure — atomic read-merge-write, retry-safe; the
prior get-clone-modify-insert lost a retained verifying key when two rotations
for one node merged concurrently — regression gate
`concurrent_merges_for_one_node_never_drop_a_key`, which loses ~87% of keys
against the old impl).

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

`CoreCtx::soft_state_advertised` (formerly `TaskCtx::caps_advertised`) is stored with
`Release` (first persist-loop tick) and loaded with `Acquire` (the `/ready` handler).
This makes the readiness gate correct: when `/ready` sees `true`, the soft-state KV
keys that preceded the store are visible to the same thread.

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
| `GET /stats` | `node_id`, `store_entries`, `dropped_frames`, `task_count`, `commit_conflicts`, `sys_namespace_violations` |
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
  **Fan-in joins** (two-stream rendezvous by correlation key) ARE now
  expressible via **keyed-exact-match `take`** (ROADMAP M13 / WS-G, shipped):
  `put_keyed(stage, key, payload)` + `take_by_key(stage, key, timeout)` +
  `complete_keyed` claim by an O(1) exact-match key (a keyed index + keyed-waiter
  map per `StageState`, kept separate from the FIFO; durable across crash/promotion
  via WAL v2 record kinds `REC_PUT_KEYED`/`REC_COMPLETE_KEYED`, v1 replay accepted).
  Exact-match only — associative *template* matching remains the blackboard
  companion's territory (Paper 1 §9.4). Gateway: `POST /gateway/tuple/put` (optional
  `key`) + `/gateway/tuple/take_by_key`; py/ts `put_keyed`/`take_by_key`.
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

### Blackboard companion crate (`mycelium-blackboard/`)

Blackboard-style shared working memory, built **entirely on the public API** (WS-G / G3, shipped
2026-06-21, PRs #95–#100). The content-routed sibling of the tuple space: where the tuple space
routes by lane *position*, the blackboard routes by *content* (a predicate over fact attributes).
Design: `docs/plans/mycelium-blackboard.md` (sketch) + `docs/plans/v2-wsg-g3-blackboard.md` (build).

Key facts for future sessions:

- **The one new primitive is `claim(predicate)`** — competitive destructive claim-by-predicate
  (Linda's `in`): a finite fact matching the predicate is claimed by exactly one agent (single-owner,
  **non-blocking** — the loser gets `None`, no parked waiters, unlike the tuple space's blocking
  `take`). Non-destructive `read` (`rd`) is shared/concurrent. `ack` is the idempotent terminal;
  `release` / the in-flight deadline re-queue (at-least-once).
- **Predicate language** = the capability attribute-filter grammar (equality + presence), **not**
  unification/template matching — `Predicate::new().eq(k,v).present(k)`.
- **Two layers**: `BoardStore` (pure in-memory core + WAL, testable via `transient()`/`persistent()`)
  and `Blackboard` (agent-backed: roles + RPC + failover). `BoardRole` = `Auto`/`Primary`/`Secondary`/
  `Client`, mirroring `TupleRole`.
- **Replication is `Post`/`Ack`-only** (the deliberate divergence from the tuple space): a
  `Claim`/`Release` doesn't change a mirror's liveness — a claimed-but-unacked fact stays claimable in
  the mirror = the at-least-once re-queue a promotion wants. So **no heartbeat / WAL-replay cursor** is
  needed; snapshot-on-join + live replication keep the mirror a complete live view.
- **Exactly-once discipline**: the blackboard is the *second* real user of the WAL claim/ack/requeue
  discipline (the tuple space is the first). The shared-overlay extraction was **examined and
  declined-with-evidence** — the two diverge on a load-bearing axis (tuple = wall-clock-ms,
  WAL-persisted, cross-node; blackboard = monotonic `Instant`, in-process). The contract is the shared
  artifact, not code — see `docs/design/exactly-once-effect.md`.
- **WAL**: magic `MBBWAL`, records `Post`/`Claim`/`Ack`/`Release`, replay liveness = Posted-and-not-Acked.
- **Gates**: `cargo test -p mycelium-blackboard --features gateway` + clippy `--features gateway
  --all-targets`; SDKs in `mycelium-py/src/mycelium/blackboard.py` + `mycelium-ts/src/blackboard.ts`;
  the `microgrid` example + `ci_smoke.sh` (CI `blackboard` job); cross-node `tests/failover.rs`,
  gateway `tests/gateway.rs`. Gateway: `POST /gateway/bb/{post,read,claim,ack,release}` + `GET
  /gateway/bb/depth`.

### Gateway feature gate

The `gateway` feature (on by default) enables the embedded Axum HTTP server. Disable
it for bare-metal / WASM / no-std targets:

```toml
mycelium = { version = "1", default-features = false }
```

Without `gateway`, `with_http_routes`, `with_a2a`, the SSE/WebSocket endpoints, and
the MCP-over-HTTP bridge are all compiled away. The gossip core, KV store, signal mesh,
consensus, and all typed sub-handles (`KvHandle`, `MeshHandle`, etc.) remain available.

### RBAC / identity (compliance feature) — WS1

The `compliance` feature (`= ["gateway", "tls"]`) adds role-based access control
on top of the Ed25519 node identity. It is **opt-in and additive**: with the
feature off every type below compiles away and behaviour is unchanged; with it
on but unconfigured (no roles advertised, no scoped tokens) behaviour is still
unchanged. Lives in [`src/agent/rbac.rs`](src/agent/rbac.rs) plus the gateway
middleware in [`src/agent/http.rs`](src/agent/http.rs). Plan: [`docs/plans/v1x-completion.md`](docs/plans/v1x-completion.md) §WS1.

Four layers, each obeying the **detection-not-prevention / promise-strength**
posture — RBAC is enforced where a resource is *served*, never by teaching
Layer I a higher-layer law:

1. **Signed role claims (Layer I).** `advertise_roles(roles, clearance)` writes a
   `SignedRoleClaim` to `sys/role/{node}` — an Ed25519 signature (the `tls`
   identity key) over `{node_id, roles, clearance, issued_at_ms}`. `roles_of(node)`
   returns the claim **only if** the signature verifies against `node`'s identity
   key as learned from the cluster (`sys/identity/` → `peer_keys`), *not* the
   forgeable KV bytes. A forged `sys/role/` write reads back as `None`. Roles are
   canonicalised (sorted/deduped) in `RoleClaim::new`; `clearance` is the L1/L2/L3
   data-classification level. `verified_roles` / `caller_admitted` are pure
   functions, unit-tested without a live agent.

2. **Provider-side capability authz.** `caller_authorized(sender, allow)` admits a
   verified RPC `sender` if `allow` (a capability's `authorized_callers`) is empty
   (open), lists the sender's NodeId, or lists a role the sender *verifiably*
   holds. Under the `tls` identity the inbound RPC sender is signature-checked at
   the connection layer, so `req.sender()` is trustworthy — this is the one place
   `authorized_callers` is genuinely enforceable (the caller controls its own
   resolve). Wired into the SkillRunner serve loop: a denied invoke is audited and
   answered with an error, never silently dropped.

3. **OAuth2 scope gateway ACLs.** `GossipConfig::gateway_scoped_tokens:
   Vec<GatewayToken>` maps a bearer token to `resource:verb` scopes
   (`kv:read`, `kv:write`, `mesh:write`, `consensus:read`, `llm:invoke`, …). Each
   `/gateway/**` route requires a scope (`required_scope` table in `http.rs`);
   a request is admitted iff its token's grant holds that scope or `"*"`.
   **Deny-by-default**: an unmapped gateway route requires `admin`. The legacy
   `gateway_auth_token` resolves to `["*"]`, so single-token deployments upgrade
   with no behaviour change. **M16 edge criterion:** `/health`, `/ready`, `/stats`,
   `/metrics`, and the descriptor path are *not* under `/gateway` and stay public
   and uncredentialed.

   **OIDC SSO (WS4, `src/agent/oidc.rs`).** When `GossipConfig::oidc` is set, the
   middleware tries OIDC *first*: a JWT bearer is validated against the IdP's
   JWKS and its groups mapped to scopes, which then feed the *same* scope gate —
   so an OIDC principal is authorized exactly like a scoped token, just
   signature-authenticated. **Security:** `validate_token` enforces an
   asymmetric-only algorithm allowlist (RS*/ES*/PS*) *before* key selection and
   pins verification to the vetted alg — closing JWT alg-confusion (HS256 with
   the public key as MAC secret → rejected). `iss`/`aud`/`exp` all checked.
   `OidcVerifier` caches the JWKS (TTL + refresh-on-unknown-kid; discovery via
   `.well-known/openid-configuration`). Human-operator auth, **not** agent
   identity. A forged/expired JWT matches no static token → 401.

4. **`sys/` namespace-ownership tripwire (Layer I, core — not compliance-gated).**
   `sys/identity|load|role|tuple/{node}` is owned by `{node}`; only that node
   should originate writes. The connection handler flags an *inbound* (remote)
   write naming **self** in one of these prefixes — `warn!` + a cumulative
   `SystemStats::sys_namespace_violations` counter (on `/stats`). **Detection
   only**: the write still applies per LWW, exactly like the consensus
   commit-conflict tripwire — do **not** turn this into an `apply_and_notify`
   write guard. `sys/quorum/` is excluded (peers legitimately attest about the
   observed node). Signed keys (`identity`, `role`) also fail verification at
   read; unsigned keys (`load`, `tuple`) rely on this signal alone.

**Gates:** `cargo test --lib --features compliance` and
`cargo clippy --lib --tests --features compliance -- -D warnings`. Cross-node
integration scenarios in `src/lib_tests.rs`
(`test_ws1_rbac_signed_roles_propagate_and_authorize_across_nodes`,
`test_gateway_scoped_token_acl_end_to_end`,
`test_sys_namespace_tripwire_flags_foreign_self_owned_write`).

### Audit trail (compliance feature) — WS2

Tamper-evident, signed, hash-chained audit trail. Core in
[`src/agent/audit.rs`](src/agent/audit.rs); endpoint in
[`src/agent/http.rs`](src/agent/http.rs); SkillRunner integration in
`src/bin/skillrunner/`. Plan: `docs/plans/v1x-completion.md` §WS2.

- **Per-node chains, by necessity.** Records live at `sys/audit/{node}/{seq:016x}`;
  each `prev_hash` is the SHA-256 `content_hash` of the predecessor in *that node's*
  stream (genesis = zero). A single global chain would need a sequencer — a
  coordinator — which violates principle #1, so the chain is per-node and the
  cluster trail is the union of independently verifiable streams. `sys/audit/` is
  **not** in the `SELF_OWNED_SYS_PREFIXES` tripwire set (many nodes write audit
  entries; only `identity|load|role|tuple` are single-owner).
- **`SignedAuditRecord`** = Ed25519 over canonical record bytes (reuses the tls
  identity, like `SignedRoleClaim`). `agent.audit(action, principal, target,
  outcome, detail)` seals + writes; `audit_stream` / `audit_verify` /
  `audit_stream_nodes` read + verify. `verify_chain` returns a precise
  `AuditVerifyError` (BadSignature / BrokenLink / SequenceGap / WrongOwner /
  UnknownSigner) naming the offending `seq`.
- **Sealing concurrency.** The chain head (lock #8) is held only for seq/prev_hash
  assignment + `content_hash` + head advance (~µs); signing (~tens of µs) and the
  KV write happen **after** the lock releases — do not move signing back under it.
- **`GET /gateway/audit`** (scope `audit:read`, deny-by-default): per-stream
  `verified` + `head_hash` + per-record `content_hash`. The `content_hash` is the
  stable, M16-citable identifier (named for what it is, never an AgentFacts field).
- **SkillRunner** routes every invocation through `agent.audit(Invoke, verified-caller,
  ns/name, outcome)` under `compliance` (the read-side principal binding is the
  *served-path* verified caller); the plain `audit/{ts}` writer remains only as the
  non-compliance fallback. **Detection-not-prevention**: records are plain KV
  entries; tampering makes `verify_chain` fail, it is never blocked at the store.

**Gates:** as WS1, plus `src/lib_tests.rs::test_ws2_audit_chain_writes_and_verifies_on_a_node`,
`src/agent/http.rs::test_gateway_audit_endpoint_verifies_and_scope_gates`, and the
10 `audit::tests` chain/tamper unit tests.

### Crown-jewel posture (data-at-rest + egress) — WS3

Two **feature-free, opt-in** blast-radius controls (no cargo feature; zero
overhead when unused). Threat model: [`docs/threat-model.md`](docs/threat-model.md);
runbook: [`docs/operations/crown-jewel.md`](docs/operations/crown-jewel.md).

- **Data-at-rest hook.** `DataAtRestCipher` trait (`src/persistence.rs`,
  re-exported at crate root) + `GossipAgent::with_data_at_rest_cipher` (set once,
  `OnceLock`, before `start`). Applied at the four on-disk boundaries — WAL append
  (encrypt), WAL replay (decrypt; failure = corrupt tail, stop), snapshot write
  (encrypt), snapshot read (decrypt; failure = skip). The length prefix frames the
  ciphertext. Substrate is neutral on key custody (operator wraps a KMS); scope is
  **on-disk only** (wire = `tls`, memory = plaintext). Decrypt failure reuses the
  existing corrupt-record path — no new failure mode.
- **Egress allowlist.** `EgressPolicy { allow_hosts }` in `GossipConfig`
  (serializable, default empty = allow-all). `permits_host` (exact + `.suffix`
  subdomain, case-insensitive) / `permits_url` (fail-closed on unparseable host).
  Enforced at **every outbound HTTP path the substrate chooses**: MCP client
  bridge (`connect_mcp_server`), capability probes (`passes_probe`), core LLM
  prompt skills (`handle_llm_invoke` via `LlmBackend::endpoint()`), and the
  SkillRunner LLM call (`agent.egress_policy()`). Intra-cluster bulk fetches and
  operator-configured OIDC JWKS are intentionally not gated (the A2A *client* is
  SDK-side). A node-local posture, not a coordinator.

**Gates:** `src/lib_tests.rs::test_ws3_data_at_rest_cipher_encrypts_wal_and_round_trips`
(default feature set — on-disk plaintext absence, same-key recovery, wrong-key
rejection), the `config::tests::egress_*` gate unit tests, and
`src/agent/mcp.rs::test_mcp_egress_policy_denies_disallowed_host`.

### Hot cert / identity rotation (tls feature) — WS5

Rotate a node's Ed25519 TLS/identity key live, with no cluster disruption.
Runbook: [`docs/operations/cert-rotation.md`](docs/operations/cert-rotation.md).

- **Swappable identity.** `NodeTls` holds its signing key + rustls server/client
  configs behind lock-free `arc_swap::ArcSwap` cells (the `TaskCtx::tls` *handle*
  is still set-once; its *contents* rotate). Read them through the accessor
  methods (`signing_key()`, `server_config()`, …) — never cache a clone past a
  rotation. `tls_accept`/`tls_connect` call the config accessors **per connection**,
  so a cutover is live (new connections use the new cert; existing sessions keep
  their CA-trusted one) — **no listener drain-swap**. ArcSwap is lock-free, so no
  lock-order-table entry.
- **`rotate_identity(propagation)`**: generate (CA-signed, persisted, not active)
  → publish `sys/identity/{self}` = `new‖old` signed by the **old** key → wait →
  `NodeTls::activate` swaps atomically. Order matters: publish-before-cutover so
  peers accept the new key first.
- **Retained-key verification (option B).** `peer_keys` is a `Vec<[u8;32]>` per
  node, **accumulated** (union; `helpers::merge_peer_keys`), never mirrored — so a
  rotation never drops a still-needed historical key. Every verify path tries the
  set: `connection` SignedData (fail-open preserved), `consensus` decode_verify,
  `rbac` roles_of, `audit` verify_stream (via `verify_chain_keys`). `sys/identity/{node}`
  stores the **full** key history (`32 × N`, current first; `helpers::{parse_identity_keys,
  encode_identity_history}`), so verification survives any number of rotations + restarts
  (grows 32 B/rotation). **Compromise caveat:** a retired key stays accepted for
  verification — compromise needs explicit revocation, not just rotation.

**Gates:** `src/lib_tests.rs::test_ws5_rotate_identity_verifies_across_rotation_on_peer`
(two tls nodes; A rotates mid-stream; peer B verifies A's audit chain across the
rotation), `audit::tests::chain_spanning_a_key_rotation_verifies_against_the_key_set`.

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

- **Workspace (v2 M1):** the substrate is the `mycelium-core` crate; `mycelium` is the full
  crate that depends on it. Build/test both: `cargo build`, `cargo test`, `cargo clippy
  --all-targets` (workspace-wide), or `-p mycelium-core` / `-p mycelium` to scope. The
  `mycelium-core/tls|metrics|compliance` features are forwarded from `mycelium`'s; its
  `test-support` feature (enabled via `mycelium`'s dev-dependency) exposes core's
  `#[cfg(test)]` helpers across the crate boundary. Workspace members now also include the
  companion crates (`mycelium-tuple-space`, `mycelium-wasm-host`, `mycelium-agentfacts`) and the
  `examples/coop` suite (`mycelium-coop-examples`) — so a workspace-wide `cargo build` pulls
  `wasmtime` (via wasm-host, used by coop demos 04/09); scope with `-p` to skip it.
- `cargo build --lib`, `cargo test --lib`, `cargo clippy --lib --tests` (the full `mycelium` crate)
- `cargo build -p mycelium-core` — the embeddable Layers I+II substrate, standalone (≈48 deps
  vs ≈140 for `mycelium`; no axum/hyper/gateway). The dep-tree win M1 exists for.
- `cargo build --lib --no-default-features` to verify the gateway-free embedded build
- **`consensus` feature (default-on, v2 M2)** gates Layer III: the epidemic consensus engine +
  the consistency overlay built on it (`consistent_set`/`get`/distributed lock). Drop it for
  minimal embeds: `--no-default-features --features gateway`. A consensus-disabled node still
  *forwards* PROPOSE/VOTE/COMMIT (forwarding is in `mycelium-core`), it just never acts;
  `suggest_leader` degrades from trust-weighted to pure-load. Verify with
  `cargo test --lib --no-default-features --features gateway`.
- `cargo build --lib --features metrics` to include the Prometheus scrape endpoint
- `cargo build --lib --features a2a` to include the A2A protocol adapter
- `cargo build --lib --features llm` to include the Prompt Skills LLM adapter
- `cargo build --lib --features compliance` to include RBAC / signed identity roles,
  OAuth2 gateway ACLs, and the tamper-evident hash-chained audit trail. **v1.x is
  engineering-complete: WS1 RBAC, WS2 audit, WS3 crown-jewel (feature-free),
  WS4 OIDC SSO, WS5 hot cert rotation all shipped** — see the per-WS sections above
  and [`docs/plans/v1x-completion.md`](docs/plans/v1x-completion.md).
- Lib tests at HEAD (totals unchanged by M1/M3 — tests moved with their modules, none lost):
  **318** default · **323** `tls` · **365**+ `compliance` across the workspace, now split
  between the two crates — default = **82** `mycelium-core` + **236** `mycelium` (the v2-M3
  handle pushdown moved the `SchemaHandle`/`KvHandle`/`MeshHandle` pure tests into core and
  relocated the `GossipAgent`-driven cases to `src/agent/{schema,kv,mesh}_handle_tests.rs`).
  Clippy `-D warnings` clean on both crates. The `compliance` delta is the WS1 RBAC + WS2 audit
  + WS4 OIDC + WS5 rotation unit/integration tests; the core `sys/` tripwire and WS3 crown-jewel
  (data-at-rest + egress) tests are feature-free and in the default count. WS5's retained-key-set
  verification + multi-key archival live under `tls`. (These M3-era totals are stale: M4/M5 SWIM
  and the WS-B M11 codec / `serde_fixint` / Merkle-digest suites added to the core count —
  `mycelium-core --lib` is **117** at HEAD. The codec equivalence tests use `bincode` as a
  dev-dependency oracle; it is not in the shipped tree.)
- Wire version is currently **v12** (`PREV_WIRE_VERSION = 11` — rolling upgrade window open).
  v12 (WS-B M11) switches anti-entropy from the full `key_timestamps` index to a Merkle
  digest: `WireMessage::StateRequest` now carries `bucket_hashes: Vec<u64>` (a 256-bucket
  per-bucket XOR digest of the live store — `store::store_bucket_hashes`), and the responder
  returns only entries in divergent buckets (`O(divergence)` vs `O(keys)`). v11 peers send a
  `key_timestamps` request; `WireMessageV11` / `codec::decode_wire_v11` downgrade it to
  `bucket_hashes = vec![]` (full-snapshot sentinel). v11 added `hlc_seq` to `Signal`; v10 added
  `SignedData`.
- **Serialization (WS-B M11): `bincode` is retired** (RUSTSEC-2025-0141 — no longer in the
  shipped dependency tree, kept only as a `mycelium-core` dev-dependency test oracle). Two
  in-tree codecs replace it, both byte-identical to the old `bincode 2.x` fixed-int format
  (proven by equivalence tests, so no on-disk migration or signature/hash-chain breakage):
  `mycelium_core::codec` (hand-rolled `WireMessage` encoder/decoder preserving the zero-copy
  Data offsets) for the gossip hot path, and `mycelium_core::serde_fixint` (a generic serde
  binary format) for every other `#[derive(Serialize,Deserialize)]` type (persistence,
  capabilities, consensus, audit, RBAC, SWIM datagrams). `framing::bincode_cfg()` is now
  `#[cfg(test)]`.
- **Food-Rescue Co-op example suite**: `examples/coop/` — **eleven** runnable demos that exercise the
  newer capability surface, composed in *one constructive world* (a co-op of depot nodes rescuing
  surplus food, no central dispatcher) rather than isolated API toys. Standalone workspace member
  (`mycelium-coop-examples`) depending on `mycelium` + the three companion crates; each demo is its
  own `[[bin]]`. **01** `mailbox_llm` (actor↔LLM via the HLC-ordered mailbox) · **02** `stigmergy`
  (load-shed via `sys/load` pheromone) · **03** `elastic_intent` (management-as-intent / elastic
  `MembershipGovernor`, operator-optional) · **04** `provisioning` ⭐ (the autonomic loop —
  tuple-space buffer + WASM self-provision + failover) · **05** `federation_facts` (cross-domain
  self-certified AgentFacts discovery) · **06** `rotation` (zero-disruption identity rotation;
  retained-key verify) · **07** `consensus` (Layer III cross-group multi-bloc agreement + leased
  decay) · **08** `llm_pipeline` (homogeneous LLM workers, competitive pull) · **09** `mcp_toolgrowth`
  (an LLM agent grows the fabric's toolset — MCP tool loaded on demand) · **10** `llm_council` (a
  council of *differentiated* LLM agents — fan-out → synthesis → iterative refinement; names the
  keyed-fan-in M13 boundary) · **11** `catalog` (the cluster-wide artifact catalogue — gossiped
  `installable/`, register/discover/pull-over-mesh/provision; no registry server). Shared harness in
  `src/common/` (bootstrap + domain types + `facts_lens` mounting the AgentFacts edge on every depot).
  `examples/coop/ci_smoke.sh` runs all eleven Docker-free (retry-hardened for constrained runners) and
  is wired into CI as the `coop-smoke` job. Plan + shipped-status table:
  [`docs/plans/example-suite.md`](docs/plans/example-suite.md); per-demo descriptions:
  [`examples/coop/README.md`](examples/coop/README.md). **The suite anchors the developer docs** (see
  the docs-alignment plan [`docs/plans/docs-and-examples-alignment.md`](docs/plans/docs-and-examples-alignment.md)):
  guide [`00-concepts.md`](docs/guide/00-concepts.md) (native model — Capability/Skill — vs edge
  standards — A2A/MCP/AgentFacts), [`14-patterns-and-pitfalls.md`](docs/guide/14-patterns-and-pitfalls.md)
  (grounded in these examples), the [`cookbook.md`](docs/guide/cookbook.md) ("how do I…"), and the
  two-audience [`docs/operations/`](docs/operations/) runbooks (deployment, observability/AgentFacts,
  dynamic-scaling, artifacts). Built-while-stress-testing: surfaced/fixed the
  governor-vs-emergent-autojoin bug (#56→#57), the `crdt.rs` retained-key gap (#51), and filed #55
  (cross-node Individual-scoped signals). Retired `prompt_skill_demo`→`mailbox_llm` (+ live
  template-update §05) and `mesh_demo`→`llm_agent`.
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
