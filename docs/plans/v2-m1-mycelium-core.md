# v2 M1 — `mycelium-core` extraction: working plan

Branch: `v2/m1-mycelium-core`. Milestone: ROADMAP §v2.0 M1 (WS-A keystone). This is the
execution record; the canonical milestone design is in `ROADMAP.md`.

## Philosophy binding (reviewed against `docs/philosophy.html`)

M1 is the **Layering Principle made physical**: the substrate = Layers I + II (KV + signal
mesh), which "has no concept of agreement, coordination, or workflow"; everything else *uses*
it. The cut must honour three things:

1. **The II↔III seam** is the crate boundary. `mycelium-core` = Layers I + II only.
2. **Inverted dependency** (philosophy §5a): `mycelium-core` must never reference a Layer III
   type — the substrate cannot become aware of the layers above it.
3. **Library, not platform** (Paremus lesson): core stays an embedded library; no daemon.

## Stage 0 — seam map (COMPLETE)

Import-graph scan of every candidate-core module:

- **Clean core (Layers I+II):** `seen, hlc, error, stream, tls, locality, node_id, framing,
  config, persistence, store, signal` + agent-level Layer II (`kv, kv_handle, mesh_handle,
  opacity, signal_ops`). Only lateral/downward deps. `locality` is a leaf value type → core.
- **The one structural entanglement:** `connection.rs` (Layer I transport) is parameterised
  over `agent::TaskCtx` — the 22-field God Object spanning all three layers. This is exactly
  what `CoreCtx` carves.
- **Two benign references (not blockers):** `store.rs`→`kv_quorum` is test-only;
  `helpers.rs`→`consensus_ns` is one group-resolution helper that relocates to the upper crate.

## The carve design (validated against the live struct)

Technique: **`impl Deref<Target=CoreCtx> for TaskCtx`** — the same pattern `KvState`→`KvStore`
already uses. Field access auto-derefs, so all ~380 `ctx.<corefield>` sites and the two
`lifecycle.rs` `OnceLock.set` sites are **untouched**. Only `connection.rs`/`writer.rs`
signatures move to `CoreCtx`, and only at Stage 3 (physical move) — Stage 1 leaves them on
`TaskCtx` (still resolves via Deref while in one crate).

**TaskCtx 22-field classification:**

| → `CoreCtx` (Layers I+II + identity/networking/lifecycle/transport-security) | → stays in `TaskCtx` (Layer III+) |
|---|---|
| `node_id`, `config`, `default_ttl` | `caps_advertised`, `filter_opacity_registry`, `group_roster_cache` |
| `seen`, `hlc`, `gossip_txs`, `kv_state`, `wal` | `llm_skills`, `llm_dispatch_spawned` (cfg llm) |
| `signal_boundary`, `signal_handlers`, `reorder_buf` | `bulk_transport`, `rpc_pending` |
| `peers` | `commit_conflicts` (consensus listener tripwire) |
| `shutdown_tx`, `task_handles` | `audit_chain` (cfg compliance) |
| `tls`, `peer_keys` (connection.rs SignedData verify) | |
| `sys_namespace_violations` (connection.rs inbound tripwire) | |

`commit_conflicts` stays (incremented only by the Layer III consensus listener);
`sys_namespace_violations` is core (incremented by the connection handler's inbound `sys/`
tripwire). `tls`/`peer_keys` are core (connection-layer verification needs them).

**Three constructor sites to split:** `agent/mod.rs:636` (real), `lib_tests.rs:119`, `:526`.

## Stage sequence (each ends at a build/test gate)

| Stage | Work | Gate |
|---|---|---|
| 0 ✓ | branch, philosophy, seam map, carve design | done |
| 1 ✓ | Carve `CoreCtx` from `TaskCtx` in place (+`Deref`); fix 3 constructors | full build + tests green, one crate — **committed** |
| 2 ✓ | Decouple `connection.rs` from `rpc_pending` via the `ReplyInterceptor` hook | zero core→III refs *in the transport modules* — **committed** |
| 2.5 ✓ | Resolve three core→upper couplings the Stage-2 scan missed (see below) | core production code references no upper type — **committed** |
| 3a ✓ | Stand up `mycelium-core`; move the **leaf** modules (`error`, `hlc`, `seen`); prove the mechanism end-to-end | both crates build/test/clippy green — **committed** |
| 3b | Move the interdependent transport cluster (`store, connection, framing, writer, signal, persistence, stream, tls, locality, config`) + `CoreCtx`; `connection`/`writer` → `CoreCtx`; relocate the `store.rs` quorum test | `mycelium-core` builds standalone |
| 4 | `mycelium` depends on core; re-export for API stability; fix paths | full feature matrix builds |
| 5 | Tests green (318/323/365), clippy clean, no-default-features | CLAUDE.md test posture |
| 6 | Philosophy compliance review (no core→III; library-not-platform; seam at II↔III) | sign-off |

## Stage 2 decisions (the de-coupling)

- **Consistency overlays stay upper.** Philosophy: *"Consistency as a service, not a
  foundation."* `kv_quorum` and `overlay_consistent` (and `KvHandle`'s `consistent_*` methods)
  are higher-order → they remain in `mycelium`, not `mycelium-core`. `kv_handle.rs`'s
  references to them are therefore fine (handle layer is upper).
- **The RPC fast-path coupling is gone.** `connection.rs` no longer reads `rpc_pending` (a
  Layer III field). Core now exposes `CoreCtx::reply_interceptor: Option<ReplyInterceptor>`;
  the upper layer registers a closure (capturing `rpc_pending`) at agent construction that
  claims correlated `rpc.result`/`bulk.result` replies. Core asks only "did anything claim
  this signal?" — mechanism in core, RPC law above. Verified by the RPC tests.
- **Minimal-core decision.** `mycelium-core` = the 14 substrate modules (`store, connection,
  framing, writer, seen, signal, hlc, node_id, error, config, persistence, stream, tls,
  locality`) + `CoreCtx`. The agent **handle/ops layer** (`kv_handle`, `mesh_handle`,
  `helpers`, …) stays in `mycelium` and is re-exported — it's the ergonomic API *over* the
  core mechanism, holds `Arc<CoreCtx>`/`Arc<TaskCtx>`, and pulls in nothing the substrate
  modules need. This is the minimal correct cut for M1; pushing the handle layer down too is
  a later refinement, not required.
- **Stage 3 mechanical note:** the `store.rs` `concurrent_quorum_trackers_coexist…` **test**
  references `kv_quorum`; it relocates to the upper crate alongside `kv_quorum` during the
  physical move (it tests an overlay, not core storage).

## Stage 2.5 — two core→upper type couplings the Stage-2 scan missed

The Stage-2 scan used `(crate|super|self)::(UPPER)` and so **missed the `crate::agent::X` form** —
a real blind spot. The Stage-3 pre-flight check found two production type-dependencies from core
types into upper modules:

1. **`KvState.quorum_trackers`** (`store.rs:77`) is typed `crate::agent::kv_quorum::TrackerList`
   (upper). Core *uses* it: `apply_and_notify` (`store.rs:611`) calls `tracker.observe(sender,
   timestamp)` on each echoed write. **Fix (same pattern as `ReplyInterceptor`):** define a core
   trait `QuorumObserver { fn observe(&self, sender: u64, timestamp: u64); }`; make
   `quorum_trackers` hold `Arc<dyn QuorumObserver>`; the upper `QuorumAckTracker` implements it.
   `install_tracker`/`remove_tracker` operate on the trait object (identity removal via
   `Arc::ptr_eq` still works). Mechanism in core, the ack-counting law above.
2. **`GossipConfig.oidc`** (`config.rs:598`) is typed `crate::agent::oidc::OidcConfig` (upper).
   **Fix:** `OidcConfig` is a plain serde config struct (+ pure `scopes_for_groups`) → move it to
   `config.rs` (core, which is config's home anyway). The OIDC *verifier* (`oidc.rs`, jsonwebtoken,
   `OidcVerifier`) stays upper and imports `crate::config::OidcConfig`.

3. **`persistence.rs` → `agent::is_self_opaque`** (`persistence.rs:332`, found in the same
   preflight once the scan caught the `crate::agent::` form). The WAL snapshot loop (core Layer I)
   called the Layer II opacity predicate to defer a snapshot when the node is already opaque.
   `is_self_opaque` can't trivially move to core (it depends on the `LoadState` encoding in
   `opacity.rs`). **Fix (hook pattern again):** `spawn_wal_writer` takes an optional
   `SnapshotDeferHook = Arc<dyn Fn() -> bool + Send + Sync>`; the upper layer (`lifecycle.rs`)
   supplies `|| is_self_opaque(...)`. Core consults the closure, unaware of `sys/load/`. `None`
   for pure-core embeds (never defers).

All three are contained, gateable, in-crate fixes (no file move) and prerequisites for Stage 3.
**Done:** default + no-default + full-matrix build, lib tests 318/383 (0 failed), clippy
`-D warnings` clean. The only remaining core→upper refs are the two Stage-3 mechanical items:
`connection.rs`'s `TaskCtx` (→ `CoreCtx` at the move) and the `store.rs` quorum **test** (relocates
with `kv_quorum`).

**Compliance review criteria for Stage 6:** (a) `grep` shows zero `mycelium-core` →
consensus/capability/service references; (b) `mycelium-core` has no `daemon`/control-plane
surface; (c) the public API is unchanged (re-exports); (d) `CoreCtx` contains only the
classified core fields.
