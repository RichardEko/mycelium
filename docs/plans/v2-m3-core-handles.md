# v2 M3 — core-handle pushdown: working plan

Branch: `v2/m3-core-handles`. Milestone: ROADMAP §v2.0 M3 (WS-A). Execution record; canonical
milestone framing is in `ROADMAP.md` / `docs/plans/v2.0.md`.

## Finding that reshaped the milestone

M3's literal goal — *"ownable values that do not require a live `GossipAgent` borrow… passable
across crate boundaries without exposing `GossipAgent`"* — is **already satisfied** by the
current `Arc<TaskCtx>`-based handles (`Clone + Send + Sync`, public methods; the tuple-space
companion already uses them cross-crate). The pre-M1 one-liner predates the split.

The genuinely-valuable, architecturally-clean residual (chosen 2026-06-15, "cleanest
architecture, take the pain now"): **push the Layer-I/II handles down into `mycelium-core`**,
backed by `Arc<CoreCtx>`, so a core-only embedder (no `GossipAgent`) gets real KV + signal +
schema handles. This completes M1's explicitly-deferred handle-layer pushdown.

## The cut (verified against field/type usage)

| Handle | Disposition |
|---|---|
| `MeshHandle` | → **core**. Pure Layer II; needs `emit_signal*` + a few kv/group helpers moved with it. |
| `SchemaHandle` | → **core**. Pure core (no upper refs). |
| `KvHandle` | → **core** for all Layer I methods (`set`/`get`/`subscribe`/`scan`/`delete`/`keys`/`append`/`scan_log`/`compact_log`/`subscribe_log`/`subscribe_log_group`/`quorum_persistent`). Its **one** upper-typed method `set_with_min_acks` (builds `kv_quorum::QuorumAckTracker`) moves to an extension trait. |
| `CapabilitiesHandle` (`filter_opacity_registry`), `ServiceHandle` (`bulk_transport`), `ConsensusHandle` (consensus-gated, holds `consistent_*`/lock) | **stay upper** — genuinely Layer III+. |

**Architecturally correct, not a compromise:** the substrate `KvHandle` does substrate KV; the
quorum-durability overlay (`set_with_min_acks`) becomes `KvQuorumExt` in `mycelium` — *"consistency
as a service, not a foundation"* made explicit in the type system. Cost: callers of
`set_with_min_acks` add `use mycelium::KvQuorumExt;` (a deliberate, legible API adjustment).

## Helpers to move `mycelium` → `mycelium-core` (change `&TaskCtx` → `&CoreCtx`)

From `agent/helpers.rs`: `emit_signal`, `emit_signal_ordered`, `emit_signal_async`,
`deliver_locally`, `kv_get`, `kv_set`, `kv_set_async`, `kv_delete`, `kv_delete_async`,
`kv_subscribe`, `kv_subscribe_prefix`, `kv_subscribe_prefix_with_predicate`, `kv_scan_prefix`,
`group_members_ctx`. From `agent/kv.rs`: `run_kv_persist_task` + `PersistPayloadFn`/`PersistOnTickFn`.

- They only touch `CoreCtx` fields, so `&TaskCtx` → `&CoreCtx` is mechanical; **upper call sites
  pass `&TaskCtx` which Deref-coerces to `&CoreCtx`** (no call-site churn).
- `emit_signal*`'s one blocker — the local `rpc_pending` fast-path — is replaced by the M1
  `CoreCtx::reply_interceptor` (mechanism-in-core; the RPC correlation closure is already
  registered there by the upper service layer).
- `make_gossip_update`/`apply_and_notify`/framing are **already in core** (M1).

New core home: `mycelium-core/src/ops.rs` (free helper fns) + the three handle modules moved in.

## Stage sequence (gated)

| Stage | Work | Gate / Status |
|---|---|---|
| 1 ✓ | Move emit/kv/group helpers to `mycelium-core::ops` (`&CoreCtx`, reply_interceptor); `helpers.rs` re-exports | **committed `edd8bb2`** — both crates build (consensus on/off), clippy clean |
| 2a ✓ | Move `SchemaHandle` to core (`Arc<CoreCtx>` + `#[doc(hidden)] from_core`); relocate `GossipAgent` tests | **committed `6389025`** — core 81 tests, upper green, clippy clean |
| 2b | Move `MeshHandle`: core methods (emit/join/leave/subscribe/signal_rx/suppress) → core; `advertise`/`manage_opacity` → upper `MeshTaskExt` (they use `run_kv_persist_task`, whose `on_tick` is `TaskCtx`-typed + sets `caps_advertised`) | full matrix builds |
| 3 | Split `KvHandle`: `LogEntry` + core methods → core; `SubscribeHandle` (overlay) stays upper; `set_with_min_acks` → `KvQuorumExt` in `mycelium` (needs `#[doc(hidden)] pub fn core(&self)->&Arc<CoreCtx>` on the core handle); relocate 238-line test module | API compiles; tuple-space + tests updated |
| 4 | Full gates: default / no-consensus / matrix tests + clippy `-D warnings`; `mycelium-core` standalone | CLAUDE.md posture |
| 5 | Philosophy compliance review (substrate handles in core; overlay/task-helper methods explicit as ext traits above) | sign-off |

**Pattern proven by Stage 2a (`SchemaHandle`):** each handle move = (1) `git mv` to core,
(2) `ctx: Arc<CoreCtx>` + `#[doc(hidden)] pub fn from_core(ctx)`, (3) imports → `crate::ops`,
(4) split the test module — pure tests stay in core, `GossipAgent`-driven tests → an upper
`*_tests.rs`, (5) core `lib.rs` `pub mod` + re-export, (6) upper `agent/mod.rs` re-export from
`mycelium_core` + accessor → `Handle::from_core(Arc::clone(&self.task_ctx.core))`. Handles with
upper-coupled methods additionally need an **upper extension trait** (`KvQuorumExt`,
`MeshTaskExt`) + a `pub fn core()` accessor on the core handle. New core dep discovered:
`serde_json` (schema validation).

**Invariant:** `mycelium-core` still references nothing upper; the `layer1_…` purity guard and
the M1 inverted-dependency compile-time guarantee must hold.
