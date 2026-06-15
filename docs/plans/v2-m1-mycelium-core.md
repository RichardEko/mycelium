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
| 1 | Carve `CoreCtx` from `TaskCtx` in place (+`Deref`); fix 3 constructors | full build + tests green, one crate |
| 2 | Split `helpers.rs` (core vs upper); relocate `kv_quorum` hooks | zero core→III refs |
| 3 | Create `mycelium-core` member; physically move core modules; `connection`/`writer` → `CoreCtx` | `mycelium-core` builds standalone |
| 4 | `mycelium` depends on core; re-export for API stability; fix paths | full feature matrix builds |
| 5 | Tests green (318/323/365), clippy clean, no-default-features | CLAUDE.md test posture |
| 6 | Philosophy compliance review (no core→III; library-not-platform; seam at II↔III) | sign-off |

**Compliance review criteria for Stage 6:** (a) `grep` shows zero `mycelium-core` →
consensus/capability/service references; (b) `mycelium-core` has no `daemon`/control-plane
surface; (c) the public API is unchanged (re-exports); (d) `CoreCtx` contains only the
classified core fields.
