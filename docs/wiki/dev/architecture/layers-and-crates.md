# The layer model and the crate split

↑ [architecture](architecture.md) · siblings: [runtime-invariants](runtime-invariants.md)

## Three layers, one substrate

| Layer | What it does | Where |
|---|---|---|
| **I — KV store** | Last-write-wins state propagation over TCP; anti-entropy synced (Merkle-bucketed since wire v12). | `mycelium-core/src/{store,connection,framing,writer,seen}.rs` |
| **II — Signal mesh** | Ephemeral scoped events; per-node admission boundaries; pheromone-style opacity. | `mycelium-core/src/{signal,mesh_handle}.rs`, `src/agent/opacity.rs` |
| **III — Consensus** | Epidemic group/system/cross-group proposals; `GroupQuorum` + `cross_group_propose`. | `src/consensus.rs`, `src/agent/consensus_ops.rs` |

Plus the capability/requirement subsystem (emergent groups, wiring, demand pressure —
`src/capability.rs` + `src/agent/{capability_ops,wiring,emergent_groups,demand}.rs`) and HLCs
for causal LWW ordering (`mycelium-core/src/hlc.rs` — read its module doc for the drift-bound
rationale).

**Two distinct "TTL"s — never conflate** (a documented calibration-ledger drift, Runs 10–15):
wire frames carry a hop-count TTL (`u8`, decremented per forward); key **evaporation** is a
read-side convention (`CapEntry::is_fresh`: entries older than 3× their `refresh_interval_ms`
— or stamped further than 3× in the *future* — read as gone). The store never time-evicts live
keys; only tombstones are GC'd.

## Core design rules

1. **Single KV substrate.** Higher layers own key prefixes (table in `src/lib.rs`) and write
   via `make_gossip_update` + `apply_and_notify`. Documented, not a layer violation.
2. **Opacity composition.** Every reason a node is opaque is a distinct `sys/load/{self}/…`
   key with `is_opaque = true`; `is_self_opaque()` scans the prefix. New opacity causes = new
   keys, no new mechanism.
3. **HLC ordering.** Local writes tick, received updates observe — a local write after a
   remote observation is strictly greater. LWW compares the packed `u64`.
4. **Emergent groups.** Nodes self-join `CapabilityGroupDef`s by evaluating their own
   capabilities; no coordinator assigns membership.
5. **Inter-group wiring is per-emission.** `signal_wired_via(filter)` re-resolves at each
   call; there is no stored binding.
6. **TLS is opt-in and transport-only.** One Ed25519 keypair serves transport, identity
   (`sys/identity/{node}`), and consensus signing. `NodeTls` is always defined (zero-size
   without the feature).

## The crate split (v2 M1) — inversion as a compile-time guarantee

Layers I+II live in **`mycelium-core`** (≈48 deps, no axum); the full `mycelium` crate
(consensus, capabilities, services, gateway, tls) depends on it. `mycelium-core` cannot
reference `mycelium` — it would be a Cargo cycle — so "the substrate is never aware of the
layers above" is enforced by the compiler. The boundary was drawn *around* the sanctioned
Layer I/II entanglement rather than severing it. Execution record:
`docs/plans/v2-m1-mycelium-core.md`.

**The two named Layer I/II crossing points** (the only places either layer reaches into the
other): `apply_and_notify` (`mycelium-core/src/store.rs` — writes the store, then notifies
`KvState::subscriptions`) and `subscribe`/`subscribe_prefix`
(`mycelium-core/src/{kv_handle,ops}.rs`). New single-layer features stay in their layer.

## CoreCtx / TaskCtx

The former 22-field God Object is split: `mycelium_core::CoreCtx`
(`mycelium-core/src/context.rs`) carries the Layers I+II infrastructure (`node_id`, `config`,
`seen`, `hlc`, `gossip_txs`, `kv_state`, `wal`, signal state, `tls`, `peers`, `shutdown_tx`,
`task_handles`, `spawn_task`, `soft_state_advertised`); `src/agent/mod.rs::TaskCtx` holds
`core: Arc<CoreCtx>` plus Layer III+ fields (`rpc_pending`, `commit_conflicts`,
`audit_chain`, LLM state…) and `Deref`s to core, so `ctx.<core-field>` call sites are
unchanged. The three core↔upper couplings are `None`-safe hooks (`reply_interceptor`,
`QuorumObserver`, `SnapshotDeferHook`) — pure-core embeds run without them.
