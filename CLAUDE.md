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
| **II — Signal mesh** | Ephemeral scoped events with per-node admission boundaries; pheromone-style opacity composition. | `src/signal.rs`, `src/agent/signal_ops.rs`, `src/agent/opacity.rs` |
| **III — Consensus** | Epidemic group / system proposals with optional Hard topology enforcement. | `src/consensus.rs`, `src/agent/consensus_ops.rs` |

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

## Active follow-up plans (memory)

These are real work items captured in the memory directory at
`~/.claude/projects/-Volumes-Scratch-Gossip/memory/`. Anyone resuming
should read [`MEMORY.md`](~/.claude/projects/-Volumes-Scratch-Gossip/memory/MEMORY.md)
for the index.

| Plan | What's pending |
|---|---|
| `plan_signal_reorder_buffer.md` | Receiver-side per-(sender, kind) HLC-keyed reorder buffer for causal signal delivery |
| `plan_watcher_scalability.md` | C1 predicate-based prefix subscribe + C2 reconcile debounce + C3 per-group task consolidation |
| `plan_fuzz_harness.md` | cargo-fuzz targets for WireMessage + capability decoders |
| `plan_layer_coherence_refactor.md` | E1 SignalHandlers split + E4 ConsensusEngine::propose extraction |
| `plan_locality_topology_capabilities.md` | Original feature plan (Phases 0–9; Phase 8 cross-group consensus federation still deferred to its own follow-up) |

## Working in this repo

- `cargo build --lib`, `cargo test --lib`, `cargo clippy --lib --tests`
- 202 tests at HEAD; clippy at baseline 53 (pre-existing
  `field_reassign_with_default` in test code).
- Wire version is currently **v9** with `PREV_WIRE_VERSION = 9`
  (i.e., no legacy frames accepted). Bumping requires a follow-up
  plan because timestamps are HLC-packed under v9.
