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

These are real work items captured in the memory directory at
`~/.claude/projects/-Volumes-Scratch-Mycelium/memory/`. Anyone resuming
should read [`MEMORY.md`](~/.claude/projects/-Volumes-Scratch-Mycelium/memory/MEMORY.md)
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
- `cargo build --lib --features metrics` to include the Prometheus scrape endpoint
- `cargo build --lib --features a2a` to include the A2A protocol adapter
- 239 tests at HEAD (with `--features a2a`); 235 without; clippy at baseline 61
  (pre-existing `field_reassign_with_default` in test code).
- Wire version is currently **v10** (`PREV_WIRE_VERSION = 9` — rolling upgrade window open).
  v10 adds `WireMessage::SignedData` for Ed25519-signed KV writes under the `tls` feature.
- **Agentic Flow Networks demo**: `examples/fluid_pipeline/` — 10-worker fluid pool,
  KV ring as distributed buffer, 4-stage news article pipeline. Run with
  `docker compose up --build --scale worker=10`. See `docs/flow_networks.html` for the
  concept document and `docs/fluid_pipeline_viz.html` for the visualisation.
- Integration test count: **11 scenarios** (scenario 11 = AFN pipeline test: substrate
  unity, topology emergence, and stage-transition pattern via `/gateway/kv` + `/gateway/capability/`).
