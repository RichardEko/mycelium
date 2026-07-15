# v2 M2 — `consensus` feature gate: working plan & record

Branch: `v2/m2-consensus-feature`. Milestone: ROADMAP §v2.0 M2 (WS-A). Canonical milestone
design is in `ROADMAP.md`; this is the execution record.

## Goal

Make the Layer III epidemic consensus engine — and the consistency overlay built on it —
an opt-in Cargo feature so minimal embeds can drop ~2,200 LOC of Paxos-style machinery.
`consensus` is **default-on** (existing behaviour unchanged); embedders drop it via
`default-features = false` (then re-add `gateway`/etc.), the same pattern as `gateway`.

## Philosophy binding

Sanctioned directly by `docs/philosophy.md` §2: *"Consensus is an emergent higher-order
concern, not a substrate primitive."* Gating it out removes an optional layer; it never
touches the substrate (Layers I+II, in `mycelium-core`). Mixed clusters are
philosophy-consistent: forwarding lives in `mycelium-core` (unconditional), so a
consensus-disabled node still **relays** PROPOSE/VOTE/COMMIT signals — it just never *acts*
on them. Only acting is gated; the medium still floods.

## What's gated (`#[cfg(feature = "consensus")]`)

| Surface | Detail |
|---|---|
| `consensus.rs`, `consensus_ops.rs`, `consensus_handle.rs` | the engine + listener + `ConsensusHandle` |
| `overlay_consistent.rs` + `KvHandle`/`SubscribeHandle::distributed_lock` | the consistency overlay (`consistent_set`/`get`/lock) — built on consensus |
| `GossipAgent::consensus()` accessor | the entry point |
| `helpers::make_consensus_engine_ctx` + `compute_quorum_size` + `cached_group_members_ctx` | consensus-only helpers |
| `opacity::count_opaque_{members,system,members_in_kv,all_in_kv}` | ballot-time opacity counters (consensus-only callers) |
| `http.rs`: `/consensus/{slot}`, `/gateway/consensus/cross_group_propose`, `/gateway/overlay/{consistent/*,lock/*,elect,log/group/subscribe}` routes + handlers + helpers (`overlay_make_engine`, `overlay_{system,group}_propose`, `CrossGroupProposeBody`) | the gateway surface; `log/group/subscribe` rides along because it claims via the distributed lock |
| `lib.rs` re-exports | `ConsensusConfig, ConsensusListenerHandle, ConsensusResult, GroupQuorum, consensus_kind, consensus_ns, ConsensusHandle, ConsistencyError, LockGuard` |
| consensus tests in `lib_tests.rs`, `wiring.rs`, the gated modules | `#[cfg(feature = "consensus")]` |

## Kept unconditional

- **`commit_conflicts`** (TaskCtx field + `SystemStats` + `/stats`): a diagnostic `AtomicU64`;
  only the *increment* (in the gated `consensus.rs`) is gated, so it reads `0` without
  consensus and the `SystemStats` API is unchanged.
- The non-consensus overlays: ordered log (`log/append|scan|compact|subscribe`) and reliable
  delivery (`emit_reliable`) — KV/anti-entropy based, not consensus.
- `RosterEntry` / `group_roster_cache` — populated unconditionally (cheap); `allow(dead_code)`
  without consensus rather than gating 3 construction sites.

## The one graceful-degradation point

`suggest_leader` (a **capabilities**-layer method, *not* consensus-gated) trust-weights leader
choice using consensus `trust/` slices. Without `consensus` those slices don't exist, so the
trust branch is gated and the score degrades to **pure load** (`trust = 0` everywhere →
`fill / (1.0 + 0)`). `suggest_leader` keeps working without consensus — it just stops applying
trust weighting. (Graceful degradation, not a hard dependency.)

## Verification (all green)

- default (consensus on): 239 lib tests, clippy `-D warnings` clean.
- full matrix `tls,metrics,a2a,llm,compliance`: 302 tests, clippy clean.
- **no-consensus** (`--no-default-features --features gateway`): 196 tests, clippy clean.
- consensus-off composed with `tls,compliance`: builds + clippy clean.
- `mycelium-core`: 79 tests, unaffected (no consensus reference, by construction).

No file moves, no crate-boundary work, no `pub` escalation — purely additive feature gating
in the `mycelium` crate.
