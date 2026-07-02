# Runtime invariants — do not "fix" these

↑ [architecture](architecture.md) · siblings: [layers-and-crates](layers-and-crates.md)

Invariants that look like bugs or optimization targets and are neither. Each has burned a
session or shipped a regression before being written down.

## Layer III posture: detection, not prevention

The substrate never enforces the `consensus/` prefix — a rogue writer can clobber
`consensus/committed/{slot}` and LWW accepts it. The deliberate response is the
**commit-conflict tripwire** (listener refuses to re-endorse a conflicting COMMIT, `warn!`s,
increments `SystemStats::commit_conflicts`). Do **not** add a `consensus/`-prefix write guard
to `apply_and_notify` — that teaches Layer I a Layer III law and inverts the dependency.
The same posture governs the `sys/` namespace tripwire (`sys_namespace_violations`, see
[security](../security.md)) and the RBAC design generally.

**Epoch-leased commitments** (`ConsensusConfig::committed_lease_secs`, default `None` =
permanent): when set, readers apply the capability-style read-side freshness convention —
expired lease reads as not-committed and the slot reopens. Renewal = re-proposing the *same*
value; a different value while live returns `Superseded`. `consensus_rx` stays the raw KV view.

**Listener handlers register synchronously.** `start_consensus_listener` registers
PROPOSE/COMMIT receivers *before* spawning the voter task. Registration inside the task's
first poll silently dropped proposals racing startup (single-node tests self-quorum and never
notice). Keep it synchronous through any refactor.

## Individual-scope routing: forwarding stays unconditional

`SignalScope::Individual` carries RPC and consensus votes. The gossip loop sends directly to
a directly-peered target (latency optimization) and otherwise **falls back to flooding** —
the seen-set dedups, hop-TTL bounds it. Pre-2026-06-12 the fallback was "optimized" to a
silent drop, which broke RPC and ballot voting in partial meshes. Only *admission* is scoped
(`Boundary::admits`); forwarding never is. Regression gates:
`test_individual_signal_reaches_unpeered_target_via_relay`,
`test_individual_consumers_over_random_partial_meshes` (both in `src/lib_tests.rs`).

## Fan-out activation is event-driven

The connection handler publishes the peer list the moment a new peer is inserted (Ping
receipt); waiting for the health monitor's next tick left inbound-only nodes (seeds, tuple
primaries) mute for live sends for up to 2× `health_check_interval`. The health monitor
remains the reconciler/evictor, not the activator.

## Anti-entropy is chunked; KV writes are size-gated (2026-07-02)

`StateResponse` is sent as multiple frames under a per-chunk byte budget
(`mycelium-core/src/connection.rs`), so a store larger than one frame can still bootstrap a
late joiner; an individually un-frameable legacy entry is skipped by name. Upstream,
`kv_set`/`kv_set_async` reject writes over `framing::MAX_KV_WRITE_BYTES` outright, and the
per-peer writer drops a `FrameTooLarge` frame *without* tearing down the connection. Gates:
`test_late_joiner_converges_past_frame_sized_store_via_chunked_anti_entropy`,
`test_oversized_value_is_rejected_outright_and_cluster_stays_healthy`. History: analysis
Run 28 Finding 1 (`docs/analysis/ratings.md`).
