# Lock-free mutation rules (papaya) and the atomics policy

↑ [concurrency](concurrency.md) · sibling: [lock-order](lock-order.md)

## The papaya rules — the race family that keeps recurring

Four M2-audit findings (Runs 16–18) plus a sweep all reduced to one shape: **a lock-free
operation followed by an unserialised derived effect.** Two rules close the family; apply
them to every new papaya call site:

1. **`compute` closures must be retry-safe.** papaya re-invokes the closure on concurrent
   change. Never `take()` a single-use value inside one (panics on retry); clone per
   invocation and reset captured outputs at the top of the closure (see
   `apply_and_notify`'s `old_ts_if_live`).
2. **Never act on a stale read.** Collect-then-`remove()`, check-then-act
   (`is_empty()` → spawn), unconditional remove keyed by something another caller may have
   replaced — all must re-validate inside a `compute` (conditional remove), behind an atomic
   `swap`, by `Arc::ptr_eq` identity, or under a stripe lock with a re-read.

Reference implementations: `get_or_spawn_writer` (claim-by-sentinel, spawn outside the
closure), `ShardedSeen::evict_below` (conditional remove), `kv_quorum::{install_tracker,
remove_tracker}` (copy-on-write + identity-checked removal), `helpers::merge_peer_keys`
(union recomputed inside `compute` — the prior get-clone-modify-insert lost ~87% of retained
keys under concurrent rotation merges; gate:
`concurrent_merges_for_one_node_never_drop_a_key`).

The same shape reappears **outside papaya** wherever a check and its effect straddle an
await or a lock release — the `AgentStateMachine::transition` budget race (Run 28) was
check-then-act with the counters incremented after commit; the fix is `try_commit`
(validate-and-swap + reserve under the state lock, `src/agent/state_machine.rs`).

## Memory-ordering policy for atomics

- **`Relaxed` — purely diagnostic counters** (`dropped_frames`, `hash_acc`,
  `listener_count`, `AliveGuard` flags): read by stats/logging only; no control flow depends
  on precise visibility.
- **`Release`+`Acquire` — generation counters and readiness gates:**
  `KvState::grp_generation` (bump on `grp/` write; cache reader sees all prior writes),
  `CoreCtx::soft_state_advertised` (persist-loop store; `/ready` load).
- **`AcqRel`+`Acquire` — lifecycle state:** `GossipAgent::state` CAS transitions
  (`src/agent/lifecycle.rs`).
- **Cancelled flags:** `RegEntry::cancelled` — `Release` on handle drop, `Acquire` in the
  opacity watcher, so pre-drop work is visible before the watcher stops.
