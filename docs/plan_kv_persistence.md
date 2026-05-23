# Plan: KV Persistence — WAL + Snapshot + Opacity-Aware Compaction (rev 3)

Changes from rev 2:
- (1) WAL call sites extended to cover `agent/kv.rs` (primary application write path)
  and `consensus.rs` committed-slot writes, not just `connection.rs`.
- (2) `WalHandle` moves from `KvState` (`store.rs`) to `TaskCtx` and `ConnContext`
  to avoid a circular import between `store.rs` and `persistence.rs`.
- (3) Snapshot snapshot exclusion logic removed — `LoadState` entries carry
  `written_at_ms` and evaporate naturally on replay; no special filtering needed.
- (4) Sync/async durability contract stated explicitly for the public API.

---

## Assumptions

- Every deployment target has a writable local filesystem. Nodes without a configured
  persistence path run in-memory-only mode with a startup `WARN` — no hard failure.
- Persistence is a local recovery mechanism. Replication is still epidemic gossip.
- Anti-entropy will re-sync any entries missed during a restart window; the WAL is a
  fast-path that avoids a full anti-entropy catch-up on restart.

---

## Configuration

New types added to `src/config.rs`, wired into `GossipConfig`:

```rust
pub struct PersistenceConfig {
    /// Root path. Data lives under `{base_path}/{node_id}/kv/`.
    /// Using node_id as the directory component gives collision-free layout
    /// when multiple nodes share a machine.
    pub base_path: PathBuf,

    /// Fsync policy on WAL appends. Does not affect snapshot writes
    /// (snapshots always fdatasync before the atomic rename).
    pub sync_mode: SyncMode,

    /// Trigger a snapshot when WAL entry count reaches this value.
    /// Prevents unbounded WAL growth. Default: 10_000.
    pub snapshot_wal_threshold: usize,

    /// Also trigger a snapshot on this timer interval. Default: 300 s.
    pub snapshot_interval_secs: u64,
}

pub enum SyncMode {
    /// fdatasync after every WAL append. ~1 ms overhead per write on SSD. Safest.
    Flush,
    /// OS-buffered writes. Fast; last few writes lost on power failure.
    Async,
    /// No explicit sync. Development and tests only.
    Os,
}
```

In `GossipConfig`:

```rust
/// None = current in-memory-only behaviour (default).
pub persistence: Option<PersistenceConfig>,
```

`GossipConfig::validate()`: if `Some`, verify `base_path` is writable at startup;
log `WARN` and fall back to `None` on failure — misconfiguration should not
hard-crash a cluster.

---

## Directory Layout

```
{base_path}/
  {node_id}/
    kv/
      wal.bin        ← append-only; records are length-prefixed SyncEntry values
      snapshot.bin   ← last full snapshot; replaced atomically on each compaction
      snapshot.tmp   ← in-progress snapshot write target; renamed on completion
```

Consensus committed slots (`consensus/committed/*`) live in the KV store and are
covered by the KV WAL automatically. No separate consensus directory is needed.

---

## Record Format — `SyncEntry`

Both WAL records and snapshot entries use the existing `SyncEntry` from `framing.rs`:

```rust
// Already exists — no new type.
pub(crate) struct SyncEntry {
    pub(crate) key:          Arc<str>,
    pub(crate) value:        Bytes,
    pub(crate) timestamp:    u64,
    pub(crate) is_tombstone: bool,
}
```

`GossipUpdate` is the wrong type: it carries `nonce`, `sender`, and `ttl` — wire
routing concerns that are meaningless on disk. `SyncEntry` has the four fields needed
to reconstruct `StoreEntry` state and is identical to the records in
`WireMessage::StateResponse`, so the existing store-iteration code in `connection.rs`
can be reused directly for snapshot writes.

Each WAL record:

```
[4 bytes: payload length as u32 LE]
[N bytes: bincode-encoded SyncEntry]   ← bincode_cfg() fixed-int, already in tree
```

---

## Snapshot Format

```rust
struct KvSnapshot {
    /// HLC timestamp of the most-recent entry included in this snapshot.
    /// WAL replay skips entries with timestamp ≤ this value.
    snapshot_hlc: u64,
    entries: Vec<SyncEntry>,
}
```

The snapshot contains all live store entries without filtering — `LoadState` entries
under `sys/load/` carry a `written_at_ms` field and are evaporated by readers that
check `now_ms − written_at_ms` against their window. No exclusion logic is needed;
the evaporation mechanism already handles stale soft-state entries on replay.

Written to `snapshot.tmp` via `bincode_cfg()`, then `fdatasync`, then
`std::fs::rename` to `snapshot.bin` (atomic on POSIX).

The store scan that builds `entries` is not atomic across the full `papaya::HashMap`.
This is acceptable for the same reason the anti-entropy `StateResponse` scan accepts
it: the mesh provides eventual consistency and the WAL covers any entries written
during the scan window.

---

## `WalHandle` Location — `TaskCtx` and `ConnContext`, Not `KvState`

`persistence.rs` needs to import `KvState` from `store.rs` (for the snapshot scan).
If `KvState` also imported `WalHandle` from `persistence.rs`, that would be a circular
dependency. The fix: `WalHandle` lives in `persistence.rs` and is held by the two
context structs that already aggregate shared state for background tasks:

- **`TaskCtx`** (`agent/mod.rs`) — add `pub(crate) wal: Option<Arc<WalHandle>>`.
  Gives access to: `GossipAgent` methods (`set`, `set_async`, etc. via
  `self.task_ctx.wal`), `ConsensusEngine` (holds `Arc<TaskCtx>` directly),
  and any background task that already receives `Arc<TaskCtx>`.

- **`ConnContext`** (`connection.rs`) — add `pub(crate) wal: Option<Arc<WalHandle>>`.
  `ConnContext` does not hold `Arc<TaskCtx>`; it has its own parallel field set.
  The WAL handle is passed in when `ConnContext` is constructed in `GossipAgent::start`,
  cloned from `task_ctx.wal`.

`KvState` (`store.rs`) is not modified. `store.rs` does not import `persistence.rs`.

---

## WAL Write Path — Channel-Based, Async-Safe

`apply_and_notify` is synchronous and called from multiple concurrent async tasks.
Adding blocking file I/O inside it would stall the Tokio runtime. A dedicated
`WalWriter` task owns the file handle and receives entries over a bounded channel.

### Message type (in `persistence.rs`)

```rust
enum WalMsg {
    Append {
        entry: SyncEntry,
        /// Some in Flush mode — caller awaits fsync before proceeding.
        /// None in Async/Os — fire and forget.
        ack: Option<oneshot::Sender<Result<(), io::Error>>>,
    },
    TriggerSnapshot {
        ack: oneshot::Sender<Result<(), io::Error>>,
    },
    Shutdown,
}
```

### `WalHandle` (in `persistence.rs`)

```rust
pub(crate) struct WalHandle {
    tx: mpsc::Sender<WalMsg>,
    sync_mode: SyncMode,
}

impl WalHandle {
    /// Standard append. Awaits fsync in Flush mode; fire-and-forget otherwise.
    pub(crate) async fn append(&self, entry: SyncEntry) -> Result<(), io::Error> { ... }

    /// Try-send variant for sync callers. Always fire-and-forget regardless of sync_mode.
    pub(crate) fn append_try(&self, entry: SyncEntry) { ... }

    /// Always awaits fsync regardless of sync_mode. Used for consensus committed slots.
    pub(crate) async fn append_sync(&self, entry: SyncEntry) -> Result<(), io::Error> { ... }

    pub(crate) async fn trigger_snapshot(&self) -> Result<(), io::Error> { ... }
}
```

---

## Call Sites — Which Writes Get WAL-Appended

Not all `apply_and_notify` call sites need WAL. The split is:

### Hard state — WAL-append these

| Location | Lines | What | Durability |
|---|---|---|---|
| `agent/kv.rs` | 61, 80 | `GossipAgent::set` / `delete` (sync) | `append_try` — fire-and-forget into channel |
| `agent/kv.rs` | 95, 110 | `GossipAgent::set_async` / `delete_async` | `append().await` — fsync in Flush mode |
| `connection.rs` | 302, 342, 400 | Received gossip + anti-entropy | `append().await` — fsync in Flush mode |
| `consensus.rs` | committed-slot write only | `consensus/committed/{slot}` | `append_sync().await` — always fsynced |

### Soft state — do NOT WAL-append these

These keys are re-emitted during normal node startup and do not need to survive a
restart:

| Location | Namespace written | Why skip |
|---|---|---|
| `agent/kv.rs:440,451` (`run_kv_persist_task`) | `cap/`, `sys/load/` | Periodic capability/load beacons; regenerated by `advertise_persistent` on restart |
| `agent/opacity.rs` | `sys/load/{node_id}/` | Regenerated by `manage_opacity` governors on restart |
| `agent/capability_ops.rs` | `cap/{node_id}/` | Regenerated by `advertise_capability` on restart |
| `agent/emergent_groups.rs` | `grp/`, `gcap/` | Group membership re-evaluated on restart |
| `agent/demand.rs` | demand keys | Regenerated by `demand` task on restart |
| `agent/mcp.rs` | MCP session keys | Ephemeral session state |
| `agent/state_machine.rs` | agent state keys | Regenerated on restart |
| `consensus.rs` ballot writes | `consensus/ballot/{slot}` | In-progress ballot; peers time out and restart cleanly |

### Sync/async durability contract

`GossipAgent::set` and `delete` are synchronous functions and cannot await an fsync
ack. They use `append_try` (bounded `try_send`): the entry is queued in the WAL
channel but the fsync is not awaited. If the channel is full, the entry is dropped
from the WAL (but was already applied to the in-memory store and queued for gossip).

`GossipAgent::set_async` and `delete_async` use `append().await` and in `Flush` mode
will wait for the fsync before returning. **Operators requiring hard durability for
application writes must use the async API.**

This contract should be documented in the `GossipAgent::set` doc comment.

---

## Startup Replay Sequence

In the async init path of `GossipAgent::new`, before the gossip loop starts:

1. If `persistence` is `None`, skip entirely.
2. Create `{base_path}/{node_id}/kv/` if absent.
3. If `snapshot.bin` exists, deserialise it. For each `SyncEntry`:
   - Intern key if `config.intern_keys` is set (`intern_key(entry.key, max_keys)`).
   - Build a `GossipUpdate` with `nonce = ANTI_ENTROPY_NONCE`, `sender = 0`, `ttl = 1`,
     and the entry's `timestamp`, `key`, `value`, `is_tombstone`.
   - Call `apply_and_notify(&kv_state, &update)`.

   This is identical to how `connection.rs` applies `StateResponse` entries — the same
   code path as anti-entropy. Replay therefore populates the prefix index, `hash_acc`,
   `grp_generation`, `peer_localities`, and all watcher channels correctly.
   `apply_to_store` is `#[cfg(test)]` only and must not be used here.

4. If `wal.bin` exists, read records sequentially. For each `SyncEntry` with
   `timestamp > snapshot_hlc` (or all entries if no snapshot exists), apply via
   `apply_and_notify` as above. Truncated records at the tail (crash mid-write) are
   silently dropped — detect by `record_len > remaining_bytes_in_file`.

5. Observe the HLC with the highest timestamp seen during replay:
   `hlc.observe(max_replayed_ts)`. This ensures any locally-originated write after
   replay has a strictly greater HLC than all recovered entries, preserving causal
   ordering under clock skew.

6. Immediately trigger a snapshot via `TriggerSnapshot` to compact the WAL.

7. Spawn the `WalWriter` task; store the `WalHandle` in `TaskCtx` and clone it into
   `ConnContext` when constructing connection handlers.

---

## Snapshot Procedure (inside WalWriter task)

The `WalWriter` task holds `Arc<KvState>` (passed at spawn). This is the standard
pattern for all background tasks in the system.

1. **Raise opacity.** Build and apply a `LoadState { fill_ratio: 1.0, is_opaque: true,
   written_at_ms: now_ms }` under `sys/load/{node_id}/persistence` using
   `make_gossip_update` + `apply_and_notify`. This follows the same write pattern as
   `manage_opacity_impl` and composes automatically with all other opacity causes via
   `is_self_opaque`'s prefix scan. The `"persistence"` kind string appears alongside
   load-based opacity in the management dashboard with no extra plumbing.

2. **Scan store.** Iterate `kv_state.store` and collect all entries into
   `Vec<SyncEntry>`. Non-atomic; acceptable (same as anti-entropy). The WAL covers any
   entries written during the scan window.

3. **Write snapshot.** Serialise `KvSnapshot { snapshot_hlc, entries }` to
   `snapshot.tmp`; `fdatasync`; rename to `snapshot.bin`.

4. **Truncate WAL.** Seek to offset 0; truncate `wal.bin`; `fdatasync`.

5. **Lower opacity.** Tombstone `sys/load/{node_id}/persistence` via
   `make_gossip_update(is_tombstone: true)` + `apply_and_notify`. Same pattern as
   `manage_opacity_impl`'s clear path.

`LoadState` carries `written_at_ms`. On a future replay, this entry appears with a
past timestamp and is naturally evaporated by any reader that checks the evaporation
window. No special exclusion is needed.

### Snapshot triggers

| Trigger | Condition | Deferrable? |
|---|---|---|
| WAL threshold | `wal_entry_count >= snapshot_wal_threshold` | No — unbounded WAL is worse than a brief opacity window |
| Timer | Every `snapshot_interval_secs` | Yes — defer 30 s if `is_self_opaque()` already true for another reason |
| Graceful shutdown | Node is leaving the mesh cleanly | No |
| Post-startup replay | Always, immediately after replay | No |

---

## Consensus Committed Slots — Force-Fsync

`ConsensusEngine` holds `Arc<TaskCtx>` and gains access to the WAL handle via
`self.task_ctx.wal` at no additional cost from the fix to Issue 2.

Before returning `ConsensusResult::Committed`, the engine calls
`task_ctx.wal.append_sync(committed_entry).await`. `append_sync` always sends
`ack: Some(...)` and awaits the fsync result, bypassing `sync_mode`. This ensures
committed slots survive a restart regardless of the operator's chosen durability mode,
because they represent decisions that peers may already have acted on.

Ballot writes (`consensus/ballot/{slot}`) are not WAL-appended — in-flight ballots
are abandoned on restart and peers time out cleanly.

---

## Implementation Phases

| Phase | Scope | Gate |
|---|---|---|
| P1 | `PersistenceConfig` + `SyncMode` in `GossipConfig`; directory creation on startup | Unblocks all |
| P2 | `WalHandle`, `WalMsg`, `WalWriter` task in `persistence.rs`; add `wal` field to `TaskCtx` + `ConnContext` | Required before P3/P4 |
| P3 | WAL `append_try` in `agent/kv.rs` sync methods; `append().await` in async methods | Application write durability |
| P4 | WAL `append().await` in `connection.rs` before each `apply_and_notify` | Received gossip durability |
| P5 | Startup replay loop (snapshot + WAL replay via `apply_and_notify` + HLC observe + key interning) | Recovery |
| P6 | Snapshot procedure: store scan + `snapshot.tmp` + atomic rename + WAL truncate | Compaction |
| P7 | Snapshot triggers: post-replay, WAL threshold, timer with opacity-aware deferral | Full persistence |
| P8 | Opacity raise/lower around snapshot via `make_gossip_update` + `apply_and_notify` | Mesh-aware |
| P9 | `append_sync` in `consensus.rs` committed-slot write | Consensus safety |

P1–P7 are the minimum viable persistence story.
P8 is operational polish (mesh routes around snapshotting nodes).
P9 is required before consensus is used in production.

---

## Files to Create / Modify

| File | Change |
|---|---|
| `src/config.rs` | Add `PersistenceConfig`, `SyncMode`; add `persistence: Option<PersistenceConfig>` to `GossipConfig` |
| `src/persistence.rs` | **New.** `WalHandle`, `WalMsg`, `WalWriter` task, snapshot write/read, replay iterator |
| `src/agent/mod.rs` | Add `wal: Option<Arc<WalHandle>>` to `TaskCtx`; startup replay + `WalWriter` spawn |
| `src/connection.rs` | Add `wal: Option<Arc<WalHandle>>` to `ConnContext`; `append().await` before each `apply_and_notify` |
| `src/agent/kv.rs` | `append_try` in `set`/`delete`; `append().await` in `set_async`/`delete_async`; update doc comments with durability contract |
| `src/consensus.rs` | `append_sync().await` before returning `ConsensusResult::Committed` |

`store.rs` is not modified. No new Cargo dependencies: `bincode` (v2 + serde),
`tokio::fs`, and `std::fs` are already available.

---

## What This Does Not Cover

- Shared / networked storage (NFS, S3, EFS) — local disk only
- Encryption at rest — follow-up tied to mTLS / identity work (blocker #2)
- WAL replication to a secondary disk — operator concern (RAID, cloud volume replication)
- Cross-cluster state migration — not a Mycelium primitive
