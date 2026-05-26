# Mycelium TupleSpace — Implementation Plan

## Vision

**`mycelium-tuple-space`** is a companion crate that provides a high-throughput,
resilient pipeline buffer built entirely from Mycelium's public API. It replaces the
gossip-replicated KV approach used in AFN with a single-copy store accessed via
blocking RPC — matching Tuple Space `in()` / `out()` semantics, with in-flight
protection via Mycelium KV TTLs and automatic failover via capability advertisement.

**Why a companion crate, not a core feature:**
Mycelium is a substrate — KV ring, signal mesh, capability ring, RPC. These are
universal coordination primitives. A Tuple Space is a specific application-level
pattern, fully composable from those primitives. Embedding it in the core would
make Mycelium a framework rather than a foundation. The companion crate structure
makes the composability concrete: `mycelium-tuple-space` depends only on Mycelium's
public API, which validates that the public API surface is sufficient for this class
of pattern without any internal access.

**Primary use cases:**
1. **Scatter-gather at volume** — thousands of fan-out tasks per second where each
   result needs to be routed to the right aggregator. AFN gossip replication becomes
   a genuine bottleneck here: every item goes to every node. TupleSpace keeps items
   single-copy and routes them point-to-point via RPC, scaling linearly with worker
   count rather than with cluster size.
2. **Heavy AI pipeline flows** — multi-stage LLM/embedding pipelines where each item
   is seconds of compute. Workers park on `take()`, items flow through stages, the
   WAL survives restarts. The cluster is the queue; no external broker needed.

**Design constraints:**
- Zero external systems — KV ring, capability ring, signal mesh, RPC: all Mycelium
- Uses only Mycelium's public API — no internal access, no layer violation
- Hot path: sub-microsecond when waiter is present (direct handoff, no storage write)
- Resilience: primary + 1 secondary; in-flight items re-queued on worker timeout
- Monitoring: written to Mycelium KV so the management API, dashboard, and any
  Prometheus scrape sees it natively
- Use case anchor: 10–10 000 workers, multi-stage pipelines or scatter-gather fans,
  items up to ~50 MB (image tensors), worker latency 1 ms – 300 s per item

---

## Crate Structure

### Workspace layout

```
Mycelium/                          ← existing workspace root
├── Cargo.toml                     ← add mycelium-tuple-space as workspace member
├── src/                           ← Mycelium core (unchanged, except one public API addition)
└── mycelium-tuple-space/          ← new companion crate
    ├── Cargo.toml                 ← depends on mycelium = { path = ".." }
    └── src/
        ├── lib.rs                 ← public API: TupleSpace, TupleConfig, TupleError, TupleRole
        ├── store.rs               ← TupleStore, StageState, WalWriter, WalReplay
        ├── rpc.rs                 ← RPC handlers, background tasks
        └── http.rs                ← http_router(ts: Arc<TupleSpace>) → axum::Router
```

### Public API dependency model

`TupleSpace` wraps an `Arc<GossipAgent>` and uses only public methods:

```rust
pub struct TupleSpace {
    agent:  Arc<GossipAgent>,
    store:  Arc<TupleStore>,
    config: TupleConfig,
    // background task handles
}

impl TupleSpace {
    pub async fn new(agent: Arc<GossipAgent>, cfg: TupleConfig) -> Result<Self, TupleError>
    pub async fn put(…) -> Result<u64, TupleError>
    pub async fn take(…) -> Result<(u64, Bytes), TupleError>
    pub async fn complete(…) -> Result<u64, TupleError>
    pub async fn ack(…) -> Result<(), TupleError>
    pub fn depth(…) -> Vec<(Arc<str>, u32)>
    pub fn http_router(self: Arc<Self>) -> axum::Router
}
```

### HTTP gateway integration

`TupleSpace::http_router()` returns an axum `Router` with the five gateway endpoints.
Callers register it with their `GossipAgent`'s HTTP server:

```rust
let ts = Arc::new(TupleSpace::new(agent.clone(), cfg).await?);
let agent = agent.with_http_routes(ts.http_router());  // one line
```

This requires `GossipAgent::with_http_routes(router: axum::Router) -> Self` to be public.
It is already used internally by the A2A adapter — exposing it as a public method is the
only change needed in the Mycelium core.

### Python and TypeScript SDKs

The Python and TypeScript SDKs call the HTTP gateway endpoints. They are standalone files
(`mycelium-py/src/mycelium/tuple.py` and `mycelium-ts/src/tuple.ts`) — they do not depend
on the Rust companion crate directly.

---

## Philosophical Grounding

TupleSpace is **specialisation from a common foundation**, not a departure from it.
Every coordination concern uses an existing Mycelium layer for what that layer
is designed for. The local `TupleStore` (VecDeque + WAL) is the *execution substrate*
inside the node role — the same way the KV store's internal `DashMap` is not itself
gossiped. It is local working memory, not a new coordination primitive.

### Layer assignment

| Concern | Layer used | Why |
|---|---|---|
| Worker discovery of buffer node | **KV / capability ring** | `advertise_capability("tuple/{ns}/primary")` — same path as any skill |
| Role election (primary/secondary) | **KV / capability ring** | Emergent: nodes observe capability TTLs, self-assign. No coordinator. |
| Payload transfer (put/take) | **RPC** | Point-to-point, acknowledged, carries large payloads without fan-out |
| Replication to secondary | **RPC** (acknowledged) + **Signal** (lag tracking) | RPC carries payloads point-to-point. Signal carries `wal_head` offset for replication lag tracking only. Failure detection is capability TTL — no separate heartbeat counter. |
| In-flight lifecycle | **KV TTL** | Expiry = automatic re-queue. Opacity composition for backpressure. |
| Monitoring | **KV** (`sys/tuple/…`) | Same pattern as `sys/load/` and `sys/identity/`. Picked up by mgmt API natively. |
| Backpressure | **KV opacity** (`sys/load/…`) | Adds a new *cause* to the existing `is_self_opaque()` mechanism. Zero new code path. |
| Execution (the queue itself) | **Local TupleStore** | Execution substrate. Analogous to DashMap inside KV store. Not a layer violation. |

The original plan used gossip KV for secondary mirroring. That was the one
inconsistency: it put payload data into a mechanism designed for durable,
cluster-wide state, causing N×M fan-out for every pipeline item. The corrected
design keeps payloads in point-to-point RPC and Signals; KV carries only
metadata (capability ads, inflight TTLs, monitoring counters).

### AFN vs TupleSpace — two points on one spectrum

Both patterns use the same Mycelium foundation; they differ in which layer
carries the hot data:

| Pattern | KV ring role | Payload routing | Best for |
|---|---|---|---|
| Pure KV (Layer I direct) | IS the buffer | Gossip to all nodes | Small state, maximum resilience |
| AFN | IS the buffer | Gossip to all nodes | Moderate throughput, substrate unity, AI pipeline demo |
| **TupleSpace** | Metadata + lifecycle only | RPC point-to-point | **Scatter-gather at volume** (1 000+ tasks/s fan-out), large-payload AI pipelines |
| RPC / Skills | Capability advertisements | Direct RPC | Stateless compute, no buffer needed |

AFN and TupleSpace diverge most visibly at scale: in AFN, every item write gossips to every
node — O(N) replication cost per item. In TupleSpace, every item is one RPC to the primary
— O(1) regardless of cluster size. For scatter-gather workloads producing thousands of fan-out
tasks per second this difference is the difference between a working system and a gossip storm.

Workers are unaware of which pattern is in use — they resolve a capability
and call an RPC in both cases.

---

## Architecture Overview

```
  Producer                  TupleSpace (primary)          Worker
     │                           │                           │
     │──── rpc("tuple.put") ────▶│                           │
     │                           │  waiter present?          │
     │                           │◀─── rpc("tuple.take") ───│
     │                           │  yes → hand Bytes directly │──▶ compute
     │                           │  no  → store in TupleStore │
     │                           │        (deferred wakeup)  │
     │                           │                           │
     │                           │── rpc("tuple.replicate") ─▶ secondary
     │                           │── signal("tuple.heartbeat")▶ secondary (lag tracking: wal_head)
     │                           │── inflight TTL key ───────▶ KV ring
     │                           │                           │
     │◀── ok ────────────────────│  rpc("tuple.complete") ───│
                                 │    atomically: ack(id) + put(next_stage, result)
                                 │  or inflight TTL expires → re-queue

  Failure detection (secondary):
     capability tuple/{ns}/primary TTL expires (no refresh from primary) → promote
     Signal heartbeat role: delivers wal_head for replay gap estimation only
```

### Critical path — waiter present (hot path)
1. Producer RPC arrives: `tuple.put("stage-a", payload: Bytes)`
2. Lock-free lookup of `StageState` in `papaya::HashMap`
3. Single `parking_lot::Mutex` lock on `StageInner` — covers both entries and waiters atomically
4. `waiters.pop_front()` → `oneshot::Sender`; lock **dropped before** `tx.send()`
5. `tx.send((id, payload))` — hands the `Arc<[u8]>` refcount, zero copy, no lock held
6. Worker's parked `await` resolves immediately
7. Total: 1 lock/unlock + 1 pointer swap. No I/O, no second lock, no TOCTOU.

### Critical path — no waiter (store path)
1–3 same as above
4. `waiters` empty → `entries.push_back(payload)` → lock released → WAL append
5. Worker arrives: same single lock → `entries.pop_front()` → return immediately

### Critical path — worker arrives before item (park path)
1. Worker RPC: `tuple.take("stage-a", timeout_secs: 30)`
2. Single lock → `entries` empty → register `oneshot::Sender` in `waiters` → lock released
3. Worker task parks on `rx.await` — zero spin, zero polling
4. Next producer put: single lock, pop waiter, drop lock, send — worker wakes

---

## Phase 1 — Core TupleStore + RPC Handlers

### New file: `mycelium-tuple-space/src/store.rs`

```rust
// Inner state under a single lock — eliminates TOCTOU between waiter-check and entry-store.
struct StageInner {
    entries: VecDeque<(u64, Bytes)>,
    waiters: VecDeque<oneshot::Sender<(u64, Bytes)>>,
}

pub(crate) struct StageState {
    inner:         parking_lot::Mutex<StageInner>,
    depth:         AtomicU32,   // shadow counter for lock-free monitoring reads
    waiters_count: AtomicU32,   // approximate (timed-out waiters counted down on expiry)
}

pub(crate) struct TupleStore {
    stages:   papaya::HashMap<Arc<str>, Arc<StageState>>,
    next_id:  AtomicU64,    // monotonic item ID
    op_count: AtomicU64,    // for checkpoint trigger
    wal:      Option<WalWriter>,
}
```

**`put(stage, payload) -> u64 (item_id)`**
```
id = next_id.fetch_add(1, Relaxed)
state = stages.get_or_insert(stage)
wal.append(Put { stage, id, payload })      // ← written ONCE, before any dispatch decision
{                                           //   covers both hot-path and store-path
    let mut g = state.inner.lock()          // single lock covers both queues — no TOCTOU
    loop {
        match g.waiters.pop_front() {
            None => break,                  // no waiters → store below
            Some(tx) => {
                state.waiters_count.fetch_sub(1, Relaxed)
                drop(g)                     // release before send — no I/O under lock
                match tx.send((id, payload)) {
                    Ok(()) => return id     // hot path: direct handoff, WAL already committed
                    Err(p) => {             // receiver timed out — try next waiter or store
                        payload = p.0.1
                        g = state.inner.lock()
                    }
                }
            }
        }
    }
    g.entries.push_back((id, payload))      // store path: item in queue AND in WAL
    state.depth.fetch_add(1, Relaxed)
}                                           // lock released
return id
```

**Why the WAL write is hoisted before the lock and loop:**

The previous design wrote `wal.append` inside the `Some(tx)` branch and again after the
loop for the store path. With N timed-out waiters followed by a store, that fires the
append N+1 times for one item — N+1 `PutRecord`s for the same `id`. On replay each
`PutRecord` lacking an `AckRecord` causes a re-enqueue: the item is enqueued N+1 times.
One WAL write before the loop eliminates this entirely.

**Orphan-PutRecord lifecycle (running node):** WAL is committed before any dispatch.
If all waiters have timed out and no new waiter arrives before the lock is re-acquired,
the item falls into `entries` — it is in both WAL and the in-memory queue, immediately
visible to the next `take()` call. There is no WAL-only limbo for a running node: every
item in WAL is also either in `entries` (awaiting a worker) or in `waiters` (being
dispatched) or already in-flight (TakeRecord written). The WAL is always a superset of
the in-memory state, never a disjoint ghost.

**Orphan-PutRecord lifecycle (crashed node):** if the node crashes after `wal.append`
but before `entries.push_back` or `tx.send`, the `PutRecord` survives. On restart,
replay rule "PutRecord with no AckRecord/CompleteRecord → enqueue" re-queues it
correctly. This is the intended recovery path, not a degenerate case.

**`take(stage, timeout) -> Option<(u64, Bytes)>`**
```
state = stages.get_or_insert(stage)
{
    let mut g = state.inner.lock()
    if let Some(item) = g.entries.pop_front() {
        state.depth.fetch_sub(1, Relaxed)
        return Some(item)                   // immediate, no async
    }
    let (tx, rx) = oneshot::channel()
    g.waiters.push_back(tx)
    state.waiters_count.fetch_add(1, Relaxed)
}                                           // lock released before parking
let result = select! { rx.await => Some(item), sleep(timeout) => None }
if result.is_none() { state.waiters_count.fetch_sub(1, Relaxed) }  // timed out
result
```

**WAL (optional, enabled when `tuple_config.persist = true`)**

Four record types. The fourth is essential: `complete` must be a single indivisible
WAL unit so replay cannot apply half of a stage transition.

```
PutRecord      { stage, id, payload_len, payload }
               written on put(), before returning

TakeRecord     { id, taken_at_ts }
               written on take(), before responding to worker

AckRecord      { id }
               written on terminal ack() only (last stage or explicit abandon)

CompleteRecord { old_id, old_stage, new_id, new_stage, payload_len, payload }
               written on complete(), replacing both AckRecord(old_id) and
               PutRecord(new_id) — a single fsync covers both sides
```

**Why `CompleteRecord` must be a distinct type, not `AckRecord + PutRecord`:**
On WAL replay, the two must be applied atomically — either the old item is acked
AND the new item is queued, or neither. Storing them as two separate records means
a restart between the two records would re-queue the old item (TakeRecord with no
Ack) AND put the new item again (PutRecord replayed) — the classic duplicate that
`complete` was designed to eliminate. With `CompleteRecord`, replay is:
```
CompleteRecord found → ack old_id (remove from in-flight set) + enqueue new_id
```

**Compaction rules:**
- `PutRecord(id)` safe to compact when `AckRecord(id)` or `CompleteRecord(old_id=id)` exists
- `TakeRecord(id)` safe to compact alongside its `PutRecord`
- `CompleteRecord` safe to compact when `AckRecord` or further `CompleteRecord` for `new_id` exists
- Any `PutRecord` or `TakeRecord` with no corresponding ack/complete = in-flight or abandoned;
  retain for re-queue

**Startup replay order:**
1. Scan all records, build map: `id → (Put, Take?, Ack/Complete?)`
2. Items with `Put` + no `Ack`/`Complete` → enqueue (including items with `TakeRecord`, re-queued as abandoned)
3. Items from `CompleteRecord`: apply new_id as a fresh `Put` if no subsequent ack exists

- Flushed via `File::sync_data()` every `checkpoint_every` ops (default 500) via
  `tokio::task::spawn_blocking`
- Compact when `acked_count / total_count > 0.5`: rewrite live entries, swap file

### New file: `mycelium-tuple-space/src/rpc.rs`

RPC handlers wired to `TupleStore`. Exposes four methods:

| RPC method | Direction | Description |
|---|---|---|
| `tuple.put` | producer → primary | `{stage, payload_b64}` → `{id}` |
| `tuple.take` | worker → primary | `{stage, timeout_secs}` → `{id, payload_b64}` |
| `tuple.complete` | worker → primary | `{id, next_stage, next_payload_b64}` → `{next_id}` — atomic ack + advance |
| `tuple.ack` | worker → primary | `{id}` → `{ok}` — terminal ack (last stage or error path) |
| `tuple.depth` | anyone → primary | `{stage?}` → `{stages: [{stage, depth, waiters}]}` |
| `tuple.replicate` | primary → secondary | `{id, stage, payload_b64\|wal_offset}` → `{ok}` |
| `tuple.wal_replay` | secondary → primary | `{from_offset, limit}` → `{entries: [...], next_offset, done}` — paginated; secondary drives the loop |

`tuple.complete` is the preferred hot path for pipeline workers — it writes a single
`CompleteRecord` WAL entry, atomically acks the old item and enqueues the new one.
No crash window between stage transition. `tuple.ack` is reserved for the terminal
stage (last in pipeline, or explicit abandonment on error).

**`tuple.wal_replay` protocol — secondary drives pagination:**
```
secondary loop:
  offset = last_known_wal_head  (from last heartbeat Signal, or 0)
  loop:
    resp = rpc_call(primary, "tuple.wal_replay",
                    {from_offset: offset,
                     limit: cfg.replay_chunk_size,       // default 200 entries
                     max_bytes: cfg.replay_chunk_bytes})  // default 32 MB
    for entry in resp.entries:
      tuple_store.put(entry.stage, entry.payload)
    offset = resp.next_offset
    if resp.done: break
```

Primary serialises live WAL entries from `from_offset`, capping the response at
`min(limit entries, max_bytes bytes)` — whichever is hit first. No streaming required.

**⚠ Operator note — large-payload pipelines must tune `replay_chunk_size`:**

`replay_chunk_size` is an *entry count* limit, not a byte limit. The byte limit
(`replay_chunk_bytes`, default 32 MB) is the safety net. At the default of 200 entries:

| Avg payload size | Bytes per chunk | Safe? |
|---|---|---|
| 10 KB (text, embeddings) | ~2 MB | ✓ fine |
| 1 MB (image thumbnails) | ~200 MB | ✗ exceeds 32 MB byte cap → auto-truncated |
| 5 MB (image tensors) | ~1 GB | ✗ byte cap limits to ~6 entries per chunk |

For large-payload pipelines the byte cap kicks in automatically — `replay_chunk_size`
becomes irrelevant and `replay_chunk_bytes` governs. The default 32 MB cap keeps each
RPC response within Mycelium's normal operating range. Operators running image tensor
pipelines should lower `replay_chunk_size` to 5–10 to reduce the number of WAL entries
the primary scans per chunk before the byte cap applies.

**Public API — `mycelium-tuple-space/src/lib.rs`**

```rust
// ── Config types ────────────────────────────────────────────────────────────
pub struct TupleConfig {
    pub namespace:            Arc<str>,          // e.g. "pipeline"
    pub role:                 TupleRole,         // Auto | Primary | Secondary
    pub persist:              bool,              // WAL-backed or transient
    pub wal_path:             PathBuf,           // ignored if persist = false
    pub checkpoint_every:     u64,               // ops between fdatasync (default 500)
    pub worker_timeout_secs:  u64,               // inflight TTL (default 300)
    pub high_watermark:       u32,               // stage depth → opacity (default 500)
    pub mirror_payload_limit: usize,             // bytes; larger replicate handle-only (default 1 MB)
    pub heartbeat_interval:   Duration,          // Signal cadence for lag tracking (default 5 s)
    pub replay_chunk_size:    usize,             // max entries per wal_replay response (default 200)
    pub replay_chunk_bytes:   usize,             // max bytes per wal_replay response (default 32 MB)
                                                 // chunk bounded by min(chunk_size, chunk_bytes)
    pub backpressure_mode:    BackpressureMode,  // Raise | Block(timeout)
}

pub enum BackpressureMode {
    Raise,          // put() returns Err(Backpressure) immediately
    Block(Duration), // put() retries with backoff up to timeout
}

pub enum TupleRole { Auto, Primary, Secondary }

pub enum TupleError {
    NoProvider,
    Backpressure { retry_after_ms: u64 },   // producer should back off
    Timeout,
    NotFound,
    Io(std::io::Error),
}

// ── TupleSpace — companion to Arc<GossipAgent> ──────────────────────────────
pub struct TupleSpace { /* private */ }

impl TupleSpace {
    /// Construct and start background tasks. Registers RPC handlers with agent.
    pub async fn new(agent: Arc<GossipAgent>, cfg: TupleConfig) -> Result<Self, TupleError>

    /// Returns an axum Router with the five HTTP gateway endpoints.
    /// Register with: agent.with_http_routes(ts.http_router())
    pub fn http_router(self: Arc<Self>) -> axum::Router

    // ── Producer API ────────────────────────────────────────────────────────
    /// Write item to stage. Returns item_id.
    /// Returns Err(Backpressure) when primary is opaque (depth > high_watermark).
    pub async fn put(&self, stage: &str, payload: Bytes) -> Result<u64, TupleError>

    // ── Worker API ──────────────────────────────────────────────────────────
    /// Blocking claim. Parks until an item is available or timeout elapses.
    pub async fn take(&self, stage: &str, timeout: Duration) -> Result<(u64, Bytes), TupleError>

    /// Atomic pipeline advance: acks `id` AND puts `next_stage` in one WAL entry.
    /// PREFERRED over separate put + ack for all mid-pipeline transitions.
    pub async fn complete(&self, id: u64, next_stage: &str, payload: Bytes) -> Result<u64, TupleError>

    /// Terminal ack: last stage or explicit error abandonment only.
    pub async fn ack(&self, id: u64) -> Result<(), TupleError>

    // ── Inspection ──────────────────────────────────────────────────────────
    pub fn depth(&self, stage: Option<&str>) -> Vec<(Arc<str>, u32)>
}
```

**Minimal Mycelium core change** — expose `with_http_routes` publicly (`src/agent/mod.rs` or
`src/agent/http.rs`):
```rust
// GossipAgent — existing internal method, made public
pub fn with_http_routes(self, router: axum::Router) -> Self
```
This is the only change needed in the Mycelium core for companion crate HTTP integration.

### Unit tests (`mycelium-tuple-space/src/store.rs`)
- `put_then_take` — basic roundtrip, FIFO order guaranteed (VecDeque)
- `take_before_put` — worker parks, producer unblocks it; verify no spurious wakeup
- `timed_out_waiter_skipped` — take() times out, subsequent put() skips dead receiver and stores; next take() gets item
- `all_waiters_timed_out_item_in_queue` — 3 workers all take() with 10 ms timeout and expire; producer put() retries all three dead receivers, falls through to entries queue; 4th worker take() immediately receives item; WAL has exactly ONE PutRecord for the item (not three)
- `concurrent_100` — 10 producers × 100 puts, 10 consumers × 100 takes → all 1 000 items delivered exactly once, FIFO per producer
- `take_timeout` — take with 50 ms timeout on empty stage returns `Err(Timeout)`
- `complete_atomic_no_crash_window` — put item, take it, complete(id, next, payload); verify: old stage empty, new stage has item, no inflight key; simulate crash between put and ack in the non-atomic path to demonstrate the duplicate hazard (documents why `complete` is preferred)
- `wal_records_take` — put + take → WAL has PutRecord + TakeRecord; compaction does NOT remove unacked item
- `wal_replay` — put 10 items, simulate restart, verify items survive in queue
- `wal_replay_inflight` — put 5, take 3 (no ack), restart; verify 3 re-queued + 2 remaining = 5 total
- `wal_compact` — ack 60% of items via complete(), trigger compaction, verify remaining 40% survive
- `backpressure_error` — depth exceeds high_watermark → put() returns `Err(Backpressure)`; depth drops below hysteresis → put() succeeds again

---

## Phase 2 — Resilience

### Primary / Secondary role election

Nodes self-assign roles based on what they observe in the capability ring — no
coordinator, consistent with Mycelium's emergent design:

```
On startup (TupleRole::Auto):
  1. Advertise capability: tuple/{ns}/candidate
  2. Sleep 2 s (let other candidates settle)
  3. Resolve tuple/{ns}/primary → providers list
     - empty → promote to Primary
     - non-empty → become Secondary
```

**Primary advertises:** `tuple/{ns}/primary`
**Secondary advertises:** `tuple/{ns}/secondary`

Workers resolve `tuple/{ns}/primary` for normal operation. On RPC failure, fall back
to `tuple/{ns}/secondary` and surface a warning via `tracing::warn!`.

### Secondary replication — Signal heartbeat + RPC payload transfer

Replication uses two mechanisms, each doing what its layer is designed for.
Gossip KV is deliberately **not** used for payload mirroring — it would fan payloads
out to the whole cluster, defeating the single-copy design goal.

**Replication lag tracking — Signal (fire-and-forget, ephemeral)**

Primary emits a `Signal` to the secondary every `heartbeat_interval` (default 5 s):
```
kind:    "tuple.heartbeat"
scope:   SignalScope::Individual(secondary_node_id)
payload: {ns, put_total, take_total, wal_head}
```
The Signal's **sole purpose** is delivering `wal_head` — the primary's current WAL
offset. Secondary records this; on promotion it requests `tuple.wal_replay` from
`last_known_wal_head` to catch up on items received since the last signal. If no
heartbeat has ever arrived, `from_offset = 0` (full replay).

Signals are the right layer here: ephemeral, directional, zero KV churn.

**Failure detection — capability TTL (not the Signal)**

Secondary promotes when `resolve_capability("tuple/{ns}/primary")` returns empty —
meaning the primary's capability advertisement TTL has lapsed without a refresh.
This uses the existing capability TTL mechanism with no new primitives. There is
no missed-heartbeat counter; the capability ring IS the failure detector.

Consequence: promotion latency ≈ capability advertisement interval (default ~10 s).
This is acceptable for AI pipeline use cases where worker timeout is 300 s.

**Payload replication — RPC (acknowledged, point-to-point)**

For every `tuple.put`, primary fires a background RPC to the secondary:
```
method:  "tuple.replicate"
payload: {id, stage, payload_bytes}   // full payload ≤ mirror_payload_limit (default 1 MB)
         {id, stage, wal_offset}       // WAL handle only for larger items
```
`rpc_call` returns an acknowledgment. Primary does not block the producer on this —
replication fires concurrently with the `put` response. On RPC failure, the item
is logged as "unconfirmed" in a small in-memory set. The WAL on the primary is
the source of truth; unconfirmed items are retried once before being accepted as
potentially lost on catastrophic primary failure.

RPC is the right layer here: point-to-point, acknowledged, carries arbitrarily
large payloads without gossip fan-out.

**Promotion sequence**

1. Secondary polls `resolve_capability("tuple/{ns}/primary")` — returns empty
   (primary TTL lapsed, no refresh received)
2. Wait one full capability advertisement interval to guard against a transient
   resolve miss (reduces false-positive split-brain)
3. Re-resolve — still empty → proceed with promotion
4. Attempt `tuple.wal_replay` from `last_known_wal_head`:
   - Primary still TCP-reachable: loop until `done = true` — drain the WAL fully
     before advertising as primary; no producer traffic is accepted during this window
     (workers still see no primary in capability ring, so puts block/backpressure)
   - Primary unreachable: proceed with what was already replicated; items above
     `mirror_payload_limit` that weren't replicated are lost (at-least-once boundary)
5. Advance `next_id` to `max(local_next_id, max_replayed_id) + 1` before serving any
   puts. This prevents ID collisions between items replayed from the old primary's WAL
   and new items assigned by the promoted secondary.
6. Secondary re-advertises as `tuple/{ns}/primary`; stops advertising as secondary
7. Workers re-resolve capability and connect — producers unblock

No heartbeat counter, no missed-beat logic. One mechanism: capability TTL.

**Why step 5 (`next_id` fence) is necessary:** the old primary assigned IDs from its own
`AtomicU64`, starting from 0 on its last boot. The secondary starts its own `next_id`
from 0. After replay, if secondary starts assigning IDs at 0, a new item with `id=5`
collides with a replayed item that also has `id=5`. The WAL compaction, inflight KV
keys, and `complete()` records all key on `id` — a collision corrupts them. Setting
`next_id = max_replayed_id + 1` is a one-line fix with no architectural cost.

For items above `mirror_payload_limit` that were not yet replicated: secondary
promotes without them. At-least-once delivery means producers may need to
re-submit; the inflight TTL key (still in KV) triggers automatic re-queue for
items already taken but not yet acked.

### In-flight protection

On `take()`, primary writes before responding to the worker:

```
Key:   tuple/inflight/{ns}/{item_id}
Value: {stage, worker_node_id, taken_at_ts}
TTL:   worker_timeout_secs   (default 300 s)
```

On `ack({item_id})`: delete the inflight key.

Background task on primary (runs every 30 s): scan `tuple/inflight/{ns}/` for entries
whose `taken_at_ts + worker_timeout_secs < now`. Any found were not acked within the
deadline — re-queue the item via `tuple_store.put(stage, payload)`.
Payload is recovered from the primary's WAL (WAL records are retained until all
in-flight items referencing them are acked, then compacted).

This gives **at-least-once delivery**. For LLM inference pipelines this is correct —
a re-submitted inference produces the same result and the consumer deduplicates on
`item_id` if needed.

### Failure modes and recovery

| Failure | Detection | Recovery |
|---|---|---|
| Primary crash mid-put | Producer gets RPC error → `NoProvider` after re-resolve | Producer retries; idempotent if same item_id used |
| Primary crash, replication confirmed | Capability TTL lapses → secondary promotes | Secondary has item in local TupleStore |
| Primary crash, replication in-flight | Capability TTL lapses → secondary promotes | Secondary requests `wal_replay` from `last_known_wal_head`; catches up if primary still TCP-reachable |
| Primary crash, large-payload handle-only | Capability TTL lapses → secondary promotes | Secondary has handle, requests payload chunk via `wal_replay`; if primary unreachable, item lost (at-least-once boundary — producer must retry) |
| Worker crash between `put(next)` and `ack(id)` | Inflight TTL expires | Item re-queued; `next_stage` has a duplicate. **Use `tuple.complete` to eliminate this window.** |
| Worker crash during compute | Inflight TTL expires | Primary re-queues after `worker_timeout_secs`; WAL has payload |
| Secondary crash | RPC replication fails; unconfirmed set grows | Primary continues solo; next `Auto` joiner becomes new secondary |
| Network partition | Capability TTL lapses on each side | Both promote — split brain, items duplicated. Acceptable at-least-once. Resolve via Layer III consensus for exactly-once (future). |

---

## Phase 3 — Monitoring and Backpressure

### KV-based metrics (pure Mycelium)

Background task on every TupleSpace node: every 10 s, write the following KV keys
(TTL = 60 s, refreshed every 10 s → stale metrics vanish automatically):

```
sys/tuple/{node_id}/role                         → "primary" | "secondary" | "candidate"
sys/tuple/{node_id}/stage/{stage}/depth          → u32  (items waiting in queue)
sys/tuple/{node_id}/stage/{stage}/waiters        → u32  (workers parked on take)
sys/tuple/{node_id}/stage/{stage}/inflight       → u32  (taken, not yet acked)
sys/tuple/{node_id}/stage/{stage}/put_total      → u64  (monotonic put counter)
sys/tuple/{node_id}/stage/{stage}/take_total     → u64  (monotonic take counter)
sys/tuple/{node_id}/stage/{stage}/hot_total      → u64  (puts delivered directly to a parked worker)
sys/tuple/{node_id}/stage/{stage}/queue_p99_us   → u32  (P99 store-path queue wait, μs — store path only)
sys/tuple/{node_id}/wal_bytes                    → u64  (WAL file size, 0 if transient)
```

**Two distinct delivery paths, two distinct metrics:**

- `hot_total` counts items delivered directly to a parked worker (hot path). These never
  enter the queue; their latency is sub-microsecond and uninformative. Counting them
  separately tells operators the proportion of demand being met instantly.

- `queue_p99_us` measures store-path queue wait only: time from `put()` storing an item
  to a subsequent `take()` removing it. Maintained as a ring buffer of the last 1 000
  store-path timestamps (HLC ticks → microseconds). Computed every 10 s. No histogram
  crate needed. Hot-path deliveries are **excluded** — including them would collapse the
  distribution and hide true queueing latency.

Operator interpretation: `hot_total / take_total` ≈ fraction of demand met instantly.
`queue_p99_us` rising → workers not keeping up with producers → scale workers or check
for a slow stage upstream.

**Management API extension** (`mycelium-tuple-space/src/http.rs`, registered via `with_http_routes`):

`GET /api/tuple` → aggregates `sys/tuple/{*}` KV prefix across all nodes → JSON:
```json
{
  "nodes": [
    { "node_id": "10.0.0.2:7200", "role": "primary",
      "stages": [
        { "stage": "stage-a", "depth": 47, "waiters": 3,
          "inflight": 12, "put_total": 1042, "take_total": 995,
          "hot_total": 731, "queue_p99_us": 4800 }
      ],
      "wal_bytes": 2097152
    }
  ]
}
```

### Opacity backpressure

When `depth[stage] > high_watermark` (default 500 items), primary writes:
```
sys/load/{node_id}/tuple-pressure/{stage} → {is_opaque: true, depth: N}
```

`is_self_opaque()` returns `true` → the primary node disappears from
`resolve_capability("tuple/{ns}/primary")`. Producers get an empty provider list
and should back off. This uses Mycelium's existing opacity composition with zero
new mechanism.

When depth drops below `high_watermark * 0.7` (hysteresis): write `is_opaque: false`
or let the key expire. Prevents oscillation.

### Backpressure contract (producer-side)

When the primary is opaque, `resolve_capability("tuple/{ns}/primary")` returns empty.
Every call site that touches `tuple_put()` must handle this case explicitly:

**Rust SDK:** `tuple_put()` returns `Err(TupleError::Backpressure { retry_after_ms: 500 })`.
Caller decides policy: exponential backoff, drop, or surface to application.

**HTTP gateway:** `POST /gateway/tuple/put` returns `503 Service Unavailable` with
`Retry-After: 1` header when no provider resolves. Clients must respect this.

**Python SDK:** `TupleSpace.put()` raises `TupleBackpressureError(retry_after_ms=500)`
by default. An optional `backpressure="block"` mode retries with backoff internally
(max `backpressure_timeout_secs`, default 30 s) before raising.

The secondary does **not** serve `put()` during normal operation — it is a passive
mirror. Routing `put()` to secondary during backpressure would bypass the flow
control signal entirely.

---

## Phase 4 — SDK Surface

### HTTP Gateway (`mycelium-tuple-space/src/http.rs`)

| Method | Endpoint | Body / Query | Response |
|---|---|---|---|
| POST | `/gateway/tuple/put` | `{ns, stage, payload_b64}` | `{id}` or `503 Retry-After` |
| POST | `/gateway/tuple/take` | `{ns, stage, timeout_secs}` | `{id, stage, payload_b64}` |
| POST | `/gateway/tuple/complete` | `{ns, id, next_stage, next_payload_b64}` | `{next_id}` |
| POST | `/gateway/tuple/ack` | `{ns, id}` | `{ok}` |
| GET  | `/gateway/tuple/depth` | `?ns=…&stage=…` | `{depth, waiters, inflight}` |

`take` and `complete` are blocking HTTP requests — handlers await internally.
Workers must use HTTP client timeout of `timeout_secs + 5` s minimum.
`put` returns `503 Service Unavailable` with `Retry-After: 1` when primary is opaque.

### Python SDK (`mycelium-py/src/mycelium/tuple.py`)

```python
class TupleBackpressureError(Exception):
    def __init__(self, retry_after_ms: int): ...

class TupleSpace:
    def __init__(self, agent: Agent, ns: str = "pipeline"): ...

    def put(self, stage: str, payload: bytes, *,
            backpressure: str = "raise",          # "raise" | "block"
            backpressure_timeout_secs: float = 30.0) -> int:
        """Write item to stage. Returns item_id.
        backpressure="raise": raises TupleBackpressureError immediately.
        backpressure="block": retries with exponential backoff until timeout."""

    def take(self, stage: str, timeout_secs: float = 30.0) -> tuple[int, bytes]:
        """Blocking claim. Returns (item_id, payload). Raises TimeoutError."""

    def complete(self, item_id: int, next_stage: str, payload: bytes) -> int:
        """Atomic pipeline advance: acks item_id AND puts next_stage.
        No crash window between stages. PREFERRED over separate put+ack.
        Returns next_id."""

    def ack(self, item_id: int) -> None:
        """Terminal ack: last stage or explicit error abandonment only."""

    def depth(self, stage: str | None = None) -> dict:
        """Return {stage: {depth, waiters, inflight}} for all or one stage."""
```

Worker pattern for heavy AI flows:
```python
ts = TupleSpace(agent, ns="news-pipeline")
while True:
    item_id, payload = ts.take("stage-a", timeout_secs=60)
    try:
        result = run_llm(payload)               # seconds of compute
        ts.complete(item_id, "stage-b", result) # atomic: no crash window
    except Exception:
        pass  # inflight TTL re-queues automatically; no explicit ack needed
```

### TypeScript SDK (`mycelium-ts/src/tuple.ts`)

```typescript
export class TupleBackpressureError extends Error {
    constructor(public retryAfterMs: number) { super("backpressure") }
}

export class TupleSpace {
    constructor(agent: Agent, ns: string = "pipeline") {}
    async put(stage: string, payload: Uint8Array,
              opts?: { backpressure?: "raise" | "block",
                       backpressureTimeoutSecs?: number }): Promise<number>
    async take(stage: string, timeoutSecs?: number): Promise<[number, Uint8Array]>
    async complete(itemId: number, nextStage: string,
                   payload: Uint8Array): Promise<number>  // atomic advance
    async ack(itemId: number): Promise<void>              // terminal only
    async depth(stage?: string): Promise<Record<string, StageDepth>>
}
```

---

## Phase 5 — Integration Test

**Scenario 12: TupleSpace pipeline** (`tests/integration/scenarios/12_tuple_space.sh`)

Uses the existing 4-node cluster. node-a acts as primary TupleSpace, node-b as secondary.

1. Enable TupleSpace on node-a: `POST /api/tuple/enable {ns: "ts12", role: "primary"}`
   (or startup config — TBD on exact enablement API)
2. Verify primary capability visible: `GET /gateway/capability/resolve?ns=tuple/ts12&name=primary`
3. Put 10 items on node-a via `/gateway/tuple/put`
4. Take 10 items via node-b (routes RPC to primary) — verify all 10 delivered
5. Simulate primary failure (not possible in test harness without container kill — 
   test instead: take with timeout → verify inflight key visible in `/api/state`)
6. Ack all 10 — verify inflight keys gone
7. Verify `/api/tuple` shows correct depth=0, put_total=10, take_total=10

---

## New and Modified Files

### Companion crate — `mycelium-tuple-space/` (all new)

| File | Description |
|---|---|
| `mycelium-tuple-space/Cargo.toml` | `[dependencies] mycelium = { path = ".." }` + `parking_lot`, `bytes`, `tokio` |
| `mycelium-tuple-space/src/lib.rs` | Public API: `TupleSpace`, `TupleConfig`, `TupleError`, `TupleRole`, `BackpressureMode` |
| `mycelium-tuple-space/src/store.rs` | `TupleStore`, `StageState`, `StageInner`, `WalWriter`, `WalReplay` |
| `mycelium-tuple-space/src/rpc.rs` | RPC handlers (`tuple.put/take/complete/ack/replicate/wal_replay`), background tasks (monitoring, re-queue, checkpoint, heartbeat) |
| `mycelium-tuple-space/src/http.rs` | `http_router(ts: Arc<TupleSpace>) → axum::Router` — 5 gateway endpoints + `/api/tuple` |

### Mycelium core — minimal changes

| File | Status | Description |
|---|---|---|
| `Cargo.toml` | modified | Add `mycelium-tuple-space` to `[workspace] members` |
| `src/agent/mod.rs` or `src/agent/http.rs` | modified | Expose `with_http_routes(router: axum::Router) -> Self` as `pub` |

### Language SDKs — call HTTP gateway

| File | Status | Description |
|---|---|---|
| `mycelium-py/src/mycelium/tuple.py` | **new** | `TupleSpace` Python class |
| `mycelium-py/src/mycelium/__init__.py` | modified | re-export `TupleSpace` |
| `mycelium-ts/src/tuple.ts` | **new** | `TupleSpace` TypeScript class |
| `mycelium-ts/src/index.ts` | modified | re-export `TupleSpace` |

### Integration test

| File | Status | Description |
|---|---|---|
| `tests/integration/scenarios/12_tuple_space.sh` | **new** | Scenario 12 — TupleSpace pipeline |
| `tests/integration/run.sh` | modified | Add scenario 12 |

---

## Completion Criteria

| Phase | Signal |
|---|---|
| 1 — Core | `cargo test -p mycelium-tuple-space` green; `put` + `take` + `ack` work end-to-end via RPC; WAL replay verified in unit test |
| 2 — Resilience | Primary/secondary election visible in capability ring; inflight TTL keys written/deleted correctly; re-queue on expired inflight verified |
| 3 — Monitoring | `GET /api/tuple` returns correct depths; opacity written when `depth > high_watermark`; `sys/tuple/…` keys visible in `/api/state` |
| 4 — SDK | Python `TupleSpace.take()` blocks and wakes on `put()`; TypeScript equivalent; HTTP gateway round-trips clean |
| 5 — Integration | Scenario 12 passes in `make test`; CLAUDE.md updated to 12 scenarios |

**Build commands:**
```sh
# Companion crate only
cargo build -p mycelium-tuple-space
cargo test  -p mycelium-tuple-space
cargo clippy -p mycelium-tuple-space -- -D warnings

# Core must still pass unchanged
cargo test --lib
cargo clippy --lib --tests -- -D warnings

# Full integration
make test
```

---

## Throughput Expectations

For heavy AI flows the bottleneck is always the AI compute, not the TupleSpace.
For scatter-gather at volume the bottleneck is typically network bandwidth or
aggregator compute. In both cases the queue itself is transparent. Expected
TupleSpace overhead:

| Metric | Target | Mechanism |
|---|---|---|
| Hot-path put→take latency (waiter present) | < 5 μs | Zero-copy Bytes handoff, 1 mutex op each side |
| Store-path put latency (no waiter) | < 10 μs | VecDeque push + optional WAL append |
| Take latency (item present) | < 5 μs | VecDeque pop |
| Concurrent workers | 1 000+ | One parked tokio task per blocked take — effectively free |
| Max item size | ~50 MB | Bytes is reference-counted; no copy on handoff |
| Throughput ceiling (single node) | > 50 000 put/take pairs/s | Scatter-gather fans of thousands/s comfortably within range |

The resilience overhead per item:
- **Inflight KV write** (~1 ms gossip propagation) — invisible at LLM timescales
- **Replication RPC** (fired concurrently, not on critical path) — ~RTT to secondary,
  typically < 1 ms on co-located nodes
- **Heartbeat Signal** (5 s interval, amortised to zero per item)

None of these are on the producer's critical path. The producer's `tuple.put` returns
as soon as the item is in the local TupleStore (or handed to a waiting worker). The
KV write and replication RPC fire concurrently in background tasks.

---

## Future Extension — Sharded TupleSpace

### Scaling into finance-grade throughput

The single-primary design has a throughput ceiling (~50–200K ops/second per node).
For high-frequency finance workloads this is insufficient. However, Mycelium already
has the primitive to remove that ceiling: `shard_for` / `emit_sharded`.

### How sharding would work

Run N TupleSpace primaries, each owning a partition of the keyspace. Route `put(item)`
to the correct primary using the existing consistent-hash ring:

```rust
let owner = agent.shard_for(item_key, &CapFilter::new("tuple", &ns))?;
agent.emit_sharded("tuple.put", item_key, &CapFilter::new("tuple", &ns), payload).await?;
```

Each shard is an independent primary/secondary pair with its own WAL. Throughput scales
linearly: 100 shards × 100K ops/shard = **10M+ ops/second**, in Apache Flink's range,
at μs latency Flink cannot match (JVM floor is ~5ms; Rust floor is ~5μs).

### Natural fit for finance

Per-instrument sharding maps directly to the consistent hash ring — AAPL always routes
to shard 3, MSFT to shard 7. Per-instrument FIFO ordering is preserved within each
shard, which is exactly what market data pipelines require.

### Blockers before this is production-grade for finance

| Issue | Detail |
|---|---|
| **Cross-shard `complete()` atomicity** | The single-`CompleteRecord` WAL guarantee only holds when both stages live on the same primary. Cross-shard stage transitions require a two-phase protocol — significant additional complexity. |
| **Exactly-once delivery** | The current design is at-least-once (inflight TTL re-queue). Finance often requires exactly-once. Getting there requires idempotent consumers or distributed transactions. |
| **Flink's feature moat** | Windowing, temporal joins, SQL layer, CEP pattern matching, savepoints, connector ecosystem. A sharded TupleSpace competes on throughput and latency; it does not attempt to replicate Flink's analytical depth. |

### Realistic niche

A sharded TupleSpace would own **low-latency, high-throughput routing** — the part of
finance infrastructure that ingests and routes market events *before* they reach the
analytical layer. In that role it feeds Flink jobs rather than replacing them: sub-ms
per-instrument dispatch at 10M+ events/second, with no JVM GC spikes on the critical path.

### When to pursue this

When a concrete finance or high-frequency use case emerges that demonstrably hits the
single-primary ceiling. The sharding primitive is already in Mycelium; the TupleSpace
design is done. The incremental work is the cross-shard coordination protocol and
exactly-once semantics — both substantial but not architectural rewrites.
