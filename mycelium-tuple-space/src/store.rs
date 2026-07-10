//! Local execution substrate: per-stage queues, parked waiters, in-flight
//! tracking, and the optional write-ahead log.
//!
//! This module has no dependency on the Mycelium agent — it is the same kind
//! of node-local working memory as the `DashMap` inside the KV store. All
//! cluster concerns (RPC, capability election, inflight visibility keys) live
//! in `rpc.rs` / `lib.rs`.
//!
//! ## Lock order
//!
//! Three lock families exist here. When held simultaneously they must be
//! acquired in this order (release in reverse):
//!
//! 1. `WalInner` — held across the whole compaction rewrite; appends are
//!    short. Compaction acquires stage/inflight locks *inside* the WAL lock
//!    to snapshot live items.
//! 2. `StageInner` — the put/take hot-path lock; never held across `await`.
//! 3. `inflight` — leaf lock. `dispatch` and `take` acquire it while holding
//!    a `StageInner` lock (stage → inflight), never the reverse.
//!
//! No path holds a stage or inflight lock while *waiting* for the WAL lock
//! (appends happen after the stage guard is dropped), so the WAL-first order
//! used by compaction cannot deadlock against the hot path.

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::oneshot;

use crate::TupleError;

// ─── Stage state ─────────────────────────────────────────────────────────────

/// Inner state under a single lock — eliminates TOCTOU between waiter-check
/// and entry-store. The `Instant` is the enqueue time, for store-path queue
/// wait measurement (hot-path items never carry one — they never queue).
struct StageInner {
    entries: VecDeque<(u64, Bytes, std::time::Instant)>,
    waiters: VecDeque<oneshot::Sender<(u64, Bytes)>>,
    /// M13 (WS-G / G1a) keyed-exact-match rendezvous, kept **separate** from the FIFO so a keyed
    /// `take_by_key` and an unkeyed `take` on the same stage never interfere. Items put with a
    /// correlation key wait here to be claimed by that exact key (an O(1) hash lookup — not template
    /// matching). Both count toward `depth` (backpressure sees all queued items).
    keyed_entries: HashMap<Arc<str>, (u64, Bytes, std::time::Instant)>,
    /// Workers parked on a specific key (one per key — a second waiter on the same pending key
    /// replaces the first, whose oneshot then times out; a correlation key is, by definition, unique).
    keyed_waiters: HashMap<Arc<str>, oneshot::Sender<(u64, Bytes)>>,
}

/// Ring of the last N store-path queue waits (µs), for P99 reporting.
/// Hot-path deliveries are deliberately excluded — including their
/// sub-microsecond times would collapse the distribution and hide true
/// queueing latency.
const QUEUE_WAIT_SAMPLES: usize = 1000;

pub(crate) struct StageState {
    inner: Mutex<StageInner>,
    /// Shadow counter for lock-free monitoring reads.
    pub(crate) depth: AtomicU32,
    /// Strict mirror of `waiters.len()`: incremented at push, decremented at
    /// pop, BOTH under the stage lock. The timeout path deliberately does not
    /// touch it — the dead sender is still in the deque until a dispatch
    /// skips it, so the count may transiently include timed-out waiters but
    /// can never underflow. (Run-17 probe `metrics_accounting_identity`
    /// caught the previous double-decrement wrapping this to ~4.29e9.)
    pub(crate) waiters_count: AtomicU32,
    pub(crate) put_total: AtomicU64,
    pub(crate) take_total: AtomicU64,
    /// Puts delivered directly to a parked worker (never queued).
    pub(crate) hot_total: AtomicU64,
    queue_waits_us: Mutex<VecDeque<u32>>,
}

impl StageState {
    fn new() -> Self {
        Self {
            inner: Mutex::new(StageInner {
                entries: VecDeque::new(),
                waiters: VecDeque::new(),
                keyed_entries: HashMap::new(),
                keyed_waiters: HashMap::new(),
            }),
            depth: AtomicU32::new(0),
            waiters_count: AtomicU32::new(0),
            put_total: AtomicU64::new(0),
            take_total: AtomicU64::new(0),
            hot_total: AtomicU64::new(0),
            queue_waits_us: Mutex::new(VecDeque::with_capacity(64)),
        }
    }

    fn record_queue_wait(&self, waited: Duration) {
        let us = waited.as_micros().min(u32::MAX as u128) as u32;
        let mut g = self.queue_waits_us.lock();
        if g.len() == QUEUE_WAIT_SAMPLES {
            g.pop_front();
        }
        g.push_back(us);
    }

    /// P99 of the sampled store-path queue waits; 0 with no samples.
    pub(crate) fn queue_p99_us(&self) -> u32 {
        let mut samples: Vec<u32> = self.queue_waits_us.lock().iter().copied().collect();
        if samples.is_empty() {
            return 0;
        }
        samples.sort_unstable();
        samples[(samples.len() - 1) * 99 / 100]
    }
}

#[derive(Clone)]
pub(crate) struct Inflight {
    pub(crate) stage: Arc<str>,
    pub(crate) payload: Bytes,
    pub(crate) taken_at_ms: u64,
    /// M13 / WS-G: the correlation key if this item was put keyed, so a crash-requeue or compaction
    /// snapshot re-queues it under its key (not the FIFO). `None` for ordinary FIFO items.
    pub(crate) key: Option<Arc<str>>,
}

// ─── TupleStore ──────────────────────────────────────────────────────────────

pub(crate) struct TupleStore {
    stages: papaya::HashMap<Arc<str>, Arc<StageState>>,
    /// Items taken (or hot-handed) but not yet acked. Source of truth for
    /// re-queue; the WAL mirrors it for crash recovery.
    inflight: Mutex<HashMap<u64, Inflight>>,
    next_id: AtomicU64,
    high_watermark: u32,
    wal: Option<WalWriter>,
}

pub(crate) struct StageDepth {
    pub stage: Arc<str>,
    pub depth: u32,
    pub waiters: u32,
}

pub(crate) struct StageMetrics {
    pub stage: Arc<str>,
    pub depth: u32,
    pub waiters: u32,
    pub inflight: u32,
    pub put_total: u64,
    pub take_total: u64,
    pub hot_total: u64,
    pub queue_p99_us: u32,
}

impl TupleStore {
    /// Transient store (no WAL).
    pub(crate) fn transient(high_watermark: u32) -> Self {
        Self {
            stages: papaya::HashMap::new(),
            inflight: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(0),
            high_watermark,
            wal: None,
        }
    }

    /// WAL-backed store. Replays any existing log at `path`: every item with
    /// a `PutRecord`/`CompleteRecord` and no terminal ack is re-queued
    /// (including items that had a `TakeRecord` — they re-queue as abandoned).
    pub(crate) fn persistent(
        path: &Path,
        checkpoint_every: u64,
        high_watermark: u32,
    ) -> io::Result<Self> {
        let (wal, live, max_id) = WalWriter::open(path, checkpoint_every)?;
        let store = Self {
            stages: papaya::HashMap::new(),
            inflight: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(max_id.map_or(0, |m| m + 1)),
            high_watermark,
            wal: Some(wal),
        };
        for (id, stage, payload, key) in live {
            let state = store.stage(&stage);
            // Direct enqueue: no waiters can exist yet, no WAL write needed
            // (the records being replayed are already in the log). A keyed item re-queues into the
            // keyed index under its key (M13) so a post-restart take_by_key still rendezvouses.
            {
                let mut g = state.inner.lock();
                match key {
                    Some(k) => { g.keyed_entries.insert(k, (id, payload, std::time::Instant::now())); }
                    None => { g.entries.push_back((id, payload, std::time::Instant::now())); }
                }
            }
            state.depth.fetch_add(1, Ordering::Relaxed);
        }
        Ok(store)
    }

    fn stage(&self, name: &str) -> Arc<StageState> {
        let map = self.stages.pin();
        if let Some(s) = map.get(name) {
            return Arc::clone(s);
        }
        let key: Arc<str> = Arc::from(name);
        Arc::clone(map.get_or_insert_with(key, || Arc::new(StageState::new())))
    }

    /// Write item to `stage`. Returns the assigned item id.
    pub(crate) fn put(&self, stage: &str, payload: Bytes) -> Result<u64, TupleError> {
        let state = self.stage(stage);
        if state.depth.load(Ordering::Relaxed) >= self.high_watermark {
            return Err(TupleError::Backpressure { retry_after_ms: 500 });
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        // WAL once, before any dispatch decision — covers hot path and store
        // path with a single PutRecord regardless of how many dead waiters
        // the dispatch loop skips.
        if let Some(wal) = &self.wal {
            wal.append(&Record::Put {
                id,
                stage: Arc::from(stage),
                payload: payload.clone(),
                key: None,
            })?;
        }
        state.put_total.fetch_add(1, Ordering::Relaxed);
        let hot = self.dispatch(&state, Arc::from(stage), id, payload)?;
        if hot {
            state.hot_total.fetch_add(1, Ordering::Relaxed);
            state.take_total.fetch_add(1, Ordering::Relaxed);
        }
        Ok(id)
    }

    /// Hand `(id, payload)` to a parked waiter, or queue it. Returns `true`
    /// when the item was delivered directly (hot path).
    ///
    /// The item is registered in `inflight` *before* the oneshot send so a
    /// concurrent compaction snapshot can never observe it in neither place.
    fn dispatch(
        &self,
        state: &StageState,
        stage: Arc<str>,
        id: u64,
        mut payload: Bytes,
    ) -> Result<bool, TupleError> {
        let mut g = state.inner.lock();
        loop {
            match g.waiters.pop_front() {
                None => {
                    g.entries.push_back((id, payload, std::time::Instant::now()));
                    state.depth.fetch_add(1, Ordering::Relaxed);
                    return Ok(false);
                }
                Some(tx) => {
                    state.waiters_count.fetch_sub(1, Ordering::Relaxed);
                    self.inflight.lock().insert(
                        id,
                        Inflight {
                            stage: Arc::clone(&stage),
                            payload: payload.clone(),
                            taken_at_ms: now_ms(),
                            key: None,
                        },
                    );
                    drop(g); // release before send — no I/O or wakeup under lock
                    match tx.send((id, payload)) {
                        Ok(()) => {
                            if let Some(wal) = &self.wal {
                                wal.append(&Record::Take { id, taken_at_ms: now_ms() })?;
                            }
                            return Ok(true);
                        }
                        Err(p) => {
                            // Receiver timed out — undo, try next waiter or store.
                            self.inflight.lock().remove(&id);
                            payload = p.1;
                            g = state.inner.lock();
                        }
                    }
                }
            }
        }
    }

    /// Blocking claim. Parks until an item is available or `timeout` elapses.
    pub(crate) async fn take(
        &self,
        stage: &str,
        timeout: Duration,
    ) -> Result<(u64, Bytes), TupleError> {
        let state = self.stage(stage);
        let mut rx = {
            let mut g = state.inner.lock();
            if let Some((id, payload, enqueued)) = g.entries.pop_front() {
                state.depth.fetch_sub(1, Ordering::Relaxed);
                state.record_queue_wait(enqueued.elapsed());
                self.inflight.lock().insert(
                    id,
                    Inflight {
                        stage: Arc::from(stage),
                        payload: payload.clone(),
                        taken_at_ms: now_ms(),
                        key: None,
                    },
                );
                drop(g);
                if let Some(wal) = &self.wal {
                    wal.append(&Record::Take { id, taken_at_ms: now_ms() })?;
                }
                state.take_total.fetch_add(1, Ordering::Relaxed);
                return Ok((id, payload));
            }
            let (tx, rx) = oneshot::channel();
            g.waiters.push_back(tx);
            state.waiters_count.fetch_add(1, Ordering::Relaxed);
            rx
        }; // stage lock released before parking
        tokio::select! {
            item = &mut rx => match item {
                Ok((id, payload)) => {
                    state.take_total.fetch_add(1, Ordering::Relaxed);
                    Ok((id, payload))
                }
                // Sender dropped without sending (store shutdown).
                Err(_) => Err(TupleError::Timeout),
            },
            _ = tokio::time::sleep(timeout) => {
                // The dispatcher may have sent in the same instant the timer
                // fired — drain the channel before giving up so the item is
                // not lost (it is already registered as inflight).
                match rx.try_recv() {
                    Ok((id, payload)) => {
                        state.take_total.fetch_add(1, Ordering::Relaxed);
                        Ok((id, payload))
                    }
                    // No waiters_count decrement here: the dead sender is
                    // still queued; the dispatch that eventually pops it
                    // performs the matching decrement.
                    Err(_) => Err(TupleError::Timeout),
                }
            }
        }
    }

    // ── M13 / G1a: keyed-exact-match rendezvous (fan-in joins) ───────────────

    /// Put `payload` on `stage` under correlation `key` (WS-G / M13). Claimed only by
    /// [`take_by_key`](Self::take_by_key) with the same key — the two-stream rendezvous ("an invoice
    /// AND its matching purchase order") that exact lane names can't express without one lane per
    /// key. Hands off to a parked keyed waiter (hot path) or stores under the key.
    pub(crate) fn put_keyed(&self, stage: &str, key: Arc<str>, payload: Bytes) -> Result<u64, TupleError> {
        let state = self.stage(stage);
        if state.depth.load(Ordering::Relaxed) >= self.high_watermark {
            return Err(TupleError::Backpressure { retry_after_ms: 500 });
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if let Some(wal) = &self.wal {
            // WAL v2: the key is persisted so a keyed in-flight item re-queues under its key across
            // a crash / promotion (G1b).
            wal.append(&Record::Put {
                id,
                stage: Arc::from(stage),
                payload: payload.clone(),
                key: Some(Arc::clone(&key)),
            })?;
        }
        state.put_total.fetch_add(1, Ordering::Relaxed);
        let hot = self.dispatch_keyed(&state, Arc::from(stage), key, id, payload)?;
        if hot {
            state.hot_total.fetch_add(1, Ordering::Relaxed);
            state.take_total.fetch_add(1, Ordering::Relaxed);
        }
        Ok(id)
    }

    /// Hand a keyed `(id, payload)` to a waiter parked on `key`, or store it under `key`. Returns
    /// `true` on a direct (hot-path) delivery. Mirrors [`dispatch`](Self::dispatch): the item is
    /// registered in `inflight` before the oneshot send so a compaction snapshot never loses it.
    fn dispatch_keyed(
        &self,
        state: &StageState,
        stage: Arc<str>,
        key: Arc<str>,
        id: u64,
        payload: Bytes,
    ) -> Result<bool, TupleError> {
        let mut g = state.inner.lock();
        if let Some(tx) = g.keyed_waiters.remove(&key) {
            self.inflight.lock().insert(
                id,
                Inflight { stage: Arc::clone(&stage), payload: payload.clone(), taken_at_ms: now_ms(), key: Some(Arc::clone(&key)) },
            );
            drop(g);
            match tx.send((id, payload)) {
                Ok(()) => {
                    if let Some(wal) = &self.wal {
                        wal.append(&Record::Take { id, taken_at_ms: now_ms() })?;
                    }
                    return Ok(true);
                }
                Err(p) => {
                    // The keyed waiter timed out — undo the in-flight claim and fall through to store.
                    self.inflight.lock().remove(&id);
                    let mut g2 = state.inner.lock();
                    g2.keyed_entries.insert(key, (p.0, p.1, std::time::Instant::now()));
                    state.depth.fetch_add(1, Ordering::Relaxed);
                    return Ok(false);
                }
            }
        }
        g.keyed_entries.insert(key, (id, payload, std::time::Instant::now()));
        state.depth.fetch_add(1, Ordering::Relaxed);
        Ok(false)
    }

    /// Blocking keyed claim (WS-G / M13). Claims the item on `stage` whose correlation key is `key`,
    /// or parks a keyed waiter until one arrives or `timeout` elapses. O(1) exact-match — never
    /// template matching (that is the blackboard companion's territory).
    pub(crate) async fn take_by_key(
        &self,
        stage: &str,
        key: &str,
        timeout: Duration,
    ) -> Result<(u64, Bytes), TupleError> {
        let state = self.stage(stage);
        let key: Arc<str> = Arc::from(key);
        let mut rx = {
            let mut g = state.inner.lock();
            if let Some((id, payload, enqueued)) = g.keyed_entries.remove(&key) {
                state.depth.fetch_sub(1, Ordering::Relaxed);
                state.record_queue_wait(enqueued.elapsed());
                self.inflight.lock().insert(
                    id,
                    Inflight { stage: Arc::from(stage), payload: payload.clone(), taken_at_ms: now_ms(), key: Some(Arc::clone(&key)) },
                );
                drop(g);
                if let Some(wal) = &self.wal {
                    wal.append(&Record::Take { id, taken_at_ms: now_ms() })?;
                }
                state.take_total.fetch_add(1, Ordering::Relaxed);
                return Ok((id, payload));
            }
            let (tx, rx) = oneshot::channel();
            // One waiter per key; a prior waiter on this key is dropped (its take then times out).
            g.keyed_waiters.insert(Arc::clone(&key), tx);
            rx
        };
        tokio::select! {
            item = &mut rx => match item {
                Ok((id, payload)) => {
                    state.take_total.fetch_add(1, Ordering::Relaxed);
                    Ok((id, payload))
                }
                Err(_) => Err(TupleError::Timeout),
            },
            _ = tokio::time::sleep(timeout) => {
                match rx.try_recv() {
                    Ok((id, payload)) => {
                        state.take_total.fetch_add(1, Ordering::Relaxed);
                        Ok((id, payload))
                    }
                    Err(_) => {
                        // Remove our now-dead keyed waiter if it's still ours (a racing put may have
                        // already removed it to deliver).
                        state.inner.lock().keyed_waiters.remove(&key);
                        Err(TupleError::Timeout)
                    }
                }
            }
        }
    }

    /// Terminal ack: removes the in-flight entry and writes `AckRecord`.
    pub(crate) fn ack(&self, id: u64) -> Result<(), TupleError> {
        let removed = self.inflight.lock().remove(&id);
        match removed {
            None => Err(TupleError::NotFound),
            Some(_) => {
                if let Some(wal) = &self.wal {
                    wal.append(&Record::Ack { id })?;
                    wal.note_acked();
                }
                Ok(())
            }
        }
    }

    /// Atomic pipeline advance: acks `id` AND enqueues `payload` on
    /// `next_stage` under a single `CompleteRecord` — replay can never apply
    /// half of the transition.
    pub(crate) fn complete(
        &self,
        id: u64,
        next_stage: &str,
        payload: Bytes,
    ) -> Result<u64, TupleError> {
        // Claim the old item first; if it already expired back to the queue
        // (worker overran worker_timeout_secs) the complete is refused and
        // the at-least-once re-queue path owns the item.
        let removed = self.inflight.lock().remove(&id);
        if removed.is_none() {
            return Err(TupleError::NotFound);
        }
        let new_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if let Some(wal) = &self.wal {
            wal.append(&Record::Complete {
                old_id: id,
                new_id,
                stage: Arc::from(next_stage),
                payload: payload.clone(),
                key: None,
            })?;
            wal.note_acked();
        }
        let state = self.stage(next_stage);
        state.put_total.fetch_add(1, Ordering::Relaxed);
        let hot = self.dispatch(&state, Arc::from(next_stage), new_id, payload)?;
        if hot {
            state.hot_total.fetch_add(1, Ordering::Relaxed);
            state.take_total.fetch_add(1, Ordering::Relaxed);
        }
        Ok(new_id)
    }

    /// Atomic keyed pipeline advance (WS-G / M13): acks `id` AND puts `payload` on `next_stage`
    /// under correlation `key` in one `CompleteRecord` — the keyed analogue of [`complete`](Self::complete).
    pub(crate) fn complete_keyed(
        &self,
        id: u64,
        next_stage: &str,
        key: Arc<str>,
        payload: Bytes,
    ) -> Result<u64, TupleError> {
        let removed = self.inflight.lock().remove(&id);
        if removed.is_none() {
            return Err(TupleError::NotFound);
        }
        let new_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if let Some(wal) = &self.wal {
            wal.append(&Record::Complete {
                old_id: id,
                new_id,
                stage: Arc::from(next_stage),
                payload: payload.clone(),
                key: Some(Arc::clone(&key)),
            })?;
            wal.note_acked();
        }
        let state = self.stage(next_stage);
        state.put_total.fetch_add(1, Ordering::Relaxed);
        let hot = self.dispatch_keyed(&state, Arc::from(next_stage), key, new_id, payload)?;
        if hot {
            state.hot_total.fetch_add(1, Ordering::Relaxed);
            state.take_total.fetch_add(1, Ordering::Relaxed);
        }
        Ok(new_id)
    }

    /// Applies a replicated or replayed item under its ORIGINAL id: enqueue
    /// bypassing the watermark (replication must not drop), and fence
    /// `next_id` past it so a promoted secondary never re-issues an id that
    /// the old primary already assigned.
    pub(crate) fn put_with_id(
        &self,
        stage: &str,
        id: u64,
        payload: Bytes,
        key: Option<Arc<str>>,
    ) -> Result<(), TupleError> {
        self.next_id.fetch_max(id + 1, Ordering::Relaxed);
        if let Some(wal) = &self.wal {
            wal.append(&Record::Put {
                id,
                stage: Arc::from(stage),
                payload: payload.clone(),
                key: key.clone(),
            })?;
        }
        let state = self.stage(stage);
        state.put_total.fetch_add(1, Ordering::Relaxed);
        // A keyed replicated item routes into the keyed index so a take_by_key on a promoted mirror
        // still rendezvouses (G1b).
        let hot = match key {
            Some(k) => self.dispatch_keyed(&state, Arc::from(stage), k, id, payload)?,
            None => self.dispatch(&state, Arc::from(stage), id, payload)?,
        };
        if hot {
            state.hot_total.fetch_add(1, Ordering::Relaxed);
            state.take_total.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Removes a queued (not in-flight) item by id — used when a mirror
    /// applies a replicated ack/complete for an item it holds queued.
    /// Returns `true` when the item was found and removed.
    pub(crate) fn remove_queued(&self, stage: &str, id: u64) -> bool {
        let state = self.stage(stage);
        let mut g = state.inner.lock();
        let before = g.entries.len();
        g.entries.retain(|(eid, _, _)| *eid != id);
        let mut removed = before - g.entries.len();
        // A keyed queued item (M13) lives in the keyed index, not the FIFO.
        if removed == 0 {
            let key = g.keyed_entries.iter().find(|(_, (eid, _, _))| *eid == id).map(|(k, _)| Arc::clone(k));
            if let Some(k) = key {
                g.keyed_entries.remove(&k);
                removed = 1;
            }
        }
        if removed > 0 {
            state.depth.fetch_sub(removed as u32, Ordering::Relaxed);
            if let Some(wal) = &self.wal {
                drop(g);
                let _ = wal.append(&Record::Ack { id });
                wal.note_acked();
            }
            true
        } else {
            false
        }
    }

    /// Re-queues every in-flight item older than `timeout`. Returns the ids
    /// re-queued. No WAL record is written: the item's `PutRecord` is still
    /// live (no ack), so replay already re-queues it.
    pub(crate) fn requeue_expired(&self, timeout: Duration) -> Vec<u64> {
        let cutoff = now_ms().saturating_sub(timeout.as_millis() as u64);
        let expired: Vec<(u64, Inflight)> = {
            let mut g = self.inflight.lock();
            let ids: Vec<u64> = g
                .iter()
                .filter(|(_, v)| v.taken_at_ms <= cutoff)
                .map(|(k, _)| *k)
                .collect();
            ids.into_iter()
                .filter_map(|id| g.remove(&id).map(|v| (id, v)))
                .collect()
        }; // inflight lock released before dispatch re-acquires stage → inflight
        let mut requeued = Vec::with_capacity(expired.len());
        for (id, item) in expired {
            let state = self.stage(&item.stage);
            // A keyed item re-queues under its key (M13) so its waiter still rendezvouses.
            let ok = match item.key {
                Some(k) => self.dispatch_keyed(&state, Arc::clone(&item.stage), k, id, item.payload).is_ok(),
                None => self.dispatch(&state, Arc::clone(&item.stage), id, item.payload).is_ok(),
            };
            if ok {
                requeued.push(id);
            }
        }
        requeued
    }

    /// Lock-free depth snapshot for one stage or all stages.
    pub(crate) fn depth(&self, stage: Option<&str>) -> Vec<StageDepth> {
        let map = self.stages.pin();
        map.iter()
            .filter(|(name, _)| stage.is_none_or(|s| s == name.as_ref()))
            .map(|(name, state)| StageDepth {
                stage: Arc::clone(name),
                depth: state.depth.load(Ordering::Relaxed),
                waiters: state.waiters_count.load(Ordering::Relaxed),
            })
            .collect()
    }

    /// Full per-stage counter snapshot for the metrics writer.
    pub(crate) fn metrics_snapshot(&self) -> Vec<StageMetrics> {
        let by_stage = self.inflight_by_stage();
        self.stage_states()
            .into_iter()
            .map(|(stage, state)| StageMetrics {
                inflight: by_stage.get(stage.as_ref()).copied().unwrap_or(0),
                depth: state.depth.load(Ordering::Relaxed),
                waiters: state.waiters_count.load(Ordering::Relaxed),
                put_total: state.put_total.load(Ordering::Relaxed),
                take_total: state.take_total.load(Ordering::Relaxed),
                hot_total: state.hot_total.load(Ordering::Relaxed),
                queue_p99_us: state.queue_p99_us(),
                stage,
            })
            .collect()
    }

    /// Exercised by unit tests; the per-stage view is in `metrics_snapshot`.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn inflight_count(&self) -> usize {
        self.inflight.lock().len()
    }

    /// In-flight counts grouped by stage, for depth reporting.
    pub(crate) fn inflight_by_stage(&self) -> HashMap<Arc<str>, u32> {
        let g = self.inflight.lock();
        let mut out: HashMap<Arc<str>, u32> = HashMap::new();
        for item in g.values() {
            *out.entry(Arc::clone(&item.stage)).or_insert(0) += 1;
        }
        out
    }

    pub(crate) fn inflight_snapshot(&self) -> Vec<(u64, Inflight)> {
        self.inflight
            .lock()
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }

    pub(crate) fn stage_states(&self) -> Vec<(Arc<str>, Arc<StageState>)> {
        let map = self.stages.pin();
        map.iter()
            .map(|(k, v)| (Arc::clone(k), Arc::clone(v)))
            .collect()
    }

    pub(crate) fn wal_bytes(&self) -> u64 {
        self.wal.as_ref().map_or(0, WalWriter::file_len)
    }

    /// True when more than half the logged items are acked — caller should
    /// invoke [`compact_now`](Self::compact_now).
    pub(crate) fn wants_compaction(&self) -> bool {
        self.wal.as_ref().is_some_and(WalWriter::wants_compaction)
    }

    /// Rewrites the WAL to contain only live items (queued + in-flight) and
    /// atomically swaps it in. Holds the WAL lock for the duration; appends
    /// block but the in-memory hot path does not.
    pub(crate) fn compact_now(&self) -> io::Result<()> {
        let Some(wal) = &self.wal else { return Ok(()) };
        wal.compact(|| {
            let mut live: Vec<Record> = Vec::new();
            for (name, state) in self.stage_states() {
                let g = state.inner.lock();
                for (id, payload, _) in &g.entries {
                    live.push(Record::Put {
                        id: *id,
                        stage: Arc::clone(&name),
                        payload: payload.clone(),
                        key: None,
                    });
                }
                // Keyed queued items are preserved under their key (M13 / G1b).
                for (key, (id, payload, _)) in &g.keyed_entries {
                    live.push(Record::Put {
                        id: *id,
                        stage: Arc::clone(&name),
                        payload: payload.clone(),
                        key: Some(Arc::clone(key)),
                    });
                }
            }
            for (id, item) in self.inflight_snapshot() {
                live.push(Record::Put {
                    id,
                    stage: Arc::clone(&item.stage),
                    payload: item.payload.clone(),
                    key: item.key.clone(),
                });
                live.push(Record::Take { id, taken_at_ms: item.taken_at_ms });
            }
            live
        })
    }

    /// `(epoch, head)` of the WAL, or `(0, 0)` for a transient store.
    pub(crate) fn wal_position(&self) -> (u64, u64) {
        self.wal.as_ref().map_or((0, 0), WalWriter::position)
    }

    /// Current-state chunk for the join-time backfill of a **transient** (WAL-less) store:
    /// live items (queued FIFO + keyed + inflight) with `id >= from_id`, encoded as `Put`
    /// records, ordered by id, bounded by `max_entries`/`max_bytes`. The id is the pagination
    /// cursor (ids are monotone from `next_id`), so items put mid-scan are still picked up and
    /// items acked mid-scan simply drop out — at-least-once, deduped by the mirror's apply.
    /// Returns `(raw, next_id, done)`. Lock discipline: one stage lock at a time, then the
    /// inflight lock — sequential, never nested.
    pub(crate) fn state_chunk(
        &self,
        from_id: u64,
        max_entries: usize,
        max_bytes: usize,
    ) -> (Vec<u8>, u64, bool) {
        // (id, stage, payload, correlation key) — one live item's snapshot.
        type LiveItem = (u64, Arc<str>, Bytes, Option<Arc<str>>);
        let mut items: Vec<LiveItem> = Vec::new();
        {
            let guard = self.stages.pin();
            for (stage, st) in guard.iter() {
                let g = st.inner.lock();
                for (id, payload, _) in g.entries.iter() {
                    if *id >= from_id {
                        items.push((*id, Arc::clone(stage), payload.clone(), None));
                    }
                }
                for (key, (id, payload, _)) in g.keyed_entries.iter() {
                    if *id >= from_id {
                        items.push((*id, Arc::clone(stage), payload.clone(), Some(Arc::clone(key))));
                    }
                }
            }
        }
        {
            let g = self.inflight.lock();
            for (id, inf) in g.iter() {
                if *id >= from_id {
                    items.push((*id, Arc::clone(&inf.stage), inf.payload.clone(), inf.key.clone()));
                }
            }
        }
        items.sort_unstable_by_key(|(id, ..)| *id);
        let mut raw = Vec::new();
        let mut next = from_id;
        for (n, (id, stage, payload, key)) in items.into_iter().enumerate() {
            if n >= max_entries || raw.len() >= max_bytes {
                return (raw, next, false);
            }
            Record::Put { id, stage, payload, key }.encode(&mut raw);
            next = id + 1;
        }
        (raw, next, true)
    }

    /// Serves a replay chunk from the WAL. `None` for a transient store.
    pub(crate) fn wal_read_chunk(
        &self,
        expect_epoch: u64,
        from_offset: u64,
        max_entries: usize,
        max_bytes: usize,
    ) -> io::Result<Option<WalChunkData>> {
        match &self.wal {
            None => Ok(None),
            Some(wal) => wal
                .read_chunk(expect_epoch, from_offset, max_entries, max_bytes)
                .map(Some),
        }
    }

    /// Fsync the WAL when the ops threshold is reached, or unconditionally
    /// when `force` is set (periodic 1 s safety sync, shutdown). Cheap no-op
    /// when nothing is pending.
    pub(crate) fn checkpoint_if_due(&self, force: bool) -> io::Result<()> {
        match &self.wal {
            Some(wal) => wal.sync_if_due(force),
            None => Ok(()),
        }
    }
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

// ─── WAL ─────────────────────────────────────────────────────────────────────

/// WAL file header: magic + format version. A file whose version is newer
/// than this build understands is REFUSED at open — never truncated. Without
/// the header, a future format change would read as a torn tail and be
/// silently truncated: an upgrade data-loss hazard (Run-17 finding,
/// Evolvability).
const WAL_MAGIC: &[u8; 6] = b"MTSWAL";
/// v2 (WS-G / M13) adds the keyed record kinds (`REC_PUT_KEYED` / `REC_COMPLETE_KEYED`). Unkeyed
/// records are byte-identical to v1, so a v1 WAL replays unchanged and a v2 WAL with no keyed items
/// is byte-identical to a v1 one. v1 is accepted on read (rolling-upgrade window).
const WAL_VERSION: u16 = 2;
const PREV_WAL_VERSION: u16 = 1;
const WAL_HEADER_LEN: u64 = 8; // magic + u16 LE version

fn wal_header() -> [u8; WAL_HEADER_LEN as usize] {
    let mut h = [0u8; WAL_HEADER_LEN as usize];
    h[..6].copy_from_slice(WAL_MAGIC);
    h[6..8].copy_from_slice(&WAL_VERSION.to_le_bytes());
    h
}

const REC_PUT: u8 = 1;
const REC_TAKE: u8 = 2;
const REC_ACK: u8 = 3;
const REC_COMPLETE: u8 = 4;
// WS-G / M13 (WAL v2): keyed variants carry a correlation key after the stage.
const REC_PUT_KEYED: u8 = 5;
const REC_COMPLETE_KEYED: u8 = 6;

/// One chunk of raw WAL records served to a replaying secondary.
pub(crate) struct WalChunkData {
    pub epoch: u64,
    pub raw: Vec<u8>,
    pub next_offset: u64,
    pub done: bool,
}

/// One WAL record. `Complete` is a distinct type (not Ack + Put) so replay
/// applies the stage transition atomically — see the plan doc for the
/// duplicate hazard the split encoding reintroduces.
#[derive(Debug)]
pub(crate) enum Record {
    Put { id: u64, stage: Arc<str>, payload: Bytes, key: Option<Arc<str>> },
    Take { id: u64, taken_at_ms: u64 },
    Ack { id: u64 },
    Complete { old_id: u64, new_id: u64, stage: Arc<str>, payload: Bytes, key: Option<Arc<str>> },
}

impl Record {
    pub(crate) fn encode(&self, buf: &mut Vec<u8>) {
        let body_start = buf.len() + 5; // [kind u8][len u32]
        match self {
            Record::Put { id, stage, payload, key } => {
                buf.push(if key.is_some() { REC_PUT_KEYED } else { REC_PUT });
                buf.extend_from_slice(&[0; 4]);
                buf.extend_from_slice(&id.to_le_bytes());
                buf.extend_from_slice(&(stage.len() as u16).to_le_bytes());
                buf.extend_from_slice(stage.as_bytes());
                if let Some(k) = key {
                    buf.extend_from_slice(&(k.len() as u16).to_le_bytes());
                    buf.extend_from_slice(k.as_bytes());
                }
                buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
                buf.extend_from_slice(payload);
            }
            Record::Take { id, taken_at_ms } => {
                buf.push(REC_TAKE);
                buf.extend_from_slice(&[0; 4]);
                buf.extend_from_slice(&id.to_le_bytes());
                buf.extend_from_slice(&taken_at_ms.to_le_bytes());
            }
            Record::Ack { id } => {
                buf.push(REC_ACK);
                buf.extend_from_slice(&[0; 4]);
                buf.extend_from_slice(&id.to_le_bytes());
            }
            Record::Complete { old_id, new_id, stage, payload, key } => {
                buf.push(if key.is_some() { REC_COMPLETE_KEYED } else { REC_COMPLETE });
                buf.extend_from_slice(&[0; 4]);
                buf.extend_from_slice(&old_id.to_le_bytes());
                buf.extend_from_slice(&new_id.to_le_bytes());
                buf.extend_from_slice(&(stage.len() as u16).to_le_bytes());
                buf.extend_from_slice(stage.as_bytes());
                if let Some(k) = key {
                    buf.extend_from_slice(&(k.len() as u16).to_le_bytes());
                    buf.extend_from_slice(k.as_bytes());
                }
                buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
                buf.extend_from_slice(payload);
            }
        }
        let body_len = (buf.len() - body_start) as u32;
        buf[body_start - 4..body_start].copy_from_slice(&body_len.to_le_bytes());
    }

    /// Decodes one record from `data`. Returns `(record, bytes_consumed)`,
    /// or `None` when the remaining bytes are a truncated tail.
    fn decode(data: &[u8]) -> Option<(Record, usize)> {
        if data.len() < 5 {
            return None;
        }
        let kind = data[0];
        let body_len = u32::from_le_bytes(data[1..5].try_into().ok()?) as usize;
        let body = data.get(5..5 + body_len)?;
        let rec = match kind {
            REC_PUT | REC_PUT_KEYED => {
                let id = u64::from_le_bytes(body.get(..8)?.try_into().ok()?);
                let stage_len =
                    u16::from_le_bytes(body.get(8..10)?.try_into().ok()?) as usize;
                let stage = std::str::from_utf8(body.get(10..10 + stage_len)?).ok()?;
                let mut p = 10 + stage_len;
                let key = if kind == REC_PUT_KEYED {
                    let kl = u16::from_le_bytes(body.get(p..p + 2)?.try_into().ok()?) as usize;
                    let k = std::str::from_utf8(body.get(p + 2..p + 2 + kl)?).ok()?;
                    p += 2 + kl;
                    Some(Arc::from(k))
                } else {
                    None
                };
                let payload_len =
                    u32::from_le_bytes(body.get(p..p + 4)?.try_into().ok()?) as usize;
                let payload = body.get(p + 4..p + 4 + payload_len)?;
                Record::Put {
                    id,
                    stage: Arc::from(stage),
                    payload: Bytes::copy_from_slice(payload),
                    key,
                }
            }
            REC_TAKE => Record::Take {
                id: u64::from_le_bytes(body.get(..8)?.try_into().ok()?),
                taken_at_ms: u64::from_le_bytes(body.get(8..16)?.try_into().ok()?),
            },
            REC_ACK => Record::Ack {
                id: u64::from_le_bytes(body.get(..8)?.try_into().ok()?),
            },
            REC_COMPLETE | REC_COMPLETE_KEYED => {
                let old_id = u64::from_le_bytes(body.get(..8)?.try_into().ok()?);
                let new_id = u64::from_le_bytes(body.get(8..16)?.try_into().ok()?);
                let stage_len =
                    u16::from_le_bytes(body.get(16..18)?.try_into().ok()?) as usize;
                let stage = std::str::from_utf8(body.get(18..18 + stage_len)?).ok()?;
                let mut p = 18 + stage_len;
                let key = if kind == REC_COMPLETE_KEYED {
                    let kl = u16::from_le_bytes(body.get(p..p + 2)?.try_into().ok()?) as usize;
                    let k = std::str::from_utf8(body.get(p + 2..p + 2 + kl)?).ok()?;
                    p += 2 + kl;
                    Some(Arc::from(k))
                } else {
                    None
                };
                let payload_len =
                    u32::from_le_bytes(body.get(p..p + 4)?.try_into().ok()?) as usize;
                let payload = body.get(p + 4..p + 4 + payload_len)?;
                Record::Complete {
                    old_id,
                    new_id,
                    stage: Arc::from(stage),
                    payload: Bytes::copy_from_slice(payload),
                    key,
                }
            }
            _ => return None, // unknown kind — treat as corrupt tail
        };
        Some((rec, 5 + body_len))
    }
}

struct WalInner {
    file: File,
    path: PathBuf,
    file_len: u64,
    ops_since_sync: u64,
    /// Live put-side records (Put + the new half of Complete).
    total: u64,
    /// Terminal records (Ack + the old half of Complete).
    acked: u64,
    /// Bumped on every compaction. Byte offsets are only comparable within
    /// one epoch; a replay client whose epoch is stale must restart from 0.
    epoch: u64,
}

pub(crate) struct WalWriter {
    inner: Mutex<WalInner>,
    checkpoint_every: u64,
    file_len_shadow: AtomicU64,
}

impl WalWriter {
    /// Opens (or creates) the WAL at `path`, replays it, and returns the
    /// writer plus the live items `(id, stage, payload)` in id order and the
    /// highest id seen (for the `next_id` fence).
    #[allow(clippy::type_complexity)]
    fn open(
        path: &Path,
        checkpoint_every: u64,
    ) -> io::Result<(Self, Vec<(u64, Arc<str>, Bytes, Option<Arc<str>>)>, Option<u64>)> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(path)?;
        let mut data = Vec::new();
        file.seek(SeekFrom::Start(0))?;
        file.read_to_end(&mut data)?;

        // Header handling. Empty file: stamp the current header. Non-empty:
        // the magic must match and the version must be one this build
        // understands — anything else is refused with the file untouched
        // (silent truncation of a future format is the failure mode this
        // header exists to prevent).
        if data.is_empty() {
            file.write_all(&wal_header())?;
            data.extend_from_slice(&wal_header());
        } else if data.len() < WAL_HEADER_LEN as usize
            || &data[..WAL_MAGIC.len()] != WAL_MAGIC
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{} is not a mycelium-tuple-space WAL (missing MTSWAL magic); \
                     refusing to open or modify it",
                    path.display()
                ),
            ));
        } else {
            let version =
                u16::from_le_bytes(data[6..8].try_into().expect("two header bytes"));
            // Accept the current and the previous format (rolling upgrade); refuse a *newer* one
            // (no silent truncation of a format this build does not understand). v1 unkeyed records
            // are byte-identical, so a v1 WAL replays unchanged here.
            if version != WAL_VERSION && version != PREV_WAL_VERSION {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{} is WAL format v{version}, but this build supports v{WAL_VERSION} \
                         (and reads v{PREV_WAL_VERSION}); refusing to open (no silent truncation \
                         of newer formats)",
                        path.display()
                    ),
                ));
            }
        }

        struct ItemState {
            stage: Arc<str>,
            payload: Bytes,
            acked: bool,
            key: Option<Arc<str>>,
        }
        let mut items: BTreeMap<u64, ItemState> = BTreeMap::new();
        let mut total = 0u64;
        let mut acked = 0u64;
        let mut offset = WAL_HEADER_LEN as usize;
        while offset < data.len() {
            match Record::decode(&data[offset..]) {
                None => break, // truncated tail from a mid-append crash
                Some((rec, consumed)) => {
                    offset += consumed;
                    match rec {
                        Record::Put { id, stage, payload, key } => {
                            total += 1;
                            items.insert(id, ItemState { stage, payload, acked: false, key });
                        }
                        // Take alone never terminates an item: taken-but-unacked
                        // re-queues as abandoned.
                        Record::Take { .. } => {}
                        Record::Ack { id } => {
                            acked += 1;
                            if let Some(it) = items.get_mut(&id) {
                                it.acked = true;
                            }
                        }
                        Record::Complete { old_id, new_id, stage, payload, key } => {
                            acked += 1;
                            total += 1;
                            if let Some(it) = items.get_mut(&old_id) {
                                it.acked = true;
                            }
                            items.insert(new_id, ItemState { stage, payload, acked: false, key });
                        }
                    }
                }
            }
        }
        if offset < data.len() {
            // Drop the corrupt/truncated tail so future appends start clean.
            file.set_len(offset as u64)?;
        }
        let max_id = items.keys().next_back().copied();
        let live: Vec<(u64, Arc<str>, Bytes, Option<Arc<str>>)> = items
            .into_iter()
            .filter(|(_, it)| !it.acked)
            .map(|(id, it)| (id, it.stage, it.payload, it.key))
            .collect();
        let file_len = offset as u64;
        let writer = Self {
            inner: Mutex::new(WalInner {
                file,
                path: path.to_path_buf(),
                file_len,
                ops_since_sync: 0,
                total,
                acked,
                epoch: 0,
            }),
            checkpoint_every,
            file_len_shadow: AtomicU64::new(file_len),
        };
        Ok((writer, live, max_id))
    }

    fn append(&self, rec: &Record) -> Result<(), TupleError> {
        let mut buf = Vec::with_capacity(64);
        rec.encode(&mut buf);
        let mut g = self.inner.lock();
        g.file.write_all(&buf).map_err(TupleError::Io)?;
        g.file_len += buf.len() as u64;
        g.ops_since_sync += 1;
        if matches!(rec, Record::Put { .. } | Record::Complete { .. }) {
            g.total += 1;
        }
        self.file_len_shadow.store(g.file_len, Ordering::Relaxed);
        Ok(())
    }

    fn note_acked(&self) {
        self.inner.lock().acked += 1;
    }

    fn wants_compaction(&self) -> bool {
        let g = self.inner.lock();
        g.total > 0 && g.acked * 2 > g.total
    }

    fn file_len(&self) -> u64 {
        self.file_len_shadow.load(Ordering::Relaxed)
    }

    /// Fsync when the ops threshold is reached, or when `force` is set and
    /// any appends are pending.
    fn sync_if_due(&self, force: bool) -> io::Result<()> {
        let mut g = self.inner.lock();
        let due = g.ops_since_sync >= self.checkpoint_every
            || (force && g.ops_since_sync > 0);
        if due {
            g.file.sync_data()?;
            g.ops_since_sync = 0;
        }
        Ok(())
    }

    /// Rewrites the log with the records produced by `snapshot`, atomically
    /// swapping the new file in. `snapshot` runs while the WAL lock is held
    /// so no append can interleave with the rewrite.
    fn compact(&self, snapshot: impl FnOnce() -> Vec<Record>) -> io::Result<()> {
        let mut g = self.inner.lock();
        let live = snapshot();
        let tmp_path = g.path.with_extension("compact");
        let mut tmp = File::create(&tmp_path)?;
        let mut buf = Vec::new();
        buf.extend_from_slice(&wal_header());
        let mut total = 0u64;
        for rec in &live {
            if matches!(rec, Record::Put { .. } | Record::Complete { .. }) {
                total += 1;
            }
            rec.encode(&mut buf);
        }
        tmp.write_all(&buf)?;
        tmp.sync_data()?;
        std::fs::rename(&tmp_path, &g.path)?;
        let file = OpenOptions::new().read(true).append(true).open(&g.path)?;
        g.file = file;
        g.file_len = buf.len() as u64;
        g.ops_since_sync = 0;
        g.total = total;
        g.acked = 0;
        g.epoch += 1;
        self.file_len_shadow.store(g.file_len, Ordering::Relaxed);
        Ok(())
    }

    /// `(epoch, head)` — the current compaction epoch and byte length.
    fn position(&self) -> (u64, u64) {
        let g = self.inner.lock();
        (g.epoch, g.file_len)
    }

    /// Reads raw record bytes starting at `from_offset`, bounded by
    /// `min(max_entries, max_bytes)` — whichever is hit first. Returns
    /// `(epoch, raw, next_offset, done)`. When `expect_epoch` is stale the
    /// caller's offsets are meaningless: returns the current epoch with no
    /// data and `next_offset = 0` so the client restarts.
    fn read_chunk(
        &self,
        expect_epoch: u64,
        from_offset: u64,
        max_entries: usize,
        max_bytes: usize,
    ) -> io::Result<WalChunkData> {
        let mut g = self.inner.lock();
        if g.epoch != expect_epoch {
            return Ok(WalChunkData {
                epoch: g.epoch,
                raw: Vec::new(),
                next_offset: 0,
                done: false,
            });
        }
        // A cursor of 0 (or anything inside the header) starts at the first
        // record, not at the magic bytes — which would decode as garbage and
        // stall the replay loop.
        let from_offset = from_offset.max(WAL_HEADER_LEN);
        let head = g.file_len;
        if from_offset >= head {
            return Ok(WalChunkData {
                epoch: g.epoch,
                raw: Vec::new(),
                next_offset: head,
                done: true,
            });
        }
        let want = ((head - from_offset) as usize).min(max_bytes.max(64));
        let mut raw = vec![0u8; want];
        // Positioned reads so the append cursor (O_APPEND) is untouched.
        g.file.seek(SeekFrom::Start(from_offset))?;
        let mut filled = 0;
        while filled < want {
            let n = g.file.read(&mut raw[filled..])?;
            if n == 0 {
                break;
            }
            filled += n;
        }
        raw.truncate(filled);
        // Trim to whole records within the entry budget.
        let mut consumed = 0usize;
        let mut entries = 0usize;
        while consumed < raw.len() && entries < max_entries {
            match Record::decode(&raw[consumed..]) {
                Some((_, n)) => {
                    consumed += n;
                    entries += 1;
                }
                None => break, // partial record at the end of the chunk
            }
        }
        raw.truncate(consumed);
        let next = from_offset + consumed as u64;
        Ok(WalChunkData {
            epoch: g.epoch,
            raw,
            next_offset: next,
            done: next >= head,
        })
    }
}

/// Decodes a raw record stream (as served by `wal_read_chunk`) back into
/// records. Stops at the first malformed/partial record.
pub(crate) fn decode_records(data: &[u8]) -> Vec<Record> {
    let mut out = Vec::new();
    let mut offset = 0;
    while offset < data.len() {
        match Record::decode(&data[offset..]) {
            Some((rec, n)) => {
                out.push(rec);
                offset += n;
            }
            None => break,
        }
    }
    out
}

/// Test-only: read every record currently in the WAL file at `path`,
/// skipping the file header.
#[cfg(test)]
pub(crate) fn read_wal(path: &Path) -> Vec<Record> {
    let data = std::fs::read(path).unwrap_or_default();
    let mut out = Vec::new();
    let mut offset = WAL_HEADER_LEN as usize;
    while offset < data.len() {
        match Record::decode(&data[offset..]) {
            Some((rec, consumed)) => {
                out.push(rec);
                offset += consumed;
            }
            None => break,
        }
    }
    out
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_wal(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "mts-wal-{}-{}.log",
            std::process::id(),
            name
        ));
        let _ = std::fs::remove_file(&p);
        p
    }

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[tokio::test]
    async fn put_then_take() {
        let store = TupleStore::transient(500);
        let id_a = store.put("s", b("alpha")).unwrap();
        let id_b = store.put("s", b("beta")).unwrap();
        let (got_a, pa) = store.take("s", Duration::from_millis(100)).await.unwrap();
        let (got_b, pb) = store.take("s", Duration::from_millis(100)).await.unwrap();
        // FIFO order guaranteed by the VecDeque.
        assert_eq!((got_a, pa.as_ref()), (id_a, &b"alpha"[..]));
        assert_eq!((got_b, pb.as_ref()), (id_b, &b"beta"[..]));
        assert_eq!(store.depth(Some("s"))[0].depth, 0);
    }

    // ── M13 / WS-G · G-G1a: keyed-exact-match rendezvous ────────────────────

    #[tokio::test]
    async fn keyed_put_then_take_by_key() {
        let store = TupleStore::transient(500);
        let id = store.put_keyed("po", Arc::from("inv-42"), b("purchase-order")).unwrap();
        // The wrong key does not match (times out fast); the right key claims it.
        assert!(store.take_by_key("po", "inv-99", Duration::from_millis(50)).await.is_err());
        let (got, payload) = store.take_by_key("po", "inv-42", Duration::from_millis(100)).await.unwrap();
        assert_eq!((got, payload.as_ref()), (id, &b"purchase-order"[..]));
        assert_eq!(store.depth(Some("po"))[0].depth, 0, "keyed take decrements depth");
    }

    #[tokio::test]
    async fn keyed_take_parks_until_keyed_put() {
        // The two-stream join: a worker waits on a correlation key before the item arrives.
        let store = Arc::new(TupleStore::transient(500));
        let s2 = Arc::clone(&store);
        let waiter = tokio::spawn(async move {
            s2.take_by_key("po", "inv-7", Duration::from_secs(5)).await
        });
        tokio::time::sleep(Duration::from_millis(50)).await; // ensure the waiter parks first
        let id = store.put_keyed("po", Arc::from("inv-7"), b("matched")).unwrap();
        let (got, payload) = waiter.await.unwrap().unwrap();
        assert_eq!((got, payload.as_ref()), (id, &b"matched"[..]));
    }

    #[tokio::test]
    async fn keyed_and_unkeyed_lanes_do_not_interfere() {
        let store = TupleStore::transient(500);
        // A keyed item is NOT claimable by an unkeyed FIFO take, and vice versa.
        store.put_keyed("s", Arc::from("k1"), b("keyed")).unwrap();
        assert!(store.take("s", Duration::from_millis(50)).await.is_err(),
            "an unkeyed take must not claim a keyed item");
        let id_u = store.put("s", b("unkeyed")).unwrap();
        assert!(store.take_by_key("s", "k1", Duration::from_millis(50)).await.is_ok(),
            "the keyed item is still claimable by its key");
        let (got, _) = store.take("s", Duration::from_millis(50)).await.unwrap();
        assert_eq!(got, id_u, "the unkeyed item is claimable by an unkeyed take");
    }

    #[tokio::test]
    async fn keyed_complete_advances_under_key() {
        let store = TupleStore::transient(500);
        let id = store.put_keyed("a", Arc::from("corr"), b("v1")).unwrap();
        let (claimed, _) = store.take_by_key("a", "corr", Duration::from_millis(100)).await.unwrap();
        assert_eq!(claimed, id);
        // complete_keyed advances the item onto stage "b" under the same correlation key.
        store.complete_keyed(id, "b", Arc::from("corr"), b("v2")).unwrap();
        let (_, payload) = store.take_by_key("b", "corr", Duration::from_millis(100)).await.unwrap();
        assert_eq!(payload.as_ref(), &b"v2"[..]);
    }

    #[tokio::test]
    async fn take_before_put() {
        let store = Arc::new(TupleStore::transient(500));
        let s2 = Arc::clone(&store);
        let waiter =
            tokio::spawn(async move { s2.take("s", Duration::from_secs(5)).await });
        // Poll until the waiter is parked (structural, not a sleep).
        for _ in 0..100 {
            if store.depth(Some("s")).first().map(|d| d.waiters) == Some(1) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let id = store.put("s", b("x")).unwrap();
        let (got, payload) = waiter.await.unwrap().unwrap();
        assert_eq!(got, id);
        assert_eq!(payload.as_ref(), b"x");
        // No spurious wakeup: queue stayed empty, hot counter incremented.
        assert_eq!(store.depth(Some("s"))[0].depth, 0);
        let states = store.stage_states();
        assert_eq!(states[0].1.hot_total.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn take_timeout() {
        let store = TupleStore::transient(500);
        let r = store.take("empty", Duration::from_millis(50)).await;
        assert!(matches!(r, Err(TupleError::Timeout)));
    }

    #[tokio::test]
    async fn timed_out_waiter_skipped() {
        let store = TupleStore::transient(500);
        let r = store.take("s", Duration::from_millis(20)).await;
        assert!(matches!(r, Err(TupleError::Timeout)));
        // The dead waiter's sender is still queued; put must skip it and store.
        let id = store.put("s", b("x")).unwrap();
        let (got, _) = store.take("s", Duration::from_millis(100)).await.unwrap();
        assert_eq!(got, id);
    }

    #[tokio::test]
    async fn all_waiters_timed_out_item_in_queue() {
        let path = temp_wal("all-timeout");
        let store = TupleStore::persistent(&path, 10_000, 500).unwrap();
        for _ in 0..3 {
            let r = store.take("s", Duration::from_millis(10)).await;
            assert!(matches!(r, Err(TupleError::Timeout)));
        }
        let id = store.put("s", b("x")).unwrap();
        assert_eq!(store.depth(Some("s"))[0].depth, 1);
        let (got, _) = store.take("s", Duration::from_millis(50)).await.unwrap();
        assert_eq!(got, id);
        // Exactly ONE PutRecord despite three dead waiters being skipped.
        let puts = read_wal(&path)
            .iter()
            .filter(|r| matches!(r, Record::Put { .. }))
            .count();
        assert_eq!(puts, 1);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_100() {
        let store = Arc::new(TupleStore::transient(100_000));
        let mut handles = Vec::new();
        for p in 0..10u64 {
            let s = Arc::clone(&store);
            handles.push(tokio::spawn(async move {
                for i in 0..100u64 {
                    let payload = Bytes::from(format!("{p}:{i}"));
                    s.put("s", payload).unwrap();
                }
            }));
        }
        let mut consumers = Vec::new();
        for _ in 0..10 {
            let s = Arc::clone(&store);
            consumers.push(tokio::spawn(async move {
                let mut got = Vec::new();
                for _ in 0..100 {
                    let (id, _) = s.take("s", Duration::from_secs(10)).await.unwrap();
                    got.push(id);
                }
                got
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let mut all: Vec<u64> = Vec::new();
        for c in consumers {
            all.extend(c.await.unwrap());
        }
        // All 1 000 items delivered exactly once.
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), 1000);
    }

    #[tokio::test]
    async fn complete_atomic_no_crash_window() {
        let store = TupleStore::transient(500);
        let id = store.put("a", b("payload")).unwrap();
        let (got, payload) = store.take("a", Duration::from_millis(100)).await.unwrap();
        assert_eq!(got, id);
        let next_id = store.complete(id, "b", payload).unwrap();
        assert_ne!(next_id, id);
        // Old stage empty, new stage has the item, nothing in-flight.
        assert_eq!(store.depth(Some("a"))[0].depth, 0);
        assert_eq!(store.depth(Some("b"))[0].depth, 1);
        assert_eq!(store.inflight_count(), 0);
        // Double-complete / double-ack refused.
        assert!(matches!(store.ack(id), Err(TupleError::NotFound)));
    }

    /// Documents the duplicate hazard `complete` eliminates: with separate
    /// put + ack, a crash between the two replays BOTH the old item (taken,
    /// never acked) and the new one — the classic duplicate.
    #[tokio::test]
    async fn separate_put_ack_crash_window_duplicates() {
        let path = temp_wal("crash-window");
        {
            let store = TupleStore::persistent(&path, 10_000, 500).unwrap();
            let id = store.put("a", b("v")).unwrap();
            let (_, payload) = store.take("a", Duration::from_millis(100)).await.unwrap();
            // Non-atomic transition: put(next) succeeded…
            store.put("b", payload).unwrap();
            // …crash before ack(id). (Store dropped without ack.)
            let _ = id;
        }
        let store = TupleStore::persistent(&path, 10_000, 500).unwrap();
        // Old item re-queued AND new item present — 2 live items from 1.
        let total: u32 = store.depth(None).iter().map(|d| d.depth).sum();
        assert_eq!(total, 2);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn wal_records_take() {
        let path = temp_wal("records-take");
        let store = TupleStore::persistent(&path, 10_000, 500).unwrap();
        store.put("s", b("x")).unwrap();
        store.take("s", Duration::from_millis(100)).await.unwrap();
        let recs = read_wal(&path);
        assert!(matches!(recs[0], Record::Put { .. }));
        assert!(matches!(recs[1], Record::Take { .. }));
        // Compaction must NOT drop the unacked (in-flight) item.
        store.compact_now().unwrap();
        let recs = read_wal(&path);
        assert!(recs.iter().any(|r| matches!(r, Record::Put { .. })));
        assert!(recs.iter().any(|r| matches!(r, Record::Take { .. })));
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn wal_replay() {
        let path = temp_wal("replay");
        let mut ids = Vec::new();
        {
            let store = TupleStore::persistent(&path, 10_000, 500).unwrap();
            for i in 0..10 {
                ids.push(store.put("s", Bytes::from(format!("item-{i}"))).unwrap());
            }
        }
        let store = TupleStore::persistent(&path, 10_000, 500).unwrap();
        assert_eq!(store.depth(Some("s"))[0].depth, 10);
        // FIFO order and ids survive the restart; next_id is fenced past them.
        for expect in &ids {
            let (id, _) = store.take("s", Duration::from_millis(100)).await.unwrap();
            assert_eq!(id, *expect);
        }
        let fresh = store.put("s", b("new")).unwrap();
        assert!(fresh > *ids.last().unwrap());
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn wal_replay_inflight() {
        let path = temp_wal("replay-inflight");
        {
            let store = TupleStore::persistent(&path, 10_000, 500).unwrap();
            for i in 0..5 {
                store.put("s", Bytes::from(format!("{i}"))).unwrap();
            }
            for _ in 0..3 {
                store.take("s", Duration::from_millis(100)).await.unwrap();
            }
            // 3 in-flight (no ack), 2 queued — then crash.
        }
        let store = TupleStore::persistent(&path, 10_000, 500).unwrap();
        // 3 re-queued as abandoned + 2 remaining = 5.
        assert_eq!(store.depth(Some("s"))[0].depth, 5);
        let _ = std::fs::remove_file(&path);
    }

    // ── M13 / WS-G · G-G1b: keyed durability (WAL v2 + replay) ──────────────

    #[tokio::test]
    async fn keyed_item_survives_wal_replay_under_its_key() {
        let path = temp_wal("keyed-replay");
        {
            let store = TupleStore::persistent(&path, 10_000, 500).unwrap();
            store.put_keyed("po", Arc::from("inv-7"), b("matched")).unwrap();
        }
        // Reopen (the WAL is v2 with a keyed Put record) — the item re-queues under its key, so a
        // post-restart take_by_key still rendezvouses (and an unkeyed take does NOT claim it).
        let store = TupleStore::persistent(&path, 10_000, 500).unwrap();
        assert!(store.take("po", Duration::from_millis(50)).await.is_err(),
            "the replayed keyed item is not on the FIFO");
        let (_, payload) = store.take_by_key("po", "inv-7", Duration::from_millis(100)).await.unwrap();
        assert_eq!(payload.as_ref(), &b"matched"[..]);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn acked_keyed_item_does_not_resurrect() {
        let path = temp_wal("keyed-acked");
        {
            let store = TupleStore::persistent(&path, 10_000, 500).unwrap();
            let id = store.put_keyed("po", Arc::from("k"), b("v")).unwrap();
            let (got, _) = store.take_by_key("po", "k", Duration::from_millis(100)).await.unwrap();
            assert_eq!(got, id);
            store.ack(id).unwrap();
        }
        let store = TupleStore::persistent(&path, 10_000, 500).unwrap();
        assert!(store.take_by_key("po", "k", Duration::from_millis(50)).await.is_err(),
            "an acked keyed item must not resurrect on replay");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn v1_wal_replays_on_v2_build() {
        // A WAL stamped with the PREVIOUS format version, holding an (unkeyed) Put, must replay on
        // this v2 build — the rolling-upgrade guarantee. Unkeyed records are byte-identical across
        // versions, so only the header version differs.
        let path = temp_wal("v1-compat");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(WAL_MAGIC);
        bytes.extend_from_slice(&PREV_WAL_VERSION.to_le_bytes());
        Record::Put { id: 1, stage: Arc::from("s"), payload: b("legacy"), key: None }.encode(&mut bytes);
        std::fs::write(&path, &bytes).unwrap();

        let store = TupleStore::persistent(&path, 10_000, 500).unwrap();
        let (id, payload) = store.take("s", Duration::from_millis(100)).await.unwrap();
        assert_eq!((id, payload.as_ref()), (1, &b"legacy"[..]), "v1 WAL replays unchanged");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn wal_compact() {
        let path = temp_wal("compact");
        let store = TupleStore::persistent(&path, 10_000, 500).unwrap();
        let mut ids = Vec::new();
        for i in 0..10 {
            ids.push(store.put("a", Bytes::from(format!("{i}"))).unwrap());
        }
        // Ack 60% via take + complete(terminal ack pattern: take then ack).
        for _ in 0..6 {
            let (id, _) = store.take("a", Duration::from_millis(100)).await.unwrap();
            store.ack(id).unwrap();
        }
        assert!(store.wants_compaction());
        let before = store.wal_bytes();
        store.compact_now().unwrap();
        assert!(store.wal_bytes() < before);
        // Remaining 40% survive a replay of the compacted log.
        drop(store);
        let store = TupleStore::persistent(&path, 10_000, 500).unwrap();
        assert_eq!(store.depth(Some("a"))[0].depth, 4);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn backpressure_error() {
        let store = TupleStore::transient(3);
        for i in 0..3 {
            store.put("s", Bytes::from(format!("{i}"))).unwrap();
        }
        let r = store.put("s", b("overflow"));
        assert!(matches!(r, Err(TupleError::Backpressure { .. })));
        // Depth drops below the watermark → puts succeed again.
        store.take("s", Duration::from_millis(100)).await.unwrap();
        assert!(store.put("s", b("ok")).is_ok());
    }

    #[tokio::test]
    async fn requeue_expired_returns_item() {
        let store = TupleStore::transient(500);
        let id = store.put("s", b("x")).unwrap();
        store.take("s", Duration::from_millis(100)).await.unwrap();
        assert_eq!(store.inflight_count(), 1);
        // Zero timeout: everything in-flight is expired.
        let requeued = store.requeue_expired(Duration::ZERO);
        assert_eq!(requeued, vec![id]);
        assert_eq!(store.inflight_count(), 0);
        let (got, _) = store.take("s", Duration::from_millis(100)).await.unwrap();
        assert_eq!(got, id);
    }

    #[tokio::test]
    async fn wal_chunk_pagination_and_epoch_reset() {
        let path = temp_wal("chunk");
        let store = TupleStore::persistent(&path, 10_000, 500).unwrap();
        for i in 0..10 {
            store.put("s", Bytes::from(format!("item-{i}"))).unwrap();
        }
        // Page through with a 3-entry budget; records must round-trip.
        let (epoch, head) = store.wal_position();
        assert_eq!(epoch, 0);
        let mut offset = 0u64;
        let mut seen = 0usize;
        loop {
            let c = store
                .wal_read_chunk(epoch, offset, 3, 1 << 20)
                .unwrap()
                .unwrap();
            assert_eq!(c.epoch, epoch);
            seen += decode_records(&c.raw).len();
            offset = c.next_offset;
            if c.done {
                break;
            }
        }
        assert_eq!(seen, 10);
        assert_eq!(offset, head);
        // Stale epoch (e.g. after a compaction) → no data, restart cursor.
        for _ in 0..6 {
            let (id, _) = store.take("s", Duration::from_millis(50)).await.unwrap();
            store.ack(id).unwrap();
        }
        store.compact_now().unwrap();
        let c = store
            .wal_read_chunk(epoch, offset, 3, 1 << 20)
            .unwrap()
            .unwrap();
        assert_eq!(c.epoch, epoch + 1);
        assert!(c.raw.is_empty());
        assert_eq!(c.next_offset, 0);
        let _ = std::fs::remove_file(&path);
    }

    /// Run-17 improvement target (Evolvability): a WAL stamped with a FUTURE
    /// format version must be refused untouched — never silently truncated.
    #[tokio::test]
    async fn wal_future_version_refused_untouched() {
        let path = temp_wal("future-version");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(WAL_MAGIC);
        bytes.extend_from_slice(&99u16.to_le_bytes());
        bytes.extend_from_slice(b"opaque future-format payload");
        std::fs::write(&path, &bytes).unwrap();

        let err = match TupleStore::persistent(&path, 10_000, 500) {
            Ok(_) => panic!("future WAL version must refuse to open"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("v99"), "error names the version: {err}");
        assert_eq!(
            std::fs::read(&path).unwrap(),
            bytes,
            "refusal must leave the file byte-identical"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn wal_foreign_file_refused_untouched() {
        let path = temp_wal("foreign");
        std::fs::write(&path, b"definitely not a tuple-space wal").unwrap();
        let err = match TupleStore::persistent(&path, 10_000, 500) {
            Ok(_) => panic!("non-WAL file must refuse to open"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("magic"), "error names the cause: {err}");
        assert_eq!(
            std::fs::read(&path).unwrap().as_slice(),
            b"definitely not a tuple-space wal",
            "refusal must leave the file byte-identical"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// M2 Run-18 probe (Evolvability): a header torn mid-write — magic intact
    /// but version bytes missing — must also be refused untouched, not
    /// stamped over or truncated.
    #[tokio::test]
    async fn wal_torn_header_refused_untouched() {
        let path = temp_wal("torn-header");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(WAL_MAGIC);
        bytes.push(1); // first byte of the version, second missing
        std::fs::write(&path, &bytes).unwrap();
        assert!(
            TupleStore::persistent(&path, 10_000, 500).is_err(),
            "7-byte torn header must refuse to open"
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            bytes,
            "refusal must leave the file byte-identical"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn wal_header_written_and_survives_compaction() {
        let path = temp_wal("header");
        {
            let store = TupleStore::persistent(&path, 10_000, 500).unwrap();
            store.put("s", b("x")).unwrap();
            store.compact_now().unwrap();
        }
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(&bytes[..6], WAL_MAGIC);
        assert_eq!(u16::from_le_bytes(bytes[6..8].try_into().unwrap()), WAL_VERSION);
        // And the post-compaction file replays.
        let store = TupleStore::persistent(&path, 10_000, 500).unwrap();
        assert_eq!(store.depth(Some("s"))[0].depth, 1);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn put_with_id_fences_next_id() {
        let store = TupleStore::transient(500);
        store.put_with_id("s", 41, b("mirrored"), None).unwrap();
        let fresh = store.put("s", b("new")).unwrap();
        assert!(fresh > 41, "next_id not fenced past replicated id");
        assert!(store.remove_queued("s", 41));
        assert!(!store.remove_queued("s", 41), "double remove must be a no-op");
    }

    /// M2 Run-17 falsification probe (Semantic Correctness): a crash mid-append
    /// leaves a torn record at the WAL tail. Claimed invariant: replay drops
    /// the torn tail, recovers every complete record, and the log remains
    /// appendable. Probe truncates at EVERY byte offset inside the final
    /// record, not just one convenient point.
    #[tokio::test]
    async fn wal_torn_tail_recovery() {
        let path = temp_wal("torn-tail");
        {
            let store = TupleStore::persistent(&path, 10_000, 500).unwrap();
            for i in 0..5 {
                store.put("s", Bytes::from(format!("item-{i}"))).unwrap();
            }
            let (id, _) = store.take("s", Duration::from_millis(50)).await.unwrap();
            store.ack(id).unwrap();
        }
        let full = std::fs::read(&path).unwrap();
        // Find the start of the last record so we can tear inside it.
        let recs = read_wal(&path);
        assert!(recs.len() >= 7); // 5 puts + take + ack
        let mut offset = WAL_HEADER_LEN as usize;
        for _ in 0..recs.len() - 1 {
            let (_, n) = Record::decode(&full[offset..]).unwrap();
            offset += n;
        }
        for cut in offset + 1..full.len() {
            std::fs::write(&path, &full[..cut]).unwrap();
            let store = TupleStore::persistent(&path, 10_000, 500).unwrap();
            // Last record was the ack: torn ack ⇒ item re-queues (5 live);
            // the 6 preceding records (5 puts + take) must all survive,
            // with the taken-unacked item re-queued as abandoned ⇒ depth 5.
            assert_eq!(
                store.depth(Some("s"))[0].depth,
                5,
                "torn tail at byte {cut} corrupted recovery"
            );
            // Log must remain appendable and re-replayable after the tear.
            store.put("s", b("post-tear")).unwrap();
            drop(store);
            let store = TupleStore::persistent(&path, 10_000, 500).unwrap();
            assert_eq!(store.depth(Some("s"))[0].depth, 6);
        }
        let _ = std::fs::remove_file(&path);
    }

    /// M2 Run-17 falsification probe (Observability): the monitoring counters
    /// must stay exactly consistent through a chaotic lifecycle — parked-take
    /// timeouts, expiry re-queue, redelivery, mixed ack/complete. Claimed
    /// exact: depth, inflight, waiters return to 0 at quiescence;
    /// hot_total ≤ take_total; put_total counts puts+completes only (not
    /// re-queues).
    #[tokio::test]
    async fn metrics_accounting_identity() {
        let store = TupleStore::transient(500);
        // 3 waiters time out (waiters_count must come back down).
        for _ in 0..3 {
            let _ = store.take("s", Duration::from_millis(10)).await;
        }
        // 8 puts; take 4; ack 2, complete 2 onto stage "t".
        for i in 0..8 {
            store.put("s", Bytes::from(format!("{i}"))).unwrap();
        }
        let mut taken = Vec::new();
        for _ in 0..4 {
            taken.push(store.take("s", Duration::from_millis(100)).await.unwrap());
        }
        store.ack(taken[0].0).unwrap();
        store.ack(taken[1].0).unwrap();
        store.complete(taken[2].0, "t", b("x")).unwrap();
        store.complete(taken[3].0, "t", b("y")).unwrap();
        // Force-expire nothing (no inflight left on "s"); drain everything.
        assert_eq!(store.requeue_expired(Duration::from_secs(999)).len(), 0);
        for _ in 0..4 {
            let (id, _) = store.take("s", Duration::from_millis(100)).await.unwrap();
            store.ack(id).unwrap();
        }
        for _ in 0..2 {
            let (id, _) = store.take("t", Duration::from_millis(100)).await.unwrap();
            store.ack(id).unwrap();
        }
        // Quiescence: exact zeros everywhere.
        assert_eq!(store.inflight_count(), 0);
        for m in store.metrics_snapshot() {
            assert_eq!(m.depth, 0, "stage {} depth nonzero", m.stage);
            assert_eq!(m.inflight, 0, "stage {} inflight nonzero", m.stage);
            assert_eq!(m.waiters, 0, "stage {} waiters leaked", m.stage);
            assert!(m.hot_total <= m.take_total);
            match m.stage.as_ref() {
                "s" => {
                    assert_eq!(m.put_total, 8);
                    assert_eq!(m.take_total, 8);
                }
                "t" => {
                    assert_eq!(m.put_total, 2); // the two completes
                    assert_eq!(m.take_total, 2);
                }
                other => panic!("unexpected stage {other}"),
            }
        }
    }

    /// M2 Run-17 deep-dive evidence (Performance, #15): hot-path and
    /// store-path throughput smoke. Ignored by default — run explicitly:
    /// `cargo test -p mycelium-tuple-space --release wal_throughput_smoke -- --ignored --nocapture`
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "perf smoke; run explicitly with --ignored --nocapture"]
    async fn wal_throughput_smoke() {
        let n: u64 = 50_000;
        // Transient store-path: put then take, serially interleaved.
        let store = TupleStore::transient(u32::MAX);
        let t0 = std::time::Instant::now();
        for i in 0..n {
            store.put("s", Bytes::from(i.to_le_bytes().to_vec())).unwrap();
            store.take("s", Duration::from_millis(10)).await.unwrap();
        }
        let transient_pairs_s = n as f64 / t0.elapsed().as_secs_f64();
        // WAL-backed store-path (page-cache appends, background fsync model).
        let path = temp_wal("throughput");
        let store = TupleStore::persistent(&path, 10_000, u32::MAX).unwrap();
        let t0 = std::time::Instant::now();
        for i in 0..n {
            store.put("s", Bytes::from(i.to_le_bytes().to_vec())).unwrap();
            let (id, _) = store.take("s", Duration::from_millis(10)).await.unwrap();
            store.ack(id).unwrap();
        }
        let wal_pairs_s = n as f64 / t0.elapsed().as_secs_f64();
        println!("transient: {transient_pairs_s:.0} put/take pairs/s");
        println!("wal:       {wal_pairs_s:.0} put/take/ack cycles/s");
        // Very loose floors — this is a smoke alarm, not a benchmark.
        assert!(transient_pairs_s > 20_000.0);
        assert!(wal_pairs_s > 5_000.0);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn complete_unknown_id_refused() {
        let store = TupleStore::transient(500);
        assert!(matches!(
            store.complete(99, "b", b("x")),
            Err(TupleError::NotFound)
        ));
    }

    // ── Counter-invariant property test (Run-17 improvement target #2) ──────
    //
    // Two consecutive M2 runs found concurrency-accounting defects (Run 16:
    // listener registration race; Run 17: waiters_count double-decrement).
    // This is the structural response: a model-based test that replays
    // arbitrary operation sequences against a reference model and checks
    // every monitoring counter after EVERY step. The Run-17 underflow is
    // re-found by this test in seconds if the fix regresses (verified by
    // temporarily reverting the fix: fails at the first timeout+put pair).

    mod counter_model {
        use super::*;
        use proptest::prelude::*;
        use std::collections::HashMap as StdHashMap;

        #[derive(Debug, Clone)]
        pub(super) enum Op {
            Put(u8),
            TakeReady(u8),
            TakeEmptyTimeout(u8),
            AckOldest,
            CompleteOldest(u8),
            RequeueAll,
        }

        fn op_strategy() -> impl Strategy<Value = Op> {
            prop_oneof![
                3 => (0u8..3).prop_map(Op::Put),
                3 => (0u8..3).prop_map(Op::TakeReady),
                2 => (0u8..3).prop_map(Op::TakeEmptyTimeout),
                2 => Just(Op::AckOldest),
                2 => (0u8..3).prop_map(Op::CompleteOldest),
                1 => Just(Op::RequeueAll),
            ]
        }

        #[derive(Default)]
        struct Model {
            queues: StdHashMap<u8, VecDeque<u64>>,
            /// Claim order, oldest first.
            inflight: Vec<(u64, u8)>,
            /// Timed-out waiters whose dead senders are still queued.
            dead_waiters: StdHashMap<u8, u32>,
        }

        fn stage_name(s: u8) -> String {
            format!("st-{s}")
        }

        fn check(store: &TupleStore, model: &Model, step: usize) {
            let by_stage = store.inflight_by_stage();
            let model_inflight: usize = model.inflight.len();
            let store_inflight: u32 = by_stage.values().sum();
            assert_eq!(
                store_inflight as usize, model_inflight,
                "step {step}: inflight mismatch"
            );
            for m in store.metrics_snapshot() {
                let s: u8 = m.stage.trim_start_matches("st-").parse().unwrap();
                let want_depth =
                    model.queues.get(&s).map_or(0, |q| q.len()) as u32;
                let want_waiters =
                    model.dead_waiters.get(&s).copied().unwrap_or(0);
                assert_eq!(m.depth, want_depth, "step {step}: depth[{s}]");
                assert_eq!(m.waiters, want_waiters, "step {step}: waiters[{s}]");
                // Underflow canary: no counter may ever read as wrapped.
                assert!(m.depth < 1 << 30, "step {step}: depth wrapped");
                assert!(m.waiters < 1 << 30, "step {step}: waiters wrapped");
                assert!(m.hot_total <= m.take_total, "step {step}: hot > take");
            }
        }

        pub(super) async fn run(ops: Vec<Op>) {
            let store = TupleStore::transient(u32::MAX);
            let mut model = Model::default();
            for (step, op) in ops.iter().enumerate() {
                match *op {
                    Op::Put(s) => {
                        let id = store.put(&stage_name(s), b("p")).unwrap();
                        // A put pops (and skips) every dead waiter queued
                        // ahead of it on that stage.
                        model.dead_waiters.insert(s, 0);
                        model.queues.entry(s).or_default().push_back(id);
                    }
                    Op::TakeReady(s) => {
                        // Precondition: stage non-empty (else skip the op).
                        let Some(q) = model.queues.get_mut(&s) else { continue };
                        let Some(want) = q.pop_front() else { continue };
                        let (id, _) = store
                            .take(&stage_name(s), Duration::from_millis(100))
                            .await
                            .unwrap();
                        assert_eq!(id, want, "step {step}: FIFO violated");
                        model.inflight.push((id, s));
                    }
                    Op::TakeEmptyTimeout(s) => {
                        if model.queues.get(&s).is_some_and(|q| !q.is_empty()) {
                            continue; // only meaningful on an empty stage
                        }
                        let r = store
                            .take(&stage_name(s), Duration::from_millis(1))
                            .await;
                        assert!(matches!(r, Err(TupleError::Timeout)));
                        *model.dead_waiters.entry(s).or_default() += 1;
                        // Ensure the stage exists in metrics even if never put to.
                        model.queues.entry(s).or_default();
                    }
                    Op::AckOldest => {
                        if model.inflight.is_empty() {
                            continue;
                        }
                        let (id, _) = model.inflight.remove(0);
                        store.ack(id).unwrap();
                    }
                    Op::CompleteOldest(to) => {
                        if model.inflight.is_empty() {
                            continue;
                        }
                        let (id, _) = model.inflight.remove(0);
                        let new_id =
                            store.complete(id, &stage_name(to), b("c")).unwrap();
                        model.dead_waiters.insert(to, 0); // dispatch reaps them
                        model.queues.entry(to).or_default().push_back(new_id);
                    }
                    Op::RequeueAll => {
                        // Model stays exact only when re-queue order is
                        // unambiguous: restrict to ≤ 1 in-flight item.
                        if model.inflight.len() > 1 {
                            continue;
                        }
                        let requeued = store.requeue_expired(Duration::ZERO);
                        assert_eq!(requeued.len(), model.inflight.len());
                        if let Some((id, s)) = model.inflight.pop() {
                            assert_eq!(requeued, vec![id]);
                            // Re-queue dispatches: dead waiters on the target
                            // stage are popped and skipped, like any put.
                            model.dead_waiters.insert(s, 0);
                            model.queues.entry(s).or_default().push_back(id);
                        }
                    }
                }
                check(&store, &model, step);
            }
            // Drain to quiescence: every queued and in-flight item out, then
            // exact zeros everywhere.
            let stages: Vec<u8> = model.queues.keys().copied().collect();
            for s in stages {
                while model.queues.get(&s).is_some_and(|q| !q.is_empty()) {
                    let want = model.queues.get_mut(&s).unwrap().pop_front().unwrap();
                    let (id, _) = store
                        .take(&stage_name(s), Duration::from_millis(100))
                        .await
                        .unwrap();
                    assert_eq!(id, want);
                    store.ack(id).unwrap();
                }
            }
            for (id, _) in model.inflight.drain(..) {
                store.ack(id).unwrap();
            }
            assert_eq!(store.inflight_count(), 0);
            for m in store.metrics_snapshot() {
                assert_eq!(m.depth, 0, "quiescence: depth[{}]", m.stage);
                assert_eq!(m.inflight, 0, "quiescence: inflight[{}]", m.stage);
                // Dead waiters not yet reaped by a put are the one permitted
                // nonzero — but they must never read as wrapped.
                assert!(m.waiters < 1 << 30, "quiescence: waiters wrapped");
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig {
                cases: 64, ..ProptestConfig::default()
            })]
            #[test]
            fn counters_match_reference_model(
                ops in proptest::collection::vec(op_strategy(), 1..120)
            ) {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                    .unwrap();
                rt.block_on(run(ops));
            }
        }
    }

    /// Run-41 falsification probe (semantic correctness): `state_chunk` pagination must
    /// neither lose nor duplicate live items across chunks, must drop items acked
    /// mid-scan, and must report done on an exhausted cursor.
    #[tokio::test]
    async fn state_chunk_paginates_without_loss_or_duplication() {
        let store = TupleStore::transient(500);
        let mut ids = Vec::new();
        for i in 0..10u32 {
            ids.push(store.put("s", b(&format!("x{i}"))).unwrap());
        }
        // Paginate with max_entries=3: collect all ids across chunks.
        let mut got = Vec::new();
        let mut cursor = 0u64;
        for _ in 0..10 {
            let (raw, next, done) = store.state_chunk(cursor, 3, usize::MAX);
            let records = decode_records(&raw);
            for r in &records {
                if let Record::Put { id, .. } = r {
                    got.push(*id);
                }
            }
            cursor = next;
            if done {
                break;
            }
        }
        got.sort_unstable();
        assert_eq!(got, ids, "pagination lost or duplicated live items");

        // Items acked mid-scan drop out of later chunks (at-least-once, never resurrection
        // *within* one scan): take+ack two items, rescan from 0.
        let (a, _) = store.take("s", Duration::from_millis(200)).await.unwrap();
        let (b2, _) = store.take("s", Duration::from_millis(200)).await.unwrap();
        store.ack(a).unwrap();
        store.ack(b2).unwrap();
        let (raw, _, done) = store.state_chunk(0, usize::MAX, usize::MAX);
        let live: Vec<u64> = decode_records(&raw)
            .iter()
            .filter_map(|r| match r {
                Record::Put { id, .. } => Some(*id),
                _ => None,
            })
            .collect();
        assert!(done);
        assert_eq!(live.len(), 8, "acked items must drop out of the state chunk");
        assert!(!live.contains(&a) && !live.contains(&b2), "acked ids resurrected");

        // Exhausted cursor → empty + done.
        let (raw, _, done) = store.state_chunk(u64::MAX, 10, usize::MAX);
        assert!(done && decode_records(&raw).is_empty());
    }
}
