//! # mycelium-tuple-space
//!
//! Linda-style tuple space built entirely from Mycelium's **public** API: a
//! single-copy, WAL-backed pipeline buffer with blocking `take`, atomic
//! pipeline advance (`complete`), and emergent primary/secondary failover.
//!
//! ## Why pull, not push
//!
//! Push-based work distribution must *predict* which worker is ready from
//! state that is stale by at least one propagation delay — and prediction
//! error grows with population churn. A tuple space inverts this: workers
//! `take()` only when they are actually ready, so readiness is
//! self-announcing and the staleness question dissolves. The space removes
//! the central *decision-maker*; it is still a rendezvous point for the data
//! path, which is why it has its own failover story (capability-TTL election,
//! WAL replay — no coordinator for the election either).
//!
//! ## Stage lanes, not associative matching
//!
//! "Linda-style" refers to what is kept — generative decoupling (producers and
//! workers never know each other) and blocking pull — **not** to Linda's
//! retrieval model. Classic Linda matches typed tuples against templates
//! (`in(("stage-b", ?id, ?data))`) over one flat bag. This space deliberately
//! replaces associative matching with **named per-stage FIFO lanes**:
//!
//! - Payloads are opaque bytes — never inspected, matched, or templated.
//! - An item's pipeline position is *which lane it sits in*, not anything in
//!   its contents. [`TupleSpace::complete`] is an atomic lane-to-lane move.
//! - A worker's only "filter" is choosing which lane to `take()` from next.
//!
//! The trade: claims are O(1) FIFO pops instead of template scans; a blocking
//! `take` parks one waiter on one lane (no template re-evaluation on every
//! write); per-lane depth/waiters/inflight counters come for free and double
//! as the backpressure + fluid-worker pressure signal; and the WAL records a
//! stage transition as one indivisible `Complete` entry. What is given up is
//! content-addressable retrieval ("any tuple where priority=high") — recover
//! that idiomatically by encoding the dimension in the lane name
//! (`stage-b.high`, `stage-b.tenant-42`) and letting workers choose lanes.
//!
//! ## Pattern placement
//!
//! | Pattern | KV ring role | Payload routing |
//! |---|---|---|
//! | Pure KV / AFN | IS the buffer | Gossip to every node — O(N) per item |
//! | **TupleSpace** | Metadata + lifecycle only | RPC point-to-point — O(1) |
//!
//! ## Roles
//!
//! | Role | Behaviour |
//! |---|---|
//! | [`TupleRole::Primary`] | Serves the store immediately |
//! | [`TupleRole::Secondary`] | Mirrors the primary; promotes when its capability evaporates |
//! | [`TupleRole::Auto`] | Advertises as candidate, observes the ring, self-assigns primary or secondary — no coordinator |
//! | [`TupleRole::Client`] | Pure producer/worker; never serves |
//!
//! ## Quick start
//!
//! ```rust,no_run
//! use mycelium::{GossipAgent, GossipConfig, NodeId};
//! use mycelium_tuple_space::{TupleSpace, TupleConfig, TupleRole};
//! use bytes::Bytes;
//! use std::{sync::Arc, time::Duration};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let agent = Arc::new(GossipAgent::new(
//!     NodeId::new("127.0.0.1", 7100)?, GossipConfig::default()));
//! agent.start().await?;
//!
//! let cfg = TupleConfig {
//!     namespace: Arc::from("pipeline"),
//!     role: TupleRole::Primary,
//!     ..Default::default()
//! };
//! let ts = TupleSpace::new(Arc::clone(&agent), cfg).await?;
//!
//! // Producer
//! let id = ts.put("stage-a", Bytes::from_static(b"work")).await?;
//!
//! // Worker: park until work arrives, then advance atomically.
//! let (id, payload) = ts.take("stage-a", Duration::from_secs(30)).await?;
//! let next_id = ts.complete(id, "stage-b", payload).await?;
//! # Ok(()) }
//! ```

#![deny(unsafe_code)]
#![warn(clippy::clone_on_ref_ptr)]

mod rpc;
mod store;

#[cfg(feature = "gateway")]
mod http;

use bytes::Bytes;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use mycelium::{
    CapFilter, Capability, CapabilityReg, GossipAgent, NodeId, RpcError, SignalScope,
};

use store::{Record, TupleStore};

// ─── Config ──────────────────────────────────────────────────────────────────

/// Role this node plays for the tuple space namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TupleRole {
    /// Observe the capability ring and self-assign: advertise as candidate,
    /// settle, then become primary if none exists (lowest candidate id wins
    /// the tie) or secondary otherwise. No coordinator assigns the role.
    Auto,
    /// Serve the store immediately.
    Primary,
    /// Mirror the primary and promote when its advertisement evaporates.
    Secondary,
    /// Pure producer/worker; never serves the store.
    Client,
}

/// Producer-side behaviour when the primary is saturated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureMode {
    /// `put()` returns [`TupleError::Backpressure`] immediately.
    Raise,
    /// `put()` retries with exponential backoff up to the given deadline,
    /// then surfaces the backpressure error.
    Block(Duration),
}

#[derive(Debug, Clone)]
pub struct TupleConfig {
    /// Namespace, e.g. `"pipeline"`. Must not contain `/` (it becomes a
    /// capability name segment). Advertised as capability `tuple` /
    /// `{ns}.primary`; RPC kinds are `tuple.{ns}.put` etc.
    pub namespace: Arc<str>,
    pub role: TupleRole,
    /// WAL-backed (`true`) or transient (`false`).
    pub persist: bool,
    /// Ignored when `persist` is `false`.
    pub wal_path: PathBuf,
    /// Appends between `fdatasync` calls.
    pub checkpoint_every: u64,
    /// In-flight deadline: items taken but not acked within this window are
    /// re-queued (at-least-once delivery).
    pub worker_timeout_secs: u64,
    /// Stage depth at which `put` starts returning backpressure.
    pub high_watermark: u32,
    /// Items larger than this are not live-replicated to the secondary —
    /// the WAL replay at promotion is their only recovery path.
    pub mirror_payload_limit: usize,
    /// Cadence of the primary's replication-lag heartbeat Signal.
    pub heartbeat_interval: Duration,
    /// Max entries per `wal_replay` response.
    pub replay_chunk_size: usize,
    /// Max bytes per `wal_replay` response. The chunk is bounded by
    /// whichever of the two limits is hit first.
    pub replay_chunk_bytes: usize,
    pub backpressure_mode: BackpressureMode,
    /// Capability advertisement refresh. Readers evaporate entries at 3×
    /// this, so promotion latency after a primary crash is ≈3× this value.
    /// The default (10 s) suits LLM-timescale pipelines; tests shrink it.
    pub cap_refresh: Duration,
}

impl Default for TupleConfig {
    fn default() -> Self {
        Self {
            namespace: Arc::from("pipeline"),
            role: TupleRole::Auto,
            persist: false,
            wal_path: PathBuf::from("tuple.wal"),
            checkpoint_every: 500,
            worker_timeout_secs: 300,
            high_watermark: 500,
            mirror_payload_limit: 1024 * 1024,
            heartbeat_interval: Duration::from_secs(5),
            replay_chunk_size: 200,
            replay_chunk_bytes: 32 * 1024 * 1024,
            backpressure_mode: BackpressureMode::Raise,
            cap_refresh: Duration::from_secs(10),
        }
    }
}

// ─── Errors ──────────────────────────────────────────────────────────────────

#[derive(Debug)]
#[non_exhaustive]
pub enum TupleError {
    /// No node currently advertises `tuple/{ns}/primary` — either none was
    /// started, the primary's advertisement TTL lapsed, or it is opaque
    /// under backpressure.
    NoProvider,
    /// Primary is saturated; back off and retry.
    Backpressure { retry_after_ms: u64 },
    Timeout,
    /// Unknown item id (already acked, expired back to the queue, or never
    /// existed).
    NotFound,
    Io(std::io::Error),
    Rpc(String),
}

impl std::fmt::Display for TupleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TupleError::NoProvider => write!(f, "no tuple-space primary resolvable"),
            TupleError::Backpressure { retry_after_ms } => {
                write!(f, "primary saturated; retry after {retry_after_ms} ms")
            }
            TupleError::Timeout => write!(f, "timed out"),
            TupleError::NotFound => write!(f, "unknown item id"),
            TupleError::Io(e) => write!(f, "wal io error: {e}"),
            TupleError::Rpc(s) => write!(f, "rpc error: {s}"),
        }
    }
}

impl std::error::Error for TupleError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TupleError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for TupleError {
    fn from(e: std::io::Error) -> Self {
        TupleError::Io(e)
    }
}

/// Depth snapshot for one stage.
#[derive(Debug, Clone)]
pub struct TupleDepth {
    pub stage: Arc<str>,
    /// Items queued (not yet taken).
    pub depth: u32,
    /// Workers parked on `take` (may transiently include timed-out waiters
    /// not yet reaped by a dispatch).
    pub waiters: u32,
    /// Items taken but not yet acked.
    pub inflight: u32,
}

// ─── TupleSpace ──────────────────────────────────────────────────────────────

/// Companion handle to an `Arc<GossipAgent>`. Producer and worker API for one
/// tuple space namespace; on a serving node it also owns the store, RPC
/// handlers, and background tasks.
pub struct TupleSpace {
    agent: Arc<GossipAgent>,
    cfg: TupleConfig,
    /// Created when this node assumes a serving role (primary or mirror).
    store: parking_lot::Mutex<Option<Arc<TupleStore>>>,
    is_primary: AtomicBool,
    is_secondary: AtomicBool,
    /// Holding a registration keeps the capability advertised; dropping it
    /// tombstones the ad.
    primary_reg: parking_lot::Mutex<Option<CapabilityReg>>,
    /// Candidate / secondary advertisement.
    role_reg: parking_lot::Mutex<Option<CapabilityReg>>,
    /// `(epoch, wal_head)` last heard from the primary's heartbeat — where
    /// promotion replay starts.
    replay_cursor: parking_lot::Mutex<(u64, u64)>,
    /// Node that sent the last heartbeat — the replay target at promotion
    /// (the capability ad is gone by then, so the ring can't name it).
    last_heartbeat_sender: parking_lot::Mutex<Option<NodeId>>,
    /// Mirror bookkeeping: item id → stage, for dedupe and ack routing.
    mirror_stages: parking_lot::Mutex<HashMap<u64, Arc<str>>>,
    /// Guards against a second metrics writer after a role transition.
    metrics_running: AtomicBool,
    tasks: parking_lot::Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl TupleSpace {
    /// Constructs the tuple space and starts whatever background machinery
    /// the configured role needs. Call after `agent.start()`.
    pub async fn new(
        agent: Arc<GossipAgent>,
        cfg: TupleConfig,
    ) -> Result<Arc<Self>, TupleError> {
        let ts = Arc::new(Self {
            agent,
            cfg,
            store: parking_lot::Mutex::new(None),
            is_primary: AtomicBool::new(false),
            is_secondary: AtomicBool::new(false),
            primary_reg: parking_lot::Mutex::new(None),
            role_reg: parking_lot::Mutex::new(None),
            replay_cursor: parking_lot::Mutex::new((0, 0)),
            last_heartbeat_sender: parking_lot::Mutex::new(None),
            mirror_stages: parking_lot::Mutex::new(HashMap::new()),
            metrics_running: AtomicBool::new(false),
            tasks: parking_lot::Mutex::new(Vec::new()),
        });
        match ts.cfg.role {
            TupleRole::Primary => {
                ts.init_store()?;
                ts.become_primary();
            }
            TupleRole::Secondary => {
                ts.init_store()?;
                ts.become_secondary();
            }
            TupleRole::Auto => {
                let me = Arc::clone(&ts);
                let h = tokio::spawn(async move { me.run_election().await });
                ts.tasks.lock().push(h);
            }
            TupleRole::Client => {}
        }
        Ok(ts)
    }

    fn init_store(&self) -> Result<(), TupleError> {
        let mut g = self.store.lock();
        if g.is_none() {
            let store = if self.cfg.persist {
                TupleStore::persistent(
                    &self.cfg.wal_path,
                    self.cfg.checkpoint_every,
                    self.cfg.high_watermark,
                )?
            } else {
                TupleStore::transient(self.cfg.high_watermark)
            };
            *g = Some(Arc::new(store));
        }
        Ok(())
    }

    pub(crate) fn store(&self) -> Option<Arc<TupleStore>> {
        self.store.lock().clone()
    }

    pub(crate) fn cfg(&self) -> &TupleConfig {
        &self.cfg
    }

    /// This space's namespace.
    pub fn namespace(&self) -> &Arc<str> {
        &self.cfg.namespace
    }

    pub(crate) fn agent(&self) -> &Arc<GossipAgent> {
        &self.agent
    }

    fn store_expect(&self) -> Arc<TupleStore> {
        self.store().expect("serving role requires a store")
    }

    // ── Role assumption ──────────────────────────────────────────────────────

    /// Registers handlers, advertises `tuple`/`{ns}.primary`, and spawns the
    /// re-queue, checkpoint, and replication-heartbeat tasks.
    fn become_primary(self: &Arc<Self>) {
        let store = self.store_expect();
        let ns = &self.cfg.namespace;
        let mut tasks = rpc::spawn_primary_handlers(self);

        let reg = self.agent.capabilities().advertise_capability(
            Capability::new("tuple", format!("{ns}.primary")),
            self.cfg.cap_refresh,
        );
        *self.primary_reg.lock() = Some(reg);
        *self.role_reg.lock() = None; // retract candidate/secondary ad

        // Re-queue scan: items taken but not acked within worker_timeout are
        // returned to their stage (at-least-once delivery). Also evaporates
        // their advisory inflight keys.
        {
            let me = Arc::clone(self);
            let store2 = Arc::clone(&store);
            let timeout = Duration::from_secs(self.cfg.worker_timeout_secs);
            tasks.push(tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(30));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    for id in store2.requeue_expired(timeout) {
                        me.clear_inflight_key(id);
                        tracing::warn!(id, "tuple-space: re-queued expired in-flight item");
                    }
                    me.sweep_stale_inflight_keys(timeout);
                }
            }));
        }

        // Checkpoint + compaction: fsync on the configured cadence, compact
        // when more than half the log is acked. Both run off the hot path.
        {
            let store2 = Arc::clone(&store);
            tasks.push(tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_millis(200));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                let mut n: u32 = 0;
                loop {
                    tick.tick().await;
                    n = n.wrapping_add(1);
                    // Force a sync every ~1 s even below the ops threshold.
                    let force = n.is_multiple_of(5);
                    let s = Arc::clone(&store2);
                    let r = tokio::task::spawn_blocking(move || {
                        s.checkpoint_if_due(force)?;
                        if s.wants_compaction() {
                            s.compact_now()?;
                        }
                        std::io::Result::Ok(())
                    })
                    .await;
                    if let Ok(Err(e)) = r {
                        tracing::error!(error = %e, "tuple-space: wal maintenance failed");
                    }
                }
            }));
        }

        // Replication-lag heartbeat: Signal each secondary the current WAL
        // position. Its sole purpose is delivering `(epoch, wal_head)` —
        // failure detection is the capability TTL, not this signal.
        {
            let me = Arc::clone(self);
            let store2 = Arc::clone(&store);
            let interval = self.cfg.heartbeat_interval;
            tasks.push(tokio::spawn(async move {
                let mut tick = tokio::time::interval(interval);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                let kind = rpc::rpc_kind(&me.cfg.namespace, "heartbeat");
                loop {
                    tick.tick().await;
                    let (epoch, head) = store2.wal_position();
                    let mut payload = Vec::with_capacity(16);
                    payload.extend_from_slice(&epoch.to_le_bytes());
                    payload.extend_from_slice(&head.to_le_bytes());
                    for node in me.resolve_role("secondary") {
                        // Pin a direct route to each secondary: the primary sends it Individual-
                        // scoped traffic both ways — this heartbeat AND every RPC *reply* (take/
                        // put/complete responses). Without the pin those degrade to flood-relay and
                        // the secondary's client RPC round-trip times out (#150). Symmetric with the
                        // secondary pinning the primary in `resolve_primary`.
                        me.agent.connect_peer(node.clone());
                        let _ = me.agent.mesh().emit(
                            Arc::clone(&kind),
                            SignalScope::Individual(node),
                            Bytes::copy_from_slice(&payload),
                        );
                    }
                }
            }));
        }

        if let Some(t) = self.spawn_metrics_writer() {
            tasks.push(t);
        }

        self.tasks.lock().extend(tasks);
        self.is_secondary.store(false, Ordering::Release);
        self.is_primary.store(true, Ordering::Release);
        tracing::info!(ns = %self.cfg.namespace, "tuple-space: serving as primary");
    }

    /// Periodic monitoring writer: per-stage counters under
    /// `sys/tuple/{node}/{ns}/…` plus the backpressure pheromone.
    ///
    /// Two plan deviations, both deliberate:
    /// - Keys carry the namespace segment so two tuple spaces on one node
    ///   cannot collide.
    /// - The pressure signal is a `sys/tuple/...` key with an embedded
    ///   timestamp (read-side freshness, like every soft-state key here)
    ///   rather than a `sys/load/` entry: the load-state value encoding is
    ///   internal to the substrate, and evaporating the primary capability
    ///   under load would false-trigger the secondary's failure detector.
    fn spawn_metrics_writer(self: &Arc<Self>) -> Option<tokio::task::JoinHandle<()>> {
        // One writer per node regardless of how many role transitions happen
        // (a promoted secondary reuses the writer it already has).
        if self.metrics_running.swap(true, Ordering::AcqRel) {
            return None;
        }
        let me = Arc::clone(self);
        // Cadence: cap_refresh, capped at the plan's 10 s. Reusing the
        // advertisement interval keeps test configs single-knob.
        let interval = me.cfg.cap_refresh.min(Duration::from_secs(10));
        Some(tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let node = me.agent.node_id().to_string();
            let ns = Arc::clone(&me.cfg.namespace);
            let base = format!("sys/tuple/{node}/{ns}");
            let hw = me.cfg.high_watermark;
            let low = (hw as f64 * 0.7) as u32;
            loop {
                tick.tick().await;
                let Some(store) = me.store() else { continue };
                // Role is read each tick: a promoted secondary's writer
                // starts reporting (and pressure-marking) as primary.
                let role: &str = if me.is_primary() { "primary" } else { "secondary" };
                let kv = me.agent.kv();
                let _ = kv.set(format!("{base}/role"), Bytes::from(role.to_string()));
                let _ = kv.set(
                    format!("{base}/wal_bytes"),
                    Bytes::from(store.wal_bytes().to_string()),
                );
                for m in store.metrics_snapshot() {
                    let sb = format!("{base}/stage/{}", m.stage);
                    let _ = kv.set(format!("{sb}/depth"), Bytes::from(m.depth.to_string()));
                    let _ = kv.set(format!("{sb}/waiters"), Bytes::from(m.waiters.to_string()));
                    let _ = kv.set(format!("{sb}/inflight"), Bytes::from(m.inflight.to_string()));
                    let _ = kv.set(format!("{sb}/put_total"), Bytes::from(m.put_total.to_string()));
                    let _ = kv.set(format!("{sb}/take_total"), Bytes::from(m.take_total.to_string()));
                    let _ = kv.set(format!("{sb}/hot_total"), Bytes::from(m.hot_total.to_string()));
                    let _ = kv.set(
                        format!("{sb}/queue_p99_us"),
                        Bytes::from(m.queue_p99_us.to_string()),
                    );
                    // Backpressure pheromone with 0.7 hysteresis: written at
                    // the watermark, cleared below 70% of it, untouched in
                    // between to prevent oscillation. Primary-only.
                    if me.is_primary() {
                        let pkey = format!("{base}/pressure/{}", m.stage);
                        if m.depth >= hw {
                            let v = format!(
                                "{{\"depth\":{},\"written_at_ms\":{}}}",
                                m.depth,
                                store::now_ms()
                            );
                            let _ = kv.set(pkey, Bytes::from(v));
                        } else if m.depth < low {
                            let _ = kv.delete(pkey);
                        }
                    }
                }
            }
        }))
    }

    /// True when `node`'s pressure pheromone for `stage` is set and fresh
    /// (within 3× the metrics cadence — the standard evaporation window).
    fn pressure_fresh(&self, node: &NodeId, stage: &str) -> bool {
        let key = format!(
            "sys/tuple/{node}/{}/pressure/{stage}",
            self.cfg.namespace
        );
        let Some(value) = self.agent.kv().get(&key) else { return false };
        let written = std::str::from_utf8(&value)
            .ok()
            .and_then(|s| s.rsplit_once("\"written_at_ms\":"))
            .and_then(|(_, t)| t.trim_end_matches('}').parse::<u64>().ok());
        let window = self.cfg.cap_refresh.min(Duration::from_secs(10)) * 3;
        written.is_some_and(|w| {
            store::now_ms().saturating_sub(w) <= window.as_millis() as u64
        })
    }

    /// Advertises `{ns}.secondary`, mirrors replicated items, tracks the
    /// primary's WAL position, and promotes when the primary's capability
    /// evaporates.
    fn become_secondary(self: &Arc<Self>) {
        let ns = &self.cfg.namespace;
        let mut tasks = rpc::spawn_mirror_handlers(self);

        let reg = self.agent.capabilities().advertise_capability(
            Capability::new("tuple", format!("{ns}.secondary")),
            self.cfg.cap_refresh,
        );
        *self.role_reg.lock() = Some(reg);

        // Heartbeat receiver: record the primary's (epoch, wal_head) so
        // promotion knows where replay starts.
        {
            let me = Arc::clone(self);
            let mut rx = self
                .agent
                .mesh()
                .signal_rx(rpc::rpc_kind(&self.cfg.namespace, "heartbeat"));
            tasks.push(tokio::spawn(async move {
                while let Some(sig) = rx.recv().await {
                    let p = &sig.payload;
                    if p.len() >= 16 {
                        let epoch = u64::from_le_bytes(p[..8].try_into().unwrap());
                        let head = u64::from_le_bytes(p[8..16].try_into().unwrap());
                        *me.replay_cursor.lock() = (epoch, head);
                        *me.last_heartbeat_sender.lock() = Some(sig.sender.clone());
                    }
                }
            }));
        }

        // Promotion watch: the capability ring IS the failure detector. Two
        // consecutive empty resolves (one advertisement interval apart, per
        // the plan's split-brain guard) → catch up from the old primary's
        // WAL if reachable, fence next_id, and take over.
        {
            let me = Arc::clone(self);
            let interval = self.cfg.cap_refresh;
            tasks.push(tokio::spawn(async move {
                let mut tick = tokio::time::interval(interval);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    if !me.resolve_role("primary").is_empty() {
                        continue;
                    }
                    tokio::time::sleep(interval).await;
                    if !me.resolve_role("primary").is_empty() {
                        continue;
                    }
                    tracing::warn!(
                        ns = %me.cfg.namespace,
                        "tuple-space: primary evaporated — promoting"
                    );
                    me.replay_from_old_primary().await;
                    me.become_primary();
                    return;
                }
            }));
        }

        if let Some(t) = self.spawn_metrics_writer() {
            tasks.push(t);
        }

        self.tasks.lock().extend(tasks);
        self.is_secondary.store(true, Ordering::Release);
        tracing::info!(ns = %self.cfg.namespace, "tuple-space: mirroring as secondary");
    }

    /// `TupleRole::Auto`: advertise as candidate, let candidates settle,
    /// then self-assign. The plan's bare resolve-then-promote races when two
    /// candidates start together; the lowest candidate node id wins the tie
    /// deterministically — still no coordinator, every node reaches the same
    /// conclusion from its own view of the ring.
    async fn run_election(self: Arc<Self>) {
        let ns = &self.cfg.namespace;
        let reg = self.agent.capabilities().advertise_capability(
            Capability::new("tuple", format!("{ns}.candidate")),
            self.cfg.cap_refresh,
        );
        *self.role_reg.lock() = Some(reg);

        let settle = (self.cfg.cap_refresh * 2).max(Duration::from_secs(2));
        tokio::time::sleep(settle).await;

        loop {
            if !self.resolve_role("primary").is_empty() {
                if self.init_store().is_ok() {
                    self.become_secondary();
                } else {
                    tracing::error!("tuple-space: store init failed; staying client");
                }
                return;
            }
            let mut candidates = self.resolve_role("candidate");
            candidates.sort_by_key(NodeId::to_string);
            let self_id = self.agent.node_id().to_string();
            match candidates.first() {
                Some(lowest) if lowest.to_string() == self_id => {
                    if self.init_store().is_ok() {
                        self.become_primary();
                    } else {
                        tracing::error!("tuple-space: store init failed; staying client");
                    }
                    return;
                }
                // A lower candidate exists — wait for it to claim primary.
                _ => tokio::time::sleep(self.cfg.cap_refresh).await,
            }
        }
    }

    /// Best-effort catch-up from the (possibly dead) old primary's WAL,
    /// starting at the last heartbeat cursor. Unreachable primary → promote
    /// with what live replication already delivered (at-least-once boundary).
    async fn replay_from_old_primary(self: &Arc<Self>) {
        let old_primary = {
            // The ad evaporated, so resolve won't return it — but the replay
            // RPC may still connect if the process is alive (graceful
            // step-down, network blip). Last known provider is gone from the
            // ring; nothing to ask. Try any node still advertising candidate
            // state? No: the WAL lives on the old primary only. Use the last
            // heartbeat sender if we ever heard one.
            self.last_heartbeat_sender.lock().clone()
        };
        let Some(target) = old_primary else { return };
        let (mut epoch, mut offset) = *self.replay_cursor.lock();
        let kind = rpc::rpc_kind(&self.cfg.namespace, "wal_replay");
        for _ in 0..10_000 {
            let req = rpc::enc_wal_replay_req(
                epoch,
                offset,
                self.cfg.replay_chunk_size,
                self.cfg.replay_chunk_bytes,
            );
            let resp = self
                .agent
                .service()
                .rpc_call(target.clone(), Arc::clone(&kind), req, Duration::from_secs(10))
                .await;
            let Ok(resp) = resp else {
                tracing::warn!(
                    "tuple-space: old primary unreachable during replay; promoting with mirrored state"
                );
                return;
            };
            let Ok(chunk) = rpc::dec_wal_replay_resp(&resp) else { return };
            if chunk.epoch != epoch {
                // Compaction reset the offsets — restart from 0; the mirror
                // map dedupes re-applied puts.
                epoch = chunk.epoch;
                offset = 0;
                continue;
            }
            self.apply_records(&store::decode_records(&chunk.raw));
            offset = chunk.next_offset;
            if chunk.done {
                tracing::info!("tuple-space: wal replay drained before promotion");
                return;
            }
        }
    }

    /// Mirror application of replicated/replayed records. Identical for the
    /// live path and the promotion replay; the id → stage map dedupes.
    pub(crate) fn apply_records(&self, records: &[Record]) {
        let Some(store) = self.store() else { return };
        for rec in records {
            match rec {
                Record::Put { id, stage, payload, key } => {
                    let fresh = self
                        .mirror_stages
                        .lock()
                        .insert(*id, Arc::clone(stage))
                        .is_none();
                    if fresh && let Err(e) = store.put_with_id(stage, *id, payload.clone(), key.clone())
                    {
                        tracing::error!(id, error = %e, "tuple-space: mirror put failed");
                    }
                }
                Record::Ack { id } => {
                    if let Some(stage) = self.mirror_stages.lock().remove(id) {
                        store.remove_queued(&stage, *id);
                    }
                }
                Record::Complete { old_id, new_id, stage, payload, key } => {
                    if let Some(old_stage) = self.mirror_stages.lock().remove(old_id) {
                        store.remove_queued(&old_stage, *old_id);
                    }
                    let fresh = self
                        .mirror_stages
                        .lock()
                        .insert(*new_id, Arc::clone(stage))
                        .is_none();
                    if fresh
                        && let Err(e) = store.put_with_id(stage, *new_id, payload.clone(), key.clone())
                    {
                        tracing::error!(id = new_id, error = %e, "tuple-space: mirror complete failed");
                    }
                }
                // A mirror keeps taken items queued: on promotion they are
                // simply still available — the at-least-once contract.
                Record::Take { .. } => {}
            }
        }
    }

    // ── Serving paths (primary) — shared by RPC handlers and local calls ────

    pub(crate) fn serve_put(&self, stage: &str, payload: Bytes) -> Result<u64, TupleError> {
        let store = self.store_expect();
        let id = store.put(stage, payload.clone())?;
        self.replicate(Record::Put {
            id,
            stage: Arc::from(stage),
            payload,
            key: None,
        });
        Ok(id)
    }

    pub(crate) async fn serve_take(
        &self,
        stage: &str,
        timeout: Duration,
        worker: &NodeId,
    ) -> Result<(u64, Bytes), TupleError> {
        let store = self.store_expect();
        let (id, payload) = store.take(stage, timeout).await?;
        self.write_inflight_key(id, stage, worker);
        Ok((id, payload))
    }

    pub(crate) fn serve_complete(
        &self,
        id: u64,
        next_stage: &str,
        payload: Bytes,
    ) -> Result<u64, TupleError> {
        let store = self.store_expect();
        let new_id = store.complete(id, next_stage, payload.clone())?;
        self.clear_inflight_key(id);
        self.replicate(Record::Complete {
            old_id: id,
            new_id,
            stage: Arc::from(next_stage),
            payload,
            key: None,
        });
        Ok(new_id)
    }

    pub(crate) fn serve_ack(&self, id: u64) -> Result<(), TupleError> {
        let store = self.store_expect();
        store.ack(id)?;
        self.clear_inflight_key(id);
        self.replicate(Record::Ack { id });
        Ok(())
    }

    // ── M13 / WS-G keyed serve paths ─────────────────────────────────────────
    // The key is carried on the WAL record AND on replication (G1b) so a keyed in-flight item
    // re-queues under its key across a primary crash / promotion.
    pub(crate) fn serve_put_keyed(&self, stage: &str, key: Arc<str>, payload: Bytes) -> Result<u64, TupleError> {
        let store = self.store_expect();
        let id = store.put_keyed(stage, Arc::clone(&key), payload.clone())?;
        self.replicate(Record::Put { id, stage: Arc::from(stage), payload, key: Some(key) });
        Ok(id)
    }

    pub(crate) async fn serve_take_by_key(
        &self,
        stage: &str,
        key: &str,
        timeout: Duration,
        worker: &NodeId,
    ) -> Result<(u64, Bytes), TupleError> {
        let store = self.store_expect();
        let (id, payload) = store.take_by_key(stage, key, timeout).await?;
        self.write_inflight_key(id, stage, worker);
        Ok((id, payload))
    }

    pub(crate) fn serve_complete_keyed(
        &self,
        id: u64,
        next_stage: &str,
        key: Arc<str>,
        payload: Bytes,
    ) -> Result<u64, TupleError> {
        let store = self.store_expect();
        let new_id = store.complete_keyed(id, next_stage, Arc::clone(&key), payload.clone())?;
        self.clear_inflight_key(id);
        self.replicate(Record::Complete { old_id: id, new_id, stage: Arc::from(next_stage), payload, key: Some(key) });
        Ok(new_id)
    }

    /// Fire-and-forget replication to every live secondary. Not on the
    /// producer's critical path. Items above `mirror_payload_limit` are
    /// skipped — the WAL replay at promotion is their recovery path.
    fn replicate(&self, rec: Record) {
        if let Record::Put { payload, .. } | Record::Complete { payload, .. } = &rec
            && payload.len() > self.cfg.mirror_payload_limit
        {
            return;
        }
        let secondaries = self.resolve_role("secondary");
        if secondaries.is_empty() {
            return;
        }
        let body = rpc::enc_record(&rec);
        let kind = rpc::rpc_kind(&self.cfg.namespace, "replicate");
        let agent = Arc::clone(&self.agent);
        tokio::spawn(async move {
            for node in secondaries {
                let mut ok = false;
                for _ in 0..2 {
                    match agent
                        .service()
                        .rpc_call(
                            node.clone(),
                            Arc::clone(&kind),
                            body.clone(),
                            Duration::from_secs(5),
                        )
                        .await
                    {
                        Ok(_) => {
                            ok = true;
                            break;
                        }
                        Err(_) => continue,
                    }
                }
                if !ok {
                    tracing::warn!(%node, "tuple-space: replication unconfirmed");
                }
            }
        });
    }

    // ── Inflight visibility keys (advisory, cluster-wide) ───────────────────

    fn inflight_key(&self, id: u64) -> String {
        format!("tuple/inflight/{}/{id}", self.cfg.namespace)
    }

    fn write_inflight_key(&self, id: u64, stage: &str, worker: &NodeId) {
        let value = format!(
            "{{\"stage\":\"{stage}\",\"worker\":\"{worker}\",\"taken_at_ms\":{}}}",
            store::now_ms()
        );
        let _ = self.agent.kv().set(self.inflight_key(id), Bytes::from(value));
    }

    fn clear_inflight_key(&self, id: u64) {
        let _ = self.agent.kv().delete(self.inflight_key(id));
    }

    /// Deletes advisory inflight keys whose deadline passed — covers keys
    /// orphaned by a primary that died between re-queue and delete.
    fn sweep_stale_inflight_keys(&self, timeout: Duration) {
        let prefix = format!("tuple/inflight/{}/", self.cfg.namespace);
        let cutoff = store::now_ms().saturating_sub(timeout.as_millis() as u64);
        for (key, value) in self.agent.kv().scan_prefix(&prefix) {
            let taken = std::str::from_utf8(&value)
                .ok()
                .and_then(|s| s.rsplit_once("\"taken_at_ms\":"))
                .and_then(|(_, t)| t.trim_end_matches('}').parse::<u64>().ok());
            if taken.is_some_and(|t| t <= cutoff) {
                let _ = self.agent.kv().delete(key);
            }
        }
    }

    // ── Resolution ───────────────────────────────────────────────────────────

    fn resolve_role(&self, role: &str) -> Vec<NodeId> {
        let filter = CapFilter::new("tuple", format!("{}.{role}", self.cfg.namespace));
        self.agent
            .capabilities()
            .resolve(&filter)
            .into_iter()
            .map(|(node, _)| node)
            .collect()
    }

    /// Resolve the current primary from the capability ring.
    fn resolve_primary(&self) -> Result<NodeId, TupleError> {
        let mut providers = self.resolve_role("primary");
        if providers.is_empty() {
            return Err(TupleError::NoProvider);
        }
        // Deterministic pick if more than one claims primary (split-brain is
        // at-least-once-acceptable; the watch task converges it).
        providers.sort_by_key(NodeId::to_string);
        let primary = providers.remove(0);
        // Pin a direct forwarding route to the primary: client ops (put/take/complete) are
        // Individual-scoped RPCs, and an unpinned non-active peer would degrade them to
        // flood-relay latency → request-response timeouts (#150). Idempotent + cheap.
        self.agent.connect_peer(primary.clone());
        Ok(primary)
    }

    /// Resolve the primary, **waiting** (bounded) for its capability to appear.
    ///
    /// The `tuple/{ns}/primary` advertisement gossips at the `cap_refresh` cadence, so a client op
    /// issued before it has propagated to this node would otherwise fail immediately with
    /// `NoProvider` — a race the secondary loses right after startup/failover. `put` already
    /// tolerates this via `BackpressureMode::Block`; `take` and `complete` use this so they behave
    /// consistently instead of racing discovery (found via integration scenario S13, #150). Bounded
    /// at the capability evaporation window (3× refresh): beyond that a genuinely absent primary
    /// should surface `NoProvider`, not block forever.
    async fn resolve_primary_blocking(&self) -> Result<NodeId, TupleError> {
        let deadline = tokio::time::Instant::now() + self.cfg.cap_refresh * 3;
        let mut backoff = Duration::from_millis(100);
        loop {
            match self.resolve_primary() {
                Ok(p) => return Ok(p),
                Err(TupleError::NoProvider)
                    if tokio::time::Instant::now() + backoff < deadline =>
                {
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(1));
                }
                other => return other,
            }
        }
    }

    fn serving_locally(&self) -> Option<Arc<TupleStore>> {
        if self.is_primary.load(Ordering::Acquire) {
            self.store()
        } else {
            None
        }
    }

    // ── Producer API ─────────────────────────────────────────────────────────

    /// Write an item to `stage`. Returns the item id.
    ///
    /// With [`BackpressureMode::Raise`] a saturated primary surfaces
    /// [`TupleError::Backpressure`] immediately; with `Block` the call
    /// retries with exponential backoff until its deadline.
    pub async fn put(&self, stage: &str, payload: Bytes) -> Result<u64, TupleError> {
        match self.cfg.backpressure_mode {
            BackpressureMode::Raise => self.put_once(stage, &payload).await,
            BackpressureMode::Block(limit) => {
                let deadline = tokio::time::Instant::now() + limit;
                let mut backoff = Duration::from_millis(100);
                loop {
                    match self.put_once(stage, &payload).await {
                        Err(TupleError::Backpressure { .. } | TupleError::NoProvider)
                            if tokio::time::Instant::now() + backoff < deadline =>
                        {
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(Duration::from_secs(5));
                        }
                        other => return other,
                    }
                }
            }
        }
    }

    async fn put_once(&self, stage: &str, payload: &Bytes) -> Result<u64, TupleError> {
        if self.serving_locally().is_some() {
            return self.serve_put(stage, payload.clone());
        }
        let primary = self.resolve_primary()?;
        // The pressure pheromone lets producers back off without spending an
        // RPC on a saturated primary; the store's watermark check remains
        // the authoritative gate behind it.
        if self.pressure_fresh(&primary, stage) {
            return Err(TupleError::Backpressure { retry_after_ms: 500 });
        }
        let resp = self
            .agent
            .service()
            .rpc_call(
                primary,
                rpc::rpc_kind(&self.cfg.namespace, "put"),
                rpc::enc_put_req(stage, payload),
                Duration::from_secs(10),
            )
            .await
            .map_err(rpc_err)?;
        rpc::dec_id_resp(&resp)
    }

    // ── Worker API ───────────────────────────────────────────────────────────

    /// Blocking claim: parks until an item is available on `stage` or
    /// `timeout` elapses.
    pub async fn take(
        &self,
        stage: &str,
        timeout: Duration,
    ) -> Result<(u64, Bytes), TupleError> {
        if self.serving_locally().is_some() {
            let me = self.agent.node_id().clone();
            return self.serve_take(stage, timeout, &me).await;
        }
        let primary = self.resolve_primary_blocking().await?;
        let resp = self
            .agent
            .service()
            .rpc_call(
                primary,
                rpc::rpc_kind(&self.cfg.namespace, "take"),
                rpc::enc_take_req(stage, timeout),
                // The handler parks up to `timeout`; pad the RPC deadline so
                // the park, not the transport, decides.
                timeout + Duration::from_secs(5),
            )
            .await
            .map_err(rpc_err)?;
        rpc::dec_take_resp(&resp)
    }

    /// Atomic pipeline advance: acks `id` AND puts `payload` on `next_stage`
    /// in one WAL record — no crash window between stages. Preferred over
    /// separate `put` + `ack` for every mid-pipeline transition.
    pub async fn complete(
        &self,
        id: u64,
        next_stage: &str,
        payload: Bytes,
    ) -> Result<u64, TupleError> {
        if self.serving_locally().is_some() {
            return self.serve_complete(id, next_stage, payload);
        }
        let primary = self.resolve_primary_blocking().await?;
        let resp = self
            .agent
            .service()
            .rpc_call(
                primary,
                rpc::rpc_kind(&self.cfg.namespace, "complete"),
                rpc::enc_complete_req(id, next_stage, &payload),
                Duration::from_secs(10),
            )
            .await
            .map_err(rpc_err)?;
        rpc::dec_id_resp(&resp)
    }

    // ── M13 / WS-G: keyed-exact-match rendezvous (fan-in joins) ──────────────

    /// Put `payload` on `stage` under correlation `key` (WS-G / M13). Claimed only by a
    /// [`take_by_key`](Self::take_by_key) with the same key — the two-stream rendezvous ("an invoice
    /// AND its matching purchase order") that exact lane names can't express without one lane per key.
    /// Exact-match only (O(1) hash), never template matching.
    pub async fn put_keyed(&self, stage: &str, key: &str, payload: Bytes) -> Result<u64, TupleError> {
        if self.serving_locally().is_some() {
            return self.serve_put_keyed(stage, Arc::from(key), payload);
        }
        let primary = self.resolve_primary()?;
        if self.pressure_fresh(&primary, stage) {
            return Err(TupleError::Backpressure { retry_after_ms: 500 });
        }
        let resp = self
            .agent
            .service()
            .rpc_call(
                primary,
                rpc::rpc_kind(&self.cfg.namespace, "put_keyed"),
                rpc::enc_put_keyed_req(stage, key, &payload),
                Duration::from_secs(10),
            )
            .await
            .map_err(rpc_err)?;
        rpc::dec_id_resp(&resp)
    }

    /// Blocking keyed claim (WS-G / M13): claims the item on `stage` whose correlation key is `key`,
    /// or parks until one arrives or `timeout` elapses.
    pub async fn take_by_key(
        &self,
        stage: &str,
        key: &str,
        timeout: Duration,
    ) -> Result<(u64, Bytes), TupleError> {
        if self.serving_locally().is_some() {
            let me = self.agent.node_id().clone();
            return self.serve_take_by_key(stage, key, timeout, &me).await;
        }
        let primary = self.resolve_primary()?;
        let resp = self
            .agent
            .service()
            .rpc_call(
                primary,
                rpc::rpc_kind(&self.cfg.namespace, "take_by_key"),
                rpc::enc_take_by_key_req(stage, key, timeout),
                timeout + Duration::from_secs(5),
            )
            .await
            .map_err(rpc_err)?;
        rpc::dec_take_resp(&resp)
    }

    /// Atomic keyed pipeline advance (WS-G / M13): acks `id` AND puts `payload` on `next_stage` under
    /// correlation `key` in one record — the keyed analogue of [`complete`](Self::complete).
    pub async fn complete_keyed(
        &self,
        id: u64,
        next_stage: &str,
        key: &str,
        payload: Bytes,
    ) -> Result<u64, TupleError> {
        if self.serving_locally().is_some() {
            return self.serve_complete_keyed(id, next_stage, Arc::from(key), payload);
        }
        let primary = self.resolve_primary()?;
        let resp = self
            .agent
            .service()
            .rpc_call(
                primary,
                rpc::rpc_kind(&self.cfg.namespace, "complete_keyed"),
                rpc::enc_complete_keyed_req(id, next_stage, key, &payload),
                Duration::from_secs(10),
            )
            .await
            .map_err(rpc_err)?;
        rpc::dec_id_resp(&resp)
    }

    /// Terminal ack: last stage of a pipeline or explicit abandonment.
    pub async fn ack(&self, id: u64) -> Result<(), TupleError> {
        if self.serving_locally().is_some() {
            return self.serve_ack(id);
        }
        let primary = self.resolve_primary()?;
        let resp = self
            .agent
            .service()
            .rpc_call(
                primary,
                rpc::rpc_kind(&self.cfg.namespace, "ack"),
                rpc::enc_ack_req(id),
                Duration::from_secs(10),
            )
            .await
            .map_err(rpc_err)?;
        rpc::dec_unit_resp(&resp)
    }

    // ── Inspection ───────────────────────────────────────────────────────────

    /// Depth snapshot for one stage (`Some`) or all stages (`None`), served
    /// by the current primary.
    pub async fn depth(
        &self,
        stage: Option<&str>,
    ) -> Result<Vec<TupleDepth>, TupleError> {
        if let Some(store) = self.serving_locally() {
            let resp = Bytes::from(rpc::enc_depth_resp_from_store(&store, stage));
            return rpc::dec_depth_resp(&resp);
        }
        let primary = self.resolve_primary()?;
        let resp = self
            .agent
            .service()
            .rpc_call(
                primary,
                rpc::rpc_kind(&self.cfg.namespace, "depth"),
                rpc::enc_depth_req(stage),
                Duration::from_secs(10),
            )
            .await
            .map_err(rpc_err)?;
        rpc::dec_depth_resp(&resp)
    }

    /// Depth of THIS node's local store (mirror or primary), bypassing RPC.
    /// `None` when this node holds no store. Intended for monitoring and
    /// tests; application code should use [`depth`](Self::depth).
    pub fn local_depth(&self, stage: Option<&str>) -> Option<Vec<TupleDepth>> {
        let store = self.store()?;
        let resp = Bytes::from(rpc::enc_depth_resp_from_store(&store, stage));
        rpc::dec_depth_resp(&resp).ok()
    }

    /// True when this node is currently serving the store.
    pub fn is_primary(&self) -> bool {
        self.is_primary.load(Ordering::Acquire)
    }

    /// True when this node is mirroring the primary.
    pub fn is_secondary(&self) -> bool {
        self.is_secondary.load(Ordering::Acquire)
    }

    /// Stops background tasks and retracts every advertisement. The WAL is
    /// fsynced before tasks stop.
    pub async fn shutdown(&self) {
        *self.primary_reg.lock() = None; // tombstone the capability ads
        *self.role_reg.lock() = None;
        let tasks: Vec<_> = std::mem::take(&mut *self.tasks.lock());
        for t in &tasks {
            t.abort();
        }
        if let Some(store) = self.store() {
            let _ = tokio::task::spawn_blocking(move || store.checkpoint_if_due(true)).await;
        }
    }
}

fn rpc_err(e: RpcError) -> TupleError {
    match e {
        RpcError::Timeout => TupleError::Timeout,
        other => TupleError::Rpc(other.to_string()),
    }
}
