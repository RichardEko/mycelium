//! # mycelium-blackboard — shared working memory on Mycelium's public API
//!
//! Blackboard-style **opportunistic multi-agent reasoning over typed facts**, rebuilt on Mycelium
//! the same way [`mycelium-tuple-space`](https://docs.rs/mycelium-tuple-space) rebuilt work
//! distribution. The design rationale + worked example (a community microgrid) live in
//! `docs/plans/mycelium-blackboard.md`; the phased build plan in
//! `docs/plans/v2-wsg-g3-blackboard.md`.
//!
//! ## The reading / consuming split (Linda's `rd` vs `in`)
//!
//! A blackboard surfaces one clean distinction:
//!
//! - **Reading facts is unconditional and concurrent** (`rd`). Many agents observe the same fact —
//!   a forecaster and a pricing agent both react to a surplus-energy fact. Mycelium's substrate
//!   already does this perfectly via gossiped KV + boundary predicates; nothing new is needed.
//! - **Consuming facts is competitive and exactly-once** (`in`). Acting on a *finite* fact (the
//!   surplus exists once) means two agents whose triggers both match must **race for an atomic
//!   claim** — exactly one consumes it, the loser's claim returns empty, and a winner that drops
//!   mid-work has the claim re-queued.
//!
//! This crate adds the **one** missing primitive: **competitive destructive claim-by-predicate**.
//! Everything else (fact propagation, trigger predicates, evaporation) is the substrate's `rd`.
//!
//! ## Why not the tuple space
//!
//! The tuple space routes by *position* (named FIFO lanes, topology known per stage). The blackboard
//! routes by *content*: a consumer's criterion is a **predicate over fact attributes**, and the
//! topology is *emergent per item* — a surplus fact routes through entirely different agents than a
//! deficit fact. A lane per (fact-type × interest) explodes against each agent's private, changing
//! declarations. The predicate language is the **capability attribute-filter grammar** (equality +
//! presence), *not* unification — already implemented, already understood, and enough for trigger
//! conditions.
//!
//! ## Status (WS-G / G3)
//!
//! - **[`BoardStore`]** — the pure claim-by-predicate core (`post` / `read` / `claim` / `ack` /
//!   `release`), single-owner and exactly-once, testable without a cluster, with WAL durability.
//! - **[`Blackboard`]** — the agent-backed board: posting/reading/claiming over a coordinator-free
//!   primary discovered on the capability ring, with emergent secondary failover (`Post`/`Ack`
//!   replication + snapshot sync + promotion-on-evaporation).
//!
//! Remaining phases add the HTTP gateway + SDKs and the worked example.

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use mycelium::{CapFilter, Capability, CapabilityReg, GossipAgent, NodeId};

#[cfg(feature = "gateway")]
mod http;
mod rpc;
mod store;
mod wal;
pub use store::{BoardDepth, BoardStats, BoardStore};

/// A typed fact on the board: an attribute map (the matchable surface) plus an opaque payload.
/// Facts are non-destructively *read* by many and destructively *claimed* by at most one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fact {
    /// Board-assigned id; also the claim handle once the fact is claimed.
    pub id: u64,
    /// The matchable surface — string attributes a [`Predicate`] tests (the content-plane analogue
    /// of a capability's attributes). Routing is encoded here, never in the payload.
    pub attributes: BTreeMap<String, String>,
    /// Opaque payload (the substrate never matches on it).
    pub payload: bytes::Bytes,
}

/// One attribute constraint in a [`Predicate`]. Deliberately the capability attribute-filter
/// grammar — equality + presence — **not** unification/structural matching (scope creep until
/// demonstrated; see the crate docs' non-goals).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttrMatch {
    /// The attribute must be present and equal to this value.
    Equals(String),
    /// The attribute must be present (any value).
    Present,
}

/// A conjunctive predicate over fact attributes: **all** constraints must hold for a fact to match.
/// An empty predicate matches every fact.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Predicate {
    attrs: BTreeMap<String, AttrMatch>,
}

impl Predicate {
    /// A predicate matching every fact. Add constraints with [`eq`](Self::eq) / [`present`](Self::present).
    pub fn new() -> Self {
        Self { attrs: BTreeMap::new() }
    }

    /// Require attribute `key` to equal `value`.
    pub fn eq(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attrs.insert(key.into(), AttrMatch::Equals(value.into()));
        self
    }

    /// Require attribute `key` to be present (any value).
    pub fn present(mut self, key: impl Into<String>) -> Self {
        self.attrs.insert(key.into(), AttrMatch::Present);
        self
    }

    /// True iff every constraint holds against `attributes`.
    pub fn matches(&self, attributes: &BTreeMap<String, String>) -> bool {
        self.attrs.iter().all(|(k, m)| match m {
            AttrMatch::Present => attributes.contains_key(k),
            AttrMatch::Equals(v) => attributes.get(k).is_some_and(|got| got == v),
        })
    }

    /// Number of constraints (0 = match-all).
    pub fn len(&self) -> usize {
        self.attrs.len()
    }

    /// Whether this is the match-all predicate.
    pub fn is_empty(&self) -> bool {
        self.attrs.is_empty()
    }
}

/// Errors from the board API.
#[derive(Debug)]
#[non_exhaustive]
pub enum BlackboardError {
    /// Unknown claim id — already acked, already released, re-queued by the in-flight deadline, or
    /// never claimed.
    NotFound,
    /// No node currently serves this board (later phases — role resolution).
    NoProvider,
    /// Transport error talking to the board primary (later phases).
    Rpc(String),
    /// WAL I/O error (persistent boards).
    Io(std::io::Error),
}

impl std::fmt::Display for BlackboardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlackboardError::NotFound => write!(f, "unknown claim id"),
            BlackboardError::NoProvider => write!(f, "no blackboard primary resolvable"),
            BlackboardError::Rpc(s) => write!(f, "rpc error: {s}"),
            BlackboardError::Io(e) => write!(f, "wal io error: {e}"),
        }
    }
}

impl std::error::Error for BlackboardError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BlackboardError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for BlackboardError {
    fn from(e: std::io::Error) -> Self {
        BlackboardError::Io(e)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Agent-backed board (WS-G / G3 · Phase 3) — emergent roles + failover.
//
// Mirrors the mycelium-tuple-space role pattern: the primary is discovered by capability
// advertisement (`blackboard/{ns}.primary`); a secondary mirrors via Post/Ack replication + an
// initial snapshot and promotes when the primary's capability evaporates (the ring IS the failure
// detector); `Auto` self-elects with a lowest-candidate-id tie-break. No coordinator assigns roles.
// ═══════════════════════════════════════════════════════════════════════════════

/// Role this node plays for a board namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoardRole {
    /// Advertise as candidate, settle, then become primary (lowest candidate id wins) or secondary.
    Auto,
    /// Serve the board immediately.
    Primary,
    /// Mirror the primary and promote when its advertisement evaporates.
    Secondary,
    /// Pure poster/claimer; never serves.
    Client,
}

/// Board configuration.
#[derive(Debug, Clone)]
pub struct BoardConfig {
    /// Namespace, e.g. `"microgrid"`. Must not contain `/`. Advertised as capability
    /// `blackboard`/`{ns}.primary`; RPC kinds are `blackboard.{ns}.claim` etc.
    pub namespace: Arc<str>,
    pub role: BoardRole,
    /// WAL-backed (`true`) or transient (`false`).
    pub persist: bool,
    /// Ignored when `persist` is `false`.
    pub wal_path: PathBuf,
    /// Appends between `fdatasync` calls.
    pub checkpoint_every: u64,
    /// In-flight claim deadline: a claim not acked within this window re-queues the fact
    /// (at-least-once — the "claimer dropped mid-work" path).
    pub claim_timeout_secs: u64,
    /// Capability advertisement refresh. Readers evaporate ads at 3×, so promotion latency after a
    /// primary crash is ≈3× this value.
    pub cap_refresh: Duration,
}

impl Default for BoardConfig {
    fn default() -> Self {
        Self {
            namespace: Arc::from("board"),
            role: BoardRole::Auto,
            persist: false,
            wal_path: PathBuf::from("board.wal"),
            checkpoint_every: 500,
            claim_timeout_secs: 300,
            cap_refresh: Duration::from_secs(10),
        }
    }
}

/// An agent-backed board: posting/reading/claiming over a coordinator-free primary discovered on the
/// capability ring, with emergent secondary failover. Construct after `agent.start()`.
pub struct Blackboard {
    agent: Arc<GossipAgent>,
    cfg: BoardConfig,
    store: parking_lot::Mutex<Option<Arc<BoardStore>>>,
    is_primary: AtomicBool,
    is_secondary: AtomicBool,
    primary_reg: parking_lot::Mutex<Option<CapabilityReg>>,
    role_reg: parking_lot::Mutex<Option<CapabilityReg>>,
    /// Mirror dedup: fact ids already applied (a replicated Post may arrive twice).
    mirrored: parking_lot::Mutex<HashSet<u64>>,
    tasks: parking_lot::Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl Blackboard {
    /// Construct the board and start whatever machinery the configured role needs.
    pub async fn new(agent: Arc<GossipAgent>, cfg: BoardConfig) -> Result<Arc<Self>, BlackboardError> {
        let bb = Arc::new(Self {
            agent,
            cfg,
            store: parking_lot::Mutex::new(None),
            is_primary: AtomicBool::new(false),
            is_secondary: AtomicBool::new(false),
            primary_reg: parking_lot::Mutex::new(None),
            role_reg: parking_lot::Mutex::new(None),
            mirrored: parking_lot::Mutex::new(HashSet::new()),
            tasks: parking_lot::Mutex::new(Vec::new()),
        });
        match bb.cfg.role {
            BoardRole::Primary => {
                bb.init_store()?;
                bb.become_primary();
            }
            BoardRole::Secondary => {
                bb.init_store()?;
                bb.become_secondary();
            }
            BoardRole::Auto => {
                let me = Arc::clone(&bb);
                let h = tokio::spawn(async move { me.run_election().await });
                bb.tasks.lock().push(h);
            }
            BoardRole::Client => {}
        }
        Ok(bb)
    }

    fn init_store(&self) -> Result<(), BlackboardError> {
        let mut g = self.store.lock();
        if g.is_none() {
            let store = if self.cfg.persist {
                BoardStore::persistent(&self.cfg.wal_path, self.cfg.checkpoint_every)?
            } else {
                BoardStore::transient()
            };
            *g = Some(Arc::new(store));
        }
        Ok(())
    }

    pub(crate) fn store(&self) -> Option<Arc<BoardStore>> {
        self.store.lock().clone()
    }
    pub(crate) fn cfg(&self) -> &BoardConfig {
        &self.cfg
    }
    pub(crate) fn agent(&self) -> &Arc<GossipAgent> {
        &self.agent
    }
    /// This board's namespace.
    pub fn namespace(&self) -> &Arc<str> {
        &self.cfg.namespace
    }
    fn store_expect(&self) -> Arc<BoardStore> {
        self.store().expect("serving role requires a store")
    }
    pub(crate) fn mark_mirrored(&self, id: u64) -> bool {
        self.mirrored.lock().insert(id)
    }

    // ── Role assumption ──────────────────────────────────────────────────────

    fn become_primary(self: &Arc<Self>) {
        let store = self.store_expect();
        let ns = &self.cfg.namespace;
        let mut tasks = rpc::spawn_primary_handlers(self);

        let reg = self.agent.capabilities().advertise_capability(
            Capability::new("blackboard", format!("{ns}.primary")),
            self.cfg.cap_refresh,
        );
        *self.primary_reg.lock() = Some(reg);
        *self.role_reg.lock() = None; // retract candidate/secondary ad

        // Re-queue scan: claims not acked within the deadline return to claimable (at-least-once).
        {
            let store2 = Arc::clone(&store);
            let timeout = Duration::from_secs(self.cfg.claim_timeout_secs);
            tasks.push(tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(30));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    for id in store2.requeue_expired(timeout) {
                        tracing::warn!(id, "blackboard: re-queued expired in-flight claim");
                    }
                }
            }));
        }

        // Checkpoint + compaction, off the hot path.
        {
            let store2 = Arc::clone(&store);
            tasks.push(tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(1));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    let s = Arc::clone(&store2);
                    let _ = tokio::task::spawn_blocking(move || {
                        let _ = s.sync();
                        if s.wants_compaction() {
                            let _ = s.compact();
                        }
                    })
                    .await;
                }
            }));
        }

        self.tasks.lock().extend(tasks);
        self.is_secondary.store(false, Ordering::Release);
        self.is_primary.store(true, Ordering::Release);
        tracing::info!(ns = %self.cfg.namespace, "blackboard: serving as primary");
    }

    fn become_secondary(self: &Arc<Self>) {
        let ns = &self.cfg.namespace;
        let mut tasks = rpc::spawn_mirror_handlers(self);

        let reg = self.agent.capabilities().advertise_capability(
            Capability::new("blackboard", format!("{ns}.secondary")),
            self.cfg.cap_refresh,
        );
        *self.role_reg.lock() = Some(reg);

        // Initial sync: pull the primary's current live facts so the mirror is COMPLETE before this
        // secondary could ever be promoted. Live Post/Ack replication only ships facts posted *while
        // this secondary is present*, so a single-shot sync that hit a transient-unresolvable primary
        // (fresh ring / just-elected primary) left a PARTIAL mirror — and a later failover silently
        // lost the pre-join backlog while the operator believed redundancy was restored. Retry each
        // interval until it drains once, or until this node promotes. Ported from the tuple-space
        // join-time backfill after audit 2026-07-15 pass 3 found the blackboard lacked the retry.
        {
            let me = Arc::clone(self);
            let interval = self.cfg.cap_refresh;
            tasks.push(tokio::spawn(async move {
                let mut tick = tokio::time::interval(interval);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    if me.is_primary() {
                        return; // promoted while waiting — it now owns the full board
                    }
                    if me.sync_from_primary().await {
                        tracing::info!(ns = %me.cfg.namespace, "blackboard: initial mirror sync drained");
                        return;
                    }
                    // Unreachable / partial: retry next tick.
                }
            }));
        }

        // Promotion watch: the capability ring IS the failure detector.
        //
        // "Evaporated" means *was there, then gone*. An empty resolve of `.primary` before this watch
        // has ever SEEN the primary's advertisement is startup propagation lag, not failure — on a
        // CPU-starved host the first sighting can take many intervals, and promoting on lag creates a
        // split-brain primary that never demotes (the tuple-space #150/S13 failure). Audit 2026-07-15
        // pass 3 found the blackboard promoted after just two empty resolves with neither a
        // seen-primary gate nor an orphan grace — porting the tuple-space guard. Before first sight
        // the watch promotes only after a much longer orphan grace: the primary may genuinely be dead
        // while this secondary (re)starts, so availability still needs a bounded path; propagation lag
        // does not survive 10 intervals.
        {
            let me = Arc::clone(self);
            let interval = self.cfg.cap_refresh;
            tasks.push(tokio::spawn(async move {
                let mut tick = tokio::time::interval(interval);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                let mut seen_primary = false;
                let mut unseen_ticks: u32 = 0;
                const ORPHAN_GRACE_TICKS: u32 = 10;
                loop {
                    tick.tick().await;
                    if !me.resolve_role("primary").is_empty() {
                        seen_primary = true;
                        continue;
                    }
                    if seen_primary {
                        // Was there, now gone — confirm one interval later (split-brain guard).
                        tokio::time::sleep(interval).await;
                        if !me.resolve_role("primary").is_empty() {
                            continue;
                        }
                        tracing::warn!(ns = %me.cfg.namespace, "blackboard: primary evaporated — promoting");
                    } else {
                        // Never seen: propagation lag until the orphan grace expires.
                        unseen_ticks += 1;
                        if unseen_ticks < ORPHAN_GRACE_TICKS {
                            continue;
                        }
                        tracing::warn!(ns = %me.cfg.namespace, ticks = ORPHAN_GRACE_TICKS,
                            "blackboard: no primary ever seen within the orphan grace — promoting");
                    }
                    me.become_primary();
                    return;
                }
            }));
        }

        self.tasks.lock().extend(tasks);
        self.is_secondary.store(true, Ordering::Release);
        tracing::info!(ns = %self.cfg.namespace, "blackboard: mirroring as secondary");
    }

    async fn run_election(self: Arc<Self>) {
        let ns = &self.cfg.namespace;
        let reg = self.agent.capabilities().advertise_capability(
            Capability::new("blackboard", format!("{ns}.candidate")),
            self.cfg.cap_refresh,
        );
        *self.role_reg.lock() = Some(reg);

        let settle = (self.cfg.cap_refresh * 2).max(Duration::from_secs(2));
        tokio::time::sleep(settle).await;

        loop {
            if !self.resolve_role("primary").is_empty() {
                if self.init_store().is_ok() {
                    self.become_secondary();
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
                    }
                    return;
                }
                _ => tokio::time::sleep(self.cfg.cap_refresh).await,
            }
        }
    }

    /// Pull the primary's current live facts into the mirror (one snapshot RPC). Returns `true` iff a
    /// full snapshot was fetched and applied — the caller retries until this drains once, so a
    /// transient-unresolvable primary cannot leave a partial mirror (audit 2026-07-15 pass 3).
    async fn sync_from_primary(self: &Arc<Self>) -> bool {
        let Ok(primary) = self.resolve_primary() else { return false };
        let kind = rpc::rpc_kind(&self.cfg.namespace, "snapshot");
        let Ok(resp) = self
            .agent
            .service()
            .rpc_call(primary, kind, Bytes::new(), Duration::from_secs(10))
            .await
        else {
            return false;
        };
        let (Ok(facts), Some(store)) = (rpc::dec_facts_resp(&resp), self.store()) else {
            return false;
        };
        for f in facts {
            if self.mark_mirrored(f.id) {
                let _ = store.post_with_id(f.id, f.attributes, f.payload);
            }
        }
        true // a full snapshot (possibly empty) was applied — the mirror is now complete
    }

    // ── Serving paths (primary) ──────────────────────────────────────────────

    pub(crate) fn serve_post(self: &Arc<Self>, attrs: BTreeMap<String, String>, payload: Bytes) -> Result<u64, BlackboardError> {
        let store = self.store_expect();
        let id = store.post(attrs.clone(), payload.clone())?;
        self.replicate(rpc::enc_replicate_post(&Fact { id, attributes: attrs, payload }));
        Ok(id)
    }
    pub(crate) fn serve_read(&self, pred: &Predicate) -> Vec<Fact> {
        self.store().map(|s| s.read(pred)).unwrap_or_default()
    }
    pub(crate) fn serve_claim(&self, pred: &Predicate) -> Result<Option<Fact>, BlackboardError> {
        self.store_expect().claim(pred)
    }
    pub(crate) fn serve_ack(self: &Arc<Self>, id: u64) -> Result<(), BlackboardError> {
        self.store_expect().ack(id)?;
        self.replicate(rpc::enc_replicate_ack(id));
        Ok(())
    }
    pub(crate) fn serve_release(&self, id: u64) -> Result<(), BlackboardError> {
        self.store_expect().release(id)
    }

    /// Fire-and-forget replication of a `Post`/`Ack` record to every live secondary.
    fn replicate(self: &Arc<Self>, body: Bytes) {
        let secondaries = self.resolve_role("secondary");
        if secondaries.is_empty() {
            return;
        }
        let kind = rpc::rpc_kind(&self.cfg.namespace, "replicate");
        let agent = Arc::clone(&self.agent);
        tokio::spawn(async move {
            for node in secondaries {
                for _ in 0..2 {
                    if agent
                        .service()
                        .rpc_call(node.clone(), Arc::clone(&kind), body.clone(), Duration::from_secs(5))
                        .await
                        .is_ok()
                    {
                        break;
                    }
                }
            }
        });
    }

    // ── Resolution ───────────────────────────────────────────────────────────

    fn resolve_role(&self, role: &str) -> Vec<NodeId> {
        let filter = CapFilter::new("blackboard", format!("{}.{role}", self.cfg.namespace));
        self.agent.capabilities().resolve(&filter).into_iter().map(|(n, _)| n).collect()
    }
    fn resolve_primary(&self) -> Result<NodeId, BlackboardError> {
        let mut providers = self.resolve_role("primary");
        if providers.is_empty() {
            return Err(BlackboardError::NoProvider);
        }
        providers.sort_by_key(NodeId::to_string);
        Ok(providers.remove(0))
    }
    fn serving_locally(&self) -> bool {
        self.is_primary.load(Ordering::Acquire)
    }

    /// `true` if this node is currently serving as the namespace **primary**.
    pub fn is_primary(&self) -> bool {
        self.is_primary.load(Ordering::Acquire)
    }

    /// `true` if this node is currently mirroring as a **secondary**.
    pub fn is_secondary(&self) -> bool {
        self.is_secondary.load(Ordering::Acquire)
    }

    // ── Public API ───────────────────────────────────────────────────────────

    /// Post a fact (Linda `out`) — non-destructive; readable and claimable cluster-wide. Returns its id.
    pub async fn post(self: &Arc<Self>, attributes: BTreeMap<String, String>, payload: Bytes) -> Result<u64, BlackboardError> {
        if self.serving_locally() {
            return self.serve_post(attributes, payload);
        }
        let primary = self.resolve_primary()?;
        let resp = self
            .agent
            .service()
            .rpc_call(primary, rpc::rpc_kind(&self.cfg.namespace, "post"), rpc::enc_post_req(&attributes, &payload), Duration::from_secs(10))
            .await
            .map_err(|e| BlackboardError::Rpc(e.to_string()))?;
        rpc::dec_id_resp(&resp)
    }

    /// Non-destructive read (Linda `rd`): all facts matching `predicate`.
    pub async fn read(self: &Arc<Self>, predicate: &Predicate) -> Result<Vec<Fact>, BlackboardError> {
        if self.serving_locally() {
            return Ok(self.serve_read(predicate));
        }
        let primary = self.resolve_primary()?;
        let resp = self
            .agent
            .service()
            .rpc_call(primary, rpc::rpc_kind(&self.cfg.namespace, "read"), rpc::enc_predicate_req(predicate), Duration::from_secs(10))
            .await
            .map_err(|e| BlackboardError::Rpc(e.to_string()))?;
        rpc::dec_facts_resp(&resp)
    }

    /// Competitive destructive claim (Linda `in`): claim one fact matching `predicate`, or `None`.
    pub async fn claim(self: &Arc<Self>, predicate: &Predicate) -> Result<Option<Fact>, BlackboardError> {
        if self.serving_locally() {
            return self.serve_claim(predicate);
        }
        let primary = self.resolve_primary()?;
        let resp = self
            .agent
            .service()
            .rpc_call(primary, rpc::rpc_kind(&self.cfg.namespace, "claim"), rpc::enc_predicate_req(predicate), Duration::from_secs(10))
            .await
            .map_err(|e| BlackboardError::Rpc(e.to_string()))?;
        rpc::dec_opt_fact_resp(&resp)
    }

    /// Terminal ack: the claimed fact was consumed.
    pub async fn ack(self: &Arc<Self>, id: u64) -> Result<(), BlackboardError> {
        if self.serving_locally() {
            return self.serve_ack(id);
        }
        let primary = self.resolve_primary()?;
        let resp = self
            .agent
            .service()
            .rpc_call(primary, rpc::rpc_kind(&self.cfg.namespace, "ack"), rpc::enc_id_req(id), Duration::from_secs(10))
            .await
            .map_err(|e| BlackboardError::Rpc(e.to_string()))?;
        rpc::dec_unit_resp(&resp)
    }

    /// Release: abandon a claim — the fact returns to claimable.
    pub async fn release(self: &Arc<Self>, id: u64) -> Result<(), BlackboardError> {
        if self.serving_locally() {
            return self.serve_release(id);
        }
        let primary = self.resolve_primary()?;
        let resp = self
            .agent
            .service()
            .rpc_call(primary, rpc::rpc_kind(&self.cfg.namespace, "release"), rpc::enc_id_req(id), Duration::from_secs(10))
            .await
            .map_err(|e| BlackboardError::Rpc(e.to_string()))?;
        rpc::dec_unit_resp(&resp)
    }

    /// Live depth of the board (claimable + in-flight).
    pub async fn depth(self: &Arc<Self>) -> Result<BoardDepth, BlackboardError> {
        if self.serving_locally() {
            return Ok(self.store_expect().depth());
        }
        let primary = self.resolve_primary()?;
        let resp = self
            .agent
            .service()
            .rpc_call(primary, rpc::rpc_kind(&self.cfg.namespace, "depth"), Bytes::new(), Duration::from_secs(10))
            .await
            .map_err(|e| BlackboardError::Rpc(e.to_string()))?;
        rpc::dec_depth_resp(&resp)
    }

    /// Abort background tasks and retract advertisements.
    pub async fn shutdown(&self) {
        for h in self.tasks.lock().drain(..) {
            h.abort();
        }
        *self.primary_reg.lock() = None;
        *self.role_reg.lock() = None;
        self.is_primary.store(false, Ordering::Release);
        self.is_secondary.store(false, Ordering::Release);
    }
}
