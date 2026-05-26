//! Layer 2 — Signal / Boundary Mesh.
//!
//! Signals are ephemeral events that propagate epidemically to the entire cluster. Each node
//! holds a local [`Boundary`] (its receptor set) that decides whether it *acts* on an incoming
//! signal. Forwarding is always unconditional — the boundary only controls local delivery.
//!
//! Key types:
//! - [`SignalScope`] — System (every node), Group (members only), Individual (one node)
//! - [`Signal`] — the delivered event: kind, scope, payload, sender, nonce
//! - [`AdvertiseHandle`] — cancels a periodic `advertise()` task on drop
//!
//! Well-known kind strings live in [`signal_kind`]. KV namespace conventions live in [`kv_ns`].
//!
//! All signal APIs are exposed directly on [`GossipAgent`](crate::GossipAgent) — there is no
//! separate Layer 2 wrapper type.

use crate::framing::bincode_cfg;
use crate::node_id::NodeId;
use crate::store::StoreEntry;
use ahash::AHashSet;
use bytes::{BufMut, Bytes, BytesMut};
use papaya::HashMap as PapayaMap;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tracing::warn;

/// Default sender-log retention window (10 minutes).
///
/// Matches the default value of [`GossipConfig::signal_window_secs`].
/// Kept for doc-link references in [`ConsensusConfig`](crate::ConsensusConfig) and
/// [`GossipConfig`](crate::GossipConfig). In application code, prefer
/// [`GossipAgent::signal_window`](crate::GossipAgent::signal_window) — it reads the
/// operator-configured value. The live window stored on [`SignalHandlers`] is set from
/// `signal_window_secs` at agent construction and is used for all runtime eviction.
#[allow(dead_code)]
pub(crate) const SENDER_LOG_WINDOW: Duration = Duration::from_secs(600);

/// Scope of a signal — determines which nodes **act** on it.
///
/// All nodes **forward** all signals regardless of scope (fully epidemic propagation).
/// The receiving node's [`Boundary`] decides whether to act, not whether to forward.
/// This mirrors the chemical signalling model: hormones flood the bloodstream;
/// only cells with the matching receptor respond.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum SignalScope {
    /// Every node acts.
    ///
    /// **Best-effort epidemic delivery.** Under high message volume the opacity
    /// mechanism may shed signals at boundaries before they propagate to all nodes.
    /// Do not use for coordination that requires exactly-once or guaranteed delivery
    /// — use application-level timers with gossip KV state propagation instead.
    System,
    /// Only nodes that have joined the named group act.
    Group(Arc<str>),
    /// Only the named node acts.
    Individual(NodeId),
    /// Only nodes that have joined **any** of the named groups act (union membership).
    ///
    /// Used by [`GossipAgent::cross_group_propose`](crate::GossipAgent::cross_group_propose)
    /// to broadcast a ballot to all participants across multiple voting blocs in one shot.
    Groups(Vec<Arc<str>>),
}

/// A signal delivered to a local handler.
#[derive(Clone, Debug)]
pub struct Signal {
    /// Identifies the signal type and routes to registered handlers.
    pub kind:    Arc<str>,
    /// Scope set by the emitter — informational for handler logic.
    pub scope:   SignalScope,
    /// Application-defined payload bytes.
    pub payload: Bytes,
    /// Node that originally emitted this signal.
    pub sender:  NodeId,
    /// Random u64 used for network-level deduplication.
    pub nonce:   u64,
}

/// If `key` is a group-membership key belonging to `node_id_str`, returns the group name.
///
/// Matches keys of the form `grp/{group}/{node_id_str}` (live or tombstone). Returns `None`
/// for any other key.
pub(crate) fn parse_own_grp_key<'a>(key: &'a str, node_id_str: &str) -> Option<&'a str> {
    let inner = key.strip_prefix("grp/")?;
    let slash  = inner.rfind('/')?;
    if inner[slash + 1..] != *node_id_str { return None; }
    Some(&inner[..slash])
}

/// Returns the KV prefix for group membership keys: `grp/{group}/`.
///
/// Use this wherever a raw `format!("grp/{}/", group)` string would otherwise appear,
/// so all callers stay consistent with the [`kv_ns::GROUP`] namespace convention.
pub(crate) fn grp_prefix(group: &str) -> String {
    format!("grp/{}/", group)
}

/// Returns the KV key for a single node's group membership entry: `grp/{group}/{node_id}`.
pub(crate) fn grp_member_key(group: &str, node_id: &crate::node_id::NodeId) -> String {
    format!("{}{}", grp_prefix(group), node_id)
}

/// Local boundary filter.
///
/// Holds the set of groups this node has joined. `admits()` is O(1).
/// Mutated by `join_group` / `leave_group` (write path) and checked by the
/// connection handler (read path) via `Arc<RwLock<Boundary>>`.
pub(crate) struct Boundary {
    pub(crate) groups:  AHashSet<Arc<str>>,
    pub(crate) node_id: NodeId,
}

impl Boundary {
    pub(crate) fn new(node_id: NodeId) -> Self {
        Self { groups: AHashSet::new(), node_id }
    }

    /// Returns `true` if this node should act on a signal addressed to `scope`.
    #[inline]
    pub(crate) fn admits(&self, scope: &SignalScope) -> bool {
        match scope {
            SignalScope::System => true,
            SignalScope::Group(name) => self.groups.contains(name),
            SignalScope::Individual(id) => *id == self.node_id,
            SignalScope::Groups(names) => names.iter().any(|n| self.groups.contains(n)),
        }
    }
}

/// Per-kind sender history shard type alias.
type SenderLog = PapayaMap<Arc<str>, Arc<Mutex<VecDeque<(NodeId, Instant)>>>>;

// ── SignalHandlers sub-types ──────────────────────────────────────────────────
//
// SignalHandlers used to bundle five concerns into one struct (handler-table
// fan-out, last-seen tracking, suppression, sender-log for quorum queries,
// and per-(kind, sender) rate-limiting for sys/quorum/ writes). Split into
// four focused types below so each owns one cluster of fields + methods:
//
//   HandlerTable      — register / fill_ratio / fan-out (the admission path)
//   SignalLog         — last_seen + sender_log + quorum + seed + trim
//   SuppressionTable  — refractory-period table (suppress / unsuppress / check)
//   QuorumEvidence    — sys/quorum/ rate-limited payload + trim
//
// `SignalHandlers` (further down) holds one of each and delegates. `deliver`
// orchestrates across them in a fixed order: record → suppression-check →
// fan-out. No behaviour change vs. the pre-split implementation.

/// Handler fan-out registry: maps signal kind → list of `mpsc::Sender<Signal>`.
///
/// papaya is the hot map type because `deliver` runs on every received signal
/// and registrations happen rarely. Value is `Arc<Vec<Sender>>` so the snapshot
/// in `deliver_to_handlers` is a single atomic refcount increment; the guard
/// drops before the per-sender try_send loop.
struct HandlerTable {
    map: PapayaMap<Arc<str>, Arc<Vec<mpsc::Sender<Signal>>>>,
}

impl HandlerTable {
    fn new() -> Self { Self { map: PapayaMap::new() } }

    fn register_with_capacity(&self, kind: Arc<str>, cap: usize) -> mpsc::Receiver<Signal> {
        let (tx, rx) = mpsc::channel(cap);
        let mut slot = Some(tx);
        self.map.pin().compute(kind, |existing| -> papaya::Operation<Arc<Vec<mpsc::Sender<Signal>>>, ()> {
            match existing {
                None => papaya::Operation::Insert(Arc::new(vec![slot.take().unwrap()])),
                Some((_, arc)) => {
                    let mut v = (**arc).clone();
                    v.push(slot.take().unwrap());
                    papaya::Operation::Insert(Arc::new(v))
                }
            }
        });
        rx
    }

    fn fill_ratio(&self, kind: &Arc<str>) -> f32 {
        let guard = self.map.pin();
        let Some(senders) = guard.get(kind.as_ref()) else { return 0.0 };
        let mut max_ratio: f32 = 0.0;
        for tx in senders.iter().filter(|tx| !tx.is_closed()) {
            let ratio = 1.0_f32 - tx.capacity() as f32 / tx.max_capacity() as f32;
            if ratio > max_ratio { max_ratio = ratio; }
        }
        max_ratio.min(1.0)
    }

    /// Fans out a snapshot of senders for `signal.kind`. Closed senders are
    /// evicted lazily via a CAS write only when at least one was found closed.
    fn deliver_to_handlers(&self, signal: &Signal) {
        let snapshot: Arc<Vec<mpsc::Sender<Signal>>> = {
            let guard = self.map.pin();
            match guard.get(&*signal.kind) {
                Some(arc) => Arc::clone(arc),
                None => return,
            }
        };
        let mut has_closed = false;
        for tx in snapshot.iter() {
            match tx.try_send(signal.clone()) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    warn!(
                        kind = %signal.kind,
                        "Signal handler channel full; signal dropped. \
                         Handler is not draining fast enough — increase channel capacity \
                         via signal_rx_with_capacity or reduce signal rate.",
                    );
                }
                Err(TrySendError::Closed(_)) => { has_closed = true; }
            }
        }
        if has_closed {
            self.map.pin().compute(signal.kind.clone(), |existing| match existing {
                None => papaya::Operation::Abort(()),
                Some((_, arc)) => {
                    let filtered: Vec<_> = arc.iter().filter(|tx| !tx.is_closed()).cloned().collect();
                    if filtered.is_empty() {
                        papaya::Operation::Remove
                    } else {
                        papaya::Operation::Insert(Arc::new(filtered))
                    }
                }
            });
        }
    }
}

/// Per-kind history of admitted signals: when a kind was last seen and a
/// rolling window of `(sender, received_at)` for `quorum*` queries.
///
/// Distinct from `HandlerTable` because writes happen unconditionally — even
/// during suppression — so `quorum()` counts all received signals, not just
/// delivered ones.
struct SignalLog {
    last_seen:         PapayaMap<Arc<str>, Instant>,
    sender_log:        SenderLog,
    sender_log_window: Duration,
}

impl SignalLog {
    fn new(sender_log_window: Duration) -> Self {
        Self {
            last_seen:  PapayaMap::new(),
            sender_log: PapayaMap::new(),
            sender_log_window,
        }
    }

    /// Records that `signal` was seen at `now`, updating both `last_seen` and
    /// the sender history. Lazy retention prunes the deque front while the
    /// oldest entry is older than `sender_log_window`.
    fn record(&self, kind: &Arc<str>, sender: NodeId, now: Instant) {
        self.last_seen.pin().insert(kind.clone(), now);
        let window = self.sender_log_window;
        let arc = {
            let guard = self.sender_log.pin();
            if let Some(existing) = guard.get(kind.as_ref()) {
                existing.clone()
            } else {
                let new_arc = Arc::new(Mutex::new(VecDeque::<(NodeId, Instant)>::new()));
                let mut result: Option<Arc<Mutex<VecDeque<_>>>> = None;
                guard.compute(kind.clone(), |existing| match existing {
                    Some((_, arc)) => { result = Some(arc.clone()); papaya::Operation::Abort(()) }
                    None => { result = Some(new_arc.clone()); papaya::Operation::Insert(new_arc.clone()) }
                });
                result.unwrap()
            }
        };
        let mut log = arc.lock();
        while log.front().map(|(_, t)| t.elapsed() >= window).unwrap_or(false) {
            log.pop_front();
        }
        log.push_back((sender, now));
    }

    fn last_signal(&self, kind: &str) -> Option<Instant> {
        self.last_seen.pin().get(kind).copied()
    }

    fn quorum(&self, kind: &str, min_senders: usize, window: Duration) -> bool {
        let Some(arc) = self.sender_log.pin().get(kind).map(Arc::clone) else { return false };
        let log = arc.lock();
        let mut distinct: AHashSet<u64> = AHashSet::with_capacity(min_senders + 1);
        for (sender, received_at) in log.iter() {
            if received_at.elapsed() > window { continue; }
            distinct.insert(sender.id_hash());
            if distinct.len() >= min_senders { return true; }
        }
        false
    }

    fn quorum_for_group(
        &self,
        kind:          &str,
        member_hashes: &AHashSet<u64>,
        min_senders:   usize,
        window:        Duration,
    ) -> bool {
        let Some(arc) = self.sender_log.pin().get(kind).map(Arc::clone) else { return false };
        let log = arc.lock();
        let mut distinct: AHashSet<u64> = AHashSet::with_capacity(min_senders + 1);
        for (sender, received_at) in log.iter() {
            if received_at.elapsed() > window { continue; }
            let hash = sender.id_hash();
            if !member_hashes.contains(&hash) { continue; }
            distinct.insert(hash);
            if distinct.len() >= min_senders { return true; }
        }
        false
    }

    fn seed(&self, kind: Arc<str>, sender: NodeId, age_ms: u64) {
        if age_ms > self.sender_log_window.as_millis() as u64 { return; }
        let received_at = Instant::now()
            .checked_sub(Duration::from_millis(age_ms))
            .unwrap_or_else(Instant::now);
        let guard = self.sender_log.pin();
        let arc = if let Some(existing) = guard.get(kind.as_ref()) {
            existing.clone()
        } else {
            let new_arc = Arc::new(Mutex::new(VecDeque::<(NodeId, Instant)>::new()));
            let mut result: Option<Arc<Mutex<VecDeque<_>>>> = None;
            guard.compute(kind.clone(), |existing| match existing {
                Some((_, arc)) => { result = Some(arc.clone()); papaya::Operation::Abort(()) }
                None => { result = Some(new_arc.clone()); papaya::Operation::Insert(new_arc.clone()) }
            });
            result.unwrap()
        };
        arc.lock().push_back((sender, received_at));
    }

    /// Evicts sender_log entries older than `window`; drops kinds whose deque
    /// becomes empty.
    fn trim(&self, window: Duration) {
        let cutoff = Instant::now() - window;
        let to_remove: Vec<Arc<str>> = {
            let guard = self.sender_log.pin();
            guard.iter()
                .filter_map(|(kind, arc)| {
                    let mut log = arc.lock();
                    while log.front().map(|(_, t)| *t <= cutoff).unwrap_or(false) {
                        log.pop_front();
                    }
                    if log.is_empty() { Some(kind.clone()) } else { None }
                })
                .collect()
        };
        let guard = self.sender_log.pin();
        for kind in to_remove {
            guard.remove(&kind);
        }
    }
}

/// Per-kind refractory periods. `is_suppressed_at` is called once per
/// `deliver` so the path is on the hot side; papaya keeps reads lock-free.
struct SuppressionTable {
    suppressed: PapayaMap<Arc<str>, Instant>,
}

impl SuppressionTable {
    fn new() -> Self { Self { suppressed: PapayaMap::new() } }

    fn suppress(&self, kind: Arc<str>, until: Instant) {
        self.suppressed.pin().insert(kind, until);
    }

    fn unsuppress(&self, kind: &str) {
        self.suppressed.pin().remove(kind);
    }

    /// `now` is plumbed in so `deliver` uses the same instant for both the
    /// log record and the suppression check, avoiding two `Instant::now()`
    /// reads per delivery.
    fn is_suppressed_at(&self, kind: &str, now: Instant) -> bool {
        self.suppressed.pin().get(kind)
            .map(|until| now < *until)
            .unwrap_or(false)
    }

    fn is_suppressed(&self, kind: &str) -> bool {
        self.is_suppressed_at(kind, Instant::now())
    }
}

/// Tracks the last time a `sys/quorum/` entry was written for each
/// `{kind}/{sender}` key. Used to rate-limit Layer-I quorum-evidence writes
/// to one per second per pair without reading `KvState`.
struct QuorumEvidence {
    quorum_written: PapayaMap<Arc<str>, Instant>,
}

impl QuorumEvidence {
    fn new() -> Self { Self { quorum_written: PapayaMap::new() } }

    fn payload(&self, kind: &Arc<str>, sender: &NodeId) -> Option<(Arc<str>, Bytes)> {
        let quorum_key: Arc<str> = Arc::from(
            format!("{}{}/{}", kv_ns::QUORUM, kind, sender).as_str()
        );
        let now = Instant::now();
        let should_write = self.quorum_written.pin()
            .get(&quorum_key)
            .map(|last| now.duration_since(*last) > Duration::from_secs(1))
            .unwrap_or(true);
        if should_write {
            self.quorum_written.pin().insert(quorum_key.clone(), now);
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH).unwrap_or_default()
                .as_millis() as u64;
            Some((quorum_key, Bytes::copy_from_slice(&now_ms.to_le_bytes())))
        } else {
            None
        }
    }

    fn trim(&self, window: Duration, now: Instant) {
        let stale: Vec<Arc<str>> = self.quorum_written.pin()
            .iter()
            .filter_map(|(k, last)| {
                if now.duration_since(*last) > window { Some(k.clone()) } else { None }
            })
            .collect();
        let guard = self.quorum_written.pin();
        for k in stale {
            guard.remove(&k);
        }
    }
}

// ── SignalHandlers façade ─────────────────────────────────────────────────────

/// Fan-out registry plus auxiliary state for signal delivery.
///
/// Internally composed of four focused sub-types ([`HandlerTable`],
/// [`SignalLog`], [`SuppressionTable`], [`QuorumEvidence`]). The pub(crate)
/// surface stays unchanged: every method delegates to the appropriate
/// sub-type. `deliver` orchestrates across all four in a fixed order:
/// record → suppression-check → fan-out.
pub(crate) struct SignalHandlers {
    handlers:    HandlerTable,
    log:         SignalLog,
    suppression: SuppressionTable,
    evidence:    QuorumEvidence,
}

impl SignalHandlers {
    pub(crate) fn new(sender_log_window: Duration) -> Self {
        Self {
            handlers:    HandlerTable::new(),
            log:         SignalLog::new(sender_log_window),
            suppression: SuppressionTable::new(),
            evidence:    QuorumEvidence::new(),
        }
    }

    /// Returns a new `mpsc::Receiver<Signal>` for `kind` with the default channel depth (256).
    /// Multiple calls for the same kind produce independent receivers.
    pub(crate) fn register(&self, kind: Arc<str>) -> mpsc::Receiver<Signal> {
        self.handlers.register_with_capacity(kind, 256)
    }

    /// Returns a new `mpsc::Receiver<Signal>` for `kind` with a caller-specified channel depth.
    pub(crate) fn register_with_capacity(&self, kind: Arc<str>, cap: usize) -> mpsc::Receiver<Signal> {
        self.handlers.register_with_capacity(kind, cap)
    }

    /// Returns the maximum fill ratio across all open senders for `kind`.
    pub(crate) fn fill_ratio(&self, kind: &Arc<str>) -> f32 {
        self.handlers.fill_ratio(kind)
    }

    /// Fans out `signal` to all receivers registered for `signal.kind`.
    /// Closed senders are removed lazily. Full channels log a warning and drop the signal.
    /// Records the delivery time for [`last_signal`](Self::last_signal) regardless
    /// of whether any handlers are registered.
    ///
    /// Hot-path design: records into [`SignalLog`] first (unconditional —
    /// quorum counting includes suppressed kinds), checks the
    /// [`SuppressionTable`] using the same `now` (one `Instant::now()` call
    /// per delivery), then delegates fan-out to [`HandlerTable`].
    pub(crate) fn deliver(&self, signal: &Signal) {
        let now = Instant::now();
        self.log.record(&signal.kind, signal.sender.clone(), now);
        if self.suppression.is_suppressed_at(&signal.kind, now) {
            #[cfg(feature = "metrics")]
            metrics::counter!("gossip_signals_rejected_total").increment(1);
            return;
        }
        #[cfg(feature = "metrics")]
        metrics::counter!("gossip_signals_delivered_total", "kind" => signal.kind.to_string()).increment(1);
        self.handlers.deliver_to_handlers(signal);
    }

    /// Returns when this node last admitted a signal of `kind`, or `None` if never.
    pub(crate) fn last_signal(&self, kind: &str) -> Option<Instant> {
        self.log.last_signal(kind)
    }

    pub(crate) fn suppress(&self, kind: Arc<str>, until: Instant) {
        self.suppression.suppress(kind, until);
    }

    pub(crate) fn unsuppress(&self, kind: &str) {
        self.suppression.unsuppress(kind);
    }

    pub(crate) fn is_suppressed(&self, kind: &str) -> bool {
        self.suppression.is_suppressed(kind)
    }

    /// Removes sender-log entries and rate-limit entries older than `window`,
    /// then drops kinds whose log has become empty. Called from the GC task
    /// on each GC tick.
    pub(crate) fn trim_sender_log(&self, window: Duration) {
        self.log.trim(window);
        self.evidence.trim(window, Instant::now());
    }

    /// Seeds the sender log with a past entry reconstructed from a
    /// `sys/quorum/` Layer I record (used by
    /// `GossipAgent::warm_quorum_from_layer1`).
    pub(crate) fn seed_sender_log(&self, kind: Arc<str>, sender: NodeId, age_ms: u64) {
        self.log.seed(kind, sender, age_ms);
    }

    /// Returns `true` when at least `min_senders` distinct [`NodeId`]s have had a
    /// signal of `kind` delivered within `window`.
    pub(crate) fn quorum(&self, kind: &str, min_senders: usize, window: Duration) -> bool {
        self.log.quorum(kind, min_senders, window)
    }

    /// Like [`quorum`](Self::quorum) but only counts senders whose `id_hash()` is in
    /// `member_hashes`. **Not suitable for per-ballot consensus vote counting** —
    /// the sender log is keyed by `(kind, sender)` only, not `(slot, ballot)`.
    pub(crate) fn quorum_for_group(
        &self,
        kind:          &str,
        member_hashes: &AHashSet<u64>,
        min_senders:   usize,
        window:        Duration,
    ) -> bool {
        self.log.quorum_for_group(kind, member_hashes, min_senders, window)
    }

    /// Returns the quorum-evidence key and value to write, or `None` if the existing
    /// entry is less than 1 second old (rate-limit to prevent gossip churn).
    pub(crate) fn quorum_evidence_payload(
        &self,
        kind:   &Arc<str>,
        sender: &NodeId,
    ) -> Option<(Arc<str>, Bytes)> {
        self.evidence.payload(kind, sender)
    }
}

// ── Boundary reconciliation ───────────────────────────────────────────────────

/// Reconciles `Boundary::groups` from `grp/{group}/{node_id_str}` entries in the store.
///
/// Live entries insert into `groups`; tombstoned entries remove. Called at startup
/// (`rehydrate_boundary_from_kv`) and periodically by the GC task as a catch-all
/// for membership updates missed by the push-based path in the connection handler.
///
/// The caller holds the `RwLock` write guard and passes `&mut Boundary` directly,
/// keeping locking policy with the caller.
pub(crate) fn reconcile_boundary_from_store(
    store:       &PapayaMap<Arc<str>, StoreEntry>,
    boundary:    &mut Boundary,
    node_id_str: &str,
) {
    let mut to_insert: Vec<Arc<str>> = Vec::new();
    let mut to_remove: Vec<Arc<str>> = Vec::new();
    {
        let guard = store.pin();
        for (key, entry) in guard.iter() {
            let Some(group) = parse_own_grp_key(key, node_id_str) else { continue };
            if entry.data.is_some() {
                to_insert.push(Arc::from(group));
            } else {
                to_remove.push(Arc::from(group));
            }
        }
    }
    for g in to_insert { boundary.groups.insert(g); }
    for g in &to_remove { boundary.groups.remove(g.as_ref()); }
}

// ── Pheromone trail ───────────────────────────────────────────────────────────

/// Pheromone load state written to Layer I by [`GossipAgent::manage_opacity`].
///
/// Key convention: `sys/load/{node_id}/{kind}` (see [`kv_ns::LOAD`]).
/// Encoded with [`bincode_cfg()`](crate::framing::bincode_cfg) (fixed-int).
/// An absent key means the node is transparent (not overloaded) for that kind.
/// Tombstoned automatically when `BOUNDARY_TRANSPARENT` is emitted.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct LoadState {
    /// Handler-channel fill ratio at time of writing (0.0–1.0).
    pub fill_ratio: f32,
    /// Whether [`BOUNDARY_OPAQUE`](signal_kind::BOUNDARY_OPAQUE) has been emitted.
    pub is_opaque: bool,
    /// Milliseconds since Unix epoch when this entry was written.
    ///
    /// Readers discard entries where `now_ms − written_at_ms` exceeds their
    /// chosen evaporation window (should be ≤ [`SENDER_LOG_WINDOW`]).
    pub written_at_ms: u64,
}

pub(crate) fn encode_load_state(s: &LoadState) -> Bytes {
    let mut buf = BytesMut::new();
    let _ = bincode::serde::encode_into_std_write(s, &mut (&mut buf).writer(), bincode_cfg());
    buf.freeze()
}

pub(crate) fn decode_load_state(b: &Bytes) -> Option<LoadState> {
    bincode::serde::decode_from_slice(b, bincode_cfg())
        .ok()
        .map(|(v, _)| v)
}

/// Cancels the associated [`advertise`](crate::GossipAgent::advertise) task on drop.
///
/// Obtain one from [`GossipAgent::advertise`]. The task also exits automatically
/// when the agent shuts down, even if this handle is still live.
pub struct AdvertiseHandle {
    pub(crate) _cancel: tokio::sync::oneshot::Sender<()>,
}

/// Cancels the associated [`watch`](crate::GossipAgent::watch) task on drop.
///
/// Obtain one from [`GossipAgent::watch`]. The task also exits automatically
/// when the agent shuts down, even if this handle is still live.
pub struct WatchHandle {
    pub(crate) _cancel: tokio::sync::oneshot::Sender<()>,
}

/// Cancels the associated [`manage_opacity`](crate::GossipAgent::manage_opacity) governor
/// task on drop.
///
/// Obtain one from [`GossipAgent::manage_opacity`] or
/// [`GossipAgent::manage_opacity_gated`]. The task also exits automatically when
/// the agent shuts down, even if this handle is still live.
pub struct OpacityHandle {
    pub(crate) _cancel: tokio::sync::oneshot::Sender<()>,
}

/// Application hint for the opacity governor.
///
/// All fields have documented defaults; use [`OpacityHint::default()`] and override
/// only what you need.
#[derive(Clone, Debug)]
pub struct OpacityHint {
    /// Suggested threshold at which `BOUNDARY_OPAQUE` should be emitted (0.0–1.0).
    ///
    /// The library clamps this to `[0.4, 0.95]` and reduces it further when the
    /// fill rate is rising quickly (trend adaptation). Default: `0.75`.
    pub threshold:  f32,
    /// How far fill must drop below `threshold` before `BOUNDARY_TRANSPARENT` is emitted.
    ///
    /// Prevents oscillation at the threshold boundary. Default: `0.20`.
    pub hysteresis: f32,
    /// Payload attached to the `BOUNDARY_OPAQUE` signal.
    ///
    /// Useful for carrying application-defined context (e.g. a reason string or
    /// estimated drain time in milliseconds). Default: empty.
    pub payload:    bytes::Bytes,
}

impl Default for OpacityHint {
    fn default() -> Self {
        Self {
            threshold:  0.75,
            hysteresis: 0.20,
            payload:    bytes::Bytes::new(),
        }
    }
}

/// Snapshot of governor state passed to the application gate on each tick.
#[derive(Clone, Debug)]
pub struct OpacityState {
    /// Current fill ratio of the monitored kind's handler channel (0.0–1.0).
    pub fill_ratio:          f32,
    /// Threshold the library computed after applying trend adaptation to the hint.
    pub effective_threshold: f32,
    /// Fill change since the previous tick (positive = filling, negative = draining).
    pub trend:               f32,
    /// Whether `BOUNDARY_OPAQUE` has been emitted and not yet cleared.
    pub is_opaque:           bool,
}

/// Well-known signal kind string constants.
///
/// These are conventions, not protocol requirements. Applications are free to
/// define their own signal kinds alongside or instead of these.
pub mod signal_kind {
    /// Invoke a contract — payload is the serialized invocation request.
    pub const INVOKE:               &str = "invoke";
    /// Result of a contract invocation.
    ///
    /// **Nonce convention**: the first 8 bytes of the payload carry a little-endian u64
    /// correlation nonce that matches the first 8 bytes of the originating
    /// [`INVOKE`] or [`INVOKE_BULK`] payload. Use [`GossipAgent::request`](crate::GossipAgent::request)
    /// on the caller side to generate and match the nonce automatically.
    pub const INVOKE_RESULT:        &str = "invoke.result";
    /// Bulk-invoke signal. The sender emits this kind to trigger a batch operation
    /// on a group of peers. Payload carries a ticket/correlation ID; the actual
    /// data transfer is the responsibility of the application's Layer 3 transport
    /// (HTTP, gRPC, shared storage, etc. — not provided by this library).
    ///
    /// Responders reply with [`INVOKE_RESULT`] echoing the ticket in the first 8
    /// payload bytes so the initiator can correlate via [`GossipAgent::request`](crate::GossipAgent::request).
    pub const INVOKE_BULK:          &str = "invoke.bulk";
    /// A contract has become available at `sender`.
    pub const CONTRACT_AVAILABLE:   &str = "contract.available";
    /// A contract has been withdrawn from `sender`.
    pub const CONTRACT_WITHDRAWN:   &str = "contract.withdrawn";
    /// Cluster lifecycle event (join / leave / restart).
    pub const CLUSTER_EVENT:        &str = "cluster.event";
    /// Liveness probe.
    pub const HEALTH_PROBE:         &str = "health.probe";
    /// Liveness response.
    pub const HEALTH_ACK:           &str = "health.ack";
    /// Node is entering an opaque state — load is high, incoming work being shed.
    /// Payload: application-defined (e.g. reason string, ETA milliseconds).
    /// Upstream nodes should drain service-level connections to `sender` on receipt.
    pub const BOUNDARY_OPAQUE:      &str = "boundary.opaque";
    /// Node has cleared its load and resumed normal signal admission.
    /// The pheromone trail (`sys/load/{node_id}/{kind}`) is tombstoned immediately.
    pub const BOUNDARY_TRANSPARENT: &str = "boundary.transparent";
    /// Generic RPC reply. The correlation nonce in the first 8 bytes of the
    /// originating request payload is echoed at the start of this payload.
    /// Use [`GossipAgent::rpc_call`](crate::GossipAgent::rpc_call) /
    /// [`GossipAgent::rpc_respond`](crate::GossipAgent::rpc_respond) to handle
    /// the nonce automatically.
    pub const RPC_RESULT: &str = "rpc.result";
    /// Reply to an [`INVOKE_BULK`] call. Same nonce-prefix convention as
    /// [`RPC_RESULT`] but on a dedicated kind so bulk and RPC reply handlers
    /// do not compete for the same signal dispatch slot.
    pub const BULK_RESULT: &str = "bulk.result";
    /// MCP tool invocation — payload is a JSON-RPC 2.0 request body.
    /// Replies are sent as [`RPC_RESULT`] signals back to the caller.
    pub const MCP_INVOKE: &str = "mcp.invoke";

    /// Agent state transition notification.
    /// Payload: `{"node": "<id>", "from": "<state>", "to": "<state>"}`.
    /// Emitted by [`AgentStateMachine::transition`](crate::AgentStateMachine::transition)
    /// after every committed transition.
    pub const AGENT_STATE: &str = "agent.state";

    /// Agent requesting supervisor approval before an `Invoking` transition.
    /// Payload: `{"node": "<id>", "tool": "<name>"}`.
    /// Supervisors reply with [`AGENT_VETO`] to block the transition or stay
    /// silent to approve (default — no reply within 30 s = approved).
    pub const AGENT_APPROVE: &str = "agent.approve";

    /// Supervisor veto of a pending `Invoking` transition.
    /// Payload: `{"tool": "<name>"}`. Must arrive within 30 s of the
    /// corresponding [`AGENT_APPROVE`] signal.
    pub const AGENT_VETO: &str = "agent.veto";
}

/// Well-known KV key namespace prefixes for pheromone trail and membership state.
///
/// These conventions structure entries in the Layer 1 store. The store is the shared
/// medium — pheromone trails written here are persistent, anti-entropy synced, and
/// readable by any node at any time without signal handlers or local caches.
///
/// **Namespace protection**: entries under `sys/` are written exclusively by the
/// library. Applications must not write to `sys/load/`, `sys/quorum/`, or any other
/// `sys/` sub-namespace — doing so will corrupt pheromone trails and quorum evidence,
/// leading to incorrect opacity decisions and stale quorum reads. Use the `grp/`,
/// `svc/`, `consensus/`, and application-defined namespaces for application data.
pub mod kv_ns {
    /// Pheromone trail namespace (library-internal — do not write from application code).
    ///
    /// Key: `sys/load/{node_id}/{kind}`. Value: bincode-encoded [`LoadState`](crate::signal::LoadState).
    ///
    /// Written automatically by [`GossipAgent::manage_opacity`](crate::GossipAgent::manage_opacity)
    /// on every `BOUNDARY_OPAQUE` transition; tombstoned on `BOUNDARY_TRANSPARENT`.
    /// Readers discard entries where `now_ms − written_at_ms` exceeds their evaporation window
    /// (no coordination needed). Graceful shutdown tombstones `sys/load/{node_id}/{kind}`
    /// automatically; callers may also call `agent.delete(format!("sys/load/{}/{}", node_id, kind))`
    /// directly to force immediate evaporation.
    pub const LOAD:  &str = "sys/load/";
    /// Group membership namespace. Written automatically by `join_group`/`leave_group`.
    /// Key: `grp/<group_name>/<node_id>`. Value: `b"1"` (live) or tombstone (left).
    pub const GROUP: &str = "grp/";

    /// Advertised-capability namespace (optional persistence via
    /// [`GossipAgent::advertise_persistent`](crate::GossipAgent::advertise_persistent)).
    ///
    /// Key: `svc/{kind}/{node_id}`. Value: the payload bytes from the most recent
    /// advertise tick. Tombstoned automatically when the returned
    /// [`AdvertiseHandle`](crate::signal::AdvertiseHandle) is dropped or the agent shuts down.
    /// Late joiners can call `scan_prefix(kv_ns::ADVERTISE)` to find current capabilities
    /// without waiting for the next advertise tick.
    pub const ADVERTISE: &str = "svc/";

    /// Node identity namespace (library-internal — do not write from application code).
    ///
    /// Key: `sys/identity/{node_id}`. Value: 32-byte Ed25519 public key (raw bytes).
    /// Written at startup by nodes running with TLS enabled; used by peers to verify
    /// signed consensus messages when a mTLS cert extract is not yet available.
    pub const IDENTITY: &str = "sys/identity/";

    /// Persistent quorum evidence namespace (library-internal — do not write from application code).
    ///
    /// Key: `sys/quorum/{kind}/{sender_node_id}`. Value: 8-byte little-endian Unix millisecond
    /// timestamp of when this node last received and admitted a signal of `kind` from
    /// `sender_node_id`. Written by the connection handler on every admitted signal
    /// delivery; anti-entropy synced to peers so the evidence survives process restarts.
    ///
    /// Use [`GossipAgent::quorum_persistent`] to query the count of distinct senders
    /// within a time window. Prefer [`GossipAgent::quorum`] (in-memory) for low-latency
    /// queries — `quorum_persistent` is only needed when quorum evidence must survive
    /// crashes or restarts.
    pub const QUORUM: &str = "sys/quorum/";

    /// Ordered durable log namespace.
    ///
    /// Key: `log/{stream}/{hlc:016x}`. The 16-char zero-padded hex HLC ensures
    /// lexicographic order equals time order. Written by [`GossipAgent::append`];
    /// compacted by [`GossipAgent::compact_log`].
    pub const LOG: &str = "log/";

    /// Consumer group offset cursors.
    ///
    /// Key: `clog/{stream}/{group}/offset`. Value: 16-char hex HLC of the last
    /// processed entry. Written by [`GossipAgent::subscribe_log_group`] after each
    /// entry is successfully delivered.
    pub const CONSUMER_LOG: &str = "clog/";

    /// Distributed lock state.
    ///
    /// Key: `lock/{name}`. Value: JSON `{"holder":"ip:port","token":u64,"expires_ms":u64}`.
    /// Written by [`GossipAgent::distributed_lock`]; tombstoned when the returned
    /// [`LockGuard`](crate::LockGuard) is dropped.
    pub const LOCK: &str = "lock/";
}
