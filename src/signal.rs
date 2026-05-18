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
use ahash::AHashSet;
use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use papaya::HashMap as PapayaMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tracing::warn;

/// Retention window for the sender-log used by [`SignalHandlers::quorum`].
///
/// Entries older than this are evicted by `trim_sender_log` and ignored by `quorum`.
/// Should be ≥ any pheromone evaporation window callers rely on for load-aware routing
/// (see [`LoadState::written_at_ms`]).
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
        }
    }
}

/// Fan-out registry: maps signal kind to a list of `mpsc::Sender<Signal>`.
///
/// Multiple tasks may register receivers for the same kind. All registered
/// channels receive the signal on delivery. Closed channels are evicted lazily.
pub(crate) struct SignalHandlers {
    /// papaya::HashMap for lock-free epoch-pinned reads on the hot delivery path.
    /// Value is `Arc<Vec<Sender>>` so the snapshot in `deliver()` is an O(1) refcount
    /// increment. Registration (cold path) rebuilds the Vec+Arc via `compute()`.
    map:         PapayaMap<Arc<str>, Arc<Vec<mpsc::Sender<Signal>>>>,
    /// Lock-free reads via papaya epoch pinning — hot on every `last_signal` query.
    last_seen:   PapayaMap<Arc<str>, Instant>,
    /// Active refractory periods. Value is the `Instant` at which suppression expires.
    /// Read on every `deliver()` call — papaya epoch pinning keeps this lock-free.
    suppressed:  PapayaMap<Arc<str>, Instant>,
    /// Per-kind sender history for [`quorum`](Self::quorum) queries.
    /// Each entry is `(sender, received_at)`. Updated unconditionally in `deliver()`,
    /// including during suppression. Entries older than 10 minutes are evicted lazily.
    ///
    /// **DashMap, not papaya**: the update path in `deliver()` mutates a `Vec` in-place
    /// via `entry().or_default()` + `retain()` + `push()`. papaya's only mutation
    /// primitive is `compute()`, which requires cloning the entire value on every write —
    /// an O(window-entries) allocation on every signal delivery. DashMap's per-shard
    /// lock is the correct trade-off for this write-heavy, Vec-typed field.
    sender_log:  DashMap<Arc<str>, Vec<(NodeId, Instant)>>,
}

impl SignalHandlers {
    pub(crate) fn new() -> Self {
        Self {
            map:        PapayaMap::new(),
            last_seen:  PapayaMap::new(),
            suppressed: PapayaMap::new(),
            sender_log: DashMap::new(),
        }
    }

    /// Returns a new `mpsc::Receiver<Signal>` for `kind` with the default channel depth (256).
    /// Multiple calls for the same kind produce independent receivers.
    pub(crate) fn register(&self, kind: Arc<str>) -> mpsc::Receiver<Signal> {
        self.register_with_capacity(kind, 256)
    }

    /// Returns a new `mpsc::Receiver<Signal>` for `kind` with a caller-specified channel depth.
    pub(crate) fn register_with_capacity(&self, kind: Arc<str>, cap: usize) -> mpsc::Receiver<Signal> {
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

    /// Returns the maximum fill ratio across all open senders for `kind`.
    ///
    /// 0.0 = all channels empty (boundary fully transparent).
    /// 1.0 = at least one channel full (boundary fully opaque).
    /// Returns 0.0 when no handlers are registered — `deliver` would be a no-op anyway.
    ///
    /// Using the maximum means the opacity reading reflects the most-loaded handler.
    /// If any one handler is saturated, signals addressed to this node are shed at
    /// the boundary — consistent with the intent of load-adaptive admission control.
    /// (The previous minimum aggregation reported 0.0 opacity even when all but one
    /// handler were full, causing `opacity()` to mislead diagnostics.)
    pub(crate) fn fill_ratio(&self, kind: &Arc<str>) -> f32 {
        let guard = self.map.pin();
        let Some(senders) = guard.get(kind.as_ref()) else { return 0.0 };
        let mut max_ratio: f32 = 0.0;
        for tx in senders.iter().filter(|tx| !tx.is_closed()) {
            let ratio = 1.0_f32 - tx.capacity() as f32 / tx.max_capacity() as f32;
            if ratio > max_ratio { max_ratio = ratio; }
        }
        max_ratio.min(1.0)
    }

    /// Fans out `signal` to all receivers registered for `signal.kind`.
    /// Closed senders are removed lazily. Full channels log a warning and drop the signal.
    /// Records the delivery time for [`last_signal`](Self::last_signal) regardless
    /// of whether any handlers are registered.
    ///
    /// Hot-path design: epoch-pins the papaya map to get an `Arc<Vec<Sender>>` snapshot
    /// (one atomic refcount increment), then unpins before iterating. The guard is held
    /// for the absolute minimum time. Closed-sender eviction uses a CAS `compute()` write
    /// and only occurs when at least one sender was found closed.
    pub(crate) fn deliver(&self, signal: &Signal) {
        let now = Instant::now();
        self.last_seen.pin().insert(signal.kind.clone(), now);
        // Track sender history unconditionally — not gated by suppression so that
        // quorum() counts all received signals, not just delivered ones.
        {
            let mut log = self.sender_log.entry(signal.kind.clone()).or_default();
            log.retain(|(_sender, received_at)| received_at.elapsed() < SENDER_LOG_WINDOW);
            log.push((signal.sender.clone(), now));
        }
        // Refractory period: record timestamp but do not fan-out to handlers.
        // Forwarding (epidemic propagation) is unconditional and happens before deliver().
        if self.suppressed.pin().get(&signal.kind)
            .map(|until| now < *until)
            .unwrap_or(false)
        {
            return;
        }
        let snapshot: Arc<Vec<mpsc::Sender<Signal>>> = {
            let guard = self.map.pin();
            match guard.get(&*signal.kind) {
                Some(arc) => Arc::clone(arc),   // O(1) atomic refcount increment; guard released below
                None => return,
            }
            // guard drops here — epoch unpinned before the send loop
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

    /// Returns when this node last admitted a signal of `kind`, or `None` if never.
    pub(crate) fn last_signal(&self, kind: &str) -> Option<Instant> {
        self.last_seen.pin().get(kind).copied()
    }

    pub(crate) fn suppress(&self, kind: Arc<str>, until: Instant) {
        self.suppressed.pin().insert(kind, until);
    }

    pub(crate) fn unsuppress(&self, kind: &str) {
        self.suppressed.pin().remove(kind);
    }

    pub(crate) fn is_suppressed(&self, kind: &str) -> bool {
        self.suppressed.pin().get(kind)
            .map(|until| Instant::now() < *until)
            .unwrap_or(false)
    }

    /// Removes all sender-log entries older than 10 minutes and drops kinds whose
    /// log has become empty. Called from the GC task on each GC tick.
    ///
    /// Bounds both per-kind entry count (lazy retention inside `deliver`) and total
    /// kind count (removes kinds no longer seen), preventing unbounded growth when
    /// dynamic or high-cardinality kind strings are used.
    pub(crate) fn trim_sender_log(&self) {
        let cutoff = Instant::now() - SENDER_LOG_WINDOW;
        self.sender_log.retain(|_, log| {
            log.retain(|(_, received_at)| *received_at > cutoff);
            !log.is_empty()
        });
    }

    /// Returns `true` when at least `min_senders` distinct [`NodeId`]s have had a
    /// signal of `kind` delivered within `window`.
    ///
    /// Synchronous read; does not start a background task.
    pub(crate) fn quorum(&self, kind: &str, min_senders: usize, window: Duration) -> bool {
        let Some(log) = self.sender_log.get(kind) else { return false };
        let distinct = log.iter()
            .filter(|(_sender, received_at)| received_at.elapsed() <= window)
            .map(|(sender, _received_at)| sender.id_hash())
            .collect::<AHashSet<u64>>()
            .len();
        distinct >= min_senders
    }
}

// ── Pheromone trail ───────────────────────────────────────────────────────────

/// Pheromone load state written to Layer I by [`GossipAgent::manage_opacity`].
///
/// Key convention: `load/{node_id}/{kind}`.
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

#[allow(dead_code)]  // used by Fix B (manage_opacity_impl); remove after that commit
pub(crate) fn encode_load_state(s: &LoadState) -> Bytes {
    let mut buf = BytesMut::new();
    let _ = bincode::serde::encode_into_std_write(s, &mut (&mut buf).writer(), bincode_cfg());
    buf.freeze()
}

#[allow(dead_code)]  // used by Fix B, C, F (agent.rs); remove after those commits
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
    pub const INVOKE_RESULT:        &str = "invoke.result";
    /// Bulk invocation — payload carries a ticket; data travels via HTTP (Layer 3).
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
    /// The pheromone trail (`load/{node_id}/{kind}`) is tombstoned immediately.
    pub const BOUNDARY_TRANSPARENT: &str = "boundary.transparent";
}

/// Well-known KV key namespace prefixes for pheromone trail and membership state.
///
/// These conventions structure entries in the Layer 1 store. The store is the shared
/// medium — pheromone trails written here are persistent, anti-entropy synced, and
/// readable by any node at any time without signal handlers or local caches.
pub mod kv_ns {
    /// Pheromone trail namespace.
    ///
    /// Key: `load/{node_id}/{kind}`. Value: bincode-encoded [`LoadState`](crate::signal::LoadState).
    ///
    /// Written automatically by [`GossipAgent::manage_opacity`](crate::GossipAgent::manage_opacity)
    /// on every `BOUNDARY_OPAQUE` transition; tombstoned on `BOUNDARY_TRANSPARENT`.
    /// Readers discard entries where `now_ms − written_at_ms` exceeds their evaporation window
    /// (no coordination needed). Graceful shutdown should tombstone `load/{node_id}/{kind}`
    /// directly or rely on pheromone expiry.
    pub const LOAD:  &str = "load/";
    /// Group membership namespace. Written automatically by `join_group`/`leave_group`.
    /// Key: `grp/<group_name>/<node_id>`. Value: `b"1"` (live) or tombstone (left).
    pub const GROUP: &str = "grp/";
}
