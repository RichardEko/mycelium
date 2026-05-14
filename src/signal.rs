use crate::node_id::NodeId;
use ahash::AHashSet;
use bytes::Bytes;
use dashmap::DashMap;
use papaya::HashMap as PapayaMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tracing::warn;

/// Scope of a signal — determines which nodes **act** on it.
///
/// All nodes **forward** all signals regardless of scope (fully epidemic propagation).
/// The receiving node's [`Boundary`] decides whether to act, not whether to forward.
/// This mirrors the chemical signalling model: hormones flood the bloodstream;
/// only cells with the matching receptor respond.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum SignalScope {
    /// Every node acts.
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
    map:       DashMap<Arc<str>, Vec<mpsc::Sender<Signal>>>,
    /// Lock-free reads via papaya epoch pinning — hot on every `last_signal` query.
    last_seen: PapayaMap<Arc<str>, Instant>,
}

impl SignalHandlers {
    pub(crate) fn new() -> Self {
        Self { map: DashMap::new(), last_seen: PapayaMap::new() }
    }

    /// Returns a new `mpsc::Receiver<Signal>` for `kind` with the default channel depth (256).
    /// Multiple calls for the same kind produce independent receivers.
    pub(crate) fn register(&self, kind: Arc<str>) -> mpsc::Receiver<Signal> {
        self.register_with_capacity(kind, 256)
    }

    /// Returns a new `mpsc::Receiver<Signal>` for `kind` with a caller-specified channel depth.
    pub(crate) fn register_with_capacity(&self, kind: Arc<str>, cap: usize) -> mpsc::Receiver<Signal> {
        let (tx, rx) = mpsc::channel(cap);
        self.map.entry(kind).or_default().push(tx);
        rx
    }

    /// Returns the minimum fill ratio across all open senders for `kind`.
    ///
    /// 0.0 = all channels empty (boundary fully transparent).
    /// 1.0 = all channels full (boundary fully opaque).
    /// Returns 0.0 when no handlers are registered — `deliver` would be a no-op anyway.
    ///
    /// Using the minimum means "admit if *any* handler still has capacity."
    pub(crate) fn fill_ratio(&self, kind: &Arc<str>) -> f32 {
        let Some(senders) = self.map.get(kind) else { return 0.0 };
        let mut min_ratio = f32::MAX;
        for tx in senders.iter().filter(|tx| !tx.is_closed()) {
            let ratio = 1.0_f32 - tx.capacity() as f32 / tx.max_capacity() as f32;
            if ratio < min_ratio { min_ratio = ratio; }
        }
        if min_ratio == f32::MAX { 0.0 } else { min_ratio.max(0.0) }
    }

    /// Fans out `signal` to all receivers registered for `signal.kind`.
    /// Closed senders are removed lazily. Full channels log a warning and drop the signal.
    /// Records the delivery time for [`last_signal`](Self::last_signal) regardless
    /// of whether any handlers are registered.
    ///
    /// Hot-path design: takes a DashMap *read* lock to snapshot the sender list, then
    /// sends to each without holding the lock. The write lock is only acquired if at
    /// least one sender was found closed — the common case (no closed senders) holds
    /// no write lock at all.
    pub(crate) fn deliver(&self, signal: &Signal) {
        self.last_seen.pin().insert(signal.kind.clone(), Instant::now());
        let snapshot: Vec<mpsc::Sender<Signal>> = match self.map.get(&signal.kind) {
            Some(guard) => guard.value().clone(),
            None => return,
        };
        let mut has_closed = false;
        for tx in &snapshot {
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
            if let Some(mut entry) = self.map.get_mut(&signal.kind) {
                entry.retain(|tx| !tx.is_closed());
            }
        }
    }

    /// Returns when this node last admitted a signal of `kind`, or `None` if never.
    pub(crate) fn last_signal(&self, kind: &str) -> Option<Instant> {
        self.last_seen.pin().get(kind).copied()
    }
}

/// Cancels the associated [`advertise`](crate::GossipAgent::advertise) task on drop.
///
/// Obtain one from [`GossipAgent::advertise`]. The task also exits automatically
/// when the agent shuts down, even if this handle is still live.
pub struct AdvertiseHandle {
    pub(crate) _cancel: tokio::sync::oneshot::Sender<()>,
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
    /// Pheromone trail (`load/<node_id>`) will be refreshed within one advertise interval.
    pub const BOUNDARY_TRANSPARENT: &str = "boundary.transparent";
}

/// Well-known KV key namespace prefixes for pheromone trail and membership state.
///
/// These conventions structure entries in the Layer 1 store. The store is the shared
/// medium — pheromone trails written here are persistent, anti-entropy synced, and
/// readable by any node at any time without signal handlers or local caches.
pub mod kv_ns {
    /// Pheromone trail namespace. Workers write `load/<node_id>` containing serialised
    /// load state (queue depth, accepted kinds, `written_at_ms` timestamp).
    ///
    /// Readers discard entries where `now - written_at_ms > N × advertise_interval`
    /// (pheromone evaporation — no coordination needed). Graceful shutdown should call
    /// `agent.delete("load/<node_id>")` for immediate evaporation.
    pub const LOAD:  &str = "load/";
    /// Group membership namespace. Written automatically by `join_group`/`leave_group`.
    /// Key: `grp/<group_name>/<node_id>`. Value: `b"1"` (live) or tombstone (left).
    pub const GROUP: &str = "grp/";
}
