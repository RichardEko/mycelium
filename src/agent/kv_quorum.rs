//! Per-write ACK tracker for [`GossipAgent::set_with_min_acks`].
//!
//! `QuorumAckTracker` is installed by `set_with_min_acks` just before the write and
//! removed after the wait completes (success or timeout). It lives in
//! `KvState::quorum_trackers` keyed by the key string.  `apply_and_notify`
//! calls `observe` for every incoming update so each tracker learns when
//! distinct peers have confirmed the value.
//!
//! The tracker is per-write, not per-key: each key maps to a copy-on-write
//! *list* of trackers, so concurrent `set_with_min_acks` calls on the same
//! key coexist — every inbound update is observed by all of them, and each
//! caller removes exactly its own tracker (by `Arc` identity) on completion.
//! (A previous single-slot design let the second caller overwrite the first
//! tracker, and the first caller's unconditional cleanup then deleted the
//! second's — both callers could time out spuriously. M2 Run-18 sweep
//! finding.)

use std::sync::Arc;
use papaya::HashMap;
use tokio::sync::watch;

/// Per-key tracker list type now lives in core as
/// [`crate::store::QuorumTrackerList`] (`Arc<Vec<Arc<dyn QuorumObserver>>>`) so the
/// substrate's `KvState` never names this upper type. `QuorumAckTracker` implements
/// the core `QuorumObserver` trait; the list holds it as a trait object.
use crate::store::{QuorumObserver, QuorumTrackerList};

/// Adds `tracker` to `key`'s list, coexisting with any concurrent callers'
/// trackers on the same key. The closure is retry-safe (clones per
/// invocation — papaya re-invokes it under CAS contention).
pub(crate) fn install_tracker(
    map:     &HashMap<Arc<str>, QuorumTrackerList>,
    key:     Arc<str>,
    tracker: &Arc<QuorumAckTracker>,
) {
    let obs_concrete = Arc::clone(tracker);
    let obs: Arc<dyn QuorumObserver> = obs_concrete;
    map.pin().compute(key, |existing| -> papaya::Operation<QuorumTrackerList, ()> {
        match existing {
            None => papaya::Operation::Insert(Arc::new(vec![Arc::clone(&obs)])),
            Some((_, list)) => {
                let mut v = (**list).clone();
                v.push(Arc::clone(&obs));
                papaya::Operation::Insert(Arc::new(v))
            }
        }
    });
}

/// Removes exactly `tracker` (by `Arc` identity) from `key`'s list — never a
/// concurrent caller's tracker. Drops the map entry when the list empties.
pub(crate) fn remove_tracker(
    map:     &HashMap<Arc<str>, QuorumTrackerList>,
    key:     &Arc<str>,
    tracker: &Arc<QuorumAckTracker>,
) {
    let needle_concrete = Arc::clone(tracker);
    let needle: Arc<dyn QuorumObserver> = needle_concrete;
    map.pin().compute(Arc::clone(key), |existing| -> papaya::Operation<QuorumTrackerList, ()> {
        match existing {
            None => papaya::Operation::Abort(()),
            Some((_, list)) => {
                let v: Vec<Arc<dyn QuorumObserver>> = list
                    .iter()
                    .filter(|t| !Arc::ptr_eq(t, &needle))
                    .cloned()
                    .collect();
                if v.len() == list.len() {
                    papaya::Operation::Abort(())
                } else if v.is_empty() {
                    papaya::Operation::Remove
                } else {
                    papaya::Operation::Insert(Arc::new(v))
                }
            }
        }
    });
}

/// Tracks how many distinct peers have confirmed a particular KV write.
///
/// Created by `set_with_min_acks` and observed by `apply_and_notify`.
pub(crate) struct QuorumAckTracker {
    /// Minimum HLC timestamp of the write we are waiting for. Any incoming
    /// update for the tracked key with `timestamp >= write_ts` from a peer
    /// (i.e., `sender != self_hash`) counts as an ACK.
    pub(crate) write_ts:  u64,
    /// `id_hash()` of this node — used to filter out loopback `apply_and_notify`
    /// calls that originate from our own local write.
    pub(crate) self_hash: u64,
    /// Set of peer `id_hash` values that have confirmed the write.
    pub(crate) acked_by:  HashMap<u64, ()>,
    /// Notifies the waiter whenever `acked_by.len()` increases.
    pub(crate) notify_tx: watch::Sender<usize>,
}

impl QuorumAckTracker {
    pub(crate) fn new(write_ts: u64, self_hash: u64) -> (Arc<Self>, watch::Receiver<usize>) {
        let (tx, rx) = watch::channel(0usize);
        let tracker = Arc::new(Self {
            write_ts,
            self_hash,
            acked_by:  HashMap::new(),
            notify_tx: tx,
        });
        (tracker, rx)
    }
}

impl QuorumObserver for QuorumAckTracker {
    /// Called by `apply_and_notify` for every incoming update on the tracked key.
    /// Increments the ACK count when the update is from a different node and
    /// carries a timestamp at least as recent as the tracked write.
    fn observe(&self, sender: u64, timestamp: u64) {
        if sender != self.self_hash && timestamp >= self.write_ts {
            let n = {
                let guard = self.acked_by.pin();
                guard.insert(sender, ());
                guard.len()
            };
            let _ = self.notify_tx.send(n);
        }
    }
}

/// Error returned by [`GossipAgent::set_with_min_acks`] when the durability threshold
/// is not reached within the timeout.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub enum QuorumError {
    /// The write propagated to fewer peers than requested within the deadline.
    Timeout {
        /// How many distinct peers had confirmed the write before the timeout.
        acks_received: usize,
    },
}

impl std::fmt::Display for QuorumError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QuorumError::Timeout { acks_received } =>
                write!(f, "set_with_min_acks timed out ({acks_received} peer(s) acknowledged)"),
        }
    }
}

impl std::error::Error for QuorumError {}
