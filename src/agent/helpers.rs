use crate::consensus::ConsensusEngine;
use crate::framing::{
    dispatch_gossip_send, dispatch_gossip_try_send, ForwardHint, GossipUpdate, WireMessage,
};
use crate::node_id::NodeId;
use crate::seen::ShardedSeen;
use crate::signal::{Boundary, Signal, SignalHandlers, SignalScope};
use bytes::Bytes;
use parking_lot::RwLock;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc;

use super::GossipAgent;

// ── Private impl helpers ──────────────────────────────────────────────────────

impl GossipAgent {
    /// Encodes `update` and delivers it to the correct gossip shard via `try_send`.
    /// Returns `false` if the channel is full or the shard has died.
    pub(super) fn dispatch_update(&self, update: GossipUpdate) -> bool {
        dispatch_gossip_try_send(
            &self.gossip_txs, WireMessage::Data(update),
            self.node_id.id_hash(), ForwardHint::All, &self.dropped_frames,
        )
    }

    /// Like `dispatch_update` but awaits channel capacity rather than dropping.
    pub(super) async fn dispatch_update_async(&self, update: GossipUpdate) -> bool {
        dispatch_gossip_send(
            &self.gossip_txs, WireMessage::Data(update),
            self.node_id.id_hash(), ForwardHint::All,
        ).await
    }

    pub(super) fn make_update(&self, key: Arc<str>, value: Bytes, is_tombstone: bool) -> GossipUpdate {
        // SystemTime::now() — not cached current_ts — so each locally-originated write
        // gets a fresh timestamp. Two set() calls in the same health-monitor tick interval
        // would otherwise share a timestamp and lose LWW determinism for concurrent
        // cross-node writes to the same key.
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        GossipUpdate {
            nonce: fastrand::u64(1..),
            sender: self.node_id.id_hash(),
            ttl: self.config.default_ttl,
            is_tombstone,
            timestamp,
            key,
            value,
        }
    }

    pub(super) fn make_consensus_engine(
        &self,
        abstain_when_opaque: bool,
        use_trust_slices:    bool,
        max_abstain_ballots: u32,
    ) -> ConsensusEngine {
        ConsensusEngine {
            node_id:             self.node_id.clone(),
            seen:                self.seen.clone(),
            current_ts:          self.current_ts.clone(),
            signal_boundary:     self.signal_boundary.clone(),
            signal_handlers:     self.signal_handlers.clone(),
            gossip_txs:          self.gossip_txs.clone(),
            default_ttl:         self.config.default_ttl,
            kv_state:            self.kv_state.clone(),
            abstain_when_opaque,
            use_trust_slices,
            max_abstain_ballots,
        }
    }
}

// ── Free helpers ──────────────────────────────────────────────────────────────

/// Generates a nonce, marks it seen, delivers locally (with boundary + opacity checks),
/// encodes the wire frame, and routes to the correct gossip shard via `try_send`.
///
/// Shared by [`GossipAgent::emit`] and the [`advertise`](GossipAgent::advertise) task.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_signal(
    node_id:         &NodeId,
    seen:            &ShardedSeen,
    current_ts:      &AtomicU64,
    signal_boundary: &RwLock<Boundary>,
    signal_handlers: &SignalHandlers,
    gossip_txs:      &[mpsc::Sender<(Bytes, u64, ForwardHint)>],
    default_ttl:     u8,
    dropped_frames:  &AtomicU64,
    kind:            Arc<str>,
    scope:           SignalScope,
    payload:         Bytes,
) -> bool {
    let nonce = fastrand::u64(1..);
    let ts = current_ts.load(Ordering::Relaxed);
    let _ = seen.is_duplicate(nonce, ts);

    if signal_boundary.read().admits(&scope) {
        let admit = match &scope {
            SignalScope::Individual(_) => true,
            _ => {
                let opacity = signal_handlers.fill_ratio(&kind);
                opacity == 0.0 || fastrand::f32() >= opacity
            }
        };
        if admit {
            signal_handlers.deliver(&Signal {
                kind: kind.clone(), scope: scope.clone(),
                payload: payload.clone(), sender: node_id.clone(), nonce,
            });
        }
    }

    let hint = match &scope {
        SignalScope::System           => ForwardHint::All,
        SignalScope::Group(name)      => ForwardHint::Group(name.clone()),
        SignalScope::Individual(peer) => ForwardHint::Individual(peer.clone()),
    };
    dispatch_gossip_try_send(
        gossip_txs,
        WireMessage::Signal { ttl: default_ttl, nonce, sender: node_id.clone(), scope, kind, payload },
        node_id.id_hash(), hint, dropped_frames,
    )
}

/// Like [`emit_signal`] but awaits gossip channel capacity instead of dropping.
///
/// Used by consensus tasks that must not silently lose PROPOSE/COMMIT signals
/// under backpressure. Signal delivery to local handlers is still synchronous.
/// Returns `false` only if the shard task has crashed.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn emit_signal_async(
    node_id:         &NodeId,
    seen:            &ShardedSeen,
    current_ts:      &AtomicU64,
    signal_boundary: &RwLock<Boundary>,
    signal_handlers: &SignalHandlers,
    gossip_txs:      &[mpsc::Sender<(Bytes, u64, ForwardHint)>],
    default_ttl:     u8,
    _dropped_frames: &AtomicU64,
    kind:            Arc<str>,
    scope:           SignalScope,
    payload:         Bytes,
) -> bool {
    let nonce = fastrand::u64(1..);
    let ts = current_ts.load(Ordering::Relaxed);
    let _ = seen.is_duplicate(nonce, ts);

    if signal_boundary.read().admits(&scope) {
        let admit = match &scope {
            SignalScope::Individual(_) => true,
            _ => {
                let opacity = signal_handlers.fill_ratio(&kind);
                opacity == 0.0 || fastrand::f32() >= opacity
            }
        };
        if admit {
            signal_handlers.deliver(&Signal {
                kind: kind.clone(), scope: scope.clone(),
                payload: payload.clone(), sender: node_id.clone(), nonce,
            });
        }
    }

    let hint = match &scope {
        SignalScope::System           => ForwardHint::All,
        SignalScope::Group(name)      => ForwardHint::Group(name.clone()),
        SignalScope::Individual(peer) => ForwardHint::Individual(peer.clone()),
    };
    dispatch_gossip_send(
        gossip_txs,
        WireMessage::Signal { ttl: default_ttl, nonce, sender: node_id.clone(), scope, kind, payload },
        node_id.id_hash(), hint,
    ).await
}

pub(crate) fn compute_quorum_size(config_size: usize, member_count: usize) -> usize {
    if config_size > 0 { config_size } else { member_count / 2 + 1 }
}
