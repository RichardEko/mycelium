use crate::consensus::ConsensusEngine;
use crate::framing::{
    dispatch_gossip_send, dispatch_gossip_try_send, ForwardHint, GossipUpdate, WireMessage,
};
use crate::signal::{Boundary, Signal, SignalHandlers, SignalScope};
use bytes::Bytes;
use parking_lot::RwLock;
use std::sync::{
    atomic::Ordering,
    Arc,
};

use super::{GossipAgent, TaskCtx};

// ── Private impl helpers ──────────────────────────────────────────────────────

impl GossipAgent {
    /// Encodes `update` and delivers it to the correct gossip shard via `try_send`.
    /// Returns `false` if the channel is full or the shard has died.
    pub(super) fn dispatch_update(&self, update: GossipUpdate) -> bool {
        dispatch_gossip_try_send(
            &self.task_ctx.gossip_txs, WireMessage::Data(update),
            self.node_id.id_hash(), ForwardHint::All, &self.kv_state.dropped_frames,
        )
    }

    /// Like `dispatch_update` but awaits channel capacity rather than dropping.
    pub(super) async fn dispatch_update_async(&self, update: GossipUpdate) -> bool {
        dispatch_gossip_send(
            &self.task_ctx.gossip_txs, WireMessage::Data(update),
            self.node_id.id_hash(), ForwardHint::All,
        ).await
    }

    pub(super) fn make_update(&self, key: Arc<str>, value: Bytes, is_tombstone: bool) -> GossipUpdate {
        // SystemTime::now() — not cached current_ts — so each locally-originated write
        // gets a fresh timestamp. Two set() calls in the same health-monitor tick interval
        // would otherwise share a timestamp and lose LWW determinism for concurrent
        // cross-node writes to the same key.
        make_gossip_update(&self.node_id, self.config.default_ttl, key, value, is_tombstone)
    }

    pub(super) fn make_consensus_engine(
        &self,
        abstain_when_opaque: bool,
        use_trust_slices:    bool,
        max_abstain_ballots: u32,
    ) -> ConsensusEngine {
        ConsensusEngine {
            task_ctx: Arc::clone(&self.task_ctx),
            abstain_when_opaque,
            use_trust_slices,
            max_abstain_ballots,
        }
    }
}

// ── Free helpers ──────────────────────────────────────────────────────────────

/// Checks the boundary and opacity gate, then delivers `signal` locally.
/// `combined_fill` is `max(handler_fill, shard_fill)` — pre-computed by the caller
/// so both `emit_signal` and the connection handler use the same combined metric.
fn deliver_locally(
    signal_boundary: &RwLock<Boundary>,
    signal_handlers: &SignalHandlers,
    signal: &Signal,
    combined_fill: f32,
) {
    if !signal_boundary.read().admits(&signal.scope) { return; }
    let admit = match &signal.scope {
        SignalScope::Individual(_) => true,
        _ => combined_fill == 0.0 || fastrand::f32() >= combined_fill,
    };
    if admit {
        signal_handlers.deliver(signal);
    }
}

/// Generates a nonce, marks it seen, delivers locally (with boundary + opacity checks),
/// encodes the wire frame, and routes to the correct gossip shard via `try_send`.
///
/// Shared by [`GossipAgent::emit`] and the [`advertise`](GossipAgent::advertise) task.
pub(crate) fn emit_signal(
    ctx:     &TaskCtx,
    kind:    Arc<str>,
    scope:   SignalScope,
    payload: Bytes,
) -> bool {
    let nonce = fastrand::u64(1..);
    let ts = ctx.current_ts.load(Ordering::Relaxed);
    ctx.seen.mark_and_check(nonce, ts);
    let handler_fill = ctx.signal_handlers.fill_ratio(&kind);
    let combined = handler_fill.max(crate::framing::gossip_shard_fill(&ctx.gossip_txs));
    deliver_locally(&ctx.signal_boundary, &ctx.signal_handlers, &Signal {
        kind: kind.clone(), scope: scope.clone(),
        payload: payload.clone(), sender: ctx.node_id.clone(), nonce,
    }, combined);
    let hint = match &scope {
        SignalScope::System           => ForwardHint::All,
        SignalScope::Group(name)      => ForwardHint::Group(name.clone()),
        SignalScope::Individual(peer) => ForwardHint::Individual(peer.clone()),
    };
    dispatch_gossip_try_send(
        &ctx.gossip_txs,
        WireMessage::Signal { ttl: ctx.default_ttl, nonce, sender: ctx.node_id.clone(), scope, kind, payload },
        ctx.node_id.id_hash(), hint, &ctx.kv_state.dropped_frames,
    )
}

/// Like [`emit_signal`] but awaits gossip channel capacity instead of dropping.
///
/// Used by consensus tasks that must not silently lose PROPOSE/COMMIT signals
/// under backpressure. Signal delivery to local handlers is still synchronous.
/// Returns `false` only if the shard task has crashed.
pub(crate) async fn emit_signal_async(
    ctx:     &TaskCtx,
    kind:    Arc<str>,
    scope:   SignalScope,
    payload: Bytes,
) -> bool {
    let nonce = fastrand::u64(1..);
    let ts = ctx.current_ts.load(Ordering::Relaxed);
    ctx.seen.mark_and_check(nonce, ts);
    let handler_fill = ctx.signal_handlers.fill_ratio(&kind);
    let combined = handler_fill.max(crate::framing::gossip_shard_fill(&ctx.gossip_txs));
    deliver_locally(&ctx.signal_boundary, &ctx.signal_handlers, &Signal {
        kind: kind.clone(), scope: scope.clone(),
        payload: payload.clone(), sender: ctx.node_id.clone(), nonce,
    }, combined);
    let hint = match &scope {
        SignalScope::System           => ForwardHint::All,
        SignalScope::Group(name)      => ForwardHint::Group(name.clone()),
        SignalScope::Individual(peer) => ForwardHint::Individual(peer.clone()),
    };
    dispatch_gossip_send(
        &ctx.gossip_txs,
        WireMessage::Signal { ttl: ctx.default_ttl, nonce, sender: ctx.node_id.clone(), scope, kind, payload },
        ctx.node_id.id_hash(), hint,
    ).await
}

pub(crate) fn compute_quorum_size(config_size: usize, member_count: usize) -> usize {
    if config_size > 0 { config_size } else { member_count / 2 + 1 }
}

pub(crate) use crate::framing::make_gossip_update;
