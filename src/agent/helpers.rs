use crate::consensus::ConsensusEngine;
use crate::framing::{
    dispatch_gossip_send, dispatch_gossip_try_send, ForwardHint, GossipUpdate, WireMessage,
};
use crate::signal::{Boundary, Signal, SignalHandlers, SignalScope, signal_kind};
use bytes::Bytes;
use parking_lot::RwLock;
use std::sync::Arc;

use super::{GossipAgent, TaskCtx};

// ── Private impl helpers ──────────────────────────────────────────────────────

impl GossipAgent {
    /// Encodes `update` and delivers it to the correct gossip shard via `try_send`.
    /// When TLS is configured the frame is signed with the node's Ed25519 key.
    /// Returns `false` if the channel is full or the shard has died.
    pub(super) fn dispatch_update(&self, update: GossipUpdate) -> bool {
        let tls = self.task_ctx.tls.get().map(std::sync::Arc::as_ref);
        let msg = crate::framing::make_kv_wire_msg(update, self.node_id.id_hash(), tls);
        dispatch_gossip_try_send(
            &self.task_ctx.gossip_txs, msg,
            self.node_id.id_hash(), ForwardHint::All, &self.kv_state.dropped_frames,
        )
    }

    /// Like `dispatch_update` but awaits channel capacity rather than dropping.
    /// When TLS is configured the frame is signed with the node's Ed25519 key.
    pub(super) async fn dispatch_update_async(&self, update: GossipUpdate) -> bool {
        let tls = self.task_ctx.tls.get().map(std::sync::Arc::as_ref);
        let msg = crate::framing::make_kv_wire_msg(update, self.node_id.id_hash(), tls);
        dispatch_gossip_send(
            &self.task_ctx.gossip_txs, msg,
            self.node_id.id_hash(), ForwardHint::All,
        ).await
    }

    /// Constructs a [`GossipUpdate`] for a write that originates from
    /// `GossipAgent`'s public KV methods (`set`, `delete`,
    /// `advertise_*`, `join_group`, etc.). Thin wrapper over the canonical
    /// [`make_gossip_update`] factory in `crate::framing` — see that
    /// function's doc for the placement rationale and the HLC's
    /// causal-ordering guarantees.
    pub(super) fn make_update(&self, key: Arc<str>, value: Bytes, is_tombstone: bool) -> GossipUpdate {
        make_gossip_update(&self.node_id, self.config.default_ttl, key, value, is_tombstone, &self.task_ctx.hlc)
    }

    pub(super) fn make_consensus_engine(
        &self,
        abstain_when_opaque: bool,
        use_trust_slices:    bool,
        max_abstain_ballots: u32,
        topology_policy:     Option<crate::config::GroupTopologyPolicy>,
    ) -> ConsensusEngine {
        ConsensusEngine {
            task_ctx: Arc::clone(&self.task_ctx),
            abstain_when_opaque,
            use_trust_slices,
            max_abstain_ballots,
            self_locality: self.self_locality(),
            topology_policy,
        }
    }

    /// This node's `LocalityPath`, derived from `config.locality_path`. Returns
    /// `None` when locality is unconfigured. Shared helper used by the
    /// consensus engine builder, the gossip-shard start path, and the
    /// Phase 5 locality-aware resolution methods.
    pub(crate) fn self_locality(&self) -> Option<crate::locality::LocalityPath> {
        if self.config.locality_path.is_empty() {
            None
        } else {
            Some(crate::locality::LocalityPath::new(
                self.config.locality_path.iter().cloned(),
            ))
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
    // Seen-set TTL eviction uses physical milliseconds; extract from the
    // packed HLC so the seen-set's age math still operates in real time.
    let ts = crate::hlc::physical_ms(ctx.hlc.current());
    ctx.seen.mark_and_check(nonce, ts);
    let sig = Signal {
        kind: kind.clone(), scope: scope.clone(),
        payload: payload.clone(), sender: ctx.node_id.clone(), nonce,
    };
    // Fast-path for co-located rpc.result / bulk.result: fire the waiting
    // oneshot directly rather than fanning out through signal_handlers.
    let nonce_claimed = if payload.len() >= 8
        && (kind.as_ref() == signal_kind::RPC_RESULT || kind.as_ref() == signal_kind::BULK_RESULT)
    {
        let call_nonce = u64::from_le_bytes(payload[..8].try_into().unwrap());
        if let Some(tx) = ctx.rpc_pending.lock().unwrap().remove(&call_nonce) {
            let _ = tx.send(sig.clone());
            true
        } else {
            false
        }
    } else {
        false
    };
    if !nonce_claimed {
        let handler_fill = ctx.signal_handlers.fill_ratio(&kind);
        let combined = handler_fill.max(crate::framing::gossip_shard_fill(&ctx.gossip_txs));
        deliver_locally(&ctx.signal_boundary, &ctx.signal_handlers, &sig, combined);
    }
    let hint = match &scope {
        SignalScope::System           => ForwardHint::All,
        SignalScope::Group(name)      => ForwardHint::Group(name.clone()),
        SignalScope::Individual(peer) => ForwardHint::Individual(peer.clone()),
    };
    #[cfg(feature = "metrics")]
    {
        let scope_label = match &scope {
            SignalScope::System        => "system",
            SignalScope::Group(_)      => "group",
            SignalScope::Individual(_) => "node",
        };
        metrics::counter!("gossip_signals_emitted_total", "scope" => scope_label).increment(1);
    }
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
    // Seen-set TTL eviction uses physical milliseconds; extract from the
    // packed HLC so the seen-set's age math still operates in real time.
    let ts = crate::hlc::physical_ms(ctx.hlc.current());
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
