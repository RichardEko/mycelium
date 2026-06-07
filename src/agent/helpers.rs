use crate::framing::{
    dispatch_gossip_send, dispatch_gossip_try_send, ForwardHint, WireMessage,
};
use crate::signal::{Boundary, Signal, SignalHandlers, SignalScope, signal_kind};
use crate::store::apply_and_notify;
use bytes::Bytes;
use parking_lot::RwLock;
use std::sync::Arc;

use super::{GossipAgent, TaskCtx};

// ── Private impl helpers ──────────────────────────────────────────────────────

impl GossipAgent {
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
        kind: Arc::clone(&kind), scope: scope.clone(),
        payload: payload.clone(), sender: ctx.node_id.clone(), nonce,
    };
    // Fast-path for co-located rpc.result / bulk.result: fire the waiting
    // oneshot directly rather than fanning out through signal_handlers.
    let nonce_claimed = if payload.len() >= 8
        && (kind.as_ref() == signal_kind::RPC_RESULT || kind.as_ref() == signal_kind::BULK_RESULT)
    {
        let call_nonce = u64::from_le_bytes(payload[..8].try_into().expect("infallible: payload.len() >= 8 checked above"));
        if let Some(tx) = ctx.rpc_pending.lock().unwrap_or_else(|e| e.into_inner()).remove(&call_nonce) {
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
        SignalScope::System             => ForwardHint::All,
        SignalScope::Group(name)        => ForwardHint::Group(Arc::clone(name)),
        SignalScope::Individual(peer)   => ForwardHint::Individual(peer.clone()),
        SignalScope::Groups(_)          => ForwardHint::All,
    };
    #[cfg(feature = "metrics")]
    {
        let scope_label = match &scope {
            SignalScope::System          => "system",
            SignalScope::Group(_)        => "group",
            SignalScope::Individual(_)   => "node",
            SignalScope::Groups(_)       => "groups",
        };
        metrics::counter!("gossip_signals_emitted_total", "scope" => scope_label).increment(1);
    }
    dispatch_gossip_try_send(
        &ctx.gossip_txs,
        WireMessage::Signal { ttl: ctx.default_ttl, nonce, sender: ctx.node_id.clone(), scope, kind, payload, hlc_seq: None },
        ctx.node_id.id_hash(), hint, &ctx.kv_state.dropped_frames,
    )
}

/// Like [`emit_signal`] but stamps a HLC sequence number so the receiver can
/// buffer and deliver signals from this sender in causal order.
///
/// Calls `hlc.tick()` to obtain a strictly-monotonic timestamp and sets
/// `hlc_seq = Some(ts)` on the wire frame. Receivers with
/// `signal_ordered_delivery = true` buffer and deliver these signals in
/// ascending HLC order per `(sender, kind)`.
pub(crate) fn emit_signal_ordered(
    ctx:     &TaskCtx,
    kind:    Arc<str>,
    scope:   SignalScope,
    payload: Bytes,
) -> bool {
    let nonce  = fastrand::u64(1..);
    let ts     = crate::hlc::physical_ms(ctx.hlc.current());
    let hlc_seq = ctx.hlc.tick();
    ctx.seen.mark_and_check(nonce, ts);
    let sig = Signal {
        kind: Arc::clone(&kind), scope: scope.clone(),
        payload: payload.clone(), sender: ctx.node_id.clone(), nonce,
    };
    let nonce_claimed = if payload.len() >= 8
        && (kind.as_ref() == signal_kind::RPC_RESULT || kind.as_ref() == signal_kind::BULK_RESULT)
    {
        let call_nonce = u64::from_le_bytes(payload[..8].try_into().expect("infallible: payload.len() >= 8 checked above"));
        if let Some(tx) = ctx.rpc_pending.lock().unwrap_or_else(|e| e.into_inner()).remove(&call_nonce) {
            let _ = tx.send(sig.clone());
            true
        } else { false }
    } else { false };
    if !nonce_claimed {
        let handler_fill = ctx.signal_handlers.fill_ratio(&kind);
        let combined = handler_fill.max(crate::framing::gossip_shard_fill(&ctx.gossip_txs));
        deliver_locally(&ctx.signal_boundary, &ctx.signal_handlers, &sig, combined);
    }
    let hint = match &scope {
        SignalScope::System           => ForwardHint::All,
        SignalScope::Group(name)      => ForwardHint::Group(Arc::clone(name)),
        SignalScope::Individual(peer) => ForwardHint::Individual(peer.clone()),
        SignalScope::Groups(_)        => ForwardHint::All,
    };
    dispatch_gossip_try_send(
        &ctx.gossip_txs,
        WireMessage::Signal {
            ttl: ctx.default_ttl, nonce, sender: ctx.node_id.clone(),
            scope, kind, payload, hlc_seq: Some(hlc_seq),
        },
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
        kind: Arc::clone(&kind), scope: scope.clone(),
        payload: payload.clone(), sender: ctx.node_id.clone(), nonce,
    }, combined);
    let hint = match &scope {
        SignalScope::System             => ForwardHint::All,
        SignalScope::Group(name)        => ForwardHint::Group(Arc::clone(name)),
        SignalScope::Individual(peer)   => ForwardHint::Individual(peer.clone()),
        SignalScope::Groups(_)          => ForwardHint::All,
    };
    dispatch_gossip_send(
        &ctx.gossip_txs,
        WireMessage::Signal { ttl: ctx.default_ttl, nonce, sender: ctx.node_id.clone(), scope, kind, payload, hlc_seq: None },
        ctx.node_id.id_hash(), hint,
    ).await
}

pub(crate) fn compute_quorum_size(config_size: usize, member_count: usize) -> usize {
    if config_size > 0 { config_size } else { member_count / 2 + 1 }
}

pub(crate) use crate::framing::make_gossip_update;

// ── KV primitives usable from typed sub-handles ───────────────────────────────

/// Returns the current value for `key`, or `None` if absent or tombstoned.
pub(crate) fn kv_get(ctx: &TaskCtx, key: &str) -> Option<Bytes> {
    ctx.kv_state.store.pin().get(key).and_then(|e| e.data.clone())
}

/// Subscribes to changes for `key`. Returns a `watch::Receiver<Option<Bytes>>`
/// whose value is `None` when the key is absent or tombstoned.
pub(crate) fn kv_subscribe(ctx: &TaskCtx, key: impl Into<Arc<str>>) -> tokio::sync::watch::Receiver<Option<Bytes>> {
    let key_arc: Arc<str> = key.into();
    loop {
        let guard = ctx.kv_state.subscriptions.pin();
        if let Some(tx) = guard.get(&key_arc)
            && !tx.is_closed() { return tx.subscribe(); }
        let current = ctx.kv_state.store.pin().get(&*key_arc).and_then(|e| e.data.clone());
        let (new_tx, rx) = tokio::sync::watch::channel(current);
        let mut slot = Some(new_tx);
        let result = guard.compute(Arc::clone(&key_arc), |existing| match existing {
            Some((_, tx)) if !tx.is_closed() => papaya::Operation::Abort(()),
            _ => match slot.take() {
                Some(tx) => papaya::Operation::Insert(tx),
                None     => papaya::Operation::Abort(()),
            },
        });
        if matches!(result, papaya::Compute::Inserted(..) | papaya::Compute::Updated { .. }) {
            return rx;
        }
    }
}

/// Subscribes to any write touching keys under `prefix`. Returns a
/// `watch::Receiver<u64>` that fires (incremented counter) on each match.
pub(crate) fn kv_subscribe_prefix(ctx: &TaskCtx, prefix: impl Into<Arc<str>>) -> tokio::sync::watch::Receiver<u64> {
    let prefix_arc: Arc<str> = prefix.into();
    loop {
        let guard = ctx.kv_state.prefix_watchers.pin();
        if let Some(tx) = guard.get(&prefix_arc)
            && !tx.is_closed() { return tx.subscribe(); }
        let (new_tx, rx) = tokio::sync::watch::channel(0u64);
        let new_tx_arc   = std::sync::Arc::new(new_tx);
        let mut slot     = Some(new_tx_arc);
        let result = guard.compute(Arc::clone(&prefix_arc), |existing| match existing {
            Some((_, tx)) if !tx.is_closed() => papaya::Operation::Abort(()),
            _ => match slot.take() {
                Some(tx) => papaya::Operation::Insert(tx),
                None     => papaya::Operation::Abort(()),
            },
        });
        if matches!(result, papaya::Compute::Inserted(..) | papaya::Compute::Updated { .. }) {
            return rx;
        }
    }
}

/// Per-subscriber variant: only fires when both the prefix matches AND `predicate(&key)`
/// returns `true`. Every call returns an independent receiver (no sharing).
pub(crate) fn kv_subscribe_prefix_with_predicate<P, F>(
    ctx:       &TaskCtx,
    prefix:    P,
    predicate: F,
) -> tokio::sync::watch::Receiver<u64>
where
    P: Into<Arc<str>>,
    F: Fn(&str) -> bool + Send + Sync + 'static,
{
    use std::sync::atomic::Ordering;
    let prefix_arc: Arc<str> = prefix.into();
    let (tx, rx) = tokio::sync::watch::channel(0u64);
    let entry = crate::store::PrefixPredicateWatcher {
        prefix:    prefix_arc,
        predicate: Arc::new(predicate),
        tx:        Arc::new(tx),
    };
    let id = ctx.kv_state.next_pred_watcher_id.fetch_add(1, Ordering::Relaxed);
    ctx.kv_state.prefix_predicate_watchers.pin().insert(id, entry);
    rx
}

/// Returns all live key-value pairs whose key starts with `prefix`.
pub(crate) fn kv_scan_prefix(ctx: &TaskCtx, prefix: &str) -> Vec<(Arc<str>, Bytes)> {
    let seg = prefix.find('/').map_or(prefix, |i| &prefix[..i]);
    let store_guard = ctx.kv_state.store.pin();
    let idx_guard   = ctx.kv_state.prefix_index.pin();
    if let Some(bucket) = idx_guard.get(seg) {
        bucket.pin().iter()
            .filter_map(|(key, _)| {
                if !key.starts_with(prefix) { return None; }
                let entry = store_guard.get(key.as_ref())?;
                let data  = entry.data.clone()?;
                Some((Arc::clone(key), data))
            })
            .collect()
    } else {
        store_guard.iter()
            .filter(|(k, v)| v.data.is_some() && k.starts_with(prefix))
            .map(|(k, v)| (Arc::clone(k), v.data.clone().expect("infallible: filtered by data.is_some() above")))
            .collect()
    }
}

/// Stores `value` under `key`, queues WAL (try-send), applies locally, gossips
/// (try-send). Returns `false` if the gossip channel is full or shard has crashed.
pub(crate) fn kv_set(ctx: &TaskCtx, key: Arc<str>, value: Bytes) -> bool {
    let update = make_gossip_update(&ctx.node_id, ctx.default_ttl, key, value, false, &ctx.hlc);
    if let Some(wal) = ctx.wal.get() {
        wal.append_try(crate::framing::sync_entry_from(&update));
    }
    apply_and_notify(&ctx.kv_state, &update);
    #[cfg(feature = "metrics")]
    metrics::counter!("gossip_kv_writes_total").increment(1);
    let tls = ctx.tls.get().map(Arc::as_ref);
    let msg = crate::framing::make_kv_wire_msg(update, ctx.node_id.id_hash(), tls);
    dispatch_gossip_try_send(
        &ctx.gossip_txs, msg,
        ctx.node_id.id_hash(), ForwardHint::All, &ctx.kv_state.dropped_frames,
    )
}

/// Writes `value` under `key`, appends to WAL, applies locally, and awaits
/// gossip channel capacity. Returns `false` only if the shard has crashed.
pub(crate) async fn kv_set_async(ctx: &TaskCtx, key: Arc<str>, value: Bytes) -> bool {
    let update = make_gossip_update(&ctx.node_id, ctx.default_ttl, key, value, false, &ctx.hlc);
    if let Some(wal) = ctx.wal.get() {
        let _ = wal.append(crate::framing::sync_entry_from(&update)).await;
    }
    apply_and_notify(&ctx.kv_state, &update);
    #[cfg(feature = "metrics")]
    metrics::counter!("gossip_kv_writes_total").increment(1);
    let tls = ctx.tls.get().map(Arc::as_ref);
    let msg = crate::framing::make_kv_wire_msg(update, ctx.node_id.id_hash(), tls);
    dispatch_gossip_send(
        &ctx.gossip_txs, msg,
        ctx.node_id.id_hash(), ForwardHint::All,
    ).await
}

/// Tombstones `key`, queues WAL (try-send), applies locally, gossips (try-send).
pub(crate) fn kv_delete(ctx: &TaskCtx, key: Arc<str>) -> bool {
    let update = make_gossip_update(&ctx.node_id, ctx.default_ttl, key, Bytes::new(), true, &ctx.hlc);
    if let Some(wal) = ctx.wal.get() {
        wal.append_try(crate::framing::sync_entry_from(&update));
    }
    apply_and_notify(&ctx.kv_state, &update);
    #[cfg(feature = "metrics")]
    metrics::counter!("gossip_kv_deletes_total").increment(1);
    let tls = ctx.tls.get().map(Arc::as_ref);
    let msg = crate::framing::make_kv_wire_msg(update, ctx.node_id.id_hash(), tls);
    dispatch_gossip_try_send(
        &ctx.gossip_txs, msg,
        ctx.node_id.id_hash(), ForwardHint::All, &ctx.kv_state.dropped_frames,
    )
}

/// Returns live members of `group` from Layer I KV (`grp/{group}/`).
pub(crate) fn group_members_ctx(ctx: &TaskCtx, group: &str) -> Vec<crate::node_id::NodeId> {
    let prefix = crate::signal::grp_prefix(group);
    kv_scan_prefix(ctx, &prefix)
        .into_iter()
        .filter_map(|(key, _)| {
            key.strip_prefix(&prefix)
                .and_then(|s| s.parse::<crate::node_id::NodeId>().ok())
        })
        .collect()
}

/// Cached variant of `group_members_ctx`. Returns a cached roster when the
/// `grp_generation` counter is unchanged and the entry is within `ttl`.
pub(crate) fn cached_group_members_ctx(
    ctx:   &TaskCtx,
    group: &str,
    ttl:   std::time::Duration,
) -> Arc<super::RosterEntry> {
    use std::sync::atomic::Ordering;
    let group_key: Arc<str> = Arc::from(group);
    let current_gen = ctx.kv_state.grp_generation.load(Ordering::Relaxed);
    let guard = ctx.group_roster_cache.pin();
    if let Some(entry) = guard.get(&group_key)
        && entry.grp_gen == current_gen && entry.fetched_at.elapsed() < ttl {
            return Arc::clone(entry);
        }
    let members = group_members_ctx(ctx, group);
    let fresh = Arc::new(super::RosterEntry {
        members,
        fetched_at: std::time::Instant::now(),
        grp_gen: current_gen,
    });
    guard.insert(group_key, Arc::clone(&fresh));
    fresh
}

/// Returns this node's `LocalityPath` from `ctx.config.locality_path`.
/// Returns `None` when locality is unconfigured.
pub(super) fn self_locality_ctx(ctx: &TaskCtx) -> Option<crate::locality::LocalityPath> {
    if ctx.config.locality_path.is_empty() {
        None
    } else {
        Some(crate::locality::LocalityPath::new(
            ctx.config.locality_path.iter().cloned(),
        ))
    }
}

/// Constructs a [`ConsensusEngine`] from a `TaskCtx` reference.
/// Used by `ConsensusHandle` methods.
pub(super) fn make_consensus_engine_ctx(
    ctx:                 &Arc<TaskCtx>,
    abstain_when_opaque: bool,
    use_trust_slices:    bool,
    max_abstain_ballots: u32,
    topology_policy:     Option<crate::config::GroupTopologyPolicy>,
) -> crate::consensus::ConsensusEngine {
    crate::consensus::ConsensusEngine {
        task_ctx: Arc::clone(ctx),
        abstain_when_opaque,
        use_trust_slices,
        max_abstain_ballots,
        self_locality: self_locality_ctx(ctx),
        topology_policy,
    }
}

/// Returns the group member with the lowest observed load for `kind`,
/// operating on a `TaskCtx` reference.
pub(super) fn suggest_leader_ctx(
    ctx:     &TaskCtx,
    group:   &str,
    kind:    &str,
    max_age: std::time::Duration,
) -> crate::node_id::NodeId {
    use super::opacity::peer_load_ctx;
    use crate::consensus::consensus_ns;
    let members = group_members_ctx(ctx, group);
    if members.is_empty() {
        return ctx.node_id.clone();
    }
    let trust_prefix = format!("{}{}/", consensus_ns::TRUST, group);
    let mut trust_counts: ahash::AHashMap<u64, usize> = ahash::AHashMap::new();
    for (_, bytes) in kv_scan_prefix(ctx, &trust_prefix) {
        let Ok((peers, _)) = bincode::serde::decode_from_slice::<Vec<crate::node_id::NodeId>, _>(
            &bytes, crate::framing::bincode_cfg()
        ) else { continue };
        for p in peers {
            *trust_counts.entry(p.id_hash()).or_insert(0) += 1;
        }
    }
    let load_by_node: ahash::AHashMap<Arc<str>, f32> = peer_load_ctx(ctx, max_age)
        .into_iter()
        .filter(|(_, k, _)| k.as_ref() == kind)
        .map(|(n, _, s)| (n, s.fill_ratio))
        .collect();
    let best = members.iter().min_by(|a, b| {
        let score = |n: &crate::node_id::NodeId| -> f32 {
            let fill = load_by_node.get(n.to_string().as_str()).copied().unwrap_or(0.0);
            let trust = *trust_counts.get(&n.id_hash()).unwrap_or(&0) as f32;
            fill / (1.0 + trust)
        };
        score(a).partial_cmp(&score(b))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.id_hash().cmp(&b.id_hash()))
    });
    best.cloned().unwrap_or_else(|| ctx.node_id.clone())
}

/// Tombstones `key`, appends to WAL, applies locally, awaits gossip capacity.
pub(crate) async fn kv_delete_async(ctx: &TaskCtx, key: Arc<str>) -> bool {
    let update = make_gossip_update(&ctx.node_id, ctx.default_ttl, key, Bytes::new(), true, &ctx.hlc);
    if let Some(wal) = ctx.wal.get() {
        let _ = wal.append(crate::framing::sync_entry_from(&update)).await;
    }
    apply_and_notify(&ctx.kv_state, &update);
    #[cfg(feature = "metrics")]
    metrics::counter!("gossip_kv_deletes_total").increment(1);
    let tls = ctx.tls.get().map(Arc::as_ref);
    let msg = crate::framing::make_kv_wire_msg(update, ctx.node_id.id_hash(), tls);
    dispatch_gossip_send(
        &ctx.gossip_txs, msg,
        ctx.node_id.id_hash(), ForwardHint::All,
    ).await
}
