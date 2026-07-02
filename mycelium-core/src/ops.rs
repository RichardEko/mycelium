//! Free operations over [`CoreCtx`] — the implementation behind the Layer-I/II typed
//! handles (`KvHandle`, `MeshHandle`, `SchemaHandle`).
//!
//! These were `agent::helpers` functions over `&TaskCtx` in the full crate; M3 moved them
//! here over `&CoreCtx` so the substrate handles can live in `mycelium-core`. Upper call
//! sites pass `&TaskCtx`, which Deref-coerces to `&CoreCtx`. `emit_signal*`'s former local
//! `rpc_pending` fast-path is now [`CoreCtx::reply_interceptor`](crate::CoreCtx) — the RPC
//! correlation closure the upper service layer registers (mechanism in core; agency above).

use crate::context::CoreCtx;
use crate::framing::{
    dispatch_gossip_send, dispatch_gossip_try_send, make_gossip_update, make_kv_wire_msg,
    sync_entry_from, ForwardHint, WireMessage,
};
use crate::signal::{Boundary, Signal, SignalHandlers, SignalScope};
use crate::store::apply_and_notify;
use bytes::Bytes;
use parking_lot::RwLock;
use std::sync::Arc;

/// Checks the boundary and opacity gate, then delivers `signal` locally.
/// `combined_fill` is `max(handler_fill, shard_fill)` — pre-computed by the caller.
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

/// Generates a nonce, marks it seen, delivers locally (boundary + opacity checks),
/// encodes the wire frame, and routes to the correct gossip shard via `try_send`.
pub fn emit_signal(
    ctx:     &CoreCtx,
    kind:    Arc<str>,
    scope:   SignalScope,
    payload: Bytes,
) -> bool {
    let nonce = fastrand::u64(1..);
    let ts = crate::hlc::physical_ms(ctx.hlc.current());
    ctx.seen.mark_and_check(nonce, ts);
    let sig = Signal {
        kind: Arc::clone(&kind), scope: scope.clone(),
        payload, sender: ctx.node_id.clone(), nonce,
    };
    // Co-located rpc.result / bulk.result: the upper-registered interceptor claims the
    // reply (fires the waiting oneshot) and we skip the signal_handlers fan-out.
    let nonce_claimed = match ctx.reply_interceptor.as_ref() {
        Some(claim) => claim(&sig),
        None => false,
    };
    if !nonce_claimed {
        let handler_fill = ctx.signal_handlers.fill_ratio(&kind);
        let combined = handler_fill.max(crate::framing::gossip_shard_fill(&ctx.gossip_txs));
        deliver_locally(&ctx.signal_boundary, &ctx.signal_handlers, &sig, combined);
    }
    let hint = forward_hint(&sig.scope);
    #[cfg(feature = "metrics")]
    {
        let scope_label = scope_label(&sig.scope);
        metrics::counter!("gossip_signals_emitted_total", "scope" => scope_label).increment(1);
    }
    dispatch_gossip_try_send(
        &ctx.gossip_txs,
        WireMessage::Signal { ttl: ctx.default_ttl, nonce, sender: ctx.node_id.clone(), scope: sig.scope.clone(), kind: sig.kind.clone(), payload: sig.payload.clone(), hlc_seq: None },
        ctx.node_id.id_hash(), hint, &ctx.kv_state.dropped_frames,
    )
}

/// Like [`emit_signal`] but stamps an HLC sequence number for ordered delivery.
pub fn emit_signal_ordered(
    ctx:     &CoreCtx,
    kind:    Arc<str>,
    scope:   SignalScope,
    payload: Bytes,
) -> bool {
    let nonce   = fastrand::u64(1..);
    let ts      = crate::hlc::physical_ms(ctx.hlc.current());
    let hlc_seq = ctx.hlc.tick();
    ctx.seen.mark_and_check(nonce, ts);
    let sig = Signal {
        kind: Arc::clone(&kind), scope: scope.clone(),
        payload, sender: ctx.node_id.clone(), nonce,
    };
    let nonce_claimed = match ctx.reply_interceptor.as_ref() {
        Some(claim) => claim(&sig),
        None => false,
    };
    if !nonce_claimed {
        let handler_fill = ctx.signal_handlers.fill_ratio(&kind);
        let combined = handler_fill.max(crate::framing::gossip_shard_fill(&ctx.gossip_txs));
        deliver_locally(&ctx.signal_boundary, &ctx.signal_handlers, &sig, combined);
    }
    let hint = forward_hint(&sig.scope);
    dispatch_gossip_try_send(
        &ctx.gossip_txs,
        WireMessage::Signal {
            ttl: ctx.default_ttl, nonce, sender: ctx.node_id.clone(),
            scope: sig.scope.clone(), kind: sig.kind.clone(), payload: sig.payload.clone(), hlc_seq: Some(hlc_seq),
        },
        ctx.node_id.id_hash(), hint, &ctx.kv_state.dropped_frames,
    )
}

/// Like [`emit_signal`] but awaits gossip channel capacity instead of dropping.
pub async fn emit_signal_async(
    ctx:     &CoreCtx,
    kind:    Arc<str>,
    scope:   SignalScope,
    payload: Bytes,
) -> bool {
    let nonce = fastrand::u64(1..);
    let ts = crate::hlc::physical_ms(ctx.hlc.current());
    ctx.seen.mark_and_check(nonce, ts);
    let handler_fill = ctx.signal_handlers.fill_ratio(&kind);
    let combined = handler_fill.max(crate::framing::gossip_shard_fill(&ctx.gossip_txs));
    deliver_locally(&ctx.signal_boundary, &ctx.signal_handlers, &Signal {
        kind: Arc::clone(&kind), scope: scope.clone(),
        payload: payload.clone(), sender: ctx.node_id.clone(), nonce,
    }, combined);
    let hint = forward_hint(&scope);
    dispatch_gossip_send(
        &ctx.gossip_txs,
        WireMessage::Signal { ttl: ctx.default_ttl, nonce, sender: ctx.node_id.clone(), scope, kind, payload, hlc_seq: None },
        ctx.node_id.id_hash(), hint,
    ).await
}

fn forward_hint(scope: &SignalScope) -> ForwardHint {
    match scope {
        SignalScope::System           => ForwardHint::All,
        SignalScope::Group(name)      => ForwardHint::Group(Arc::clone(name)),
        SignalScope::Individual(peer) => ForwardHint::Individual(peer.clone()),
        SignalScope::Groups(_)        => ForwardHint::All,
    }
}

#[cfg(feature = "metrics")]
fn scope_label(scope: &SignalScope) -> &'static str {
    match scope {
        SignalScope::System        => "system",
        SignalScope::Group(_)      => "group",
        SignalScope::Individual(_) => "node",
        SignalScope::Groups(_)     => "groups",
    }
}

// ── KV primitives usable from typed sub-handles ───────────────────────────────

/// Returns the current value for `key`, or `None` if absent or tombstoned.
pub fn kv_get(ctx: &CoreCtx, key: &str) -> Option<Bytes> {
    ctx.kv_state.store.pin().get(key).and_then(|e| e.data.clone())
}

/// Subscribes to changes for `key`.
pub fn kv_subscribe(ctx: &CoreCtx, key: impl Into<Arc<str>>) -> tokio::sync::watch::Receiver<Option<Bytes>> {
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

/// Subscribes to any write touching keys under `prefix`.
pub fn kv_subscribe_prefix(ctx: &CoreCtx, prefix: impl Into<Arc<str>>) -> tokio::sync::watch::Receiver<u64> {
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

/// Per-subscriber variant: fires only when the prefix matches AND `predicate(&key)` is true.
pub fn kv_subscribe_prefix_with_predicate<P, F>(
    ctx:       &CoreCtx,
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
pub fn kv_scan_prefix(ctx: &CoreCtx, prefix: &str) -> Vec<(Arc<str>, Bytes)> {
    crate::store::scan_kv_prefix(&ctx.kv_state, prefix)
}

/// Rejects a write whose `key + value` cannot fit a single gossip frame. Accepting it
/// would apply locally (and WAL) but never propagate: the per-peer writer cannot frame
/// it and anti-entropy skips entries that don't fit — silent permanent divergence.
/// Returns `true` (= reject) after a `warn!`; callers surface it as `false` ("not queued").
fn reject_oversized_write(key: &str, value_len: usize) -> bool {
    let size = key.len() + value_len;
    if size <= crate::framing::MAX_KV_WRITE_BYTES {
        return false;
    }
    tracing::warn!(
        key, size, limit = crate::framing::MAX_KV_WRITE_BYTES,
        "kv write rejected: key + value cannot fit a gossip frame and would silently \
         diverge; use the bulk transport (bulk_call / bulk_serve) for large payloads"
    );
    true
}

/// Stores `value` under `key`, queues WAL (try-send), applies locally, gossips (try-send).
///
/// Returns `false` without applying anything if `key.len() + value.len()` exceeds
/// [`MAX_KV_WRITE_BYTES`](crate::framing::MAX_KV_WRITE_BYTES) — such a write cannot be
/// encoded into one gossip frame and would otherwise diverge silently.
pub fn kv_set(ctx: &CoreCtx, key: Arc<str>, value: Bytes) -> bool {
    if reject_oversized_write(&key, value.len()) {
        return false;
    }
    let update = make_gossip_update(&ctx.node_id, ctx.default_ttl, key, value, false, &ctx.hlc);
    if let Some(wal) = ctx.wal.get() {
        wal.append_try(sync_entry_from(&update));
    }
    apply_and_notify(&ctx.kv_state, &update);
    #[cfg(feature = "metrics")]
    metrics::counter!("gossip_kv_writes_total").increment(1);
    let tls = ctx.tls.get().map(Arc::as_ref);
    let msg = make_kv_wire_msg(update, ctx.node_id.id_hash(), tls);
    dispatch_gossip_try_send(
        &ctx.gossip_txs, msg,
        ctx.node_id.id_hash(), ForwardHint::All, &ctx.kv_state.dropped_frames,
    )
}

/// Writes `value` under `key`, appends to WAL, applies locally, awaits gossip capacity.
///
/// Returns `false` without applying anything if `key.len() + value.len()` exceeds
/// [`MAX_KV_WRITE_BYTES`](crate::framing::MAX_KV_WRITE_BYTES) — see [`kv_set`].
pub async fn kv_set_async(ctx: &CoreCtx, key: Arc<str>, value: Bytes) -> bool {
    if reject_oversized_write(&key, value.len()) {
        return false;
    }
    let update = make_gossip_update(&ctx.node_id, ctx.default_ttl, key, value, false, &ctx.hlc);
    if let Some(wal) = ctx.wal.get() {
        let _ = wal.append(sync_entry_from(&update)).await;
    }
    apply_and_notify(&ctx.kv_state, &update);
    #[cfg(feature = "metrics")]
    metrics::counter!("gossip_kv_writes_total").increment(1);
    let tls = ctx.tls.get().map(Arc::as_ref);
    let msg = make_kv_wire_msg(update, ctx.node_id.id_hash(), tls);
    dispatch_gossip_send(
        &ctx.gossip_txs, msg,
        ctx.node_id.id_hash(), ForwardHint::All,
    ).await
}

/// Tombstones `key`, queues WAL (try-send), applies locally, gossips (try-send).
pub fn kv_delete(ctx: &CoreCtx, key: Arc<str>) -> bool {
    let update = make_gossip_update(&ctx.node_id, ctx.default_ttl, key, Bytes::new(), true, &ctx.hlc);
    if let Some(wal) = ctx.wal.get() {
        wal.append_try(sync_entry_from(&update));
    }
    apply_and_notify(&ctx.kv_state, &update);
    #[cfg(feature = "metrics")]
    metrics::counter!("gossip_kv_deletes_total").increment(1);
    let tls = ctx.tls.get().map(Arc::as_ref);
    let msg = make_kv_wire_msg(update, ctx.node_id.id_hash(), tls);
    dispatch_gossip_try_send(
        &ctx.gossip_txs, msg,
        ctx.node_id.id_hash(), ForwardHint::All, &ctx.kv_state.dropped_frames,
    )
}

/// Tombstones `key`, appends to WAL, applies locally, awaits gossip capacity.
pub async fn kv_delete_async(ctx: &CoreCtx, key: Arc<str>) -> bool {
    let update = make_gossip_update(&ctx.node_id, ctx.default_ttl, key, Bytes::new(), true, &ctx.hlc);
    if let Some(wal) = ctx.wal.get() {
        let _ = wal.append(sync_entry_from(&update)).await;
    }
    apply_and_notify(&ctx.kv_state, &update);
    #[cfg(feature = "metrics")]
    metrics::counter!("gossip_kv_deletes_total").increment(1);
    let tls = ctx.tls.get().map(Arc::as_ref);
    let msg = make_kv_wire_msg(update, ctx.node_id.id_hash(), tls);
    dispatch_gossip_send(
        &ctx.gossip_txs, msg,
        ctx.node_id.id_hash(), ForwardHint::All,
    ).await
}

/// Returns live members of `group` from Layer I KV (`grp/{group}/`).
pub fn group_members_ctx(ctx: &CoreCtx, group: &str) -> Vec<crate::node_id::NodeId> {
    let prefix = crate::signal::grp_prefix(group);
    kv_scan_prefix(ctx, &prefix)
        .into_iter()
        .filter_map(|(key, _)| {
            key.strip_prefix(&prefix)
                .and_then(|s| s.parse::<crate::node_id::NodeId>().ok())
        })
        .collect()
}
