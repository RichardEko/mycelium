use crate::framing::{dispatch_gossip_try_send, make_gossip_update, ForwardHint, WireMessage};
use crate::signal::{
    decode_load_state, encode_load_state, kv_ns, LoadState, OpacityHandle, OpacityHint,
    OpacityState,
};
use crate::store::{apply_and_notify, KvState};
use ahash::AHashSet;
use bytes::Bytes;
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time;

use super::{GossipAgent, TaskCtx};
use super::helpers::emit_signal;

impl GossipAgent {
    /// Returns all peer load states newer than `max_age`, sorted highest-fill first.
    ///
    /// Each tuple is `(node_id_str, kind_str, LoadState)`. Reads `sys/load/{node}/{kind}`
    /// entries from Layer I written by [`manage_opacity`](Self::manage_opacity).
    pub fn peer_load(&self, max_age: Duration) -> Vec<(Arc<str>, Arc<str>, LoadState)> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH).unwrap_or_default()
            .as_millis() as u64;
        let max_age_ms = max_age.as_millis() as u64;
        let mut results: Vec<(Arc<str>, Arc<str>, LoadState)> = self
            .scan_prefix(kv_ns::LOAD)
            .into_iter()
            .filter_map(|(key, bytes)| {
                // Key format: "sys/load/{node_id}/{kind}"
                let tail = key.strip_prefix(kv_ns::LOAD)?;
                let slash = tail.find('/')?;
                let node_str: Arc<str> = Arc::from(&tail[..slash]);
                let kind_str: Arc<str> = Arc::from(&tail[slash + 1..]);
                let state = decode_load_state(&bytes)?;
                if now_ms.saturating_sub(state.written_at_ms) > max_age_ms {
                    return None;
                }
                Some((node_str, kind_str, state))
            })
            .collect();
        results.sort_by(|a, b| b.2.fill_ratio.partial_cmp(&a.2.fill_ratio).unwrap_or(std::cmp::Ordering::Equal));
        results
    }

    /// Returns a `watch::Receiver` that fires whenever `load/{node_id}/{kind}` changes.
    ///
    /// Unlike [`subscribe`](Self::subscribe), the receiver yields decoded [`LoadState`]
    /// values instead of raw bytes — symmetric with [`peer_load`](Self::peer_load).
    /// Fires once on registration with the current value, then on every update from
    /// anti-entropy or a peer's opacity transition. `None` means absent or tombstoned.
    ///
    /// The forwarding task exits automatically when either the underlying store channel
    /// closes (agent shutdown) or all receivers drop (caller abandoned the watch).
    #[must_use]
    pub fn peer_load_rx(
        &self,
        node_id: &crate::node_id::NodeId,
        kind: &str,
    ) -> tokio::sync::watch::Receiver<Option<LoadState>> {
        let mut raw_rx = self.subscribe(format!("{}{}/{}", kv_ns::LOAD, node_id, kind));
        let initial = raw_rx.borrow().as_ref().and_then(decode_load_state);
        let (tx, rx) = tokio::sync::watch::channel(initial);
        tokio::spawn(async move {
            loop {
                if raw_rx.changed().await.is_err() { break; }
                let decoded = raw_rx.borrow().as_ref().and_then(decode_load_state);
                if tx.send(decoded).is_err() { break; }
            }
        });
        rx
    }

    /// Starts an adaptive opacity governor for `kind`.
    ///
    /// The governor samples `kind`'s handler-channel fill ratio every 100 ms and
    /// automatically emits [`BOUNDARY_OPAQUE`](crate::signal_kind::BOUNDARY_OPAQUE) /
    /// [`BOUNDARY_TRANSPARENT`](crate::signal_kind::BOUNDARY_TRANSPARENT) on `scope`
    /// when the fill ratio crosses the adaptive threshold derived from `hint`.
    ///
    /// **Threshold adaptation** — the library clamps `hint.threshold` to `[0.4, 0.95]`
    /// and reduces it by a `trend_factor` when the channel is filling quickly, so the
    /// signal is emitted before the channel saturates rather than after.
    ///
    /// **Hysteresis** — `BOUNDARY_TRANSPARENT` is only emitted once the fill ratio
    /// drops below `effective_threshold − hint.hysteresis`, preventing oscillation at
    /// the boundary.
    ///
    /// **Layer 3 integration** — callers issuing requests via [`request`](Self::request)
    /// should race their request future against a
    /// [`BOUNDARY_OPAQUE`](crate::signal::signal_kind::BOUNDARY_OPAQUE) subscription on the
    /// target node so in-flight requests cancel promptly rather than waiting for the full
    /// timeout when the target saturates. See [`request`](Self::request) for a code example.
    ///
    /// Returns an [`OpacityHandle`] whose drop stops the governor. The task also
    /// exits automatically on [`shutdown`](Self::shutdown).
    pub fn manage_opacity(
        &self,
        kind:  impl Into<Arc<str>>,
        scope: crate::signal::SignalScope,
        hint:  OpacityHint,
    ) -> OpacityHandle {
        self.manage_opacity_impl(kind.into(), scope, hint, None)
    }

    /// Like [`manage_opacity`](Self::manage_opacity) but with an application gate.
    ///
    /// The gate is called with an [`OpacityState`] snapshot on every tick where the
    /// library wants to emit `BOUNDARY_OPAQUE`. Returning `false` defers emission
    /// until the next tick; the library re-asks every tick so the gate stays stateless.
    ///
    /// **Override**: if `fill_ratio == 1.0` (channel completely full) the library
    /// emits regardless of the gate's return value, so a vetoing gate cannot hold the
    /// cluster permanently uninformed about a saturated node.
    pub fn manage_opacity_gated<F>(
        &self,
        kind:  impl Into<Arc<str>>,
        scope: crate::signal::SignalScope,
        hint:  OpacityHint,
        gate:  F,
    ) -> OpacityHandle
    where
        F: Fn(&OpacityState) -> bool + Send + 'static,
    {
        self.manage_opacity_impl(kind.into(), scope, hint, Some(Box::new(gate)))
    }

    #[allow(clippy::type_complexity)]
    fn manage_opacity_impl(
        &self,
        kind:  Arc<str>,
        scope: crate::signal::SignalScope,
        hint:  OpacityHint,
        gate:  Option<Box<dyn Fn(&OpacityState) -> bool + Send + 'static>>,
    ) -> OpacityHandle {
        let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        let ctx: Arc<TaskCtx> = Arc::clone(&self.task_ctx);
        let clamped_threshold = hint.threshold.clamp(0.4, 0.95);

        // Seed opacity state from Layer I so the governor resumes correctly after restart.
        let init_load_key = format!("{}{}/{}", kv_ns::LOAD, ctx.node_id, kind);
        let (init_is_opaque, init_fill) = ctx.kv_state.store.pin()
            .get(&*init_load_key)
            .and_then(|e| e.data.as_ref())
            .and_then(decode_load_state)
            .map(|ls| (ls.is_opaque, ls.fill_ratio))
            .unwrap_or((false, ctx.signal_handlers.fill_ratio(&kind)));

        let handle = tokio::spawn(async move {
            let mut ticker = time::interval(Duration::from_millis(100));
            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

            let mut prev_fill = init_fill;
            let mut is_opaque = init_is_opaque;

            loop {
                tokio::select! { biased;
                    _ = &mut cancel_rx               => break,
                    _ = shutdown_rx.wait_for(|v| *v) => break,
                    _ = ticker.tick() => {
                        let handler_fill = ctx.signal_handlers.fill_ratio(&kind);
                        // Shard backpressure: a saturated gossip shard means signals are
                        // being dropped before they reach any handler. Include it in the
                        // load metric so opacity triggers before handlers see the pressure.
                        let shard_fill: f32 = ctx.gossip_txs.iter()
                            .map(|tx| {
                                let max = tx.max_capacity();
                                if max == 0 { 0.0_f32 } else { 1.0 - tx.capacity() as f32 / max as f32 }
                            })
                            .fold(0.0_f32, f32::max);
                        let fill_ratio   = handler_fill.max(shard_fill);
                        // trend_factor in [0, 0.4]: rising 0.2/tick reduces threshold by 40%.
                        let trend        = fill_ratio - prev_fill;
                        let trend_factor = (trend.max(0.0) * 2.0).min(0.4);
                        let eff          = clamped_threshold * (1.0 - trend_factor);
                        prev_fill        = fill_ratio;

                        let state = OpacityState {
                            fill_ratio,
                            effective_threshold: eff,
                            trend,
                            is_opaque,
                        };

                        if !is_opaque && fill_ratio >= eff {
                            let gate_ok = gate.as_ref()
                                .map(|g| g(&state))
                                .unwrap_or(true);
                            if gate_ok || fill_ratio >= 1.0 {
                                emit_signal(
                                    &ctx,
                                    Arc::from(crate::signal::signal_kind::BOUNDARY_OPAQUE),
                                    scope.clone(), hint.payload.clone(),
                                );
                                is_opaque = true;
                                let written_at_ms = SystemTime::now()
                                    .duration_since(UNIX_EPOCH).unwrap_or_default()
                                    .as_millis() as u64;
                                let load_key: Arc<str> = Arc::from(
                                    format!("{}{}/{}", kv_ns::LOAD, ctx.node_id, kind).as_str()
                                );
                                let upd = make_gossip_update(
                                    &ctx.node_id, ctx.default_ttl, load_key,
                                    encode_load_state(&LoadState {
                                        fill_ratio,
                                        is_opaque: true,
                                        written_at_ms,
                                    }),
                                    false,
                                );
                                apply_and_notify(&ctx.kv_state, &upd);
                                dispatch_gossip_try_send(
                                    &ctx.gossip_txs, WireMessage::Data(upd),
                                    ctx.node_id.id_hash(), ForwardHint::All, &ctx.kv_state.dropped_frames,
                                );
                            }
                        } else if is_opaque && fill_ratio < eff - hint.hysteresis {
                            emit_signal(
                                &ctx,
                                Arc::from(crate::signal::signal_kind::BOUNDARY_TRANSPARENT),
                                scope.clone(), Bytes::new(),
                            );
                            is_opaque = false;
                            // Tombstone the pheromone trail — immediate evaporation on recovery.
                            let load_key: Arc<str> = Arc::from(
                                format!("{}{}/{}", kv_ns::LOAD, ctx.node_id, kind).as_str()
                            );
                            let upd = make_gossip_update(
                                &ctx.node_id, ctx.default_ttl, load_key, Bytes::new(), true,
                            );
                            apply_and_notify(&ctx.kv_state, &upd);
                            dispatch_gossip_try_send(
                                &ctx.gossip_txs, WireMessage::Data(upd),
                                ctx.node_id.id_hash(), ForwardHint::All, &ctx.kv_state.dropped_frames,
                            );
                        }
                    }
                }
            }
        });
        {
            let mut handles = self.task_handles.lock().unwrap_or_else(|e| e.into_inner());
            handles.retain(|h| !h.is_finished());
            handles.push(handle);
        }

        OpacityHandle { _cancel: cancel_tx }
    }

    /// Returns the combined load signal for `kind`.
    ///
    /// `0.0` = all channels empty (boundary fully transparent for this kind).
    /// `1.0` = at least one channel fully saturated.
    /// Returns `0.0` when no handlers are registered and all gossip shards are empty.
    ///
    /// Takes the maximum of the handler-channel fill ratio and the worst gossip-shard
    /// fill ratio, so opacity triggers before signals are shed at the transport layer.
    pub fn opacity(&self, kind: &str) -> f32 {
        let handler_fill = self.task_ctx.signal_handlers.fill_ratio(&Arc::from(kind));
        let shard_fill: f32 = self.task_ctx.gossip_txs.iter()
            .map(|tx| {
                let max = tx.max_capacity();
                if max == 0 { 0.0_f32 } else { 1.0 - tx.capacity() as f32 / max as f32 }
            })
            .fold(0.0_f32, f32::max);
        handler_fill.max(shard_fill)
    }

    /// True if this node's own pheromone trail for `kind` records `is_opaque`.
    pub fn is_opaque(&self, kind: &str) -> bool {
        self.get(&format!("{}{}/{}", kv_ns::LOAD, self.node_id, kind))
            .and_then(|b| decode_load_state(&b))
            .map(|s| s.is_opaque)
            .unwrap_or(false)
    }

    /// Effective load for `kind` — max of the durable pheromone `fill_ratio`
    /// and the live in-memory channel fill. Returns `0.0` when neither has been written.
    pub fn effective_opacity(&self, kind: &str) -> f32 {
        let pheromone = self
            .get(&format!("{}{}/{}", kv_ns::LOAD, self.node_id, kind))
            .and_then(|b| decode_load_state(&b))
            .map(|s| s.fill_ratio)
            .unwrap_or(0.0);
        pheromone.max(self.opacity(kind))
    }

    /// True if `node`'s pheromone trail for `kind` records `is_opaque`
    /// and was written within `max_age`.
    pub fn is_node_opaque(&self, node: &crate::node_id::NodeId, kind: &str, max_age: Duration) -> bool {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        self.get(&format!("{}{}/{}", kv_ns::LOAD, node, kind))
            .and_then(|b| decode_load_state(&b))
            .map(|s| s.is_opaque && now_ms.saturating_sub(s.written_at_ms) <= max_age.as_millis() as u64)
            .unwrap_or(false)
    }

    /// Count of `member_ids` nodes that have any opaque load entry fresher than `max_age`.
    ///
    /// Scans `sys/load/` once and filters by member set, avoiding per-member store lookups.
    /// Used by `group_propose` to shrink the effective quorum when opaque members are absent.
    pub(super) fn count_opaque_members(
        &self,
        member_ids: &AHashSet<String>,
        max_age: Duration,
    ) -> usize {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH).unwrap_or_default()
            .as_millis() as u64;
        let max_age_ms = max_age.as_millis() as u64;
        self.scan_prefix(kv_ns::LOAD)
            .into_iter()
            .filter(|(k, bytes)| {
                let tail = k.strip_prefix(kv_ns::LOAD).unwrap_or("");
                let slash = tail.find('/').unwrap_or(tail.len());
                member_ids.contains(&tail[..slash])
                    && decode_load_state(bytes)
                        .map(|s| s.is_opaque
                            && now_ms.saturating_sub(s.written_at_ms) <= max_age_ms)
                        .unwrap_or(false)
            })
            .count()
    }

    /// Count of all nodes that have any opaque load entry fresher than `max_age`.
    ///
    /// Scans `sys/load/` once without member filtering.
    /// Used by `system_propose` to shrink the effective quorum when opaque nodes are absent.
    pub(super) fn count_opaque_system(&self, max_age: Duration) -> usize {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH).unwrap_or_default()
            .as_millis() as u64;
        let max_age_ms = max_age.as_millis() as u64;
        self.scan_prefix(kv_ns::LOAD)
            .into_iter()
            .filter(|(_, bytes)| {
                decode_load_state(bytes)
                    .map(|s| s.is_opaque
                        && now_ms.saturating_sub(s.written_at_ms) <= max_age_ms)
                    .unwrap_or(false)
            })
            .count()
    }
}

// ── Free helpers for opaque-count callbacks ───────────────────────────────────

/// Counts group members with any opaque load entry fresher than `max_age_ms`.
///
/// Used by `group_propose` to build the mid-ballot opaque-recompute callback so that
/// the `propose()` function (Layer III) doesn't read `KvState` directly.
pub(super) fn count_opaque_members_in_kv(
    kv_state:   &KvState,
    member_ids: &ahash::AHashSet<String>,
    max_age_ms: u64,
    now_ms:     u64,
) -> usize {
    let prefix = kv_ns::LOAD;
    let seg = prefix.find('/').map_or(prefix, |i| &prefix[..i]);
    let store = kv_state.store.pin();
    let idx   = kv_state.prefix_index.pin();

    let is_opaque_member = |key: &Arc<str>| -> bool {
        if !key.starts_with(prefix) { return false; }
        let tail = key.strip_prefix(prefix).unwrap_or("");
        let slash = tail.find('/').unwrap_or(tail.len());
        if !member_ids.contains(&tail[..slash]) { return false; }
        store.get(key.as_ref())
            .and_then(|e| e.data.as_ref().and_then(decode_load_state))
            .map(|s| s.is_opaque && now_ms.saturating_sub(s.written_at_ms) <= max_age_ms)
            .unwrap_or(false)
    };

    if let Some(bucket) = idx.get(seg) {
        bucket.pin().iter().filter(|(k, _)| is_opaque_member(k)).count()
    } else {
        store.iter()
            .filter(|(k, v)| k.starts_with(prefix) && v.data.is_some() && is_opaque_member(k))
            .count()
    }
}

/// Returns `true` if this node has any `sys/load/{node_id}/*` entry marking `is_opaque`.
///
/// Encapsulates the Layer I prefix scan here in Layer II (opacity.rs) so that
/// `ConsensusEngine` (Layer III) does not read `KvState` directly for this query.
pub(crate) fn is_self_opaque(kv_state: &KvState, node_id: &crate::node_id::NodeId) -> bool {
    let load_prefix = format!("{}{}/", kv_ns::LOAD, node_id);
    let seg = kv_ns::LOAD.split_once('/').map_or(kv_ns::LOAD, |(s, _)| s);
    let idx = kv_state.prefix_index.pin();
    idx.get(seg).map(|bucket| {
        let store = kv_state.store.pin();
        bucket.pin().iter()
            .filter(|(k, _)| k.starts_with(&*load_prefix))
            .any(|(k, _)| store.get(k.as_ref())
                .and_then(|e| e.data.as_ref().and_then(decode_load_state))
                .map(|s| s.is_opaque)
                .unwrap_or(false)
            )
    }).unwrap_or(false)
}

/// Counts all nodes with any opaque load entry fresher than `max_age_ms`.
///
/// Used by `system_propose` to build the mid-ballot opaque-recompute callback.
pub(super) fn count_opaque_all_in_kv(
    kv_state:   &KvState,
    max_age_ms: u64,
    now_ms:     u64,
) -> usize {
    let prefix = kv_ns::LOAD;
    let seg = prefix.find('/').map_or(prefix, |i| &prefix[..i]);
    let store = kv_state.store.pin();
    let idx   = kv_state.prefix_index.pin();

    let is_opaque = |key: &Arc<str>| -> bool {
        if !key.starts_with(prefix) { return false; }
        store.get(key.as_ref())
            .and_then(|e| e.data.as_ref().and_then(decode_load_state))
            .map(|s| s.is_opaque && now_ms.saturating_sub(s.written_at_ms) <= max_age_ms)
            .unwrap_or(false)
    };

    if let Some(bucket) = idx.get(seg) {
        bucket.pin().iter().filter(|(k, _)| is_opaque(k)).count()
    } else {
        store.iter()
            .filter(|(k, v)| k.starts_with(prefix) && v.data.is_some() && is_opaque(k))
            .count()
    }
}
