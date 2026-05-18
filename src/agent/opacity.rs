use crate::framing::{dispatch_gossip_try_send, ForwardHint, GossipUpdate, WireMessage};
use crate::signal::{
    decode_load_state, encode_load_state, kv_ns, LoadState, OpacityHandle, OpacityHint,
    OpacityState,
};
use crate::store::apply_and_notify;
use bytes::Bytes;
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time;

use super::GossipAgent;
use super::helpers::emit_signal;

impl GossipAgent {
    /// Returns all peer load states newer than `max_age`, sorted highest-fill first.
    ///
    /// Each tuple is `(node_id_str, kind_str, LoadState)`. Reads `load/{node}/{kind}`
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
                // Key format: "load/{node_id}/{kind}"
                let tail = key.strip_prefix("load/")?;
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
        let mut raw_rx = self.subscribe(format!("load/{}/{}", node_id, kind));
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

        let signal_handlers  = self.signal_handlers.clone();
        let node_id          = self.node_id.clone();
        let seen             = self.seen.clone();
        let current_ts       = self.current_ts.clone();
        let signal_boundary  = self.signal_boundary.clone();
        let gossip_txs       = self.gossip_txs.clone();
        let default_ttl        = self.config.default_ttl;
        let max_store_entries  = self.config.max_store_entries;
        let dropped_frames   = self.dropped_frames.clone();
        let store            = self.store.clone();
        let subscriptions    = self.subscriptions.clone();
        let prefix_index     = self.prefix_index.clone();

        let clamped_threshold = hint.threshold.clamp(0.4, 0.95);

        let handle = tokio::spawn(async move {
            let mut ticker = time::interval(Duration::from_millis(100));
            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

            // Seed prev_fill from current state so the first-tick trend is meaningful.
            let mut prev_fill = signal_handlers.fill_ratio(&kind);
            let mut is_opaque = false;

            loop {
                tokio::select! { biased;
                    _ = &mut cancel_rx               => break,
                    _ = shutdown_rx.wait_for(|v| *v) => break,
                    _ = ticker.tick() => {
                        let fill_ratio = signal_handlers.fill_ratio(&kind);
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
                                    &node_id, &seen, &current_ts, &signal_boundary,
                                    &signal_handlers, &gossip_txs, default_ttl,
                                    &dropped_frames,
                                    Arc::from(crate::signal::signal_kind::BOUNDARY_OPAQUE),
                                    scope.clone(), hint.payload.clone(),
                                );
                                is_opaque = true;
                                // Write pheromone trail to Layer I so peers observe this
                                // node's load state via `peer_load` / anti-entropy.
                                let written_at_ms = SystemTime::now()
                                    .duration_since(UNIX_EPOCH).unwrap_or_default()
                                    .as_millis() as u64;
                                let load_key: Arc<str> =
                                    Arc::from(format!("load/{}/{}", node_id, kind).as_str());
                                let pheromone_update = GossipUpdate {
                                    nonce:        fastrand::u64(1..),
                                    sender:       node_id.id_hash(),
                                    ttl:          default_ttl,
                                    is_tombstone: false,
                                    timestamp:    written_at_ms,
                                    key:          load_key.clone(),
                                    value:        encode_load_state(&LoadState {
                                        fill_ratio,
                                        is_opaque: true,
                                        written_at_ms,
                                    }),
                                };
                                apply_and_notify(&store, &subscriptions, &pheromone_update, max_store_entries, &prefix_index);
                                dispatch_gossip_try_send(
                                    &gossip_txs, WireMessage::Data(pheromone_update),
                                    node_id.id_hash(), ForwardHint::All, &dropped_frames,
                                );
                            }
                        } else if is_opaque && fill_ratio < eff - hint.hysteresis {
                            emit_signal(
                                &node_id, &seen, &current_ts, &signal_boundary,
                                &signal_handlers, &gossip_txs, default_ttl,
                                &dropped_frames,
                                Arc::from(crate::signal::signal_kind::BOUNDARY_TRANSPARENT),
                                scope.clone(), Bytes::new(),
                            );
                            is_opaque = false;
                            // Tombstone the pheromone trail — immediate evaporation on recovery.
                            let written_at_ms = SystemTime::now()
                                .duration_since(UNIX_EPOCH).unwrap_or_default()
                                .as_millis() as u64;
                            let load_key: Arc<str> =
                                Arc::from(format!("load/{}/{}", node_id, kind).as_str());
                            let tombstone_update = GossipUpdate {
                                nonce:        fastrand::u64(1..),
                                sender:       node_id.id_hash(),
                                ttl:          default_ttl,
                                is_tombstone: true,
                                timestamp:    written_at_ms,
                                key:          load_key.clone(),
                                value:        Bytes::new(),
                            };
                            apply_and_notify(&store, &subscriptions, &tombstone_update, max_store_entries, &prefix_index);
                            dispatch_gossip_try_send(
                                &gossip_txs, WireMessage::Data(tombstone_update),
                                node_id.id_hash(), ForwardHint::All, &dropped_frames,
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

    /// Returns the current fill ratio of handler channels for `kind`.
    ///
    /// `0.0` = all channels empty (boundary fully transparent for this kind).
    /// `1.0` = at least one channel full (boundary fully opaque — signals being shed).
    /// Returns `0.0` when no handlers are registered.
    ///
    /// The value reflects the **most-loaded** registered handler. If any one handler
    /// is saturated, this returns 1.0 even if others still have capacity — consistent
    /// with the opacity shedding model where a fully saturated handler would drop signals.
    pub fn opacity(&self, kind: &str) -> f32 {
        self.signal_handlers.fill_ratio(&Arc::from(kind))
    }
}
