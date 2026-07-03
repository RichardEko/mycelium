use crate::framing::{dispatch_gossip_try_send, make_gossip_update, ForwardHint, WireMessage};
use crate::signal::{
    decode_load_state, encode_load_state, kv_ns, LoadState, OpacityHandle, OpacityHint,
    OpacityState,
};
use crate::store::{apply_and_notify, KvState};
use bytes::Bytes;
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time;

use super::{GossipAgent, TaskCtx};
use super::helpers::{emit_signal, kv_get, kv_scan_prefix};

impl GossipAgent {
    /// Returns all peer load states newer than `max_age`, sorted highest-fill first.
    pub fn peer_load(&self, max_age: Duration) -> Vec<(Arc<str>, Arc<str>, LoadState)> {
        self.capabilities().peer_load(max_age)
    }

    /// Returns a `watch::Receiver` that fires whenever `load/{node_id}/{kind}` changes.
    #[must_use]
    pub fn peer_load_rx(
        &self,
        node_id: &crate::node_id::NodeId,
        kind: &str,
    ) -> tokio::sync::watch::Receiver<Option<LoadState>> {
        self.capabilities().peer_load_rx(node_id, kind)
    }

    /// Starts an adaptive opacity governor for `kind`.
    pub fn manage_opacity(
        &self,
        kind:  impl Into<Arc<str>>,
        scope: crate::signal::SignalScope,
        hint:  OpacityHint,
    ) -> OpacityHandle {
        self.capabilities().manage_opacity(kind, scope, hint)
    }

    /// Like [`manage_opacity`](Self::manage_opacity) but with an application gate.
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
        self.capabilities().manage_opacity_gated(kind, scope, hint, gate)
    }

    /// Returns the combined load signal for `kind`.
    pub fn opacity(&self, kind: &str) -> f32 {
        self.capabilities().opacity(kind)
    }

    /// True if this node's own pheromone trail for `kind` records `is_opaque`.
    pub fn is_opaque(&self, kind: &str) -> bool {
        self.capabilities().is_opaque(kind)
    }

    /// Effective load for `kind` — max of the durable pheromone `fill_ratio`
    /// and the live in-memory channel fill.
    pub fn effective_opacity(&self, kind: &str) -> f32 {
        self.capabilities().effective_opacity(kind)
    }

    /// True if `node`'s pheromone trail for `kind` records `is_opaque`
    /// and was written within `max_age`.
    pub fn is_node_opaque(&self, node: &crate::node_id::NodeId, kind: &str, max_age: Duration) -> bool {
        self.capabilities().is_node_opaque(node, kind, max_age)
    }
}

// ── Free helpers for ConsensusHandle / opacity ctx ───────────────────────────

/// Returns the combined load signal for `kind` via a `TaskCtx` reference.
/// Used by `ConsensusHandle` methods that don't have a `GossipAgent` reference.
pub(super) fn opacity_ctx(ctx: &TaskCtx, kind: &str) -> f32 {
    let handler_fill = ctx.signal_handlers.fill_ratio(&Arc::from(kind));
    handler_fill.max(crate::framing::gossip_shard_fill(&ctx.gossip_txs))
}

/// Effective load for `kind` via a `TaskCtx` reference — max of durable pheromone
/// and live in-memory channel fill.
pub(super) fn effective_opacity_ctx(ctx: &TaskCtx, kind: &str) -> f32 {
    let load_key = format!("{}{}/{}", kv_ns::LOAD, ctx.node_id, kind);
    let pheromone = kv_get(ctx, &load_key)
        .and_then(|b| crate::signal::decode_load_state(&b))
        .map(|s| s.fill_ratio)
        .unwrap_or(0.0);
    pheromone.max(opacity_ctx(ctx, kind))
}

/// Count of `member_ids` nodes that have any opaque load entry fresher than `max_age`
/// via a `TaskCtx` reference.
#[cfg(feature = "consensus")]
pub(super) fn count_opaque_members_ctx(
    ctx:        &TaskCtx,
    member_ids: &ahash::AHashSet<String>,
    max_age:    std::time::Duration,
) -> usize {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap_or_default()
        .as_millis() as u64;
    let max_age_ms = max_age.as_millis() as u64;
    kv_scan_prefix(ctx, kv_ns::LOAD)
        .into_iter()
        .filter(|(k, bytes)| {
            let tail = k.strip_prefix(kv_ns::LOAD).unwrap_or("");
            let slash = tail.find('/').unwrap_or(tail.len());
            member_ids.contains(&tail[..slash])
                && crate::signal::decode_load_state(bytes)
                    .map(|s| s.is_opaque
                        && now_ms.saturating_sub(s.written_at_ms) <= max_age_ms)
                    .unwrap_or(false)
        })
        .count()
}

/// Count of all nodes with any opaque load entry fresher than `max_age`
/// via a `TaskCtx` reference.
#[cfg(feature = "consensus")]
pub(super) fn count_opaque_system_ctx(ctx: &TaskCtx, max_age: std::time::Duration) -> usize {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap_or_default()
        .as_millis() as u64;
    let max_age_ms = max_age.as_millis() as u64;
    kv_scan_prefix(ctx, kv_ns::LOAD)
        .into_iter()
        .filter(|(_, bytes)| {
            crate::signal::decode_load_state(bytes)
                .map(|s| s.is_opaque && now_ms.saturating_sub(s.written_at_ms) <= max_age_ms)
                .unwrap_or(false)
        })
        .count()
}

/// Returns all peer load states newer than `max_age` via a `TaskCtx` reference.
pub(super) fn peer_load_ctx(
    ctx:     &TaskCtx,
    max_age: std::time::Duration,
) -> Vec<(Arc<str>, Arc<str>, crate::signal::LoadState)> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap_or_default()
        .as_millis() as u64;
    let max_age_ms = max_age.as_millis() as u64;
    let mut results: Vec<(Arc<str>, Arc<str>, crate::signal::LoadState)> =
        kv_scan_prefix(ctx, kv_ns::LOAD)
            .into_iter()
            .filter_map(|(key, bytes)| {
                let tail = key.strip_prefix(kv_ns::LOAD)?;
                let slash = tail.find('/')?;
                let node_str: Arc<str> = Arc::from(&tail[..slash]);
                let kind_str: Arc<str> = Arc::from(&tail[slash + 1..]);
                let state = crate::signal::decode_load_state(&bytes)?;
                if now_ms.saturating_sub(state.written_at_ms) > max_age_ms {
                    return None;
                }
                Some((node_str, kind_str, state))
            })
            .collect();
    results.sort_by(|a, b| b.2.fill_ratio.partial_cmp(&a.2.fill_ratio).unwrap_or(std::cmp::Ordering::Equal));
    results
}

// ── Free helpers for opaque-count callbacks ───────────────────────────────────

/// Counts group members with any opaque load entry fresher than `max_age_ms`.
///
/// Used by `group_propose` to build the mid-ballot opaque-recompute callback so that
/// the `propose()` function (Layer III) doesn't read `KvState` directly.
#[cfg(feature = "consensus")]
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
#[cfg(feature = "consensus")]
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

/// Free-function variant of `manage_opacity_impl` — no application gate.
pub(super) fn manage_opacity_ctx(
    ctx:  &Arc<TaskCtx>,
    kind: Arc<str>,
    scope: crate::signal::SignalScope,
    hint: OpacityHint,
) -> OpacityHandle {
    manage_opacity_gated_ctx(ctx, kind, scope, hint, None::<fn(&OpacityState) -> bool>)
}

/// The boundary transition an opacity-governor tick decides on. Extracted from the async tick loop
/// so the veto / library-override / hysteresis-clear decision is a **pure, deterministic** function —
/// testable without spawning the governor, whose 100 ms `time::interval` ticker starves under CI
/// parallel-test saturation (the source of a *recurring* flake: the integration test that waited on
/// the async emission was widened 3 s → 10 s → 30 s and still flaked twice). The invariant now lives
/// on the pure path; the async path is a wiring smoke.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpacityTransition {
    /// Emit `BOUNDARY_OPAQUE` — the node starts shedding.
    GoOpaque,
    /// Emit `BOUNDARY_TRANSPARENT` — the node stops shedding.
    GoTransparent,
    /// No boundary change this tick.
    Hold,
}

/// **Pure** — the trend-adjusted [`OpacityState`] for a tick, given the current + previous fill and
/// the already-`[0.4, 0.95]`-clamped threshold. A rising fill lowers the effective threshold
/// (trend adaptation); a falling one does not.
fn opacity_state_for(is_opaque: bool, fill_ratio: f32, prev_fill: f32, clamped_threshold: f32) -> OpacityState {
    let trend = fill_ratio - prev_fill;
    let trend_factor = (trend.max(0.0) * 2.0).min(0.4);
    let eff = clamped_threshold * (1.0 - trend_factor);
    OpacityState { fill_ratio, effective_threshold: eff, trend, is_opaque }
}

/// **Pure** — decide the boundary transition. The library **overrides** a vetoing gate once
/// `fill_ratio >= 1.0` (an unconditional shed at a full channel); below full, a `false` gate holds
/// the boundary transparent. Clearing requires fill to fall a full `hysteresis` below the effective
/// threshold (anti-oscillation).
fn opacity_transition(state: &OpacityState, gate_ok: bool, hysteresis: f32) -> OpacityTransition {
    if !state.is_opaque && state.fill_ratio >= state.effective_threshold {
        if gate_ok || state.fill_ratio >= 1.0 {
            OpacityTransition::GoOpaque
        } else {
            OpacityTransition::Hold
        }
    } else if state.is_opaque && state.fill_ratio < state.effective_threshold - hysteresis {
        OpacityTransition::GoTransparent
    } else {
        OpacityTransition::Hold
    }
}

/// Like [`manage_opacity_ctx`] but with a generic gate predicate (monomorphized, no vtable).
pub(super) fn manage_opacity_gated_ctx<F>(
    ctx:  &Arc<TaskCtx>,
    kind: Arc<str>,
    scope: crate::signal::SignalScope,
    hint: OpacityHint,
    gate: Option<F>,
) -> OpacityHandle
where
    F: Fn(&OpacityState) -> bool + Send + 'static,
{
    let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
    let mut shutdown_rx = ctx.shutdown_tx.subscribe();
    let ctx = Arc::clone(ctx);
    let clamped_threshold = hint.threshold.clamp(0.4, 0.95);
    // Pre-compute the KV key once; cloning Arc<str> is a single atomic increment.
    let load_key: Arc<str> = Arc::from(format!("{}{}/{}", kv_ns::LOAD, ctx.node_id, kind).as_str());
    let init_load_key = Arc::clone(&load_key);
    let (init_is_opaque, init_fill) = ctx.kv_state.store.pin()
        .get(&*init_load_key)
        .and_then(|e| e.data.as_ref())
        .and_then(decode_load_state)
        .map(|ls| (ls.is_opaque, ls.fill_ratio))
        .unwrap_or((false, ctx.signal_handlers.fill_ratio(&kind)));
    let spawn_ctx = Arc::clone(&ctx);
    spawn_ctx.spawn_task(async move {
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
                    let fill_ratio = handler_fill.max(crate::framing::gossip_shard_fill(&ctx.gossip_txs));
                    let state = opacity_state_for(is_opaque, fill_ratio, prev_fill, clamped_threshold);
                    prev_fill = fill_ratio;
                    let gate_ok = gate.as_ref().map(|g| g(&state)).unwrap_or(true);
                    match opacity_transition(&state, gate_ok, hint.hysteresis) {
                        OpacityTransition::GoOpaque => {
                            emit_signal(&ctx, Arc::from(crate::signal::signal_kind::BOUNDARY_OPAQUE), scope.clone(), hint.payload.clone());
                            is_opaque = true;
                            let written_at_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
                            let upd = make_gossip_update(&ctx.node_id, ctx.default_ttl, Arc::clone(&load_key), encode_load_state(&LoadState { fill_ratio, is_opaque: true, written_at_ms }), false, &ctx.hlc);
                            apply_and_notify(&ctx.kv_state, &upd);
                            dispatch_gossip_try_send(&ctx.gossip_txs, WireMessage::Data(upd), ctx.node_id.id_hash(), ForwardHint::All, &ctx.kv_state.dropped_frames);
                        }
                        OpacityTransition::GoTransparent => {
                            emit_signal(&ctx, Arc::from(crate::signal::signal_kind::BOUNDARY_TRANSPARENT), scope.clone(), Bytes::new());
                            is_opaque = false;
                            let upd = make_gossip_update(&ctx.node_id, ctx.default_ttl, Arc::clone(&load_key), Bytes::new(), true, &ctx.hlc);
                            apply_and_notify(&ctx.kv_state, &upd);
                            dispatch_gossip_try_send(&ctx.gossip_txs, WireMessage::Data(upd), ctx.node_id.id_hash(), ForwardHint::All, &ctx.kv_state.dropped_frames);
                        }
                        OpacityTransition::Hold => {}
                    }
                }
            }
        }
    });
    OpacityHandle { _cancel: cancel_tx }
}

#[cfg(test)]
mod tests {
    use crate::signal::{Signal, SignalHandlers, SignalScope};
    use crate::{GossipAgent, GossipConfig, NodeId};
    use bytes::Bytes;
    use std::{sync::Arc, time::Duration};

    fn make_agent() -> GossipAgent {
        GossipAgent::new(NodeId::new("127.0.0.1", 0).unwrap(), GossipConfig::default())
    }

    // ── The opacity decision, tested purely (no async governor, no ticker, no timeout). These are
    //    the deterministic authoritative gates for the veto/override + clear invariants that the
    //    integration tests (`test_manage_opacity_gate_…`) exercise through the flaky async path.

    #[test]
    fn opacity_gate_vetoes_below_full_then_the_library_overrides_at_full() {
        use super::{opacity_state_for, opacity_transition, OpacityTransition};
        let veto = |_: &crate::signal::OpacityState| false;

        // Below full (fill == threshold == 0.75, no trend ⇒ eff 0.75): threshold met, but the gate
        // vetoes and fill < 1.0 ⇒ Hold (the boundary stays transparent).
        let s = opacity_state_for(false, 0.75, 0.75, 0.75);
        assert_eq!(opacity_transition(&s, veto(&s), 0.20), OpacityTransition::Hold);

        // At full (fill == 1.0): the library overrides the veto ⇒ GoOpaque (unconditional shed).
        let s = opacity_state_for(false, 1.0, 0.75, 0.75);
        assert_eq!(opacity_transition(&s, veto(&s), 0.20), OpacityTransition::GoOpaque);

        // Gate open + above threshold (below full): GoOpaque, no override needed.
        let s = opacity_state_for(false, 0.80, 0.80, 0.75);
        assert_eq!(opacity_transition(&s, true, 0.20), OpacityTransition::GoOpaque);

        // Below threshold with an open gate: nothing to do.
        let s = opacity_state_for(false, 0.50, 0.50, 0.75);
        assert_eq!(opacity_transition(&s, true, 0.20), OpacityTransition::Hold);
    }

    #[test]
    fn opacity_clears_only_after_fill_falls_a_full_hysteresis_below_threshold() {
        use super::{opacity_state_for, opacity_transition, OpacityTransition};
        // Draining (no positive trend) ⇒ eff = 0.75; the clear line is 0.75 - 0.20 = 0.55.
        let just_inside = opacity_state_for(true, 0.60, 1.0, 0.75); // 0.60 > 0.55 ⇒ still opaque
        assert_eq!(opacity_transition(&just_inside, true, 0.20), OpacityTransition::Hold);
        let past = opacity_state_for(true, 0.50, 1.0, 0.75);        // 0.50 < 0.55 ⇒ clear
        assert_eq!(opacity_transition(&past, true, 0.20), OpacityTransition::GoTransparent);
    }

    /// **Probe — Semantic Correctness (analysis Run 31, the library-override boundary).** The
    /// decision/scheduling split (which fixed the recurring flake) must not have shifted the
    /// semantics: the library override of a vetoing gate triggers at `fill >= 1.0` and *not a hair
    /// below*. Falsifies an off-by-epsilon that the extraction could have introduced.
    #[test]
    fn probe_r31_opacity_override_boundary_is_exactly_at_full() {
        use super::{opacity_state_for, opacity_transition, OpacityTransition};
        // 0.999 with a vetoing gate: below full ⇒ Hold (the gate is respected, no override).
        let s = opacity_state_for(false, 0.999, 0.999, 0.75);
        assert_eq!(opacity_transition(&s, false, 0.20), OpacityTransition::Hold,
            "no override a hair below full");
        // Exactly 1.0: the library overrides the veto ⇒ GoOpaque.
        let s = opacity_state_for(false, 1.0, 1.0, 0.75);
        assert_eq!(opacity_transition(&s, false, 0.20), OpacityTransition::GoOpaque,
            "override triggers exactly at a full channel");
    }

    #[test]
    fn opacity_zero_when_channel_empty() {
        let handlers = SignalHandlers::new(Duration::from_secs(600));
        let kind: Arc<str> = Arc::from("probe");
        let _rx = handlers.register_with_capacity(Arc::clone(&kind), 8);
        assert_eq!(handlers.fill_ratio(&kind), 0.0);
    }

    #[test]
    fn opacity_one_when_channel_full() {
        let handlers = SignalHandlers::new(Duration::from_secs(600));
        let kind: Arc<str> = Arc::from("probe");
        let _rx = handlers.register_with_capacity(Arc::clone(&kind), 4);
        let sender = NodeId::new("127.0.0.1", 1).unwrap();
        let sig = Signal {
            kind: Arc::clone(&kind),
            scope: SignalScope::System,
            payload: Bytes::new(),
            sender,
            nonce: 1,
        };
        for _ in 0..4 {
            handlers.deliver(&sig);
        }
        assert_eq!(handlers.fill_ratio(&kind), 1.0);
    }

    #[tokio::test]
    async fn individual_scope_bypasses_opacity() {
        let node_id = NodeId::new("127.0.0.1", 1).unwrap();
        let agent = GossipAgent::new(node_id.clone(), GossipConfig::default());
        let mut agent_rx = agent.mesh().signal_rx_with_capacity("invoke", 4);
        let admitted = agent.mesh().emit("invoke", SignalScope::Individual(node_id.clone()), Bytes::from_static(b"req"));
        let _ = admitted;
        let result = tokio::time::timeout(Duration::from_millis(100), agent_rx.recv()).await;
        assert!(result.is_ok() && result.unwrap().is_some(),
            "Individual signal must be delivered regardless of opacity");
    }

    #[test]
    fn opacity_exposed_on_agent() {
        let agent = make_agent();
        assert_eq!(agent.opacity("task"), 0.0, "no handler: fully transparent");
        let _rx = agent.mesh().signal_rx_with_capacity("task", 1);
        assert_eq!(agent.opacity("task"), 0.0, "empty channel: fully transparent");
        let _ = agent.mesh().emit("task", SignalScope::System, Bytes::new());
        assert_eq!(agent.opacity("task"), 1.0, "full channel: fully opaque");
    }
}
