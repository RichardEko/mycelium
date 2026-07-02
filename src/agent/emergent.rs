//! Emergent-condition detectors (Legible Emergence, **Phase 1**) — coordinator-free
//! diagnosability at the cluster/temporal stratum, the sibling of the node-local `/stats`
//! tripwires (`commit_conflicts`, `sys_namespace_violations`, …).
//!
//! Design: `docs/design/legible-emergence-taxonomy.md`; plan:
//! `docs/plans/legible-emergence.md`. Phase-1 posture:
//!
//! - **No collector, no fan-out.** Every detector is a **node-local scan of the gossiped KV this
//!   node already holds** (KV floods the cluster). Any node computes it; killing any node loses
//!   nothing. Tier-(b) of the taxonomy.
//! - **A diagnostic is a per-node best-effort *estimate*, not fleet ground truth** (RT1/RT2). Every
//!   result is paired with a [`ViewConfidence`] — `peers_heard ≪ peers_known` is the node telling
//!   you its view is partial (it may be the partitioned one).
//! - **Detection, not prevention.** Detectors *name* a pathology (a `/stats` gauge); they never
//!   correct it. Mirrors the commit-conflict / `sys/`-namespace tripwire posture.
//! - **Zero overhead when off.** The loop is spawned only under `emergent_detectors_enabled`.
//!
//! Phase 1 ships **P1 — governed-group conflict** (the #56 governor-vs-autojoin condition:
//! governor intent bounds vs observed `grp/` membership). P2–P4/P6 land as further detectors of the
//! same shape.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use serde::Serialize;

use super::TaskCtx;
use super::capability_ops::{
    now_ms, parse_cap_key_or_warn, resolve_filter_against_kv, scan_prefix_kv, scan_prefix_kv_with_ts,
};
use super::membership_governor::{
    group_members, MembershipIntent, MEMBERSHIP_INTENT_TTL_MS, MEMBERSHIP_PREFIX,
};
use crate::capability::ReqEntry;

/// Detector tick interval.
const DETECTOR_INTERVAL: Duration = Duration::from_secs(2);
/// Consecutive ticks a condition must hold before it is a *confirmed* pathology — the hysteresis /
/// false-positive guard (taxonomy §3: sustained, not a transient during governor convergence).
/// At 2 s/tick, `CONFIRM_TICKS = 2` ≈ 4 s sustained.
const CONFIRM_TICKS: u32 = 2;
/// A peer whose last-seen is older than this is not counted as "heard" for [`ViewConfidence`].
const HEARD_WINDOW: Duration = Duration::from_secs(30);

/// This node's **best-effort estimate of its own view health** (RT1/RT2) — attached to every
/// diagnostic so a consumer never mistakes a local estimate for fleet ground truth. During a
/// partition or opacity storm `peers_heard ≪ peers_known` (or a large `max_staleness_ms`) is the
/// node self-labelling its partial view. Phase-1 fields; Phase 2 may add true HLC skew + last-AE.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ViewConfidence {
    /// Whose local view this is.
    pub observer:         String,
    /// Peers in this node's roster.
    pub peers_known:      usize,
    /// Peers this node has heard from within [`HEARD_WINDOW`] (the "am I hearing the fleet" signal).
    pub peers_heard:      usize,
    /// Age (ms) of the stalest *heard* peer's last contact — a view-staleness proxy.
    pub max_staleness_ms: u64,
    /// Is the observer itself opaque/shedding (its own inputs may be degraded)?
    pub self_degraded:    bool,
}

/// A detected **governed-group conflict** (P1, the #56 condition): observed live membership outside
/// the governor's intended `[min, max]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupConflict {
    pub group:    String,
    pub observed: usize,
    pub min:      usize,
    pub max:      Option<usize>,
}

/// **Pure P1 detector** — scan the membership-governor intents this node holds and flag any group
/// whose live `grp/` member count is outside the intent's `[min, max]`. A node-local KV scan; no
/// fan-out; unit-testable without a live cluster.
///
/// **RT3 evaporation tolerance:** only *fresh* intents count (`now − written_at_ms ≤
/// MEMBERSHIP_INTENT_TTL_MS`). A governor that has gone away — its intent evaporating — produces no
/// phantom conflict; and a genuinely-departed provider is only asserted "gone" after its `grp/`
/// entry lapses, the same evaporation the governor itself respects.
pub fn detect_governed_group_conflicts(kv_state: &crate::store::KvState, now: u64) -> Vec<GroupConflict> {
    let mut out = Vec::new();
    for (_key, bytes) in scan_prefix_kv(kv_state, MEMBERSHIP_PREFIX) {
        let Ok(intent) = mycelium_core::serde_fixint::from_slice::<MembershipIntent>(&bytes) else {
            continue;
        };
        if now.saturating_sub(intent.written_at_ms) > MEMBERSHIP_INTENT_TTL_MS {
            continue; // RT3: evaporated intent ⇒ no governor ⇒ no conflict
        }
        let observed = group_members(kv_state, &intent.group).len();
        let under = observed < intent.min;
        let over = intent.max.is_some_and(|mx| observed > mx);
        if under || over {
            out.push(GroupConflict { group: intent.group, observed, min: intent.min, max: intent.max });
        }
    }
    out
}

/// **Pure hysteresis** (generic) — fold a per-tick set of *keyed* pathologies into *confirmed*
/// ones. A key must be present for `confirm_ticks` consecutive ticks to count (the false-positive
/// guard: a transient while the governor/fleet converges is not a pathology). Keys that recover are
/// pruned from `streaks`. Deduplicates the input. Returns the count of confirmed keys. Shared by the
/// stateful detectors (P1 conflicts, P6 coverage gaps).
pub fn confirm_by_key(keys: &[String], streaks: &mut HashMap<String, u32>, confirm_ticks: u32) -> u64 {
    let current: HashSet<&str> = keys.iter().map(|s| s.as_str()).collect();
    streaks.retain(|k, _| current.contains(k.as_str())); // a recovered key resets its streak
    let mut confirmed = 0u64;
    for k in &current {
        let n = streaks.entry((*k).to_string()).or_insert(0);
        *n = n.saturating_add(1);
        if *n >= confirm_ticks {
            confirmed += 1;
        }
    }
    confirmed
}

/// P1 hysteresis over [`GroupConflict`]s — thin wrapper over [`confirm_by_key`].
pub fn confirm_conflicts(
    raw: &[GroupConflict],
    streaks: &mut HashMap<String, u32>,
    confirm_ticks: u32,
) -> u64 {
    let keys: Vec<String> = raw.iter().map(|c| c.group.clone()).collect();
    confirm_by_key(&keys, streaks, confirm_ticks)
}

/// **Pure P6 detector** — capability-coverage gaps. For each *fresh* `req/` requirement this node
/// holds, resolve its `CapFilter` against fresh `cap/` providers; a requirement with **zero fresh
/// providers** is a gap. Returns the deduplicated set of uncovered capability ids (`{ns}/{name}`) —
/// a gap is a property of the *capability*, not of each requirer. Node-local KV scan; unit-testable.
///
/// **RT3 flagship:** "retracted" and "merely unheard / GC-paused / partitioned" are *identical* in
/// local KV (both = no fresh `cap/` key). So this instantaneous detector must be paired with the
/// loop's hysteresis (a gap is only *confirmed* after `CONFIRM_TICKS`, past a provider's refresh),
/// and the result names "no provider **visible from here**," never "no provider exists" — read it
/// beside [`compute_view_confidence`].
pub fn detect_coverage_gaps(kv_state: &crate::store::KvState, now: u64) -> Vec<String> {
    let mut gaps: HashSet<String> = HashSet::new();
    for (key, bytes, hlc_ts) in scan_prefix_kv_with_ts(kv_state, "req/") {
        let Some((_node, ns, name)) = parse_cap_key_or_warn("req/", &key) else { continue };
        let Some(req) = ReqEntry::decode(&bytes) else { continue };
        if !req.is_fresh(hlc_ts, now) {
            continue; // only live requirements — a crashed requirer's declaration ages out
        }
        if resolve_filter_against_kv(kv_state, &req.filter).is_empty() {
            gaps.insert(format!("{ns}/{name}"));
        }
    }
    gaps.into_iter().collect()
}

/// A peer opacity entry older than this is not counted as live for the storm gauge (P4).
const OPAQUE_MAX_AGE_MS: u64 = 30_000;

/// **Pure P4 detector** — the fraction (integer percent, 0–100) of live nodes currently shedding
/// (opaque). A fleet-wide **opacity storm** (correlated shed / pheromone runaway) shows here. Scans
/// `sys/load/`, counts *distinct* nodes with a **fresh** `is_opaque` entry, over `live_nodes`
/// (the caller's roster + self). Node-local KV scan; unit-testable.
///
/// **RT2 flagship:** a storm degrades the very gossip this count relies on, so the result is an
/// explicitly view-confidence-qualified *estimate* — always read it beside [`compute_view_confidence`]
/// (`peers_heard ≪ peers_known` ⇒ the fraction may be undercounted, because the observer is itself
/// starved). It is a **raw gauge, not a flag**: the operator thresholds it (library-not-platform —
/// heavy alerting is their stack), so no contestable in-code bound is baked here.
pub fn opaque_node_pct(kv_state: &crate::store::KvState, live_nodes: usize, now: u64, max_age_ms: u64) -> u64 {
    let mut opaque: HashSet<String> = HashSet::new();
    for (key, bytes) in scan_prefix_kv(kv_state, crate::signal::kv_ns::LOAD) {
        let tail = key.strip_prefix(crate::signal::kv_ns::LOAD).unwrap_or("");
        let node = &tail[..tail.find('/').unwrap_or(tail.len())];
        if let Some(s) = crate::signal::decode_load_state(&bytes)
            && s.is_opaque
            && now.saturating_sub(s.written_at_ms) <= max_age_ms
        {
            opaque.insert(node.to_string());
        }
    }
    if live_nodes == 0 {
        return 0;
    }
    ((opaque.len() as u64 * 100) / live_nodes as u64).min(100)
}

/// Compute the current opacity-storm gauge (P4) — cheap, on-demand (called by `/stats`). The
/// denominator is this node's roster + self; RT3 evaporation is honoured via [`OPAQUE_MAX_AGE_MS`].
pub fn compute_opaque_node_pct(ctx: &TaskCtx) -> u64 {
    let live_nodes = ctx.peers.pin().len() + 1; // peers + self
    opaque_node_pct(&ctx.kv_state, live_nodes, now_ms(), OPAQUE_MAX_AGE_MS)
}

/// Compute this node's current [`ViewConfidence`] — cheap, on-demand (called by `/stats`).
pub fn compute_view_confidence(ctx: &TaskCtx) -> ViewConfidence {
    let guard = ctx.peers.pin();
    let peers_known = guard.len();
    let mut peers_heard = 0usize;
    let mut max_staleness_ms = 0u64;
    for (_id, last_seen) in guard.iter() {
        let age = last_seen.elapsed();
        if age <= HEARD_WINDOW {
            peers_heard += 1;
            max_staleness_ms = max_staleness_ms.max(age.as_millis() as u64);
        }
    }
    ViewConfidence {
        observer: ctx.node_id.to_string(),
        peers_known,
        peers_heard,
        max_staleness_ms,
        self_degraded: super::opacity::is_self_opaque(&ctx.kv_state, &ctx.node_id),
    }
}

/// The detector loop. Spawned only when `emergent_detectors_enabled` (zero overhead when off).
/// Each tick runs the tier-(b) detectors and reconciles the `/stats` gauges; detection only —
/// it never emits a signal or mutates another layer's state.
pub async fn run_emergent_detectors(
    ctx: Arc<TaskCtx>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut tick = tokio::time::interval(DETECTOR_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut conflict_streaks: HashMap<String, u32> = HashMap::new();
    let mut gap_streaks: HashMap<String, u32> = HashMap::new();
    loop {
        tokio::select! {
            _ = tick.tick() => {
                let now = now_ms();
                // P1 — governed-group conflict (hysteresis-confirmed).
                let conflicts = detect_governed_group_conflicts(&ctx.kv_state, now);
                let confirmed = confirm_conflicts(&conflicts, &mut conflict_streaks, CONFIRM_TICKS);
                ctx.governed_group_conflicts.store(confirmed, Ordering::Relaxed);
                // P6 — capability-coverage gap (hysteresis-confirmed; RT3 needs the sustained window
                // to tell a retracted provider from a merely-lapsed one).
                let gaps = detect_coverage_gaps(&ctx.kv_state, now);
                let confirmed_gaps = confirm_by_key(&gaps, &mut gap_streaks, CONFIRM_TICKS);
                ctx.capability_coverage_gaps.store(confirmed_gaps, Ordering::Relaxed);
            }
            _ = shutdown.wait_for(|v| *v) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framing::make_gossip_update;
    use crate::store::{apply_and_notify, KvState};
    use bytes::Bytes;
    use mycelium_core::hlc::Hlc;
    use crate::node_id::NodeId;

    fn seed_intent(kv: &KvState, hlc: &Hlc, group: &str, min: usize, max: Option<usize>, now: u64) {
        let mut intent = MembershipIntent::new(group, min, max);
        intent.written_at_ms = now;
        let key: Arc<str> = Arc::from(format!("{MEMBERSHIP_PREFIX}{group}").as_str());
        let bytes = mycelium_core::serde_fixint::to_vec(&intent).unwrap();
        apply_and_notify(kv, &make_gossip_update(
            &NodeId::new("127.0.0.1", 9000).unwrap(), 4, key, Bytes::from(bytes), false, hlc));
    }

    fn seed_members(kv: &KvState, hlc: &Hlc, group: &str, count: usize) {
        for i in 0..count {
            let node = NodeId::new("127.0.0.1", 20000 + i as u16).unwrap();
            let key: Arc<str> = Arc::from(format!("grp/{group}/{node}").as_str());
            apply_and_notify(kv, &make_gossip_update(
                &node, 4, key, Bytes::from_static(b"1"), false, hlc));
        }
    }

    /// P1 gate — the #56 shape: a governor caps a group at max=8, but observed membership is 9
    /// (emergent auto-join re-added a node). The detector must flag exactly that group.
    #[test]
    fn detects_the_56_over_max_condition() {
        let kv = KvState::new(0);
        let hlc = Hlc::new();
        let now = 1_000_000_000_000;
        seed_intent(&kv, &hlc, "workers", 2, Some(8), now);
        seed_members(&kv, &hlc, "workers", 9); // over max by one — the #56 condition

        let conflicts = detect_governed_group_conflicts(&kv, now);
        assert_eq!(conflicts.len(), 1, "exactly one group in conflict");
        assert_eq!(conflicts[0].group, "workers");
        assert_eq!(conflicts[0].observed, 9);
        assert_eq!(conflicts[0].max, Some(8));
    }

    /// Under-min is also a conflict (under-provisioned group).
    #[test]
    fn detects_under_min_condition() {
        let kv = KvState::new(0);
        let hlc = Hlc::new();
        let now = 1_000_000_000_000;
        seed_intent(&kv, &hlc, "workers", 5, Some(10), now);
        seed_members(&kv, &hlc, "workers", 2); // under min

        let conflicts = detect_governed_group_conflicts(&kv, now);
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].observed, 2);
        assert_eq!(conflicts[0].min, 5);
    }

    /// The false-positive gate — healthy churn does NOT trip it. Membership within `[min, max]`
    /// yields no conflict, whatever the exact count in range.
    #[test]
    fn healthy_membership_in_bounds_does_not_trip() {
        let hlc = Hlc::new();
        let now = 1_000_000_000_000;
        for count in [2, 5, 8] {
            // A fresh store per in-range count (the store is the unit under test).
            let kv = KvState::new(0);
            seed_intent(&kv, &hlc, "workers", 2, Some(8), now);
            seed_members(&kv, &hlc, "workers", count);
            assert!(
                detect_governed_group_conflicts(&kv, now).is_empty(),
                "in-range membership ({count} in [2,8]) must not trip the detector",
            );
        }
    }

    /// RT3 — an *evaporated* intent produces no phantom conflict even if membership is out of its
    /// (stale) bounds. A departed governor must not diagnose.
    #[test]
    fn evaporated_intent_produces_no_conflict() {
        let kv = KvState::new(0);
        let hlc = Hlc::new();
        let now = 1_000_000_000_000;
        // Intent written far in the past (older than the TTL) ⇒ evaporated.
        seed_intent(&kv, &hlc, "workers", 2, Some(8), now - MEMBERSHIP_INTENT_TTL_MS - 1);
        seed_members(&kv, &hlc, "workers", 9); // out of the stale bounds, but the intent is gone

        assert!(
            detect_governed_group_conflicts(&kv, now).is_empty(),
            "an evaporated governor intent must not produce a conflict (RT3)",
        );
    }

    /// Hysteresis — a conflict must persist `CONFIRM_TICKS` ticks before it is confirmed, and a
    /// group that recovers resets. This is the false-positive guard against convergence transients.
    #[test]
    fn hysteresis_requires_sustained_conflict() {
        let raw = vec![GroupConflict { group: "g".into(), observed: 9, min: 2, max: Some(8) }];
        let mut streaks = HashMap::new();
        assert_eq!(confirm_conflicts(&raw, &mut streaks, 2), 0, "tick 1: not yet confirmed");
        assert_eq!(confirm_conflicts(&raw, &mut streaks, 2), 1, "tick 2: confirmed");
        // Group recovers → streak pruned, count drops to 0.
        assert_eq!(confirm_conflicts(&[], &mut streaks, 2), 0, "recovered: no confirmed conflict");
        assert!(streaks.is_empty(), "recovered group's streak is pruned");
    }

    fn seed_opacity(kv: &KvState, hlc: &Hlc, node: &NodeId, kind: &str, is_opaque: bool, now: u64) {
        let key: Arc<str> = Arc::from(format!("sys/load/{node}/{kind}").as_str());
        let ls = crate::signal::LoadState { fill_ratio: if is_opaque { 1.0 } else { 0.1 }, is_opaque, written_at_ms: now };
        apply_and_notify(kv, &make_gossip_update(
            node, 4, key, crate::signal::encode_load_state(&ls), false, hlc));
    }

    /// P4 storm gate — most of the fleet shedding shows a high percentage.
    #[test]
    fn opacity_storm_shows_high_pct() {
        let kv = KvState::new(0);
        let hlc = Hlc::new();
        let now = 1_000_000_000_000;
        // 6 of 8 live nodes opaque.
        for i in 0..8 {
            let node = NodeId::new("127.0.0.1", 30000 + i as u16).unwrap();
            seed_opacity(&kv, &hlc, &node, "work", i < 6, now);
        }
        let pct = opaque_node_pct(&kv, 8, now, OPAQUE_MAX_AGE_MS);
        assert_eq!(pct, 75, "6 of 8 opaque ⇒ 75%");
    }

    /// P4 false-positive gate — a healthy fleet (few shedding) reads low.
    #[test]
    fn healthy_fleet_shows_low_opacity_pct() {
        let kv = KvState::new(0);
        let hlc = Hlc::new();
        let now = 1_000_000_000_000;
        for i in 0..8 {
            let node = NodeId::new("127.0.0.1", 31000 + i as u16).unwrap();
            seed_opacity(&kv, &hlc, &node, "work", i == 0, now); // 1 of 8
        }
        assert_eq!(opaque_node_pct(&kv, 8, now, OPAQUE_MAX_AGE_MS), 12, "1 of 8 ⇒ 12%");
    }

    /// P4 RT3 — a *stale* opaque entry (older than the freshness window) is not counted; a node
    /// that stopped refreshing its shed pheromone is no longer "opaque."
    #[test]
    fn stale_opacity_entry_not_counted() {
        let kv = KvState::new(0);
        let hlc = Hlc::new();
        let now = 1_000_000_000_000;
        let node = NodeId::new("127.0.0.1", 32000).unwrap();
        seed_opacity(&kv, &hlc, &node, "work", true, now - OPAQUE_MAX_AGE_MS - 1); // stale
        assert_eq!(opaque_node_pct(&kv, 4, now, OPAQUE_MAX_AGE_MS), 0, "stale opacity ⇒ not counted");
    }

    fn seed_requirement(kv: &KvState, hlc: &Hlc, node: &NodeId, ns: &str, name: &str) {
        use crate::capability::CapFilter;
        let key: Arc<str> = Arc::from(format!("req/{node}/{ns}/{name}").as_str());
        let req = ReqEntry { filter: CapFilter::new(ns, name), refresh_interval_ms: 60_000 };
        apply_and_notify(kv, &make_gossip_update(node, 4, key, req.encode(), false, hlc));
    }

    fn seed_capability(kv: &KvState, hlc: &Hlc, node: &NodeId, ns: &str, name: &str) {
        use crate::capability::Capability;
        let key: Arc<str> = Arc::from(format!("cap/{node}/{ns}/{name}").as_str());
        apply_and_notify(kv, &make_gossip_update(node, 4, key, Capability::new(ns, name).encode(), false, hlc));
    }

    /// P6 gap gate — a required capability with zero providers is a coverage gap.
    #[test]
    fn coverage_gap_when_no_provider() {
        let kv = KvState::new(0);
        let hlc = Hlc::new();
        let requirer = NodeId::new("127.0.0.1", 40000).unwrap();
        seed_requirement(&kv, &hlc, &requirer, "ai", "llm");
        // no cap/ provider for ai/llm
        let now = mycelium_core::hlc::physical_ms(hlc.tick()); // real now-ms, matches production now_ms()
        let gaps = detect_coverage_gaps(&kv, now);
        assert_eq!(gaps, vec!["ai/llm".to_string()], "unmet requirement ⇒ one coverage gap");
    }

    /// P6 false-positive gate — a requirement with a live provider is NOT a gap.
    #[test]
    fn no_gap_when_provider_present() {
        let kv = KvState::new(0);
        let hlc = Hlc::new();
        let requirer = NodeId::new("127.0.0.1", 41000).unwrap();
        let provider = NodeId::new("127.0.0.1", 41001).unwrap();
        seed_requirement(&kv, &hlc, &requirer, "ai", "llm");
        seed_capability(&kv, &hlc, &provider, "ai", "llm");
        let now = mycelium_core::hlc::physical_ms(hlc.tick()).max(1);
        assert!(detect_coverage_gaps(&kv, now).is_empty(), "a covered requirement is not a gap");
    }

    /// Generic hysteresis (shared by P1 + P6) confirms only sustained keys.
    #[test]
    fn confirm_by_key_requires_sustained() {
        let mut streaks = HashMap::new();
        let keys = vec!["ai/llm".to_string()];
        assert_eq!(confirm_by_key(&keys, &mut streaks, 2), 0, "tick 1");
        assert_eq!(confirm_by_key(&keys, &mut streaks, 2), 1, "tick 2 confirmed");
        assert_eq!(confirm_by_key(&[], &mut streaks, 2), 0, "recovered");
        assert!(streaks.is_empty());
    }
}
