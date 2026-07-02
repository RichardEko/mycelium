//! Track 2a — group **MembershipGovernor**: coordinator-free elastic group sizing via intent +
//! local probabilistic self-election (see `docs/plans/elastic-sizing-intent-governed.md` §7.1).
//!
//! Bounds `[min, max]` (+ `drain`) for a group are published as a [`MembershipIntent`] over the
//! Track-1 transport. Each node, on a jittered tick + on gossip change, observes the live member
//! count `N` (`grp/{group}/*`), its eligibility (does it match the group's `CapabilityGroupDef`
//! filter), and its **own** load, then independently rolls to join/leave so the count converges
//! toward the band — **probabilistically** (Option B): join with `p ∝ (min−N)/eligible`, biased
//! toward idle nodes; leave with `p ∝ (N−max)/members`, biased toward busy nodes; drain wins.
//!
//! **Bounds are convergence targets, not guarantees** (sovereign veto wins; intent evaporates).
//! No coordinator, no barrier, no controller — each node acts on local information (Principles 1/5).
//! Reuses the emergent-group machinery (`emit_membership`, `CapabilityGroupDef` filter) and the
//! Track-1 `read_fresh_intent` / `publish_intent` transport; the engine is the self-election only.

use crate::agent::{GossipAgent, TaskCtx};
use crate::capability::{Capability, CapFilter, CapabilityGroupDef};
use crate::node_id::NodeId;
use mycelium_core::kv_handle::KvHandle;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::debug;

use super::capability_ops::{is_cap_locality_key, parse_cap_key_or_warn, scan_prefix_kv};

/// `sys/govern/membership/{group}` — one fleet intent per governed group.
pub const MEMBERSHIP_PREFIX: &str = "sys/govern/membership/";
/// Evaporation window for a membership intent (refresh within this or it self-heals away).
pub const MEMBERSHIP_INTENT_TTL_MS: u64 = 5 * 60 * 1000;
/// Post-action cooldown, in convergence ticks, to damp boundary flap.
const COOLDOWN_TICKS: u32 = 3;

/// Elastic-sizing intent for one group: keep the live member count within `[min, max]`, minus any
/// explicitly drained nodes. `max = None` is unbounded. Rides the Track-1 transport.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MembershipIntent {
    pub group: String,
    pub min: usize,
    pub max: Option<usize>,
    pub drain: Vec<NodeId>,
    pub written_at_ms: u64,
    pub target: Option<NodeId>,
}

impl MembershipIntent {
    pub fn new(group: impl Into<String>, min: usize, max: Option<usize>) -> Self {
        Self { group: group.into(), min, max, drain: Vec::new(), written_at_ms: 0, target: None }
    }
    /// Names nodes that must leave (and stay out) — cooperative self-removal, not eviction.
    pub fn with_drain(mut self, nodes: Vec<NodeId>) -> Self {
        self.drain = nodes;
        self
    }
    /// Target this intent at a single node (per-node governance over gossip).
    pub fn for_node(mut self, node: NodeId) -> Self {
        self.target = Some(node);
        self
    }
}

impl super::intent::FleetIntent for MembershipIntent {
    fn written_at_ms(&self) -> u64 { self.written_at_ms }
    fn stamp(&mut self, now_ms: u64) { self.written_at_ms = now_ms; }
    fn target(&self) -> Option<&NodeId> { self.target.as_ref() }
}

/// What a node decides this tick for one group.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MembershipAction {
    Join,
    Leave,
    Hold,
}

/// Join probability for an eligible non-member: the deficit spread over the eligible non-member
/// population, biased by the node's **own** load (idle ⇒ up to ×1.5, busy ⇒ down to ×0.5 — but a
/// needed busy node keeps a non-zero floor so convergence still completes). `0.0` if not under min.
pub fn join_probability(n: usize, min: usize, eligible_non_members: usize, my_load: f64) -> f64 {
    if n >= min || eligible_non_members == 0 {
        return 0.0;
    }
    let base = (min - n) as f64 / eligible_non_members as f64;
    let bias = 1.0 + (0.5 - my_load.clamp(0.0, 1.0)); // load 0 → 1.5, load 1 → 0.5
    (base * bias).clamp(0.0, 1.0)
}

/// Leave probability for a member over max: the excess spread over members, biased toward busy
/// nodes (they shed first). `0.0` if not over max.
pub fn leave_probability(n: usize, max: usize, members: usize, my_load: f64) -> f64 {
    if n <= max || members == 0 {
        return 0.0;
    }
    let base = (n - max) as f64 / members as f64;
    let bias = 1.0 + (my_load.clamp(0.0, 1.0) - 0.5); // load 1 → 1.5, load 0 → 0.5
    (base * bias).clamp(0.0, 1.0)
}

/// Pure self-election decision: drain wins; else roll against the join/leave probabilities.
/// `roll` ∈ [0, 1). Kept pure (no I/O) so it is exhaustively unit-testable.
pub fn decide(am_member: bool, is_drain: bool, join_p: f64, leave_p: f64, roll: f64) -> MembershipAction {
    if am_member && is_drain {
        return MembershipAction::Leave;
    }
    if !am_member && roll < join_p {
        return MembershipAction::Join;
    }
    if am_member && roll < leave_p {
        return MembershipAction::Leave;
    }
    MembershipAction::Hold
}

/// Nodes whose gossiped capabilities match `filter` — the eligible set for a group.
fn resolve_eligible(kv_state: &crate::store::KvState, filter: &CapFilter) -> HashSet<NodeId> {
    let mut set = HashSet::new();
    for (key, bytes) in scan_prefix_kv(kv_state, "cap/") {
        if is_cap_locality_key(&key) {
            continue;
        }
        let Some((node, _ns, _name)) = parse_cap_key_or_warn("cap/", &key) else { continue };
        if let Some(cap) = Capability::decode(&bytes)
            && filter.matches(&cap)
        {
            set.insert(node);
        }
    }
    set
}

/// Current live members of `group` (`grp/{group}/{node}`, tombstones excluded).
pub(super) fn group_members(kv_state: &crate::store::KvState, group: &str) -> HashSet<NodeId> {
    let prefix = crate::signal::grp_prefix(group);
    let mut set = HashSet::new();
    for (key, bytes) in scan_prefix_kv(kv_state, &prefix) {
        if bytes.is_empty() {
            continue; // tombstone = left
        }
        if let Some(node_str) = key.strip_prefix(&prefix)
            && let Ok(node) = node_str.parse::<NodeId>()
        {
            set.insert(node);
        }
    }
    set
}

/// One convergence pass over every governed group on this node.
fn converge(ctx: &Arc<TaskCtx>, kv: &KvHandle, cooldown: Duration, last_action: &mut HashMap<String, Instant>) {
    let me = &ctx.node_id;
    let my_load = mycelium_core::framing::gossip_shard_fill(&ctx.gossip_txs) as f64;
    for (key, _) in scan_prefix_kv(&ctx.kv_state, MEMBERSHIP_PREFIX) {
        let Some(intent) =
            super::intent::read_fresh_intent::<MembershipIntent>(kv, &key, me, MEMBERSHIP_INTENT_TTL_MS)
        else { continue };
        let group = intent.group.as_str();
        if last_action.get(group).is_some_and(|t| t.elapsed() < cooldown) {
            continue; // recently acted — damp flap
        }
        // The group's filter (for eligibility) comes from its CapabilityGroupDef.
        let Some(def) = kv.get(&format!("cap-group/{group}")).and_then(|b| CapabilityGroupDef::decode(&b))
        else { continue };

        let eligible = resolve_eligible(&ctx.kv_state, &def.filter);
        let members = group_members(&ctx.kv_state, group);
        let n = members.len();
        let am_member = members.contains(me);
        let am_eligible = eligible.contains(me);
        let is_drain = intent.drain.iter().any(|d| d == me);
        let eligible_non_members = eligible.difference(&members).count();

        let join_p = if am_eligible {
            join_probability(n, intent.min, eligible_non_members, my_load)
        } else {
            0.0
        };
        let leave_p = intent.max.map_or(0.0, |mx| leave_probability(n, mx, members.len(), my_load));

        match decide(am_member, is_drain, join_p, leave_p, fastrand::f64()) {
            MembershipAction::Join => {
                debug!(group, n, min = intent.min, "membership: self-electing to JOIN");
                super::emergent_groups::emit_membership(ctx, me, &Arc::from(group), false);
                last_action.insert(group.to_string(), Instant::now());
            }
            MembershipAction::Leave => {
                debug!(group, n, ?intent.max, "membership: self-electing to LEAVE");
                super::emergent_groups::emit_membership(ctx, me, &Arc::from(group), true);
                last_action.insert(group.to_string(), Instant::now());
            }
            MembershipAction::Hold => {}
        }
    }
}

impl GossipAgent {
    /// Publish (or refresh) an elastic-sizing intent for a group (WS-C / elastic Track 2a). Gossips
    /// to `sys/govern/membership/{group}`; nodes running [`start_membership_governor`] converge the
    /// member count toward `[min, max]`. Re-publish within [`MEMBERSHIP_INTENT_TTL_MS`] to keep it
    /// in force (it evaporates otherwise — self-heal to emergent membership). Intent, not command.
    ///
    /// [`start_membership_governor`]: Self::start_membership_governor
    pub fn publish_membership_intent(&self, intent: MembershipIntent) -> bool {
        let key = format!("{MEMBERSHIP_PREFIX}{}", intent.group);
        super::intent::publish_intent(&self.kv(), &key, intent)
    }

    /// Opt this node in to elastic membership self-election (WS-C / elastic Track 2a). Spawns a task
    /// that, each `health_check_interval` (jittered) and on `sys/govern/membership/` changes, runs
    /// the probabilistic self-election for every governed group. Exits on shutdown.
    pub fn start_membership_governor(&self) {
        let ctx = Arc::clone(&self.task_ctx);
        let kv = KvHandle::from_core(Arc::clone(&self.task_ctx.core));
        let mut shutdown = self.task_ctx.shutdown_tx.subscribe();
        let interval_secs = self.config.health_check_interval_secs.max(1);
        let cooldown = Duration::from_secs(interval_secs * COOLDOWN_TICKS as u64);
        self.task_ctx.spawn_task(async move {
            let mut rx = kv.subscribe_prefix(MEMBERSHIP_PREFIX);
            let mut tick = tokio::time::interval(Duration::from_secs(interval_secs));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut last_action: HashMap<String, Instant> = HashMap::new();
            loop {
                // Jitter each pass so nodes don't all roll against the same instantaneous view.
                tokio::time::sleep(Duration::from_millis(fastrand::u64(0..250))).await;
                converge(&ctx, &kv, cooldown, &mut last_action);
                tokio::select! {
                    r = rx.changed() => { if r.is_err() { break; } }
                    _ = tick.tick() => {}
                    _ = shutdown.wait_for(|v| *v) => break,
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_probability_zero_when_at_or_above_min() {
        assert_eq!(join_probability(2, 2, 5, 0.5), 0.0);
        assert_eq!(join_probability(3, 2, 5, 0.5), 0.0);
        assert_eq!(join_probability(0, 2, 0, 0.5), 0.0); // no eligible non-members
    }

    #[test]
    fn join_probability_scales_with_deficit_and_idle_bias() {
        // deficit 2 over 4 eligible non-members, neutral load (0.5) → base 0.5, bias 1.0.
        assert!((join_probability(0, 2, 4, 0.5) - 0.5).abs() < 1e-9);
        // idle (load 0) biases UP (×1.5); busy (load 1) biases DOWN (×0.5) — idle joins more.
        assert!(join_probability(0, 2, 4, 0.0) > join_probability(0, 2, 4, 1.0));
        // a fully-busy but needed node still keeps a non-zero floor (convergence completes).
        assert!(join_probability(0, 1, 4, 1.0) > 0.0);
    }

    #[test]
    fn leave_probability_zero_within_band_and_busy_bias() {
        assert_eq!(leave_probability(3, 3, 3, 0.5), 0.0);
        assert_eq!(leave_probability(2, 3, 3, 0.5), 0.0);
        // over max → busy nodes (load 1) more likely to shed than idle (load 0).
        assert!(leave_probability(5, 3, 5, 1.0) > leave_probability(5, 3, 5, 0.0));
    }

    #[test]
    fn decide_drain_leaves_even_within_band() {
        assert_eq!(decide(true, true, 0.0, 0.0, 0.99), MembershipAction::Leave);
        // drain only applies to members.
        assert_eq!(decide(false, true, 0.0, 0.0, 0.99), MembershipAction::Hold);
    }

    #[test]
    fn decide_rolls_join_and_leave() {
        // non-member, roll under join_p → Join; over → Hold.
        assert_eq!(decide(false, false, 0.7, 0.0, 0.5), MembershipAction::Join);
        assert_eq!(decide(false, false, 0.7, 0.0, 0.8), MembershipAction::Hold);
        // member, roll under leave_p → Leave; over → Hold.
        assert_eq!(decide(true, false, 0.0, 0.6, 0.5), MembershipAction::Leave);
        assert_eq!(decide(true, false, 0.0, 0.6, 0.7), MembershipAction::Hold);
        // a member is never told to "join"; a non-member never to "leave" (absent drain).
        assert_eq!(decide(true, false, 0.9, 0.0, 0.1), MembershipAction::Hold);
    }
}
