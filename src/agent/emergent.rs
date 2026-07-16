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

use serde::{Deserialize, Serialize};

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
/// A `sys/health/` self-report older than this is not counted for store-convergence.
const HEALTH_MAX_AGE_MS: u64 = 30_000;
/// KV namespace for the per-node store self-report (Phase 2 convergence health).
const HEALTH_PREFIX: &str = "sys/health/";

/// A node's periodic store self-report, gossiped to `sys/health/{node}` (Phase 2). A **count**, not
/// a hash: on a live cluster the store hash churns every tick (soft-state refresh perturbs it — the
/// RT2 observer effect), so exact byte-identity is never the convergence metric. The *spread* of
/// entry counts across nodes is the honest signal — a partitioned or behind node shows far fewer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HealthReport {
    pub store_entries:  usize,
    pub written_at_ms:  u64,
}

/// Cross-node store-convergence view assembled from the fresh `sys/health/` reports this node holds.
/// `nodes_reporting` = how many nodes' fresh reports are visible; `max − min` entries is the
/// divergence indicator (0 spread ⇒ converged; a large spread ⇒ a node is behind/partitioned).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct StoreConvergence {
    pub nodes_reporting: usize,
    pub min_entries:     usize,
    pub max_entries:     usize,
}

/// **Pure** — the cross-node store-convergence view from `sys/health/` reports fresher than
/// `max_age_ms`. Node-local scan; unit-testable.
pub fn store_convergence(kv_state: &crate::store::KvState, now: u64, max_age_ms: u64) -> StoreConvergence {
    let mut min = usize::MAX;
    let mut max = 0usize;
    let mut n = 0usize;
    for (_key, bytes) in scan_prefix_kv(kv_state, HEALTH_PREFIX) {
        let Ok(r) = mycelium_core::serde_fixint::from_slice::<HealthReport>(&bytes) else { continue };
        if now.saturating_sub(r.written_at_ms) > max_age_ms {
            continue;
        }
        n += 1;
        min = min.min(r.store_entries);
        max = max.max(r.store_entries);
    }
    StoreConvergence {
        nodes_reporting: n,
        min_entries:     if n == 0 { 0 } else { min },
        max_entries:     max,
    }
}

/// Publish this node's `sys/health/{node}` store self-report. Called each detector tick.
fn publish_health(ctx: &TaskCtx, now: u64) {
    let report = HealthReport { store_entries: ctx.kv_state.store.pin().len(), written_at_ms: now };
    if let Ok(bytes) = mycelium_core::serde_fixint::to_vec(&report) {
        let key: std::sync::Arc<str> = std::sync::Arc::from(format!("{HEALTH_PREFIX}{}", ctx.node_id).as_str());
        mycelium_core::ops::kv_set(&ctx.core, key, bytes.into());
    }
}
/// Sliding window over which P2 counts a (group, node)'s membership transitions.
const FLAP_WINDOW_MS: u64 = 60_000;
/// Membership transitions within [`FLAP_WINDOW_MS`] that make a (group, node) "flapping" — 4 = two
/// full join/leave cycles (a single failover is 1–2; the threshold is the false-positive guard).
const FLAP_THRESHOLD: usize = 4;

/// Group membership as `group → {member node strings}` — the P2 snapshot unit.
pub type MembershipSnapshot = HashMap<String, HashSet<String>>;

/// **Pure** — snapshot current group membership across *all* groups from `grp/{group}/{node}`
/// (tombstones = left, excluded). Node-local KV scan.
pub fn membership_snapshot(kv_state: &crate::store::KvState) -> MembershipSnapshot {
    let mut out: MembershipSnapshot = HashMap::new();
    for (key, bytes) in scan_prefix_kv(kv_state, "grp/") {
        if bytes.is_empty() {
            continue; // tombstone = left the group
        }
        let tail = key.strip_prefix("grp/").unwrap_or("");
        let Some(slash) = tail.find('/') else { continue };
        out.entry(tail[..slash].to_string()).or_default().insert(tail[slash + 1..].to_string());
    }
    out
}

/// **Pure** — the set of (group, node) pairs whose membership *presence* changed between two
/// snapshots (joined or left). The per-tick input to the flap tracker.
pub fn flap_transitions(prev: &MembershipSnapshot, curr: &MembershipSnapshot) -> HashSet<(String, String)> {
    let mut changed = HashSet::new();
    let empty = HashSet::new();
    for g in prev.keys().chain(curr.keys()).collect::<HashSet<_>>() {
        let p = prev.get(g).unwrap_or(&empty);
        let c = curr.get(g).unwrap_or(&empty);
        for node in p.symmetric_difference(c) {
            changed.insert((g.clone(), node.clone()));
        }
    }
    changed
}

/// Sliding-window **failover-flap tracker** (P2): per (group, node), the timestamps of recent
/// membership transitions. A node that repeatedly joins/leaves — the #56 "node count flapping with
/// no signal why" — accumulates transitions here. Stateful (lives in the detector loop).
#[derive(Default)]
pub struct FlapTracker {
    events: HashMap<(String, String), std::collections::VecDeque<u64>>,
}

impl FlapTracker {
    /// Record this tick's transitions at `now`.
    pub fn record(&mut self, transitions: &HashSet<(String, String)>, now: u64) {
        for pair in transitions {
            self.events.entry(pair.clone()).or_default().push_back(now);
        }
    }

    /// Count of (group, node) pairs with ≥ `threshold` transitions within `window_ms`. Prunes
    /// expired events and forgets pairs that fall silent (so a settled failover ages out).
    pub fn flapping_count(&mut self, now: u64, window_ms: u64, threshold: usize) -> u64 {
        let cutoff = now.saturating_sub(window_ms);
        let mut count = 0u64;
        self.events.retain(|_, dq| {
            while dq.front().is_some_and(|&t| t < cutoff) {
                dq.pop_front();
            }
            if dq.is_empty() {
                return false; // no recent activity → forget the pair
            }
            if dq.len() >= threshold {
                count += 1;
            }
            true
        });
        count
    }
}

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
    let mut seen:   HashSet<String> = HashSet::new(); // all nodes with a FRESH load entry
    for (key, bytes) in scan_prefix_kv(kv_state, crate::signal::kv_ns::LOAD) {
        let tail = key.strip_prefix(crate::signal::kv_ns::LOAD).unwrap_or("");
        let node = &tail[..tail.find('/').unwrap_or(tail.len())];
        if let Some(s) = crate::signal::decode_load_state(&bytes)
            && now.saturating_sub(s.written_at_ms) <= max_age_ms
        {
            seen.insert(node.to_string());
            if s.is_opaque {
                opaque.insert(node.to_string());
            }
        }
    }
    // Denominator over a CONSISTENT population: max(roster, nodes with fresh load data). The old code
    // used only the local roster while the numerator was drawn from unbounded `sys/load/` KV, so the
    // ratio could structurally exceed 100% (masked by `.min(100)`) — a blind observer with a few
    // forged opaque keys pinned a Critical `opacity_storm` at 100% (audit 2026-07-15 pass 5). Now
    // `opaque ⊆ seen ⊆ denom`, so the percentage is a real proportion.
    let denom = live_nodes.max(seen.len());
    if denom == 0 {
        return 0;
    }
    ((opaque.len() as u64 * 100) / denom as u64).min(100)
}

/// Compute the current opacity-storm gauge (P4) — cheap, on-demand (called by `/stats`). The
/// denominator is this node's roster + self; RT3 evaporation is honoured via [`OPAQUE_MAX_AGE_MS`].
pub fn compute_opaque_node_pct(ctx: &TaskCtx) -> u64 {
    let live_nodes = ctx.peers.pin().len() + 1; // peers + self
    opaque_node_pct(&ctx.kv_state, live_nodes, now_ms(), OPAQUE_MAX_AGE_MS)
}

/// **Pure P3 detector input** — the set of `(node, kind)` pairs currently *opaque* and fresh, from
/// `sys/load/{node}/{kind}`. P3 (opacity/pheromone **oscillation** — a node "hunting" in and out of
/// shed) is the presence-set-churn sibling of P2: feed successive snapshots through
/// [`set_transitions`] into a [`FlapTracker`]. Node-local KV scan.
pub fn opacity_pairs(kv_state: &crate::store::KvState, now: u64, max_age_ms: u64) -> HashSet<(String, String)> {
    let mut out = HashSet::new();
    for (key, bytes) in scan_prefix_kv(kv_state, crate::signal::kv_ns::LOAD) {
        let tail = key.strip_prefix(crate::signal::kv_ns::LOAD).unwrap_or("");
        let Some(slash) = tail.find('/') else { continue };
        if let Some(s) = crate::signal::decode_load_state(&bytes)
            && s.is_opaque
            && now.saturating_sub(s.written_at_ms) <= max_age_ms
        {
            out.insert((tail[..slash].to_string(), tail[slash + 1..].to_string()));
        }
    }
    out
}

/// **Pure** — the set of pairs whose *presence* changed between two flat sets (symmetric
/// difference). The generic per-tick transition input shared by P3 (and any presence-set detector).
pub fn set_transitions(
    prev: &HashSet<(String, String)>,
    curr: &HashSet<(String, String)>,
) -> HashSet<(String, String)> {
    prev.symmetric_difference(curr).cloned().collect()
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

// ── Phase 3: the causal event ring (the "explain" source) ─────────────────────────────────────

/// Bounded event-ring capacity. RT4 decision (taxonomy §6): the ring is **always-on when the
/// detector feature is enabled** (so post-hoc `explain` works — a ring switched on mid-incident
/// can only explain *future* ones), fixed-memory, oldest dropped first. ~1024 significant events ×
/// ~128 B ≈ 128 KB/node — cheap enough to leave on.
const EVENT_RING_CAP: usize = 1024;

/// One *significant* fleet event, HLC-stamped for cross-node causal ordering. "Significant" =
/// state changes (a detector firing/clearing, a commit conflict), **not** a per-message firehose.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Event {
    /// HLC stamp — the causal-order key the Phase-3 fan-out assembles peer rings by.
    pub hlc:    u64,
    /// Originating node (so an assembled cross-node stream stays attributable).
    pub node:   String,
    /// Event kind, e.g. `"commit_conflict"`, `"governed_group_conflict"`.
    pub kind:   String,
    /// Human-readable detail — legible to a non-designer (the DoD bar).
    pub detail: String,
}

/// A per-node bounded ring of [`Event`]s. Leaf lock (short synchronous push/scan, never across
/// `await`) — see the lock-order table.
#[derive(Default)]
pub struct EventRing {
    events: std::sync::Mutex<std::collections::VecDeque<Event>>,
}

impl EventRing {
    /// Append an event, dropping the oldest if at capacity.
    pub fn record(&self, ev: Event) {
        let mut q = self.events.lock().unwrap_or_else(|e| e.into_inner());
        if q.len() >= EVENT_RING_CAP {
            q.pop_front();
        }
        q.push_back(ev);
    }

    /// Events with `hlc >= since`, in HLC causal order (oldest first). `since = 0` = everything.
    pub fn since(&self, since: u64) -> Vec<Event> {
        let q = self.events.lock().unwrap_or_else(|e| e.into_inner());
        let mut v: Vec<Event> = q.iter().filter(|e| e.hlc >= since).cloned().collect();
        v.sort_by_key(|e| e.hlc);
        v
    }
}

/// Record a significant event into this node's ring, HLC-stamped now. A no-op-cheap helper the
/// detector loop and the commit-conflict tripwire call. Callers gate on `emergent_detectors_enabled`
/// (the loop is only spawned when enabled; the tripwire checks the flag) — RT4 zero-overhead-off.
pub fn record_event(ctx: &TaskCtx, kind: &str, detail: String) {
    ctx.event_ring.record(Event {
        hlc:    ctx.hlc.tick(),
        node:   ctx.node_id.to_string(),
        kind:   kind.to_string(),
        detail,
    });
}

// ── Phase 3 increment 2: cross-node causal `explain` (best-effort fan-out) ────────────────────

/// RPC kind for the peer-to-peer explain-ring request/response.
pub(crate) const EXPLAIN_RPC_KIND: &str = "sys.explain";

/// How long the fan-out waits on each peer before giving up on it. Short — an `explain` is
/// interactive — and, crucially, a peer that misses it becomes a *named non-responder* (RT3),
/// never a silent gap.
const EXPLAIN_FANOUT_TIMEOUT: Duration = Duration::from_secs(2);

/// Cap on the number of peers one `explain` fans out to. Bounds the concurrent `sys.explain` RPCs
/// so an operator query on a large fleet cannot spray one RPC per node (an on-demand query must not
/// become an O(N) storm). Peers beyond the cap are *named* (`not_queried`), never silently dropped —
/// the same RT3 honesty as `non_responders`. Raise only if you genuinely need a wider single-shot
/// view; the local ring + a handful of peers already reconstruct most incidents.
const EXPLAIN_MAX_FANOUT: usize = 32;

/// Pick the deterministic subset of peers an `explain` fans out to (capped at `cap`), plus the count
/// skipped. Sorted by identity so the subset is *stable* across repeated queries; the skipped count
/// is surfaced so a capped fan-out is never mistaken for a complete one. **Pure** — unit-tested.
fn select_explain_targets(mut peers: Vec<crate::node_id::NodeId>, cap: usize) -> (Vec<crate::node_id::NodeId>, usize) {
    peers.sort_by_key(|p| p.to_string());
    let not_queried = peers.len().saturating_sub(cap);
    peers.truncate(cap);
    (peers, not_queried)
}

/// The assembled cross-node explain: every reachable node's ring merged into one HLC-ordered
/// stream, plus the RT3 honesty pair — who answered and who did **not**. A non-empty
/// `non_responders` is the view telling you the reconstruction is partial *exactly where* it says,
/// rather than silently dropping the events of the nodes that matter most during an incident.
#[derive(Debug, Clone, Serialize)]
pub struct ExplainResult {
    /// The node that assembled this view (the observer).
    pub observer:       String,
    /// Local + every responder's events, HLC-ordered — the causal narrative. Each ring is
    /// single-author (`record_event` stamps `node = self`), so the merged streams are disjoint.
    pub events:         Vec<Event>,
    /// The same events rendered as a legible, operator-readable story — one line per event, terse
    /// `kind` glossed into plain English (the #56 DoD: "no designer knowledge required to read it").
    pub narrative:      Vec<String>,
    /// Peers that answered the fan-out, sorted.
    pub responders:     Vec<String>,
    /// Peers that were queried but did **not** answer within [`EXPLAIN_FANOUT_TIMEOUT`] — the named
    /// gaps (RT3), sorted.
    pub non_responders: Vec<String>,
    /// How many known peers were **not queried at all** because the fan-out hit
    /// [`EXPLAIN_MAX_FANOUT`]. Non-zero ⇒ this is a *capped* view: raise the cap or re-query for a
    /// wider one. Named, not silently dropped — the same RT3 honesty as `non_responders`.
    pub not_queried:    usize,
}

/// Render an HLC-ordered event stream into a legible, non-designer-readable narrative — one line
/// per event: `[hlc N] <node> — <plain-English gloss> (<detail>)`. The gloss table translates the
/// terse event `kind` into what an on-call operator needs (the #56 acceptance bar: reconstruct
/// "governor capped → auto-join re-added → governor drained" with **no code knowledge**). An
/// unknown kind falls back to its raw string, so a newly-added event type is surfaced, never
/// silently dropped.
pub fn narrate(events: &[Event]) -> Vec<String> {
    events.iter().map(|e| {
        let gloss = match e.kind.as_str() {
            "governed_group_conflict" =>
                "governed-group conflict — a group's live membership left the governor's [min,max] band",
            "membership_flap" =>
                "membership flap — a node is repeatedly joining and leaving a group",
            "capability_coverage_gap" =>
                "capability-coverage gap — a demand has no fresh provider visible from here",
            "opacity_oscillation" =>
                "opacity oscillation — a node is flipping in and out of the overload state",
            "commit_conflict" =>
                "consensus commit conflict — two proposals raced for the same slot",
            other => other,
        };
        format!("[hlc {}] {} — {} ({})", e.hlc, e.node, gloss, e.detail)
    }).collect()
}

/// Serve `sys.explain` requests: reply with **this** node's ring since the requested cursor.
/// Registered as a background task (only under `emergent_detectors_enabled` — RT4 zero-overhead-off).
pub async fn run_explain_responder(
    ctx: Arc<TaskCtx>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut rx = ctx.signal_handlers.register(Arc::from(EXPLAIN_RPC_KIND));
    loop {
        tokio::select! {
            maybe = rx.recv() => {
                let Some(sig) = maybe else { break };
                let req = super::rpc::RpcRequest::from(sig);
                let since = req.payload().get(..8)
                    .and_then(|b| <[u8; 8]>::try_from(b).ok())
                    .map(u64::from_le_bytes)
                    .unwrap_or(0);
                let events = ctx.event_ring.since(since);
                let payload = mycelium_core::serde_fixint::to_vec(&events).unwrap_or_default();
                super::rpc::rpc_respond_ctx(&ctx, &req, payload);
            }
            _ = shutdown.wait_for(|v| *v) => break,
        }
    }
}

/// Assemble the cross-node causal explain: start from this node's ring, fan a best-effort
/// `sys.explain` RPC out to a bounded subset of known peers ([`EXPLAIN_MAX_FANOUT`]), merge whatever
/// returns within [`EXPLAIN_FANOUT_TIMEOUT`] in HLC order, and name both the peers that did not
/// answer and the count of peers skipped by the cap.
///
/// Deliberately **not** [`crate::agent::service_handle::ServiceHandle::scatter_gather`]: that
/// aborts once `min_ok` replies land and discards *all* partial replies on `InsufficientReplies` —
/// the RT3 failure mode, because the slow/partitioned nodes you most need mid-incident are exactly
/// the ones that time out. Here each per-peer timeout turns a silent peer into a named
/// `non_responder` while every reply that does arrive still lands in `events`. The fan-out is
/// **capped** so an operator query on a large fleet stays a bounded action, not an O(N) RPC storm;
/// skipped peers are surfaced as `not_queried`, never silently dropped.
pub async fn assemble_explain(ctx: &Arc<TaskCtx>, since: u64) -> ExplainResult {
    let mut events = ctx.event_ring.since(since);

    let all_peers: Vec<crate::node_id::NodeId> = ctx.peers.pin().keys().cloned().collect();
    let (peers, not_queried) = select_explain_targets(all_peers, EXPLAIN_MAX_FANOUT);
    let since_bytes = bytes::Bytes::copy_from_slice(&since.to_le_bytes());

    let mut js: tokio::task::JoinSet<(crate::node_id::NodeId, Result<bytes::Bytes, super::rpc::RpcError>)> =
        tokio::task::JoinSet::new();
    for peer in &peers {
        let c = Arc::clone(ctx);
        let p = peer.clone();
        let sb = since_bytes.clone();
        js.spawn(async move {
            let r = super::rpc::rpc_call_ctx(
                &c, p.clone(), Arc::from(EXPLAIN_RPC_KIND), sb, EXPLAIN_FANOUT_TIMEOUT,
            ).await;
            (p, r)
        });
    }

    let mut responders: HashSet<String> = HashSet::new();
    while let Some(joined) = js.join_next().await {
        if let Ok((peer, Ok(payload))) = joined
            && let Ok(peer_events) =
                mycelium_core::serde_fixint::from_slice::<Vec<Event>>(&payload)
        {
            events.extend(peer_events);
            responders.insert(peer.to_string());
        }
    }

    events.sort_by_key(|e| e.hlc);

    let mut non_responders: Vec<String> = peers.iter()
        .map(|p| p.to_string())
        .filter(|p| !responders.contains(p))
        .collect();
    non_responders.sort();
    let mut responders: Vec<String> = responders.into_iter().collect();
    responders.sort();

    let narrative = narrate(&events);
    ExplainResult {
        observer: ctx.node_id.to_string(), events, narrative, responders, non_responders, not_queried,
    }
}

// ── Phase 2: the relational fleet snapshot (GET /gateway/fleet) ───────────────────────────────

/// The status of one governed group in the fleet snapshot — the relational "localize" view: the
/// governor's `[min, max]` intent vs the observed live `grp/` count, and whether that is a conflict.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct GroupStatus {
    pub group:    String,
    pub min:      usize,
    pub max:      Option<usize>,
    pub observed: usize,
    pub conflict: bool,
}

/// **Pure** — every *fresh* governed group with its intent vs observed membership (not only the
/// conflicting ones, unlike [`detect_governed_group_conflicts`] — the snapshot shows the whole
/// relation). Sorted by group so independent nodes at convergence produce byte-identical output.
pub fn governed_group_statuses(kv_state: &crate::store::KvState, now: u64) -> Vec<GroupStatus> {
    let mut out = Vec::new();
    for (_key, bytes) in scan_prefix_kv(kv_state, MEMBERSHIP_PREFIX) {
        let Ok(intent) = mycelium_core::serde_fixint::from_slice::<MembershipIntent>(&bytes) else {
            continue;
        };
        if now.saturating_sub(intent.written_at_ms) > MEMBERSHIP_INTENT_TTL_MS {
            continue;
        }
        let observed = group_members(kv_state, &intent.group).len();
        let conflict = observed < intent.min || intent.max.is_some_and(|mx| observed > mx);
        out.push(GroupStatus { group: intent.group, min: intent.min, max: intent.max, observed, conflict });
    }
    out.sort_by(|a, b| a.group.cmp(&b.group));
    out
}

/// One edge of the throttle graph: `observer` observed `sender` sending at `observed_fps`
/// (M7 `sys/rate/{observer}/{sender}` shared evidence — "who is throttling whom").
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ThrottleEdge {
    pub observer:     String,
    pub sender:       String,
    pub observed_fps: u64,
}

/// **Pure** — the throttle graph from M7 rate evidence (`sys/rate/{observer}/{sender}` → fps).
/// Sorted for cross-node determinism.
pub fn throttle_graph(kv_state: &crate::store::KvState) -> Vec<ThrottleEdge> {
    let mut out = Vec::new();
    for (key, bytes) in scan_prefix_kv(kv_state, mycelium_core::rate::RATE_PREFIX) {
        let tail = key.strip_prefix(mycelium_core::rate::RATE_PREFIX).unwrap_or("");
        let Some(slash) = tail.find('/') else { continue };
        let observed_fps = std::str::from_utf8(&bytes).ok().and_then(|s| s.parse().ok()).unwrap_or(0);
        out.push(ThrottleEdge {
            observer: tail[..slash].to_string(),
            sender: tail[slash + 1..].to_string(),
            observed_fps,
        });
    }
    out.sort_by(|a, b| (&a.observer, &a.sender).cmp(&(&b.observer, &b.sender)));
    out
}

/// The `GET /gateway/fleet` relational snapshot — the operator's "localize" view, **computed
/// locally from the gossiped KV this node already holds** (no collector; any node answers it;
/// survives killing any node). Carries the RT1/RT2 [`ViewConfidence`] header: this is a *per-node
/// best-effort estimate*, and at convergence the *diagnosis* fields (`governed_groups` conflicts,
/// `capability_coverage_gaps`) agree across nodes while `view_confidence` is each observer's own.
#[derive(Debug, Clone, Serialize)]
pub struct FleetSnapshot {
    pub observer:                 String,
    pub view_confidence:          ViewConfidence,
    pub governed_groups:          Vec<GroupStatus>,
    pub capability_coverage_gaps: Vec<String>,
    pub opaque_node_pct:          u64,
    pub opaque_pairs:             Vec<(String, String)>,
    pub membership_flaps:         u64,
    pub opacity_oscillations:     u64,
    pub throttle_graph:           Vec<ThrottleEdge>,
    /// This node's own store size + content hash. **Convergence-health self-report**: two nodes at
    /// convergence share a `store_hash`; an operator scraping every node diffs these (true
    /// cross-node divergence would need a gossiped `sys/health/` key — deferred, taxonomy §8).
    pub store_entries:            usize,
    pub store_hash:               u64,
    /// Cross-node store convergence health (spread of `sys/health/` entry counts). Populated when
    /// the detector loop is running cluster-wide (it publishes the reports); `nodes_reporting = 0`
    /// otherwise.
    pub store_convergence:        StoreConvergence,
    /// Cumulative consensus commit-conflict tripwire count.
    pub commit_conflicts:         u64,
    /// Per-slot commit-conflict counts — the "hot slots" (which consensus slots saw a conflicting
    /// COMMIT, and how often). Sorted by slot; empty when none.
    pub commit_conflict_slots:    Vec<(String, u64)>,
}

/// Assemble the current fleet snapshot from local KV. Deterministic given the same store (lists
/// sorted), so independent observers at convergence agree on the diagnosis. Available whether or
/// not the detector *loop* runs (the flap/oscillation counters read 0 when it doesn't).
pub fn compute_fleet_snapshot(ctx: &TaskCtx) -> FleetSnapshot {
    let now = now_ms();
    let live_nodes = ctx.peers.pin().len() + 1; // peers + self
    let mut opaque_pairs: Vec<(String, String)> =
        opacity_pairs(&ctx.kv_state, now, OPAQUE_MAX_AGE_MS).into_iter().collect();
    opaque_pairs.sort();
    let mut gaps = detect_coverage_gaps(&ctx.kv_state, now);
    gaps.sort();
    FleetSnapshot {
        observer:                 ctx.node_id.to_string(),
        view_confidence:          compute_view_confidence(ctx),
        governed_groups:          governed_group_statuses(&ctx.kv_state, now),
        capability_coverage_gaps: gaps,
        opaque_node_pct:          opaque_node_pct(&ctx.kv_state, live_nodes, now, OPAQUE_MAX_AGE_MS),
        opaque_pairs,
        membership_flaps:         ctx.membership_flaps.load(Ordering::Relaxed),
        opacity_oscillations:     ctx.opacity_oscillations.load(Ordering::Relaxed),
        throttle_graph:           throttle_graph(&ctx.kv_state),
        store_entries:            ctx.kv_state.store.pin().len(),
        store_hash:               mycelium_core::store::store_hash_acc(&ctx.kv_state.hash_acc),
        store_convergence:        store_convergence(&ctx.kv_state, now, HEALTH_MAX_AGE_MS),
        commit_conflicts:         ctx.commit_conflicts.load(Ordering::Relaxed),
        commit_conflict_slots:    {
            let mut v: Vec<(String, u64)> = ctx.commit_conflict_slots.pin().iter()
                .map(|(slot, n)| (slot.to_string(), *n)).collect();
            v.sort();
            v
        },
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
    // P3-explain: record a *significant event* whenever a detector's confirmed count changes
    // (onset/clear), not every tick — the event ring is the state-change history, not a firehose.
    let (mut prev_conf, mut prev_gaps, mut prev_flaps, mut prev_osc) = (0u64, 0u64, 0u64, 0u64);
    // P2 flap state: seed prev-membership at spawn so the initial roster is not counted as joins.
    let mut prev_membership = membership_snapshot(&ctx.kv_state);
    let mut flap_tracker = FlapTracker::default();
    // P3 oscillation state: seed prev-opacity at spawn likewise.
    let mut prev_opacity = opacity_pairs(&ctx.kv_state, now_ms(), OPAQUE_MAX_AGE_MS);
    let mut osc_tracker = FlapTracker::default();
    loop {
        tokio::select! {
            _ = tick.tick() => {
                let now = now_ms();
                // Publish this node's store self-report (Phase 2 cross-node convergence health).
                publish_health(&ctx, now);
                // P1 — governed-group conflict (hysteresis-confirmed).
                let conflicts = detect_governed_group_conflicts(&ctx.kv_state, now);
                let confirmed = confirm_conflicts(&conflicts, &mut conflict_streaks, CONFIRM_TICKS);
                ctx.governed_group_conflicts.store(confirmed, Ordering::Relaxed);
                // P6 — capability-coverage gap (hysteresis-confirmed; RT3 needs the sustained window
                // to tell a retracted provider from a merely-lapsed one).
                let gaps = detect_coverage_gaps(&ctx.kv_state, now);
                let confirmed_gaps = confirm_by_key(&gaps, &mut gap_streaks, CONFIRM_TICKS);
                ctx.capability_coverage_gaps.store(confirmed_gaps, Ordering::Relaxed);
                // P2 — failover flap: a (group,node) toggling membership faster than a settled
                // failover (the #56 "node count flapping with no signal why").
                let curr_membership = membership_snapshot(&ctx.kv_state);
                flap_tracker.record(&flap_transitions(&prev_membership, &curr_membership), now);
                let flaps = flap_tracker.flapping_count(now, FLAP_WINDOW_MS, FLAP_THRESHOLD);
                ctx.membership_flaps.store(flaps, Ordering::Relaxed);
                prev_membership = curr_membership;
                // P3 — opacity oscillation: a (node,kind) toggling opaque state (pheromone hunting).
                let curr_opacity = opacity_pairs(&ctx.kv_state, now, OPAQUE_MAX_AGE_MS);
                osc_tracker.record(&set_transitions(&prev_opacity, &curr_opacity), now);
                let oscillations = osc_tracker.flapping_count(now, FLAP_WINDOW_MS, FLAP_THRESHOLD);
                ctx.opacity_oscillations.store(oscillations, Ordering::Relaxed);
                prev_opacity = curr_opacity;
                // Record detector-state transitions into the event ring (the "explain" history).
                if confirmed != prev_conf {
                    // Name the specific group + its band vs live count, so the assembled narrative
                    // reads the #56 beat ("governor capped at N, observed M") with no code knowledge.
                    let detail = if confirmed > prev_conf {
                        let which = conflicts.iter().map(|c| format!(
                            "{}: {} live vs band [{}, {}]",
                            c.group, c.observed, c.min,
                            c.max.map(|m| m.to_string()).unwrap_or_else(|| "∞".into()),
                        )).collect::<Vec<_>>().join("; ");
                        format!("{prev_conf} → {confirmed} — group(s) now outside the governor band: {which}")
                    } else {
                        format!("{prev_conf} → {confirmed} — a group returned inside its governor band")
                    };
                    record_event(&ctx, "governed_group_conflict", detail);
                    prev_conf = confirmed;
                }
                if confirmed_gaps != prev_gaps {
                    record_event(&ctx, "capability_coverage_gap", format!("coverage gaps {prev_gaps} → {confirmed_gaps}"));
                    prev_gaps = confirmed_gaps;
                }
                if flaps != prev_flaps {
                    record_event(&ctx, "membership_flap", format!("flapping pairs {prev_flaps} → {flaps}"));
                    prev_flaps = flaps;
                }
                if oscillations != prev_osc {
                    record_event(&ctx, "opacity_oscillation", format!("oscillating pairs {prev_osc} → {oscillations}"));
                    prev_osc = oscillations;
                }
                // The loop is the periodic emitter for the `/metrics` surface (Prometheus scrapes a
                // registry, so gauges must be *set* on a tick, not computed on scrape). Emitted with
                // the RT1/RT2 view-confidence gauges so an operator's alert can qualify a diagnostic
                // by the observer's own view health (`peers_heard` ≪ `peers_known` ⇒ partial view).
                #[cfg(feature = "metrics")]
                {
                    metrics::gauge!("mycelium_emergent_governed_group_conflicts").set(confirmed as f64);
                    metrics::gauge!("mycelium_emergent_capability_coverage_gaps").set(confirmed_gaps as f64);
                    metrics::gauge!("mycelium_emergent_membership_flaps").set(flaps as f64);
                    metrics::gauge!("mycelium_emergent_opacity_oscillations").set(oscillations as f64);
                    metrics::gauge!("mycelium_emergent_opaque_node_pct").set(compute_opaque_node_pct(&ctx) as f64);
                    let vc = compute_view_confidence(&ctx);
                    metrics::gauge!("mycelium_emergent_peers_heard").set(vc.peers_heard as f64);
                    metrics::gauge!("mycelium_emergent_peers_known").set(vc.peers_known as f64);
                    metrics::gauge!("mycelium_emergent_max_staleness_ms").set(vc.max_staleness_ms as f64);
                    // Consensus/lock family: mirror the `/stats`-only tripwire scalars onto the
                    // Prometheus surface so consensus is alertable like every other pathology
                    // (the `_timeouts_total` counter is event-emitted from `consensus.rs`).
                    metrics::gauge!("mycelium_consensus_commit_conflicts")
                        .set(ctx.commit_conflicts.load(Ordering::Relaxed) as f64);
                    metrics::gauge!("mycelium_schema_mismatch")
                        .set(ctx.schema_mismatch.load(Ordering::Relaxed) as f64);
                }
            }
            _ = shutdown.wait_for(|v| *v) => break,
        }
    }
}

// ── Phase 4: the fleet narrative — "why is the fleet in this state" ────────────────────────────
//
// A templated rule engine over the Phase-2 snapshot (+ the Phase-3 counters it carries). Where the
// snapshot *localizes* a pathology and the explain ring *sequences* it, the diagnosis *names the
// cause* in terms an on-call engineer who did NOT build the system can act on. One rule per Phase-0
// pathology; each fires only when its condition holds, and the throttle graph supplies the *because*
// for opacity. RT1/RT2: every diagnosis is qualified by the observer's own view health.

/// Fleet-opacity fraction (percent) at or above which opacity is a *storm* (Critical) rather than
/// incidental (Warning) — one third of the fleet shedding is the point at which work visibly pools.
const OPACITY_STORM_PCT: u64 = 34;

/// Severity of a [`Finding`] — orders the diagnosis (most severe first) and colours an operator
/// alert. Only the two tiers the current rules emit; an informational tier can be added with its
/// first user.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum Severity {
    /// A pathology an operator should look at.
    Warning,
    /// A pathology actively degrading the fleet.
    Critical,
}

/// One diagnosed condition: a stable `pathology` id, its `severity`, and the `cause` — a
/// code-free, actionable sentence (the Phase-4 bar).
#[derive(Debug, Clone, Serialize)]
pub struct Finding {
    pub pathology: String,
    pub severity:  Severity,
    pub cause:     String,
}

/// The fleet diagnosis: the observer, a one-line `summary`, the `findings` (most severe first), and
/// an RT1/RT2 `caveat` when the observer's own view is partial or degraded (so a clean diagnosis
/// from a blind node is never mistaken for a healthy fleet).
#[derive(Debug, Clone, Serialize)]
pub struct FleetDiagnosis {
    pub observer: String,
    pub summary:  String,
    pub findings: Vec<Finding>,
    pub caveat:   Option<String>,
}

/// **Pure** — diagnose the fleet from a snapshot. Deterministic given the same snapshot, so
/// independent observers at convergence produce the same diagnosis (their `caveat` differs — it is
/// each observer's own view health). One rule per Phase-0 pathology; a healthy fleet yields no
/// findings and a "nominal" summary.
pub fn diagnose_fleet(s: &FleetSnapshot) -> FleetDiagnosis {
    let mut findings = Vec::new();

    // P1 (+P2) — governed-group conflict, escalated to the #56 thrash when membership is flapping.
    for g in s.governed_groups.iter().filter(|g| g.conflict) {
        let band = format!("[{}, {}]", g.min, g.max.map(|m| m.to_string()).unwrap_or_else(|| "∞".into()));
        if s.membership_flaps > 0 {
            findings.push(Finding {
                pathology: "governed_group_thrash".into(),
                severity:  Severity::Critical,
                cause: format!(
                    "Group '{}': live membership {} is outside the governor's band {} AND membership \
                     is flapping ({} pair(s) fleet-wide). This is the governor-vs-auto-join thrash: \
                     the governor caps the group while auto-join keeps re-adding nodes. Action: align \
                     the governor intent with the intended size, or pause auto-join for this group.",
                    g.group, g.observed, band, s.membership_flaps,
                ),
            });
        } else {
            findings.push(Finding {
                pathology: "governed_group_conflict".into(),
                severity:  Severity::Warning,
                cause: format!(
                    "Group '{}': live membership {} is outside the governor's band {} (steady, not \
                     flapping). Action: reconcile the governor intent with the actual pool size.",
                    g.group, g.observed, band,
                ),
            });
        }
    }

    // P4 — fleet-opacity storm. The throttle graph names *why* (which senders are being rate-limited).
    if s.opaque_node_pct > 0 {
        let because = if s.throttle_graph.is_empty() {
            String::new()
        } else {
            let edges = s.throttle_graph.iter().take(3)
                .map(|e| format!("{}→{} @ {} fps", e.sender, e.observer, e.observed_fps))
                .collect::<Vec<_>>().join(", ");
            format!(" Rate-limited edges (the likely reason): {edges}.")
        };
        if s.opaque_node_pct >= OPACITY_STORM_PCT {
            findings.push(Finding {
                pathology: "opacity_storm".into(),
                severity:  Severity::Critical,
                cause: format!(
                    "Opacity storm: {}% of the fleet is opaque (overloaded / shedding load), so work \
                     pools onto the non-opaque nodes.{} Action: add capacity, or raise the rate \
                     limits that are shedding.",
                    s.opaque_node_pct, because,
                ),
            });
        } else {
            findings.push(Finding {
                pathology: "opacity_present".into(),
                severity:  Severity::Warning,
                cause: format!(
                    "{}% of the fleet is opaque (some nodes are shedding load).{} Action: watch for it \
                     spreading; check the rate limits on the opaque nodes.",
                    s.opaque_node_pct, because,
                ),
            });
        }
    }

    // P6 — capability-coverage gap.
    if !s.capability_coverage_gaps.is_empty() {
        findings.push(Finding {
            pathology: "capability_coverage_gap".into(),
            severity:  Severity::Warning,
            cause: format!(
                "No provider visible for demand(s): {}. Consumers of these capabilities will stall. \
                 Action: check whether the providers crashed or were never deployed; (re-)advertise \
                 the capability. NB: 'not visible from here' — a partitioned provider looks identical.",
                s.capability_coverage_gaps.join(", "),
            ),
        });
    }

    // P3 — opacity oscillation.
    if s.opacity_oscillations > 0 {
        findings.push(Finding {
            pathology: "opacity_oscillation".into(),
            severity:  Severity::Warning,
            cause: format!(
                "{} node/kind pair(s) are oscillating in and out of the overload state (unstable \
                 back-pressure — the load sits right at a rate threshold). Action: widen the rate \
                 hysteresis or smooth the offered load so nodes settle.",
                s.opacity_oscillations,
            ),
        });
    }

    // Consensus commit conflicts (the tripwire's hot slots).
    if !s.commit_conflict_slots.is_empty() {
        let slots = s.commit_conflict_slots.iter().take(5)
            .map(|(slot, n)| format!("{slot} (×{n})")).collect::<Vec<_>>().join(", ");
        findings.push(Finding {
            pathology: "commit_conflict".into(),
            severity:  Severity::Critical,
            cause: format!(
                "Consensus commit conflicts on slot(s): {slots}. Two proposals committed for the same \
                 slot — a sign of split-brain proposing or a partition healing. Action: check \
                 consensus membership and whether the cluster recently rejoined after a split.",
            ),
        });
    }

    findings.sort_by_key(|f| std::cmp::Reverse(f.severity));

    let summary = if findings.is_empty() {
        "Fleet nominal — no emergent pathologies detected from this observer's view.".into()
    } else {
        let crit = findings.iter().filter(|f| f.severity == Severity::Critical).count();
        let warn = findings.iter().filter(|f| f.severity == Severity::Warning).count();
        let mut parts = Vec::new();
        if crit > 0 { parts.push(format!("{crit} critical")); }
        if warn > 0 { parts.push(format!("{warn} warning")); }
        format!("{} condition(s) detected ({}).", findings.len(), parts.join(", "))
    };

    // RT1/RT2 — qualify the diagnosis by the observer's own view health. A clean diagnosis from a
    // blind or degraded node must NOT read as "the fleet is healthy".
    let vc = &s.view_confidence;
    let mut caveats = Vec::new();
    if vc.self_degraded {
        caveats.push("this observer is itself opaque/shedding, so its own inputs may be degraded".into());
    }
    if vc.peers_heard < vc.peers_known {
        caveats.push(format!(
            "partial view — heard {} of {} peers (stalest {} ms); pathologies on unheard nodes are \
             invisible from here",
            vc.peers_heard, vc.peers_known, vc.max_staleness_ms,
        ));
    }
    let caveat = if caveats.is_empty() { None } else { Some(format!("⚠ {}.", caveats.join("; "))) };

    FleetDiagnosis { observer: s.observer.clone(), summary, findings, caveat }
}

/// Compute this node's fleet diagnosis from local KV (snapshot → rule engine). The
/// `GET /gateway/diagnose` surface.
pub fn compute_fleet_diagnosis(ctx: &TaskCtx) -> FleetDiagnosis {
    diagnose_fleet(&compute_fleet_snapshot(ctx))
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

    /// Audit 2026-07-15 pass 5: the % must be over a CONSISTENT population. A blind observer (small
    /// roster) that holds fresh `sys/load/` entries for more nodes than its roster used to pin the
    /// gauge at 100% (numerator from unbounded KV, denominator = roster only) → a false Critical
    /// `opacity_storm`. Now `denom = max(roster, seen)`, so it's a real proportion.
    #[test]
    fn regression_opaque_pct_consistent_population_not_pinned_at_100() {
        let kv = KvState::new(0);
        let hlc = Hlc::new();
        let now = 1_000_000_000_000;
        seed_opacity(&kv, &hlc, &NodeId::new("127.0.0.1", 40001).unwrap(), "work", true,  now);
        seed_opacity(&kv, &hlc, &NodeId::new("127.0.0.1", 40002).unwrap(), "work", false, now);
        seed_opacity(&kv, &hlc, &NodeId::new("127.0.0.1", 40003).unwrap(), "work", false, now);
        // Blind observer: roster (live_nodes) = 1. Pre-fix: 1*100/1 = 100. Post-fix: max(1,3 seen)=3 → 33.
        assert_eq!(opaque_node_pct(&kv, 1, now, OPAQUE_MAX_AGE_MS), 33,
            "% must be over the observed population (1 of 3), not pinned at 100% by roster mismatch");
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

    fn snap(pairs: &[(&str, &[&str])]) -> MembershipSnapshot {
        pairs.iter()
            .map(|(g, ns)| (g.to_string(), ns.iter().map(|n| n.to_string()).collect()))
            .collect()
    }

    /// P2 — a join and a leave between snapshots are both transitions.
    #[test]
    fn flap_transitions_detects_join_and_leave() {
        let prev = snap(&[("g", &["a", "b"])]);
        let curr = snap(&[("g", &["b", "c"])]); // a left, c joined
        let t = flap_transitions(&prev, &curr);
        assert_eq!(t.len(), 2);
        assert!(t.contains(&("g".to_string(), "a".to_string())));
        assert!(t.contains(&("g".to_string(), "c".to_string())));
        // No change ⇒ no transitions.
        assert!(flap_transitions(&curr, &curr).is_empty());
    }

    /// P2 flap gate — a node toggling ≥ threshold times in the window flaps; recovery ages out.
    #[test]
    fn flap_tracker_flags_sustained_toggling_and_ages_out() {
        let mut tr = FlapTracker::default();
        let pair: HashSet<(String, String)> =
            std::iter::once(("g".to_string(), "n".to_string())).collect();
        // 4 transitions within a 60 s window ⇒ flapping.
        for i in 0..4 {
            tr.record(&pair, 1_000 + i * 5_000);
        }
        assert_eq!(tr.flapping_count(1_000 + 3 * 5_000, FLAP_WINDOW_MS, FLAP_THRESHOLD), 1);
        // Well past the window with no new events ⇒ pruned, no longer flapping.
        assert_eq!(tr.flapping_count(1_000 + 3 * 5_000 + FLAP_WINDOW_MS + 1, FLAP_WINDOW_MS, FLAP_THRESHOLD), 0);
    }

    /// P2 false-positive gate — a single failover (1 transition) is not a flap.
    #[test]
    fn single_failover_is_not_a_flap() {
        let mut tr = FlapTracker::default();
        let pair: HashSet<(String, String)> =
            std::iter::once(("g".to_string(), "n".to_string())).collect();
        tr.record(&pair, 1_000);
        assert_eq!(tr.flapping_count(2_000, FLAP_WINDOW_MS, FLAP_THRESHOLD), 0, "one join is not a flap");
    }

    /// P3 — opacity_pairs picks up fresh-opaque (node,kind), ignores non-opaque and stale.
    #[test]
    fn opacity_pairs_selects_fresh_opaque() {
        let kv = KvState::new(0);
        let hlc = Hlc::new();
        let now = 1_000_000_000_000;
        let a = NodeId::new("127.0.0.1", 50000).unwrap();
        let b = NodeId::new("127.0.0.1", 50001).unwrap();
        seed_opacity(&kv, &hlc, &a, "work", true, now);                    // fresh opaque ✓
        seed_opacity(&kv, &hlc, &b, "work", false, now);                   // not opaque ✗
        seed_opacity(&kv, &hlc, &a, "sync", true, now - OPAQUE_MAX_AGE_MS - 1); // stale ✗
        let pairs = opacity_pairs(&kv, now, OPAQUE_MAX_AGE_MS);
        assert_eq!(pairs, std::iter::once((a.to_string(), "work".to_string())).collect());
    }

    /// P3 — set_transitions is the symmetric difference (appeared or disappeared).
    #[test]
    fn set_transitions_is_symmetric_difference() {
        let prev: HashSet<(String, String)> = [("n".into(), "a".into())].into_iter().collect();
        let curr: HashSet<(String, String)> = [("n".into(), "b".into())].into_iter().collect();
        let t = set_transitions(&prev, &curr);
        assert_eq!(t.len(), 2); // a disappeared, b appeared
        assert!(set_transitions(&curr, &curr).is_empty());
    }

    /// Phase 2 — the relational governed-group view reports *every* fresh group with its status
    /// (conflict flagged), sorted deterministically so independent nodes at convergence agree.
    #[test]
    fn governed_group_statuses_reports_all_groups_sorted() {
        let kv = KvState::new(0);
        let hlc = Hlc::new();
        let now = 1_000_000_000_000;
        seed_intent(&kv, &hlc, "zeta", 2, Some(8), now);   // out of order + in-bounds
        seed_members(&kv, &hlc, "zeta", 5);
        seed_intent(&kv, &hlc, "alpha", 1, Some(2), now);  // over max ⇒ conflict
        seed_members(&kv, &hlc, "alpha", 4);

        let s = governed_group_statuses(&kv, now);
        assert_eq!(s.len(), 2);
        assert_eq!(s[0].group, "alpha"); // sorted
        assert!(s[0].conflict, "alpha: 4 > max 2 ⇒ conflict");
        assert_eq!(s[0].observed, 4);
        assert_eq!(s[1].group, "zeta");
        assert!(!s[1].conflict, "zeta: 5 ∈ [2,8] ⇒ no conflict");
    }

    /// Phase 2 — the throttle graph reports `sys/rate/` observer→sender edges, sorted.
    #[test]
    fn throttle_graph_reports_rate_edges_sorted() {
        let kv = KvState::new(0);
        let hlc = Hlc::new();
        let obs = NodeId::new("127.0.0.1", 60000).unwrap();
        let put = |o: &str, sender: &str, fps: &str| {
            let key: Arc<str> = Arc::from(format!("sys/rate/{o}/{sender}").as_str());
            apply_and_notify(&kv, &make_gossip_update(
                &obs, 4, key, Bytes::copy_from_slice(fps.as_bytes()), false, &hlc));
        };
        put("nodeB", "flooder", "9000");
        put("nodeA", "flooder", "8000");
        let g = throttle_graph(&kv);
        assert_eq!(g.len(), 2);
        assert_eq!(g[0].observer, "nodeA"); // sorted by (observer, sender)
        assert_eq!(g[0].sender, "flooder");
        assert_eq!(g[0].observed_fps, 8000);
        assert_eq!(g[1].observer, "nodeB");
    }

    /// Phase 3 — the event ring is bounded (oldest dropped), and `since` returns HLC-ordered events
    /// at or after the cursor.
    #[test]
    fn event_ring_is_bounded_and_since_filters_in_order() {
        let ring = EventRing::default();
        // Insert out of HLC order; `since` must sort.
        ring.record(Event { hlc: 30, node: "n".into(), kind: "k".into(), detail: "c".into() });
        ring.record(Event { hlc: 10, node: "n".into(), kind: "k".into(), detail: "a".into() });
        ring.record(Event { hlc: 20, node: "n".into(), kind: "k".into(), detail: "b".into() });
        let all = ring.since(0);
        assert_eq!(all.iter().map(|e| e.hlc).collect::<Vec<_>>(), vec![10, 20, 30], "HLC-sorted");
        assert_eq!(ring.since(20).iter().map(|e| e.hlc).collect::<Vec<_>>(), vec![20, 30], "since filters");

        // Bounded: fill past capacity, oldest dropped.
        let ring = EventRing::default();
        for i in 0..(EVENT_RING_CAP as u64 + 50) {
            ring.record(Event { hlc: i, node: "n".into(), kind: "k".into(), detail: String::new() });
        }
        let all = ring.since(0);
        assert_eq!(all.len(), EVENT_RING_CAP, "ring is fixed-memory");
        assert_eq!(all[0].hlc, 50, "oldest 50 dropped");
    }

    /// Phase 2 — store-convergence reports the spread of fresh `sys/health/` entry counts; a stale
    /// report is excluded (a behind/gone node stops counting).
    #[test]
    fn store_convergence_reports_entry_spread_and_ignores_stale() {
        let kv = KvState::new(0);
        let hlc = Hlc::new();
        let now = 1_000_000_000_000;
        let put = |node: &str, entries: usize, at: u64| {
            let r = HealthReport { store_entries: entries, written_at_ms: at };
            let key: Arc<str> = Arc::from(format!("sys/health/{node}").as_str());
            let n = NodeId::new("127.0.0.1", 9000).unwrap();
            apply_and_notify(&kv, &make_gossip_update(
                &n, 4, key, mycelium_core::serde_fixint::to_vec(&r).unwrap().into(), false, &hlc));
        };
        put("a", 100, now);
        put("b", 40, now); // behind ⇒ spread
        put("c", 100, now - HEALTH_MAX_AGE_MS - 1); // stale ⇒ excluded
        let sc = store_convergence(&kv, now, HEALTH_MAX_AGE_MS);
        assert_eq!(sc.nodes_reporting, 2, "stale report excluded");
        assert_eq!(sc.min_entries, 40);
        assert_eq!(sc.max_entries, 100);
    }

    #[test]
    fn narrate_renders_the_56_sequence_legibly() {
        // The #56 story as an assembled cross-node ring: governor cap exceeded → node flaps →
        // returns to band. `narrate` must render it operator-legibly, in order, with the specific
        // group/band surviving and no raw `kind` string leaking into the story.
        let events = vec![
            Event { hlc: 10, node: "n1".into(), kind: "governed_group_conflict".into(),
                detail: "0 → 1 — group(s) now outside the governor band: workers: 4 live vs band [1, 2]".into() },
            Event { hlc: 20, node: "n2".into(), kind: "membership_flap".into(),
                detail: "flapping pairs 0 → 1".into() },
            Event { hlc: 30, node: "n1".into(), kind: "governed_group_conflict".into(),
                detail: "1 → 0 — a group returned inside its governor band".into() },
        ];
        let story = narrate(&events);
        assert_eq!(story.len(), 3);
        // HLC-ordered, one line per event.
        assert!(story[0].contains("[hlc 10]"));
        assert!(story[1].contains("[hlc 20]"));
        assert!(story[2].contains("[hlc 30]"));
        // Legible: terse kind glossed to plain English; the specific group + band survive.
        assert!(story[0].contains("governor's [min,max] band"), "conflict glossed: {}", story[0]);
        assert!(story[0].contains("workers: 4 live vs band [1, 2]"), "group/band named: {}", story[0]);
        assert!(story[1].contains("repeatedly joining and leaving"), "flap glossed: {}", story[1]);
        // No raw event-kind identifier leaks into the operator narrative.
        assert!(!story.iter().any(|l| l.contains("governed_group_conflict") || l.contains("membership_flap")),
            "raw kind strings must not appear in the narrative: {story:?}");
    }

    #[test]
    fn narrate_surfaces_unknown_kinds_rather_than_dropping_them() {
        // A newly-added detector with no gloss must still appear (raw kind), never be silently lost.
        let events = vec![Event {
            hlc: 1, node: "n".into(), kind: "some_future_detector".into(), detail: "x".into(),
        }];
        let story = narrate(&events);
        assert_eq!(story.len(), 1);
        assert!(story[0].contains("some_future_detector"), "unknown kind surfaced: {}", story[0]);
    }

    #[test]
    fn select_explain_targets_caps_the_fanout_and_names_the_remainder() {
        // Over the cap: exactly `cap` targets fan out, the rest are counted (never silently dropped),
        // and the subset is deterministic (stable across repeated queries).
        let peers: Vec<NodeId> = (0..40)
            .map(|i| NodeId::new("127.0.0.1", 20_000 + i).unwrap()).collect();
        let (targets, skipped) = select_explain_targets(peers.clone(), EXPLAIN_MAX_FANOUT);
        assert_eq!(targets.len(), EXPLAIN_MAX_FANOUT, "fan-out is capped");
        assert_eq!(skipped, 40 - EXPLAIN_MAX_FANOUT, "the remainder is named, not dropped");
        let (targets2, _) = select_explain_targets(peers, EXPLAIN_MAX_FANOUT);
        assert_eq!(targets, targets2, "the capped subset is deterministic");

        // Under the cap: everyone is queried, nothing skipped.
        let few: Vec<NodeId> = (0..3)
            .map(|i| NodeId::new("127.0.0.1", 21_000 + i).unwrap()).collect();
        let (t, s) = select_explain_targets(few, EXPLAIN_MAX_FANOUT);
        assert_eq!(t.len(), 3);
        assert_eq!(s, 0);
    }

    /// **Probe — Architecture/Scalability (analysis Run 31, the capped fan-out).** The explain
    /// fan-out target set is bounded by the cap for *any* fleet size (never an O(N) RPC storm), the
    /// remainder is always accounted for (targets + skipped == N, nothing silently dropped), and the
    /// selection is deterministic — an observer picks the same subset every time, no coordination.
    #[test]
    fn probe_r31_explain_fanout_is_bounded_and_deterministic_for_any_fleet() {
        for n in [0usize, 1, 31, 32, 33, 100, 1000, 4000] {
            // Distinct identities via unique host octets (avoids port wraparound at large n).
            let peers: Vec<NodeId> = (0..n)
                .map(|i| NodeId::new(&format!("10.{}.{}.{}", (i / 65536) % 256, (i / 256) % 256, i % 256), 9000).unwrap())
                .collect();
            let (targets, skipped) = select_explain_targets(peers.clone(), EXPLAIN_MAX_FANOUT);
            assert!(targets.len() <= EXPLAIN_MAX_FANOUT, "n={n}: fan-out bounded by the cap");
            assert_eq!(targets.len() + skipped, n, "n={n}: remainder accounted, nothing dropped");
            // Deterministic: the same peer set yields the same subset.
            let (targets2, _) = select_explain_targets(peers, EXPLAIN_MAX_FANOUT);
            assert_eq!(targets, targets2, "n={n}: capped subset is deterministic");
        }
    }

    // ── Phase 4: fleet diagnosis (the "why is the fleet in this state" rule engine) ────────────

    /// A healthy-fleet snapshot with a full, current view. Tests mutate one axis at a time.
    fn nominal_snapshot() -> FleetSnapshot {
        FleetSnapshot {
            observer: "n1".into(),
            view_confidence: ViewConfidence {
                observer: "n1".into(), peers_known: 3, peers_heard: 3,
                max_staleness_ms: 0, self_degraded: false,
            },
            governed_groups: vec![],
            capability_coverage_gaps: vec![],
            opaque_node_pct: 0,
            opaque_pairs: vec![],
            membership_flaps: 0,
            opacity_oscillations: 0,
            throttle_graph: vec![],
            store_entries: 10,
            store_hash: 0,
            store_convergence: StoreConvergence { nodes_reporting: 3, min_entries: 10, max_entries: 10 },
            commit_conflicts: 0,
            commit_conflict_slots: vec![],
        }
    }

    #[test]
    fn diagnose_healthy_fleet_is_nominal_with_no_findings() {
        let d = diagnose_fleet(&nominal_snapshot());
        assert!(d.findings.is_empty());
        assert!(d.summary.to_lowercase().contains("nominal"), "summary: {}", d.summary);
        assert!(d.caveat.is_none(), "a full current view has no caveat");
    }

    #[test]
    fn diagnose_names_the_56_thrash_actionably() {
        // Governed-group conflict + flapping membership = the #56 governor-vs-auto-join thrash.
        let mut s = nominal_snapshot();
        s.governed_groups = vec![GroupStatus {
            group: "workers".into(), min: 1, max: Some(2), observed: 4, conflict: true,
        }];
        s.membership_flaps = 1;
        let d = diagnose_fleet(&s);
        let f = d.findings.iter().find(|f| f.pathology == "governed_group_thrash")
            .expect("thrash diagnosed");
        assert_eq!(f.severity, Severity::Critical);
        // Names the group, the band, the observed count, and an action — no code jargon.
        assert!(f.cause.contains("workers") && f.cause.contains("[1, 2]") && f.cause.contains('4'));
        assert!(f.cause.contains("Action:"), "actionable: {}", f.cause);
        assert!(f.cause.to_lowercase().contains("auto-join"), "names the cause: {}", f.cause);
    }

    #[test]
    fn diagnose_opacity_storm_names_the_throttle_reason() {
        // A storm-level opacity fraction + a throttle edge: the diagnosis must name *why* (the edge).
        let mut s = nominal_snapshot();
        s.opaque_node_pct = 50;
        s.throttle_graph = vec![ThrottleEdge {
            observer: "n7".into(), sender: "n3".into(), observed_fps: 5,
        }];
        let d = diagnose_fleet(&s);
        let f = d.findings.iter().find(|f| f.pathology == "opacity_storm").expect("storm diagnosed");
        assert_eq!(f.severity, Severity::Critical);
        assert!(f.cause.contains("50%"), "names the fraction: {}", f.cause);
        assert!(f.cause.contains("n3→n7") && f.cause.contains("5 fps"), "names the throttle reason: {}", f.cause);
    }

    #[test]
    fn diagnose_coverage_gap_is_flagged_and_partial_view_caveated() {
        let mut s = nominal_snapshot();
        s.capability_coverage_gaps = vec!["ai/llm".into()];
        // Observer hears only 1 of 3 peers ⇒ RT1/RT2 caveat.
        s.view_confidence.peers_heard = 1;
        s.view_confidence.max_staleness_ms = 9_000;
        let d = diagnose_fleet(&s);
        assert!(d.findings.iter().any(|f| f.pathology == "capability_coverage_gap"
            && f.cause.contains("ai/llm") && f.cause.contains("Action:")));
        let caveat = d.caveat.expect("partial view is caveated");
        assert!(caveat.contains("heard 1 of 3"), "caveat names the partial view: {caveat}");
    }

    #[test]
    fn diagnose_orders_findings_most_severe_first() {
        // A warning (coverage gap) plus a critical (commit conflict): critical must sort first.
        let mut s = nominal_snapshot();
        s.capability_coverage_gaps = vec!["ai/llm".into()];
        s.commit_conflict_slots = vec![("slot-9".into(), 3)];
        let d = diagnose_fleet(&s);
        assert!(d.findings.len() >= 2);
        assert_eq!(d.findings[0].severity, Severity::Critical, "most-severe first");
        assert!(d.summary.contains("critical"), "summary counts severities: {}", d.summary);
    }
}
