//! Inter-group wiring + locality-aware resolution + capability ranking
//! (Phases 4, 5, and 6 of the locality/topology/capabilities plan).
//!
//! Two pull entry points (`resolve_wiring`, `resolve_with_locality`,
//! `resolve_wiring_with_locality`) and one push entry point
//! (`watch_wiring`) plus two send variants (`signal_wired_via`,
//! `signal_wired_via_locality`). All of them ultimately go through
//! [`wiring_snapshot`], which is the single point that walks `cap/`
//! and `gcap/`, applies the filter and ranking, and emits a
//! [`WiringStatus`].

use crate::capability::{
    CapEntry, Capability, CapFilter, CapRanking, CapValue, RankingOrder,
    WiredEmitOutcome, WiringProvider, WiringStatus, partial_cmp_cap,
};
use crate::locality::{LocalityPath, LocalityPreference};
use crate::node_id::NodeId;
use ahash::AHashMap;
use bytes::Bytes;
use std::sync::Arc;
use tracing::warn;

use super::TaskCtx;
use super::helpers::emit_signal;
use super::capability_ops::{
    is_cap_locality_key, now_ms, parse_cap_key_or_warn, scan_prefix_kv_with_ts,
};

/// Free-function variant of `dispatch_to_providers` for callers that hold
/// only `Arc<TaskCtx>` (e.g. `CapabilitiesHandle`).
pub(super) fn dispatch_to_providers_ctx(
    ctx:       &TaskCtx,
    kind:      Arc<str>,
    payload:   Bytes,
    providers: Vec<WiringProvider>,
) -> Vec<WiringProvider> {
    let mut emitted = Vec::with_capacity(providers.len());
    for provider in providers {
        let scope = match &provider {
            WiringProvider::Group { name, .. }    => crate::signal::SignalScope::Group(Arc::clone(name)),
            WiringProvider::Node  { node_id, .. } => crate::signal::SignalScope::Individual(node_id.clone()),
        };
        let _ = emit_signal(ctx, Arc::clone(&kind), scope, payload.clone());
        emitted.push(provider);
    }
    emitted
}

/// Free-function variant of [`GossipAgent::signal_wired_via`] for callers
/// that hold only `Arc<TaskCtx>` (e.g. `CapabilitiesHandle`).
///
/// When `pref` is `Some`, providers are annotated with locality depth and
/// filtered/sorted before dispatch (same logic as `signal_wired_via_locality`).
pub(super) fn signal_wired_via_ctx(
    ctx:     &TaskCtx,
    filter:  &CapFilter,
    kind:    Arc<str>,
    payload: Bytes,
    pref:    Option<crate::locality::LocalityPreference>,
) -> WiredEmitOutcome {
    let status = if let Some(pref) = pref {
        let self_loc = super::helpers::self_locality_ctx(ctx);
        let raw = wiring_snapshot(&ctx.kv_state, filter);
        let providers = match raw {
            WiringStatus::Wired { providers } => providers,
            WiringStatus::Unwired { filter }  => return WiredEmitOutcome::Unwired { filter },
        };
        let mut annotated: Vec<WiringProvider> = providers.into_iter()
            .map(|p| annotate_provider_with_locality(p, &ctx.kv_state, self_loc.as_ref()))
            .collect();
        apply_locality_pref(&mut annotated, pref, provider_depth);
        if annotated.is_empty() {
            WiringStatus::Unwired { filter: filter.clone() }
        } else {
            WiringStatus::Wired { providers: annotated }
        }
    } else {
        wiring_snapshot(&ctx.kv_state, filter)
    };
    let providers = match status {
        WiringStatus::Wired { providers } => providers,
        WiringStatus::Unwired { filter }  => return WiredEmitOutcome::Unwired { filter },
    };
    let emitted = dispatch_to_providers_ctx(ctx, kind, payload, providers);
    WiredEmitOutcome::Emitted { providers: emitted }
}

// ── Free helpers ─────────────────────────────────────────────────────────────

/// Computes the current [`WiringStatus`] for `filter` by scanning both
/// `cap/` (standalone providers) and `gcap/` (group projections). The
/// `shared_locality_depth` field is left as `0` here; locality is layered
/// on by `resolve_wiring_with_locality`.
///
/// When `filter.ranking` is set, providers are sorted by the named attribute
/// — Nodes by their own attribute value, Groups by the **best**-ranking
/// contributor's value (largest for `Descending`, smallest for `Ascending`).
/// Missing or incomparable values sort to the end deterministically.
pub(super) fn wiring_snapshot(
    kv_state: &crate::store::KvState,
    filter:   &CapFilter,
) -> WiringStatus {
    // (provider, sort_value_if_any) tuples — we keep the sort key alongside
    // each provider so the final sort doesn't need a second lookup.
    let mut keyed: Vec<(WiringProvider, Option<CapValue>)> = Vec::new();
    let now = now_ms();

    // Standalone-node providers from cap/.
    for (key, bytes, hlc_ts) in scan_prefix_kv_with_ts(kv_state, "cap/") {
        if is_cap_locality_key(&key) { continue; }
        let Some((node_id, _ns, _name)) = parse_cap_key_or_warn("cap/", &key) else { continue };
        let Some(cap_entry) = CapEntry::decode(&bytes)
            .or_else(|| Capability::decode(&bytes).map(|cap| CapEntry { capability: cap, refresh_interval_ms: 60_000 }))
        else {
            warn!(key = %key, "malformed Capability — peer sent bytes that did not decode");
            continue;
        };
        if !cap_entry.is_fresh(hlc_ts, now) { continue; }
        let cap = cap_entry.capability;
        if !filter.matches(&cap) { continue; }
        let sort_value = filter.ranking.as_ref()
            .and_then(|r| cap.attributes.get(&r.attribute).cloned());
        keyed.push((
            WiringProvider::Node { node_id, capability: cap, shared_locality_depth: 0 },
            sort_value,
        ));
    }

    // Group providers from gcap/. Key format: gcap/{group}/{ns}/{name}/{contributor}.
    // One matching contributor entry is enough for the group to count as a
    // provider; we collect every contributor so callers can observe partial
    // coverage if they want. The per-group best ranking value is the
    // most-ranking-favoured value across contributors.
    let mut groups: AHashMap<Arc<str>, (Vec<NodeId>, Option<CapValue>)> = AHashMap::new();
    for (key, bytes, hlc_ts) in scan_prefix_kv_with_ts(kv_state, "gcap/") {
        let Some((group, contributor)) = parse_gcap_key(&key, filter) else { continue };
        let Some(cap_entry) = CapEntry::decode(&bytes)
            .or_else(|| Capability::decode(&bytes).map(|cap| CapEntry { capability: cap, refresh_interval_ms: 60_000 }))
        else {
            warn!(key = %key, "malformed Capability — peer sent bytes that did not decode");
            continue;
        };
        if !cap_entry.is_fresh(hlc_ts, now) { continue; }
        let cap = cap_entry.capability;
        if !filter.matches(&cap) { continue; }
        let candidate = filter.ranking.as_ref()
            .and_then(|r| cap.attributes.get(&r.attribute).cloned());
        let entry = groups.entry(group).or_default();
        entry.0.push(contributor);
        if let Some(ranking) = &filter.ranking {
            entry.1 = better_value(entry.1.take(), candidate, ranking.order);
        }
    }
    for (name, (contributors, sort_value)) in groups {
        keyed.push((
            WiringProvider::Group { name, contributors, shared_locality_depth: 0 },
            sort_value,
        ));
    }

    if let Some(ranking) = &filter.ranking {
        keyed.sort_by(|a, b| cmp_optional_capvalues(&a.1, &b.1, ranking.order));
    }

    let providers: Vec<WiringProvider> = keyed.into_iter().map(|(p, _)| p).collect();
    if providers.is_empty() {
        WiringStatus::Unwired { filter: filter.clone() }
    } else {
        WiringStatus::Wired { providers }
    }
}

/// Ranks `(NodeId, Capability)` matches from `resolve()` by `ranking.attribute`.
/// Stable sort; entries missing the ranked attribute land at the end.
pub(super) fn rank_node_matches(matches: &mut [(NodeId, Capability)], ranking: &CapRanking) {
    matches.sort_by(|a, b| {
        let av = a.1.attributes.get(&ranking.attribute);
        let bv = b.1.attributes.get(&ranking.attribute);
        cmp_optional_capvalues(&av.cloned(), &bv.cloned(), ranking.order)
    });
}

/// Picks the more-favoured `CapValue` under `order`. Used to track each group's
/// best ranking value as we scan `gcap/` contributors.
fn better_value(current: Option<CapValue>, candidate: Option<CapValue>, order: RankingOrder) -> Option<CapValue> {
    match (current, candidate) {
        (None, c)            => c,
        (Some(prev), None)   => Some(prev),
        (Some(prev), Some(c)) => {
            let cmp = partial_cmp_cap(&c, &prev).unwrap_or(std::cmp::Ordering::Equal);
            let prefer_new = match order {
                RankingOrder::Descending => cmp == std::cmp::Ordering::Greater,
                RankingOrder::Ascending  => cmp == std::cmp::Ordering::Less,
            };
            Some(if prefer_new { c } else { prev })
        }
    }
}

/// Compares two optional `CapValue`s under `order`. Some-vs-None: Some wins
/// (entries with the ranked attribute sort before entries without). Some-vs-Some
/// that are incomparable (e.g. across types, or `Float(NaN)` vs a finite float)
/// fall through to `Ordering::Equal` so the surrounding stable sort preserves
/// insertion order — deterministic across runs.
fn cmp_optional_capvalues(
    a:     &Option<CapValue>,
    b:     &Option<CapValue>,
    order: RankingOrder,
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Some(av), Some(bv)) => {
            let raw = partial_cmp_cap(av, bv).unwrap_or(Ordering::Equal);
            match order {
                RankingOrder::Ascending  => raw,
                RankingOrder::Descending => raw.reverse(),
            }
        }
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None)    => Ordering::Equal,
    }
}

/// Looks up `peer_localities[node]` and returns `shared_prefix_len` against
/// `self_loc`. Returns `0` when either side has no known locality — that's
/// the correct "no known sharing" answer and matches the semantics of
/// `LocalityPath::shared_prefix_len` when one path is empty.
pub(super) fn locality_depth(
    kv_state: &crate::store::KvState,
    self_loc: Option<&LocalityPath>,
    node:     &NodeId,
) -> usize {
    let Some(self_loc) = self_loc else { return 0; };
    let guard = kv_state.peer_localities.pin();
    match guard.get(node) {
        Some(other) => self_loc.shared_prefix_len(other),
        None        => 0,
    }
}

/// Replaces a provider's `shared_locality_depth` with the value computed
/// against our own locality. For groups, the value is the **maximum** across
/// the listed contributors.
pub(super) fn annotate_provider_with_locality(
    provider: WiringProvider,
    kv_state: &crate::store::KvState,
    self_loc: Option<&LocalityPath>,
) -> WiringProvider {
    match provider {
        WiringProvider::Node { node_id, capability, .. } => {
            let depth = locality_depth(kv_state, self_loc, &node_id);
            WiringProvider::Node { node_id, capability, shared_locality_depth: depth }
        }
        WiringProvider::Group { name, contributors, .. } => {
            let depth = contributors.iter()
                .map(|n| locality_depth(kv_state, self_loc, n))
                .max()
                .unwrap_or(0);
            WiringProvider::Group { name, contributors, shared_locality_depth: depth }
        }
    }
}

#[inline]
pub(super) fn provider_depth(p: &WiringProvider) -> usize {
    match p {
        WiringProvider::Node  { shared_locality_depth, .. } => *shared_locality_depth,
        WiringProvider::Group { shared_locality_depth, .. } => *shared_locality_depth,
    }
}

/// Applies a [`LocalityPreference`] to an annotated provider list in place.
/// `depth_of` extracts the integer depth from each element. Stable sort so
/// providers of equal depth retain their original ordering (scan order).
pub(super) fn apply_locality_pref<T, F>(items: &mut Vec<T>, pref: LocalityPreference, depth_of: F)
where
    F: Fn(&T) -> usize,
{
    match pref {
        LocalityPreference::Any => {}
        LocalityPreference::PreferShared(_) => {
            items.sort_by_key(|item| std::cmp::Reverse(depth_of(item)));
        }
        LocalityPreference::Strict(threshold) => {
            items.retain(|item| depth_of(item) >= threshold);
            items.sort_by_key(|item| std::cmp::Reverse(depth_of(item)));
        }
    }
}

// ── gcap/ key parsing ────────────────────────────────────────────────────────

/// Parsed shape of a `gcap/{group}/{ns}/{name}/{contributor}` key.
struct GcapKeyShape {
    group:       Arc<str>,
    namespace:   Arc<str>,
    name:        Arc<str>,
    contributor: NodeId,
}

/// Parses the shape of a `gcap/` key. Returns `None` only when the key is
/// genuinely malformed (wrong segment count, contributor not a `NodeId`, etc.).
/// Filter matching is a separate concern handled by the caller.
fn parse_gcap_key_shape(key: &str) -> Option<GcapKeyShape> {
    let rest = key.strip_prefix("gcap/")?;
    let mut parts = rest.splitn(4, '/');
    let group       = parts.next()?;
    let namespace   = parts.next()?;
    let name        = parts.next()?;
    let contributor = parts.next()?;
    if contributor.contains('/') { return None; }
    let contributor = contributor.parse::<NodeId>().ok()?;
    Some(GcapKeyShape {
        group:       Arc::from(group),
        namespace:   Arc::from(namespace),
        name:        Arc::from(name),
        contributor,
    })
}

/// Splits `gcap/{group}/{ns}/{name}/{contributor}` into `(group, contributor)`
/// when `(ns, name)` match the filter's `(namespace, name)`. Returns `None`
/// for misshapen keys (logged as warn) or non-matching namespace/name pairs
/// (silent — that's normal flow).
pub(super) fn parse_gcap_key(key: &str, filter: &CapFilter) -> Option<(Arc<str>, NodeId)> {
    let shape = match parse_gcap_key_shape(key) {
        Some(s) => s,
        None => {
            warn!(key = %key, "malformed gcap/ key — could not parse shape");
            return None;
        }
    };
    if shape.namespace != filter.namespace || shape.name != filter.name {
        return None;
    }
    Some((shape.group, shape.contributor))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nid(port: u16) -> NodeId {
        NodeId::new("127.0.0.1", port).expect("valid loopback NodeId")
    }

    #[test]
    fn parse_gcap_key_extracts_group_and_contributor() {
        let filter = CapFilter::new("compute", "gpu");
        let key = "gcap/gpu-workers/compute/gpu/127.0.0.1:8080";
        let (group, contributor) = parse_gcap_key(key, &filter).expect("parse");
        assert_eq!(group.as_ref(), "gpu-workers");
        assert_eq!(contributor.to_string(), "127.0.0.1:8080");
    }

    #[test]
    fn parse_gcap_key_rejects_wrong_namespace_or_name() {
        let key = "gcap/gpu-workers/compute/gpu/127.0.0.1:8080";
        let wrong_ns   = CapFilter::new("storage", "gpu");
        let wrong_name = CapFilter::new("compute", "tpu");
        assert!(parse_gcap_key(key, &wrong_ns).is_none());
        assert!(parse_gcap_key(key, &wrong_name).is_none());
    }

    #[test]
    fn parse_gcap_key_rejects_truncated() {
        let filter = CapFilter::new("compute", "gpu");
        assert!(parse_gcap_key("gcap/gpu-workers/compute/gpu", &filter).is_none());
        assert!(parse_gcap_key("gcap/gpu-workers/compute", &filter).is_none());
    }

    #[test]
    fn apply_locality_pref_any_preserves_order() {
        let mut v = vec![5usize, 1, 3, 0];
        apply_locality_pref(&mut v, LocalityPreference::Any, |x| *x);
        assert_eq!(v, vec![5, 1, 3, 0]);
    }

    #[test]
    fn apply_locality_pref_prefer_shared_sorts_descending() {
        let mut v = vec![0usize, 3, 1, 2];
        apply_locality_pref(&mut v, LocalityPreference::PreferShared(0), |x| *x);
        assert_eq!(v, vec![3, 2, 1, 0]);
    }

    #[test]
    fn apply_locality_pref_strict_filters_and_sorts() {
        let mut v = vec![0usize, 3, 1, 2];
        apply_locality_pref(&mut v, LocalityPreference::Strict(2), |x| *x);
        assert_eq!(v, vec![3, 2]);
    }

    #[test]
    fn apply_locality_pref_strict_can_empty() {
        let mut v = vec![0usize, 1];
        apply_locality_pref(&mut v, LocalityPreference::Strict(5), |x| *x);
        assert!(v.is_empty());
    }

    #[test]
    fn rank_node_matches_sorts_descending() {
        let mut matches = vec![
            (nid(1), Capability::new("compute", "gpu").with("vram_gb", CapValue::Integer(24))),
            (nid(2), Capability::new("compute", "gpu").with("vram_gb", CapValue::Integer(80))),
            (nid(3), Capability::new("compute", "gpu").with("vram_gb", CapValue::Integer(48))),
        ];
        let ranking = CapRanking {
            attribute: Arc::from("vram_gb"),
            order:     RankingOrder::Descending,
        };
        rank_node_matches(&mut matches, &ranking);
        let order: Vec<u16> = matches.iter()
            .map(|(n, _)| n.to_string().split(':').nth(1).unwrap().parse().unwrap())
            .collect();
        assert_eq!(order, vec![2, 3, 1]);
    }

    #[test]
    fn rank_node_matches_missing_attribute_sorts_last() {
        let mut matches = vec![
            (nid(1), Capability::new("ai", "agent")),
            (nid(2), Capability::new("ai", "agent").with("model_size", CapValue::Integer(70))),
            (nid(3), Capability::new("ai", "agent")),
        ];
        let ranking = CapRanking {
            attribute: Arc::from("model_size"),
            order:     RankingOrder::Descending,
        };
        rank_node_matches(&mut matches, &ranking);
        let first_port: u16 = matches[0].0.to_string().split(':').nth(1).unwrap().parse().unwrap();
        assert_eq!(first_port, 2);
    }

    #[test]
    fn better_value_prefers_larger_for_descending() {
        let result = better_value(
            Some(CapValue::Integer(5)),
            Some(CapValue::Integer(10)),
            RankingOrder::Descending,
        );
        assert_eq!(result, Some(CapValue::Integer(10)));
    }

    #[test]
    fn better_value_prefers_smaller_for_ascending() {
        let result = better_value(
            Some(CapValue::Integer(5)),
            Some(CapValue::Integer(10)),
            RankingOrder::Ascending,
        );
        assert_eq!(result, Some(CapValue::Integer(5)));
    }

    #[test]
    fn better_value_handles_missing_inputs() {
        assert_eq!(
            better_value(None, Some(CapValue::Integer(3)), RankingOrder::Descending),
            Some(CapValue::Integer(3)),
        );
        assert_eq!(
            better_value(Some(CapValue::Integer(7)), None, RankingOrder::Descending),
            Some(CapValue::Integer(7)),
        );
        assert_eq!(
            better_value(None, None, RankingOrder::Ascending),
            None,
        );
    }

    // ── suggest_leader ────────────────────────────────────────────────────

    #[cfg(feature = "consensus")]
    fn make_agent_for_suggest() -> crate::GossipAgent {
        crate::GossipAgent::new(nid(0), crate::GossipConfig::default())
    }

    #[test]
    #[cfg(feature = "consensus")]
    fn suggest_leader_weighs_trust_over_load() {
        use crate::signal::{encode_load_state, LoadState};
        use crate::consensus_ns;

        let agent = make_agent_for_suggest();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;

        let node_b = nid(7011);
        let node_c = nid(7012);
        let _ = agent.kv().set(format!("grp/workers/{}", node_b), bytes::Bytes::from_static(b"1"));
        let _ = agent.kv().set(format!("grp/workers/{}", node_c), bytes::Bytes::from_static(b"1"));

        let b_state = LoadState { fill_ratio: 0.8, is_opaque: false, written_at_ms: now_ms };
        let c_state = LoadState { fill_ratio: 0.2, is_opaque: false, written_at_ms: now_ms };
        let _ = agent.kv().set(format!("sys/load/{}/task", node_b), encode_load_state(&b_state));
        let _ = agent.kv().set(format!("sys/load/{}/task", node_c), encode_load_state(&c_state));

        let trusted_b = vec![node_b.clone()];
        for port in [7020u16, 7021, 7022, 7023] {
            let voter = nid(port);
            let encoded = mycelium_core::serde_fixint::to_vec(&trusted_b).unwrap();
            let _ = agent.kv().set(format!("{}{}/{}", consensus_ns::TRUST, "workers", voter), encoded);
        }

        let suggested = agent.consensus().suggest_leader("workers", "task", std::time::Duration::from_secs(600));
        assert_eq!(suggested, node_b, "B should be preferred despite higher load because it has higher trust");
    }

    #[test]
    #[cfg(feature = "consensus")]
    fn suggest_leader_returns_least_loaded_member() {
        use crate::signal::{encode_load_state, LoadState};

        let agent = make_agent_for_suggest();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;

        let node_a = nid(7001);
        let node_b = nid(7002);
        let _ = agent.kv().set(format!("grp/workers/{}", node_a), bytes::Bytes::from_static(b"1"));
        let _ = agent.kv().set(format!("grp/workers/{}", node_b), bytes::Bytes::from_static(b"1"));

        let heavy = LoadState { fill_ratio: 0.9, is_opaque: true,  written_at_ms: now_ms };
        let light = LoadState { fill_ratio: 0.1, is_opaque: false, written_at_ms: now_ms };
        let _ = agent.kv().set(format!("sys/load/{}/task", node_a), encode_load_state(&heavy));
        let _ = agent.kv().set(format!("sys/load/{}/task", node_b), encode_load_state(&light));

        let suggested = agent.consensus().suggest_leader("workers", "task", std::time::Duration::from_secs(600));
        assert_eq!(suggested, node_b, "suggest_leader should pick the lighter-loaded member");
    }
}
