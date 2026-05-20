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
    Capability, CapFilter, CapRanking, CapValue, RankingOrder,
    WiringProvider, WiringStatus, partial_cmp_cap,
};
use crate::locality::{LocalityPath, LocalityPreference};
use crate::node_id::NodeId;
use ahash::AHashMap;
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::warn;

use super::GossipAgent;
use super::capability_ops::{await_shutdown, parse_cap_key_or_warn, scan_prefix_kv};

impl GossipAgent {
    /// Snapshot scan of provider groups (via `gcap/`) and standalone nodes
    /// (via `cap/`) that currently satisfy `filter`. Returns
    /// [`WiringStatus::Wired`] with the discovered providers, or
    /// [`WiringStatus::Unwired`] if neither source has a match.
    ///
    /// `shared_locality_depth` is hard-coded to `0` here; use
    /// [`resolve_wiring_with_locality`](Self::resolve_wiring_with_locality)
    /// for topology-aware variants.
    pub fn resolve_wiring(&self, filter: &CapFilter) -> WiringStatus {
        wiring_snapshot(&self.kv_state, filter)
    }

    /// Like [`resolve`](Self::resolve) but annotates each provider with its
    /// `shared_prefix_len` against this node's own locality, then applies
    /// the requested [`LocalityPreference`]:
    ///
    /// - `Any`: returns matches in scan order with depth `0`.
    /// - `PreferShared(_)`: keeps every match and sorts by depth descending.
    /// - `Strict(depth)`: drops providers with `shared_prefix_len < depth`.
    ///
    /// When this node has no configured locality (empty `locality_path`),
    /// every provider reports depth `0`; `Strict(d)` with `d > 0` therefore
    /// returns an empty result.
    pub fn resolve_with_locality(
        &self,
        filter: &CapFilter,
        pref:   LocalityPreference,
    ) -> Vec<(NodeId, Capability, usize)> {
        let self_loc = self.self_locality();
        let mut annotated: Vec<(NodeId, Capability, usize)> = self.resolve(filter)
            .into_iter()
            .map(|(node_id, cap)| {
                let depth = locality_depth(&self.kv_state, self_loc.as_ref(), &node_id);
                (node_id, cap, depth)
            })
            .collect();
        apply_locality_pref(&mut annotated, pref, |(_, _, d)| *d);
        annotated
    }

    /// Locality-aware version of [`resolve_wiring`](Self::resolve_wiring).
    /// Each `WiringProvider` is annotated with `shared_locality_depth`:
    /// - `Node`: shared prefix length against this node's locality.
    /// - `Group`: the **maximum** shared prefix length across the group's
    ///   contributors — a group counts as "close" if any one of its members
    ///   is close to us.
    ///
    /// The preference is then applied to the combined provider list. An
    /// `Unwired` status is returned unchanged.
    pub fn resolve_wiring_with_locality(
        &self,
        filter: &CapFilter,
        pref:   LocalityPreference,
    ) -> WiringStatus {
        let self_loc = self.self_locality();
        let raw = wiring_snapshot(&self.kv_state, filter);
        let WiringStatus::Wired { providers } = raw else { return raw; };
        let mut annotated: Vec<WiringProvider> = providers.into_iter()
            .map(|p| annotate_provider_with_locality(p, &self.kv_state, self_loc.as_ref()))
            .collect();
        apply_locality_pref(&mut annotated, pref, provider_depth);
        if annotated.is_empty() {
            WiringStatus::Unwired { filter: filter.clone() }
        } else {
            WiringStatus::Wired { providers: annotated }
        }
    }

    /// Push-based view of the wiring state for `filter`. The returned receiver
    /// fires whenever a `cap/` or `gcap/` write or tombstone causes the
    /// resolved provider set to change. The initial value is a snapshot at
    /// subscription time; subsequent values are debounced — identical
    /// statuses are not re-broadcast.
    pub fn watch_wiring(&self, filter: CapFilter) -> watch::Receiver<WiringStatus> {
        let initial         = wiring_snapshot(&self.kv_state, &filter);
        let (tx, rx)        = watch::channel(initial);
        let mut cap_rx      = self.subscribe_prefix(Arc::<str>::from("cap/"));
        let mut gcap_rx     = self.subscribe_prefix(Arc::<str>::from("gcap/"));
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let kv_state        = Arc::clone(&self.kv_state);
        self.spawn_task(async move {
            loop {
                tokio::select! { biased;
                    _ = await_shutdown(&mut shutdown_rx) => return,
                    r = cap_rx.changed()  => { if r.is_err() { return; } }
                    r = gcap_rx.changed() => { if r.is_err() { return; } }
                }
                let next = wiring_snapshot(&kv_state, &filter);
                // Debounce: skip when the resolved status is unchanged.
                let unchanged = {
                    let current = tx.borrow();
                    *current == next
                };
                if unchanged { continue; }
                if tx.send(next).is_err() { return; }
            }
        });
        rx
    }

    /// Emits `payload` as a signal of `kind` to every provider that satisfies
    /// `filter` at the moment of the call. Groups (from `gcap/`) receive via
    /// `SignalScope::Group(name)`; standalone matching nodes (from `cap/`)
    /// receive via `SignalScope::Individual(node_id)`. The returned vec lists
    /// the recipient identifiers in the order they were dispatched; it is
    /// empty when no provider currently matches (caller can detect via
    /// `watch_wiring` to await wiring restoration).
    ///
    /// Re-wiring is implicit: a subsequent call re-resolves against the
    /// current KV state. There is no stored binding.
    pub fn signal_wired_via(
        &self,
        filter:  &CapFilter,
        kind:    impl Into<Arc<str>>,
        payload: impl Into<Bytes>,
    ) -> Vec<WiringProvider> {
        let kind:    Arc<str> = kind.into();
        let payload: Bytes    = payload.into();
        let status = wiring_snapshot(&self.kv_state, filter);
        let providers = match status {
            WiringStatus::Wired { providers } => providers,
            WiringStatus::Unwired { .. }      => return Vec::new(),
        };
        let mut emitted = Vec::with_capacity(providers.len());
        for provider in providers {
            let scope = match &provider {
                WiringProvider::Group { name, .. }    => crate::signal::SignalScope::Group(name.clone()),
                WiringProvider::Node  { node_id, .. } => crate::signal::SignalScope::Individual(node_id.clone()),
            };
            // Use the existing public emit() which generates a nonce, delivers
            // locally if admitted, and queues for gossip. Return value (queued
            // vs dropped) is ignored — the caller observes successful routing
            // via the returned recipient list and per-receiver acknowledgement
            // if their protocol provides one.
            let _ = self.emit(kind.clone(), scope, payload.clone());
            emitted.push(provider);
        }
        emitted
    }

    /// Locality-aware variant of
    /// [`signal_wired_via`](Self::signal_wired_via): resolves wiring with
    /// [`resolve_wiring_with_locality`](Self::resolve_wiring_with_locality)
    /// (so `pref` filters or reorders providers before dispatch), then emits
    /// to each surviving provider in iteration order. Useful when the
    /// caller wants to confine signals to a topology region — e.g. a
    /// storage cohort that should not bleed traffic across AZs.
    pub fn signal_wired_via_locality(
        &self,
        filter:  &CapFilter,
        pref:    LocalityPreference,
        kind:    impl Into<Arc<str>>,
        payload: impl Into<Bytes>,
    ) -> Vec<WiringProvider> {
        let kind:    Arc<str> = kind.into();
        let payload: Bytes    = payload.into();
        let status = self.resolve_wiring_with_locality(filter, pref);
        let providers = match status {
            WiringStatus::Wired { providers } => providers,
            WiringStatus::Unwired { .. }      => return Vec::new(),
        };
        let mut emitted = Vec::with_capacity(providers.len());
        for provider in providers {
            let scope = match &provider {
                WiringProvider::Group { name, .. }    => crate::signal::SignalScope::Group(name.clone()),
                WiringProvider::Node  { node_id, .. } => crate::signal::SignalScope::Individual(node_id.clone()),
            };
            let _ = self.emit(kind.clone(), scope, payload.clone());
            emitted.push(provider);
        }
        emitted
    }
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

    // Standalone-node providers from cap/.
    for (key, bytes) in scan_prefix_kv(kv_state, "cap/") {
        let Some((node_id, _ns, _name)) = parse_cap_key_or_warn("cap/", &key) else { continue };
        let Some(cap) = Capability::decode(&bytes) else {
            warn!(key = %key, "malformed Capability — peer sent bytes that did not decode");
            continue;
        };
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
    for (key, bytes) in scan_prefix_kv(kv_state, "gcap/") {
        let Some((group, contributor)) = parse_gcap_key(&key, filter) else { continue };
        let Some(cap) = Capability::decode(&bytes) else {
            warn!(key = %key, "malformed Capability — peer sent bytes that did not decode");
            continue;
        };
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
fn locality_depth(
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
fn annotate_provider_with_locality(
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
fn provider_depth(p: &WiringProvider) -> usize {
    match p {
        WiringProvider::Node  { shared_locality_depth, .. } => *shared_locality_depth,
        WiringProvider::Group { shared_locality_depth, .. } => *shared_locality_depth,
    }
}

/// Applies a [`LocalityPreference`] to an annotated provider list in place.
/// `depth_of` extracts the integer depth from each element. Stable sort so
/// providers of equal depth retain their original ordering (scan order).
fn apply_locality_pref<T, F>(items: &mut Vec<T>, pref: LocalityPreference, depth_of: F)
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
}
