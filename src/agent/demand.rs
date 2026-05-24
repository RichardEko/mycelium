//! Demand-pressure surface (Phase 9 of the locality/topology/capabilities plan).
//!
//! Surfaces "how many want this vs. how many provide it" for a `CapFilter` as
//! a derived view of `req/`, `cap/`, and `gcap/`. No new KV state is created;
//! the library never auto-advertises in response to high demand — that's an
//! application-layer decision (orchestrators, autoscalers, dashboards).

use crate::capability::{CapEntry, Capability, CapFilter, DemandStatus, ReqEntry};
use crate::node_id::NodeId;
use ahash::AHashSet;
use std::sync::Arc;
use tokio::{sync::watch, time};
use tracing::warn;

use super::GossipAgent;
use super::capability_ops::{await_shutdown, is_cap_locality_key, now_ms, parse_cap_key_or_warn, scan_prefix_kv_with_ts, WATCHER_DEBOUNCE_WINDOW};
use super::wiring::parse_gcap_key;

impl GossipAgent {
    /// Snapshot count of declared demand vs. available providers for `filter`.
    /// Demand = unique nodes whose `req/{node}/{ns}/{name}` entry shares the
    /// filter's `(namespace, name)`. Providers = unique nodes either
    /// directly advertising a matching capability (`cap/`) or contributing
    /// to a matching group projection (`gcap/`). Pressure =
    /// `demanding.len() / max(providers.len(), 1)`.
    ///
    /// The library does NOT auto-respond to high pressure — this is a
    /// surfaced signal for orchestrators / autoscalers / dashboards.
    pub fn demand(&self, filter: &CapFilter) -> DemandStatus {
        demand_snapshot(&self.kv_state, filter)
    }

    /// Push-based view of demand pressure for `filter`. Fires whenever a
    /// matching `req/`, `cap/`, or `gcap/` entry is written or tombstoned.
    /// Debounced — identical `DemandStatus` values are not re-broadcast.
    pub fn watch_demand(&self, filter: CapFilter) -> watch::Receiver<DemandStatus> {
        let initial         = demand_snapshot(&self.kv_state, &filter);
        let (tx, rx)        = watch::channel(initial);
        // C1: narrow all three watchers to this filter's (ns, name). req/ and
        // cap/ keys end in /{ns}/{name}; gcap/{group}/{ns}/{name}/{contributor}
        // contains /{ns}/{name}/ as a substring.
        let needle_endswith = format!("/{}/{}",  filter.namespace, filter.name);
        let needle_contains = format!("/{}/{}/", filter.namespace, filter.name);
        let req_needle  = needle_endswith.clone();
        let cap_needle  = needle_endswith;
        let gcap_needle = needle_contains;
        let mut req_rx  = self.subscribe_prefix_with_predicate(
            Arc::<str>::from("req/"),  move |k| k.ends_with(&req_needle));
        let mut cap_rx  = self.subscribe_prefix_with_predicate(
            Arc::<str>::from("cap/"),  move |k| k.ends_with(&cap_needle));
        let mut gcap_rx = self.subscribe_prefix_with_predicate(
            Arc::<str>::from("gcap/"), move |k| k.contains(&gcap_needle));
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let kv_state        = Arc::clone(&self.kv_state);
        self.spawn_task(async move {
            loop {
                tokio::select! { biased;
                    _ = await_shutdown(&mut shutdown_rx) => return,
                    r = req_rx.changed()  => { if r.is_err() { return; } }
                    r = cap_rx.changed()  => { if r.is_err() { return; } }
                    r = gcap_rx.changed() => { if r.is_err() { return; } }
                }
                // Debounce burst writes: drain further fires for the next
                // WATCHER_DEBOUNCE_WINDOW before computing a new snapshot.
                let deadline = time::Instant::now() + WATCHER_DEBOUNCE_WINDOW;
                loop {
                    tokio::select! { biased;
                        _ = time::sleep_until(deadline) => break,
                        r = req_rx.changed()  => { if r.is_err() { return; } }
                        r = cap_rx.changed()  => { if r.is_err() { return; } }
                        r = gcap_rx.changed() => { if r.is_err() { return; } }
                    }
                }
                let next = demand_snapshot(&kv_state, &filter);
                let unchanged = { *tx.borrow() == next };
                if unchanged { continue; }
                if tx.send(next).is_err() { return; }
            }
        });
        rx
    }
}

/// Computes the current [`DemandStatus`] for `filter`. Deduplicates both
/// demanding nodes and providers — a node contributing to multiple matching
/// groups still counts once, and a node with both a direct match and a group
/// contribution counts once.
pub(super) fn demand_snapshot(
    kv_state: &crate::store::KvState,
    filter:   &CapFilter,
) -> DemandStatus {
    // Demanders: nodes whose req/{node}/{ns}/{name} entry shares the filter's
    // (namespace, name). We don't deep-compare filter contents — the
    // namespace/name pair is the declared "need shape," and that's what we
    // are scoring demand for.
    let now = now_ms();
    let mut demanding: AHashSet<NodeId> = AHashSet::new();
    for (key, bytes, hlc_ts) in scan_prefix_kv_with_ts(kv_state, "req/") {
        let Some((node_id, ns, name)) = parse_cap_key_or_warn("req/", &key) else { continue };
        if ns != filter.namespace || name != filter.name { continue; }
        let Some(req_entry) = ReqEntry::decode(&bytes)
            .or_else(|| CapFilter::decode(&bytes).map(|f| ReqEntry { filter: f, refresh_interval_ms: 60_000 }))
        else {
            warn!(key = %key, "malformed CapFilter under req/ — peer sent bytes that did not decode");
            continue;
        };
        if !req_entry.is_fresh(hlc_ts, now) { continue; }
        demanding.insert(node_id);
    }

    // Providers: union of direct cap/ matches and gcap/ contributors.
    let mut providers: AHashSet<NodeId> = AHashSet::new();
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
        if filter.matches(&cap_entry.capability) {
            providers.insert(node_id);
        }
    }
    for (key, bytes, hlc_ts) in scan_prefix_kv_with_ts(kv_state, "gcap/") {
        let Some((_group, contributor)) = parse_gcap_key(&key, filter) else { continue };
        let Some(cap_entry) = CapEntry::decode(&bytes)
            .or_else(|| Capability::decode(&bytes).map(|cap| CapEntry { capability: cap, refresh_interval_ms: 60_000 }))
        else {
            warn!(key = %key, "malformed Capability — peer sent bytes that did not decode");
            continue;
        };
        if !cap_entry.is_fresh(hlc_ts, now) { continue; }
        if filter.matches(&cap_entry.capability) {
            providers.insert(contributor);
        }
    }

    let demanding_nodes: Vec<NodeId> = demanding.into_iter().collect();
    let providers_vec:   Vec<NodeId> = providers.into_iter().collect();
    let demand_pressure = demanding_nodes.len() as f32 / providers_vec.len().max(1) as f32;
    DemandStatus {
        filter:          filter.clone(),
        demanding_nodes,
        providers:       providers_vec,
        demand_pressure,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::CapValue;
    use crate::framing::make_gossip_update;
    use crate::hlc::Hlc;
    use crate::store::{apply_and_notify, KvState};
    use bytes::Bytes;

    fn nid(port: u16) -> NodeId {
        NodeId::new("127.0.0.1", port).expect("valid loopback NodeId")
    }

    fn write(kv: &KvState, sender: &NodeId, key: &str, value: Bytes) {
        let hlc = Hlc::new();
        let upd = make_gossip_update(sender, 5, Arc::from(key), value, false, &hlc);
        apply_and_notify(kv, &upd);
    }

    #[test]
    fn demand_snapshot_counts_unique_demand_and_providers() {
        let kv = KvState::new(0);
        let sender = nid(99);
        let filter = CapFilter::new("compute", "gpu");
        write(&kv, &sender, "req/127.0.0.1:1/compute/gpu", filter.encode());
        write(&kv, &sender, "req/127.0.0.1:2/compute/gpu", filter.encode());
        let cap = Capability::new("compute", "gpu");
        write(&kv, &sender, "cap/127.0.0.1:3/compute/gpu", cap.encode());
        write(&kv, &sender, "gcap/gpu-pool/compute/gpu/127.0.0.1:4", cap.encode());
        let status = demand_snapshot(&kv, &filter);
        assert_eq!(status.demanding_nodes.len(), 2);
        assert_eq!(status.providers.len(), 2);
        assert!((status.demand_pressure - 1.0).abs() < f32::EPSILON);
        // Touch CapValue so its import is meaningful when the test compiles.
        let _ = CapValue::Integer(1);
    }

    #[test]
    fn demand_snapshot_zero_providers_yields_pressure_equal_to_demand() {
        let kv = KvState::new(0);
        let sender = nid(99);
        let filter = CapFilter::new("ai", "agent");
        write(&kv, &sender, "req/127.0.0.1:1/ai/agent", filter.encode());
        write(&kv, &sender, "req/127.0.0.1:2/ai/agent", filter.encode());
        write(&kv, &sender, "req/127.0.0.1:3/ai/agent", filter.encode());
        let status = demand_snapshot(&kv, &filter);
        assert_eq!(status.demanding_nodes.len(), 3);
        assert_eq!(status.providers.len(), 0);
        assert!((status.demand_pressure - 3.0).abs() < f32::EPSILON);
    }

    #[test]
    fn demand_snapshot_dedupes_provider_appearing_in_cap_and_gcap() {
        let kv = KvState::new(0);
        let sender = nid(99);
        let filter = CapFilter::new("compute", "gpu");
        let cap = Capability::new("compute", "gpu");
        write(&kv, &sender, "cap/127.0.0.1:7/compute/gpu", cap.encode());
        write(&kv, &sender, "gcap/gpu-pool/compute/gpu/127.0.0.1:7", cap.encode());
        let status = demand_snapshot(&kv, &filter);
        assert_eq!(status.providers.len(), 1, "should dedupe by NodeId");
    }
}
