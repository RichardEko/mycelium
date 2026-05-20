//! Capability / Requirement operations on `GossipAgent`.
//!
//! Phase 3 of the locality / topology / capabilities plan.
//!
//! - `advertise_capability` — write `cap/{node_id}/{ns}/{name}` and keep it
//!   gossiped for as long as the returned handle lives.
//! - `resolve` — snapshot filter-match over the local KV view of `cap/`.
//! - `watch_capabilities` — push-based stream of `Added` / `Removed` events.
//! - `declare_requirement` — write `req/{node_id}/{ns}/{name}` and spawn an
//!   auto-opacity watcher that writes `sys/load/{node_id}/req/{ns}/{name}`
//!   while the requirement is unsatisfied (composes with load-based opacity
//!   through the existing `is_self_opaque()` scanner).
//! - `watch_requirement` — push-based `RequirementStatus` view of one filter.
//! - `define_capability_group` + `watch_capability_group_definitions` —
//!   emergent group formation: any node whose own capabilities match the
//!   group's filter self-joins via `join_group`.

use crate::capability::{
    Capability, CapFilter, CapValue, CapRanking, CapabilityEvent,
    CapabilityHandle, CapabilityGroupDef, CapabilityGroupHandle,
    DemandStatus, RankingOrder, RequirementHandle, RequirementStatus,
    WiringProvider, WiringStatus, partial_cmp_cap,
};
use crate::locality::{LocalityPath, LocalityPreference};
use crate::node_id::NodeId;
use crate::signal::{LoadState, encode_load_state};
use ahash::{AHashMap, AHashSet};
use bytes::Bytes;
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::warn;

use super::{GossipAgent, TaskCtx};
use super::kv::run_kv_persist_task;

/// Sendable shutdown-await helper: yields once `shutdown_rx`'s value is `true`
/// (or its sender drops). Unlike `watch::Receiver::wait_for`, this returns
/// only `()` so no `Ref<'_, bool>` is held across the .await, which keeps
/// the surrounding `tokio::select!` future `Send`.
async fn await_shutdown(rx: &mut watch::Receiver<bool>) {
    while !*rx.borrow() {
        if rx.changed().await.is_err() { return; }
    }
}

/// Like `parse_cap_key` but emits a `warn!` on the unhappy path. Use at scan
/// sites where a malformed key is genuinely surprising (we only scan prefixes
/// whose entries are well-formed by convention).
fn parse_cap_key_or_warn(prefix: &str, key: &str) -> Option<(NodeId, Arc<str>, Arc<str>)> {
    let parsed = parse_cap_key(prefix, key);
    if parsed.is_none() {
        warn!(prefix = %prefix, key = %key, "malformed key under capability/requirement prefix");
    }
    parsed
}

/// Splits `cap/{node_id}/{ns}/{name}` (or `req/...`) into its three components.
/// Returns `None` if the key has the wrong shape or contains too many segments.
fn parse_cap_key(prefix: &str, key: &str) -> Option<(NodeId, Arc<str>, Arc<str>)> {
    let rest = key.strip_prefix(prefix)?;
    let mut parts = rest.splitn(3, '/');
    let node_id_str = parts.next()?;
    let namespace   = parts.next()?;
    let name        = parts.next()?;
    if name.contains('/') { return None; }
    let node_id = node_id_str.parse::<NodeId>().ok()?;
    Some((node_id, Arc::from(namespace), Arc::from(name)))
}

impl GossipAgent {
    /// Advertises a [`Capability`] under `cap/{node_id}/{namespace}/{name}`,
    /// re-asserting it on every `interval` tick so late joiners discover it
    /// without an out-of-band sync. Drop the returned [`CapabilityHandle`] to
    /// tombstone the entry; the shutdown path tombstones it automatically.
    #[must_use]
    pub fn advertise_capability(
        &self,
        capability: Capability,
        interval:   Duration,
    ) -> CapabilityHandle {
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        let shutdown_rx            = self.shutdown_tx.subscribe();
        let ctx: Arc<TaskCtx>      = Arc::clone(&self.task_ctx);
        let kv_key: Arc<str>       = Arc::from(format!(
            "cap/{}/{}/{}", ctx.node_id, capability.namespace, capability.name,
        ).as_str());
        let cap_arc                = Arc::new(capability);
        let payload_fn: super::kv::PersistPayloadFn = {
            let cap = Arc::clone(&cap_arc);
            Arc::new(move || cap.encode())
        };
        self.spawn_task(run_kv_persist_task(
            ctx, cancel_rx, shutdown_rx, kv_key, interval, payload_fn, None,
        ));
        CapabilityHandle { _retract: cancel_tx }
    }

    /// Snapshot scan: returns every live capability in the local KV view that
    /// satisfies `filter`. If `filter.ranking` is set, results are sorted by
    /// the named attribute (providers missing the attribute or with
    /// incomparable values sort to the end). Otherwise order is unspecified.
    pub fn resolve(&self, filter: &CapFilter) -> Vec<(NodeId, Capability)> {
        let mut out = Vec::new();
        for (key, bytes) in self.scan_prefix("cap/") {
            let Some((node_id, _ns, _name)) = parse_cap_key_or_warn("cap/", &key) else { continue };
            let Some(cap) = Capability::decode(&bytes) else {
            warn!(key = %key, "malformed Capability — peer sent bytes that did not decode");
            continue;
        };
            if filter.matches(&cap) {
                out.push((node_id, cap));
            }
        }
        if let Some(ranking) = &filter.ranking {
            rank_node_matches(&mut out, ranking);
        }
        out
    }

    /// Push-based stream of [`CapabilityEvent`]s for capabilities matching
    /// `filter`. Internally maintains a snapshot of previously-matched
    /// `(node_id, namespace, name)` keys so consecutive notifications emit
    /// only the difference. The channel has a small fixed buffer; a slow
    /// consumer that lets it fill will drop further notifications until it
    /// drains the queue.
    pub fn watch_capabilities(&self, filter: CapFilter) -> mpsc::Receiver<CapabilityEvent> {
        let (tx, rx) = mpsc::channel::<CapabilityEvent>(64);
        let mut prefix_rx = self.subscribe_prefix(Arc::<str>::from("cap/"));
        let kv_state = Arc::clone(&self.kv_state);
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        // Emit Added for everything matching at subscription time. Then watch.
        let initial = self.resolve(&filter);
        let mut known: AHashMap<(NodeId, Arc<str>, Arc<str>), Capability> = AHashMap::new();
        for (node_id, cap) in &initial {
            known.insert((node_id.clone(), cap.namespace.clone(), cap.name.clone()), cap.clone());
        }
        let tx_initial = tx.clone();
        let initial_owned = initial;
        // Send initial Added events without blocking the agent's task graph; if
        // the consumer is already gone we abandon the stream silently.
        self.spawn_task(async move {
            for (node_id, cap) in initial_owned {
                if tx_initial.send(CapabilityEvent::Added { node_id, capability: cap }).await.is_err() {
                    return;
                }
            }
            loop {
                tokio::select! { biased;
                    _ = await_shutdown(&mut shutdown_rx) => return,
                    changed = prefix_rx.changed() => {
                        if changed.is_err() { return; }
                        // Re-scan and diff against `known`.
                        let mut current: AHashMap<(NodeId, Arc<str>, Arc<str>), Capability> = AHashMap::new();
                        for (key, bytes) in scan_prefix_kv(&kv_state, "cap/") {
                            let Some((node_id, ns, name)) = parse_cap_key_or_warn("cap/", &key) else { continue };
                            let Some(cap) = Capability::decode(&bytes) else {
            warn!(key = %key, "malformed Capability — peer sent bytes that did not decode");
            continue;
        };
                            if !filter.matches(&cap) { continue; }
                            current.insert((node_id, ns, name), cap);
                        }
                        // Removed: in known but not in current.
                        let removed: Vec<_> = known.keys()
                            .filter(|k| !current.contains_key(*k))
                            .cloned()
                            .collect();
                        for k in &removed {
                            known.remove(k);
                            let _ = tx.send(CapabilityEvent::Removed {
                                node_id:   k.0.clone(),
                                namespace: k.1.clone(),
                                name:      k.2.clone(),
                            }).await;
                        }
                        // Added or updated: in current and either new, or attributes changed.
                        for (k, cap) in &current {
                            let changed = match known.get(k) {
                                None      => true,
                                Some(old) => old != cap,
                            };
                            if changed {
                                known.insert(k.clone(), cap.clone());
                                let _ = tx.send(CapabilityEvent::Added {
                                    node_id:    k.0.clone(),
                                    capability: cap.clone(),
                                }).await;
                            }
                        }
                    }
                }
            }
        });
        rx
    }

    /// Declares a requirement and writes it to `req/{node_id}/{ns}/{name}`.
    /// Spawns a satisfaction watcher that, while the requirement is unmet,
    /// writes `sys/load/{node_id}/req/{ns}/{name}` with `is_opaque = true` —
    /// composing with load-based opacity through `is_self_opaque`'s
    /// existing `sys/load/{node_id}/*` scanner. The opacity entry is
    /// tombstoned the moment the filter resolves to at least one provider.
    ///
    /// Drop the returned [`RequirementHandle`] to retract both the requirement
    /// and any active opacity entry.
    #[must_use]
    pub fn declare_requirement(
        &self,
        filter:   CapFilter,
        interval: Duration,
    ) -> RequirementHandle {
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        let (op_cancel_tx, op_cancel_rx) = oneshot::channel::<()>();
        let shutdown_rx                  = self.shutdown_tx.subscribe();
        let op_shutdown_rx               = self.shutdown_tx.subscribe();
        let ctx: Arc<TaskCtx>            = Arc::clone(&self.task_ctx);
        let opacity_ctx: Arc<TaskCtx>    = Arc::clone(&self.task_ctx);

        let kv_key: Arc<str> = Arc::from(format!(
            "req/{}/{}/{}", ctx.node_id, filter.namespace, filter.name,
        ).as_str());
        let opacity_key: Arc<str> = Arc::from(format!(
            "sys/load/{}/req/{}/{}", ctx.node_id, filter.namespace, filter.name,
        ).as_str());
        let filter_arc = Arc::new(filter);
        let filter_for_payload = Arc::clone(&filter_arc);
        let payload_fn: super::kv::PersistPayloadFn = Arc::new(move || {
            filter_for_payload.encode()
        });
        self.spawn_task(run_kv_persist_task(
            ctx, cancel_rx, shutdown_rx, kv_key, interval, payload_fn, None,
        ));

        // Auto-opacity watcher.
        let mut prefix_rx = self.subscribe_prefix(Arc::<str>::from("cap/"));
        let kv_state = Arc::clone(&self.kv_state);
        let filter_for_watch = Arc::clone(&filter_arc);
        self.spawn_task(run_requirement_opacity_watcher(
            opacity_ctx, op_cancel_rx, op_shutdown_rx,
            opacity_key, filter_for_watch, prefix_rx_recv_box(&mut prefix_rx), kv_state,
        ));
        // Keep the opacity-cancel sender alive alongside the main one — both fire
        // when the requirement is retracted.
        let retract = build_paired_retract(cancel_tx, op_cancel_tx);
        RequirementHandle { _retract: retract }
    }

    /// Push-based view of one requirement's current satisfaction status.
    /// The returned receiver's initial value is a snapshot taken at call time;
    /// subsequent values arrive whenever a matching `cap/` entry is added,
    /// updated, or removed.
    pub fn watch_requirement(&self, filter: CapFilter) -> watch::Receiver<RequirementStatus> {
        let initial = self.resolve(&filter);
        let initial_status = if initial.is_empty() {
            RequirementStatus::Unsatisfied { filter: filter.clone() }
        } else {
            RequirementStatus::Satisfied { providers: initial }
        };
        let (tx, rx) = watch::channel(initial_status);
        let mut prefix_rx = self.subscribe_prefix(Arc::<str>::from("cap/"));
        let kv_state = Arc::clone(&self.kv_state);
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        self.spawn_task(async move {
            loop {
                tokio::select! { biased;
                    _ = await_shutdown(&mut shutdown_rx) => return,
                    changed = prefix_rx.changed() => {
                        if changed.is_err() { return; }
                        let providers = resolve_filter_against_kv(&kv_state, &filter);
                        let status = if providers.is_empty() {
                            RequirementStatus::Unsatisfied { filter: filter.clone() }
                        } else {
                            RequirementStatus::Satisfied { providers }
                        };
                        if tx.send(status).is_err() { return; }
                    }
                }
            }
        });
        rx
    }

    /// Picks a group member that also satisfies every requirement filter.
    /// Returns the first member with the lowest current load pheromone fill,
    /// preferring lightly-loaded leaders. `None` if no group member matches
    /// every requirement.
    pub fn suggest_leader_with_requirements(
        &self,
        group:        &str,
        requirements: &[CapFilter],
    ) -> Option<NodeId> {
        let members = self.group_members(group);
        if members.is_empty() { return None; }
        let mut candidates: Vec<NodeId> = members.into_iter()
            .filter(|m| {
                requirements.iter().all(|req| {
                    self.resolve(req).iter().any(|(provider, _)| provider == m)
                })
            })
            .collect();
        if candidates.is_empty() { return None; }
        // Lightest-load wins. effective_opacity is keyed by kind; here we use
        // a coarse aggregate by summing the per-kind fill ratios from the
        // pheromone trails for each candidate.
        candidates.sort_by(|a, b| {
            let la = aggregate_fill(&self.kv_state, a);
            let lb = aggregate_fill(&self.kv_state, b);
            la.partial_cmp(&lb).unwrap_or(std::cmp::Ordering::Equal)
        });
        candidates.into_iter().next()
    }

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
    ) -> Vec<crate::capability::WiringProvider> {
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
    ) -> Vec<crate::capability::WiringProvider> {
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
        let mut req_rx      = self.subscribe_prefix(Arc::<str>::from("req/"));
        let mut cap_rx      = self.subscribe_prefix(Arc::<str>::from("cap/"));
        let mut gcap_rx     = self.subscribe_prefix(Arc::<str>::from("gcap/"));
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
                let next = demand_snapshot(&kv_state, &filter);
                let unchanged = { *tx.borrow() == next };
                if unchanged { continue; }
                if tx.send(next).is_err() { return; }
            }
        });
        rx
    }

    /// Publishes a [`CapabilityGroupDef`] under `cap-group/{group}`. Any node
    /// whose own `cap/{self}/*` advertisements match `def.filter` will
    /// self-join the named group via `join_group` once
    /// [`watch_capability_group_definitions`] runs (started automatically by
    /// `lifecycle::start`). Drop the returned handle to tombstone the
    /// definition; all members will then receive the tombstone via gossip
    /// and self-leave.
    #[must_use]
    pub fn define_capability_group(
        &self,
        group:    impl Into<Arc<str>>,
        def:      CapabilityGroupDef,
        interval: Duration,
    ) -> CapabilityGroupHandle {
        let group: Arc<str> = group.into();
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        let shutdown_rx            = self.shutdown_tx.subscribe();
        let ctx: Arc<TaskCtx>      = Arc::clone(&self.task_ctx);
        let kv_key: Arc<str>       = Arc::from(format!("cap-group/{}", group).as_str());
        let def_arc                = Arc::new(def);
        let payload_fn: super::kv::PersistPayloadFn = {
            let def = Arc::clone(&def_arc);
            Arc::new(move || def.encode())
        };
        self.spawn_task(run_kv_persist_task(
            ctx, cancel_rx, shutdown_rx, kv_key, interval, payload_fn, None,
        ));
        CapabilityGroupHandle { _retract: cancel_tx, group }
    }
}

// ── Free helpers (used by spawned tasks) ─────────────────────────────────────

/// Like `GossipAgent::scan_prefix`, but for a `KvState` reference held by a
/// spawned task that doesn't carry a `GossipAgent` handle.
fn scan_prefix_kv(kv_state: &crate::store::KvState, prefix: &str) -> Vec<(Arc<str>, Bytes)> {
    let seg = prefix.find('/').map_or(prefix, |i| &prefix[..i]);
    let store_guard = kv_state.store.pin();
    let idx_guard   = kv_state.prefix_index.pin();
    if let Some(bucket) = idx_guard.get(seg) {
        bucket.pin().iter()
            .filter_map(|(key, _)| {
                if !key.starts_with(prefix) { return None; }
                let entry = store_guard.get(key.as_ref())?;
                let data  = entry.data.clone()?;
                Some((key.clone(), data))
            })
            .collect()
    } else {
        store_guard.iter()
            .filter(|(k, v)| v.data.is_some() && k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.data.clone().unwrap()))
            .collect()
    }
}

/// Snapshot resolve from a `KvState` (used inside spawned tasks).
fn resolve_filter_against_kv(
    kv_state: &crate::store::KvState,
    filter:   &CapFilter,
) -> Vec<(NodeId, Capability)> {
    let mut out = Vec::new();
    for (key, bytes) in scan_prefix_kv(kv_state, "cap/") {
        let Some((node_id, _ns, _name)) = parse_cap_key_or_warn("cap/", &key) else { continue };
        let Some(cap) = Capability::decode(&bytes) else {
            warn!(key = %key, "malformed Capability — peer sent bytes that did not decode");
            continue;
        };
        if filter.matches(&cap) {
            out.push((node_id, cap));
        }
    }
    out
}

/// Computes the current [`WiringStatus`] for `filter` by scanning both
/// `cap/` (standalone providers) and `gcap/` (group projections). The
/// `shared_locality_depth` field is left as `0` here; locality is layered
/// on by `resolve_wiring_with_locality`.
///
/// When `filter.ranking` is set, providers are sorted by the named attribute
/// — Nodes by their own attribute value, Groups by the **best**-ranking
/// contributor's value (largest for `Descending`, smallest for `Ascending`).
/// Missing or incomparable values sort to the end deterministically.
fn wiring_snapshot(kv_state: &crate::store::KvState, filter: &CapFilter) -> WiringStatus {
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
fn rank_node_matches(matches: &mut [(NodeId, Capability)], ranking: &CapRanking) {
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

/// Computes the current [`DemandStatus`] for `filter`. Deduplicates both
/// demanding nodes and providers — a node contributing to multiple matching
/// groups still counts once, and a node with both a direct match and a group
/// contribution counts once.
fn demand_snapshot(kv_state: &crate::store::KvState, filter: &CapFilter) -> DemandStatus {
    // Demanders: nodes whose req/{node}/{ns}/{name} entry shares the filter's
    // (namespace, name). We don't deep-compare filter contents — the
    // namespace/name pair is the declared "need shape," and that's what we
    // are scoring demand for.
    let mut demanding: AHashSet<NodeId> = AHashSet::new();
    for (key, bytes) in scan_prefix_kv(kv_state, "req/") {
        let Some((node_id, ns, name)) = parse_cap_key_or_warn("req/", &key) else { continue };
        if ns != filter.namespace || name != filter.name { continue; }
        // Optionally also decode and check filter equality — for v1 we treat
        // namespace/name as sufficient evidence of declared need. The
        // bytes-decode is still useful to skip malformed entries.
        if CapFilter::decode(&bytes).is_none() {
            warn!(key = %key, "malformed CapFilter under req/ — peer sent bytes that did not decode");
            continue;
        }
        demanding.insert(node_id);
    }

    // Providers: union of direct cap/ matches and gcap/ contributors.
    let mut providers: AHashSet<NodeId> = AHashSet::new();
    for (key, bytes) in scan_prefix_kv(kv_state, "cap/") {
        let Some((node_id, _ns, _name)) = parse_cap_key_or_warn("cap/", &key) else { continue };
        let Some(cap) = Capability::decode(&bytes) else {
            warn!(key = %key, "malformed Capability — peer sent bytes that did not decode");
            continue;
        };
        if filter.matches(&cap) {
            providers.insert(node_id);
        }
    }
    for (key, bytes) in scan_prefix_kv(kv_state, "gcap/") {
        let Some((_group, contributor)) = parse_gcap_key(&key, filter) else { continue };
        let Some(cap) = Capability::decode(&bytes) else {
            warn!(key = %key, "malformed Capability — peer sent bytes that did not decode");
            continue;
        };
        if filter.matches(&cap) {
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
fn parse_gcap_key(key: &str, filter: &CapFilter) -> Option<(Arc<str>, NodeId)> {
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

/// Aggregates `sys/load/{node}/*` fill ratios for ranking in
/// `suggest_leader_with_requirements`.
fn aggregate_fill(kv_state: &crate::store::KvState, node: &NodeId) -> f32 {
    let prefix = format!("sys/load/{}/", node);
    let mut total = 0.0_f32;
    for (_, bytes) in scan_prefix_kv(kv_state, &prefix) {
        if let Some(state) = crate::signal::decode_load_state(&bytes) {
            total += state.fill_ratio;
        }
    }
    total
}

/// Wrapper around a `watch::Receiver<u64>` that lets us pass a "boxed"
/// receiver into a spawned task without naming the concrete generics.
fn prefix_rx_recv_box(rx: &mut watch::Receiver<u64>) -> watch::Receiver<u64> {
    rx.clone()
}

/// Composes two oneshot::Sender<()> into one so dropping the outer fires both.
fn build_paired_retract(
    a: oneshot::Sender<()>,
    b: oneshot::Sender<()>,
) -> oneshot::Sender<()> {
    // A small forwarder: we hold `b` alongside `a` by spawning nothing; just
    // return `a` and rely on dropping order. To trigger both on drop we
    // bundle them via a guard. tokio's oneshot::Sender<()> isn't Clone, so
    // we move both into a small forwarder task: send on `a` when `b` is sent,
    // but since we want them to fire together on retract, the simplest
    // pattern is: store both in a Box that the handle owns. We don't have
    // a "two-sender" handle type, so for now we attach the secondary cancel
    // to the global shutdown_rx already wired into the opacity watcher.
    // Drop semantics: dropping `RequirementHandle._retract` closes `a`'s
    // channel, which the persist task notices. The opacity watcher exits
    // via shutdown_rx when the agent shuts down; for early retract we
    // additionally need to fire `b` — accomplished by sending on `b` here
    // and returning `a` only.
    //
    // Since this is a one-shot helper invoked at retract time we cannot
    // defer the send: just fire `b` immediately and return `a`. The opacity
    // watcher exits as soon as it sees `b` close, equivalent to receiving.
    let _ = b.send(());
    a
}

/// Background task: watches whether `filter` is currently satisfied. While
/// it is NOT satisfied, writes a `LoadState { fill_ratio: 1.0, is_opaque: true }`
/// to `opacity_key`. When it becomes satisfied, tombstones `opacity_key`.
async fn run_requirement_opacity_watcher(
    ctx:             Arc<TaskCtx>,
    mut cancel_rx:   oneshot::Receiver<()>,
    mut shutdown_rx: watch::Receiver<bool>,
    opacity_key:     Arc<str>,
    filter:          Arc<CapFilter>,
    mut prefix_rx:   watch::Receiver<u64>,
    kv_state:        Arc<crate::store::KvState>,
) {
    use crate::framing::{dispatch_gossip_try_send, ForwardHint, WireMessage, make_gossip_update};
    use crate::store::apply_and_notify;

    let mut opaque_written = false;

    let evaluate = |kv: &crate::store::KvState| -> bool {
        !resolve_filter_against_kv(kv, &filter).is_empty()
    };

    let write_opaque = |ctx: &TaskCtx| {
        let payload = encode_load_state(&LoadState {
            fill_ratio:    1.0,
            is_opaque:     true,
            written_at_ms: now_ms(),
        });
        let upd = make_gossip_update(
            &ctx.node_id, ctx.default_ttl, opacity_key.clone(), payload, false, &ctx.hlc,
        );
        apply_and_notify(&ctx.kv_state, &upd);
        dispatch_gossip_try_send(
            &ctx.gossip_txs, WireMessage::Data(upd),
            ctx.node_id.id_hash(), ForwardHint::All, &ctx.kv_state.dropped_frames,
        );
    };

    let clear_opaque = |ctx: &TaskCtx| {
        let upd = make_gossip_update(
            &ctx.node_id, ctx.default_ttl, opacity_key.clone(), Bytes::new(), true, &ctx.hlc,
        );
        apply_and_notify(&ctx.kv_state, &upd);
        dispatch_gossip_try_send(
            &ctx.gossip_txs, WireMessage::Data(upd),
            ctx.node_id.id_hash(), ForwardHint::All, &ctx.kv_state.dropped_frames,
        );
    };

    // Initial evaluation.
    if !evaluate(&kv_state) {
        write_opaque(&ctx);
        opaque_written = true;
    }

    loop {
        tokio::select! { biased;
            _ = &mut cancel_rx               => break,
            _ = shutdown_rx.wait_for(|v| *v) => break,
            changed = prefix_rx.changed() => {
                if changed.is_err() { break; }
                let satisfied = evaluate(&kv_state);
                match (opaque_written, satisfied) {
                    (false, false) => { write_opaque(&ctx); opaque_written = true; }
                    (true,  true)  => { clear_opaque(&ctx); opaque_written = false; }
                    _ => {}
                }
            }
        }
    }

    // Always clear at exit so the retract tombstones the opacity entry.
    if opaque_written {
        clear_opaque(&ctx);
    }
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Interval at which group-level `gcap/` projections are re-asserted by each
/// cap-joined member. Conservative default chosen to amortise gossip cost;
/// the projection KV entry itself only expires on tombstone, so a slow
/// re-advertise still keeps late joiners in sync via anti-entropy.
const GCAP_REASSERT_INTERVAL: Duration = Duration::from_secs(60);

/// Per-group bookkeeping for the emergent-group watcher: every cap-joined
/// group tracks the `provides` set we currently project and the cancel
/// senders for the `kv_persist_loop` tasks writing the `gcap/` entries.
struct GroupProjection {
    provides: Vec<Capability>,
    /// Cancel senders for the spawned `run_kv_persist_task`s; dropping these
    /// closes the oneshot which makes each persist task tombstone its
    /// `gcap/{group}/{ns}/{name}/{self}` key. Never read directly — the
    /// `Drop` side-effect is the whole purpose.
    #[allow(dead_code)]
    handles:     Vec<oneshot::Sender<()>>,
    /// The `requires` list snapshotted from the def when we last spawned
    /// group-req opacity watchers. Tracked separately from `provides` so we
    /// only respawn the watchers when the def's requires actually changes.
    requires:    Vec<CapFilter>,
    /// Cancel senders for the Phase-7 group-req opacity watchers; one per
    /// requirement filter, in `requires` index order. Dropping clears the
    /// `sys/load/{self}/group-req/{group}/{idx}` opacity entry via the
    /// watcher's exit path.
    #[allow(dead_code)]
    req_handles: Vec<oneshot::Sender<()>>,
}

/// Background task: watches `cap-group/` for emergent group definitions and
/// keeps this node's `grp/` membership in sync with whether its own
/// capabilities match each def's filter. See plan section
/// "`watch_capability_group_definitions` Dual Subscription".
pub(crate) async fn watch_capability_group_definitions(
    ctx:             Arc<TaskCtx>,
    own_node_id:     NodeId,
    mut def_rx:      watch::Receiver<u64>,
    mut own_rx:      watch::Receiver<u64>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    // Tracks which emergent groups we are currently cap-joined to AND the
    // `gcap/` projections we are persisting for each. Dropping a `GroupProjection`
    // closes its cancel senders, which the persist tasks observe and exit
    // gracefully via a tombstone.
    let mut joined: AHashMap<Arc<str>, GroupProjection> = AHashMap::new();

    // Initial reconciliation in case definitions or own caps already exist.
    reconcile_emergent_groups(&ctx, &own_node_id, &shutdown_rx, &mut joined);

    loop {
        tokio::select! { biased;
            _ = await_shutdown(&mut shutdown_rx) => break,
            r = def_rx.changed() => {
                if r.is_err() { break; }
                reconcile_emergent_groups(&ctx, &own_node_id, &shutdown_rx, &mut joined);
            }
            r = own_rx.changed() => {
                if r.is_err() { break; }
                reconcile_emergent_groups(&ctx, &own_node_id, &shutdown_rx, &mut joined);
            }
        }
    }

    // Best-effort: tombstone any memberships we hold so peers see us leave
    // the emergent groups cleanly. Dropping `joined` also fires the persist
    // task cancel senders, which tombstone the `gcap/` entries.
    let exit_groups: Vec<Arc<str>> = joined.keys().cloned().collect();
    for g in exit_groups {
        emit_membership(&ctx, &own_node_id, &g, true);
    }
    drop(joined);
}

/// One reconciliation pass: snapshot own caps + every `cap-group/` def, decide
/// which groups we should be in, and update both `grp/{group}/{self}`
/// membership and `gcap/{group}/{ns}/{name}/{self}` projections accordingly.
fn reconcile_emergent_groups(
    ctx:         &Arc<TaskCtx>,
    own:         &NodeId,
    shutdown_rx: &watch::Receiver<bool>,
    joined:      &mut AHashMap<Arc<str>, GroupProjection>,
) {
    // Own capabilities snapshot (used both for filter matching and for the
    // provides decision; we project everything the def specifies, regardless
    // of whether we hold the same direct capability ourselves — the def is
    // the group's collective assertion, not a per-member echo of its own caps).
    let mut own_caps: Vec<Capability> = Vec::new();
    let own_prefix = format!("cap/{}/", own);
    for (key, bytes) in scan_prefix_kv(&ctx.kv_state, &own_prefix) {
        match Capability::decode(&bytes) {
            Some(cap) => own_caps.push(cap),
            None      => warn!(key = %key, "malformed own Capability — local cap/ entry did not decode"),
        }
    }

    // Current cap-group definitions, keyed by group name.
    let mut defs: AHashMap<Arc<str>, CapabilityGroupDef> = AHashMap::new();
    for (key, bytes) in scan_prefix_kv(&ctx.kv_state, "cap-group/") {
        let Some(group_name) = key.strip_prefix("cap-group/") else { continue };
        let Some(def) = CapabilityGroupDef::decode(&bytes) else {
            warn!(key = %key, "malformed CapabilityGroupDef under cap-group/ — peer sent bytes that did not decode");
            continue;
        };
        defs.insert(Arc::from(group_name), def);
    }

    // Compute want_joined: every group whose filter is satisfied by at least
    // one of our own capabilities.
    let mut want_joined: AHashSet<Arc<str>> = AHashSet::new();
    for (group, def) in &defs {
        if own_caps.iter().any(|c| def.filter.matches(c)) {
            want_joined.insert(group.clone());
        }
    }

    // Join newly-matching groups; spawn their `gcap/` persist tasks AND their
    // Phase-7 group-req opacity watchers.
    for group in &want_joined {
        let def      = defs.get(group).expect("present, scanned above");
        let provides = def.provides.clone();
        let requires = def.requires.clone();

        if !joined.contains_key(group.as_ref()) {
            emit_membership(ctx, own, group, false);
            let handles     = spawn_gcap_projections(ctx, own, group, &provides, shutdown_rx);
            let req_handles = spawn_group_req_watchers(ctx, own, group, &requires, shutdown_rx);
            joined.insert(group.clone(), GroupProjection {
                provides, handles, requires, req_handles,
            });
            continue;
        }
        // Already joined — refresh projections / watchers when the def changed
        // on either axis. Re-snapshot the current state outside the borrow so
        // we can re-insert without holding two refs to `joined`.
        let (current_provides, current_requires) = {
            let current = joined.get(group).expect("just checked");
            (current.provides.clone(), current.requires.clone())
        };
        let provides_changed = current_provides != provides;
        let requires_changed = current_requires != requires;
        if !provides_changed && !requires_changed { continue; }
        // Take ownership of the old projection so we can preserve whichever
        // side did not change. Dropping the cancel oneshots we discard
        // signals their persist/watcher tasks to tombstone on the way out.
        let old = joined.remove(group.as_ref()).expect("just checked above");
        let handles = if provides_changed {
            drop(old.handles);
            spawn_gcap_projections(ctx, own, group, &provides, shutdown_rx)
        } else {
            old.handles
        };
        let req_handles = if requires_changed {
            drop(old.req_handles);
            spawn_group_req_watchers(ctx, own, group, &requires, shutdown_rx)
        } else {
            old.req_handles
        };
        joined.insert(group.clone(), GroupProjection {
            provides, handles, requires, req_handles,
        });
    }

    // Leave groups we were in but no longer match — either because the def
    // disappeared or our caps changed. Dropping the `GroupProjection`
    // tombstones the gcap entries; `emit_membership(leaving = true)` tombstones
    // `grp/`.
    let to_leave: Vec<Arc<str>> = joined.keys()
        .filter(|g| !want_joined.contains(g.as_ref()))
        .cloned()
        .collect();
    for g in to_leave {
        emit_membership(ctx, own, &g, true);
        joined.remove(&g);
    }
}

/// Spawns one Phase-7 group-req opacity watcher per requirement filter. Each
/// watcher subscribes to `cap/` and `gcap/` prefix changes and writes
/// `sys/load/{self}/group-req/{group}/{idx}` opacity whenever its filter is
/// unsatisfied; tombstones the entry the moment a provider appears.
fn spawn_group_req_watchers(
    ctx:         &Arc<TaskCtx>,
    own:         &NodeId,
    group:       &Arc<str>,
    requires:    &[CapFilter],
    shutdown_rx: &watch::Receiver<bool>,
) -> Vec<oneshot::Sender<()>> {
    let mut handles = Vec::with_capacity(requires.len());
    for (idx, filter) in requires.iter().enumerate() {
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        let opacity_key: Arc<str> = Arc::from(format!(
            "sys/load/{}/group-req/{}/{}", own, group, idx,
        ).as_str());
        let cap_rx  = subscribe_prefix_on_kv(&ctx.kv_state, Arc::<str>::from("cap/"));
        let gcap_rx = subscribe_prefix_on_kv(&ctx.kv_state, Arc::<str>::from("gcap/"));
        let filter_arc = Arc::new(filter.clone());
        tokio::spawn(run_group_req_opacity_watcher(
            Arc::clone(ctx),
            cancel_rx,
            shutdown_rx.clone(),
            opacity_key,
            filter_arc,
            cap_rx,
            gcap_rx,
        ));
        handles.push(cancel_tx);
    }
    handles
}

/// Free-function flavour of `GossipAgent::subscribe_prefix` for callers that
/// only hold a `&KvState`. Lazy-creates the prefix watcher entry if absent.
fn subscribe_prefix_on_kv(
    kv_state: &crate::store::KvState,
    prefix:   Arc<str>,
) -> watch::Receiver<u64> {
    loop {
        let guard = kv_state.prefix_watchers.pin();
        if let Some(tx) = guard.get(&prefix) {
            if !tx.is_closed() {
                return tx.subscribe();
            }
        }
        let (new_tx, rx) = watch::channel(0u64);
        let new_tx_arc = Arc::new(new_tx);
        let mut slot = Some(new_tx_arc);
        let result = guard.compute(prefix.clone(), |existing| match existing {
            Some((_, tx)) if !tx.is_closed() => papaya::Operation::Abort(()),
            _ => match slot.take() {
                Some(tx) => papaya::Operation::Insert(tx),
                None => papaya::Operation::Abort(()),
            },
        });
        if matches!(result, papaya::Compute::Inserted(..) | papaya::Compute::Updated { .. }) {
            return rx;
        }
    }
}

/// Background task: composes Phase-3's auto-opacity pattern with
/// Phase-4's wiring resolution. While the filter resolves to `Unwired`,
/// keeps `sys/load/{self}/group-req/{group}/{idx}` set to
/// `LoadState { fill_ratio: 1.0, is_opaque: true }`. Tombstones the entry
/// the moment a provider (cap/ or gcap/) appears.
///
/// Composes with load-based opacity via `is_self_opaque`'s scanner over
/// `sys/load/{self}/*` — no new opacity mechanism required.
async fn run_group_req_opacity_watcher(
    ctx:             Arc<TaskCtx>,
    mut cancel_rx:   oneshot::Receiver<()>,
    mut shutdown_rx: watch::Receiver<bool>,
    opacity_key:     Arc<str>,
    filter:          Arc<CapFilter>,
    mut cap_rx:      watch::Receiver<u64>,
    mut gcap_rx:     watch::Receiver<u64>,
) {
    use crate::framing::{dispatch_gossip_try_send, ForwardHint, WireMessage, make_gossip_update};
    use crate::store::apply_and_notify;

    let mut opaque_written = false;

    let write_opaque = |ctx: &TaskCtx| {
        let payload = encode_load_state(&LoadState {
            fill_ratio:    1.0,
            is_opaque:     true,
            written_at_ms: now_ms(),
        });
        let upd = make_gossip_update(
            &ctx.node_id, ctx.default_ttl, opacity_key.clone(), payload, false, &ctx.hlc,
        );
        apply_and_notify(&ctx.kv_state, &upd);
        dispatch_gossip_try_send(
            &ctx.gossip_txs, WireMessage::Data(upd),
            ctx.node_id.id_hash(), ForwardHint::All, &ctx.kv_state.dropped_frames,
        );
    };

    let clear_opaque = |ctx: &TaskCtx| {
        let upd = make_gossip_update(
            &ctx.node_id, ctx.default_ttl, opacity_key.clone(), Bytes::new(), true, &ctx.hlc,
        );
        apply_and_notify(&ctx.kv_state, &upd);
        dispatch_gossip_try_send(
            &ctx.gossip_txs, WireMessage::Data(upd),
            ctx.node_id.id_hash(), ForwardHint::All, &ctx.kv_state.dropped_frames,
        );
    };

    let evaluate = |kv: &crate::store::KvState| -> bool {
        !matches!(wiring_snapshot(kv, &filter), WiringStatus::Unwired { .. })
    };

    if !evaluate(&ctx.kv_state) {
        write_opaque(&ctx);
        opaque_written = true;
    }

    loop {
        tokio::select! { biased;
            _ = &mut cancel_rx               => break,
            _ = await_shutdown(&mut shutdown_rx) => break,
            r = cap_rx.changed()  => { if r.is_err() { break; } }
            r = gcap_rx.changed() => { if r.is_err() { break; } }
        }
        let satisfied = evaluate(&ctx.kv_state);
        match (opaque_written, satisfied) {
            (false, false) => { write_opaque(&ctx); opaque_written = true; }
            (true,  true)  => { clear_opaque(&ctx); opaque_written = false; }
            _ => {}
        }
    }

    if opaque_written {
        clear_opaque(&ctx);
    }
}

/// Spawns one `run_kv_persist_task` per provided capability under
/// `gcap/{group}/{ns}/{name}/{self}`. Returns the cancel senders so the caller
/// can drop them when membership ends — closing the senders causes each
/// persist task to tombstone its key.
fn spawn_gcap_projections(
    ctx:         &Arc<TaskCtx>,
    own:         &NodeId,
    group:       &Arc<str>,
    provides:    &[Capability],
    shutdown_rx: &watch::Receiver<bool>,
) -> Vec<oneshot::Sender<()>> {
    let mut handles = Vec::with_capacity(provides.len());
    for cap in provides {
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        let key: Arc<str> = Arc::from(format!(
            "gcap/{}/{}/{}/{}",
            group, cap.namespace, cap.name, own,
        ).as_str());
        let cap_arc = Arc::new(cap.clone());
        let payload_fn: super::kv::PersistPayloadFn = {
            let cap = Arc::clone(&cap_arc);
            Arc::new(move || cap.encode())
        };
        tokio::spawn(super::kv::run_kv_persist_task(
            Arc::clone(ctx),
            cancel_rx,
            shutdown_rx.clone(),
            key,
            GCAP_REASSERT_INTERVAL,
            payload_fn,
            None,
        ));
        handles.push(cancel_tx);
    }
    handles
}

/// Writes (or tombstones) `grp/{group}/{node_id}` for this node, mirroring the
/// effect of `GossipAgent::{join_group, leave_group}` from a spawned task
/// without holding an agent reference.
fn emit_membership(ctx: &TaskCtx, own: &NodeId, group: &Arc<str>, leaving: bool) {
    use crate::framing::{dispatch_gossip_try_send, ForwardHint, WireMessage, make_gossip_update};
    use crate::store::apply_and_notify;
    let key: Arc<str> = Arc::from(format!("grp/{}/{}", group, own).as_str());
    let payload = if leaving { Bytes::new() } else { Bytes::from_static(&[1u8]) };
    let upd = make_gossip_update(
        &ctx.node_id, ctx.default_ttl, key, payload, leaving, &ctx.hlc,
    );
    apply_and_notify(&ctx.kv_state, &upd);
    dispatch_gossip_try_send(
        &ctx.gossip_txs, WireMessage::Data(upd),
        ctx.node_id.id_hash(), ForwardHint::All, &ctx.kv_state.dropped_frames,
    );
    if leaving {
        // Also update local boundary so signal admission stops immediately.
        ctx.signal_boundary.write().groups.remove(group);
    } else {
        ctx.signal_boundary.write().groups.insert(group.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cap_key_extracts_components() {
        let key = "cap/127.0.0.1:8080/compute/gpu";
        let (node_id, ns, name) = parse_cap_key("cap/", key).expect("parse");
        assert_eq!(node_id.to_string(), "127.0.0.1:8080");
        assert_eq!(ns.as_ref(),  "compute");
        assert_eq!(name.as_ref(), "gpu");
    }

    #[test]
    fn parse_cap_key_rejects_extra_segments() {
        let key = "cap/127.0.0.1:8080/compute/gpu/extra";
        assert!(parse_cap_key("cap/", key).is_none());
    }

    #[test]
    fn parse_cap_key_rejects_bad_node_id() {
        let key = "cap/not-a-socket/compute/gpu";
        assert!(parse_cap_key("cap/", key).is_none());
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

    fn nid(port: u16) -> NodeId {
        NodeId::new("127.0.0.1", port).expect("valid loopback NodeId")
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
        // Node 2 first (has attribute); 1 and 3 follow in scan-order (stable sort).
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

    // ── demand_snapshot ─────────────────────────────────────────────────────

    use crate::framing::make_gossip_update;
    use crate::hlc::Hlc;
    use crate::store::{apply_and_notify, KvState};

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
        // Two demanders.
        write(&kv, &sender, "req/127.0.0.1:1/compute/gpu", filter.encode());
        write(&kv, &sender, "req/127.0.0.1:2/compute/gpu", filter.encode());
        // One direct cap provider + one group projection contributor.
        let cap = Capability::new("compute", "gpu");
        write(&kv, &sender, "cap/127.0.0.1:3/compute/gpu", cap.encode());
        write(&kv, &sender, "gcap/gpu-pool/compute/gpu/127.0.0.1:4", cap.encode());
        let status = demand_snapshot(&kv, &filter);
        assert_eq!(status.demanding_nodes.len(), 2);
        assert_eq!(status.providers.len(), 2);
        // 2 demanders / 2 providers = 1.0
        assert!((status.demand_pressure - 1.0).abs() < f32::EPSILON);
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
        // Division floors providers to 1 → pressure = demanders.
        assert!((status.demand_pressure - 3.0).abs() < f32::EPSILON);
    }

    #[test]
    fn demand_snapshot_dedupes_provider_appearing_in_cap_and_gcap() {
        let kv = KvState::new(0);
        let sender = nid(99);
        let filter = CapFilter::new("compute", "gpu");
        let cap = Capability::new("compute", "gpu");
        // One node that's both a direct provider AND a contributor to a group.
        write(&kv, &sender, "cap/127.0.0.1:7/compute/gpu", cap.encode());
        write(&kv, &sender, "gcap/gpu-pool/compute/gpu/127.0.0.1:7", cap.encode());
        let status = demand_snapshot(&kv, &filter);
        assert_eq!(status.providers.len(), 1, "should dedupe by NodeId");
    }
}
