//! Capability / opacity / wiring operations — [`CapabilitiesHandle`].
//!
//! Consolidates capability advertisement, requirement declaration, wiring
//! resolution, demand tracking, emergent group definitions, and the full
//! opacity/load pheromone trail API.
//!
//! Obtain a handle via [`GossipAgent::capabilities`](crate::GossipAgent::capabilities).

use crate::capability::{
    CallerContext, CapabilityGroupDef, CapabilityGroupHandle,
    CapabilityReg, Capability, CapEntry, CapFilter, CapabilityEvent,
    ReqEntry, RequirementHandle, RequirementStatus, WiringStatus, WiringProvider,
};
use crate::node_id::NodeId;
use crate::signal::{LoadState, OpacityHandle, OpacityHint, OpacityState};
use ahash::AHashMap;
use bytes::Bytes;
use std::{sync::Arc, time::Duration};
use tokio::{sync::{mpsc, watch}, time};
use tracing::warn;

use super::TaskCtx;
use super::capability_ops::{
    aggregate_fill, await_shutdown, is_cap_locality_key, now_ms, parse_cap_key_or_warn,
    resolve_filter_against_kv, scan_prefix_kv_with_ts, RegEntry,
    run_consolidated_opacity_watcher, WATCHER_DEBOUNCE_WINDOW,
};
use super::demand::demand_snapshot;
use super::helpers::{
    group_members_ctx, kv_get, kv_subscribe,
    kv_subscribe_prefix_with_predicate, self_locality_ctx,
};
use super::kv::run_kv_persist_task;
use super::opacity::{
    effective_opacity_ctx, manage_opacity_ctx, manage_opacity_gated_ctx,
    opacity_ctx, peer_load_ctx,
};
use super::wiring::{
    annotate_provider_with_locality, apply_locality_pref, locality_depth,
    provider_depth, wiring_snapshot,
};
use crate::capability::DemandStatus;
use crate::locality::LocalityPreference;
use crate::signal::kv_ns;
use std::sync::atomic::Ordering;

/// Domain handle for capability, opacity, wiring, and demand operations.
/// Obtained via [`GossipAgent::capabilities()`].
///
/// Covers capability advertisement, requirement declaration, wiring resolution,
/// demand tracking, emergent group definitions, and the load pheromone trail API.
///
/// The handle is `Clone + Send + Sync` and can be stored, moved across tasks,
/// or captured in closures.
#[derive(Clone)]
pub struct CapabilitiesHandle {
    pub(crate) ctx: Arc<TaskCtx>,
}

impl CapabilitiesHandle {
    // ── Capability advertisement ─────────────────────────────────────────────

    /// Advertises a [`Capability`] under `cap/{node_id}/{namespace}/{name}`.
    ///
    /// Re-asserts on every `interval` tick. Drop the returned [`CapabilityReg`]
    /// to tombstone the entry; shutdown tombstones it automatically.
    #[must_use]
    pub fn advertise_capability(
        &self,
        capability: Capability,
        interval:   Duration,
    ) -> CapabilityReg {
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let shutdown_rx            = self.ctx.shutdown_tx.subscribe();
        let ctx: Arc<TaskCtx>      = Arc::clone(&self.ctx);
        let kv_key: Arc<str>       = Arc::from(format!(
            "cap/{}/{}/{}", ctx.node_id, capability.namespace, capability.name,
        ).as_str());
        let interval_ms = interval.as_millis() as u64;
        let entry = Arc::new(CapEntry { capability, refresh_interval_ms: interval_ms });
        let payload_fn: super::kv::PersistPayloadFn = {
            let e = Arc::clone(&entry);
            Arc::new(move || e.encode())
        };
        self.ctx.spawn_task(run_kv_persist_task(
            Arc::clone(&ctx.core), cancel_rx, shutdown_rx, kv_key, interval, payload_fn, None,
        ));
        CapabilityReg { _retract: cancel_tx }
    }

    // ── Resolution ───────────────────────────────────────────────────────────

    /// Snapshot scan: returns every live capability in the local KV view matching `filter`.
    pub fn resolve(&self, filter: &CapFilter) -> Vec<(NodeId, Capability)> {
        self.resolve_for_caller(filter, &CallerContext::unrestricted())
    }

    /// Like `resolve`, but enforces `Capability::authorized_callers`.
    pub fn resolve_for_caller(
        &self,
        filter: &CapFilter,
        ctx:    &CallerContext,
    ) -> Vec<(NodeId, Capability)> {
        use super::capability_ops::{scan_prefix_kv_with_ts, is_cap_locality_key, parse_cap_key_or_warn};
        use super::wiring::rank_node_matches;
        use crate::capability::CapEntry;
        let now_ms_val = now_ms();
        let mut out = Vec::new();
        for (key, bytes, hlc_ts) in scan_prefix_kv_with_ts(&self.ctx.kv_state, "cap/") {
            if is_cap_locality_key(&key) { continue; }
            let Some((node_id, _ns, _name)) = parse_cap_key_or_warn("cap/", &key) else { continue };
            let Some(entry) = CapEntry::decode(&bytes)
                .or_else(|| Capability::decode(&bytes).map(|cap| CapEntry { capability: cap, refresh_interval_ms: 60_000 }))
            else {
                warn!(key = %key, "malformed Capability — peer sent bytes that did not decode");
                continue;
            };
            if !entry.is_fresh(hlc_ts, now_ms_val) { continue; }
            if let Some(max_age) = filter.max_age {
                let entry_physical_ms = crate::hlc::physical_ms(hlc_ts);
                if now_ms_val.saturating_sub(entry_physical_ms) > max_age.as_millis() as u64 { continue; }
            }
            let cap = entry.capability;
            // WS-F / Schema-Evo (E2): detect a schema-version mismatch — the provider matches the
            // requested (ns, name) + attributes but advertises a different schema_id than the
            // filter asked for. Count it (legible drift) rather than letting the schema-strict
            // matches() below silently exclude it. Detection-not-prevention: the provider is still
            // routed around; register a migration (tier 3) or reconcile the versions.
            if filter.schema_id.is_some()
                && filter.matches_ignoring_schema(&cap)
                && !filter.matches(&cap)
            {
                warn!(node = %node_id, ns = %cap.namespace, name = %cap.name,
                    expected = ?filter.schema_id, advertised = ?cap.schema_id,
                    "schema version mismatch — routing around the provider");
                self.ctx.schema_mismatch.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                continue;
            }
            if filter.matches(&cap) && ctx.can_see(&cap) {
                // WS-D / M6 (D4 enforce, D5 detect): if a capauthz policy governs `ns/name`, route
                // around an advertiser whose signed role does not satisfy it — and count the
                // rejection (detection-not-prevention; the advertisement still propagated per LWW).
                #[cfg(feature = "compliance")]
                if let Some(required) = super::capauthz::required_roles(&self.ctx, &cap.namespace, &cap.name)
                    && !super::capauthz::advertiser_authorized(&self.ctx, &node_id, &required)
                {
                    warn!(node = %node_id, ns = %cap.namespace, name = %cap.name,
                        "capauthz: advertiser lacks a required role — routing around it");
                    self.ctx.cap_authz_violations.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    continue;
                }
                out.push((node_id, cap));
            }
        }
        if let Some(ranking) = &filter.ranking {
            rank_node_matches(&mut out, ranking);
        }
        out
    }

    /// Push-based stream of [`CapabilityEvent`]s for capabilities matching `filter`.
    pub fn watch_capabilities(&self, filter: CapFilter) -> mpsc::Receiver<CapabilityEvent> {
        let (tx, rx) = mpsc::channel::<CapabilityEvent>(64);
        let needle = format!("/{}/{}", filter.namespace, filter.name);
        let mut prefix_rx = kv_subscribe_prefix_with_predicate(
            &self.ctx,
            Arc::<str>::from("cap/"),
            move |k| k.ends_with(&needle),
        );
        let kv_state = Arc::clone(&self.ctx.kv_state);
        let mut shutdown_rx = self.ctx.shutdown_tx.subscribe();

        let initial = self.resolve(&filter);
        let mut known: AHashMap<(NodeId, Arc<str>, Arc<str>), Capability> = AHashMap::new();
        for (node_id, cap) in &initial {
            known.insert((node_id.clone(), Arc::clone(&cap.namespace), Arc::clone(&cap.name)), cap.clone());
        }
        let tx_initial = tx.clone();
        let initial_owned = initial;
        self.ctx.spawn_task(async move {
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
                        let deadline = time::Instant::now() + WATCHER_DEBOUNCE_WINDOW;
                        loop {
                            tokio::select! { biased;
                                _ = time::sleep_until(deadline) => break,
                                r = prefix_rx.changed() => { if r.is_err() { return; } }
                            }
                        }
                        let now_ms_v = now_ms();
                        let mut current: AHashMap<(NodeId, Arc<str>, Arc<str>), Capability> = AHashMap::new();
                        for (key, bytes, hlc_ts) in scan_prefix_kv_with_ts(&kv_state, "cap/") {
                            if is_cap_locality_key(&key) { continue; }
                            let Some((node_id, ns, name)) = parse_cap_key_or_warn("cap/", &key) else { continue };
                            let Some(entry) = CapEntry::decode(&bytes)
                                .or_else(|| Capability::decode(&bytes).map(|cap| CapEntry { capability: cap, refresh_interval_ms: 60_000 }))
                            else { continue; };
                            if !entry.is_fresh(hlc_ts, now_ms_v) { continue; }
                            let cap = entry.capability;
                            if !filter.matches(&cap) { continue; }
                            current.insert((node_id, ns, name), cap);
                        }
                        let removed: Vec<_> = known.keys().filter(|k| !current.contains_key(*k)).cloned().collect();
                        for k in &removed {
                            known.remove(k);
                            let _ = tx.send(CapabilityEvent::Removed { node_id: k.0.clone(), namespace: Arc::clone(&k.1), name: Arc::clone(&k.2) }).await;
                        }
                        for (k, cap) in &current {
                            let changed = match known.get(k) { None => true, Some(old) => old != cap };
                            if changed {
                                known.insert(k.clone(), cap.clone());
                                let _ = tx.send(CapabilityEvent::Added { node_id: k.0.clone(), capability: cap.clone() }).await;
                            }
                        }
                    }
                }
            }
        });
        rx
    }

    // ── Requirements ─────────────────────────────────────────────────────────

    /// Declares a requirement and spawns an opacity watcher. Drop the returned
    /// [`RequirementHandle`] to retract both the requirement and active opacity entry.
    #[must_use]
    pub fn declare_requirement(&self, filter: CapFilter, interval: Duration) -> RequirementHandle {
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let shutdown_rx             = self.ctx.shutdown_tx.subscribe();
        let ctx: Arc<TaskCtx>       = Arc::clone(&self.ctx);

        let kv_key: Arc<str> = Arc::from(format!(
            "req/{}/{}/{}", ctx.node_id, filter.namespace, filter.name,
        ).as_str());
        let opacity_key: Arc<str> = Arc::from(format!(
            "sys/load/{}/req/{}/{}", ctx.node_id, filter.namespace, filter.name,
        ).as_str());
        let interval_ms = interval.as_millis() as u64;
        let filter_arc = Arc::new(filter);
        let payload_fn: super::kv::PersistPayloadFn = {
            let e = Arc::new(ReqEntry { filter: (*filter_arc).clone(), refresh_interval_ms: interval_ms });
            Arc::new(move || e.encode())
        };
        self.ctx.spawn_task(run_kv_persist_task(
            Arc::clone(&ctx.core), cancel_rx, shutdown_rx, kv_key, interval, payload_fn, None,
        ));

        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let registry  = Arc::clone(&ctx.filter_opacity_registry);
        registry.entries.lock().unwrap_or_else(|e| e.into_inner()).push(RegEntry {
            opacity_key,
            filter:    Arc::clone(&filter_arc),
            cancelled: Arc::clone(&cancelled),
        });
        registry.notify.notify_one();

        if !registry.spawned.swap(true, Ordering::AcqRel) {
            let reg_arc = Arc::clone(&registry);
            let ctx2    = Arc::clone(&ctx);
            let sd_rx   = self.ctx.shutdown_tx.subscribe();
            self.ctx.spawn_task(run_consolidated_opacity_watcher(ctx2, sd_rx, reg_arc));
        }

        let opacity_drop = crate::capability::OpacityDropGuard {
            cancelled,
            notify: Arc::clone(&registry.notify),
        };
        RequirementHandle { _retract: cancel_tx, _opacity_drop: opacity_drop }
    }

    /// Push-based view of one requirement's current satisfaction status.
    pub fn watch_requirement(&self, filter: CapFilter) -> watch::Receiver<RequirementStatus> {
        let initial = self.resolve(&filter);
        let initial_status = if initial.is_empty() {
            RequirementStatus::Unsatisfied { filter: filter.clone() }
        } else {
            RequirementStatus::Satisfied { providers: initial }
        };
        let (tx, rx) = watch::channel(initial_status);
        let needle = format!("/{}/{}", filter.namespace, filter.name);
        let mut prefix_rx = kv_subscribe_prefix_with_predicate(
            &self.ctx,
            Arc::<str>::from("cap/"),
            move |k| k.ends_with(&needle),
        );
        let kv_state = Arc::clone(&self.ctx.kv_state);
        let mut shutdown_rx = self.ctx.shutdown_tx.subscribe();
        self.ctx.spawn_task(async move {
            loop {
                tokio::select! { biased;
                    _ = await_shutdown(&mut shutdown_rx) => return,
                    changed = prefix_rx.changed() => {
                        if changed.is_err() { return; }
                        let deadline = time::Instant::now() + WATCHER_DEBOUNCE_WINDOW;
                        loop {
                            tokio::select! { biased;
                                _ = time::sleep_until(deadline) => break,
                                r = prefix_rx.changed() => { if r.is_err() { return; } }
                            }
                        }
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

    /// Picks a group member that satisfies all requirement filters with the lowest load.
    pub fn suggest_leader_with_requirements(
        &self,
        group:        &str,
        requirements: &[CapFilter],
    ) -> Option<NodeId> {
        let members = group_members_ctx(&self.ctx, group);
        if members.is_empty() { return None; }
        let mut candidates: Vec<NodeId> = members.into_iter()
            .filter(|m| {
                requirements.iter().all(|req| {
                    resolve_filter_against_kv(&self.ctx.kv_state, req).iter().any(|(provider, _)| provider == m)
                })
            })
            .collect();
        if candidates.is_empty() { return None; }
        candidates.sort_by(|a, b| {
            let la = aggregate_fill(&self.ctx.kv_state, a);
            let lb = aggregate_fill(&self.ctx.kv_state, b);
            la.partial_cmp(&lb).unwrap_or(std::cmp::Ordering::Equal)
        });
        candidates.into_iter().next()
    }

    // ── Emergent groups ──────────────────────────────────────────────────────

    /// Publishes a [`CapabilityGroupDef`] to the mesh and re-asserts it on every `interval`.
    ///
    /// **Capability groups** control **emergent discovery**: each node independently evaluates
    /// the group's capability filter against its own advertised capabilities. Nodes that match
    /// self-join and project their capabilities into `gcap/{group}/...`; no coordinator assigns
    /// membership. The group becomes visible to `resolve_wiring` and `watch_wiring` automatically.
    ///
    /// This is distinct from **signal boundary groups** (`MeshHandle::join_group`), which control
    /// **explicit routing**: a node joins by calling `join_group` directly, and
    /// `SignalScope::Group(name)` then delivers only to current members.
    ///
    /// Drop the returned [`CapabilityGroupHandle`] to retract the definition.
    #[must_use]
    pub fn define_capability_group(
        &self,
        group:    impl Into<Arc<str>>,
        def:      CapabilityGroupDef,
        interval: Duration,
    ) -> CapabilityGroupHandle {
        let group: Arc<str> = group.into();
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let shutdown_rx            = self.ctx.shutdown_tx.subscribe();
        let ctx: Arc<TaskCtx>      = Arc::clone(&self.ctx);
        let kv_key: Arc<str>       = Arc::from(format!("cap-group/{}", group).as_str());
        let def_arc                = Arc::new(def);
        let payload_fn: super::kv::PersistPayloadFn = {
            let def = Arc::clone(&def_arc);
            Arc::new(move || def.encode())
        };
        self.ctx.spawn_task(run_kv_persist_task(
            Arc::clone(&ctx.core), cancel_rx, shutdown_rx, kv_key, interval, payload_fn, None,
        ));
        CapabilityGroupHandle { _retract: cancel_tx, group }
    }

    // ── Wiring ───────────────────────────────────────────────────────────────

    /// Resolves the wiring state for `filter` against the local KV view.
    pub fn resolve_wiring(&self, filter: &CapFilter) -> WiringStatus {
        wiring_snapshot(&self.ctx.kv_state, filter)
    }

    /// Like `resolve` but annotates each provider with its shared locality depth.
    pub fn resolve_with_locality(
        &self,
        filter: &CapFilter,
        pref:   LocalityPreference,
    ) -> Vec<(NodeId, Capability, usize)> {
        let self_loc = self_locality_ctx(&self.ctx);
        let mut annotated: Vec<(NodeId, Capability, usize)> = self.resolve(filter)
            .into_iter()
            .map(|(node_id, cap)| {
                let depth = locality_depth(&self.ctx.kv_state, self_loc.as_ref(), &node_id);
                (node_id, cap, depth)
            })
            .collect();
        apply_locality_pref(&mut annotated, pref, |(_, _, d)| *d);
        annotated
    }

    /// Locality-aware version of [`resolve_wiring`](Self::resolve_wiring).
    pub fn resolve_wiring_with_locality(
        &self,
        filter: &CapFilter,
        pref:   LocalityPreference,
    ) -> WiringStatus {
        let self_loc = self_locality_ctx(&self.ctx);
        let raw = wiring_snapshot(&self.ctx.kv_state, filter);
        let WiringStatus::Wired { providers } = raw else { return raw; };
        let mut annotated: Vec<WiringProvider> = providers.into_iter()
            .map(|p| annotate_provider_with_locality(p, &self.ctx.kv_state, self_loc.as_ref()))
            .collect();
        apply_locality_pref(&mut annotated, pref, provider_depth);
        if annotated.is_empty() {
            WiringStatus::Unwired { filter: filter.clone() }
        } else {
            WiringStatus::Wired { providers: annotated }
        }
    }

    /// Push-based view of the wiring state for `filter`.
    pub fn watch_wiring(&self, filter: CapFilter) -> watch::Receiver<WiringStatus> {
        let initial  = wiring_snapshot(&self.ctx.kv_state, &filter);
        let (tx, rx) = watch::channel(initial);
        let cap_needle  = format!("/{}/{}",  filter.namespace, filter.name);
        let gcap_needle = format!("/{}/{}/", filter.namespace, filter.name);
        let mut cap_rx  = kv_subscribe_prefix_with_predicate(
            &self.ctx, Arc::<str>::from("cap/"),  move |k| k.ends_with(&cap_needle));
        let mut gcap_rx = kv_subscribe_prefix_with_predicate(
            &self.ctx, Arc::<str>::from("gcap/"), move |k| k.contains(&gcap_needle));
        let kv_state = Arc::clone(&self.ctx.kv_state);
        let mut shutdown_rx = self.ctx.shutdown_tx.subscribe();
        self.ctx.spawn_task(async move {
            loop {
                tokio::select! { biased;
                    _ = await_shutdown(&mut shutdown_rx) => return,
                    r = cap_rx.changed()  => { if r.is_err() { return; } }
                    r = gcap_rx.changed() => { if r.is_err() { return; } }
                }
                let deadline = time::Instant::now() + WATCHER_DEBOUNCE_WINDOW;
                loop {
                    tokio::select! { biased;
                        _ = time::sleep_until(deadline) => break,
                        r = cap_rx.changed()  => { if r.is_err() { return; } }
                        r = gcap_rx.changed() => { if r.is_err() { return; } }
                    }
                }
                let next = wiring_snapshot(&kv_state, &filter);
                let unchanged = { *tx.borrow() == next };
                if unchanged { continue; }
                if tx.send(next).is_err() { return; }
            }
        });
        rx
    }

    /// Emits `kind` to the best wired provider for `filter`.
    pub fn signal_wired_via(
        &self,
        filter:  &CapFilter,
        kind:    impl Into<Arc<str>>,
        payload: impl Into<Bytes>,
    ) -> crate::capability::WiredEmitOutcome {
        super::wiring::signal_wired_via_ctx(&self.ctx, filter, kind.into(), payload.into(), None)
    }

    /// Locality-aware version of [`signal_wired_via`](Self::signal_wired_via).
    pub fn signal_wired_via_locality(
        &self,
        filter:  &CapFilter,
        kind:    impl Into<Arc<str>>,
        payload: impl Into<Bytes>,
        pref:    LocalityPreference,
    ) -> crate::capability::WiredEmitOutcome {
        super::wiring::signal_wired_via_ctx(&self.ctx, filter, kind.into(), payload.into(), Some(pref))
    }

    // ── Demand ───────────────────────────────────────────────────────────────

    /// Snapshot count of declared demand vs. available providers for `filter`.
    pub fn demand(&self, filter: &CapFilter) -> DemandStatus {
        demand_snapshot(&self.ctx.kv_state, filter)
    }

    /// Push-based view of demand pressure for `filter`.
    pub fn watch_demand(&self, filter: CapFilter) -> watch::Receiver<DemandStatus> {
        let initial  = demand_snapshot(&self.ctx.kv_state, &filter);
        let (tx, rx) = watch::channel(initial);
        let needle_endswith = format!("/{}/{}",  filter.namespace, filter.name);
        let needle_contains = format!("/{}/{}/", filter.namespace, filter.name);
        let req_needle  = needle_endswith.clone();
        let cap_needle  = needle_endswith;
        let gcap_needle = needle_contains;
        let mut req_rx  = kv_subscribe_prefix_with_predicate(
            &self.ctx, Arc::<str>::from("req/"),  move |k| k.ends_with(&req_needle));
        let mut cap_rx  = kv_subscribe_prefix_with_predicate(
            &self.ctx, Arc::<str>::from("cap/"),  move |k| k.ends_with(&cap_needle));
        let mut gcap_rx = kv_subscribe_prefix_with_predicate(
            &self.ctx, Arc::<str>::from("gcap/"), move |k| k.contains(&gcap_needle));
        let mut shutdown_rx = self.ctx.shutdown_tx.subscribe();
        let kv_state = Arc::clone(&self.ctx.kv_state);
        self.ctx.spawn_task(async move {
            loop {
                tokio::select! { biased;
                    _ = await_shutdown(&mut shutdown_rx) => return,
                    r = req_rx.changed()  => { if r.is_err() { return; } }
                    r = cap_rx.changed()  => { if r.is_err() { return; } }
                    r = gcap_rx.changed() => { if r.is_err() { return; } }
                }
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

    // ── Opacity / load pheromone ─────────────────────────────────────────────

    /// Returns all peer load states newer than `max_age`, sorted highest-fill first.
    pub fn peer_load(&self, max_age: Duration) -> Vec<(Arc<str>, Arc<str>, LoadState)> {
        peer_load_ctx(&self.ctx, max_age)
    }

    /// Returns a `watch::Receiver` that fires whenever `load/{node_id}/{kind}` changes.
    #[must_use]
    pub fn peer_load_rx(
        &self,
        node_id: &NodeId,
        kind: &str,
    ) -> tokio::sync::watch::Receiver<Option<LoadState>> {
        use crate::signal::decode_load_state;
        let mut raw_rx = kv_subscribe(&self.ctx, format!("{}{}/{}", kv_ns::LOAD, node_id, kind));
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
    /// Returns an [`OpacityHandle`] whose drop stops the governor.
    pub fn manage_opacity(
        &self,
        kind:  impl Into<Arc<str>>,
        scope: crate::signal::SignalScope,
        hint:  OpacityHint,
    ) -> OpacityHandle {
        manage_opacity_ctx(&self.ctx, kind.into(), scope, hint)
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
        manage_opacity_gated_ctx(&self.ctx, kind.into(), scope, hint, Some(gate))
    }

    /// Returns the combined load signal for `kind`. `0.0` = transparent, `1.0` = fully saturated.
    pub fn opacity(&self, kind: &str) -> f32 {
        opacity_ctx(&self.ctx, kind)
    }

    /// True if this node's own pheromone trail for `kind` records `is_opaque`.
    pub fn is_opaque(&self, kind: &str) -> bool {
        kv_get(&self.ctx, &format!("{}{}/{}", kv_ns::LOAD, self.ctx.node_id, kind))
            .and_then(|b| crate::signal::decode_load_state(&b))
            .map(|s| s.is_opaque)
            .unwrap_or(false)
    }

    /// Effective load — max of durable pheromone fill ratio and live in-memory channel fill.
    pub fn effective_opacity(&self, kind: &str) -> f32 {
        effective_opacity_ctx(&self.ctx, kind)
    }

    /// True if `node`'s pheromone trail for `kind` records `is_opaque` within `max_age`.
    pub fn is_node_opaque(&self, node: &NodeId, kind: &str, max_age: Duration) -> bool {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now_ms_val = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        kv_get(&self.ctx, &format!("{}{}/{}", kv_ns::LOAD, node, kind))
            .and_then(|b| crate::signal::decode_load_state(&b))
            .map(|s| s.is_opaque && now_ms_val.saturating_sub(s.written_at_ms) <= max_age.as_millis() as u64)
            .unwrap_or(false)
    }

    // ── Leader suggestion ────────────────────────────────────────────────────

    /// Returns the group member with the lowest observed load for `kind`.
    pub fn suggest_leader(&self, group: &str, kind: &str, max_age: Duration) -> NodeId {
        super::helpers::suggest_leader_ctx(&self.ctx, group, kind, max_age)
    }
}
