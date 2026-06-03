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
    CallerContext, CapEntry, Capability, CapFilter, CapabilityEvent,
    CapabilityHandle, ReqEntry,
    RequirementHandle, RequirementStatus,
};
use crate::node_id::NodeId;
use crate::signal::{LoadState, encode_load_state};
use ahash::AHashMap;
use bytes::Bytes;
use std::{
    sync::{Arc, atomic::{AtomicBool, Ordering}},
    time::Duration,
};
use tokio::{sync::{mpsc, oneshot, watch, Notify}, time};
use tracing::warn;

/// Shared registry for the consolidated opacity watcher spawned by
/// `declare_requirement`. Exactly one background task reads from this registry;
/// `declare_requirement` pushes entries and `notify`s the task, avoiding the
/// N-tasks-per-N-requirements scalability issue (C2 fix).
pub(crate) struct FilterOpacityRegistry {
    pub(crate) entries: std::sync::Mutex<Vec<RegEntry>>,
    pub(crate) notify:  Arc<Notify>,
    spawned:            AtomicBool,
}

pub(crate) struct RegEntry {
    pub(crate) opacity_key: Arc<str>,
    pub(crate) filter:      Arc<CapFilter>,
    pub(crate) cancelled:   Arc<AtomicBool>,
}

impl FilterOpacityRegistry {
    pub(crate) fn new() -> Self {
        Self {
            entries: std::sync::Mutex::new(Vec::new()),
            notify:  Arc::new(Notify::new()),
            spawned: AtomicBool::new(false),
        }
    }
}

use super::{GossipAgent, TaskCtx};
use super::kv::run_kv_persist_task;
use super::wiring::rank_node_matches;

/// Sendable shutdown-await helper: yields once `shutdown_rx`'s value is `true`
/// (or its sender drops). Unlike `watch::Receiver::wait_for`, this returns
/// only `()` so no `Ref<'_, bool>` is held across the .await, which keeps
/// the surrounding `tokio::select!` future `Send`.
pub(super) async fn await_shutdown(rx: &mut watch::Receiver<bool>) {
    while !*rx.borrow() {
        if rx.changed().await.is_err() { return; }
    }
}

/// Idle window after the first prefix-watcher fire before a watcher runs its
/// reaction. Anti-entropy sync, partition heal, bulk-config push, and any
/// other burst of writes within this window are coalesced into one reaction
/// instead of N. 50 ms is a deliberate trade-off: small enough that single
/// writes still feel snappy (interactive operations stay sub-100 ms
/// end-to-end), large enough to cover typical kernel-level batching and
/// tokio scheduler granularity.
///
/// Used by every watcher that follows the "wait → reconcile" pattern:
/// `watch_capabilities`, `watch_requirement`, `watch_wiring`, `watch_demand`,
/// `watch_capability_group_definitions`, and the opacity watcher.
pub(super) const WATCHER_DEBOUNCE_WINDOW: Duration = Duration::from_millis(50);

/// Like `parse_cap_key` but emits a `warn!` on the unhappy path. Use at scan
/// sites where a malformed key is genuinely surprising (we only scan prefixes
/// whose entries are well-formed by convention).
pub(super) fn parse_cap_key_or_warn(prefix: &str, key: &str) -> Option<(NodeId, Arc<str>, Arc<str>)> {
    let parsed = parse_cap_key(prefix, key);
    if parsed.is_none() {
        warn!(prefix = %prefix, key = %key, "malformed key under capability/requirement prefix");
    }
    parsed
}

/// Returns `true` when `key` lives under the `cap/` prefix but is **not** a
/// bincode-encoded `Capability` — currently the only case is
/// `cap/{node}/locality/self`, which carries a `LocalityPath::encode` payload.
///
/// Every scanner of the `cap/` prefix must skip these keys before calling
/// `Capability::decode`. Without the skip, a multi-segment LocalityPath byte
/// stream can pattern-match enough of bincode's Capability layout that
/// `decode` reads a giant Vec length prefix and allocates terabytes of RAM
/// before failing.
pub(super) fn is_cap_locality_key(key: &str) -> bool {
    key.ends_with("/locality/self")
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
        let interval_ms = interval.as_millis() as u64;
        let entry = Arc::new(CapEntry { capability, refresh_interval_ms: interval_ms });
        let payload_fn: super::kv::PersistPayloadFn = {
            let e = Arc::clone(&entry);
            Arc::new(move || e.encode())
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
        self.resolve_for_caller(filter, &CallerContext::unrestricted())
    }

    /// Like `resolve`, but also enforces `Capability::authorized_callers`.
    /// A capability with a non-empty `authorized_callers` list is returned
    /// only when `ctx.caller_id` is in that list. Use this for language-bridge
    /// and SkillRunner tool-discovery to prevent token-bloat and confused-deputy
    /// issues — see the Layer 4 security primitive design in ROADMAP.md.
    pub fn resolve_for_caller(
        &self,
        filter: &CapFilter,
        ctx:    &CallerContext,
    ) -> Vec<(NodeId, Capability)> {
        let now_ms = age_now_ms();
        let mut out = Vec::new();
        for (key, bytes, hlc_ts) in scan_prefix_kv_with_ts(&self.kv_state, "cap/") {
            if is_cap_locality_key(&key) { continue; }
            let Some((node_id, _ns, _name)) = parse_cap_key_or_warn("cap/", &key) else { continue };
            let Some(entry) = CapEntry::decode(&bytes)
                .or_else(|| Capability::decode(&bytes).map(|cap| CapEntry { capability: cap, refresh_interval_ms: 60_000 }))
            else {
                warn!(key = %key, "malformed Capability — peer sent bytes that did not decode");
                continue;
            };
            if !entry.is_fresh(hlc_ts, now_ms) { continue; }
            if let Some(max_age) = filter.max_age {
                if is_stale(hlc_ts, now_ms, max_age) { continue; }
            }
            let cap = entry.capability;
            if filter.matches(&cap) && ctx.can_see(&cap) {
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
        // C1: narrow the cap/ prefix watcher to (namespace, name) of this
        // filter. cap/{node}/{ns}/{name} — predicate fires only when the
        // changed key ends in /{ns}/{name}. False positives are still
        // re-screened by the post-debounce reconcile.
        let needle = format!("/{}/{}", filter.namespace, filter.name);
        let mut prefix_rx = self.subscribe_prefix_with_predicate(
            Arc::<str>::from("cap/"),
            move |k| k.ends_with(&needle),
        );
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
                        // Coalesce burst writes within WATCHER_DEBOUNCE_WINDOW
                        // into a single reconcile pass.
                        let deadline = time::Instant::now() + WATCHER_DEBOUNCE_WINDOW;
                        loop {
                            tokio::select! { biased;
                                _ = time::sleep_until(deadline) => break,
                                r = prefix_rx.changed() => { if r.is_err() { return; } }
                            }
                        }
                        // Re-scan and diff against `known`.
                        let now_ms = age_now_ms();
                        let mut current: AHashMap<(NodeId, Arc<str>, Arc<str>), Capability> = AHashMap::new();
                        for (key, bytes, hlc_ts) in scan_prefix_kv_with_ts(&kv_state, "cap/") {
                            if is_cap_locality_key(&key) { continue; }
                            let Some((node_id, ns, name)) = parse_cap_key_or_warn("cap/", &key) else { continue };
                            let Some(entry) = CapEntry::decode(&bytes)
                                .or_else(|| Capability::decode(&bytes).map(|cap| CapEntry { capability: cap, refresh_interval_ms: 60_000 }))
                            else {
                                warn!(key = %key, "malformed Capability — peer sent bytes that did not decode");
                                continue;
                            };
                            if !entry.is_fresh(hlc_ts, now_ms) { continue; }
                            let cap = entry.capability;
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
        let shutdown_rx             = self.shutdown_tx.subscribe();
        let ctx: Arc<TaskCtx>       = Arc::clone(&self.task_ctx);

        let kv_key: Arc<str> = Arc::from(format!(
            "req/{}/{}/{}", ctx.node_id, filter.namespace, filter.name,
        ).as_str());
        let opacity_key: Arc<str> = Arc::from(format!(
            "sys/load/{}/req/{}/{}", ctx.node_id, filter.namespace, filter.name,
        ).as_str());
        let interval_ms = interval.as_millis() as u64;
        let filter_arc = Arc::new(filter);
        let payload_fn: super::kv::PersistPayloadFn = {
            let e = Arc::new(ReqEntry {
                filter:              (*filter_arc).clone(),
                refresh_interval_ms: interval_ms,
            });
            Arc::new(move || e.encode())
        };
        self.spawn_task(run_kv_persist_task(
            Arc::clone(&ctx), cancel_rx, shutdown_rx, kv_key, interval, payload_fn, None,
        ));

        // Register with the consolidated opacity watcher (C2 fix: one task for
        // all requirements instead of N tasks subscribing to cap/ independently).
        let cancelled = Arc::new(AtomicBool::new(false));
        let registry  = Arc::clone(&ctx.filter_opacity_registry);
        registry.entries.lock().unwrap().push(RegEntry {
            opacity_key,
            filter:    Arc::clone(&filter_arc),
            cancelled: Arc::clone(&cancelled),
        });
        registry.notify.notify_one();

        // Spawn the consolidated task lazily on the first declare_requirement call.
        if !registry.spawned.swap(true, Ordering::AcqRel) {
            let reg_arc = Arc::clone(&registry);
            let ctx2    = Arc::clone(&ctx);
            let sd_rx   = self.shutdown_tx.subscribe();
            self.spawn_task(run_consolidated_opacity_watcher(ctx2, sd_rx, reg_arc));
        }

        let opacity_drop = crate::capability::OpacityDropGuard {
            cancelled,
            notify: Arc::clone(&registry.notify),
        };
        RequirementHandle { _retract: cancel_tx, _opacity_drop: opacity_drop }
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
        // C1: narrow the cap/ prefix watcher to this filter's (ns, name).
        let needle = format!("/{}/{}", filter.namespace, filter.name);
        let mut prefix_rx = self.subscribe_prefix_with_predicate(
            Arc::<str>::from("cap/"),
            move |k| k.ends_with(&needle),
        );
        let kv_state = Arc::clone(&self.kv_state);
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        self.spawn_task(async move {
            loop {
                tokio::select! { biased;
                    _ = await_shutdown(&mut shutdown_rx) => return,
                    changed = prefix_rx.changed() => {
                        if changed.is_err() { return; }
                        // Coalesce burst writes within WATCHER_DEBOUNCE_WINDOW
                        // into a single status recompute.
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

}

// ── Free helpers (used by spawned tasks) ─────────────────────────────────────

/// Like `GossipAgent::scan_prefix`, but for a `KvState` reference held by a
/// spawned task that doesn't carry a `GossipAgent` handle.
pub(super) fn scan_prefix_kv(kv_state: &crate::store::KvState, prefix: &str) -> Vec<(Arc<str>, Bytes)> {
    scan_prefix_kv_with_ts(kv_state, prefix)
        .into_iter()
        .map(|(k, v, _ts)| (k, v))
        .collect()
}

/// Like `scan_prefix_kv`, but also returns each entry's HLC timestamp so
/// callers can apply `CapFilter::max_age` liveness checks.
pub(super) fn scan_prefix_kv_with_ts(kv_state: &crate::store::KvState, prefix: &str) -> Vec<(Arc<str>, Bytes, u64)> {
    let seg = prefix.find('/').map_or(prefix, |i| &prefix[..i]);
    let store_guard = kv_state.store.pin();
    let idx_guard   = kv_state.prefix_index.pin();
    if let Some(bucket) = idx_guard.get(seg) {
        bucket.pin().iter()
            .filter_map(|(key, _)| {
                if !key.starts_with(prefix) { return None; }
                let entry = store_guard.get(key.as_ref())?;
                let data  = entry.data.clone()?;
                Some((key.clone(), data, entry.timestamp))
            })
            .collect()
    } else {
        store_guard.iter()
            .filter(|(k, v)| v.data.is_some() && k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.data.clone().unwrap(), v.timestamp))
            .collect()
    }
}

/// Scans `cap_ns_index` for entries matching `"{seg}/{ns}/{name}"` and returns
/// the corresponding store entries. O(k) where k is the number of nodes that
/// advertise this specific (seg, ns, name) combination — avoids a full bucket
/// scan when only one (ns, name) pair is queried.
pub(super) fn scan_cap_by_ns_name(
    kv_state: &crate::store::KvState,
    seg: &str,
    ns: &str,
    name: &str,
) -> Vec<(Arc<str>, Bytes, u64)> {
    let identity = format!("{seg}/{ns}/{name}");
    let store_guard = kv_state.store.pin();
    let idx_guard   = kv_state.cap_ns_index.pin();
    let Some(bucket) = idx_guard.get(identity.as_str()) else { return Vec::new() };
    let result: Vec<_> = bucket.pin().iter()
        .filter_map(|(key, _)| {
            let entry = store_guard.get(key.as_ref())?;
            let data  = entry.data.clone()?;
            Some((key.clone(), data, entry.timestamp))
        })
        .collect();
    result
}

/// Returns current wall-clock milliseconds for max_age comparisons.
#[inline]
fn age_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// True if the capability entry is older than `max_age` based on its HLC timestamp.
#[inline]
fn is_stale(hlc_ts: u64, now_ms: u64, max_age: std::time::Duration) -> bool {
    let entry_physical_ms = crate::hlc::physical_ms(hlc_ts);
    now_ms.saturating_sub(entry_physical_ms) > max_age.as_millis() as u64
}

/// Snapshot resolve from a `KvState` (used inside spawned tasks).
pub(super) fn resolve_filter_against_kv(
    kv_state: &crate::store::KvState,
    filter:   &CapFilter,
) -> Vec<(NodeId, Capability)> {
    let now_ms = age_now_ms();
    let mut out = Vec::new();
    for (key, bytes, hlc_ts) in scan_prefix_kv_with_ts(kv_state, "cap/") {
        if is_cap_locality_key(&key) { continue; }
        let Some((node_id, _ns, _name)) = parse_cap_key_or_warn("cap/", &key) else { continue };
        let Some(entry) = CapEntry::decode(&bytes)
            .or_else(|| Capability::decode(&bytes).map(|cap| CapEntry { capability: cap, refresh_interval_ms: 60_000 }))
        else {
            warn!(key = %key, "malformed Capability — peer sent bytes that did not decode");
            continue;
        };
        if !entry.is_fresh(hlc_ts, now_ms) { continue; }
        if let Some(max_age) = filter.max_age {
            if is_stale(hlc_ts, now_ms, max_age) { continue; }
        }
        let cap = entry.capability;
        if filter.matches(&cap) {
            out.push((node_id, cap));
        }
    }
    out
}

// (wiring_snapshot, locality helpers, and gcap key parsing live in
// `super::wiring`.) Below: helpers that remain co-located with their
// callers in this module.


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

/// Free-function flavour of `GossipAgent::subscribe_prefix` for callers that
/// only hold a `&KvState`. Lazy-creates the prefix watcher entry if absent.
pub(crate) fn subscribe_prefix_on_kv(
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

/// Background task for the Phase-3 `declare_requirement` opacity coupling.
/// Writes `LoadState { is_opaque: true }` to `opacity_key` in the local KV
/// store and fans it out over gossip.
fn write_opacity_key(ctx: &TaskCtx, opacity_key: &Arc<str>) {
    use crate::framing::{dispatch_gossip_try_send, ForwardHint, WireMessage, make_gossip_update};
    use crate::store::apply_and_notify;
    let payload = encode_load_state(&LoadState {
        fill_ratio: 1.0, is_opaque: true, written_at_ms: now_ms(),
    });
    let upd = make_gossip_update(
        &ctx.node_id, ctx.default_ttl, opacity_key.clone(), payload, false, &ctx.hlc,
    );
    apply_and_notify(&ctx.kv_state, &upd);
    dispatch_gossip_try_send(
        &ctx.gossip_txs, WireMessage::Data(upd),
        ctx.node_id.id_hash(), ForwardHint::All, &ctx.kv_state.dropped_frames,
    );
}

/// Tombstones `opacity_key` in the local KV store and fans the tombstone over
/// gossip. Used by the consolidated watcher when a requirement becomes
/// satisfied or is retracted.
fn clear_opacity_key(ctx: &TaskCtx, opacity_key: &Arc<str>) {
    use crate::framing::{dispatch_gossip_try_send, ForwardHint, WireMessage, make_gossip_update};
    use crate::store::apply_and_notify;
    let upd = make_gossip_update(
        &ctx.node_id, ctx.default_ttl, opacity_key.clone(), Bytes::new(), true, &ctx.hlc,
    );
    apply_and_notify(&ctx.kv_state, &upd);
    dispatch_gossip_try_send(
        &ctx.gossip_txs, WireMessage::Data(upd),
        ctx.node_id.id_hash(), ForwardHint::All, &ctx.kv_state.dropped_frames,
    );
}

/// Single background task that manages opacity for all declared requirements
/// (C2 fix: replaces N per-requirement `run_filter_opacity_watcher` tasks with
/// one task and one `cap/` subscription).
///
/// Wakes on any `cap/` change or on a `registry.notify` signal (fired when a
/// requirement is added or retracted). On each wake, merges new entries from
/// the registry into its local tracking list, then scans all live entries and
/// updates opacity accordingly. Cancelled entries are cleaned up immediately.
///
/// Phase-7 group-req opacity is handled inline by C3's
/// `run_group_membership_task`, which co-locates it with the `gcap/`
/// reassertion loop.
async fn run_consolidated_opacity_watcher(
    ctx:             Arc<TaskCtx>,
    mut shutdown_rx: watch::Receiver<bool>,
    registry:        Arc<FilterOpacityRegistry>,
) {
    struct LocalEntry {
        opacity_key:    Arc<str>,
        filter:         Arc<CapFilter>,
        cancelled:      Arc<AtomicBool>,
        opaque_written: bool,
    }

    let mut local: Vec<LocalEntry> = Vec::new();
    let mut cap_rx = subscribe_prefix_on_kv(&ctx.kv_state, Arc::<str>::from("cap/"));

    // Initial pass: pick up any entries registered before the task started.
    {
        let guard = registry.entries.lock().unwrap();
        for reg in guard.iter() {
            let satisfied = !resolve_filter_against_kv(&ctx.kv_state, &reg.filter).is_empty();
            let opaque_written = if !satisfied {
                write_opacity_key(&ctx, &reg.opacity_key);
                true
            } else {
                false
            };
            local.push(LocalEntry {
                opacity_key:    reg.opacity_key.clone(),
                filter:         Arc::clone(&reg.filter),
                cancelled:      Arc::clone(&reg.cancelled),
                opaque_written,
            });
        }
    }

    loop {
        tokio::select! { biased;
            _ = await_shutdown(&mut shutdown_rx)  => break,
            _ = registry.notify.notified()        => {},
            r = cap_rx.changed() => { if r.is_err() { break; } }
        }

        // Merge entries added since last wake.
        {
            let guard = registry.entries.lock().unwrap();
            for reg in guard.iter() {
                if local.iter().any(|e| e.opacity_key == reg.opacity_key) {
                    continue;
                }
                let satisfied = !resolve_filter_against_kv(&ctx.kv_state, &reg.filter).is_empty();
                let opaque_written = if !satisfied {
                    write_opacity_key(&ctx, &reg.opacity_key);
                    true
                } else {
                    false
                };
                local.push(LocalEntry {
                    opacity_key:    reg.opacity_key.clone(),
                    filter:         Arc::clone(&reg.filter),
                    cancelled:      Arc::clone(&reg.cancelled),
                    opaque_written,
                });
            }
        }

        // Scan all live entries; remove cancelled ones after clearing their key.
        let mut i = 0;
        while i < local.len() {
            if local[i].cancelled.load(Ordering::Acquire) {
                if local[i].opaque_written {
                    clear_opacity_key(&ctx, &local[i].opacity_key);
                }
                registry.entries.lock().unwrap()
                    .retain(|e| e.opacity_key != local[i].opacity_key);
                local.swap_remove(i);
                continue;
            }
            let satisfied = !resolve_filter_against_kv(&ctx.kv_state, &local[i].filter).is_empty();
            match (local[i].opaque_written, satisfied) {
                (false, false) => { write_opacity_key(&ctx, &local[i].opacity_key); local[i].opaque_written = true; }
                (true,  true)  => { clear_opacity_key(&ctx, &local[i].opacity_key); local[i].opaque_written = false; }
                _ => {}
            }
            i += 1;
        }
    }

    // Shutdown: clear all remaining opacity keys so KV is left clean.
    for entry in &local {
        if entry.opaque_written {
            clear_opacity_key(&ctx, &entry.opacity_key);
        }
    }
}

pub(super) fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
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

}
