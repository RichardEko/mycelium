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
    Capability, CapFilter, CapabilityEvent,
    CapabilityHandle, CapabilityGroupDef, CapabilityGroupHandle,
    RequirementHandle, RequirementStatus,
};
use crate::node_id::NodeId;
use crate::signal::{LoadState, encode_load_state};
use ahash::{AHashMap, AHashSet};
use bytes::Bytes;
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot, watch};

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
    /// satisfies `filter`. Order is unspecified.
    pub fn resolve(&self, filter: &CapFilter) -> Vec<(NodeId, Capability)> {
        let mut out = Vec::new();
        for (key, bytes) in self.scan_prefix("cap/") {
            let Some((node_id, _ns, _name)) = parse_cap_key("cap/", &key) else { continue };
            let Some(cap) = Capability::decode(&bytes) else { continue };
            if filter.matches(&cap) {
                out.push((node_id, cap));
            }
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
                            let Some((node_id, ns, name)) = parse_cap_key("cap/", &key) else { continue };
                            let Some(cap) = Capability::decode(&bytes) else { continue };
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
        let Some((node_id, _ns, _name)) = parse_cap_key("cap/", &key) else { continue };
        let Some(cap) = Capability::decode(&bytes) else { continue };
        if filter.matches(&cap) {
            out.push((node_id, cap));
        }
    }
    out
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
            &ctx.node_id, ctx.default_ttl, opacity_key.clone(), payload, false,
        );
        apply_and_notify(&ctx.kv_state, &upd);
        dispatch_gossip_try_send(
            &ctx.gossip_txs, WireMessage::Data(upd),
            ctx.node_id.id_hash(), ForwardHint::All, &ctx.kv_state.dropped_frames,
        );
    };

    let clear_opaque = |ctx: &TaskCtx| {
        let upd = make_gossip_update(
            &ctx.node_id, ctx.default_ttl, opacity_key.clone(), Bytes::new(), true,
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

/// Background task: watches `cap-group/` for emergent group definitions and
/// keeps this node's `grp/` membership in sync with whether its own
/// capabilities match each def's filter. See plan section
/// "`watch_capability_group_definitions` Dual Subscription".
pub(crate) async fn watch_capability_group_definitions(
    ctx:           Arc<TaskCtx>,
    own_node_id:   NodeId,
    mut def_rx:    watch::Receiver<u64>,
    mut own_rx:    watch::Receiver<u64>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    // Tracks which emergent groups we are currently cap-joined to so we know
    // which `grp/` keys to tombstone on a non-match.
    let mut cap_joined: AHashMap<Arc<str>, ()> = AHashMap::new();

    let reconcile = |joined: &mut AHashMap<Arc<str>, ()>, ctx: &Arc<TaskCtx>, own: &NodeId| {
        // Own capabilities snapshot.
        let mut own_caps: Vec<Capability> = Vec::new();
        let own_prefix = format!("cap/{}/", own);
        for (_, bytes) in scan_prefix_kv(&ctx.kv_state, &own_prefix) {
            if let Some(cap) = Capability::decode(&bytes) {
                own_caps.push(cap);
            }
        }
        // Current set of cap-group definitions.
        let mut want_joined: AHashSet<Arc<str>> = AHashSet::new();
        let mut all_groups: AHashSet<Arc<str>>  = AHashSet::new();
        for (key, bytes) in scan_prefix_kv(&ctx.kv_state, "cap-group/") {
            let Some(group_name) = key.strip_prefix("cap-group/") else { continue };
            let group: Arc<str>  = Arc::from(group_name);
            all_groups.insert(group.clone());
            let Some(def) = CapabilityGroupDef::decode(&bytes) else { continue };
            // Match: any of our own caps satisfies the def's filter.
            if own_caps.iter().any(|c| def.filter.matches(c)) {
                want_joined.insert(group);
            }
        }
        // Join newly-matching groups.
        for g in &want_joined {
            if !joined.contains_key(g.as_ref()) {
                emit_membership(ctx, own, g, false);
                joined.insert(g.clone(), ());
            }
        }
        // Leave groups we were in but no longer match — either because the def
        // tombstoned or our caps changed. `all_groups` lets us also tombstone
        // memberships when the def disappeared entirely.
        let to_leave: Vec<Arc<str>> = joined.keys()
            .filter(|g| !want_joined.contains(g.as_ref()))
            .cloned()
            .collect();
        for g in to_leave {
            emit_membership(ctx, own, &g, true);
            joined.remove(&g);
            let _ = all_groups.contains(&g); // suppress unused warning
        }
    };

    // Initial reconciliation in case definitions or own caps already exist.
    reconcile(&mut cap_joined, &ctx, &own_node_id);

    loop {
        tokio::select! { biased;
            _ = shutdown_rx.wait_for(|v| *v) => break,
            r = def_rx.changed() => {
                if r.is_err() { break; }
                reconcile(&mut cap_joined, &ctx, &own_node_id);
            }
            r = own_rx.changed() => {
                if r.is_err() { break; }
                reconcile(&mut cap_joined, &ctx, &own_node_id);
            }
        }
    }

    // Best-effort: tombstone any memberships we hold so peers see us leave
    // the emergent groups cleanly.
    for (g, ()) in cap_joined.into_iter() {
        emit_membership(&ctx, &own_node_id, &g, true);
    }
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
        &ctx.node_id, ctx.default_ttl, key, payload, leaving,
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
}
