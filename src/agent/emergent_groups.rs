//! Emergent group formation (Phase 3g/3h) plus the Phase-7 group-requirement
//! opacity coupling.
//!
//! `define_capability_group` publishes a `CapabilityGroupDef` under
//! `cap-group/{group}`; the agent-wide `watch_capability_group_definitions`
//! background task subscribes to both `cap-group/` and `cap/{self}/` and
//! reconciles whether this node should self-join each emergent group based
//! on whether its own capabilities match the group's filter.
//!
//! When the node joins, the watcher additionally spawns:
//! - one `run_kv_persist_task` per `provides` capability under
//!   `gcap/{group}/{ns}/{name}/{self}` — the group-level projection that
//!   inter-group wiring (see `super::wiring`) discovers and routes to;
//! - one `run_filter_opacity_watcher(OpacityEvaluator::GroupReq)` per
//!   `requires` filter that writes `sys/load/{self}/group-req/{group}/{idx}`
//!   while the requirement is unsatisfied, composing with load-based
//!   opacity through the existing `is_self_opaque` scanner.

use crate::capability::{Capability, CapFilter, CapabilityGroupDef, CapabilityGroupHandle};
use crate::node_id::NodeId;
use ahash::{AHashMap, AHashSet};
use bytes::Bytes;
use std::{sync::Arc, time::Duration};
use tokio::{sync::{oneshot, watch}, time};
use tracing::warn;

use super::{GossipAgent, TaskCtx};
use super::capability_ops::{
    await_shutdown, scan_prefix_kv,
    OpacityEvaluator, run_filter_opacity_watcher,
    WATCHER_DEBOUNCE_WINDOW,
};
use super::kv::{run_kv_persist_task, PersistPayloadFn};

impl GossipAgent {
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
        let payload_fn: PersistPayloadFn = {
            let def = Arc::clone(&def_arc);
            Arc::new(move || def.encode())
        };
        self.spawn_task(run_kv_persist_task(
            ctx, cancel_rx, shutdown_rx, kv_key, interval, payload_fn, None,
        ));
        CapabilityGroupHandle { _retract: cancel_tx, group }
    }
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

    'outer: loop {
        tokio::select! { biased;
            _ = await_shutdown(&mut shutdown_rx) => break 'outer,
            r = def_rx.changed() => { if r.is_err() { break 'outer; } }
            r = own_rx.changed() => { if r.is_err() { break 'outer; } }
        }
        // Coalesce burst writes (anti-entropy sync, bulk cap-group push)
        // into a single reconcile pass. See WATCHER_DEBOUNCE_WINDOW.
        let deadline = time::Instant::now() + WATCHER_DEBOUNCE_WINDOW;
        loop {
            tokio::select! { biased;
                _ = await_shutdown(&mut shutdown_rx) => break 'outer,
                _ = time::sleep_until(deadline) => break,
                r = def_rx.changed() => { if r.is_err() { break 'outer; } }
                r = own_rx.changed() => { if r.is_err() { break 'outer; } }
            }
        }
        reconcile_emergent_groups(&ctx, &own_node_id, &shutdown_rx, &mut joined);
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
        let filter_arc = Arc::new(filter.clone());
        tokio::spawn(run_filter_opacity_watcher(
            Arc::clone(ctx),
            cancel_rx,
            shutdown_rx.clone(),
            opacity_key,
            filter_arc,
            OpacityEvaluator::GroupReq,
        ));
        handles.push(cancel_tx);
    }
    handles
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
        let payload_fn: PersistPayloadFn = {
            let cap = Arc::clone(&cap_arc);
            Arc::new(move || cap.encode())
        };
        tokio::spawn(run_kv_persist_task(
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
