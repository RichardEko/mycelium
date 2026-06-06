//! Emergent group formation (Phase 3g/3h) plus the Phase-7 group-requirement
//! opacity coupling.
//!
//! `define_capability_group` publishes a `CapabilityGroupDef` under
//! `cap-group/{group}`; the agent-wide `watch_capability_group_definitions`
//! background task subscribes to both `cap-group/` and `cap/{self}/` and
//! reconciles whether this node should self-join each emergent group based
//! on whether its own capabilities match the group's filter.
//!
//! When the node joins, the watcher spawns one consolidated
//! `run_group_membership_task` per group that internally:
//! - re-asserts `gcap/{group}/{ns}/{name}/{self}` for every `provides`
//!   capability on a `GCAP_REASSERT_INTERVAL` ticker — the group-level
//!   projection that inter-group wiring (see `super::wiring`) discovers
//!   and routes to;
//! - tracks `sys/load/{self}/group-req/{group}/{idx}` opacity for every
//!   `requires` filter, writing when unsatisfied and tombstoning when
//!   satisfied, composing with load-based opacity through the existing
//!   `is_self_opaque` scanner.
//!
//! Pre-C3 each provide + each requirement was its own spawned tokio task.
//! After C3 it is one task per group, regardless of provides/requires count.

use crate::capability::{CapEntry, Capability, CapFilter, CapabilityGroupDef, WiringStatus};
use crate::node_id::NodeId;
use crate::signal::{LoadState, encode_load_state};
use ahash::{AHashMap, AHashSet};
use bytes::Bytes;
use std::{sync::Arc, time::Duration};
use tokio::{sync::{oneshot, watch}, time};
use tracing::warn;

use super::TaskCtx;
use super::capability_ops::{
    await_shutdown, is_cap_locality_key, now_ms, scan_prefix_kv,
    subscribe_prefix_on_kv, WATCHER_DEBOUNCE_WINDOW,
};
use super::wiring::wiring_snapshot;

/// Interval at which group-level `gcap/` projections are re-asserted by each
/// cap-joined member. Conservative default chosen to amortise gossip cost;
/// the projection KV entry itself only expires on tombstone, so a slow
/// re-advertise still keeps late joiners in sync via anti-entropy.
const GCAP_REASSERT_INTERVAL: Duration = Duration::from_secs(60);

/// Per-group bookkeeping for the emergent-group watcher: every cap-joined
/// group has one consolidated `run_group_membership_task` (C3) that owns
/// both the `gcap/` reassertion loop and per-requirement opacity tracking.
/// Dropping the projection closes `_cancel`, which the membership task
/// observes and exits gracefully — tombstoning every `gcap/` and
/// `sys/load/{self}/group-req/{group}/{idx}` key it owns.
struct GroupProjection {
    /// Snapshot of the def's `provides` used to detect changes during
    /// reconciliation. When this differs from a new def, the projection
    /// is rebuilt (cancel old, spawn new).
    provides: Vec<Capability>,
    /// Snapshot of the def's `requires`, used the same way.
    requires: Vec<CapFilter>,
    /// Cancel sender for the consolidated membership task. Drop or
    /// `send(())` to make the task tombstone everything it wrote and
    /// return.
    #[allow(dead_code)]
    _cancel:  oneshot::Sender<()>,
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
        if is_cap_locality_key(&key) { continue; }
        let cap = CapEntry::decode(&bytes).map(|e| e.capability)
            .or_else(|| Capability::decode(&bytes));
        match cap {
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

    // Join newly-matching groups; spawn one consolidated membership task per
    // group (provides + requires handled internally).
    for group in &want_joined {
        let def      = defs.get(group).expect("present, scanned above");
        let provides = def.provides.clone();
        let requires = def.requires.clone();

        if !joined.contains_key(group.as_ref()) {
            emit_membership(ctx, own, group, false);
            let _cancel = spawn_group_membership(
                ctx, own, group, &provides, &requires, shutdown_rx,
            );
            joined.insert(group.clone(), GroupProjection {
                provides, requires, _cancel,
            });
            continue;
        }
        // Already joined — rebuild the membership task only when the def
        // actually changed.
        let (current_provides, current_requires) = {
            let current = joined.get(group).expect("just checked");
            (current.provides.clone(), current.requires.clone())
        };
        if current_provides == provides && current_requires == requires { continue; }
        // Drop old projection (cancels old membership task → tombstones
        // its keys) then spawn a fresh one with the new def.
        let _ = joined.remove(group.as_ref());
        let _cancel = spawn_group_membership(
            ctx, own, group, &provides, &requires, shutdown_rx,
        );
        joined.insert(group.clone(), GroupProjection {
            provides, requires, _cancel,
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

/// Spawns the single consolidated membership task for `group` and returns its
/// cancel sender. The task owns both the `gcap/` reassertion loop for every
/// `provides` capability AND opacity tracking for every `requires` filter.
/// Pre-C3 this was `provides.len() + requires.len()` separate tokio tasks
/// per group; after C3 it is one.
fn spawn_group_membership(
    ctx:         &Arc<TaskCtx>,
    own:         &NodeId,
    group:       &Arc<str>,
    provides:    &[Capability],
    requires:    &[CapFilter],
    shutdown_rx: &watch::Receiver<bool>,
) -> oneshot::Sender<()> {
    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
    tokio::spawn(run_group_membership_task(
        Arc::clone(ctx),
        own.clone(),
        Arc::clone(group),
        provides.to_vec(),
        requires.to_vec(),
        cancel_rx,
        shutdown_rx.clone(),
    ));
    cancel_tx
}

/// Consolidated per-group membership task. Owns:
/// - the reassertion ticker that re-writes every `gcap/{group}/{ns}/{name}/{self}`
///   on `GCAP_REASSERT_INTERVAL`;
/// - the opacity tracker that writes/clears `sys/load/{self}/group-req/{group}/{idx}`
///   for every requirement filter as wiring status changes.
///
/// Wakes on cancel, shutdown, the reassert ticker, or any `cap/`/`gcap/`
/// change (debounced by [`WATCHER_DEBOUNCE_WINDOW`]). On exit, tombstones
/// every `gcap/` and any opacity key it had written.
async fn run_group_membership_task(
    ctx:             Arc<TaskCtx>,
    own:             NodeId,
    group:           Arc<str>,
    provides:        Vec<Capability>,
    requires:        Vec<CapFilter>,
    mut cancel_rx:   oneshot::Receiver<()>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    use crate::framing::{
        dispatch_gossip_send, dispatch_gossip_try_send, ForwardHint, WireMessage,
        make_gossip_update,
    };
    use crate::store::apply_and_notify;

    let gcap_keys: Vec<Arc<str>> = provides.iter().map(|c| {
        Arc::from(format!("gcap/{}/{}/{}/{}", group, c.namespace, c.name, own).as_str())
    }).collect();
    let opacity_keys: Vec<Arc<str>> = (0..requires.len()).map(|i| {
        Arc::from(format!("sys/load/{}/group-req/{}/{}", own, group, i).as_str())
    }).collect();
    let mut opaque_written = vec![false; requires.len()];

    let write_provide = |key: &Arc<str>, cap: &Capability| {
        let payload = CapEntry {
            capability:          cap.clone(),
            refresh_interval_ms: GCAP_REASSERT_INTERVAL.as_millis() as u64,
        }.encode();
        let upd = make_gossip_update(
            &ctx.node_id, ctx.default_ttl, key.clone(), payload, false, &ctx.hlc,
        );
        apply_and_notify(&ctx.kv_state, &upd);
        dispatch_gossip_try_send(
            &ctx.gossip_txs, WireMessage::Data(upd),
            ctx.node_id.id_hash(), ForwardHint::All, &ctx.kv_state.dropped_frames,
        );
    };
    let write_opaque = |key: &Arc<str>| {
        let payload = encode_load_state(&LoadState {
            fill_ratio:    1.0,
            is_opaque:     true,
            written_at_ms: now_ms(),
        });
        let upd = make_gossip_update(
            &ctx.node_id, ctx.default_ttl, key.clone(), payload, false, &ctx.hlc,
        );
        apply_and_notify(&ctx.kv_state, &upd);
        dispatch_gossip_try_send(
            &ctx.gossip_txs, WireMessage::Data(upd),
            ctx.node_id.id_hash(), ForwardHint::All, &ctx.kv_state.dropped_frames,
        );
    };
    let clear_opaque = |key: &Arc<str>| {
        let upd = make_gossip_update(
            &ctx.node_id, ctx.default_ttl, key.clone(), Bytes::new(), true, &ctx.hlc,
        );
        apply_and_notify(&ctx.kv_state, &upd);
        dispatch_gossip_try_send(
            &ctx.gossip_txs, WireMessage::Data(upd),
            ctx.node_id.id_hash(), ForwardHint::All, &ctx.kv_state.dropped_frames,
        );
    };
    let is_satisfied = |filter: &CapFilter| -> bool {
        !matches!(wiring_snapshot(&ctx.kv_state, filter), WiringStatus::Unwired { .. })
    };

    // Initial writes: project all provides and evaluate every requirement.
    for (key, cap) in gcap_keys.iter().zip(&provides) {
        write_provide(key, cap);
    }
    for (i, filter) in requires.iter().enumerate() {
        if !is_satisfied(filter) {
            write_opaque(&opacity_keys[i]);
            opaque_written[i] = true;
        }
    }

    let mut cap_rx  = subscribe_prefix_on_kv(&ctx.kv_state, Arc::<str>::from("cap/"));
    let mut gcap_rx = subscribe_prefix_on_kv(&ctx.kv_state, Arc::<str>::from("gcap/"));

    let mut ticker = time::interval(GCAP_REASSERT_INTERVAL);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    ticker.tick().await; // Consume the immediate first tick — we just wrote.

    'main: loop {
        tokio::select! { biased;
            _ = &mut cancel_rx                   => break 'main,
            _ = await_shutdown(&mut shutdown_rx) => break 'main,
            _ = ticker.tick() => {
                for (key, cap) in gcap_keys.iter().zip(&provides) {
                    write_provide(key, cap);
                }
                continue 'main;
            }
            r = cap_rx.changed()  => { if r.is_err() { break 'main; } }
            r = gcap_rx.changed() => { if r.is_err() { break 'main; } }
        }
        // Coalesce: drain further cap/gcap fires for the debounce window
        // before recomputing every requirement's satisfaction once.
        let deadline = time::Instant::now() + WATCHER_DEBOUNCE_WINDOW;
        loop {
            tokio::select! { biased;
                _ = time::sleep_until(deadline) => break,
                r = cap_rx.changed()  => { if r.is_err() { break 'main; } }
                r = gcap_rx.changed() => { if r.is_err() { break 'main; } }
            }
        }
        for (i, filter) in requires.iter().enumerate() {
            let satisfied = is_satisfied(filter);
            match (opaque_written[i], satisfied) {
                (false, false) => { write_opaque(&opacity_keys[i]); opaque_written[i] = true; }
                (true,  true)  => { clear_opaque(&opacity_keys[i]); opaque_written[i] = false; }
                _ => {}
            }
        }
    }

    // Tombstone every gcap projection (use ordered send so the final
    // tombstones actually leave the agent before the task is dropped),
    // and clear any still-opaque requirement entries.
    for key in &gcap_keys {
        let upd = make_gossip_update(
            &ctx.node_id, ctx.default_ttl, key.clone(), Bytes::new(), true, &ctx.hlc,
        );
        apply_and_notify(&ctx.kv_state, &upd);
        dispatch_gossip_send(
            &ctx.gossip_txs, WireMessage::Data(upd),
            ctx.node_id.id_hash(), ForwardHint::All,
        ).await;
    }
    for (i, key) in opacity_keys.iter().enumerate() {
        if opaque_written[i] {
            clear_opaque(key);
        }
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
