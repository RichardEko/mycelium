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

use crate::capability::{CapEntry, Capability, CapFilter};
use crate::node_id::NodeId;
use crate::signal::{LoadState, encode_load_state};
use bytes::Bytes;
use std::{
    sync::{Arc, atomic::{AtomicBool, Ordering}},
    time::Duration,
};
use tokio::sync::{watch, Notify};
use tracing::warn;

/// Shared registry for the consolidated opacity watcher spawned by
/// `declare_requirement`. Exactly one background task reads from this registry;
/// `declare_requirement` pushes entries and `notify`s the task, avoiding the
/// N-tasks-per-N-requirements scalability issue (C2 fix).
pub(crate) struct FilterOpacityRegistry {
    pub(crate) entries: std::sync::Mutex<Vec<RegEntry>>,
    pub(crate) notify:  Arc<Notify>,
    pub(super) spawned: AtomicBool,
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

use super::TaskCtx;

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
                Some((Arc::clone(key), data, entry.timestamp))
            })
            .collect()
    } else {
        store_guard.iter()
            .filter(|(k, v)| v.data.is_some() && k.starts_with(prefix))
            .map(|(k, v)| (Arc::clone(k), v.data.clone().unwrap(), v.timestamp))
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
            Some((Arc::clone(key), data, entry.timestamp))
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
        if let Some(max_age) = filter.max_age
            && is_stale(hlc_ts, now_ms, max_age) { continue; }
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
pub(super) fn aggregate_fill(kv_state: &crate::store::KvState, node: &NodeId) -> f32 {
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
        if let Some(tx) = guard.get(&prefix)
            && !tx.is_closed() {
                return tx.subscribe();
            }
        let (new_tx, rx) = watch::channel(0u64);
        let new_tx_arc = Arc::new(new_tx);
        let mut slot = Some(new_tx_arc);
        let result = guard.compute(Arc::clone(&prefix), |existing| match existing {
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
        &ctx.node_id, ctx.default_ttl, Arc::clone(opacity_key), payload, false, &ctx.hlc,
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
        &ctx.node_id, ctx.default_ttl, Arc::clone(opacity_key), Bytes::new(), true, &ctx.hlc,
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
pub(super) async fn run_consolidated_opacity_watcher(
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
                opacity_key:    Arc::clone(&reg.opacity_key),
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
                    opacity_key:    Arc::clone(&reg.opacity_key),
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
