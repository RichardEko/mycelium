//! [`run_kv_persist_task`] — the substrate's generic soft-state advertisement loop.
//!
//! A standing background task that re-asserts a single KV key every `interval`
//! tick (capability beacons, locality, `advertise_persistent`, …) and tombstones
//! it on exit (cancel, shutdown, or sender drop). Pure Layer I: it writes to the
//! store and gossips, nothing more. The first tick flips
//! [`CoreCtx::soft_state_advertised`](crate::CoreCtx) so the gateway readiness
//! probe can distinguish "process up" from "soft state hydrated".

use crate::context::CoreCtx;
use crate::framing::{dispatch_gossip_send, dispatch_gossip_try_send, ForwardHint, WireMessage};
use crate::store::apply_and_notify;
use bytes::Bytes;
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, time};

/// Closure that produces the payload bytes for one tick of [`run_kv_persist_task`].
pub type PersistPayloadFn = Arc<dyn Fn() -> Bytes + Send + Sync>;
/// Optional per-tick side-effect (e.g. a matching signal emission) invoked
/// synchronously before the KV write.
pub type PersistOnTickFn = Arc<dyn Fn(&Arc<CoreCtx>, &Bytes) + Send + Sync>;

/// Shared persist-loop primitive: ticks at `interval` and writes `payload_fn()`
/// to `kv_key` (Layer I) plus gossips it. Optional `on_tick` runs synchronously
/// before the KV write — used by `MeshHandle::advertise_persistent` to emit a
/// matching signal, and by capability ops with `None` (write only).
///
/// Tombstones `kv_key` at exit (cancel, shutdown, or sender drop), awaiting
/// channel capacity so the retraction is never silently dropped.
#[allow(clippy::too_many_arguments)]
pub async fn run_kv_persist_task(
    ctx:             Arc<CoreCtx>,
    mut cancel_rx:   tokio::sync::oneshot::Receiver<()>,
    mut shutdown_rx: watch::Receiver<bool>,
    kv_key:          Arc<str>,
    interval:        Duration,
    payload_fn:      PersistPayloadFn,
    on_tick:         Option<PersistOnTickFn>,
) {
    let mut ticker = time::interval(interval);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut first_tick = true;
    loop {
        tokio::select! { biased;
            _ = &mut cancel_rx               => break,
            _ = shutdown_rx.wait_for(|v| *v) => break,
            _ = ticker.tick() => {
                let payload = payload_fn();
                if let Some(ref f) = on_tick {
                    f(&ctx, &payload);
                }
                let update = crate::framing::make_gossip_update(
                    &ctx.node_id, ctx.default_ttl, Arc::clone(&kv_key), payload, false, &ctx.hlc,
                );
                apply_and_notify(&ctx.kv_state, &update);
                if first_tick {
                    ctx.soft_state_advertised.store(true, std::sync::atomic::Ordering::Release);
                    first_tick = false;
                }
                dispatch_gossip_try_send(
                    &ctx.gossip_txs, WireMessage::Data(update),
                    ctx.node_id.id_hash(), ForwardHint::All, &ctx.kv_state.dropped_frames,
                );
            }
        }
    }
    let tombstone = crate::framing::make_gossip_update(
        &ctx.node_id, ctx.default_ttl, Arc::clone(&kv_key), Bytes::new(), true, &ctx.hlc,
    );
    apply_and_notify(&ctx.kv_state, &tombstone);
    dispatch_gossip_send(
        &ctx.gossip_txs, WireMessage::Data(tombstone),
        ctx.node_id.id_hash(), ForwardHint::All,
    ).await;
}
