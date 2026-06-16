//! [`SubscribeHandle`] — minimal agent proxy for the HTTP gateway's
//! consumer-group log endpoint.
//!
//! Stays in the full `mycelium` crate (not `mycelium-core` with `KvHandle`)
//! because its only method, `distributed_lock`, builds on the Layer III
//! consensus overlay (`overlay_consistent::LockGuard`). Consensus-gated: the
//! only callers are the gateway's consumer-group log endpoint and
//! `distributed_lock` itself.

#![cfg(all(feature = "gateway", feature = "consensus"))]

use crate::framing::ForwardHint;
use crate::store::KvState;
use bytes::Bytes;
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use super::TaskCtx;

/// Minimal agent proxy used by the HTTP gateway's consumer-group log endpoint.
pub(super) struct SubscribeHandle {
    pub(super) task_ctx: Arc<TaskCtx>,
    kv_state:            Arc<KvState>,
}

impl SubscribeHandle {
    /// Construct from an `Arc<TaskCtx>`.
    pub(super) fn from_task_ctx(task_ctx: Arc<TaskCtx>) -> Self {
        let kv_state = Arc::clone(&task_ctx.kv_state);
        Self { task_ctx, kv_state }
    }

    pub(super) async fn distributed_lock(
        &self,
        name: &str,
        ttl:  Duration,
    ) -> Result<super::overlay_consistent::LockGuard, super::overlay_consistent::ConsistencyError>
    {
        use super::helpers::make_gossip_update;
        use crate::store::apply_and_notify;

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let lock_json = serde_json::json!({
            "holder":     self.task_ctx.node_id.to_string(),
            "expires_ms": now_ms + ttl.as_millis() as u64,
        });
        let value = Bytes::from(serde_json::to_vec(&lock_json).unwrap_or_default());

        let key: Arc<str> = Arc::from(format!("lock/{name}").as_str());
        let update = make_gossip_update(
            &self.task_ctx.node_id,
            self.task_ctx.default_ttl,
            Arc::clone(&key),
            value,
            false,
            &self.task_ctx.hlc,
        );
        apply_and_notify(&self.kv_state, &update);
        crate::framing::dispatch_gossip_try_send(
            &self.task_ctx.gossip_txs,
            crate::framing::WireMessage::Data(update),
            self.task_ctx.node_id.id_hash(),
            ForwardHint::All,
            &self.kv_state.dropped_frames,
        );

        Ok(super::overlay_consistent::LockGuard {
            ctx:      Arc::clone(&self.task_ctx),
            name:     Arc::from(name),
            token:    self.task_ctx.hlc.current(),
            released: false,
        })
    }
}
