//! [`SubscribeHandle`] — minimal agent proxy for the HTTP gateway's
//! consumer-group log endpoint.
//!
//! Stays in the full `mycelium` crate (not `mycelium-core` with `KvHandle`)
//! because its only method, `distributed_lock`, acquires through the Layer III
//! consensus overlay (`ConsensusHandle::distributed_lock`). Consensus-gated: the
//! only callers are the gateway's consumer-group log endpoint and
//! `distributed_lock` itself.

#![cfg(all(feature = "gateway", feature = "consensus"))]

use std::{sync::Arc, time::Duration};

use super::TaskCtx;

/// Minimal agent proxy used by the HTTP gateway's consumer-group log endpoint.
pub(super) struct SubscribeHandle {
    pub(super) task_ctx: Arc<TaskCtx>,
}

impl SubscribeHandle {
    /// Construct from an `Arc<TaskCtx>`.
    pub(super) fn from_task_ctx(task_ctx: Arc<TaskCtx>) -> Self {
        Self { task_ctx }
    }

    /// Acquire a named distributed lock through cluster consensus.
    ///
    /// Delegates to [`ConsensusHandle::distributed_lock`](super::ConsensusHandle::distributed_lock):
    /// the lock record is committed via `system_propose`, so the guard is returned **only if this
    /// node's proposal committed** — genuine mutual exclusion across nodes.
    ///
    /// This previously wrote the `lock/{name}` KV entry directly (`apply_and_notify`) and returned
    /// a guard *unconditionally* — a bare LWW write with no consensus, no compare-and-swap, and no
    /// read-back. It provided **no** mutual exclusion: N cross-node consumer-group subscribers each
    /// "acquired" the claim simultaneously and each drained the whole log stream, breaking the
    /// exact-once delivery guarantee (issue #149; regression gate: overlay scenario S11).
    pub(super) async fn distributed_lock(
        &self,
        name: &str,
        ttl:  Duration,
    ) -> Result<super::overlay_consistent::LockGuard, super::overlay_consistent::ConsistencyError>
    {
        let consensus = super::ConsensusHandle { ctx: Arc::clone(&self.task_ctx) };
        consensus.distributed_lock(name, ttl).await
    }
}
