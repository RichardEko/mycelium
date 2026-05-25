use crate::capability::CapFilter;
use crate::node_id::NodeId;
use crate::signal::SignalScope;
use bytes::Bytes;
use std::sync::Arc;

use super::{GossipAgent, sharding::{shard_owner, ShardError}};

impl GossipAgent {
    /// Returns the deterministic shard owner for `shard_key` among providers
    /// matching `filter` in the local capability view.
    ///
    /// Uses a consistent-hash ring over `NodeId::id_hash()` — all nodes seeing
    /// the same provider set return the same owner for the same key.
    ///
    /// Returns `None` when no providers match the filter.
    pub fn shard_for(&self, shard_key: &str, filter: &CapFilter) -> Option<NodeId> {
        let providers = self.resolve(filter);
        shard_owner(shard_key, &providers)
    }

    /// Routes `payload` directly to the consistent-hash owner for `shard_key`
    /// among providers matching `filter`.
    ///
    /// Resolves the shard owner then emits the signal with
    /// `SignalScope::Individual(owner)` — direct, non-broadcast delivery.
    ///
    /// Returns `Ok(owner_node_id)` on successful dispatch, or
    /// `Err(ShardError::NoProviders)` when the filter matches nothing.
    pub async fn emit_sharded(
        &self,
        kind:      impl Into<Arc<str>>,
        shard_key: &str,
        filter:    &CapFilter,
        payload:   impl Into<Bytes>,
    ) -> Result<NodeId, ShardError> {
        let owner = self.shard_for(shard_key, filter)
            .ok_or(ShardError::NoProviders)?;
        let _ = self.emit_async(kind, SignalScope::Individual(owner.clone()), payload).await;
        Ok(owner)
    }
}
