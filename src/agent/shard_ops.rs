use crate::capability::CapFilter;
use crate::node_id::NodeId;
use bytes::Bytes;
use std::sync::Arc;

use super::{GossipAgent, sharding::ShardError};

impl GossipAgent {
    /// Returns the deterministic shard owner for `shard_key` among providers
    /// matching `filter` in the local capability view.
    ///
    /// Use [`ServiceHandle::shard_for`] via [`GossipAgent::service`] instead.
    pub fn shard_for(&self, shard_key: &str, filter: &CapFilter) -> Option<NodeId> {
        self.service().shard_for(shard_key, filter)
    }

    /// Routes `payload` directly to the consistent-hash owner for `shard_key`.
    ///
    /// Use [`ServiceHandle::emit_sharded`] via [`GossipAgent::service`] instead.
    pub async fn emit_sharded(
        &self,
        kind:      impl Into<Arc<str>>,
        shard_key: &str,
        filter:    &CapFilter,
        payload:   impl Into<Bytes>,
    ) -> Result<NodeId, ShardError> {
        self.service().emit_sharded(kind, shard_key, filter, payload).await
    }
}
