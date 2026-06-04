use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;

use crate::node_id::NodeId;
use super::GossipAgent;

/// Result of a reliable delivery attempt via [`GossipAgent::emit_reliable`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AckResult {
    Acknowledged,
    Timeout,
}

impl GossipAgent {
    /// Send `payload` to `target` and wait for an explicit RPC ACK.
    ///
    /// Use [`ServiceHandle::emit_reliable`] via [`GossipAgent::service`] instead.
    pub async fn emit_reliable(
        &self,
        target:  NodeId,
        kind:    impl Into<Arc<str>>,
        payload: impl Into<Bytes>,
        timeout: Duration,
    ) -> AckResult {
        self.service().emit_reliable(target, kind, payload, timeout).await
    }
}
