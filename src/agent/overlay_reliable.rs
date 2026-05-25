use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;

use crate::node_id::NodeId;
use super::GossipAgent;
use super::rpc::RpcError;

/// Result of a reliable delivery attempt via [`GossipAgent::emit_reliable`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AckResult {
    Acknowledged,
    Timeout,
}

impl GossipAgent {
    /// Send `payload` to `target` and wait for an explicit RPC ACK.
    ///
    /// The receiver calls `rpc_respond(&req, b"")` to acknowledge.
    /// Returns [`AckResult::Timeout`] if no ACK arrives within `timeout`.
    pub async fn emit_reliable(
        &self,
        target:  NodeId,
        kind:    impl Into<Arc<str>>,
        payload: impl Into<Bytes>,
        timeout: Duration,
    ) -> AckResult {
        match self.rpc_call(target, kind, payload, timeout).await {
            Ok(_)                  => AckResult::Acknowledged,
            Err(RpcError::Timeout) => AckResult::Timeout,
        }
    }
}
