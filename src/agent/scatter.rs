//! Scatter-gather: fan-out RPC with configurable minimum-reply threshold.
//!
//! [`GossipAgent::scatter_gather`] fans out identical requests to a list of
//! target nodes in parallel via the existing [`rpc_call`](GossipAgent::rpc_call)
//! primitive, collects replies as they arrive, and returns as soon as
//! `min_ok` successful replies have been received — aborting the remaining
//! in-flight calls. If fewer than `min_ok` replies arrive before `timeout`,
//! `Err(ScatterError::InsufficientReplies)` is returned.

use crate::node_id::NodeId;
use bytes::Bytes;
use std::{sync::Arc, time::Duration};

use super::GossipAgent;

/// A single successful reply from a scatter-gather fan-out.
#[derive(Debug)]
pub struct ScatterResult {
    pub node_id: NodeId,
    pub payload: Bytes,
}

/// Error returned by [`GossipAgent::scatter_gather`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScatterError {
    /// Fewer than `min_ok` targets replied before the timeout elapsed.
    InsufficientReplies { got: usize, needed: usize },
}

impl std::fmt::Display for ScatterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScatterError::InsufficientReplies { got, needed } =>
                write!(f, "scatter_gather: {got} of {needed} required replies received"),
        }
    }
}

impl std::error::Error for ScatterError {}

impl GossipAgent {
    /// Sends `payload` to every node in `targets` as a point-to-point RPC and
    /// collects replies.
    ///
    /// Use [`ServiceHandle::scatter_gather`] via [`GossipAgent::service`] instead.
    pub async fn scatter_gather(
        &self,
        targets:  Vec<NodeId>,
        kind:     impl Into<Arc<str>>,
        payload:  impl Into<Bytes>,
        timeout:  Duration,
        min_ok:   usize,
    ) -> Result<Vec<ScatterResult>, ScatterError> {
        self.service().scatter_gather(targets, kind, payload, timeout, min_ok).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GossipAgent, GossipConfig, NodeId};
    use bytes::Bytes;
    use std::{sync::Arc, time::Duration};

    fn alloc_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap().local_addr().unwrap().port()
    }

    #[tokio::test]
    async fn scatter_gather_two_of_two() {
        let pa = alloc_port(); let pb = alloc_port();
        let id_a = NodeId::new("127.0.0.1", pa).unwrap();
        let id_b = NodeId::new("127.0.0.1", pb).unwrap();

        let mut cfg_a = GossipConfig::default(); cfg_a.bind_port = pa;
        cfg_a.bootstrap_peers = vec![id_b.clone()];
        let mut cfg_b = GossipConfig::default(); cfg_b.bind_port = pb;
        cfg_b.bootstrap_peers = vec![id_a.clone()];

        let agent_a = Arc::new(GossipAgent::new(id_a.clone(), cfg_a));
        let agent_b = Arc::new(GossipAgent::new(id_b.clone(), cfg_b));
        agent_a.start().await.unwrap();
        agent_b.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(60)).await;

        // Register an "echo-scatter" responder on node-b
        let responder = Arc::clone(&agent_b);
        tokio::spawn(async move {
            let mut rx = responder.rpc_rx("echo-scatter");
            while let Some(req) = rx.recv().await {
                responder.rpc_respond(&req, req.payload());
            }
        });

        let targets = vec![id_b.clone()];
        let result = agent_a.scatter_gather(
            targets, "echo-scatter", Bytes::from_static(b"ping"),
            Duration::from_secs(2), 1,
        ).await;

        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let replies = result.unwrap();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].payload, Bytes::from_static(b"ping"));
        assert_eq!(replies[0].node_id, id_b);

        agent_a.shutdown().await;
        agent_b.shutdown().await;
    }

    #[tokio::test]
    async fn scatter_gather_insufficient_replies() {
        let pa = alloc_port();
        let id_a = NodeId::new("127.0.0.1", pa).unwrap();
        let mut cfg_a = GossipConfig::default(); cfg_a.bind_port = pa;
        let agent_a = Arc::new(GossipAgent::new(id_a, cfg_a));
        agent_a.start().await.unwrap();

        // Ghost targets that won't reply
        let ghost1 = NodeId::new("127.0.0.1", 19991).unwrap();
        let ghost2 = NodeId::new("127.0.0.1", 19992).unwrap();
        let result = agent_a.scatter_gather(
            vec![ghost1, ghost2], "noop", Bytes::new(),
            Duration::from_millis(200), 1,
        ).await;

        assert!(matches!(result, Err(ScatterError::InsufficientReplies { got: 0, needed: 1 })));
        agent_a.shutdown().await;
    }
}
