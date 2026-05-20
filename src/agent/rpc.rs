//! Formalised point-to-point RPC primitive built on top of the signal mesh.
//!
//! `rpc_call` / `rpc_respond` codify the `signal_once + nonce` pattern that was
//! previously implicit in `INVOKE` / `INVOKE_RESULT` usage. Callers and responders
//! work with typed `Bytes` — the 8-byte correlation nonce is prepended by
//! `rpc_call` and echoed back by `rpc_respond` without either side managing it
//! directly.
//!
//! All replies flow through the single `RPC_RESULT` signal kind; the nonce
//! distinguishes concurrent in-flight calls.

use crate::node_id::NodeId;
use crate::signal::{Signal, SignalScope, signal_kind};
use bytes::{BufMut, Bytes, BytesMut};
use std::{sync::Arc, time::Duration};

use super::GossipAgent;
use super::emit_signal;

/// Error returned by [`GossipAgent::rpc_call`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcError {
    /// No reply arrived before the timeout elapsed.
    Timeout,
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcError::Timeout => f.write_str("rpc call timed out — no reply from target"),
        }
    }
}

impl std::error::Error for RpcError {}

impl GossipAgent {
    /// Sends a point-to-point RPC request to `target` and awaits the reply.
    ///
    /// Generates a random 8-byte correlation nonce, prepends it to `payload`,
    /// emits `kind` as `SignalScope::Individual(target)`, then awaits the first
    /// `"rpc.result"` signal from `target` whose payload starts with the same nonce.
    ///
    /// Returns `Ok(Bytes)` with the reply payload (nonce prefix stripped), or
    /// `Err(RpcError::Timeout)` if no reply arrives within `timeout`.
    ///
    /// The reply handler is registered **before** the request is emitted so no
    /// reply can be lost even if the responder is co-located and responds synchronously.
    ///
    /// # Example
    /// ```ignore
    /// // Caller
    /// let reply = agent.rpc_call(worker_id, "mcp.invoke", request_bytes, Duration::from_secs(30)).await?;
    ///
    /// // Responder (in a signal handler loop)
    /// let mut rx = agent.signal_rx("mcp.invoke");
    /// while let Some(req) = rx.recv().await {
    ///     let result = handle_invoke(&req.payload[8..]);  // skip nonce
    ///     agent.rpc_respond(&req, result);
    /// }
    /// ```
    pub async fn rpc_call(
        &self,
        target:  NodeId,
        kind:    impl Into<Arc<str>>,
        payload: impl Into<Bytes>,
        timeout: Duration,
    ) -> Result<Bytes, RpcError> {
        let target_clone = target.clone();
        let reply = self.request(
            kind,
            SignalScope::Individual(target),
            payload,
            signal_kind::RPC_RESULT,
            timeout,
        ).await;

        match reply {
            Some(sig) if sig.sender == target_clone => {
                // Strip the echoed 8-byte nonce prefix; caller gets the bare result.
                Ok(sig.payload.slice(8..))
            }
            Some(sig) => {
                // rpc.result arrived from a different sender — nonce matched by chance
                // (extremely unlikely) or a bug. Treat as timeout.
                tracing::warn!(
                    target = %target_clone,
                    actual_sender = %sig.sender,
                    "rpc_call: rpc.result sender mismatch — treating as timeout"
                );
                Err(RpcError::Timeout)
            }
            None => Err(RpcError::Timeout),
        }
    }

    /// Sends a reply to an incoming RPC request.
    ///
    /// Extracts the 8-byte correlation nonce from `request.payload[..8]`, prepends
    /// it to `result`, and emits `"rpc.result"` as
    /// `SignalScope::Individual(request.sender)`.
    ///
    /// Calling this with a signal that was not originated by [`rpc_call`](Self::rpc_call)
    /// (i.e. whose payload is shorter than 8 bytes) is a no-op — the missing nonce
    /// means the caller's correlation will never match and the reply is silently dropped.
    ///
    /// # Example
    /// ```ignore
    /// let mut rx = agent.signal_rx("mcp.invoke");
    /// while let Some(req) = rx.recv().await {
    ///     let result = compute_result(&req.payload[8..]);
    ///     agent.rpc_respond(&req, result);
    /// }
    /// ```
    pub fn rpc_respond(&self, request: &Signal, result: impl Into<Bytes>) {
        let Some(nonce_bytes) = request.payload.get(..8) else {
            tracing::warn!(
                sender = %request.sender,
                kind   = %request.kind,
                "rpc_respond called on signal with payload shorter than 8 bytes — no nonce to echo"
            );
            return;
        };
        let result_bytes: Bytes = result.into();
        let mut buf = BytesMut::with_capacity(8 + result_bytes.len());
        buf.put_slice(nonce_bytes);
        buf.put(result_bytes);
        emit_signal(
            &self.task_ctx,
            Arc::from(signal_kind::RPC_RESULT),
            SignalScope::Individual(request.sender.clone()),
            buf.freeze(),
        );
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
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    fn agent_pair() -> (Arc<GossipAgent>, Arc<GossipAgent>) {
        let port_a = alloc_port();
        let port_b = alloc_port();
        let id_a = NodeId::new("127.0.0.1", port_a).unwrap();
        let id_b = NodeId::new("127.0.0.1", port_b).unwrap();

        let mut cfg_a = GossipConfig::default();
        cfg_a.bind_port = port_a;
        cfg_a.bootstrap_peers = vec![NodeId::new("127.0.0.1", port_b).unwrap()];

        let mut cfg_b = GossipConfig::default();
        cfg_b.bind_port = port_b;
        cfg_b.bootstrap_peers = vec![NodeId::new("127.0.0.1", port_a).unwrap()];

        (
            Arc::new(GossipAgent::new(id_a, cfg_a)),
            Arc::new(GossipAgent::new(id_b, cfg_b)),
        )
    }

    #[tokio::test]
    async fn test_rpc_round_trip() {
        let (agent_a, agent_b) = agent_pair();
        agent_a.start().await.unwrap();
        agent_b.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let node_b = agent_b.node_id().clone();

        let responder = Arc::clone(&agent_b);
        tokio::spawn(async move {
            let mut rx = responder.signal_rx("echo");
            if let Some(req) = rx.recv().await {
                let body = req.payload.slice(8..);
                responder.rpc_respond(&req, body);
            }
        });

        let result = agent_a.rpc_call(
            node_b,
            "echo",
            Bytes::from_static(b"hello"),
            Duration::from_secs(2),
        ).await;

        assert!(result.is_ok(), "expected Ok, got {result:?}");
        assert_eq!(result.unwrap(), Bytes::from_static(b"hello"));

        agent_a.shutdown().await;
        agent_b.shutdown().await;
    }

    #[tokio::test]
    async fn test_rpc_timeout() {
        let port = alloc_port();
        let id = NodeId::new("127.0.0.1", port).unwrap();
        let mut cfg = GossipConfig::default();
        cfg.bind_port = port;
        let agent = Arc::new(GossipAgent::new(id, cfg));
        agent.start().await.unwrap();

        let ghost = NodeId::new("127.0.0.1", 19999).unwrap();
        let result = agent.rpc_call(
            ghost,
            "noop",
            Bytes::from_static(b"ping"),
            Duration::from_millis(150),
        ).await;

        assert_eq!(result, Err(RpcError::Timeout));
        agent.shutdown().await;
    }

    #[tokio::test]
    async fn test_rpc_nonce_isolation() {
        let (agent_a, agent_b) = agent_pair();
        agent_a.start().await.unwrap();
        agent_b.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let node_b = agent_b.node_id().clone();

        let responder = Arc::clone(&agent_b);
        tokio::spawn(async move {
            let mut rx = responder.signal_rx("tagged");
            while let Some(req) = rx.recv().await {
                let body = req.payload.slice(8..);
                responder.rpc_respond(&req, body);
            }
        });

        let b1 = node_b.clone();
        let b2 = node_b.clone();
        let a1 = Arc::clone(&agent_a);
        let a2 = Arc::clone(&agent_a);
        let (r1, r2) = tokio::join!(
            async move { a1.rpc_call(b1, "tagged", Bytes::from_static(b"call-one"), Duration::from_secs(2)).await },
            async move { a2.rpc_call(b2, "tagged", Bytes::from_static(b"call-two"), Duration::from_secs(2)).await },
        );

        assert_eq!(r1.unwrap(), Bytes::from_static(b"call-one"));
        assert_eq!(r2.unwrap(), Bytes::from_static(b"call-two"));

        agent_a.shutdown().await;
        agent_b.shutdown().await;
    }
}
