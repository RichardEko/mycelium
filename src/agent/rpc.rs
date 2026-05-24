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
use tokio::sync::mpsc;

use super::{GossipAgent, TaskCtx};
use super::emit_signal;

// ── RpcRequest newtype ────────────────────────────────────────────────────────

/// A received RPC request signal with the 8-byte correlation nonce hidden.
///
/// Obtained from [`GossipAgent::rpc_rx`] or by wrapping a [`Signal`] with
/// `RpcRequest::from`. The nonce is used internally by [`GossipAgent::rpc_respond`];
/// callers work only with `payload()` and `sender()`.
#[derive(Clone, Debug)]
pub struct RpcRequest(pub(crate) Signal);

impl RpcRequest {
    /// Application payload with the 8-byte nonce prefix stripped.
    pub fn payload(&self) -> Bytes   { self.0.payload.slice(8..) }
    /// NodeId of the node that sent the request.
    pub fn sender(&self)  -> &NodeId { &self.0.sender }
    /// Signal kind (e.g. `"mcp.invoke"`).
    pub fn kind(&self)    -> &Arc<str> { &self.0.kind }
    /// RPC correlation nonce (the 8 bytes prepended by `rpc_call`). Useful as
    /// a per-invocation trace correlator in audit records.
    pub fn nonce(&self) -> u64 {
        let b: [u8; 8] = self.0.payload.slice(..8).as_ref().try_into()
            .unwrap_or([0u8; 8]);
        u64::from_le_bytes(b)
    }
}

impl From<Signal> for RpcRequest {
    fn from(s: Signal) -> Self { RpcRequest(s) }
}

/// A signal receiver that yields [`RpcRequest`] values.
///
/// Returned by [`GossipAgent::rpc_rx`]. Thin wrapper around
/// `mpsc::Receiver<Signal>` that applies `RpcRequest::from` on each message.
pub struct RpcRequestRx(pub(crate) mpsc::Receiver<Signal>);

impl RpcRequestRx {
    /// Receives the next RPC request. Returns `None` when the agent shuts down.
    pub async fn recv(&mut self) -> Option<RpcRequest> {
        self.0.recv().await.map(RpcRequest)
    }
}

/// Error returned by [`GossipAgent::rpc_call`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcError {
    /// No reply arrived before the timeout elapsed.
    Timeout,
}

/// Registers a one-shot receiver in `ctx.rpc_pending` and awaits the first
/// reply signal whose correlation nonce (first 8 bytes of payload, LE) matches
/// `nonce` and whose sender matches `target`.
///
/// Registration happens synchronously in the first poll — before any yield
/// point — so it is safe to call `emit_signal` immediately before this
/// without missing a co-located reply.
///
/// Returns `Some(payload)` with the 8-byte nonce prefix stripped, or `None`
/// on timeout (including sender mismatch, which is astronomically rare with
/// 64-bit nonces).
pub(crate) async fn await_nonce_reply(
    ctx:      &TaskCtx,
    nonce:    u64,
    target:   &NodeId,
    deadline: tokio::time::Instant,
) -> Option<Bytes> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    ctx.rpc_pending.lock().unwrap().insert(nonce, tx);
    let result = match tokio::time::timeout_at(deadline, rx).await {
        Ok(Ok(sig)) if sig.sender == *target => Some(sig.payload.slice(8..)),
        _ => None,
    };
    ctx.rpc_pending.lock().unwrap().remove(&nonce);
    result
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcError::Timeout => f.write_str("rpc call timed out — no reply from target"),
        }
    }
}

impl std::error::Error for RpcError {}

/// `rpc_respond` logic operating on [`TaskCtx`] directly.
///
/// Used by callers that hold an `Arc<TaskCtx>` rather than a full `GossipAgent`
/// (e.g. MCP task functions). [`GossipAgent::rpc_respond`] delegates here.
pub(crate) fn rpc_respond_ctx(ctx: &TaskCtx, request: &RpcRequest, result: impl Into<Bytes>) {
    let nonce_bytes = request.0.payload.slice(..8);
    let result_bytes: Bytes = result.into();
    let mut buf = BytesMut::with_capacity(8 + result_bytes.len());
    buf.put_slice(&nonce_bytes);
    buf.put(result_bytes);
    emit_signal(
        ctx,
        Arc::from(signal_kind::RPC_RESULT),
        SignalScope::Individual(request.0.sender.clone()),
        buf.freeze(),
    );
}

/// Core `rpc_call` logic operating on [`TaskCtx`] directly.
///
/// Exposed as `pub(crate)` so HTTP handlers that hold only `Arc<TaskCtx>` (not a
/// full `GossipAgent`) can issue RPC calls. [`GossipAgent::rpc_call`] delegates here.
pub(crate) async fn rpc_call_ctx(
    ctx:     &TaskCtx,
    target:  NodeId,
    kind:    Arc<str>,
    payload: Bytes,
    timeout: Duration,
) -> Result<Bytes, RpcError> {
    let nonce: u64 = fastrand::u64(1..);

    let mut buf = BytesMut::with_capacity(8 + payload.len());
    buf.put_u64_le(nonce);
    buf.put(payload);

    emit_signal(ctx, kind, SignalScope::Individual(target.clone()), buf.freeze());

    let deadline = tokio::time::Instant::now() + timeout;
    match await_nonce_reply(ctx, nonce, &target, deadline).await {
        Some(b) => Ok(b),
        None    => Err(RpcError::Timeout),
    }
}

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
        rpc_call_ctx(&self.task_ctx, target, kind.into(), payload.into(), timeout).await
    }

    /// Sends a reply to an incoming RPC request.
    ///
    /// Echoes the correlation nonce from `request` back to the caller and emits
    /// `"rpc.result"` as `SignalScope::Individual(request.sender())`.
    ///
    /// # Example
    /// ```ignore
    /// let mut rx = agent.rpc_rx("mcp.invoke");
    /// while let Some(req) = rx.recv().await {
    ///     let result = compute_result(req.payload());
    ///     agent.rpc_respond(&req, result);
    /// }
    /// ```
    pub fn rpc_respond(&self, request: &RpcRequest, result: impl Into<Bytes>) {
        rpc_respond_ctx(&self.task_ctx, request, result);
    }

    /// Returns a typed receiver for incoming RPC requests of `kind`.
    ///
    /// Equivalent to `signal_rx(kind)` but yields [`RpcRequest`] values with
    /// the nonce already stripped from `payload()`.
    pub fn rpc_rx(&self, kind: impl Into<Arc<str>>) -> RpcRequestRx {
        RpcRequestRx(self.signal_rx(kind))
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
            let mut rx = responder.rpc_rx("echo");
            if let Some(req) = rx.recv().await {
                responder.rpc_respond(&req, req.payload());
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
            let mut rx = responder.rpc_rx("tagged");
            while let Some(req) = rx.recv().await {
                responder.rpc_respond(&req, req.payload());
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
