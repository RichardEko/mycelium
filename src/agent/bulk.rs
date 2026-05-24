//! invoke.bulk — point-to-point RPC for large payloads.
//!
//! Regular [`rpc_call`](super::GossipAgent::rpc_call) encodes the entire
//! payload inside the gossip signal, which floods every intermediate node.
//! `bulk_call` stages the payload in a node-local HTTP endpoint and sends
//! only a lightweight ticket (nonce + kind) over the signal mesh;
//! the responder fetches the payload directly from the caller's HTTP server.
//!
//! ## Wire format (INVOKE_BULK signal payload)
//!
//! ```text
//! ┌────────────────┬────────────┬───────────────────┐
//! │ nonce  (8 B LE)│ port (2 LE)│ kind (UTF-8 bytes) │
//! └────────────────┴────────────┴───────────────────┘
//! ```
//!
//! The caller stages the payload at `GET /bulk/{nonce:016x}` on its own HTTP
//! server. The target fetches it there using the caller's IP (from the signal
//! envelope) and the caller's `port` (from the ticket), processes it, and
//! replies via `bulk.result` (a dedicated signal kind, separate from
//! `rpc.result`) so bulk reply handlers do not compete with RPC reply handlers.
//!
//! The port is per-ticket (not per-node configuration) because the caller's
//! HTTP port must be communicated to the serving node; the server's own
//! `BulkTransport::http_port` is irrelevant to the fetch.
//!
//! ## Endpoints
//!
//! The embedded HTTP gateway (`src/agent/http.rs`) exposes `GET /bulk/{id}`.
//! Applications that run their own HTTP server must add an equivalent route
//! using [`GossipAgent::bulk_staging_get`].
//!
//! ## Transport configuration
//!
//! All bulk-transport configuration (HTTP port, fetch timeout, pooled HTTP
//! client) is encapsulated in [`BulkTransport`], which is constructed once
//! in [`GossipAgent::new`] from [`GossipConfig`](crate::GossipConfig) and
//! stored in [`TaskCtx`](super::TaskCtx). Call sites do not pass `http_port`
//! explicitly; they read it from the configured transport.

use crate::node_id::NodeId;
use crate::signal::{Signal, SignalScope, signal_kind};
use bytes::{BufMut, Bytes, BytesMut};
use std::{sync::{Arc, atomic::{AtomicU16, Ordering}}, time::Duration};
use tokio::sync::oneshot;
use tracing::warn;

use super::{GossipAgent, TaskCtx};
use super::emit_signal;
use super::rpc::await_nonce_reply;

// ── Transport adapter ─────────────────────────────────────────────────────────

/// Encapsulates all bulk-transport concerns: staging map, HTTP port, fetch
/// timeout, and the pooled HTTP client used by `bulk_serve` to retrieve
/// staged payloads from remote callers.
pub struct BulkTransport {
    staging:   papaya::HashMap<u64, Bytes>,
    http_port: AtomicU16,
    client:    reqwest::Client,
}

impl BulkTransport {
    pub fn new(http_port: u16, fetch_timeout: Duration) -> Self {
        let client = reqwest::Client::builder()
            .timeout(fetch_timeout)
            .build()
            .expect("reqwest::Client build should never fail with valid config");
        Self {
            staging:   papaya::HashMap::new(),
            http_port: AtomicU16::new(http_port),
            client,
        }
    }

    pub fn http_port(&self) -> u16 {
        self.http_port.load(Ordering::Relaxed)
    }

    /// Overrides the HTTP port used when advertising staged payloads to peers.
    ///
    /// Call this when using a custom HTTP server instead of the embedded gateway
    /// (`GossipConfig::http_port`). The port is stored atomically so it can be
    /// updated at any time after agent construction.
    pub fn set_http_port(&self, port: u16) {
        self.http_port.store(port, Ordering::Relaxed);
    }

    /// Stages `payload` under `nonce` and returns a [`StagedGuard`] that
    /// removes the entry on drop (cancellation-safe cleanup).
    fn stage(&self, nonce: u64, payload: Bytes) -> StagedGuard<'_> {
        self.staging.pin().insert(nonce, payload);
        StagedGuard { nonce, staging: &self.staging }
    }

    /// Returns the staged payload for `nonce`, without removing it.
    pub fn get(&self, nonce: u64) -> Option<Bytes> {
        self.staging.pin().get(&nonce).cloned()
    }

    /// Fetches a staged payload from a remote node's HTTP endpoint.
    pub async fn fetch(&self, url: &str) -> Result<Bytes, reqwest::Error> {
        self.client.get(url).send().await?.error_for_status()?.bytes().await
    }
}

// ── RAII guard ────────────────────────────────────────────────────────────────

struct StagedGuard<'a> {
    nonce:   u64,
    staging: &'a papaya::HashMap<u64, Bytes>,
}

impl Drop for StagedGuard<'_> {
    fn drop(&mut self) {
        self.staging.pin().remove(&self.nonce);
    }
}

// ── Error type ────────────────────────────────────────────────────────────────

/// Error returned by [`GossipAgent::bulk_call`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BulkError {
    /// No reply arrived before the timeout elapsed.
    Timeout,
    /// The caller does not have an HTTP server port configured.
    ///
    /// Set `GossipConfig::http_port` before starting the agent, or call
    /// `GossipConfig::set_http_port`.
    NoHttpPort,
}

impl std::fmt::Display for BulkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BulkError::Timeout    => f.write_str("bulk_call timed out — no reply from target"),
            BulkError::NoHttpPort => f.write_str("bulk_call requires an http_port in GossipConfig"),
        }
    }
}

impl std::error::Error for BulkError {}

/// Cancels the corresponding `bulk_serve` background task on drop.
pub struct BulkServeHandle {
    pub(crate) _cancel: oneshot::Sender<()>,
}

// ── Core logic ────────────────────────────────────────────────────────────────

/// Core `bulk_call` logic.
pub(crate) async fn bulk_call_ctx(
    ctx:     &TaskCtx,
    target:  NodeId,
    kind:    Arc<str>,
    payload: Bytes,
    timeout: Duration,
) -> Result<Bytes, BulkError> {
    let http_port = ctx.bulk_transport.http_port();
    if http_port == 0 { return Err(BulkError::NoHttpPort); }

    let nonce: u64 = fastrand::u64(1..);

    // Stage the payload; _guard removes it on any exit (timeout, cancel, success).
    let _guard = ctx.bulk_transport.stage(nonce, payload);

    // Build the ticket: nonce(8) | http_port(2) | kind_bytes
    let kind_bytes = kind.as_bytes();
    let mut buf = BytesMut::with_capacity(10 + kind_bytes.len());
    buf.put_u64_le(nonce);
    buf.put_u16_le(http_port);
    buf.put(kind_bytes);
    let ticket = buf.freeze();

    emit_signal(ctx, Arc::from(signal_kind::INVOKE_BULK), SignalScope::Individual(target.clone()), ticket);

    let deadline = tokio::time::Instant::now() + timeout;
    match await_nonce_reply(ctx, nonce, &target, deadline).await {
        Some(b) => Ok(b),
        None    => Err(BulkError::Timeout),
    }
}

/// Background task that handles incoming `INVOKE_BULK` signals for a given kind.
async fn bulk_serve_task<F, Fut>(
    ctx:         Arc<TaskCtx>,
    kind:        Arc<str>,
    handler:     Arc<F>,
    mut cancel_rx:   oneshot::Receiver<()>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
)
where
    F: Fn(NodeId, Bytes) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Bytes> + Send + 'static,
{
    let mut rx = ctx.signal_handlers.register_with_capacity(
        Arc::from(signal_kind::INVOKE_BULK),
        256,
    );

    loop {
        tokio::select! { biased;
            _ = &mut cancel_rx => break,
            result = shutdown_rx.changed() => {
                if result.is_err() || *shutdown_rx.borrow() { break; }
            }
            msg = rx.recv() => {
                let Some(sig) = msg else { break };
                handle_bulk_signal(&ctx, &kind, &handler, sig).await;
            }
        }
    }
}

async fn handle_bulk_signal<F, Fut>(
    ctx:     &Arc<TaskCtx>,
    kind:    &Arc<str>,
    handler: &Arc<F>,
    sig:     Signal,
)
where
    F: Fn(NodeId, Bytes) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Bytes> + Send + 'static,
{
    // Wire: nonce(8) | http_port(2) | kind_bytes
    if sig.payload.len() < 10 { return; }
    let nonce     = u64::from_le_bytes(sig.payload[..8].try_into().unwrap());
    let http_port = u16::from_le_bytes(sig.payload[8..10].try_into().unwrap());
    let sig_kind  = match std::str::from_utf8(&sig.payload[10..]) {
        Ok(s)  => s,
        Err(_) => return,
    };
    if sig_kind != kind.as_ref() { return; }

    let sender_ip = sig.sender.to_socket_addr().ip();
    let url = format!("http://{sender_ip}:{http_port}/bulk/{nonce:016x}");

    let handler_clone = Arc::clone(handler);
    let sender        = sig.sender.clone();
    let ctx_clone     = Arc::clone(ctx);

    tokio::spawn(async move {
        let payload = match ctx_clone.bulk_transport.fetch(&url).await {
            Ok(b)  => b,
            Err(e) => { warn!(%url, "bulk_serve: fetch failed: {e}"); return; }
        };

        let result = handler_clone(sender.clone(), payload).await;

        let mut buf = BytesMut::with_capacity(8 + result.len());
        buf.put_u64_le(nonce);
        buf.put(result);
        emit_signal(
            &ctx_clone,
            Arc::from(signal_kind::BULK_RESULT),
            SignalScope::Individual(sender),
            buf.freeze(),
        );
    });
}

// ── GossipAgent API ───────────────────────────────────────────────────────────

impl GossipAgent {
    /// Sends a large `payload` to `target` via the bulk-call protocol.
    ///
    /// The payload is staged at `GET /bulk/{nonce}` on the caller's HTTP server
    /// (configured via `GossipConfig::http_port`). A lightweight ticket is sent
    /// over the signal mesh; the target fetches the payload directly and sends
    /// back a reply.
    ///
    /// Returns `Ok(Bytes)` with the reply, or an error on timeout or
    /// configuration problems.
    pub async fn bulk_call(
        &self,
        target:  NodeId,
        kind:    impl Into<Arc<str>>,
        payload: impl Into<Bytes>,
        timeout: Duration,
    ) -> Result<Bytes, BulkError> {
        bulk_call_ctx(
            &self.task_ctx, target, kind.into(), payload.into(), timeout,
        ).await
    }

    /// Reads a staged bulk payload by nonce, without removing it.
    ///
    /// Used by application HTTP handlers to serve `GET /bulk/{nonce}` requests
    /// from bulk-call targets. Returns `None` when the nonce is not found or
    /// has already been cleaned up.
    pub fn bulk_staging_get(&self, nonce: u64) -> Option<Bytes> {
        self.task_ctx.bulk_transport.get(nonce)
    }

    /// Overrides the HTTP port used when advertising staged bulk payloads.
    ///
    /// Use this when running a custom HTTP server (not the embedded gateway)
    /// that serves `GET /bulk/{nonce}` via [`bulk_staging_get`](Self::bulk_staging_get).
    /// Must be called before the first [`bulk_call`](Self::bulk_call).
    pub fn set_bulk_serving_port(&self, port: u16) {
        self.task_ctx.bulk_transport.set_http_port(port);
    }

    /// Registers a handler for incoming bulk calls of a given `kind`.
    ///
    /// Spawns a background task that listens for `INVOKE_BULK` signals
    /// matching `kind`, fetches the staged payload from the caller's HTTP
    /// endpoint, passes it to `handler`, and sends the result back.
    ///
    /// The returned [`BulkServeHandle`] cancels the task when dropped.
    pub fn bulk_serve<F, Fut>(
        &self,
        kind:    impl Into<Arc<str>>,
        handler: F,
    ) -> BulkServeHandle
    where
        F: Fn(NodeId, Bytes) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Bytes> + Send + 'static,
    {
        let (cancel_tx, cancel_rx) = oneshot::channel();
        let ctx         = Arc::clone(&self.task_ctx);
        let shutdown_rx = self.shutdown_tx.subscribe();
        let kind: Arc<str> = kind.into();
        let handler = Arc::new(handler);
        tokio::spawn(bulk_serve_task(ctx, kind, handler, cancel_rx, shutdown_rx));
        BulkServeHandle { _cancel: cancel_tx }
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

    /// Smoke-test the staging map without a real HTTP round-trip.
    #[tokio::test]
    async fn bulk_staging_insert_remove() {
        let port = alloc_port();
        let id   = NodeId::new("127.0.0.1", port).unwrap();
        let mut cfg = GossipConfig::default(); cfg.bind_port = port;
        let agent = Arc::new(GossipAgent::new(id, cfg));
        agent.start().await.unwrap();

        // Manually stage and retrieve a payload.
        let nonce: u64 = 42;
        agent.task_ctx.bulk_transport.staging.pin().insert(nonce, Bytes::from_static(b"test-payload"));
        let got = agent.bulk_staging_get(nonce);
        assert_eq!(got, Some(Bytes::from_static(b"test-payload")));

        // Simulate cleanup.
        agent.task_ctx.bulk_transport.staging.pin().remove(&nonce);
        assert!(agent.bulk_staging_get(nonce).is_none());

        agent.shutdown().await;
    }

    #[tokio::test]
    async fn bulk_call_returns_no_http_port_on_zero() {
        let port = alloc_port();
        let id   = NodeId::new("127.0.0.1", port).unwrap();
        let mut cfg = GossipConfig::default(); cfg.bind_port = port;
        let agent = Arc::new(GossipAgent::new(id.clone(), cfg));
        agent.start().await.unwrap();

        let ghost = NodeId::new("127.0.0.1", 19993).unwrap();
        let err = agent.bulk_call(ghost, "noop", Bytes::new(), Duration::from_millis(100)).await;
        assert_eq!(err, Err(BulkError::NoHttpPort));
        agent.shutdown().await;
    }
}
