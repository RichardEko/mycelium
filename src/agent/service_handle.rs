//! Service operations — [`ServiceHandle`].
//!
//! Consolidates point-to-point communication primitives:
//! RPC, bulk transfer, scatter-gather, reliable delivery,
//! persistent mailboxes, and consistent-hash sharding.
//!
//! Obtain a handle via [`GossipAgent::service`](crate::GossipAgent::service).

use crate::capability::CapFilter;
use crate::node_id::NodeId;
use crate::signal::SignalScope;
use bytes::Bytes;
use std::{sync::Arc, time::Duration};
use tokio::task::JoinSet;

use super::TaskCtx;
use super::bulk::{BulkError, bulk_call_ctx};
#[cfg(feature = "gateway")]
use super::bulk::{BulkServeHandle, bulk_serve_task};
use super::capability_ops::resolve_filter_against_kv;
use super::helpers::emit_signal_async;
use super::mailbox::{deliver_event_ctx, open_mailbox_ctx, MailboxHandle, MeshEvent};
use super::rpc::{rpc_call_ctx, rpc_respond_ctx, RpcError, RpcRequest, RpcRequestRx};
use super::scatter::{ScatterError, ScatterResult};
use super::sharding::{shard_owner, ShardError};
use tokio::sync::mpsc;

/// Domain handle for service / communication operations. Obtained via [`GossipAgent::service()`].
///
/// Covers point-to-point RPC, bulk payload transfer, scatter-gather fan-out,
/// reliable delivery, persistent mailboxes, and consistent-hash sharding.
///
/// The handle is `Clone + Send + Sync` and can be stored, moved across tasks,
/// or captured in closures.
#[derive(Clone)]
pub struct ServiceHandle {
    pub(crate) ctx: Arc<TaskCtx>,
}

impl ServiceHandle {
    // ── RPC ──────────────────────────────────────────────────────────────────

    /// Sends a request to `target` and awaits a single reply.
    ///
    /// Prepends a random 8-byte correlation nonce to `payload`. The responder
    /// calls [`rpc_respond`](Self::rpc_respond) with the original request, which echoes the
    /// nonce back, and the nonce is used to route the reply to this caller.
    ///
    /// Returns `Ok(Bytes)` with the reply payload (nonce stripped), or
    /// `Err(RpcError::Timeout)` if no reply arrives within `timeout`.
    pub async fn rpc_call(
        &self,
        target:  NodeId,
        kind:    impl Into<Arc<str>>,
        payload: impl Into<Bytes>,
        timeout: Duration,
    ) -> Result<Bytes, RpcError> {
        rpc_call_ctx(&self.ctx, target, kind.into(), payload.into(), timeout).await
    }

    /// Sends a reply to an incoming RPC request.
    ///
    /// Echoes the correlation nonce from `request` back to the caller and emits
    /// `"rpc.result"` as `SignalScope::Individual(request.sender())`.
    pub fn rpc_respond(&self, request: &RpcRequest, result: impl Into<Bytes>) {
        rpc_respond_ctx(&self.ctx, request, result);
    }

    /// Returns a typed receiver for incoming RPC requests of `kind`.
    pub fn rpc_rx(&self, kind: impl Into<Arc<str>>) -> RpcRequestRx {
        RpcRequestRx(self.ctx.signal_handlers.register(kind.into()))
    }

    // ── Bulk ─────────────────────────────────────────────────────────────────

    /// Sends a large payload to `target` via HTTP staging rather than the signal mesh.
    ///
    /// Stages the payload locally and sends a lightweight ticket over the mesh;
    /// the target fetches the actual bytes directly from this node's HTTP server.
    pub async fn bulk_call(
        &self,
        target:  NodeId,
        kind:    impl Into<Arc<str>>,
        payload: impl Into<Bytes>,
        timeout: Duration,
    ) -> Result<Bytes, BulkError> {
        bulk_call_ctx(&self.ctx, target, kind.into(), payload.into(), timeout).await
    }

    /// Reads a staged bulk payload by nonce without removing it.
    ///
    /// Used by application HTTP handlers to serve `GET /bulk/{nonce}` requests.
    pub fn bulk_staging_get(&self, nonce: u64) -> Option<Bytes> {
        self.ctx.bulk_transport.get(nonce)
    }

    /// Overrides the HTTP port used when advertising staged bulk payloads.
    pub fn set_bulk_serving_port(&self, port: u16) {
        self.ctx.bulk_transport.set_http_port(port);
    }

    /// Registers a handler for incoming bulk calls of a given `kind`.
    ///
    /// Spawns a background task. The returned [`BulkServeHandle`] cancels the
    /// task when dropped.
    #[cfg(feature = "gateway")]
    pub fn bulk_serve<F, Fut>(
        &self,
        kind:    impl Into<Arc<str>>,
        handler: F,
    ) -> BulkServeHandle
    where
        F: Fn(NodeId, Bytes) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Bytes> + Send + 'static,
    {
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
        let ctx         = Arc::clone(&self.ctx);
        let shutdown_rx = self.ctx.shutdown_tx.subscribe();
        let kind: Arc<str> = kind.into();
        let handler = Arc::new(handler);
        tokio::spawn(bulk_serve_task(ctx, kind, handler, cancel_rx, shutdown_rx));
        BulkServeHandle { _cancel: cancel_tx }
    }

    // ── Scatter-gather ───────────────────────────────────────────────────────

    /// Fans out `payload` to every node in `targets` concurrently and collects replies.
    ///
    /// Returns as soon as `min_ok` successful replies arrive; remaining in-flight
    /// calls are cancelled. Returns `Err(ScatterError::InsufficientReplies)` when
    /// fewer than `min_ok` replies arrive before `timeout`.
    pub async fn scatter_gather(
        &self,
        targets:  Vec<NodeId>,
        kind:     impl Into<Arc<str>>,
        payload:  impl Into<Bytes>,
        timeout:  Duration,
        min_ok:   usize,
    ) -> Result<Vec<ScatterResult>, ScatterError> {
        let kind:    Arc<str> = kind.into();
        let payload: Bytes    = payload.into();
        let ctx = Arc::clone(&self.ctx);

        let mut js: JoinSet<(NodeId, Result<Bytes, RpcError>)> = JoinSet::new();
        for target in targets {
            let c = Arc::clone(&ctx);
            let k = Arc::clone(&kind);
            let p = payload.clone();
            let t = target.clone();
            js.spawn(async move {
                let res = rpc_call_ctx(&c, t.clone(), k, p, timeout).await;
                (t, res)
            });
        }

        let mut results = Vec::new();
        while let Some(join_res) = js.join_next().await {
            if let Ok((node_id, Ok(reply))) = join_res {
                results.push(ScatterResult { node_id, payload: reply });
                if results.len() >= min_ok {
                    js.abort_all();
                    break;
                }
            }
        }

        if results.len() >= min_ok {
            Ok(results)
        } else {
            Err(ScatterError::InsufficientReplies { got: results.len(), needed: min_ok })
        }
    }

    // ── Reliable delivery ────────────────────────────────────────────────────

    /// Sends `payload` to `target` and waits for an explicit ACK.
    ///
    /// The receiver calls `rpc_respond(&req, b"")` to acknowledge.
    /// Returns [`AckResult::Timeout`] if no ACK arrives within `timeout`.
    pub async fn emit_reliable(
        &self,
        target:  NodeId,
        kind:    impl Into<Arc<str>>,
        payload: impl Into<Bytes>,
        timeout: Duration,
    ) -> super::overlay_reliable::AckResult {
        use super::overlay_reliable::AckResult;
        match rpc_call_ctx(&self.ctx, target, kind.into(), payload.into(), timeout).await {
            Ok(_)                  => AckResult::Acknowledged,
            Err(RpcError::Timeout) => AckResult::Timeout,
        }
    }

    // ── Mailbox ──────────────────────────────────────────────────────────────

    /// Delivers an event to `target`'s mailbox.
    ///
    /// Writes to the gossip KV store at `mailbox/{target}/{kind}/{hlc}`.
    /// Returns `true` if the write was queued for gossip propagation.
    pub fn deliver_event(
        &self,
        target:  &NodeId,
        kind:    impl Into<Arc<str>>,
        payload: impl Into<Bytes>,
    ) -> bool {
        deliver_event_ctx(
            &self.ctx,
            &self.ctx.node_id,
            target,
            kind.into(),
            payload.into(),
        )
    }

    /// Opens a mailbox for events of `kind` addressed to this node.
    ///
    /// Spawns a background watcher task that drains events in HLC order into
    /// the returned `Receiver<MeshEvent>`. The [`MailboxHandle`] cancels the
    /// watcher on drop.
    pub fn open_mailbox(
        &self,
        kind:     impl Into<Arc<str>>,
        capacity: usize,
    ) -> (MailboxHandle, mpsc::Receiver<MeshEvent>) {
        let shutdown_rx = self.ctx.shutdown_tx.subscribe();
        open_mailbox_ctx(
            Arc::clone(&self.ctx),
            &self.ctx.node_id,
            kind.into(),
            capacity,
            shutdown_rx,
        )
    }

    // ── Sharding ─────────────────────────────────────────────────────────────

    /// Returns the deterministic shard owner for `shard_key` among providers
    /// matching `filter` in the local capability view.
    ///
    /// Uses a consistent-hash ring over `NodeId::id_hash()`.
    /// Returns `None` when no providers match the filter.
    pub fn shard_for(&self, shard_key: &str, filter: &CapFilter) -> Option<NodeId> {
        let providers = resolve_filter_against_kv(&self.ctx.kv_state, filter);
        shard_owner(shard_key, &providers)
    }

    /// Routes `payload` to the consistent-hash owner for `shard_key`.
    ///
    /// Resolves the shard owner then emits with `SignalScope::Individual(owner)`.
    /// Returns `Ok(owner_node_id)` on success, `Err(ShardError::NoProviders)` when
    /// the filter matches nothing.
    pub async fn emit_sharded(
        &self,
        kind:      impl Into<Arc<str>>,
        shard_key: &str,
        filter:    &CapFilter,
        payload:   impl Into<Bytes>,
    ) -> Result<NodeId, ShardError> {
        let owner = self.shard_for(shard_key, filter)
            .ok_or(ShardError::NoProviders)?;
        let _ = emit_signal_async(
            &self.ctx,
            kind.into(),
            SignalScope::Individual(owner.clone()),
            payload.into(),
        ).await;
        Ok(owner)
    }
}
