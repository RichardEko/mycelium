//! MCP (Model Context Protocol) bridge — Layer 4 server role.
//!
//! Tools are registered under `tools/{name}/{node_id}` in the KV store so any
//! node can discover them via `scan_prefix("tools/")`. The HTTP `/mcp` endpoint
//! (Phase 2, in `http.rs`) scans this prefix for `tools/list` and routes
//! `tools/call` invocations to the provider node via `rpc_call`.

use crate::framing::{dispatch_gossip_send, make_gossip_update, ForwardHint, WireMessage};
use crate::signal::{Signal, SignalScope, signal_kind};
use crate::store::apply_and_notify;
use bytes::{BufMut, Bytes, BytesMut};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::warn;

use super::{emit_signal, GossipAgent, TaskCtx};

/// Handle returned by [`GossipAgent::register_mcp_tool`].
///
/// Dropping this handle tombstones the tool's KV entry (`tools/{name}/{node_id}`)
/// and stops the handler task. While the handle is live the tool is discoverable
/// via `scan_prefix("tools/")` and callable through the signal mesh or the
/// HTTP `/mcp` endpoint.
#[must_use = "dropping McpToolHandle immediately retracts the tool"]
pub struct McpToolHandle {
    _cancel: oneshot::Sender<()>,
}

/// Error type for MCP operations.
#[derive(Debug)]
pub enum McpError {
    /// Underlying RPC call timed out.
    Rpc(super::rpc::RpcError),
    /// JSON serialisation or deserialisation failed.
    SerdeJson(String),
    /// No provider found for the requested tool name.
    ToolNotFound(String),
    /// Tool handler returned an error.
    ToolError(String),
    /// Network or HTTP transport error.
    Transport(String),
}

impl std::fmt::Display for McpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            McpError::Rpc(e)             => write!(f, "rpc error: {e}"),
            McpError::SerdeJson(s)       => write!(f, "json error: {s}"),
            McpError::ToolNotFound(name) => write!(f, "tool not found: {name}"),
            McpError::ToolError(msg)     => write!(f, "tool error: {msg}"),
            McpError::Transport(msg)     => write!(f, "transport error: {msg}"),
        }
    }
}

impl std::error::Error for McpError {}

impl From<super::rpc::RpcError> for McpError {
    fn from(e: super::rpc::RpcError) -> Self {
        McpError::Rpc(e)
    }
}

impl GossipAgent {
    /// Registers an MCP tool on this node and returns a lifetime handle.
    ///
    /// Writes `schema` under `tools/{name}/{node_id}` in the KV store so any
    /// node in the cluster can discover the tool via `scan_prefix("tools/")`.
    /// Subscribes to incoming `"mcp.invoke"` signals and routes calls whose
    /// `params.name` matches `name` to `handler`.
    ///
    /// Drop the returned [`McpToolHandle`] to tombstone the KV entry and stop
    /// the handler task. The agent's shutdown sequence tombstones all live tools
    /// automatically.
    ///
    /// # Arguments
    ///
    /// * `name`    — tool name, unique per node for clean call demux.
    /// * `schema`  — MCP `inputSchema` JSON Schema object.
    /// * `handler` — async fn `(serde_json::Value) -> Result<serde_json::Value, String>`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let _handle = agent.register_mcp_tool(
    ///     "add",
    ///     serde_json::json!({
    ///         "type": "object",
    ///         "properties": {
    ///             "a": {"type": "number"},
    ///             "b": {"type": "number"},
    ///         },
    ///         "required": ["a", "b"],
    ///     }),
    ///     |args| async move {
    ///         let sum = args["a"].as_f64().unwrap_or(0.0)
    ///                 + args["b"].as_f64().unwrap_or(0.0);
    ///         Ok(serde_json::json!(sum))
    ///     },
    /// );
    /// ```
    #[must_use]
    pub fn register_mcp_tool<F, Fut>(
        &self,
        name:    impl Into<Arc<str>>,
        schema:  serde_json::Value,
        handler: F,
    ) -> McpToolHandle
    where
        F: Fn(serde_json::Value) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'static,
    {
        let name: Arc<str>   = name.into();
        let kv_key: Arc<str> = Arc::from(
            format!("tools/{}/{}", name, self.task_ctx.node_id).as_str(),
        );
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        let shutdown_rx = self.shutdown_tx.subscribe();
        let ctx         = Arc::clone(&self.task_ctx);
        let rx          = self.signal_rx(signal_kind::MCP_INVOKE);

        let _ = self.set(kv_key.clone(), schema.to_string().into_bytes());

        self.spawn_task(run_mcp_tool_task(
            ctx, cancel_rx, shutdown_rx, kv_key, name, rx, handler,
        ));
        McpToolHandle { _cancel: cancel_tx }
    }
}

async fn run_mcp_tool_task<F, Fut>(
    ctx:             Arc<TaskCtx>,
    mut cancel_rx:   oneshot::Receiver<()>,
    mut shutdown_rx: watch::Receiver<bool>,
    kv_key:          Arc<str>,
    tool_name:       Arc<str>,
    mut rx:          mpsc::Receiver<Signal>,
    handler:         F,
)
where
    F: Fn(serde_json::Value) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'static,
{
    loop {
        let req = tokio::select! { biased;
            _ = &mut cancel_rx => break,
            _ = await_shutdown(&mut shutdown_rx) => break,
            result = rx.recv() => match result {
                Some(req) => req,
                None => break,
            },
        };

        if req.payload.len() < 8 {
            warn!(tool = %tool_name, len = req.payload.len(), "mcp.invoke signal too short — skipping");
            continue;
        }

        let rpc_req: serde_json::Value = match serde_json::from_slice(&req.payload[8..]) {
            Ok(v)  => v,
            Err(e) => {
                warn!(tool = %tool_name, err = %e, "mcp.invoke: malformed JSON-RPC — skipping");
                continue;
            }
        };

        let req_name = rpc_req["params"]["name"].as_str().unwrap_or("");
        if req_name != tool_name.as_ref() {
            continue;
        }

        let args   = rpc_req["params"]["arguments"].clone();
        let result = handler(args).await;

        let response = match result {
            Ok(val) => json!({
                "jsonrpc": "2.0",
                "id": rpc_req["id"],
                "result": {"content": [{"type": "text", "text": val.to_string()}]},
            }),
            Err(msg) => json!({
                "jsonrpc": "2.0",
                "id": rpc_req["id"],
                "error": {"code": -32603, "message": msg},
            }),
        };

        let nonce = req.payload.slice(..8);
        let resp_bytes = Bytes::from(response.to_string().into_bytes());
        let mut buf = BytesMut::with_capacity(8 + resp_bytes.len());
        buf.put_slice(&nonce);
        buf.put(resp_bytes);
        emit_signal(
            &ctx,
            Arc::from(signal_kind::RPC_RESULT),
            SignalScope::Individual(req.sender.clone()),
            buf.freeze(),
        );
    }

    // Tombstone the KV entry whether we exited via cancel or shutdown.
    let tombstone = make_gossip_update(
        &ctx.node_id, ctx.default_ttl, kv_key, Bytes::new(), true, &ctx.hlc,
    );
    apply_and_notify(&ctx.kv_state, &tombstone);
    dispatch_gossip_send(
        &ctx.gossip_txs,
        WireMessage::Data(tombstone),
        ctx.node_id.id_hash(),
        ForwardHint::All,
    )
    .await;
}

async fn await_shutdown(rx: &mut watch::Receiver<bool>) {
    while !*rx.borrow() {
        if rx.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GossipAgent, GossipConfig, NodeId};
    use crate::signal::signal_kind;
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
    async fn test_register_and_call_tool_via_signal() {
        let (agent_a, agent_b) = agent_pair();
        agent_a.start().await.unwrap();
        agent_b.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let _handle = agent_b.register_mcp_tool(
            "add",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "a": {"type": "number"},
                    "b": {"type": "number"},
                },
                "required": ["a", "b"],
            }),
            |args| async move {
                let a = args["a"].as_f64().unwrap_or(0.0);
                let b = args["b"].as_f64().unwrap_or(0.0);
                Ok(serde_json::json!(a + b))
            },
        );

        let node_b = agent_b.node_id().clone();
        let rpc_req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "add", "arguments": {"a": 3.0, "b": 4.0}},
        });

        let reply = agent_a
            .rpc_call(
                node_b,
                signal_kind::MCP_INVOKE,
                Bytes::from(rpc_req.to_string().into_bytes()),
                Duration::from_secs(2),
            )
            .await
            .expect("rpc_call failed");

        let resp: serde_json::Value = serde_json::from_slice(&reply).expect("invalid JSON reply");
        assert_eq!(resp["result"]["content"][0]["type"], "text");
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        // json!(3.0 + 4.0) = json!(7.0) → "7.0"
        assert!(text.contains('7'), "expected sum '7' in text '{text}'");

        agent_a.shutdown().await;
        agent_b.shutdown().await;
    }

    #[tokio::test]
    async fn test_tool_handle_drop_tombstones_kv() {
        let port = alloc_port();
        let id   = NodeId::new("127.0.0.1", port).unwrap();
        let mut cfg = GossipConfig::default();
        cfg.bind_port = port;
        let agent = Arc::new(GossipAgent::new(id.clone(), cfg));
        agent.start().await.unwrap();

        let kv_key = format!("tools/ping/{id}");
        let handle = agent.register_mcp_tool(
            "ping",
            serde_json::json!({"type": "object", "properties": {}}),
            |_| async move { Ok(serde_json::json!("pong")) },
        );

        assert!(
            agent.get(&kv_key).is_some(),
            "tools/ping KV entry not found after registration"
        );

        drop(handle);
        // Allow the handler task to run and tombstone the entry.
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(
            agent.get(&kv_key).is_none(),
            "tools/ping KV entry not tombstoned after handle drop"
        );

        agent.shutdown().await;
    }

    #[tokio::test]
    async fn test_multiple_tools_same_node_demux() {
        let (agent_a, agent_b) = agent_pair();
        agent_a.start().await.unwrap();
        agent_b.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let schema = serde_json::json!({
            "type": "object",
            "properties": {"n": {"type": "number"}},
            "required": ["n"],
        });

        let _handle_double = agent_b.register_mcp_tool(
            "double",
            schema.clone(),
            |args| async move {
                let n = args["n"].as_f64().unwrap_or(0.0);
                Ok(serde_json::json!(n * 2.0))
            },
        );
        let _handle_negate = agent_b.register_mcp_tool(
            "negate",
            schema,
            |args| async move {
                let n = args["n"].as_f64().unwrap_or(0.0);
                Ok(serde_json::json!(-n))
            },
        );

        let node_b = agent_b.node_id().clone();

        // Call "double" with n=5 → expect 10.
        let req_double = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "double", "arguments": {"n": 5.0}},
        });
        let reply_double = agent_a
            .rpc_call(node_b.clone(), signal_kind::MCP_INVOKE,
                      Bytes::from(req_double.to_string().into_bytes()), Duration::from_secs(2))
            .await
            .expect("double call failed");
        let resp_double: serde_json::Value = serde_json::from_slice(&reply_double).unwrap();
        let text_double = resp_double["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text_double.contains("10"), "expected 10, got '{text_double}'");

        // Call "negate" with n=3 → expect -3.
        let req_negate = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {"name": "negate", "arguments": {"n": 3.0}},
        });
        let reply_negate = agent_a
            .rpc_call(node_b, signal_kind::MCP_INVOKE,
                      Bytes::from(req_negate.to_string().into_bytes()), Duration::from_secs(2))
            .await
            .expect("negate call failed");
        let resp_negate: serde_json::Value = serde_json::from_slice(&reply_negate).unwrap();
        let text_negate = resp_negate["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text_negate.contains("-3"), "expected -3, got '{text_negate}'");

        agent_a.shutdown().await;
        agent_b.shutdown().await;
    }
}
