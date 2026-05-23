//! MCP (Model Context Protocol) bridge — Layer 4 server + client roles.
//!
//! **Server role** (`register_mcp_tool`): tools are registered under
//! `tools/{name}/{node_id}` in the KV store. The HTTP `/mcp` endpoint
//! (Phase 2, `http.rs`) scans this prefix for `tools/list` and routes
//! `tools/call` invocations to the provider via `rpc_call`.
//!
//! **Client role** (`connect_mcp_server`): bridges an external MCP server's
//! tools into the Mycelium mesh. Discovered tools are written to KV under the
//! same `tools/` namespace and calls are proxied outbound to the remote server.

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

// ── MCP client role ───────────────────────────────────────────────────────────

/// Handle returned by [`GossipAgent::connect_mcp_server`].
///
/// Dropping this handle tombstones all KV entries registered on behalf of the
/// remote server and stops the proxy task. While live, the remote server's
/// tools are visible in `scan_prefix("tools/")` and callable from anywhere in
/// the Mycelium cluster.
#[must_use = "dropping McpClientHandle immediately retracts all bridged tools"]
pub struct McpClientHandle {
    _cancel: oneshot::Sender<()>,
}

impl GossipAgent {
    /// Connects to an external MCP server at `server_url`, discovers its tools,
    /// and bridges them into the Mycelium mesh.
    ///
    /// 1. Sends `initialize` to handshake with the server.
    /// 2. Sends `tools/list` to enumerate available tools.
    /// 3. Writes each tool's schema under `tools/{name}/{node_id}` in the KV store.
    /// 4. Subscribes to `"mcp.invoke"` and proxies matching calls to the server
    ///    via HTTP `tools/call`.
    ///
    /// Drop the returned [`McpClientHandle`] to tombstone all KV entries and
    /// stop the proxy task.
    ///
    /// # Errors
    ///
    /// Returns `Err(McpError::Transport(...))` if the HTTP handshake or tool
    /// discovery fails. Individual call failures after connection are logged and
    /// reported as JSON-RPC errors to the caller.
    pub async fn connect_mcp_server(
        &self,
        server_url: impl Into<String>,
    ) -> Result<McpClientHandle, McpError> {
        let server_url = server_url.into();
        let http_client = reqwest::Client::new();

        // ── Handshake ─────────────────────────────────────────────────────────
        let init_req = json!({
            "jsonrpc": "2.0", "id": 0, "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "mycelium", "version": env!("CARGO_PKG_VERSION")},
            },
        });
        http_client
            .post(&server_url)
            .json(&init_req)
            .send()
            .await
            .map_err(|e| McpError::Transport(e.to_string()))?;

        // ── Tool discovery ────────────────────────────────────────────────────
        let list_req = json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}});
        let list_resp: serde_json::Value = http_client
            .post(&server_url)
            .json(&list_req)
            .send()
            .await
            .map_err(|e| McpError::Transport(e.to_string()))?
            .json()
            .await
            .map_err(|e| McpError::SerdeJson(e.to_string()))?;

        let tools = list_resp["result"]["tools"]
            .as_array()
            .cloned()
            .unwrap_or_default();

        // ── Register tool KV entries ──────────────────────────────────────────
        let node_id = self.task_ctx.node_id.clone();
        let mut kv_keys: Vec<Arc<str>> = Vec::with_capacity(tools.len());
        let mut tool_names: Vec<Arc<str>> = Vec::with_capacity(tools.len());

        for tool in &tools {
            let name = match tool["name"].as_str() {
                Some(n) => n,
                None    => continue,
            };
            let schema = tool.get("inputSchema").cloned().unwrap_or(json!({}));
            let kv_key: Arc<str> = Arc::from(format!("tools/{name}/{node_id}").as_str());
            let _ = self.set(kv_key.clone(), schema.to_string().into_bytes());
            kv_keys.push(kv_key);
            tool_names.push(Arc::from(name));
        }

        if kv_keys.is_empty() {
            tracing::warn!(url = %server_url, "MCP server advertised no tools");
        }

        // ── Spawn proxy task ──────────────────────────────────────────────────
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        let shutdown_rx = self.shutdown_tx.subscribe();
        let ctx         = Arc::clone(&self.task_ctx);
        let rx          = self.signal_rx(signal_kind::MCP_INVOKE);

        self.spawn_task(run_mcp_client_task(
            ctx,
            cancel_rx,
            shutdown_rx,
            kv_keys,
            tool_names,
            rx,
            server_url,
            http_client,
        ));

        Ok(McpClientHandle { _cancel: cancel_tx })
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_mcp_client_task(
    ctx:             Arc<TaskCtx>,
    mut cancel_rx:   oneshot::Receiver<()>,
    mut shutdown_rx: watch::Receiver<bool>,
    kv_keys:         Vec<Arc<str>>,
    tool_names:      Vec<Arc<str>>,
    mut rx:          mpsc::Receiver<Signal>,
    server_url:      String,
    http_client:     reqwest::Client,
) {
    loop {
        let req = tokio::select! { biased;
            _ = &mut cancel_rx => break,
            _ = await_shutdown(&mut shutdown_rx) => break,
            result = rx.recv() => match result {
                Some(req) => req,
                None => break,
            },
        };

        if req.payload.len() < 8 { continue; }

        let rpc_req: serde_json::Value = match serde_json::from_slice(&req.payload[8..]) {
            Ok(v)  => v,
            Err(_) => continue,
        };

        let req_name = rpc_req["params"]["name"].as_str().unwrap_or("");
        if !tool_names.iter().any(|n| n.as_ref() == req_name) {
            continue;
        }

        let arguments = rpc_req["params"]["arguments"].clone();
        let call_req  = json!({
            "jsonrpc": "2.0",
            "id": rpc_req["id"],
            "method": "tools/call",
            "params": {"name": req_name, "arguments": arguments},
        });

        let response: serde_json::Value = match http_client
            .post(&server_url)
            .json(&call_req)
            .send()
            .await
        {
            Ok(resp) => match resp.json().await {
                Ok(v)  => v,
                Err(e) => json!({"jsonrpc":"2.0","id":rpc_req["id"],
                                 "error":{"code":-32603,"message":e.to_string()}}),
            },
            Err(e) => json!({"jsonrpc":"2.0","id":rpc_req["id"],
                             "error":{"code":-32000,"message":e.to_string()}}),
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

    // Tombstone all KV entries for bridged tools.
    for kv_key in kv_keys {
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

    // ── MCP client tests (Phase 3) ────────────────────────────────────────────

    /// Minimal in-process mock MCP server using axum.
    async fn spawn_mock_mcp_server(tools: Vec<serde_json::Value>) -> (u16, tokio::task::JoinHandle<()>) {
        use axum::{Router, extract::Json as AJson, routing::post};
        let tools = std::sync::Arc::new(tools);
        let app = Router::new().route("/", post({
            let tools = tools.clone();
            move |AJson(req): AJson<serde_json::Value>| {
                let tools = tools.clone();
                async move {
                    let method = req["method"].as_str().unwrap_or("");
                    let id     = req["id"].clone();
                    let resp = match method {
                        "initialize" => serde_json::json!({
                            "jsonrpc": "2.0", "id": id,
                            "result": {
                                "protocolVersion": "2024-11-05",
                                "capabilities": {"tools": {}},
                                "serverInfo": {"name": "mock", "version": "0"},
                            },
                        }),
                        "tools/list" => serde_json::json!({
                            "jsonrpc": "2.0", "id": id,
                            "result": {"tools": *tools},
                        }),
                        "tools/call" => {
                            let name = req["params"]["name"].as_str().unwrap_or("unknown");
                            serde_json::json!({
                                "jsonrpc": "2.0", "id": id,
                                "result": {"content": [{"type": "text",
                                           "text": format!("echo:{name}")}]},
                            })
                        }
                        _ => serde_json::json!({
                            "jsonrpc": "2.0", "id": id,
                            "error": {"code": -32601, "message": "method not found"},
                        }),
                    };
                    AJson(resp)
                }
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (port, handle)
    }

    #[tokio::test]
    async fn test_mcp_client_discovers_tools() {
        let mock_tools = vec![
            serde_json::json!({
                "name": "remote-echo",
                "inputSchema": {"type": "object", "properties": {"msg": {"type": "string"}}},
            }),
        ];
        let (mock_port, _mock_server) = spawn_mock_mcp_server(mock_tools).await;

        let port = alloc_port();
        let id   = NodeId::new("127.0.0.1", port).unwrap();
        let mut cfg = GossipConfig::default();
        cfg.bind_port = port;
        let agent = Arc::new(GossipAgent::new(id.clone(), cfg));
        agent.start().await.unwrap();

        let server_url = format!("http://127.0.0.1:{mock_port}/");
        let _handle = agent
            .connect_mcp_server(server_url)
            .await
            .expect("connect_mcp_server failed");

        // The bridged tool should now appear in scan_prefix("tools/").
        let keys = agent.scan_prefix("tools/");
        let found = keys.iter().any(|(k, _)| k.contains("remote-echo"));
        assert!(found, "remote-echo not in tools/ after connect: {:?}", keys);

        agent.shutdown().await;
    }

    #[tokio::test]
    async fn test_mcp_client_proxies_call() {
        let mock_tools = vec![
            serde_json::json!({
                "name": "proxy-target",
                "inputSchema": {"type": "object", "properties": {}},
            }),
        ];
        let (mock_port, _mock_server) = spawn_mock_mcp_server(mock_tools).await;

        // Agent with HTTP enabled so we can drive the call via POST /mcp.
        let gossip_port = alloc_port();
        let http_port   = alloc_port();
        let id = NodeId::new("127.0.0.1", gossip_port).unwrap();
        let mut cfg = GossipConfig::default();
        cfg.bind_port = gossip_port;
        cfg.http_port = Some(http_port);
        let agent = Arc::new(GossipAgent::new(id, cfg));
        agent.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let server_url = format!("http://127.0.0.1:{mock_port}/");
        let _handle = agent
            .connect_mcp_server(&server_url)
            .await
            .expect("connect_mcp_server failed");

        // Call the bridged tool via the HTTP /mcp endpoint.
        let resp = reqwest::Client::new()
            .post(format!("http://127.0.0.1:{http_port}/mcp"))
            .json(&serde_json::json!({
                "jsonrpc": "2.0", "id": 99, "method": "tools/call",
                "params": {"name": "proxy-target", "arguments": {}},
            }))
            .send()
            .await
            .expect("tools/call to proxied tool failed");

        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert!(body.get("error").is_none(), "unexpected error: {body}");
        let text = body["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("proxy-target"), "expected echo, got '{text}'");

        agent.shutdown().await;
    }
}
