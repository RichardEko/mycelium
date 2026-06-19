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
use crate::signal::Signal;
use super::rpc::{RpcRequest, rpc_respond_ctx};
use crate::store::apply_and_notify;
use bytes::Bytes;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::warn;

use super::TaskCtx;

/// Handle returned by [`McpHandle::register_mcp_tool`].
///
/// Dropping this handle tombstones the tool's KV entry (`tools/{name}/{node_id}`)
/// and stops the handler task. While the handle is live the tool is discoverable
/// via `scan_prefix("tools/")` and callable through the signal mesh or the
/// HTTP `/mcp` endpoint.
#[must_use = "dropping McpToolHandle immediately retracts the tool"]
pub struct McpToolHandle {
    pub(super) _cancel: oneshot::Sender<()>,
}

/// Error type for MCP operations.
#[non_exhaustive]
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


pub(super) async fn run_mcp_tool_task<F, Fut>(
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

        let req = RpcRequest::from(req);
        let rpc_req: serde_json::Value = match serde_json::from_slice(&req.payload()) {
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

        rpc_respond_ctx(&ctx, &req, Bytes::from(response.to_string().into_bytes()));
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

pub(super) async fn await_shutdown(rx: &mut watch::Receiver<bool>) {
    while !*rx.borrow() {
        if rx.changed().await.is_err() {
            return;
        }
    }
}

// ── MCP client role ───────────────────────────────────────────────────────────

/// Handle returned by [`McpHandle::connect_mcp_server`].
///
/// Dropping this handle tombstones all KV entries registered on behalf of the
/// remote server and stops the proxy task. While live, the remote server's
/// tools are visible in `scan_prefix("tools/")` and callable from anywhere in
/// the Mycelium cluster.
#[cfg(feature = "gateway")]
#[must_use = "dropping McpClientHandle immediately retracts all bridged tools"]
pub struct McpClientHandle {
    pub(super) _cancel: oneshot::Sender<()>,
}

#[cfg(feature = "gateway")]
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_mcp_client_task(
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

        let req = RpcRequest::from(req);
        let rpc_req: serde_json::Value = match serde_json::from_slice(&req.payload()) {
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

        rpc_respond_ctx(&ctx, &req, Bytes::from(response.to_string().into_bytes()));
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

    fn alloc_port() -> u16 { crate::test_util::alloc_port() }

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

        let _handle = agent_b.mcp().register_mcp_tool(
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
            .service().rpc_call(
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
        let handle = agent.mcp().register_mcp_tool(
            "ping",
            serde_json::json!({"type": "object", "properties": {}}),
            |_| async move { Ok(serde_json::json!("pong")) },
        );

        assert!(
            agent.kv().get(&kv_key).is_some(),
            "tools/ping KV entry not found after registration"
        );

        drop(handle);
        // Allow the handler task to run and tombstone the entry.
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(
            agent.kv().get(&kv_key).is_none(),
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

        let _handle_double = agent_b.mcp().register_mcp_tool(
            "double",
            schema.clone(),
            |args| async move {
                let n = args["n"].as_f64().unwrap_or(0.0);
                Ok(serde_json::json!(n * 2.0))
            },
        );
        let _handle_negate = agent_b.mcp().register_mcp_tool(
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
            .service().rpc_call(node_b.clone(), signal_kind::MCP_INVOKE,
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
            .service().rpc_call(node_b, signal_kind::MCP_INVOKE,
                      Bytes::from(req_negate.to_string().into_bytes()), Duration::from_secs(2))
            .await
            .expect("negate call failed");
        let resp_negate: serde_json::Value = serde_json::from_slice(&reply_negate).unwrap();
        let text_negate = resp_negate["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text_negate.contains("-3"), "expected -3, got '{text_negate}'");

        agent_a.shutdown().await;
        agent_b.shutdown().await;
    }

    // ── MCP client tests (Phase 3 — gateway feature required) ────────────────

    #[cfg(feature = "gateway")]
    /// Minimal in-process mock MCP server using axum.
    async fn spawn_mock_mcp_server(tools: Vec<serde_json::Value>) -> (u16, tokio::task::JoinHandle<()>) {
        use axum::{Router, extract::Json as AJson, routing::post};
        let tools = std::sync::Arc::new(tools);
        let app = Router::new().route("/", post({
            let tools = Arc::clone(&tools);
            move |AJson(req): AJson<serde_json::Value>| {
                let tools = Arc::clone(&tools);
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

    #[cfg(feature = "gateway")]
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
            .mcp().connect_mcp_server(server_url)
            .await
            .expect("connect_mcp_server failed");

        // The bridged tool should now appear in scan_prefix("tools/").
        let keys = agent.kv().scan_prefix("tools/");
        let found = keys.iter().any(|(k, _)| k.contains("remote-echo"));
        assert!(found, "remote-echo not in tools/ after connect: {:?}", keys);

        agent.shutdown().await;
    }

    /// WS3 egress gate: a non-empty `allow_hosts` that does not list the target
    /// host denies `connect_mcp_server` *before* any outbound request.
    #[cfg(feature = "gateway")]
    #[tokio::test]
    async fn test_mcp_egress_policy_denies_disallowed_host() {
        let port = alloc_port();
        let id   = NodeId::new("127.0.0.1", port).unwrap();
        let mut cfg = GossipConfig::default();
        cfg.bind_port = port;
        cfg.egress = crate::EgressPolicy { allow_hosts: vec!["allowed.example".into()] };
        let agent = Arc::new(GossipAgent::new(id, cfg));
        agent.start().await.unwrap();

        // 127.0.0.1 is not on the allowlist → denied (no server need exist).
        // `.err()` avoids requiring Debug on the Ok type (McpClientHandle).
        let err = agent
            .mcp().connect_mcp_server("http://127.0.0.1:9/")
            .await
            .err()
            .expect("egress policy must deny a disallowed host");
        match err {
            super::McpError::Transport(msg) => assert!(
                msg.contains("egress denied"),
                "expected egress-denied transport error, got: {msg}"
            ),
            _ => panic!("expected Transport(egress denied)"),
        }

        // A listed host passes the gate (the connect then fails on transport,
        // proving the gate did not block it — a different error).
        let err2 = agent
            .mcp().connect_mcp_server("http://allowed.example:9/")
            .await
            .err()
            .expect("unreachable host should fail at transport, not the gate");
        if let super::McpError::Transport(msg) = err2 {
            assert!(!msg.contains("egress denied"), "allowed host must pass the gate");
        }

        agent.shutdown().await;
    }

    #[cfg(feature = "gateway")]
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
            .mcp().connect_mcp_server(&server_url)
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
