//! MCP (Model Context Protocol) bridge — [`McpHandle`].
//!
//! Obtain a handle via [`GossipAgent::mcp()`](crate::GossipAgent::mcp).

use std::sync::Arc;
use bytes::Bytes;
use tokio::sync::oneshot;

use super::TaskCtx;
use super::helpers::kv_set;
use super::mcp::{McpToolHandle, run_mcp_tool_task};
#[cfg(feature = "gateway")]
use {
    serde_json::json,
    super::mcp::{McpError, McpClientHandle, run_mcp_client_task},
};
use crate::signal::signal_kind;

/// Domain handle for MCP (Model Context Protocol) tool registration and client bridging.
/// Obtained via [`GossipAgent::mcp()`](crate::GossipAgent::mcp).
///
/// Covers server-role tool registration (`register_mcp_tool`) and client-role
/// bridging of external MCP servers into the Mycelium mesh (`connect_mcp_server`).
///
/// The handle is `Clone + Send + Sync` and can be stored, moved across tasks,
/// or captured in closures.
#[derive(Clone)]
pub struct McpHandle {
    pub(crate) ctx: Arc<TaskCtx>,
}

impl McpHandle {
    /// Registers an MCP tool on this node and returns a lifetime handle.
    ///
    /// Writes `schema` under `tools/{name}/{node_id}` in the KV store so any
    /// node in the cluster can discover the tool via `scan_prefix("tools/")`.
    /// Subscribes to incoming `"mcp.invoke"` signals and routes calls whose
    /// `params.name` matches `name` to `handler`.
    ///
    /// Drop the returned [`McpToolHandle`] to tombstone the KV entry and stop
    /// the handler task.
    ///
    /// # Arguments
    ///
    /// * `name`    — tool name, unique per node for clean call demux.
    /// * `schema`  — MCP `inputSchema` JSON Schema object.
    /// * `handler` — async fn `(serde_json::Value) -> Result<serde_json::Value, String>`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use mycelium::{GossipAgent, GossipConfig, NodeId};
    /// # let agent = GossipAgent::new(NodeId::new("127.0.0.1", 7000).unwrap(), GossipConfig::default());
    /// let _handle = agent.mcp().register_mcp_tool(
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
            format!("tools/{}/{}", name, self.ctx.node_id).as_str(),
        );
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        let shutdown_rx = self.ctx.shutdown_tx.subscribe();
        let ctx         = Arc::clone(&self.ctx);
        let rx          = self.ctx.signal_handlers.register(Arc::from(signal_kind::MCP_INVOKE));

        kv_set(&self.ctx, Arc::clone(&kv_key), Bytes::from(schema.to_string().into_bytes()));

        self.ctx.spawn_task(run_mcp_tool_task(
            ctx, cancel_rx, shutdown_rx, kv_key, name, rx, handler,
        ));
        McpToolHandle { _cancel: cancel_tx }
    }

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
    /// discovery fails.
    #[cfg(feature = "gateway")]
    pub async fn connect_mcp_server(
        &self,
        server_url: impl Into<String>,
    ) -> Result<McpClientHandle, McpError> {
        let server_url  = server_url.into();
        // WS3 egress gate: a node-local allowlist may constrain outbound reach.
        // Enforced here at the canonical "twin reaches an external tool server"
        // boundary. Empty allowlist (default) permits all.
        if !self.ctx.config.egress.permits_url(&server_url) {
            return Err(McpError::Transport(format!(
                "egress denied by policy: {server_url}"
            )));
        }
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
        let node_id = self.ctx.node_id.clone();
        let mut kv_keys: Vec<Arc<str>>   = Vec::with_capacity(tools.len());
        let mut tool_names: Vec<Arc<str>> = Vec::with_capacity(tools.len());

        for tool in &tools {
            let name = match tool["name"].as_str() {
                Some(n) => n,
                None    => continue,
            };
            let schema = tool.get("inputSchema").cloned().unwrap_or(json!({}));
            let kv_key: Arc<str> = Arc::from(format!("tools/{name}/{node_id}").as_str());
            kv_set(&self.ctx, Arc::clone(&kv_key), Bytes::from(schema.to_string().into_bytes()));
            kv_keys.push(kv_key);
            tool_names.push(Arc::from(name));
        }

        if kv_keys.is_empty() {
            tracing::warn!(url = %server_url, "MCP server advertised no tools");
        }

        // ── Spawn proxy task ──────────────────────────────────────────────────
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        let shutdown_rx = self.ctx.shutdown_tx.subscribe();
        let ctx         = Arc::clone(&self.ctx);
        let rx          = self.ctx.signal_handlers.register(Arc::from(signal_kind::MCP_INVOKE));

        self.ctx.spawn_task(run_mcp_client_task(
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

