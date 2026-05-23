//! Embedded HTTP server — Layer 3 + Layer 4 gateway.
//!
//! ## Library-level endpoints
//! - `GET  /health`              — liveness probe
//! - `GET  /stats`               — KV store metrics
//! - `GET  /signals/{kind}`      — SSE stream of admitted signals
//! - `POST /mcp`                 — JSON-RPC 2.0 MCP protocol bridge
//!
//! ## Language-bridge gateway endpoints (`/gateway/*`)
//! These endpoints let Python/TypeScript agents participate in the mesh
//! without a Rust dependency. The gateway is the HTTP sidecar described in
//! the Layer 4 architecture.
//!
//! - `POST   /gateway/capability/advertise`    — advertise a capability; returns handle_id
//! - `DELETE /gateway/capability/{handle_id}`  — retract (tombstone) a capability
//! - `GET    /gateway/capability/resolve`      — filter-match with optional caller_id scoping
//! - `POST   /gateway/signal/emit`             — fire a signal into the mesh
//! - `GET    /gateway/signal/sse/{kind}`       — SSE stream for a signal kind
//! - `GET    /gateway/demand`                  — demand pressure for a capability filter
//! - `POST   /gateway/rpc/call`               — blocking RPC call to a named node
//!
//! Started when `GossipConfig::http_port` is `Some(port)`. Shuts down cleanly
//! when the agent's broadcast shutdown signal fires.

use axum::{
    Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Sse},
    response::sse::{Event, KeepAlive},
    routing::{delete, get, post},
};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::HashMap,
    convert::Infallible,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{oneshot, watch};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt as _;
use tracing::info;

use super::TaskCtx;

/// Shared state passed to every HTTP handler.
struct HttpCtx {
    agent_ctx:       Arc<TaskCtx>,
    /// Capability handle table for the language gateway.
    /// Key: opaque handle_id string returned to the caller.
    /// Value: cancel sender — dropping it tombstones the capability.
    gateway_caps:    Arc<Mutex<HashMap<String, oneshot::Sender<()>>>>,
    /// Shutdown receiver used when spawning gateway advertisement tasks.
    shutdown_rx:     watch::Receiver<bool>,
}

/// Starts the axum HTTP server on `addr`. Returns when the agent shuts down
/// (shutdown_rx fires) or if the listener fails to bind.
pub(super) async fn run_http_server(
    addr:        SocketAddr,
    ctx:         Arc<TaskCtx>,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<(), std::io::Error> {
    let state = Arc::new(HttpCtx {
        agent_ctx:    ctx,
        gateway_caps: Arc::new(Mutex::new(HashMap::new())),
        shutdown_rx:  shutdown_rx.clone(),
    });

    let app = Router::new()
        // ── Library endpoints ──────────────────────────────────────────────
        .route("/health",          get(health_handler))
        .route("/ready",           get(ready_handler))
        .route("/stats",           get(stats_handler))
        .route("/signals/{kind}",  get(signal_sse_handler))
        .route("/mcp",             post(mcp_handler))
        // ── Language-bridge gateway ────────────────────────────────────────
        .route("/gateway/capability/advertise",   post(gw_cap_advertise))
        .route("/gateway/capability/{handle_id}", delete(gw_cap_drop))
        .route("/gateway/capability/resolve",     get(gw_cap_resolve))
        .route("/gateway/signal/emit",            post(gw_signal_emit))
        .route("/gateway/signal/sse/{kind}",      get(gw_signal_sse))
        .route("/gateway/demand",                 get(gw_demand))
        .route("/gateway/rpc/call",               post(gw_rpc_call))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(addr = %listener.local_addr().unwrap(), "HTTP server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(shutdown_rx))
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))
}

async fn shutdown_signal(mut rx: watch::Receiver<bool>) {
    let _ = rx.wait_for(|v| *v).await;
}

// ── Handlers ─────────────────────────────────────────────────────────────────

async fn health_handler(State(ctx): State<Arc<HttpCtx>>) -> impl IntoResponse {
    Json(json!({
        "status":  "ok",
        "node_id": ctx.agent_ctx.node_id.to_string(),
    }))
}

/// Readiness probe: returns 200 once soft-state keys (capabilities, locality)
/// have been written to the local store after startup or restart.
/// Returns 503 while WAL replay is still pending or the first advertisement
/// tick has not yet fired.
///
/// Use `/health` for liveness; use `/ready` before sending traffic that
/// depends on accurate capability or membership state.
async fn ready_handler(State(ctx): State<Arc<HttpCtx>>) -> impl IntoResponse {
    if ctx.agent_ctx.caps_advertised.load(std::sync::atomic::Ordering::Acquire) {
        (StatusCode::OK, Json(json!({ "status": "ready", "node_id": ctx.agent_ctx.node_id.to_string() }))).into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(json!({ "status": "starting", "node_id": ctx.agent_ctx.node_id.to_string() }))).into_response()
    }
}

async fn stats_handler(State(ctx): State<Arc<HttpCtx>>) -> impl IntoResponse {
    let kv = &ctx.agent_ctx.kv_state;
    Json(json!({
        "node_id":      ctx.agent_ctx.node_id.to_string(),
        "store_entries": kv.store.pin().len(),
        "dropped_frames": kv.dropped_frames.load(std::sync::atomic::Ordering::Relaxed),
    }))
}

/// SSE endpoint — streams admitted signals of the requested `kind`.
///
/// Each event carries:
/// - `event` field: the signal kind
/// - `data` field: JSON `{"sender":"<node_id>","payload":"<base64>"}`
///
/// The subscription is torn down automatically when the client disconnects.
async fn signal_sse_handler(
    Path(kind):  Path<String>,
    State(ctx):  State<Arc<HttpCtx>>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let rx = ctx.agent_ctx.signal_handlers.register_with_capacity(
        std::sync::Arc::from(kind.as_str()),
        256,
    );

    let stream = ReceiverStream::new(rx).map(|sig: crate::signal::Signal| {
        use base64::Engine as _;
        let payload_b64 = base64::engine::general_purpose::STANDARD.encode(&sig.payload);
        let data = json!({
            "sender":  sig.sender.to_string(),
            "payload": payload_b64,
        });
        Ok(Event::default()
            .event(sig.kind.as_ref())
            .data(data.to_string()))
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// JSON-RPC 2.0 handler for the MCP protocol (`POST /mcp`).
///
/// Dispatches on `method`:
/// - `initialize`   — returns server capabilities.
/// - `tools/list`   — scans `tools/` prefix and returns registered tools.
/// - `tools/call`   — locates a provider and proxies the call via `rpc_call_ctx`.
async fn mcp_handler(
    State(ctx): State<Arc<HttpCtx>>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v)  => v,
        Err(_) => {
            return Json(json!({
                "jsonrpc": "2.0", "id": null,
                "error": {"code": -32700, "message": "parse error"},
            })).into_response();
        }
    };

    let id     = req.get("id").cloned().unwrap_or(serde_json::Value::Null);
    let method = req["method"].as_str().unwrap_or("");

    match method {
        "initialize" => Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {
                    "name": "mycelium",
                    "version": env!("CARGO_PKG_VERSION"),
                },
            },
        })).into_response(),

        "tools/list" => {
            let mut tool_map: std::collections::HashMap<String, serde_json::Value>
                = Default::default();
            for (key, bytes) in
                crate::store::scan_kv_prefix(&ctx.agent_ctx.kv_state, "tools/")
            {
                let rest = key.strip_prefix("tools/").unwrap_or_default();
                let Some((name, _node_id)) = rest.split_once('/') else { continue };
                if tool_map.contains_key(name) { continue; }
                let schema: serde_json::Value =
                    serde_json::from_slice(&bytes).unwrap_or(json!({}));
                tool_map.insert(name.to_string(), schema);
            }
            let tools: Vec<serde_json::Value> = tool_map.into_iter().map(|(name, schema)| {
                let description = schema.get("description")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let mut entry = json!({"name": name, "inputSchema": schema});
                if let Some(desc) = description {
                    entry["description"] = json!(desc);
                }
                entry
            }).collect();
            Json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {"tools": tools},
            })).into_response()
        }

        "tools/call" => {
            let name = req["params"]["name"].as_str().unwrap_or("").to_string();
            let arguments = req["params"]["arguments"].clone();

            if name.is_empty() {
                return Json(json!({
                    "jsonrpc": "2.0", "id": id,
                    "error": {"code": -32602, "message": "invalid params: missing tool name"},
                })).into_response();
            }

            let prefix = format!("tools/{name}/");
            let provider = crate::store::scan_kv_prefix(&ctx.agent_ctx.kv_state, &prefix)
                .into_iter()
                .find_map(|(key, _)| {
                    let rest = key.strip_prefix(&prefix)?;
                    rest.parse::<crate::node_id::NodeId>().ok()
                });

            let Some(provider_node_id) = provider else {
                return Json(json!({
                    "jsonrpc": "2.0", "id": id,
                    "error": {"code": -32601, "message": format!("tool not found: {name}")},
                })).into_response();
            };

            let tool_req = json!({
                "jsonrpc": "2.0",
                "id": req["id"],
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments},
            });

            match super::rpc::rpc_call_ctx(
                &ctx.agent_ctx,
                provider_node_id,
                std::sync::Arc::from(crate::signal::signal_kind::MCP_INVOKE),
                Bytes::from(tool_req.to_string().into_bytes()),
                Duration::from_secs(30),
            ).await {
                Ok(reply_bytes) => {
                    let resp: serde_json::Value = serde_json::from_slice(&reply_bytes)
                        .unwrap_or_else(|_| json!({
                            "jsonrpc": "2.0", "id": id,
                            "error": {"code": -32603, "message": "tool returned invalid JSON"},
                        }));
                    Json(resp).into_response()
                }
                Err(super::rpc::RpcError::Timeout) => Json(json!({
                    "jsonrpc": "2.0", "id": id,
                    "error": {"code": -32000, "message": "tool invocation timed out"},
                })).into_response(),
            }
        }

        _ => Json(json!({
            "jsonrpc": "2.0", "id": id,
            "error": {"code": -32601, "message": format!("method not found: {method}")},
        })).into_response(),
    }
}

// ── Language-bridge gateway handlers ─────────────────────────────────────────
//
// These seven endpoints form the HTTP sidecar API for Python/TypeScript agents.
// All inputs and outputs use JSON. Binary payloads are base64-encoded.

/// `POST /gateway/capability/advertise`
///
/// Advertises a capability on behalf of a language-bridge agent. The
/// returned `handle_id` must be supplied to `DELETE /gateway/capability/{id}`
/// to retract the advertisement (tombstone the KV entry).
///
/// Request body:
/// ```json
/// { "ns": "compute", "name": "gpu",
///   "interval_secs": 30,
///   "attributes": { "model": "A100" },
///   "authorized_callers": ["orchestrator"] }
/// ```
/// Response: `{ "handle_id": "<uuid>" }`
async fn gw_cap_advertise(
    State(ctx): State<Arc<HttpCtx>>,
    Json(body):  Json<serde_json::Value>,
) -> impl IntoResponse {
    use crate::capability::{Capability, CapValue};

    let ns   = match body["ns"].as_str()   { Some(s) => s.to_string(), None => return (StatusCode::BAD_REQUEST, Json(json!({"error":"missing ns"}))).into_response() };
    let name = match body["name"].as_str() { Some(s) => s.to_string(), None => return (StatusCode::BAD_REQUEST, Json(json!({"error":"missing name"}))).into_response() };
    let interval_secs = body["interval_secs"].as_u64().unwrap_or(30);

    let mut cap = Capability::new(ns.as_str(), name.as_str());

    if let Some(attrs) = body["attributes"].as_object() {
        for (k, v) in attrs {
            let cv = match v {
                serde_json::Value::String(s) => CapValue::Text(Arc::from(s.as_str())),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() { CapValue::Integer(i) }
                    else if let Some(f) = n.as_f64() { CapValue::Float(f) }
                    else { continue }
                }
                serde_json::Value::Bool(b) => CapValue::Bool(*b),
                _ => continue,
            };
            cap = cap.with(k.as_str(), cv);
        }
    }

    if let Some(callers) = body["authorized_callers"].as_array() {
        let list: Vec<Arc<str>> = callers.iter()
            .filter_map(|v| v.as_str())
            .map(Arc::from)
            .collect();
        cap = cap.with_authorized_callers(list);
    }

    let interval = Duration::from_secs(interval_secs.max(1));
    let kv_key: Arc<str> = Arc::from(
        format!("cap/{}/{}/{}", ctx.agent_ctx.node_id, cap.namespace, cap.name).as_str()
    );
    let cap_arc = Arc::new(cap);
    let payload_fn: super::kv::PersistPayloadFn = {
        let cap = Arc::clone(&cap_arc);
        Arc::new(move || cap.encode())
    };

    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
    let shutdown_rx = ctx.shutdown_rx.clone();
    tokio::spawn(super::kv::run_kv_persist_task(
        Arc::clone(&ctx.agent_ctx), cancel_rx, shutdown_rx, kv_key, interval, payload_fn, None,
    ));

    let handle_id = format!("{:x}", fastrand::u128(..));
    ctx.gateway_caps.lock().unwrap().insert(handle_id.clone(), cancel_tx);

    Json(json!({ "handle_id": handle_id })).into_response()
}

/// `DELETE /gateway/capability/{handle_id}`
///
/// Retracts a previously-advertised capability. Drops the cancel sender,
/// which causes the persist task to tombstone the KV entry.
async fn gw_cap_drop(
    Path(handle_id): Path<String>,
    State(ctx):      State<Arc<HttpCtx>>,
) -> impl IntoResponse {
    let removed = ctx.gateway_caps.lock().unwrap().remove(&handle_id).is_some();
    if removed {
        Json(json!({ "ok": true })).into_response()
    } else {
        (StatusCode::NOT_FOUND, Json(json!({ "error": "handle not found" }))).into_response()
    }
}

/// `GET /gateway/capability/resolve?ns=X&name=Y[&caller_id=Z]`
///
/// Snapshot filter-match over the local `cap/` KV view. If `caller_id` is
/// supplied, capabilities with non-empty `authorized_callers` are filtered
/// to only those that list the caller's identity.
#[derive(Deserialize)]
struct ResolveQuery {
    ns:        String,
    name:      String,
    caller_id: Option<String>,
}

async fn gw_cap_resolve(
    Query(q):   Query<ResolveQuery>,
    State(ctx): State<Arc<HttpCtx>>,
) -> impl IntoResponse {
    use crate::capability::{CallerContext, CapFilter, Capability};

    let filter     = CapFilter::new(q.ns.as_str(), q.name.as_str());
    let caller_ctx = match q.caller_id {
        Some(id) => CallerContext::for_caller(id.as_str()),
        None     => CallerContext::unrestricted(),
    };

    let mut results = Vec::new();
    for (key, bytes) in crate::store::scan_kv_prefix(&ctx.agent_ctx.kv_state, "cap/") {
        if super::capability_ops::is_cap_locality_key(&key) { continue; }
        let Some((node_id, _ns, _name)) =
            super::capability_ops::parse_cap_key_or_warn("cap/", &key)
            else { continue };
        let Some(cap) = Capability::decode(&bytes) else { continue };
        if filter.matches(&cap) && caller_ctx.can_see(&cap) {
            let attrs: serde_json::Map<String, serde_json::Value> = cap.attributes.iter()
                .map(|(k, v)| (k.as_ref().to_string(), capvalue_to_json(v)))
                .collect();
            results.push(json!({
                "node_id":    node_id.to_string(),
                "ns":         cap.namespace.as_ref(),
                "name":       cap.name.as_ref(),
                "attributes": attrs,
            }));
        }
    }

    Json(json!({ "providers": results })).into_response()
}

fn capvalue_to_json(v: &crate::capability::CapValue) -> serde_json::Value {
    use crate::capability::CapValue;
    match v {
        CapValue::Text(s)    => serde_json::Value::String(s.as_ref().to_string()),
        CapValue::Integer(n) => json!(n),
        CapValue::Float(f)   => json!(f),
        CapValue::Bool(b)    => json!(b),
        CapValue::Version(v) => serde_json::Value::String(format!("{}.{}.{}", v[0], v[1], v[2])),
    }
}

/// `POST /gateway/signal/emit`
///
/// Fires a signal into the mesh. `scope` is `"system"`, `"group:NAME"`, or
/// `"node:IP:PORT"`. `payload_b64` is the base64-encoded signal payload.
async fn gw_signal_emit(
    State(ctx): State<Arc<HttpCtx>>,
    Json(body):  Json<serde_json::Value>,
) -> impl IntoResponse {
    use base64::Engine as _;
    use crate::signal::SignalScope;

    let kind = match body["kind"].as_str() {
        Some(k) => Arc::from(k),
        None    => return (StatusCode::BAD_REQUEST, Json(json!({"error":"missing kind"}))).into_response(),
    };

    let scope_str = body["scope"].as_str().unwrap_or("system");
    let scope = if scope_str == "system" {
        SignalScope::System
    } else if let Some(rest) = scope_str.strip_prefix("group:") {
        SignalScope::Group(Arc::from(rest))
    } else if let Some(rest) = scope_str.strip_prefix("node:") {
        match rest.parse::<crate::node_id::NodeId>() {
            Ok(nid) => SignalScope::Individual(nid),
            Err(_)  => return (StatusCode::BAD_REQUEST, Json(json!({"error":"invalid node id"}))).into_response(),
        }
    } else {
        SignalScope::System
    };

    let payload = if let Some(b64) = body["payload_b64"].as_str() {
        match base64::engine::general_purpose::STANDARD.decode(b64) {
            Ok(v)  => Bytes::from(v),
            Err(_) => return (StatusCode::BAD_REQUEST, Json(json!({"error":"invalid base64 payload"}))).into_response(),
        }
    } else {
        Bytes::new()
    };

    // Same code path as GossipAgent::emit — local delivery + gossip fan-out
    let ok = super::helpers::emit_signal(&ctx.agent_ctx, kind, scope, payload);
    Json(json!({ "ok": ok })).into_response()
}

/// `GET /gateway/signal/sse/{kind}` — SSE stream of admitted signals for a kind.
///
/// Each event has `event: <kind>` and `data: {"sender":"…","payload_b64":"…","nonce":…}`.
async fn gw_signal_sse(
    Path(kind):  Path<String>,
    State(ctx):  State<Arc<HttpCtx>>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let rx = ctx.agent_ctx.signal_handlers.register_with_capacity(
        Arc::from(kind.as_str()),
        256,
    );

    let stream = ReceiverStream::new(rx).map(|sig: crate::signal::Signal| {
        use base64::Engine as _;
        let payload_b64 = base64::engine::general_purpose::STANDARD.encode(&sig.payload);
        let data = json!({
            "sender":      sig.sender.to_string(),
            "payload_b64": payload_b64,
            "nonce":       sig.nonce,
        });
        Ok(Event::default()
            .event(sig.kind.as_ref())
            .data(data.to_string()))
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// `GET /gateway/demand?ns=X&name=Y`
///
/// Returns the demand-pressure snapshot for a capability filter.
#[derive(Deserialize)]
struct DemandQuery { ns: String, name: String }

async fn gw_demand(
    Query(q):   Query<DemandQuery>,
    State(ctx): State<Arc<HttpCtx>>,
) -> impl IntoResponse {
    use crate::capability::CapFilter;

    let filter   = CapFilter::new(q.ns.as_str(), q.name.as_str());
    let kv       = &ctx.agent_ctx.kv_state;

    let providers = crate::store::scan_kv_prefix(kv, "cap/")
        .into_iter()
        .filter(|(k, v)| {
            if super::capability_ops::is_cap_locality_key(k) { return false; }
            crate::capability::Capability::decode(v)
                .map(|c| filter.matches(&c))
                .unwrap_or(false)
        })
        .count();

    let requirers = crate::store::scan_kv_prefix(kv, "req/")
        .into_iter()
        .filter(|(_, v)| {
            crate::capability::CapFilter::decode(v)
                .map(|f| f.namespace == filter.namespace && f.name == filter.name)
                .unwrap_or(false)
        })
        .count();

    let pressure = (requirers as f64) / (providers.max(1) as f64);

    Json(json!({
        "ns":              q.ns,
        "name":            q.name,
        "providers":       providers,
        "requirers":       requirers,
        "demand_pressure": pressure,
    })).into_response()
}

/// `POST /gateway/rpc/call`
///
/// Sends a blocking RPC call to a named node. `payload_b64` is base64.
/// Returns `{ "ok": true, "result_b64": "…" }` or `{ "ok": false, "error": "timeout" }`.
async fn gw_rpc_call(
    State(ctx): State<Arc<HttpCtx>>,
    Json(body):  Json<serde_json::Value>,
) -> impl IntoResponse {
    use base64::Engine as _;

    let target_str = match body["target"].as_str() {
        Some(s) => s.to_string(),
        None    => return (StatusCode::BAD_REQUEST, Json(json!({"error":"missing target"}))).into_response(),
    };
    let target: crate::node_id::NodeId = match target_str.parse() {
        Ok(n)  => n,
        Err(_) => return (StatusCode::BAD_REQUEST, Json(json!({"error":"invalid target node id"}))).into_response(),
    };

    let method = match body["method"].as_str() {
        Some(m) => Arc::from(m),
        None    => return (StatusCode::BAD_REQUEST, Json(json!({"error":"missing method"}))).into_response(),
    };

    let payload = if let Some(b64) = body["payload_b64"].as_str() {
        match base64::engine::general_purpose::STANDARD.decode(b64) {
            Ok(v)  => Bytes::from(v),
            Err(_) => return (StatusCode::BAD_REQUEST, Json(json!({"error":"invalid base64 payload"}))).into_response(),
        }
    } else {
        Bytes::new()
    };

    let timeout_secs = body["timeout_secs"].as_u64().unwrap_or(30);
    let timeout      = Duration::from_secs(timeout_secs.clamp(1, 300));

    match super::rpc::rpc_call_ctx(&ctx.agent_ctx, target, method, payload, timeout).await {
        Ok(result) => {
            let result_b64 = base64::engine::general_purpose::STANDARD.encode(&result);
            Json(json!({ "ok": true, "result_b64": result_b64 })).into_response()
        }
        Err(super::rpc::RpcError::Timeout) => {
            (StatusCode::GATEWAY_TIMEOUT, Json(json!({ "ok": false, "error": "timeout" }))).into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{GossipAgent, GossipConfig, NodeId};
    use std::{sync::Arc, time::Duration};

    fn alloc_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    #[tokio::test]
    async fn test_http_health_responds() {
        let gossip_port = alloc_port();
        let http_port   = alloc_port();

        let id  = NodeId::new("127.0.0.1", gossip_port).unwrap();
        let mut cfg = GossipConfig::default();
        cfg.bind_port  = gossip_port;
        cfg.http_port  = Some(http_port);
        cfg.http_addr  = "127.0.0.1".to_string();

        let agent = Arc::new(GossipAgent::new(id, cfg));
        agent.start().await.unwrap();
        // Brief pause for the HTTP server to bind and accept.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let url = format!("http://127.0.0.1:{http_port}/health");
        let resp = reqwest::get(&url).await.expect("HTTP request failed");
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "ok");
        assert!(body["node_id"].as_str().unwrap().contains("127.0.0.1"));

        agent.shutdown().await;
    }

    #[tokio::test]
    async fn test_http_stats_responds() {
        let gossip_port = alloc_port();
        let http_port   = alloc_port();

        let id  = NodeId::new("127.0.0.1", gossip_port).unwrap();
        let mut cfg = GossipConfig::default();
        cfg.bind_port = gossip_port;
        cfg.http_port = Some(http_port);

        let agent = Arc::new(GossipAgent::new(id, cfg));
        agent.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let url = format!("http://127.0.0.1:{http_port}/stats");
        let resp = reqwest::get(&url).await.expect("stats request failed");
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = resp.json().await.unwrap();
        assert!(body["store_entries"].is_number());
        assert!(body["dropped_frames"].is_number());

        agent.shutdown().await;
    }

    #[tokio::test]
    async fn test_sse_delivers_signals() {
        use crate::signal::SignalScope;
        use bytes::Bytes;

        let gossip_port = alloc_port();
        let http_port   = alloc_port();

        let id  = NodeId::new("127.0.0.1", gossip_port).unwrap();
        let mut cfg = GossipConfig::default();
        cfg.bind_port = gossip_port;
        cfg.http_port = Some(http_port);
        // Must be in the "test-sse" group to admit the signal.
        let agent = Arc::new(GossipAgent::new(id, cfg));
        agent.join_group("test-sse");
        agent.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Connect SSE client.
        let url = format!("http://127.0.0.1:{http_port}/signals/sse-probe");
        let mut resp = reqwest::Client::new()
            .get(&url)
            .send()
            .await
            .expect("SSE connect failed");
        assert_eq!(resp.status(), 200);

        // Emit a signal to self.
        let _ = agent.emit("sse-probe", SignalScope::System, Bytes::from_static(b"payload"));

        // Read SSE chunks until we see the expected event or timeout.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let mut found = false;
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(200), resp.chunk()).await {
                Ok(Ok(Some(chunk))) => {
                    let text = String::from_utf8_lossy(&chunk);
                    if text.contains("sse-probe") {
                        found = true;
                        break;
                    }
                }
                _ => break,
            }
        }
        assert!(found, "SSE event for 'sse-probe' was not received within timeout");

        agent.shutdown().await;
    }

    // ── MCP endpoint tests ────────────────────────────────────────────────────

    fn mcp_agent(http_port: u16) -> Arc<GossipAgent> {
        let gossip_port = alloc_port();
        let id = NodeId::new("127.0.0.1", gossip_port).unwrap();
        let mut cfg = GossipConfig::default();
        cfg.bind_port = gossip_port;
        cfg.http_port = Some(http_port);
        Arc::new(GossipAgent::new(id, cfg))
    }

    #[tokio::test]
    async fn test_mcp_initialize() {
        let http_port = alloc_port();
        let agent = mcp_agent(http_port);
        agent.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let resp = reqwest::Client::new()
            .post(format!("http://127.0.0.1:{http_port}/mcp"))
            .json(&serde_json::json!({
                "jsonrpc": "2.0", "id": 0, "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "test", "version": "1.0"},
                },
            }))
            .send()
            .await
            .expect("initialize request failed");

        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["result"]["protocolVersion"], "2024-11-05");
        assert!(body["result"]["serverInfo"]["name"].as_str().is_some());

        agent.shutdown().await;
    }

    #[tokio::test]
    async fn test_mcp_tools_list() {
        let http_port = alloc_port();
        let agent = mcp_agent(http_port);
        agent.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let _handle = agent.register_mcp_tool(
            "greet",
            serde_json::json!({
                "type": "object",
                "description": "Greets a person",
                "properties": {"name": {"type": "string"}},
                "required": ["name"],
            }),
            |args| async move {
                Ok(serde_json::json!(format!("hello, {}", args["name"].as_str().unwrap_or("?"))))
            },
        );

        let resp = reqwest::Client::new()
            .post(format!("http://127.0.0.1:{http_port}/mcp"))
            .json(&serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {},
            }))
            .send()
            .await
            .expect("tools/list request failed");

        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        let tools = body["result"]["tools"].as_array().unwrap();
        assert!(
            tools.iter().any(|t| t["name"] == "greet"),
            "tool 'greet' not in list: {body}"
        );

        agent.shutdown().await;
    }

    #[tokio::test]
    async fn test_mcp_tools_call_round_trip() {
        let http_port = alloc_port();
        let agent = mcp_agent(http_port);
        agent.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let _handle = agent.register_mcp_tool(
            "square",
            serde_json::json!({
                "type": "object",
                "properties": {"n": {"type": "number"}},
                "required": ["n"],
            }),
            |args| async move {
                let n = args["n"].as_f64().unwrap_or(0.0);
                Ok(serde_json::json!(n * n))
            },
        );

        let resp = reqwest::Client::new()
            .post(format!("http://127.0.0.1:{http_port}/mcp"))
            .json(&serde_json::json!({
                "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                "params": {"name": "square", "arguments": {"n": 5.0}},
            }))
            .send()
            .await
            .expect("tools/call request failed");

        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert!(body.get("error").is_none(), "unexpected error: {body}");
        let text = body["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("25"), "expected 25, got '{text}'");

        agent.shutdown().await;
    }

    #[tokio::test]
    async fn test_mcp_tools_call_not_found() {
        let http_port = alloc_port();
        let agent = mcp_agent(http_port);
        agent.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let resp = reqwest::Client::new()
            .post(format!("http://127.0.0.1:{http_port}/mcp"))
            .json(&serde_json::json!({
                "jsonrpc": "2.0", "id": 3, "method": "tools/call",
                "params": {"name": "no-such-tool", "arguments": {}},
            }))
            .send()
            .await
            .expect("tools/call request failed");

        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["error"]["code"], -32601);
        assert!(
            body["error"]["message"].as_str().unwrap().contains("no-such-tool"),
            "unexpected error message: {body}"
        );

        agent.shutdown().await;
    }
}
