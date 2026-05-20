//! Embedded HTTP server — Layer 3 gateway.
//!
//! Provides `/health`, `/stats`, and `/signals/{kind}` (SSE streaming) endpoints.
//! Started when `GossipConfig::http_port` is `Some(port)`. Shuts down cleanly
//! when the agent's broadcast shutdown signal fires.
//!
//! Layer 4 (MCP bridge, language gateway) will add routes to this router.

use axum::{
    Router,
    extract::{Path, State},
    response::{IntoResponse, Json, Sse},
    response::sse::{Event, KeepAlive},
    routing::get,
};
use serde_json::json;
use std::{convert::Infallible, net::SocketAddr, sync::Arc};
use tokio::sync::watch;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt as _;
use tracing::info;

use super::TaskCtx;

/// Shared state passed to every HTTP handler.
struct HttpCtx {
    agent_ctx: Arc<TaskCtx>,
}

/// Starts the axum HTTP server on `addr`. Returns when the agent shuts down
/// (shutdown_rx fires) or if the listener fails to bind.
pub(super) async fn run_http_server(
    addr:        SocketAddr,
    ctx:         Arc<TaskCtx>,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<(), std::io::Error> {
    let state = Arc::new(HttpCtx { agent_ctx: ctx });

    let app = Router::new()
        .route("/health",          get(health_handler))
        .route("/stats",           get(stats_handler))
        .route("/signals/{kind}",  get(signal_sse_handler))
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
}
