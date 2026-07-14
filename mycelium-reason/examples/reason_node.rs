//! The long-running reason node the Python CI drives — one gateway-carrying mesh node
//! with the full `/gateway/reason/*` surface plus an echo model, configured entirely
//! by environment so a CI step (or a local shell) can start two of them as a mesh:
//!
//! - `BIND_PORT`  (required) — gossip bind port; also the node id (`127.0.0.1:BIND_PORT`).
//! - `HTTP_PORT`  (required) — the embedded HTTP gateway port.
//! - `BLOB_DIR`   (required) — content-addressed blob directory ([`FsBlobStore`]).
//! - `BOOTSTRAP`  (optional) — `host:port` of a peer to join.
//! - `MODEL`      (optional) — model id to serve, default `fable-mini`.
//!
//! The node mounts [`reason_router`] (before `start` — `with_http_routes` ignores
//! routes registered after), serves blobs to peers ([`spawn_blob_server`]), and serves
//! `MODEL` as a prompt skill with [`EchoBackend`] and template `{{input}}` — so a call's
//! output is `echo: {input}`, which the Python `call_typed` test extracts JSON from.
//! Prints `reason node ready on <http_port>` once its own gateway answers `/health`,
//! then parks until SIGTERM / ctrl-c.
//!
//! Run: `BIND_PORT=7101 HTTP_PORT=8101 BLOB_DIR=/tmp/blobs-a \
//!       cargo run -p mycelium-reason --features llm,gateway --example reason_node`

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use mycelium::{EchoBackend, GossipAgent, GossipConfig, NodeId, PromptTemplate};
use mycelium_reason::{FsBlobStore, ModelProfile, reason_router, serve_model, spawn_blob_server};

/// A required env var, parsed; panics with a usable message when absent or malformed.
fn required<T: std::str::FromStr>(name: &str) -> T {
    let raw = std::env::var(name).unwrap_or_else(|_| panic!("{name} is required (see module doc)"));
    raw.parse().unwrap_or_else(|_| panic!("{name}={raw} did not parse"))
}

#[tokio::main]
async fn main() {
    let bind_port: u16 = required("BIND_PORT");
    let http_port: u16 = required("HTTP_PORT");
    let blob_dir: String = required("BLOB_DIR");
    let model = std::env::var("MODEL").unwrap_or_else(|_| "fable-mini".into());

    let bootstrap_peers = match std::env::var("BOOTSTRAP") {
        Ok(peer) => {
            let (host, port) = peer.rsplit_once(':').expect("BOOTSTRAP must be host:port");
            let port: u16 = port.parse().expect("BOOTSTRAP port did not parse");
            vec![NodeId::new(host, port).expect("BOOTSTRAP host:port invalid")]
        }
        Err(_) => Vec::new(),
    };
    let cfg = GossipConfig {
        bind_port,
        http_port: Some(http_port),
        bootstrap_peers,
        cluster_name: Some(std::env::var("GOSSIP_CLUSTER_NAME").unwrap_or_else(|_| "reason".to_string())),
        ..Default::default()
    };

    let agent = Arc::new(GossipAgent::new(
        NodeId::new("127.0.0.1", bind_port).expect("BIND_PORT invalid"),
        cfg,
    ));
    let store = Arc::new(FsBlobStore::open(&blob_dir).expect("BLOB_DIR must be creatable"));

    // Routes must be mounted BEFORE start — with_http_routes silently ignores late calls.
    agent.with_http_routes(reason_router(Arc::clone(&agent), Arc::clone(&store)));
    agent.start().await.expect("agent start failed");

    // Serve this node's blobs to peers (mesh fetch behind GET /gateway/reason/blob/{id})…
    let _blobs = spawn_blob_server(&agent, store);
    // …and the echo model as a prompt skill (capability `llm/{model}`).
    let template = PromptTemplate {
        system: "You are a deterministic echo used by integration tests.".into(),
        user_template: "{{input}}".into(),
        max_tokens: 512,
        temperature: 0.0,
        metadata: HashMap::new(),
    };
    let profile = ModelProfile {
        model: model.clone(),
        ctx_window: Some(8192),
        family: Some("echo".into()),
        extra: Vec::new(),
    };
    let _model = serve_model(&agent, profile, template, Arc::new(EchoBackend))
        .await
        .expect("serve_model failed");

    // Readiness: the printed marker means the gateway actually answers, not merely
    // that start() returned — the Python CI greps for it / polls /health.
    let health = format!("http://127.0.0.1:{http_port}/health");
    let http = reqwest::Client::new();
    for attempt in 0.. {
        if http.get(&health).send().await.is_ok_and(|r| r.status().is_success()) {
            break;
        }
        assert!(attempt < 100, "gateway never answered /health");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    println!("reason node ready on {http_port}");

    // Park until SIGTERM (CI cleanup) or ctrl-c (local shells).
    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("SIGTERM handler");
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await.ok();

    agent.shutdown_with_timeout(Duration::from_secs(5)).await;
    println!("reason node on {http_port}: shut down");
}
