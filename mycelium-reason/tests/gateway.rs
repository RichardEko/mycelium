//! The HTTP gateway edge: blob PUT → GET byte-identical, and the trace endpoint
//! serving a recorded run — the paths the Python LangGraph checkpointer uses over
//! the wire. Transport mirrors the wiki's gateway test (real agent + `http_port`,
//! routes mounted via `with_http_routes` before start).
#![cfg(all(feature = "gateway", feature = "llm"))]
#![allow(clippy::field_reassign_with_default)]

use std::sync::Arc;
use std::time::Duration;

use mycelium::{GossipAgent, GossipConfig, NodeId};
use mycelium_reason::{FsBlobStore, TraceRecorder, reason_router};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gateway_blob_roundtrip_and_trace_endpoint() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FsBlobStore::open(dir.path()).unwrap());

    // Retry fresh ports when a bind loses the bind-:0-then-drop TOCTOU race against
    // parallel test binaries (the AddrInUse CI flake class, 2026-07-07).
    let mut started = None;
    for _ in 0..16 {
        let base = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let http_port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();

        let mut cfg = GossipConfig::default();
        cfg.bind_port = base;
        cfg.http_port = Some(http_port);
        let agent = Arc::new(GossipAgent::new(NodeId::new("127.0.0.1", base).unwrap(), cfg));
        agent.with_http_routes(reason_router(Arc::clone(&agent), Arc::clone(&store)));
        if agent.start().await.is_ok() {
            started = Some((agent, http_port));
            break;
        }
    }
    let (agent, http_port) = started.expect("could not bind agent + gateway after 16 attempts");

    let url = format!("http://127.0.0.1:{http_port}");
    let http = reqwest::Client::new();
    for _ in 0..100 {
        if http.get(format!("{url}/health")).send().await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // PUT a blob → its content address comes back.
    let payload: Vec<u8> = (0..100_000u32).flat_map(|i| i.to_le_bytes()).collect();
    let resp: serde_json::Value = http
        .put(format!("{url}/gateway/reason/blob"))
        .body(payload.clone())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = resp["id"].as_str().expect("an id came back").to_string();

    // GET it back byte-identical, as an octet stream.
    let got = http.get(format!("{url}/gateway/reason/blob/{id}")).send().await.unwrap();
    assert_eq!(got.status().as_u16(), 200);
    assert_eq!(
        got.headers().get("content-type").and_then(|v| v.to_str().ok()),
        Some("application/octet-stream")
    );
    assert_eq!(got.bytes().await.unwrap().as_ref(), payload.as_slice());

    // Bad hex → 400; unknown id → 404.
    let bad = http.get(format!("{url}/gateway/reason/blob/nothex")).send().await.unwrap();
    assert_eq!(bad.status().as_u16(), 400);
    let missing = http
        .get(format!("{url}/gateway/reason/blob/{}", "0".repeat(64)))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status().as_u16(), 404);

    // Record a trace → the endpoint serves events + narrative. record() writes to the
    // local KV synchronously, but poll structurally anyway (parity with real replicas).
    let tr = TraceRecorder::new(Arc::clone(&agent), "gw-run");
    tr.tool_call("checkpoint-write", true);
    tr.resume("fable-mini", 250, &[agent.node_id().clone()]);

    let mut trace: Option<serde_json::Value> = None;
    for _ in 0..100 {
        let t: serde_json::Value = http
            .get(format!("{url}/gateway/reason/trace/gw-run"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if t["events"].as_array().map(Vec::len) == Some(2) {
            trace = Some(t);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let trace = trace.expect("both trace events served");
    assert_eq!(trace["run_id"].as_str(), Some("gw-run"));
    assert_eq!(trace["events"][0]["kind"].as_str(), Some("tool_call"));
    assert_eq!(trace["events"][1]["kind"].as_str(), Some("resume"));
    let narrative: Vec<String> = trace["narrative"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l.as_str().unwrap().to_string())
        .collect();
    assert_eq!(narrative.len(), 2);
    assert!(narrative[0].contains("tool checkpoint-write (ok)"));
    assert!(narrative[1].contains("resumed with fable-mini"));

    agent.shutdown_with_timeout(Duration::from_secs(5)).await;
}
