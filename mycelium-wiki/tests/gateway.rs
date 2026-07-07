//! Phase 4 · the HTTP gateway: the propose → (curator applies) → read/query lifecycle driven across
//! `/gateway/wiki/*` — the path the Python/TS `WikiClient`s use over the wire.
#![cfg(feature = "gateway")]
#![allow(clippy::field_reassign_with_default)]

use std::sync::Arc;
use std::time::Duration;

use mycelium::{GossipAgent, GossipConfig, NodeId};
use mycelium_wiki::{FsStore, Wiki, WikiConfig, WikiRole};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gateway_propose_apply_read_query_lifecycle() {
    let dir = tempfile::tempdir().unwrap();

    // Retry fresh ports when a bind loses the bind-:0-then-drop TOCTOU race against parallel
    // test binaries (the AddrInUse CI flake class, 2026-07-07). The wiki is rebuilt per
    // attempt; a failed attempt's wiki is shut down to reclaim its tasks (the Run-32 lesson).
    let mut started = None;
    for _ in 0..16 {
        let base = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let http_port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();

        let mut cfg = GossipConfig::default();
        cfg.bind_port = base;
        cfg.http_port = Some(http_port);
        let agent = Arc::new(GossipAgent::new(NodeId::new("127.0.0.1", base).unwrap(), cfg));

        let store = Arc::new(FsStore::open(dir.path(), "council").unwrap());
        let wcfg = WikiConfig {
            group: "council".into(), role: WikiRole::Curator,
            cap_refresh: Duration::from_millis(300), drain_interval: Duration::from_millis(100),
            lint_interval: Duration::from_secs(5),
        };
        let wiki = Wiki::new(Arc::clone(&agent), wcfg, store).await;
        agent.with_http_routes(Arc::clone(&wiki).http_router());
        if agent.start().await.is_ok() {
            started = Some((agent, wiki, http_port));
            break;
        }
        wiki.shutdown().await;
    }
    let (agent, _wiki, http_port) =
        started.expect("could not bind agent + gateway after 16 attempts");

    let url = format!("http://127.0.0.1:{http_port}");
    let http = reqwest::Client::new();
    for _ in 0..100 {
        if http.get(format!("{url}/health")).send().await.is_ok() { break; }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // propose → returns a proposal key + the minted section id.
    let resp: serde_json::Value = http
        .post(format!("{url}/gateway/wiki/propose"))
        .json(&serde_json::json!({
            "group": "council", "page": "decisions/elm-street",
            "heading": "Resolution 2026-14", "body": "protected bike lane approved",
            "attributes": { "topic": "transport" },
        }))
        .send().await.unwrap().json().await.unwrap();
    assert!(resp["proposal"].as_str().is_some(), "a proposal key came back");
    assert!(resp["section"].as_str().is_some(), "a section id was minted");

    // The curator applies it; read via HTTP eventually shows the section (poll — structural, generous).
    let mut body = None;
    for _ in 0..100 {
        let r: serde_json::Value = http
            .post(format!("{url}/gateway/wiki/read"))
            .json(&serde_json::json!({ "group": "council", "page": "decisions/elm-street" }))
            .send().await.unwrap().json().await.unwrap();
        if let Some(b) = r["page"]["sections"].get(0).and_then(|s| s["body"].as_str()) {
            body = Some(b.to_string());
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(body.as_deref(), Some("protected bike lane approved"), "the curator applied the HTTP propose");

    // query by attribute → the section ref comes back.
    let q: serde_json::Value = http
        .post(format!("{url}/gateway/wiki/query"))
        .json(&serde_json::json!({ "group": "council", "equals": { "topic": "transport" } }))
        .send().await.unwrap().json().await.unwrap();
    assert_eq!(q["hits"].as_array().map(|a| a.len()), Some(1));
    assert_eq!(q["hits"][0]["page"].as_str(), Some("decisions/elm-street"));

    // read of an absent page → {"page": null}.
    let miss: serde_json::Value = http
        .post(format!("{url}/gateway/wiki/read"))
        .json(&serde_json::json!({ "group": "council", "page": "nope" }))
        .send().await.unwrap().json().await.unwrap();
    assert!(miss["page"].is_null());

    // wrong group → 400.
    let bad = http
        .post(format!("{url}/gateway/wiki/read"))
        .json(&serde_json::json!({ "group": "other", "page": "x" }))
        .send().await.unwrap();
    assert_eq!(bad.status().as_u16(), 400);

    agent.shutdown_with_timeout(Duration::from_secs(5)).await;
}
