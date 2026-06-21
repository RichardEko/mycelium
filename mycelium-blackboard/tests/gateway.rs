//! WS-G / G3 · Phase 4 (Gate G-G3.4): the post → read → claim → ack lifecycle, and the competitive
//! claim race, driven across the HTTP gateway — the path the Python/TS SDKs use over the wire.

#![cfg(feature = "gateway")]

use mycelium::{GossipAgent, GossipConfig, NodeId};
use mycelium_blackboard::{Blackboard, BoardConfig, BoardRole};
use std::sync::Arc;
use std::time::Duration;

fn b64(data: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(data)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gateway_claim_lifecycle_and_race() {
    let base = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let http_port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();

    let cfg = GossipConfig { bind_port: base, http_port: Some(http_port), health_check_max_jitter_ms: 50, ..Default::default() };
    let agent = Arc::new(GossipAgent::new(NodeId::new("127.0.0.1", base).unwrap(), cfg));

    let bb = Blackboard::new(
        Arc::clone(&agent),
        BoardConfig { namespace: Arc::from("gw"), role: BoardRole::Primary, cap_refresh: Duration::from_millis(200), ..Default::default() },
    )
    .await
    .unwrap();
    agent.with_http_routes(Arc::clone(&bb).http_router());
    agent.start().await.unwrap();

    let url = format!("http://127.0.0.1:{http_port}");
    let http = reqwest::Client::new();
    for _ in 0..100 {
        if http.get(format!("{url}/health")).send().await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // post → id.
    let resp: serde_json::Value = http
        .post(format!("{url}/gateway/bb/post"))
        .json(&serde_json::json!({ "ns": "gw", "attributes": { "kind": "surplus", "feeder": "4" }, "payload_b64": b64(b"3.2 kWh") }))
        .send().await.unwrap().json().await.unwrap();
    let id = resp["id"].as_u64().expect("id");

    // read → the fact is visible (non-destructive).
    let resp: serde_json::Value = http
        .post(format!("{url}/gateway/bb/read"))
        .json(&serde_json::json!({ "ns": "gw", "eq": { "kind": "surplus" } }))
        .send().await.unwrap().json().await.unwrap();
    assert_eq!(resp["facts"].as_array().map(|a| a.len()), Some(1));

    // claim → wins; second claim → empty (the competitive race).
    let c1: serde_json::Value = http
        .post(format!("{url}/gateway/bb/claim"))
        .json(&serde_json::json!({ "ns": "gw", "eq": { "kind": "surplus" } }))
        .send().await.unwrap().json().await.unwrap();
    assert_eq!(c1["claimed"], serde_json::json!(true));
    assert_eq!(c1["fact"]["id"].as_u64(), Some(id));
    assert_eq!(c1["fact"]["payload_b64"].as_str(), Some(b64(b"3.2 kWh").as_str()));

    let c2: serde_json::Value = http
        .post(format!("{url}/gateway/bb/claim"))
        .json(&serde_json::json!({ "ns": "gw", "eq": { "kind": "surplus" } }))
        .send().await.unwrap().json().await.unwrap();
    assert_eq!(c2["claimed"], serde_json::json!(false), "the second claimer gets nothing");

    // depth → 1 in-flight.
    let d: serde_json::Value = http.get(format!("{url}/gateway/bb/depth?ns=gw")).send().await.unwrap().json().await.unwrap();
    assert_eq!(d["inflight"].as_u64(), Some(1));

    // ack → terminal; claim now empty.
    let a = http.post(format!("{url}/gateway/bb/ack")).json(&serde_json::json!({ "ns": "gw", "id": id })).send().await.unwrap();
    assert!(a.status().is_success());
    let c3: serde_json::Value = http
        .post(format!("{url}/gateway/bb/claim"))
        .json(&serde_json::json!({ "ns": "gw", "eq": { "kind": "surplus" } }))
        .send().await.unwrap().json().await.unwrap();
    assert_eq!(c3["claimed"], serde_json::json!(false), "an acked fact does not re-serve");

    // double ack → 404.
    let dup = http.post(format!("{url}/gateway/bb/ack")).json(&serde_json::json!({ "ns": "gw", "id": id })).send().await.unwrap();
    assert_eq!(dup.status().as_u16(), 404);

    // wrong namespace → 400.
    let bad = http.post(format!("{url}/gateway/bb/ack")).json(&serde_json::json!({ "ns": "other", "id": 1 })).send().await.unwrap();
    assert_eq!(bad.status().as_u16(), 400);

    bb.shutdown().await;
    agent.shutdown().await;
}
