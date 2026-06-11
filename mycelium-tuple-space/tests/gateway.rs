//! Phase 4: the HTTP gateway round-trips the full item lifecycle and the
//! /api/tuple aggregation reflects the metrics keys. Mirrors what the Python
//! and TypeScript SDKs do over the wire.

#![cfg(feature = "gateway")]

use mycelium::{GossipAgent, GossipConfig, NodeId};
use mycelium_tuple_space::{TupleConfig, TupleRole, TupleSpace};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gateway_roundtrip_and_api_tuple() {
    let base: u16 = 22400 + (std::process::id() % 400) as u16 * 2;
    let http_port = base + 1;

    let cfg = GossipConfig {
        bind_port: base,
        http_port: Some(http_port),
        health_check_max_jitter_ms: 50,
        ..Default::default()
    };
    let agent = Arc::new(GossipAgent::new(
        NodeId::new("127.0.0.1", base).expect("node id"),
        cfg,
    ));

    // Routes must be registered before agent.start(): construct the tuple
    // space first (it does not require a started agent), then start.
    let ts = TupleSpace::new(
        Arc::clone(&agent),
        TupleConfig {
            namespace: Arc::from("gw"),
            role: TupleRole::Primary,
            cap_refresh: Duration::from_millis(200),
            ..Default::default()
        },
    )
    .await
    .expect("tuple space");
    agent.with_http_routes(Arc::clone(&ts).http_router());
    agent.start().await.expect("agent start");

    let url = format!("http://127.0.0.1:{http_port}");
    let http = reqwest::Client::new();

    // Wait for the gateway to listen.
    for _ in 0..100 {
        if http.get(format!("{url}/health")).send().await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // put → returns id.
    let resp: serde_json::Value = http
        .post(format!("{url}/gateway/tuple/put"))
        .json(&serde_json::json!({
            "ns": "gw",
            "stage": "stage-a",
            "payload_b64": base64_encode(b"gateway payload"),
        }))
        .send()
        .await
        .expect("put request")
        .json()
        .await
        .expect("put json");
    let id = resp["id"].as_u64().expect("id");

    // take → same id, payload round-trips through base64.
    let resp: serde_json::Value = http
        .post(format!("{url}/gateway/tuple/take"))
        .json(&serde_json::json!({ "ns": "gw", "stage": "stage-a", "timeout_secs": 5 }))
        .send()
        .await
        .expect("take request")
        .json()
        .await
        .expect("take json");
    assert_eq!(resp["id"].as_u64(), Some(id));
    assert_eq!(
        resp["payload_b64"].as_str(),
        Some(base64_encode(b"gateway payload").as_str())
    );

    // complete → advances atomically.
    let resp: serde_json::Value = http
        .post(format!("{url}/gateway/tuple/complete"))
        .json(&serde_json::json!({
            "ns": "gw",
            "id": id,
            "next_stage": "stage-b",
            "next_payload_b64": base64_encode(b"advanced"),
        }))
        .send()
        .await
        .expect("complete request")
        .json()
        .await
        .expect("complete json");
    let next_id = resp["next_id"].as_u64().expect("next_id");
    assert_ne!(next_id, id);

    // depth shows stage-b holding one item.
    let resp: serde_json::Value = http
        .get(format!("{url}/gateway/tuple/depth?ns=gw&stage=stage-b"))
        .send()
        .await
        .expect("depth request")
        .json()
        .await
        .expect("depth json");
    assert_eq!(resp["stages"][0]["depth"].as_u64(), Some(1));

    // drain + terminal ack via gateway.
    let resp: serde_json::Value = http
        .post(format!("{url}/gateway/tuple/take"))
        .json(&serde_json::json!({ "ns": "gw", "stage": "stage-b", "timeout_secs": 5 }))
        .send()
        .await
        .expect("take b")
        .json()
        .await
        .expect("take b json");
    assert_eq!(resp["id"].as_u64(), Some(next_id));
    let resp = http
        .post(format!("{url}/gateway/tuple/ack"))
        .json(&serde_json::json!({ "ns": "gw", "id": next_id }))
        .send()
        .await
        .expect("ack request");
    assert!(resp.status().is_success());

    // double ack → 404.
    let resp = http
        .post(format!("{url}/gateway/tuple/ack"))
        .json(&serde_json::json!({ "ns": "gw", "id": next_id }))
        .send()
        .await
        .expect("double ack request");
    assert_eq!(resp.status().as_u16(), 404);

    // wrong namespace → 400.
    let resp = http
        .post(format!("{url}/gateway/tuple/ack"))
        .json(&serde_json::json!({ "ns": "other", "id": 1 }))
        .send()
        .await
        .expect("wrong ns request");
    assert_eq!(resp.status().as_u16(), 400);

    // /api/tuple aggregates the metrics keys (give the writer one cadence).
    let mut nodes_seen = 0;
    for _ in 0..50 {
        let resp: serde_json::Value = http
            .get(format!("{url}/api/tuple"))
            .send()
            .await
            .expect("api tuple")
            .json()
            .await
            .expect("api tuple json");
        let nodes = resp["nodes"].as_array().cloned().unwrap_or_default();
        nodes_seen = nodes.len();
        if nodes_seen == 1
            && nodes[0]["role"] == "primary"
            && nodes[0]["stages"].as_array().is_some_and(|s| s.len() == 2)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(nodes_seen, 1, "/api/tuple never aggregated the metrics keys");

    ts.shutdown().await;
    agent.shutdown().await;
}

fn base64_encode(data: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(data)
}
