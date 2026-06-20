//! WS-G / M13 · G-G1c: the keyed two-stream rendezvous (fan-in join) drives across the HTTP
//! gateway — a consumer `take_by_key` parks until a producer `put` with the matching key arrives,
//! exactly the path the Python/TypeScript SDKs use over the wire.

#![cfg(feature = "gateway")]

use mycelium::{GossipAgent, GossipConfig, NodeId};
use mycelium_tuple_space::{TupleConfig, TupleRole, TupleSpace};
use std::sync::Arc;
use std::time::Duration;

fn b64(data: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(data)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyed_join_over_the_gateway() {
    let base: u16 = 23200 + (std::process::id() % 380) as u16 * 2;
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
    let ts = TupleSpace::new(
        Arc::clone(&agent),
        TupleConfig {
            namespace: Arc::from("join"),
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
    for _ in 0..100 {
        if http.get(format!("{url}/health")).send().await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // A consumer parks on take_by_key BEFORE the matching item exists (the join's blocking side).
    let url2 = url.clone();
    let http2 = http.clone();
    let consumer = tokio::spawn(async move {
        http2
            .post(format!("{url2}/gateway/tuple/take_by_key"))
            .json(&serde_json::json!({ "ns": "join", "stage": "po", "key": "inv-7", "timeout_secs": 8 }))
            .send()
            .await
            .expect("take_by_key request")
            .json::<serde_json::Value>()
            .await
            .expect("take_by_key json")
    });

    // Give the consumer a moment to park, then a NON-matching put (wrong key) must NOT satisfy it.
    tokio::time::sleep(Duration::from_millis(200)).await;
    http.post(format!("{url}/gateway/tuple/put"))
        .json(&serde_json::json!({ "ns": "join", "stage": "po", "key": "inv-OTHER", "payload_b64": b64(b"nope") }))
        .send().await.expect("decoy put");

    // The matching keyed put completes the rendezvous.
    let resp: serde_json::Value = http
        .post(format!("{url}/gateway/tuple/put"))
        .json(&serde_json::json!({ "ns": "join", "stage": "po", "key": "inv-7", "payload_b64": b64(b"purchase-order") }))
        .send().await.expect("matching put").json().await.expect("put json");
    let matched_id = resp["id"].as_u64().expect("id");

    let claimed = consumer.await.expect("consumer task");
    assert_eq!(claimed["id"].as_u64(), Some(matched_id), "the keyed take claims the matching item");
    assert_eq!(claimed["key"].as_str(), Some("inv-7"));
    assert_eq!(claimed["payload_b64"].as_str(), Some(b64(b"purchase-order").as_str()));

    // The decoy (wrong key) is still queued — claimable only by its own key.
    let decoy: serde_json::Value = http
        .post(format!("{url}/gateway/tuple/take_by_key"))
        .json(&serde_json::json!({ "ns": "join", "stage": "po", "key": "inv-OTHER", "timeout_secs": 3 }))
        .send().await.expect("decoy take").json().await.expect("decoy json");
    assert_eq!(decoy["payload_b64"].as_str(), Some(b64(b"nope").as_str()));

    ts.shutdown().await;
    agent.shutdown().await;
}
