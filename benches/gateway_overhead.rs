//! HTTP-gateway overhead baseline — the cost of reaching the substrate through the embedded
//! language-bridge gateway versus calling it in-process. This is the bench behind the deck's
//! gateway-overhead claim: the *overhead* is the delta between a `gateway/*` bar and its `direct/*`
//! twin (same KV op, one over loopback HTTP, one direct), and `gateway/health` is the pure-transport
//! floor (a route that touches no store). Loopback only — no network, no second node; it isolates the
//! axum + serde + reqwest round-trip, which is the honest thing a single-node "overhead" figure means.
//!
//! Run: `cargo bench --bench gateway_overhead --features gateway,test-util`
//!
//! The gateway is async and `agent.start()` spawns the server onto the runtime, so this uses
//! Criterion's `async_tokio` support (already enabled) — the agent, client, and base URL are built
//! once on a shared runtime; each iteration is one steady-state request (a single pooled
//! `reqwest::Client`, response body drained so the keep-alive connection is reused).

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use mycelium::{GossipAgent, GossipConfig, NodeId};
use tokio::runtime::Runtime;

/// A key/value pre-encoded as the gateway's JSON body shape (`value_b64` = base64 of `b"v"`).
const KV_KEY: &str = "bench:gw:key";
const KV_VAL: &[u8] = b"v";
const KV_VAL_B64: &str = "dg=="; // base64("v")

fn gateway_overhead(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Boot one gateway-enabled node on the runtime; hand back the pieces the bench bodies need.
    let (agent, base, client) = rt.block_on(async {
        let http_port = mycelium::alloc_port();
        let mut cfg = GossipConfig::default();
        cfg.bind_port = mycelium::alloc_port();
        cfg.http_port = Some(http_port);
        let agent = Arc::new(GossipAgent::new(
            NodeId::new("127.0.0.1", cfg.bind_port).unwrap(),
            cfg,
        ));
        agent.start().await.unwrap();
        // The server is a spawned task — give it a beat to bind before the first request.
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Seed the key so the GET path has something to read.
        let _ = agent.kv().set(KV_KEY, Bytes::from_static(KV_VAL));
        (agent, format!("http://127.0.0.1:{http_port}"), reqwest::Client::new())
    });

    let mut g = c.benchmark_group("gateway_overhead");

    // ── pure-transport floor: a route that does no store work ──────────────────────────────────
    g.bench_function("gateway/health", |b| {
        b.to_async(&rt).iter(|| async {
            let r = client.get(format!("{base}/health")).send().await.unwrap();
            let _ = r.bytes().await.unwrap(); // drain body → keep-alive connection reuse
        });
    });

    // ── KV set: gateway HTTP path vs the same call in-process ───────────────────────────────────
    g.bench_function("direct/kv_set", |b| {
        b.iter(|| {
            let _ = agent.kv().set(KV_KEY, Bytes::from_static(KV_VAL));
        });
    });
    g.bench_function("gateway/kv_set", |b| {
        b.to_async(&rt).iter(|| async {
            let r = client
                .post(format!("{base}/gateway/kv"))
                .json(&serde_json::json!({ "key": KV_KEY, "value_b64": KV_VAL_B64 }))
                .send()
                .await
                .unwrap();
            let _ = r.bytes().await.unwrap();
        });
    });

    // ── KV get: gateway HTTP path vs the same call in-process ───────────────────────────────────
    g.bench_function("direct/kv_get", |b| {
        b.iter(|| {
            let _ = agent.kv().get(KV_KEY);
        });
    });
    g.bench_function("gateway/kv_get", |b| {
        b.to_async(&rt).iter(|| async {
            let r = client
                .get(format!("{base}/gateway/kv"))
                .query(&[("key", KV_KEY)])
                .send()
                .await
                .unwrap();
            let _ = r.bytes().await.unwrap();
        });
    });

    g.finish();
    rt.block_on(async { agent.shutdown().await });
}

criterion_group!(benches, gateway_overhead);
criterion_main!(benches);
