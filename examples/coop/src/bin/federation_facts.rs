//! Example 05 — **cross-domain discovery via self-certified AgentFacts** (WS-F federation).
//!
//! Two **separate domains** (separate clusters, separate auto-CAs — they do *not* peer): our
//! food-rescue co-op, and a neighbouring co-op with overflow it can't route. The neighbour
//! discovers our `route/optimize` capability the way a NANDA-style quilt does — it **pulls our
//! AgentFacts at the edge** (`/.well-known/agent-facts.json`), a self-signed JSON-LD document, and
//! **verifies the signature itself**. There is no shared trust authority: the facts are
//! self-certified by our node identity, and trust is the *fetcher's* decision (Core Principle 1).
//!
//!   • domain A (`coop-a`) — advertises `route/optimize`; serves signed AgentFacts at its edge.
//!   • domain B (`coop-b`) — a separate cluster; fetches A's edge doc, verifies it, reads the
//!     capability list, and decides to route overflow to A.
//!
//! Run:  cargo run -p mycelium-coop-examples --bin federation_facts

use std::time::Duration;

use coop::common::{alloc_ports, spawn_depot, DepotOpts};
use mycelium::Capability;
use serde_json::Value;

async fn wait_until(secs: u64, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    while std::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    cond()
}

/// Reconstruct a `SignedFacts` from a served edge document — exactly what an independent NANDA-quilt
/// puller does (the lib type round-trips through JSON). The caller checks `.verify()`.
fn parse_signed_facts(body: &str) -> Option<mycelium_agentfacts::SignedFacts> {
    let v: Value = serde_json::from_str(body).ok()?;
    Some(mycelium_agentfacts::SignedFacts {
        document: v.get("document")?.clone(),
        alg: "ed25519",
        public_key_b64: v.get("public_key_b64")?.as_str()?.to_string(),
        signature_b64: v.get("signature_b64")?.as_str()?.to_string(),
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(tracing::Level::WARN).init();

    // Two domains ⇒ two separate cert dirs (separate CAs) so the clusters do NOT peer.
    let cert_a = std::env::temp_dir().join(format!("coop-fed-a-{}", std::process::id()));
    let cert_b = std::env::temp_dir().join(format!("coop-fed-b-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&cert_a);
    let _ = std::fs::remove_dir_all(&cert_b);
    let p = alloc_ports(4);

    // ── domain A: advertises route/optimize, serves signed AgentFacts at its edge ──
    let coop_a = spawn_depot(DepotOpts {
        name: "coop-a".into(), gossip_port: p[0], http_port: p[1],
        zone: "southwark".into(), bootstrap: vec![], cert_dir: cert_a.clone(), health_secs: None,
    }).await?;
    let _cap = coop_a.agent.capabilities()
        .advertise_capability(Capability::new("route", "optimize"), Duration::from_secs(30));
    let a_facts_url = format!("http://127.0.0.1:{}/.well-known/agent-facts.json", coop_a.http_port);
    println!("[coop-a] up — advertises route/optimize; AgentFacts at {a_facts_url}");

    // ── domain B: a separate cluster (does not peer with A) ─────────────────────
    let coop_b = spawn_depot(DepotOpts {
        name: "coop-b".into(), gossip_port: p[2], http_port: p[3],
        zone: "camden".into(), bootstrap: vec![], cert_dir: cert_b.clone(), health_secs: None,
    }).await?;
    println!("[coop-b] up — a separate domain with overflow to route");

    // A's own capability must be in its local view before the facts can report it.
    let cap_local = wait_until(15, || {
        !coop_a.agent.kv().scan_prefix(&format!("cap/{}/route/", coop_a.node_id())).is_empty()
    }).await;
    assert!(cap_local, "coop-a's own route/optimize must land in its local view");

    // ── B pulls A's edge AgentFacts and verifies the self-signature ─────────────
    // Generous budget: the edge fetch can race A's gateway startup + the capability reaching the
    // facts builder; the loop tolerates connection errors, non-200s, and empty-capability docs.
    let client = reqwest::Client::new();
    let mut signed = None;
    for _ in 0..150 {
        if let Ok(resp) = client.get(&a_facts_url).send().await
            && resp.status().is_success()
            && let Ok(body) = resp.text().await
            && let Some(sf) = parse_signed_facts(&body)
            && !sf.document["capabilities"].as_array().map(|a| a.is_empty()).unwrap_or(true)
        {
            signed = Some(sf);
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let signed = signed.expect("coop-b fetched coop-a's AgentFacts (with its capability)");
    assert!(signed.verify(), "[coop-b] A's self-signed AgentFacts must verify against its embedded key");
    println!("[coop-b] fetched coop-a's AgentFacts and verified the self-signature ✓ (no shared CA)");

    // ── B reads the capability list and decides to route overflow to A ──────────
    let doc = &signed.document;
    let caps = doc["capabilities"].as_array().cloned().unwrap_or_default();
    let cap_ids: Vec<String> = caps.iter().filter_map(|c| c["id"].as_str().map(String::from)).collect();
    println!("[coop-b] coop-a advertises: {cap_ids:?}");
    assert!(caps.iter().any(|c| c["id"] == "route/optimize"), "verified facts must list route/optimize");
    assert_eq!(doc["certification"]["scheme"], "self-certified", "self-certified, no issuer authority");
    assert_eq!(doc["jurisdiction"], "southwark", "facts carry A's jurisdiction");
    println!("[coop-b] → routing overflow to coop-a for route/optimize (discovered + verified across domains)");

    // ── a tampered document must fail verification (detection-not-prevention) ───
    let mut forged = signed.clone();
    forged.document["jurisdiction"] = serde_json::json!("forged-zone");
    assert!(!forged.verify(), "a tampered AgentFacts document must fail verification");
    println!("[coop-b] a tampered copy of the document fails verification ✓");

    println!("\nAll assertions passed — cross-domain capability discovery via self-certified AgentFacts, no shared trust authority.");

    coop_b.shutdown().await;
    coop_a.shutdown().await;
    let _ = std::fs::remove_dir_all(&cert_a);
    let _ = std::fs::remove_dir_all(&cert_b);
    Ok(())
}
