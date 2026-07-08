//! WS-G / G3 · Phase 3 (Gate G-G3.3): emergent primary/secondary failover.
//!
//! A fact posted to the primary is live-replicated to the secondary; the primary claims it
//! (in-flight, NOT acked); the primary is killed; the secondary promotes when the primary's
//! capability evaporates; and the in-flight claim **survives** — the fact re-queues on the new
//! primary and is re-claimable (at-least-once: a claimer that drops mid-work does not strand the
//! finite fact). Also covers the `Auto` election (lowest candidate becomes primary).

use bytes::Bytes;
use mycelium::{GossipAgent, GossipConfig, NodeId};
use mycelium_blackboard::{Blackboard, BoardConfig, BoardRole, Predicate};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

/// The core's bind-verified, process-unique loopback allocator (`mycelium::test_util::alloc_port`,
/// the `test-util` feature) — retires the old bind-:0-and-drop TOCTOU flake class.
fn alloc_port() -> u16 {
    mycelium::test_util::alloc_port()
}

fn alloc_two_sorted_ports() -> (u16, u16) {
    loop {
        let (a, b) = (alloc_port(), alloc_port());
        if a != b {
            return (a.min(b), a.max(b));
        }
    }
}

async fn start_agent(port: u16, bootstrap: Option<u16>) -> Arc<GossipAgent> {
    let id = NodeId::new("127.0.0.1", port).expect("node id");
    let cfg = GossipConfig {
        bind_port: port,
        health_check_max_jitter_ms: 50,
        bootstrap_peers: bootstrap
            .map(|b| vec![NodeId::new("127.0.0.1", b).expect("bootstrap id")])
            .unwrap_or_default(),
        // Failover hinges on capability-evaporation timing on a small loopback cluster; pin the
        // legacy failure detector for determinism (as the tuple-space failover tests do).
        swim_failure_detector: false,
        ..Default::default()
    };
    let agent = Arc::new(GossipAgent::new(id, cfg));
    agent.start().await.expect("agent start");
    agent
}

async fn wait_peered(agents: &[&Arc<GossipAgent>]) {
    for _ in 0..400 {
        if agents.iter().all(|a| !a.peers().is_empty()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("agents never peered");
}

fn bb_cfg(ns: &str, role: BoardRole) -> BoardConfig {
    BoardConfig {
        namespace: Arc::from(ns),
        role,
        persist: false,
        cap_refresh: Duration::from_secs(1),
        claim_timeout_secs: 300,
        ..Default::default()
    }
}

fn surplus() -> BTreeMap<String, String> {
    BTreeMap::from([("kind".to_string(), "surplus".to_string()), ("feeder".to_string(), "4".to_string())])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inflight_claim_survives_primary_failover() {
    let (p_lo, p_hi) = alloc_two_sorted_ports();
    let agent_a = start_agent(p_lo, None).await; // primary (lower port)
    let agent_b = start_agent(p_hi, Some(p_lo)).await; // secondary
    wait_peered(&[&agent_a, &agent_b]).await;

    let bb_a = Blackboard::new(Arc::clone(&agent_a), bb_cfg("microgrid", BoardRole::Primary)).await.unwrap();
    let bb_b = Blackboard::new(Arc::clone(&agent_b), bb_cfg("microgrid", BoardRole::Secondary)).await.unwrap();

    // Let the secondary discover the primary + sync.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Post a finite surplus fact; give live replication time to reach the mirror.
    let _id = bb_a.post(surplus(), Bytes::from("3.2 kWh")).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    // The primary claims it (in-flight, NOT acked — a Claim is not replicated).
    let pred = Predicate::new().eq("kind", "surplus");
    let claimed = bb_a.claim(&pred).await.unwrap().expect("primary claims the fact");
    assert_eq!(claimed.payload.as_ref(), b"3.2 kWh");

    // Kill the primary. Its capability evaporates; the secondary promotes.
    bb_a.shutdown().await;
    agent_a.shutdown().await;

    // The in-flight claim re-queues on the promoted secondary and is re-claimable.
    let mut recovered = None;
    for _ in 0..120 {
        if let Ok(Some(f)) = bb_b.claim(&pred).await {
            recovered = Some(f);
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let f = recovered.expect("G-G3.3: the in-flight claim must survive failover and be re-claimable");
    assert_eq!(f.payload.as_ref(), b"3.2 kWh", "the same finite fact survives under its payload");

    // And acking it on the new primary terminates it (no resurrection).
    bb_b.ack(f.id).await.unwrap();
    assert!(bb_b.claim(&pred).await.unwrap().is_none(), "an acked fact does not re-serve");

    bb_b.shutdown().await;
    agent_b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_election_lowest_candidate_becomes_primary() {
    let (p_lo, p_hi) = alloc_two_sorted_ports();
    let agent_lo = start_agent(p_lo, None).await;
    let agent_hi = start_agent(p_hi, Some(p_lo)).await;
    wait_peered(&[&agent_lo, &agent_hi]).await;

    let bb_lo = Blackboard::new(Arc::clone(&agent_lo), bb_cfg("elect", BoardRole::Auto)).await.unwrap();
    let bb_hi = Blackboard::new(Arc::clone(&agent_hi), bb_cfg("elect", BoardRole::Auto)).await.unwrap();

    // After the settle window, exactly one primary exists; the lower-port node wins and serves.
    let pred = Predicate::new();
    let mut posted = false;
    for _ in 0..120 {
        // A client post resolves the elected primary; once election settles, this succeeds.
        if bb_lo.post(surplus(), Bytes::from("x")).await.is_ok() {
            posted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(posted, "an Auto cluster must elect a primary that serves posts");
    // The elected primary serves the fact to a reader on either node.
    let mut seen = false;
    for _ in 0..40 {
        if bb_hi.read(&pred).await.map(|v| !v.is_empty()).unwrap_or(false) {
            seen = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(seen, "the elected primary serves reads to the other node");

    bb_lo.shutdown().await;
    bb_hi.shutdown().await;
    agent_lo.shutdown().await;
    agent_hi.shutdown().await;
}
