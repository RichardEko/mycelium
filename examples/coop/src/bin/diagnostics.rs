//! Example 12 — **diagnosing an emergent condition** (the operator surface for Legible Emergence).
//!
//! The co-op's morning-rush coordinator publishes an intent: *keep the `rush-pool` of delivery
//! depots between 1 and 2 for the hand-off window*. But four depots are already serving the pool —
//! more than the intended cap. This is a benign **intent-vs-reality mismatch**, not a crisis: the
//! kind of drift that, on a coordinator-free fleet, an on-call volunteer would otherwise see only as
//! "the depot count looks off" with no signal *why*.
//!
//! Legible Emergence closes that gap. Every depot computes a **fleet diagnosis** from the gossiped
//! KV it already holds — no collector, no control plane. So we induce the mismatch on `depot-a` and
//! then ask a *different* depot, `depot-b`, "what's wrong?" — and it names the cause in plain terms
//! an operator can act on, purely from its own local view. Diagnostics **as data**
//! (`agent.fleet_diagnosis()` / `GET /gateway/diagnose`), the library-not-platform way.
//!
//!   1. Two depots form a mesh.
//!   2. The coordinator caps `rush-pool` at [1, 2] and four depots register in it (the mismatch).
//!   3. `depot-b` — which never saw the operator's action directly — diagnoses the conflict from its
//!      own flooded KV, names the group + band, and tells the operator what to do.
//!
//! Run:  cargo run -p mycelium-coop-examples --bin diagnostics

use std::time::Duration;

use coop::common::{alloc_ports, spawn_depot, DepotOpts};
use mycelium::MembershipIntent;

const GROUP: &str = "rush-pool";
const CAP_MIN: usize = 1;
const CAP_MAX: usize = 2;
const ACTIVE: usize = 4; // depots actually serving the pool — over the cap

async fn wait_until(secs: u64, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    while std::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    cond()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(tracing::Level::WARN).init();

    let cert_dir = std::env::temp_dir().join(format!("coop-diagnostics-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&cert_dir);
    let p = alloc_ports(4);

    // ── two depots form a mesh ──────────────────────────────────────────────────
    let a = spawn_depot(DepotOpts {
        name: "depot-a".into(),
        gossip_port: p[0], http_port: p[1],
        zone: "camden".into(),
        bootstrap: vec![],
        cert_dir: cert_dir.clone(),
        health_secs: Some(4),
    })
    .await?;
    let b = spawn_depot(DepotOpts {
        name: "depot-b".into(),
        gossip_port: p[2], http_port: p[3],
        zone: "hackney".into(),
        bootstrap: vec![p[0]],
        cert_dir: cert_dir.clone(),
        health_secs: Some(4),
    })
    .await?;
    println!("[{}] and [{}] up; forming mesh …", a.name, b.name);
    wait_until(20, || !a.agent.peers().is_empty() && !b.agent.peers().is_empty()).await;

    // ── induce the mismatch on depot-a: cap the pool, but ACTIVE depots serve it ─
    println!("\n[coordinator @ {}] intent: keep '{GROUP}' in [{CAP_MIN}, {CAP_MAX}] for the hand-off",
        a.name);
    assert!(a.agent.publish_membership_intent(MembershipIntent::new(GROUP, CAP_MIN, Some(CAP_MAX))),
        "intent published");
    for i in 0..ACTIVE {
        assert!(a.agent.kv().set(format!("grp/{GROUP}/127.0.0.1:{}", 27300 + i), "1"),
            "depot registered in the pool");
    }
    println!("[reality] {ACTIVE} depots are actually serving '{GROUP}' — over the cap of {CAP_MAX}");

    // ── depot-b diagnoses it from its OWN flooded KV — no collector, no control plane ──
    // Wait for the FULL picture to flood to depot-b: the intent and the `grp/` keys gossip
    // independently, and an intent with 0 members seen yet is *also* a (transient, under-min)
    // conflict — so we wait until depot-b observes all ACTIVE members, i.e. the intended over-cap
    // state, before reading the diagnosis.
    println!("\n[on-call @ {}] asking depot-b to diagnose the fleet (its local view only) …", b.name);
    let named = wait_until(25, || {
        b.agent.fleet_snapshot().governed_groups.iter()
            .any(|g| g.group == GROUP && g.observed == ACTIVE && g.conflict)
    })
    .await;

    let diagnosis = b.agent.fleet_diagnosis();
    println!("\n┌─ fleet diagnosis (observer: {}) ─────────────────────────", diagnosis.observer);
    println!("│ {}", diagnosis.summary);
    for f in &diagnosis.findings {
        println!("│ • [{:?}] {}", f.severity, f.cause);
    }
    if let Some(caveat) = &diagnosis.caveat {
        println!("│ {caveat}");
    }
    println!("└──────────────────────────────────────────────────────────");

    // ── assertions: the diagnosis names the cause, actionably, from a node that never saw it seeded ─
    assert!(named, "depot-b must diagnose the '{GROUP}' conflict from its own gossiped KV");
    let f = diagnosis.findings.iter()
        .find(|f| f.pathology.starts_with("governed_group") && f.cause.contains(GROUP))
        .expect("the governed-group conflict is named");
    assert!(f.cause.contains("Action:"), "the diagnosis is actionable: {}", f.cause);
    assert!(f.cause.contains(&format!("[{CAP_MIN}, {CAP_MAX}]")) && f.cause.contains(&ACTIVE.to_string()),
        "names the band and the observed count: {}", f.cause);

    println!("\nAll assertions passed — depot-b diagnosed the governed-group conflict on '{GROUP}' \
        from its own local view, with no collector and no control plane.");

    a.shutdown().await;
    b.shutdown().await;
    Ok(())
}
