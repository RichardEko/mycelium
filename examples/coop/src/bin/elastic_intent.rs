//! Example 03 — **elastic sizing as evaporating intent** (management = intent + local reconcile).
//!
//! An operator declares "keep the `rush-pool` between MIN and MAX depots online for the morning
//! rush" by publishing a `MembershipIntent` — gossiped, **evaporating soft-state, not a command**.
//! Candidate depots run a `MembershipGovernor` and **self-elect** so the live member count
//! converges into the band (a *subset* of the eligible depots — no controller picks who).
//!
//!   1. Converge: with MIN..=MAX and N candidates, the pool holds a subset in the band.
//!   2. Operator vanishes: the intent persists in gossip (within its TTL), so the cluster keeps
//!      running on it — the band still holds with no operator present.
//!   3. Self-heal: kill a pool member; the governors notice the deficit and re-elect to restore MIN.
//!
//! No coordinator, no barrier — each node acts on local information. (When the intent finally
//! evaporates, membership reverts to emergent/un-bounded — the litmus: *if management vanishes,
//! the cluster keeps working*.)
//!
//! Run:  cargo run -p mycelium-coop-examples --bin elastic_intent

use std::time::Duration;

use coop::common::{alloc_ports, spawn_depot, Depot, DepotOpts};
use mycelium::{Capability, CapFilter, CapabilityGroupDef, MembershipIntent};

const GROUP: &str = "rush-pool";
const N_CANDIDATES: usize = 5;
const MIN: usize = 2;
const MAX: usize = 3;

/// Live members of the pool, counted from each candidate's own `groups()` view.
fn members(candidates: &[Depot]) -> usize {
    candidates.iter().filter(|d| d.agent.groups().iter().any(|g| g.as_ref() == GROUP)).count()
}

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

    let cert_dir = std::env::temp_dir().join(format!("coop-elastic-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&cert_dir);

    let p = alloc_ports((N_CANDIDATES + 1) * 2);

    // ── operator node (seed) — publishes intent, hosts no group capability ──────
    let operator = spawn_depot(DepotOpts {
        name: "coop-operator".into(),
        gossip_port: p[0], http_port: p[1],
        zone: "hub".into(),
        bootstrap: vec![],
        cert_dir: cert_dir.clone(),
        health_secs: Some(4),
    })
    .await?;
    println!("[{}] up (operator / seed)", operator.name);
    let seed = operator.gossip_port;

    // ── candidate depots — advertise the rush capability + run the governor ─────
    let mut candidates = Vec::new();
    let mut _regs = Vec::new();
    for i in 0..N_CANDIDATES {
        let depot = spawn_depot(DepotOpts {
            name: format!("depot-{i}"),
            gossip_port: p[2 + i * 2], http_port: p[3 + i * 2],
            zone: format!("zone-{i}"),
            bootstrap: vec![seed],
            cert_dir: cert_dir.clone(),
            health_secs: Some(4),
        })
        .await?;
        _regs.push(depot.agent.capabilities()
            .advertise_capability(Capability::new("rush", "worker"), Duration::from_secs(30)));
        depot.agent.start_membership_governor();
        candidates.push(depot);
    }

    // The operator owns the group definition (it gossips to every candidate).
    let _grp = operator.agent.capabilities().define_capability_group(
        GROUP,
        CapabilityGroupDef {
            filter: CapFilter::new("rush", "worker"),
            topology_policy: None,
            provides: vec![],
            requires: vec![],
        },
        Duration::from_secs(60),
    );
    println!("{N_CANDIDATES} candidates eligible for {GROUP}; governors on, awaiting intent");

    wait_until(15, || {
        candidates.iter().all(|d| !d.agent.peers().is_empty())
            && operator.agent.kv().get(&format!("cap-group/{GROUP}")).is_some()
    })
    .await;

    // ── Phase 1 — publish the band; the pool converges to a subset in [MIN, MAX] ─
    println!("\n[operator] intent: keep {GROUP} in [{MIN}, {MAX}]  (of {N_CANDIDATES} eligible)");
    let _ = operator.agent
        .publish_membership_intent(MembershipIntent::new(GROUP, MIN, Some(MAX)));

    let converged = wait_until(45, || (MIN..=MAX).contains(&members(&candidates))).await;
    let m1 = members(&candidates);
    println!("[phase 1] pool members: {m1} (band [{MIN}, {MAX}], a subset of {N_CANDIDATES})");
    assert!(converged && (MIN..=MAX).contains(&m1),
        "pool must converge into the band [{MIN}, {MAX}] — got {m1}");
    assert!(m1 < N_CANDIDATES, "the band is a SUBSET — not every eligible depot joins");

    // ── Phase 2 — operator vanishes; intent persists in gossip, band still holds ─
    println!("[operator] going offline — intent stays gossiped within its TTL …");
    operator.shutdown().await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    let m2 = members(&candidates);
    println!("[phase 2] operator gone; pool members still: {m2} (band maintained without a controller)");
    assert!((MIN..=MAX).contains(&m2), "band maintained without the operator — got {m2}");

    // ── Phase 3 — kill a pool member; governors self-heal back to >= MIN ─────────
    let victim = candidates.iter().position(|d| d.agent.groups().iter().any(|g| g.as_ref() == GROUP))
        .expect("a pool member exists");
    println!("[phase 3] removing pool member {} …", candidates[victim].name);
    candidates[victim].shutdown().await;
    candidates.remove(victim);

    let healed = wait_until(45, || members(&candidates) >= MIN).await;
    let m3 = members(&candidates);
    println!("[phase 3] after loss, pool self-healed to: {m3} (>= MIN {MIN})");
    assert!(healed && m3 >= MIN, "governors must self-heal the pool back to >= MIN — got {m3}");

    println!("\nAll assertions passed — elastic band held by intent + local self-election, no coordinator.");

    drop(_grp);
    for d in candidates {
        d.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(&cert_dir);
    Ok(())
}
