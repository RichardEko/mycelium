//! Example 07 — **multi-bloc agreement via cross-group consensus** (Layer III).
//!
//! A large donation spans two depot blocs — `north` and `south` — and accepting it commits *both*
//! to cold-chain capacity. Neither bloc may bind the other, so acceptance requires **each bloc to
//! independently reach quorum**: `cross_group_propose` over two `GroupQuorum`s. This is Layer III —
//! an emergent coordinator (the proposer + the quorum) that exists only for the decision and
//! dissolves once it commits, riding ordinary signals on the same substrate.
//!
//! The commitment is **epoch-leased** (`committed_lease_secs`): like a capability pheromone it
//! decays, so a stale acceptance reads as not-committed and the slot reopens for re-proposal —
//! decisions evaporate too (the philosophy's mandate-TTL).
//!
//!   • two depots in `north`, two in `south`; all four are consensus voters.
//!   • Phase 1: propose with BOTH blocs required → commits (each reaches quorum).
//!   • Phase 2: propose requiring a third bloc with no voters → times out (no bloc can be coerced).
//!   • Phase 3: a short lease expires → the committed slot reads back as reopened.
//!
//! Run:  cargo run -p mycelium-coop-examples --bin consensus

use std::time::Duration;

use bytes::Bytes;
use coop::common::{alloc_ports, spawn_depot, Depot, DepotOpts};
use mycelium::{ConsensusConfig, ConsensusResult, GroupQuorum};

async fn wait_until(secs: u64, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    while std::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    cond()
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(tracing::Level::WARN).init();

    let cert_dir = std::env::temp_dir().join(format!("coop-consensus-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&cert_dir);
    let p = alloc_ports(8); // 4 depots × (gossip, http)

    // Four depots: depot-0/1 in the north bloc, depot-2/3 in the south bloc.
    let mut depots = Vec::new();
    for i in 0..4 {
        let depot = spawn_depot(DepotOpts {
            name: format!("depot-{i}"),
            gossip_port: p[i * 2], http_port: p[i * 2 + 1],
            zone: if i < 2 { "north".into() } else { "south".into() },
            bootstrap: if i == 0 { vec![] } else { vec![p[0]] },
            cert_dir: cert_dir.clone(),
            health_secs: Some(2),
        }).await?;
        depots.push(depot);
    }
    println!("[depot-0..3] up — north = {{0,1}}, south = {{2,3}}");

    // Every depot joins its bloc and runs a consensus listener (a voter). Multi-node consensus
    // requires a listener on every node, else votes never arrive and ballots time out.
    let _listeners: Vec<_> = depots.iter().map(|d| {
        let bloc = if d.name.ends_with('0') || d.name.ends_with('1') { "north" } else { "south" };
        d.agent.mesh().join_group(bloc);
        d.agent.consensus().start_consensus_listener(ConsensusConfig::default())
    }).collect();

    // Form the cluster + let group membership gossip so every voter sees both rosters.
    let dref: &[Depot] = &depots;
    wait_until(20, || {
        dref.iter().all(|d| d.agent.peers().len() >= 3)
            && dref[0].agent.mesh().group_members("north").len() >= 2
            && dref[0].agent.mesh().group_members("south").len() >= 2
    }).await;
    println!("cluster peered; north & south rosters each have 2 voters");

    // ── Phase 1 — both blocs required; each reaches quorum → COMMIT ─────────────
    println!("\n[phase 1] proposing acceptance of a cross-bloc donation (north AND south must agree)");
    let both = vec![
        GroupQuorum { group: "north".into(), quorum: 0.5, veto: false },
        GroupQuorum { group: "south".into(), quorum: 0.5, veto: false },
    ];
    let r1 = depots[0].agent.consensus()
        .cross_group_propose("donation/cross-bloc-42", Bytes::from_static(b"accept:5000kg"),
                             both, ConsensusConfig::default())
        .await;
    println!("[phase 1] result: {}", describe(&r1));
    assert!(matches!(r1, ConsensusResult::Committed { .. }),
        "both blocs reached quorum → the donation is accepted");

    // The committed value is readable on any node (lease-aware reader).
    let seen = wait_until(10, || depots[3].agent.consensus().consensus_get("donation/cross-bloc-42").is_some()).await;
    assert!(seen, "the commit propagates to every depot");
    println!("[phase 1] every depot reads the committed acceptance ✓");

    // ── Phase 2 — require a third bloc with no voters → no coercion, TIMES OUT ──
    println!("[phase 2] proposing a donation that also requires 'east' (a bloc with no depots)");
    let needs_east = vec![
        GroupQuorum { group: "north".into(), quorum: 0.5, veto: false },
        GroupQuorum { group: "east".into(),  quorum: 0.5, veto: false },
    ];
    let fast = ConsensusConfig {
        phase1_timeout: Duration::from_millis(300),
        max_ballots: 1,
        ..ConsensusConfig::default()
    };
    let r2 = depots[0].agent.consensus()
        .cross_group_propose("donation/needs-east", Bytes::from_static(b"accept"), needs_east, fast)
        .await;
    println!("[phase 2] result: {}", describe(&r2));
    assert!(matches!(r2, ConsensusResult::Timeout { .. }),
        "a bloc with no voters can't be coerced → no commit (promise-strength, no central authority)");

    // ── Phase 3 — an epoch-leased commitment decays → the slot reopens ─────────
    println!("[phase 3] committing a SHORT-LEASED decision (lease 1s) — decisions evaporate too");
    let leased = ConsensusConfig { committed_lease_secs: Some(1), ..ConsensusConfig::default() };
    let just_north = vec![GroupQuorum { group: "north".into(), quorum: 0.5, veto: false }];
    let r3 = depots[0].agent.consensus()
        .cross_group_propose("donation/leased-slot", Bytes::from_static(b"hold-bay-3"), just_north, leased)
        .await;
    assert!(matches!(r3, ConsensusResult::Committed { .. }), "leased decision commits while live");
    assert!(depots[0].agent.consensus().consensus_get("donation/leased-slot").is_some(),
        "the leased decision reads as committed while its lease is live");
    println!("[phase 3] leased decision committed and readable while live");

    let reopened = wait_until(10, ||
        depots[0].agent.consensus().consensus_get("donation/leased-slot").is_none()).await;
    assert!(reopened, "after the lease expires the slot reads as not-committed (reopened)");
    println!("[phase 3] lease expired → the slot reads as reopened ✓ (mandate-TTL on decisions)");

    println!("\nAll assertions passed — multi-bloc agreement reached, uncoerced, and the leased decision decayed.");

    for d in depots {
        d.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(&cert_dir);
    Ok(())
}

fn describe(r: &ConsensusResult) -> String {
    match r {
        ConsensusResult::Committed { ballot, .. } => format!("Committed (ballot {ballot})"),
        ConsensusResult::Timeout { ballots_tried, .. } => format!("Timeout after {ballots_tried} ballot(s)"),
        other => format!("{other:?}"),
    }
}
