//! Example 02 — **stigmergy: backpressure as a pheromone**.
//!
//! Three worker depots (`camden`, `hackney`, `islington`) each advertise the `depot/intake`
//! capability and run an adaptive **opacity governor** over their `work.intake` queue. A
//! `depot-dispatch` node hands out work and decides where it goes — by **reading the trail the
//! medium carries**, never by asking anyone's state.
//!
//! When one depot's intake queue fills past the governor's threshold, the governor writes an
//! `is_opaque` pheromone to `sys/load/{depot}/work.intake`. That trail gossips like any KV value.
//! Dispatch reads it (`is_node_opaque`) and routes around the busy depot — no message, no
//! coordinator, no failure detector. Drain the queue and the pheromone evaporates back to
//! transparent; the depot rejoins the eligible set on its own.
//!
//! Run:  cargo run -p mycelium-coop-examples --bin stigmergy

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::time;

use coop::common::{alloc_ports, spawn_depot, Depot, DepotOpts};
use mycelium::signal::Signal;
use mycelium::{CapFilter, Capability, GossipAgent, NodeId, OpacityHint, SignalScope};

/// The work-queue signal kind whose fill drives each depot's opacity.
const WORK: &str = "work.intake";
/// Queue capacity — 6+ buffered crosses the default 0.75 threshold.
const QUEUE_CAP: usize = 8;

/// A worker depot plus the handles that must stay alive (governor + advertisement) and its
/// undrained work queue (so we can flood and later drain it).
struct Worker {
    depot:    Depot,
    _gov:     mycelium::OpacityHandle,
    _advert:  mycelium::CapabilityReg,
    work_rx:  tokio::sync::mpsc::Receiver<Signal>,
}

async fn spawn_worker(name: &str, zone: &str, ports: (u16, u16), seed: u16, cert_dir: &std::path::Path) -> Worker {
    let depot = spawn_depot(DepotOpts {
        name: name.into(),
        gossip_port: ports.0, http_port: ports.1,
        zone: zone.into(),
        bootstrap: vec![seed],
        cert_dir: cert_dir.to_path_buf(),
        health_secs: None,
    })
    .await
    .expect("worker depot starts");

    // Hold an (undrained) queue for WORK so flooding it raises the fill ratio.
    let work_rx = depot.agent.mesh().signal_rx_with_capacity(WORK, QUEUE_CAP);
    // The adaptive governor: it writes the is_opaque pheromone once fill crosses threshold.
    let _gov = depot.agent.capabilities().manage_opacity(WORK, SignalScope::Cluster, OpacityHint::default());
    // Advertise the capability the dispatcher resolves.
    let _advert = depot.agent.capabilities()
        .advertise_capability(Capability::new("depot", "intake"), Duration::from_secs(30));

    println!("[{}] up ({zone}) — advertises depot/intake, governing {WORK}", depot.name);
    Worker { depot, _gov, _advert, work_rx }
}

/// Dispatch's view of who can take intake right now: providers of `depot/intake` whose pheromone
/// is *not* opaque. Pure read of the medium — no agent is asked anything.
fn eligible(dispatch: &Arc<GossipAgent>, candidates: &[NodeId]) -> Vec<NodeId> {
    let providers = dispatch.capabilities().resolve(&CapFilter::new("depot", "intake"));
    let provider_ids: Vec<NodeId> = providers.into_iter().map(|(id, _)| id).collect();
    candidates
        .iter()
        .filter(|id| provider_ids.contains(id))
        .filter(|id| !dispatch.capabilities().is_node_opaque(id, WORK, Duration::from_secs(10)))
        .cloned()
        .collect()
}

/// Poll until `cond` holds (structural, not a fixed sleep), up to `secs`.
async fn wait_until(secs: u64, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    while std::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        time::sleep(Duration::from_millis(150)).await;
    }
    cond()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(tracing::Level::WARN).init();

    let cert_dir = std::env::temp_dir().join(format!("coop-stigmergy-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&cert_dir);

    let p = alloc_ports(8); // 4 nodes × (gossip, http)

    // ── depot-dispatch: hands out work, reads the medium (seed) ─────────────────
    let dispatch = spawn_depot(DepotOpts {
        name: "depot-dispatch".into(),
        gossip_port: p[0], http_port: p[1],
        zone: "hub".into(),
        bootstrap: vec![],
        cert_dir: cert_dir.clone(),
        health_secs: None,
    })
    .await?;
    println!("[{}] up — will route intake by reading pheromone trails", dispatch.name);
    let seed = dispatch.gossip_port;

    // ── three worker depots ─────────────────────────────────────────────────────
    let mut camden    = spawn_worker("depot-camden",    "camden",    (p[2], p[3]), seed, &cert_dir).await;
    let hackney       = spawn_worker("depot-hackney",   "hackney",   (p[4], p[5]), seed, &cert_dir).await;
    let islington     = spawn_worker("depot-islington", "islington", (p[6], p[7]), seed, &cert_dir).await;

    let camden_id    = camden.depot.node_id();
    let hackney_id   = hackney.depot.node_id();
    let islington_id = islington.depot.node_id();
    let all = [camden_id.clone(), hackney_id.clone(), islington_id.clone()];

    // Wait for the cluster to form: all three providers visible to dispatch + peers established.
    let dispatch_agent = Arc::clone(&dispatch.agent);
    let formed = wait_until(15, || {
        let n = dispatch_agent.capabilities().resolve(&CapFilter::new("depot", "intake")).len();
        n >= 3 && !dispatch_agent.peers().is_empty()
    })
    .await;
    assert!(formed, "all three intake providers should be visible to dispatch");

    // ── Phase 1 — all transparent: every depot is eligible ──────────────────────
    let phase1 = eligible(&dispatch.agent, &all);
    println!("\n[phase 1] eligible for intake: {} depots", phase1.len());
    assert_eq!(phase1.len(), 3, "all three depots eligible when none is loaded");

    // ── Phase 2 — camden takes on a local intake backlog → it self-marks opaque ──
    // A depot's load is its OWN un-processed work: camden receives a burst of intake it can't
    // drain fast enough, so its work.intake queue saturates. The governor (sampling the local
    // handler fill) crosses threshold and writes the is_opaque pheromone to sys/load/{camden}/…
    // — a node reporting its own saturation, exactly the stigmergic contract.
    println!("[phase 2] {} hits a local intake backlog …", camden.depot.name);
    for _ in 0..QUEUE_CAP {
        let _ = camden.depot.agent.mesh().emit(WORK, SignalScope::Individual(camden_id.clone()), Bytes::new());
    }

    let dispatch_agent2 = Arc::clone(&dispatch.agent);
    let cam = camden_id.clone();
    let went_opaque = wait_until(8, || {
        dispatch_agent2.capabilities().is_node_opaque(&cam, WORK, Duration::from_secs(10))
    })
    .await;
    assert!(went_opaque, "camden's pheromone must read opaque once its queue saturates");

    let phase2 = eligible(&dispatch.agent, &all);
    println!("[phase 2] camden is opaque (self-reported via pheromone); eligible now: {} depots → {:?}",
        phase2.len(),
        phase2.iter().map(|id| id.to_string()).collect::<Vec<_>>());
    assert_eq!(phase2.len(), 2, "the busy depot drops out of the eligible set");
    assert!(!phase2.contains(&camden_id), "dispatch routes around camden — without asking it anything");

    // ── Phase 3 — drain camden's queue → pheromone evaporates to transparent ────
    println!("[phase 3] draining {}'s queue …", camden.depot.name);
    for _ in 0..QUEUE_CAP {
        let _ = time::timeout(Duration::from_millis(50), camden.work_rx.recv()).await;
    }

    let dispatch_agent3 = Arc::clone(&dispatch.agent);
    let cam2 = camden_id.clone();
    let cleared = wait_until(8, || {
        !dispatch_agent3.capabilities().is_node_opaque(&cam2, WORK, Duration::from_secs(10))
    })
    .await;
    assert!(cleared, "camden clears its pheromone once the queue drains below the clear threshold");

    let phase3 = eligible(&dispatch.agent, &all);
    println!("[phase 3] camden recovered; eligible again: {} depots", phase3.len());
    assert_eq!(phase3.len(), 3, "the recovered depot rejoins the eligible set on its own");

    println!("\nAll assertions passed — backpressure shed and recovered via pheromone alone, no coordinator.");

    // Keep handles alive until here, then tear down.
    drop((hackney, islington));
    camden.depot.shutdown().await;
    dispatch.shutdown().await;
    let _ = std::fs::remove_dir_all(&cert_dir);
    Ok(())
}
