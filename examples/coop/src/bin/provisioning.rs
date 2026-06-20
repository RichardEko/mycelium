//! Example 04 ⭐ — **the autonomic loop: buffer while peers self-provision**.
//!
//! The flagship. A surge of donations needs a `route/optimize` capability **no depot has yet**.
//! The donations **buffer in a tuple-space lane** while a depot **self-provisions** the optimizer —
//! a real WASM component pulled, content-verified, and instantiated — then advertises and serves
//! it. The moment it is live, the worker drains the backlog. Nothing predicted who would run the
//! optimizer: it was unmet demand, satisfied by a node electing to provision.
//!
//!   • `buffer`     — the tuple-space Primary (the rendezvous point).
//!   • `seeder`     — fills the `optimize` lane with donations (a client).
//!   • `provider-a` / `provider-b` — each runs a `Provisioner`: on unmet `route/optimize` demand,
//!                    one self-elects to pull+verify+instantiate the WASM optimizer, advertise it,
//!                    and serve it over RPC; the other idles as a standby.
//!   • `worker`     — declares it needs `route/optimize` (the demand), then drains the lane: take →
//!                    invoke the provisioned optimizer → complete to `done`.
//!
//! Then the **active optimizer is killed**: its capability evaporates, a second wave of donations
//! buffers, and the **standby self-provisions** to restore the capability — restart ≡ provisioning,
//! no coordinator. Both waves drain.
//!
//! The WASM artifact is the committed `echo_component.wasm` fixture standing in for a route
//! optimizer (it echoes its input — a deterministic "optimized route"), so CI needs no wasm
//! toolchain.
//!
//! Run:  cargo run -p mycelium-coop-examples --bin provisioning

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use coop::common::{alloc_ports, spawn_depot, Donation, DepotOpts};
use mycelium::{CapFilter, Capability};
use mycelium_tuple_space::{TupleConfig, TupleRole, TupleSpace};
use mycelium_wasm_host::{
    cap_invoke_kind, InMemorySource, InstallableCatalog, InstallableEntry, Provisioner, WasmHost,
};

/// The committed WASM fixture — a route optimizer stand-in (echoes its input).
const OPTIMIZER_WASM: &[u8] =
    include_bytes!("../../../../mycelium-wasm-host/tests/fixtures/echo_component.wasm");

const LANE: &str = "optimize";
const DONE: &str = "done";
const N_DONATIONS: u64 = 4;

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

/// Run an autonomic `Provisioner` on `agent`: each tick, provision `route/optimize` from the WASM
/// catalog **only while demand is unmet** (no live provider). A standby thus idles until the active
/// provider dies and demand reappears — restart ≡ first-time provisioning. Returns the loop task.
fn spawn_provisioner(agent: Arc<mycelium::GossipAgent>) -> tokio::task::JoinHandle<()> {
    let mut source = InMemorySource::new();
    let artifact = source.insert(OPTIMIZER_WASM.to_vec());
    let mut catalog = InstallableCatalog::new();
    catalog.add(InstallableEntry::new(Capability::new("route", "optimize"), artifact)
        .with_cost(OPTIMIZER_WASM.len() as u64, 1));
    let host = Arc::new(WasmHost::new().expect("wasm engine"));
    let mut prov = Provisioner::new(agent, host, catalog, Arc::new(source), 1.0);
    tokio::spawn(async move {
        loop {
            let _ = prov.provision_round();
            tokio::time::sleep(Duration::from_millis(400)).await;
        }
    })
}

/// Drain one donation from the lane: take → invoke the live optimizer (RPC → WASM) → complete.
async fn process_one(
    worker: &Arc<mycelium::GossipAgent>,
    ts: &mycelium_tuple_space::TupleSpace,
    invoke_kind: &Arc<str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (id, payload) = ts.take(LANE, Duration::from_secs(10)).await?;
    let providers = worker.capabilities().resolve(&CapFilter::new("route", "optimize"));
    let (opt_node, _) = providers.into_iter().next().ok_or("no route/optimize provider")?;
    let optimized: Bytes = worker.service()
        .rpc_call(opt_node, Arc::clone(invoke_kind), payload, Duration::from_secs(5)).await?;
    ts.complete(id, DONE, optimized).await?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(tracing::Level::WARN).init();

    let cert_dir = std::env::temp_dir().join(format!("coop-provisioning-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&cert_dir);
    let p = alloc_ports(10); // buffer, seeder, provider-a, provider-b, worker × (gossip, http)

    // ── buffer: tuple-space Primary (the rendezvous point) ──────────────────────
    let buffer = spawn_depot(DepotOpts {
        name: "buffer".into(), gossip_port: p[0], http_port: p[1],
        zone: "hub".into(), bootstrap: vec![], cert_dir: cert_dir.clone(), health_secs: None,
    }).await?;
    let seed = buffer.gossip_port;
    let _buf_ts = TupleSpace::new(Arc::clone(&buffer.agent), TupleConfig {
        namespace: Arc::from("rescue"), role: TupleRole::Primary, persist: false, ..Default::default()
    }).await?;
    println!("[buffer] up — tuple-space primary (ns rescue)");

    // ── seeder, provider, worker (all bootstrap the buffer) ─────────────────────
    let mk = |name: &str, gp: u16, hp: u16, zone: &str| DepotOpts {
        name: name.into(), gossip_port: gp, http_port: hp,
        zone: zone.into(), bootstrap: vec![seed], cert_dir: cert_dir.clone(), health_secs: None,
    };
    let seeder     = spawn_depot(mk("seeder",     p[2], p[3], "borough")).await?;
    let provider_a = spawn_depot(mk("provider-a", p[4], p[5], "depot-a")).await?;
    let provider_b = spawn_depot(mk("provider-b", p[6], p[7], "depot-b")).await?;
    let worker     = spawn_depot(mk("worker",     p[8], p[9], "depot-c")).await?;
    println!("[seeder|provider-a|provider-b|worker] up");

    // Wait for the cluster to peer and the tuple-space primary to be resolvable.
    let seeder_ts = TupleSpace::new(Arc::clone(&seeder.agent), TupleConfig {
        namespace: Arc::from("rescue"), role: TupleRole::Client, persist: false, ..Default::default()
    }).await?;
    let worker_ts = TupleSpace::new(Arc::clone(&worker.agent), TupleConfig {
        namespace: Arc::from("rescue"), role: TupleRole::Client, persist: false, ..Default::default()
    }).await?;
    wait_until(30, || !worker.agent.peers().is_empty() && !seeder.agent.peers().is_empty()).await;
    // Structural: wait until the client can reach the tuple-space primary (depth() succeeds).
    // Generous budget (30 s) — a constrained CI runner peers + elects the primary more slowly.
    let mut primary_ready = false;
    for _ in 0..300 {
        if seeder_ts.depth(None).await.is_ok() && worker_ts.depth(None).await.is_ok() {
            primary_ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(primary_ready, "clients must reach the tuple-space primary before seeding");

    // ── Phase 1 — wave 1 of donations buffers in the lane (no optimizer yet) ─────
    for id in 1..=N_DONATIONS {
        let d = Donation::new(id, "borough-market", "surplus produce", "southwark");
        seeder_ts.put(LANE, d.to_bytes()).await?;
    }
    let buffered = seeder_ts.depth(Some(LANE)).await?.first().map(|s| s.depth).unwrap_or(0);
    println!("[phase 1] {buffered} donations buffered in lane '{LANE}' — no route/optimize provider exists yet");
    assert!(buffered as u64 == N_DONATIONS, "all donations buffered while unprovisioned");

    // The worker declares it NEEDS route/optimize (the demand signal). Both providers run an
    // autonomic provisioner each, self-electing to satisfy unmet demand (one wins the round; the
    // other idles as a standby until demand reappears — restart ≡ provisioning).
    let _req = worker.agent.capabilities()
        .declare_requirement(CapFilter::new("route", "optimize"), Duration::from_secs(60));
    let prov_a = spawn_provisioner(Arc::clone(&provider_a.agent));
    let prov_b = spawn_provisioner(Arc::clone(&provider_b.agent));

    // ── Phase 2 — the optimizer self-provisions; the worker drains wave 1 ───────
    let provisioned = wait_until(40, || {
        !worker.agent.capabilities().resolve(&CapFilter::new("route", "optimize")).is_empty()
    }).await;
    assert!(provisioned, "a provider must self-provision route/optimize from unmet demand");
    println!("[phase 2] route/optimize self-provisioned (WASM pulled + verified + serving)");

    let invoke_kind: Arc<str> = Arc::from(cap_invoke_kind("route", "optimize").as_str());
    for _ in 0..N_DONATIONS {
        process_one(&worker.agent, &worker_ts, &invoke_kind).await?;
    }
    let done1 = worker_ts.depth(Some(DONE)).await?.first().map(|s| s.depth).unwrap_or(0);
    println!("[phase 2] worker drained wave 1 → {done1} donations optimized");
    assert_eq!(done1 as u64, N_DONATIONS, "wave 1 fully optimized once the capability went live");

    // Which node won the provisioning? (The live provider of route/optimize.)
    let active = worker.agent.capabilities().resolve(&CapFilter::new("route", "optimize"))
        .into_iter().next().map(|(id, _)| id).expect("a live optimizer");
    let active_is_a = active == provider_a.node_id();
    println!("[phase 2] active optimizer: {}", if active_is_a { "provider-a" } else { "provider-b" });

    // ── Phase 3 — kill the active optimizer; the standby self-heals (restart ≡ provisioning) ──
    println!("[phase 3] killing the active optimizer; its capability evaporates …");
    if active_is_a { prov_a.abort(); provider_a.shutdown().await; }
    else           { prov_b.abort(); provider_b.shutdown().await; }

    // Seed wave 2 — a fresh backlog that can only be served after the standby re-provisions.
    for id in (N_DONATIONS + 1)..=(2 * N_DONATIONS) {
        let d = Donation::new(id, "spitalfields", "surplus bread", "tower-hamlets");
        seeder_ts.put(LANE, d.to_bytes()).await?;
    }

    // The surviving provider sees the demand return (the dead node's cap/ evaporated) and re-provisions.
    let healed = wait_until(45, || {
        worker.agent.capabilities().resolve(&CapFilter::new("route", "optimize"))
            .iter().any(|(id, _)| *id != active)
    }).await;
    assert!(healed, "the standby provider must re-provide route/optimize after the active one dies");
    println!("[phase 3] standby re-provisioned route/optimize — capability restored with no coordinator");

    for _ in 0..N_DONATIONS {
        process_one(&worker.agent, &worker_ts, &invoke_kind).await?;
    }
    let done_total = worker_ts.depth(Some(DONE)).await?.first().map(|s| s.depth).unwrap_or(0);
    println!("[phase 3] worker drained wave 2 → {done_total} donations optimized in total");
    assert_eq!(done_total as u64, 2 * N_DONATIONS, "both waves fully optimized across the failover");

    println!("\nAll assertions passed — buffered, self-provisioned (WASM), drained, and self-healed across a provider death.");

    prov_a.abort();
    prov_b.abort();
    worker.shutdown().await;
    if active_is_a { provider_b.shutdown().await; } else { provider_a.shutdown().await; }
    seeder.shutdown().await;
    buffer.shutdown().await;
    let _ = std::fs::remove_dir_all(&cert_dir);
    Ok(())
}
