//! Example 04b — **the autonomic self-heal loop, visualised** in the browser.
//!
//! The continuous, animated sibling of `provisioning.rs`. Same world, same mechanism — but instead
//! of running a fixed sequence of assertions and exiting, it loops forever and streams the
//! provisioning picture to a `<canvas>` dashboard at `http://127.0.0.1:8097/`.
//!
//! The world: a `buffer` (tuple-space Primary rendezvous), a `seeder` (fills the `optimize` lane
//! with donations), a `worker` (declares it needs `route/optimize` and drains the lane), and two
//! provider depots — `provider-a` / `provider-b` — that each run an autonomic `Provisioner`. On
//! unmet `route/optimize` demand one self-elects to **pull + content-verify + instantiate** a WASM
//! optimizer component, advertise it, and serve it over RPC; the other idles as a standby.
//!
//! The loop, paced for legibility:
//!   1. a wave of donations buffers → the active provider serves it → the worker drains the wave;
//!   2. the **active provider is killed** — its `route/optimize` capability evaporates from the
//!      medium (no coordinator notices, nothing is told);
//!   3. a fresh wave buffers with no live provider;
//!   4. the **standby self-provisions** to restore the capability (restart ≡ first-time
//!      provisioning) → the worker drains the wave;
//!   5. the killed provider is **restarted** and rejoins as the new standby. Repeat forever.
//!
//! The money shot: the active node goes dark on kill while the standby lights up and re-provisions —
//! nobody predicted who would run the optimizer; it was unmet demand, satisfied by a node electing
//! to provision.
//!
//! The WASM artifact is the committed `echo_component.wasm` fixture standing in for a route
//! optimizer (it echoes its input — a deterministic "optimized route"), so no wasm toolchain is
//! needed.
//!
//! ## Loads
//! - **Content** — route/optimize — a WASM component (echo_component.wasm fixture)
//! - **Type** — `ArtifactKind::WasmComponent`
//! - **From** — InMemorySource (build-embedded fixture, single-process shortcut — see catalog for the cross-node path) → catalogue → autonomic Provisioner
//!
//! Run:  cargo run -p mycelium-coop-examples --features wasm,metrics --bin provisioning_viz
//! Then open http://127.0.0.1:8097/

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::{signal, time};

use coop::common::{
    alloc_ports, announce_loads, spawn_depot, Depot, DepotOpts, Donation, Loads,
};
use mycelium::{CapFilter, Capability, GossipAgent, NodeId};
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
/// Fixed HTTP port for the dashboard (kept clear of the gateway's OS-assigned depot ports).
const HTTP_PORT: u16 = 8097;

/// The Mycelium concepts + services this demo exercises — injected into the dashboard's "what
/// you're seeing" box (the UI-example contract; see docs/wiki/dev/ui-example-contract.md). `tag`
/// is a layer/service key the shared panel colour-codes (I·II·III·IV · companion · gateway · audit).
const CONCEPTS: &str = r#"[
  {"tag":"IV","name":"capabilities","gloss":"nodes advertise + resolve route/optimize by need — no addresses"},
  {"tag":"IV","name":"autonomic provisioner","gloss":"unmet demand → self-elect → pull WASM → verify → serve"},
  {"tag":"I","name":"gossip-KV catalogue","gloss":"the installable entry replicates like any key; bytes travel peer-to-peer"},
  {"tag":"companion","name":"WASM host","gloss":"the route/optimize component runs confined in the wasm host"},
  {"tag":"gateway","name":"gateway + metrics","gloss":"/stats · /gateway/fleet · /metrics — this Ops Console"}
]"#;

const LOADS: &[Loads] = &[Loads {
    content: "route/optimize — a WASM component (echo_component.wasm fixture)",
    kind: "ArtifactKind::WasmComponent",
    from: "InMemorySource (build-embedded fixture, single-process shortcut — see catalog for \
           the cross-node path) → catalogue → autonomic Provisioner",
}];

// ── Shared snapshot served to the browser ─────────────────────────────
#[derive(Serialize, Clone)]
struct NodeView {
    name:   String,
    zone:   String,
    role:   String,
    /// One of `idle | provisioning | serving | dead`.
    status: String,
}

#[derive(Serialize)]
struct VizState {
    nodes:            Vec<NodeView>,
    active_optimizer: Option<String>,
    demand_pending:   bool,
    buffered:         u64,
    optimized:        u64,
    phase:            String,
    events:           Vec<String>,
}

impl VizState {
    fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }
}

// ── A provider depot that can be killed and restarted on the same ports ─────────
struct Provider {
    name:        &'static str,
    zone:        &'static str,
    gossip_port: u16,
    http_port:   u16,
    depot:       Option<Depot>,
    prov:        Option<tokio::task::JoinHandle<()>>,
}

impl Provider {
    fn alive(&self) -> bool {
        self.depot.is_some()
    }
    fn node_id(&self) -> Option<NodeId> {
        self.depot.as_ref().map(|d| d.node_id())
    }
}

/// Bring a provider online: spawn its depot (bootstrapping the buffer) and start its autonomic
/// provisioner loop. Reuses the fixed ports, so a restart yields the same `NodeId` — restart ≡
/// first-time provisioning, no coordinator involved.
async fn spawn_provider(
    p: &mut Provider,
    seed: u16,
    cert_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let depot = spawn_depot(DepotOpts {
        name:        p.name.into(),
        gossip_port: p.gossip_port,
        http_port:   p.http_port,
        zone:        p.zone.into(),
        bootstrap:   vec![seed],
        cert_dir:    cert_dir.to_path_buf(),
        health_secs: None,
    })
    .await?;
    let prov = spawn_provisioner(Arc::clone(&depot.agent));
    p.depot = Some(depot);
    p.prov = Some(prov);
    Ok(())
}

/// Kill a provider: abort its provisioner and gracefully shut down its depot. Its `route/optimize`
/// capability then evaporates from the medium as the cluster stops hearing its gossip.
async fn kill_provider(p: &mut Provider) {
    if let Some(h) = p.prov.take() {
        h.abort();
    }
    if let Some(d) = p.depot.take() {
        d.shutdown().await;
    }
}

/// Run an autonomic `Provisioner` on `agent`: each tick, provision `route/optimize` from the WASM
/// catalog **only while demand is unmet** (no live provider). A standby thus idles until the active
/// provider dies and demand reappears — restart ≡ first-time provisioning. Returns the loop task.
fn spawn_provisioner(agent: Arc<GossipAgent>) -> tokio::task::JoinHandle<()> {
    let mut source = InMemorySource::new();
    let artifact = source.insert(OPTIMIZER_WASM.to_vec());
    let mut catalog = InstallableCatalog::new();
    catalog.add(
        InstallableEntry::new(Capability::new("route", "optimize"), artifact)
            .with_cost(OPTIMIZER_WASM.len() as u64, 1),
    );
    let host = Arc::new(WasmHost::new().expect("wasm engine"));
    let mut prov = Provisioner::new(agent, host, catalog, Arc::new(source), 1.0);
    tokio::spawn(async move {
        loop {
            let _ = prov.provision_round();
            time::sleep(Duration::from_millis(400)).await;
        }
    })
}

/// Drain one donation from the lane: take → invoke the live optimizer (RPC → WASM) → complete.
async fn process_one(
    worker: &Arc<GossipAgent>,
    ts: &TupleSpace,
    invoke_kind: &Arc<str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (id, payload) = ts.take(LANE, Duration::from_secs(10)).await?;
    let providers = worker.capabilities().resolve(&CapFilter::new("route", "optimize"));
    let (opt_node, _) = providers.into_iter().next().ok_or("no route/optimize provider")?;
    let optimized: Bytes = worker
        .service()
        .rpc_call(opt_node, Arc::clone(invoke_kind), payload, Duration::from_secs(5))
        .await?;
    ts.complete(id, DONE, optimized).await?;
    Ok(())
}

// ── Snapshot mutation helpers (the loop owns node status / phase / events) ──────
fn set_phase(state: &Arc<Mutex<VizState>>, phase: &str) {
    state.lock().unwrap().phase = phase.to_string();
}

fn narrate(state: &Arc<Mutex<VizState>>, msg: &str) {
    println!("{msg}");
    let mut s = state.lock().unwrap();
    s.events.push(msg.to_string());
    let len = s.events.len();
    if len > 8 {
        s.events.drain(0..len - 8);
    }
}

/// Fold ground truth — who *actually* provides `route/optimize` right now, per the resolved
/// medium — into each provider's node status, and set `active_optimizer`. A dead depot always
/// reads `dead`; a live provider in the resolve set reads `serving`; the rest read `provisioning`
/// while demand is unmet by anyone (`provisioning_hint`) or `idle` otherwise (a standby). This is
/// the only place provider status is sampled: purely a read of the gossiped capability set.
fn sync_provider_statuses(
    state: &Arc<Mutex<VizState>>,
    worker: &Arc<GossipAgent>,
    providers: &[Provider],
    provisioning_hint: bool,
) {
    let serving: Vec<NodeId> = worker
        .capabilities()
        .resolve(&CapFilter::new("route", "optimize"))
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    let someone_serving = !serving.is_empty();

    let mut s = state.lock().unwrap();
    let mut active: Option<String> = None;
    for p in providers {
        let status = if !p.alive() {
            "dead"
        } else if p.node_id().map(|id| serving.contains(&id)).unwrap_or(false) {
            active = Some(p.name.to_string());
            "serving"
        } else if provisioning_hint && !someone_serving {
            "provisioning"
        } else {
            "idle"
        };
        if let Some(n) = s.nodes.iter_mut().find(|n| n.name == p.name) {
            n.status = status.to_string();
        }
    }
    s.active_optimizer = active;
}

/// Poll until a live provider (optionally excluding `exclude`, the just-killed node) serves
/// `route/optimize`, syncing statuses each tick so the browser sees the provisioning→serving flip
/// unfold. Structural — never a fixed sleep. Returns whether it happened within `secs`.
async fn wait_provisioned(
    state: &Arc<Mutex<VizState>>,
    worker: &Arc<GossipAgent>,
    providers: &[Provider],
    exclude: Option<NodeId>,
    secs: u64,
) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        sync_provider_statuses(state, worker, providers, true);
        let serving: Vec<NodeId> = worker
            .capabilities()
            .resolve(&CapFilter::new("route", "optimize"))
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        let healed = providers.iter().any(|p| match (p.node_id(), &exclude) {
            (Some(id), Some(ex)) => id != *ex && serving.contains(&id),
            (Some(id), None) => serving.contains(&id),
            _ => false,
        });
        if healed {
            sync_provider_statuses(state, worker, providers, false);
            return true;
        }
        if Instant::now() >= deadline {
            sync_provider_statuses(state, worker, providers, false);
            return false;
        }
        time::sleep(Duration::from_millis(300)).await;
    }
}

/// Which provider is the live optimizer right now (its `route/optimize` is in the resolved set)?
fn active_provider_idx(worker: &Arc<GossipAgent>, providers: &[Provider]) -> Option<usize> {
    let serving: Vec<NodeId> = worker
        .capabilities()
        .resolve(&CapFilter::new("route", "optimize"))
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    providers
        .iter()
        .position(|p| p.node_id().map(|id| serving.contains(&id)).unwrap_or(false))
}

/// Seed a wave of `N_DONATIONS` donations into the lane. `base` seeds unique ids across the
/// infinite loop; alternate the origin so the two waves read differently.
async fn seed_wave(
    seeder_ts: &TupleSpace,
    base: u64,
    origin: (&str, &str, &str),
) -> Result<(), Box<dyn std::error::Error>> {
    for k in 1..=N_DONATIONS {
        let d = Donation::new(base + k, origin.0, origin.1, origin.2);
        seeder_ts.put(LANE, d.to_bytes()).await?;
    }
    Ok(())
}

/// Drain a full wave: `N_DONATIONS` take→optimize→complete cycles, paced so the buffered→optimized
/// flow is watchable in the browser.
async fn drain_wave(
    worker: &Arc<GossipAgent>,
    worker_ts: &TupleSpace,
    invoke_kind: &Arc<str>,
) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..N_DONATIONS {
        process_one(worker, worker_ts, invoke_kind).await?;
        time::sleep(Duration::from_millis(450)).await;
    }
    Ok(())
}

// ── Minimal HTTP server — no dependencies beyond tokio (mirrors stigmergy_viz.rs) ─
// Serves the HTML dashboard at / and the JSON state at /state.
async fn serve_http(state: Arc<Mutex<VizState>>, gw_port: u16) {
    let listener = match TcpListener::bind(format!("127.0.0.1:{HTTP_PORT}")).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("HTTP server failed to bind :{HTTP_PORT} — {e}");
            return;
        }
    };
    println!("╔══════════════════════════════════════════════════════╗");
    println!("║  Provisioning dashboard → http://127.0.0.1:{HTTP_PORT}      ║");
    println!("║  a capability self-provisions, dies, and self-heals   ║");
    println!("╚══════════════════════════════════════════════════════╝");

    loop {
        let Ok((mut stream, _)) = listener.accept().await else { continue };
        let st = state.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            let req = std::str::from_utf8(&buf[..n]).unwrap_or("");

            if req.starts_with("OPTIONS") {
                let _ = stream
                    .write_all(
                        b"HTTP/1.1 204 No Content\r\n\
                          Access-Control-Allow-Origin: *\r\n\
                          Connection: close\r\n\r\n",
                    )
                    .await;
                return;
            }

            if req.contains("GET /state") {
                let json = st.lock().unwrap().to_json();
                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                     Content-Type: application/json\r\n\
                     Access-Control-Allow-Origin: *\r\n\
                     Content-Length: {}\r\n\
                     Connection: close\r\n\r\n{}",
                    json.len(),
                    json
                );
                let _ = stream.write_all(response.as_bytes()).await;
            } else {
                // Inject the "⚙ Ops Console" back-link, pre-targeted at the buffer's gateway.
                // A coop depot always runs the gateway, so this is unconditional.
                let console_link = format!(
                    "<a class=\"opsbtn\" href=\"http://127.0.0.1:8099/?target=127.0.0.1:{gw_port}\" \
                     title=\"Open this cluster in the Mycelium Ops Console\">⚙ Ops Console</a>"
                );
                let html = include_str!("provisioning_viz.html")
                    .replace("__OPS_CONSOLE_LINK__", &console_link)
                    .replace("__CONCEPTS__", CONCEPTS);
                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                     Content-Type: text/html; charset=utf-8\r\n\
                     Content-Length: {}\r\n\
                     Connection: close\r\n\r\n{}",
                    html.len(),
                    html
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    announce_loads(LOADS);
    tracing_subscriber::fmt().with_max_level(tracing::Level::WARN).init();

    let cert_dir =
        std::env::temp_dir().join(format!("coop-provisioning-viz-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&cert_dir);
    let p = alloc_ports(10); // buffer, seeder, provider-a, provider-b, worker × (gossip, http)

    // ── buffer: tuple-space Primary (the rendezvous point) + the Ops-Console-targetable gateway ──
    let buffer = spawn_depot(DepotOpts {
        name: "buffer".into(),
        gossip_port: p[0],
        http_port: p[1],
        zone: "hub".into(),
        bootstrap: vec![],
        cert_dir: cert_dir.clone(),
        health_secs: None,
    })
    .await?;
    let seed = buffer.gossip_port;
    let gw_port = buffer.http_port; // the Ops Console back-link target
    let _buf_ts = TupleSpace::new(
        Arc::clone(&buffer.agent),
        TupleConfig {
            namespace: Arc::from("rescue"),
            role: TupleRole::Primary,
            persist: false,
            ..Default::default()
        },
    )
    .await?;
    println!("[buffer] up — tuple-space primary (ns rescue)");
    // Self-advertise this node's browser UI so the Ops Console can offer a live "↗ visualiser"
    // click-through (the `ui/viz` + `ui/label` KV convention). The reverse link (the "⚙ Ops
    // Console" button on the dashboard) is injected into the HTML in serve_http.
    let _ = buffer.agent.kv().set("ui/viz", format!("http://127.0.0.1:{HTTP_PORT}/"));
    let _ = buffer.agent.kv().set("ui/label", "Autonomic self-heal".to_string());
    println!(
        "[ops] buffer gateway is live — point the Ops Console at http://127.0.0.1:{gw_port}/  (/stats · /gateway/fleet · /metrics)"
    );

    // ── seeder + worker (bootstrap the buffer) ──────────────────────────────────
    let mk = |name: &str, gp: u16, hp: u16, zone: &str| DepotOpts {
        name: name.into(),
        gossip_port: gp,
        http_port: hp,
        zone: zone.into(),
        bootstrap: vec![seed],
        cert_dir: cert_dir.clone(),
        health_secs: None,
    };
    let seeder = spawn_depot(mk("seeder", p[2], p[3], "borough")).await?;
    let worker = spawn_depot(mk("worker", p[8], p[9], "depot-c")).await?;

    // ── the two provider depots, each running an autonomic provisioner ──────────
    let mut providers = vec![
        Provider { name: "provider-a", zone: "depot-a", gossip_port: p[4], http_port: p[5], depot: None, prov: None },
        Provider { name: "provider-b", zone: "depot-b", gossip_port: p[6], http_port: p[7], depot: None, prov: None },
    ];
    for pr in providers.iter_mut() {
        spawn_provider(pr, seed, &cert_dir).await?;
    }
    println!("[seeder|provider-a|provider-b|worker] up");

    // Tuple-space clients for the seeder (puts) and worker (takes/completes).
    let seeder_ts = TupleSpace::new(
        Arc::clone(&seeder.agent),
        TupleConfig { namespace: Arc::from("rescue"), role: TupleRole::Client, persist: false, ..Default::default() },
    )
    .await?;
    let worker_ts = Arc::new(
        TupleSpace::new(
            Arc::clone(&worker.agent),
            TupleConfig { namespace: Arc::from("rescue"), role: TupleRole::Client, persist: false, ..Default::default() },
        )
        .await?,
    );

    // ── Shared snapshot + HTTP dashboard (start before settle so the browser connects early) ──
    let state = Arc::new(Mutex::new(VizState {
        nodes: vec![
            NodeView { name: "buffer".into(),     zone: "hub".into(),     role: "tuple-space".into(), status: "idle".into() },
            NodeView { name: "seeder".into(),     zone: "borough".into(), role: "seeder".into(),      status: "idle".into() },
            NodeView { name: "worker".into(),     zone: "depot-c".into(), role: "worker".into(),      status: "idle".into() },
            NodeView { name: "provider-a".into(), zone: "depot-a".into(), role: "provisioner".into(), status: "idle".into() },
            NodeView { name: "provider-b".into(), zone: "depot-b".into(), role: "provisioner".into(), status: "idle".into() },
        ],
        active_optimizer: None,
        demand_pending: false,
        buffered: 0,
        optimized: 0,
        phase: "starting".into(),
        events: Vec::new(),
    }));
    let state_for_server = state.clone();
    tokio::spawn(async move { serve_http(state_for_server, gw_port).await });

    // ── Wait for the cluster to peer and the tuple-space primary to be resolvable. ──
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if seeder_ts.depth(None).await.is_ok()
            && worker_ts.depth(None).await.is_ok()
            && !worker.agent.peers().is_empty()
        {
            break;
        }
        time::sleep(Duration::from_millis(100)).await;
    }

    // The worker declares a standing requirement for route/optimize — the demand signal each
    // provisioner reads. Held for the whole program, so the fleet always keeps one live provider
    // (or, when it dies, races to restore it). Never dropped: dropping withdraws the demand.
    let _requirement = worker
        .agent
        .capabilities()
        .declare_requirement(CapFilter::new("route", "optimize"), Duration::from_secs(600));

    // ── Background: sample the tuple-space depths (buffered / optimized / demand). ──
    let ts_for_counts = Arc::clone(&worker_ts);
    let state_for_counts = state.clone();
    tokio::spawn(async move {
        let mut ticker = time::interval(Duration::from_millis(400));
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let buffered = ts_for_counts
                .depth(Some(LANE))
                .await
                .ok()
                .and_then(|v| v.first().map(|s| s.depth as u64))
                .unwrap_or(0);
            let optimized = ts_for_counts
                .depth(Some(DONE))
                .await
                .ok()
                .and_then(|v| v.first().map(|s| s.depth as u64))
                .unwrap_or(0);
            let mut s = state_for_counts.lock().unwrap();
            s.buffered = buffered;
            s.optimized = optimized;
            s.demand_pending = buffered > 0;
        }
    });

    let invoke_kind: Arc<str> = Arc::from(cap_invoke_kind("route", "optimize").as_str());
    let worker_agent = Arc::clone(&worker.agent);
    let mut id_ctr: u64 = 0;

    // ── Cold start (once): a wave buffers with no provider; a provider self-provisions; drain. ──
    set_phase(&state, "demand — a surge needs route/optimize, no provider exists yet");
    narrate(&state, "[demand] donations buffer in the lane — no route/optimize provider exists yet");
    id_ctr += N_DONATIONS;
    seed_wave(&seeder_ts, id_ctr - N_DONATIONS, ("borough-market", "surplus produce", "southwark")).await?;
    sync_provider_statuses(&state, &worker_agent, &providers, true);

    set_phase(&state, "provisioning — a provider self-elects, pulls + verifies the WASM optimizer");
    let provisioned = wait_provisioned(&state, &worker_agent, &providers, None, 40).await;
    if provisioned {
        narrate(&state, "[provision] route/optimize self-provisioned (WASM pulled + verified + serving)");
    } else {
        narrate(&state, "[provision] warning: no provider came up within budget — continuing");
    }
    set_phase(&state, "serving — the worker drains the buffered wave through the live optimizer");
    drain_wave(&worker_agent, &worker_ts, &invoke_kind).await?;
    narrate(&state, "[drain] wave drained — donations optimized through the live capability");
    time::sleep(Duration::from_millis(1400)).await;

    // ── Steady loop forever: serve → kill active → self-heal on the standby → restart. ──
    println!("\nSelf-heal loop running. Open http://127.0.0.1:{HTTP_PORT}/ · ctrl-c to exit.\n");
    let cycle = async {
        loop {
            // 1 · A fresh wave; the active provider serves it (normal operation).
            set_phase(&state, "serving — a fresh wave flows through the active optimizer");
            narrate(&state, "[serve] a fresh wave of donations arrives — the active optimizer serves it");
            id_ctr += N_DONATIONS;
            if seed_wave(&seeder_ts, id_ctr - N_DONATIONS, ("borough-market", "surplus produce", "southwark")).await.is_err() {
                break;
            }
            let _ = drain_wave(&worker_agent, &worker_ts, &invoke_kind).await;
            sync_provider_statuses(&state, &worker_agent, &providers, false);
            time::sleep(Duration::from_millis(1200)).await;

            // 2 · Kill the active optimizer — its capability evaporates from the medium.
            let Some(active_idx) = active_provider_idx(&worker_agent, &providers)
                .or_else(|| providers.iter().position(|p| p.alive()))
            else {
                break;
            };
            let dead_id = providers[active_idx].node_id();
            let dead_name = providers[active_idx].name;
            set_phase(&state, "failure — the active optimizer is killed; its capability evaporates");
            narrate(&state, &format!("[kill] killing the active optimizer ({dead_name}); route/optimize evaporates from the medium"));
            kill_provider(&mut providers[active_idx]).await;
            sync_provider_statuses(&state, &worker_agent, &providers, false);
            time::sleep(Duration::from_millis(1600)).await;

            // 3 · A fresh wave buffers with no live provider; the standby self-provisions to heal.
            set_phase(&state, "self-heal — a wave buffers with no provider; the standby self-provisions");
            narrate(&state, "[demand] a new wave buffers — no live optimizer; the standby sees unmet demand");
            id_ctr += N_DONATIONS;
            if seed_wave(&seeder_ts, id_ctr - N_DONATIONS, ("spitalfields", "surplus bread", "tower-hamlets")).await.is_err() {
                break;
            }
            let healed = wait_provisioned(&state, &worker_agent, &providers, dead_id, 45).await;
            if healed {
                narrate(&state, "[heal] standby re-provisioned route/optimize — capability restored with no coordinator");
            } else {
                narrate(&state, "[heal] warning: standby did not re-provision within budget — continuing");
            }

            // 4 · Drain the healed wave.
            set_phase(&state, "serving — the standby (now active) drains the buffered wave");
            let _ = drain_wave(&worker_agent, &worker_ts, &invoke_kind).await;
            narrate(&state, "[drain] wave drained through the re-provisioned optimizer");
            time::sleep(Duration::from_millis(1200)).await;

            // 5 · Restart the killed provider; it rejoins as the new standby (demand already met).
            set_phase(&state, "restart — the killed provider rejoins as the new standby");
            narrate(&state, &format!("[restart] restarting {dead_name} — it rejoins as the standby (restart ≡ provisioning)"));
            if spawn_provider(&mut providers[active_idx], seed, &cert_dir).await.is_err() {
                break;
            }
            sync_provider_statuses(&state, &worker_agent, &providers, false);
            time::sleep(Duration::from_millis(1600)).await;
        }
    };

    tokio::select! {
        _ = cycle => {}
        _ = signal::ctrl_c() => {}
    }

    // ── Shutdown ──────────────────────────────────────────────────────
    println!("\nShutting down…");
    for pr in providers.iter_mut() {
        kill_provider(pr).await;
    }
    worker.shutdown().await;
    seeder.shutdown().await;
    buffer.shutdown().await;
    let _ = std::fs::remove_dir_all(&cert_dir);
    Ok(())
}
