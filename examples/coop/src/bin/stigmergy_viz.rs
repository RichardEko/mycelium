//! Example 02b — **stigmergy, visualised**: backpressure as a pheromone, live in the browser.
//!
//! This is the continuous, animated sibling of `stigmergy.rs`. Same world, same mechanism — but
//! instead of running a fixed sequence of assertions and exiting, it loops forever and streams the
//! routing picture to a `<canvas>` dashboard at `http://127.0.0.1:8092/`.
//!
//! The world: three worker depots (`camden`, `hackney`, `islington`) each advertise the
//! `depot/intake` capability and run an adaptive **opacity governor** over their `work.intake`
//! queue. A `depot-dispatch` hub hands out work and decides where it goes — by **reading the trail
//! the medium carries**, never by asking anyone's state.
//!
//! When a depot's intake queue fills past the governor's ~0.75 threshold, the governor writes an
//! `is_opaque` pheromone to `sys/load/{depot}/work.intake`. That trail gossips like any KV value.
//! Dispatch reads it (`is_node_opaque`) and routes around the busy depot — no message, no
//! coordinator, no failure detector. Drain the queue and the pheromone evaporates back to
//! transparent; the depot rejoins the eligible set on its own.
//!
//! The loop rotates which depot gets loaded so the trail visibly appears and evaporates over and
//! over. The whole point, made legible: dispatch NEVER asks a depot its state. It routes by
//! reading the pheromone trail in the medium.
//!
//! Run:  cargo run -p mycelium-coop-examples --bin stigmergy_viz
//! Then open http://127.0.0.1:8092/

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::{signal, time};

use coop::common::{alloc_ports, spawn_depot, Depot, DepotOpts};
use mycelium::signal::Signal;
use mycelium::{CapFilter, Capability, GossipAgent, NodeId, OpacityHint, SignalScope};

/// The work-queue signal kind whose fill drives each depot's opacity.
const WORK: &str = "work.intake";
/// Queue capacity — 6+ buffered crosses the default 0.75 threshold.
const QUEUE_CAP: usize = 8;
/// Fixed HTTP port for the dashboard (kept clear of the gateway's OS-assigned depot ports).
const HTTP_PORT: u16 = 8092;
/// TTL for reading a pheromone — matches `stigmergy.rs`.
const PHEROMONE_TTL: Duration = Duration::from_secs(10);

// ── Shared snapshot served to the browser ─────────────────────────────
struct DepotView {
    name:   String,
    zone:   String,
    fill:   usize,
    cap:    usize,
    opaque: bool,
}

struct VizState {
    tick:     u64,
    depots:   Vec<DepotView>,
    eligible: Vec<String>,
    busy:     Option<String>,
}

impl VizState {
    fn to_json(&self) -> String {
        let depots: Vec<String> = self
            .depots
            .iter()
            .map(|d| {
                format!(
                    r#"{{"name":"{}","zone":"{}","fill":{},"cap":{},"opaque":{}}}"#,
                    d.name, d.zone, d.fill, d.cap, d.opaque
                )
            })
            .collect();
        let eligible: Vec<String> = self.eligible.iter().map(|n| format!("\"{n}\"")).collect();
        let busy = match &self.busy {
            Some(b) => format!("\"{b}\""),
            None => "null".to_string(),
        };
        format!(
            r#"{{"tick":{},"depots":[{}],"eligible":[{}],"busy":{}}}"#,
            self.tick,
            depots.join(","),
            eligible.join(","),
            busy,
        )
    }
}

// ── A worker depot plus the handles that must stay alive + its undrained queue ─
struct Worker {
    depot:    Depot,
    _gov:     mycelium::OpacityHandle,
    _advert:  mycelium::CapabilityReg,
    work_rx:  tokio::sync::mpsc::Receiver<Signal>,
    node_id:  NodeId,
    name:     String,
}

async fn spawn_worker(
    name: &str,
    zone: &str,
    ports: (u16, u16),
    seed: u16,
    cert_dir: &std::path::Path,
) -> Worker {
    let depot = spawn_depot(DepotOpts {
        name: name.into(),
        gossip_port: ports.0,
        http_port: ports.1,
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
    let _gov = depot
        .agent
        .capabilities()
        .manage_opacity(WORK, SignalScope::Cluster, OpacityHint::default());
    // Advertise the capability the dispatcher resolves.
    let _advert = depot
        .agent
        .capabilities()
        .advertise_capability(Capability::new("depot", "intake"), Duration::from_secs(30));

    let node_id = depot.node_id();
    println!("[{}] up ({zone}) — advertises depot/intake, governing {WORK}", depot.name);
    Worker { depot, _gov, _advert, work_rx, node_id, name: name.into() }
}

/// Dispatch's view of who can take intake right now: providers of `depot/intake` whose pheromone
/// is *not* opaque. Pure read of the medium — no agent is asked anything.
fn eligible(dispatch: &Arc<GossipAgent>, candidates: &[NodeId]) -> Vec<NodeId> {
    let providers = dispatch.capabilities().resolve(&CapFilter::new("depot", "intake"));
    let provider_ids: Vec<NodeId> = providers.into_iter().map(|(id, _)| id).collect();
    candidates
        .iter()
        .filter(|id| provider_ids.contains(id))
        .filter(|id| !dispatch.capabilities().is_node_opaque(id, WORK, PHEROMONE_TTL))
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

// ── Minimal HTTP server — no dependencies beyond tokio (mirrors examples/conway.rs) ─
// Serves the HTML dashboard at / and the JSON state at /state.
async fn serve_http(state: Arc<Mutex<VizState>>, gw_port: u16) {
    let listener = match TcpListener::bind(format!("127.0.0.1:{HTTP_PORT}")).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("HTTP server failed to bind :{HTTP_PORT} — {e}");
            return;
        }
    };
    println!("╔══════════════════════════════════════════════════╗");
    println!("║  Stigmergy dashboard → http://127.0.0.1:{HTTP_PORT}      ║");
    println!("║  dispatch routes by reading the pheromone trail  ║");
    println!("╚══════════════════════════════════════════════════╝");

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
                // Inject the "⚙ Ops Console" back-link, pre-targeted at dispatch's gateway.
                // A coop depot always runs the gateway, so this is unconditional.
                let console_link = format!(
                    "<a class=\"opsbtn\" href=\"http://127.0.0.1:8099/?target=127.0.0.1:{gw_port}\" \
                     title=\"Open this cluster in the Mycelium Ops Console\">⚙ Ops Console</a>"
                );
                let html =
                    include_str!("stigmergy_viz.html").replace("__OPS_CONSOLE_LINK__", &console_link);
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

/// Recompute what dispatch *reads from the medium* — each depot's pheromone (opaque?) and the
/// eligible set — and fold it into the shared snapshot. Never touches `fill`/`busy`, which the
/// loading loop owns. This is the only place dispatch's view is sampled: purely a read of the
/// gossiped trail, exactly as the demo claims.
fn refresh_medium(
    dispatch: &Arc<GossipAgent>,
    ids: &[NodeId],
    names: &[String],
    state: &Arc<Mutex<VizState>>,
) {
    let elig_ids = eligible(dispatch, ids);
    let opaque: Vec<bool> = ids
        .iter()
        .map(|id| dispatch.capabilities().is_node_opaque(id, WORK, PHEROMONE_TTL))
        .collect();
    let elig_names: Vec<String> = ids
        .iter()
        .zip(names)
        .filter(|(id, _)| elig_ids.contains(id))
        .map(|(_, name)| name.clone())
        .collect();

    let mut s = state.lock().unwrap();
    s.tick += 1;
    for (i, d) in s.depots.iter_mut().enumerate() {
        d.opaque = opaque[i];
    }
    s.eligible = elig_names;
}

/// Set the current fill of depot `idx` in the snapshot.
fn set_fill(state: &Arc<Mutex<VizState>>, idx: usize, fill: usize) {
    let mut s = state.lock().unwrap();
    if let Some(d) = s.depots.get_mut(idx) {
        d.fill = fill;
    }
}

/// Mark which depot dispatch is currently loading (for the header caption).
fn set_busy(state: &Arc<Mutex<VizState>>, busy: Option<String>) {
    state.lock().unwrap().busy = busy;
}

/// One full stigmergy cycle over a set of depot indices:
///   flood their queues → their governors write the pheromone → dispatch drops them from the
///   eligible set → hold → drain → the pheromone evaporates → they rejoin. Runs forever from main.
async fn run_cycle(
    indices: &[usize],
    workers: &mut [Worker],
    dispatch: &Arc<GossipAgent>,
    state: &Arc<Mutex<VizState>>,
) {
    let busy_label = indices
        .iter()
        .map(|&i| workers[i].name.clone())
        .collect::<Vec<_>>()
        .join(" + ");
    set_busy(state, Some(busy_label.clone()));
    println!("[load] {busy_label} takes on a local intake backlog …");

    // ── Flood: emit work to each depot's OWN queue, one crate at a time so the bar climbs. ──
    for step in 1..=QUEUE_CAP {
        for &idx in indices {
            let id = workers[idx].node_id.clone();
            let _ = workers[idx]
                .depot
                .agent
                .mesh()
                .emit(WORK, SignalScope::Individual(id), Bytes::new());
            set_fill(state, idx, step);
        }
        time::sleep(Duration::from_millis(180)).await;
    }

    // ── Wait for each depot to self-report opaque (dispatch reads the pheromone). ──
    for &idx in indices {
        let id = workers[idx].node_id.clone();
        let d = Arc::clone(dispatch);
        let went = wait_until(8, || d.capabilities().is_node_opaque(&id, WORK, PHEROMONE_TTL)).await;
        if went {
            println!("[load] {} is opaque — dispatch routes around it (no message sent)", workers[idx].name);
        }
    }

    // ── Hold the backlog a couple of seconds so the trail is legible in the browser. ──
    time::sleep(Duration::from_secs(2)).await;

    // ── Drain: recv from each depot's queue, one crate at a time; the pheromone evaporates. ──
    println!("[drain] draining {busy_label} …");
    for step in (0..QUEUE_CAP).rev() {
        for &idx in indices {
            let _ = time::timeout(Duration::from_millis(50), workers[idx].work_rx.recv()).await;
            set_fill(state, idx, step);
        }
        time::sleep(Duration::from_millis(160)).await;
    }

    // ── Wait for the pheromone to clear so the depot rejoins the eligible set on its own. ──
    for &idx in indices {
        let id = workers[idx].node_id.clone();
        let d = Arc::clone(dispatch);
        let cleared =
            wait_until(8, || !d.capabilities().is_node_opaque(&id, WORK, PHEROMONE_TTL)).await;
        if cleared {
            println!("[drain] {} recovered — rejoins the eligible set", workers[idx].name);
        }
        set_fill(state, idx, 0);
    }

    set_busy(state, None);
    time::sleep(Duration::from_millis(1200)).await;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(tracing::Level::WARN).init();

    let cert_dir = std::env::temp_dir().join(format!("coop-stigmergy-viz-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&cert_dir);

    let p = alloc_ports(8); // 4 nodes × (gossip, http)

    // ── depot-dispatch: hands out work, reads the medium (seed) ─────────────────
    let dispatch = spawn_depot(DepotOpts {
        name: "depot-dispatch".into(),
        gossip_port: p[0],
        http_port: p[1],
        zone: "hub".into(),
        bootstrap: vec![],
        cert_dir: cert_dir.clone(),
        health_secs: None,
    })
    .await?;
    println!("[{}] up — will route intake by reading pheromone trails", dispatch.name);
    println!("[ops] dispatch gateway is live — point the Ops Console at http://127.0.0.1:{}/  (/stats · /gateway/fleet · /gateway/diagnose)", p[1]);
    // Self-advertise this node's browser UI so the Ops Console can offer a live "↗ visualiser"
    // click-through (the `ui/viz` + `ui/label` KV convention). The reverse link (the "⚙ Ops
    // Console" button on the dashboard) is injected into the HTML in serve_http.
    let _ = dispatch.agent.kv().set("ui/viz", format!("http://127.0.0.1:{HTTP_PORT}/"));
    let _ = dispatch.agent.kv().set("ui/label", "Stigmergic routing".to_string());
    let gw_port = p[1]; // dispatch's gateway HTTP port — the Ops Console back-link target
    let seed = dispatch.gossip_port;

    // ── three worker depots ─────────────────────────────────────────────────────
    let camden = spawn_worker("depot-camden", "camden", (p[2], p[3]), seed, &cert_dir).await;
    let hackney = spawn_worker("depot-hackney", "hackney", (p[4], p[5]), seed, &cert_dir).await;
    let islington = spawn_worker("depot-islington", "islington", (p[6], p[7]), seed, &cert_dir).await;

    let mut workers = vec![camden, hackney, islington];
    let ids: Vec<NodeId> = workers.iter().map(|w| w.node_id.clone()).collect();
    let names: Vec<String> = workers.iter().map(|w| w.name.clone()).collect();

    // ── Shared snapshot + HTTP dashboard (start before settle so the browser connects early) ──
    let state = Arc::new(Mutex::new(VizState {
        tick: 0,
        depots: workers
            .iter()
            .map(|w| DepotView {
                name:   w.name.clone(),
                zone:   w.depot.name.clone(),
                fill:   0,
                cap:    QUEUE_CAP,
                opaque: false,
            })
            .collect(),
        eligible: Vec::new(),
        busy: None,
    }));
    // Zones read nicest as the bare locality; overwrite with the short zone label.
    {
        let mut s = state.lock().unwrap();
        for (d, z) in s.depots.iter_mut().zip(["camden", "hackney", "islington"]) {
            d.zone = z.to_string();
        }
    }
    let state_for_server = state.clone();
    tokio::spawn(async move { serve_http(state_for_server, gw_port).await });

    // ── Wait for the cluster to form: all three providers visible to dispatch + peers. ──
    let dispatch_agent = Arc::clone(&dispatch.agent);
    let formed = wait_until(20, || {
        let n = dispatch_agent
            .capabilities()
            .resolve(&CapFilter::new("depot", "intake"))
            .len();
        n >= 3 && !dispatch_agent.peers().is_empty()
    })
    .await;
    if !formed {
        eprintln!("warning: not all intake providers visible yet — continuing anyway");
    } else {
        println!("[cluster] formed — 3 intake providers visible to dispatch");
    }

    // ── Background: sample the medium (opaque/eligible) continuously, independent of loading. ──
    let dispatch_for_refresh = Arc::clone(&dispatch.agent);
    let ids_for_refresh = ids.clone();
    let names_for_refresh = names.clone();
    let state_for_refresh = state.clone();
    tokio::spawn(async move {
        let mut ticker = time::interval(Duration::from_millis(300));
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            refresh_medium(
                &dispatch_for_refresh,
                &ids_for_refresh,
                &names_for_refresh,
                &state_for_refresh,
            );
        }
    });

    // ── Loop forever: rotate which depot(s) get loaded so the trail appears and evaporates. ──
    println!("\nRouting loop running. Open http://127.0.0.1:{HTTP_PORT}/ · ctrl-c to exit.\n");
    let dispatch_arc = Arc::clone(&dispatch.agent);
    let mut cycle: usize = 0;
    loop {
        // Every 4th cycle, load two depots at once (occasional double-backlog); otherwise rotate one.
        let indices: Vec<usize> = if cycle % 4 == 3 {
            let a = cycle % 3;
            let b = (cycle + 1) % 3;
            vec![a, b]
        } else {
            vec![cycle % 3]
        };

        tokio::select! {
            _ = run_cycle(&indices, &mut workers, &dispatch_arc, &state) => {}
            _ = signal::ctrl_c() => break,
        }
        cycle += 1;
    }

    // ── Shutdown ──────────────────────────────────────────────────────
    println!("\nShutting down…");
    for w in &workers {
        w.depot.shutdown().await;
    }
    dispatch.shutdown().await;
    let _ = std::fs::remove_dir_all(&cert_dir);
    Ok(())
}
