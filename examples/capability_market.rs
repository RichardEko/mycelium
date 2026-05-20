//! Capability Market — Phase 3 advertise / declare / watch / demand
//!
//! 12 GossipAgents (ports 55000–55011). Four capability kinds:
//!     compute/gpu · compute/cpu · storage/disk · ai/agent
//!
//! Layout (`idx % 4` selects the capability for every node):
//!   - 0–3   : primary providers (initially ON)
//!   - 4–7   : backup providers  (initially OFF, click to bring up)
//!   - 8–11  : requirers — each declares `req/{self}/{ns}/{name}` and runs
//!             `watch_requirement` so its tile flips Satisfied/Unsatisfied
//!             live as providers come and go.
//!
//! The middle column shows one card per capability with a live demand-pressure
//! bar (`demand_pressure = demanders / max(providers, 1)`), populated from
//! `GossipAgent::demand()`.
//!
//! Kill the only `compute/gpu` provider — every dependent requirement blinks
//! red within ~50 ms (the watcher debounce window from C2). Bring up the
//! backup and the wires snap back to green.
//!
//! Run:
//!   cargo run --example capability_market
//!
//! Then open http://127.0.0.1:8097

use mycelium::{
    CapFilter, Capability, CapabilityHandle, GossipAgent, GossipConfig, NodeId,
    RequirementHandle, RequirementStatus,
};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::{signal, time};

#[cfg(unix)]
fn raise_fd_limit(target: u64) {
    unsafe {
        let mut rl: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) != 0 { return; }
        if rl.rlim_cur >= target { return; }
        rl.rlim_cur = target.min(rl.rlim_max);
        libc::setrlimit(libc::RLIMIT_NOFILE, &rl);
    }
}
#[cfg(not(unix))]
fn raise_fd_limit(_: u64) {}

const N_NODES: usize  = 12;
const BASE_PORT: u16  = 55000;
const HTTP_PORT: u16  = 8097;
const SETTLE_MS: u64  = 2_500;
const REASSERT_SECS: u64 = 30;

const CAPS: &[(&str, &str)] = &[
    ("compute", "gpu"),
    ("compute", "cpu"),
    ("storage", "disk"),
    ("ai",      "agent"),
];

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

#[derive(Clone, Copy)]
enum Role {
    Provider { cap_idx: usize, primary: bool },
    Requirer { cap_idx: usize },
}

struct NodeRecord {
    role:        Role,
    on:          bool,
    cap_handle:  Option<CapabilityHandle>,
    req_handle:  Option<RequirementHandle>,
    last_status: String,
    flips:       u64,
}

struct Event { ts_ms: u64, message: String }

struct AppState {
    nodes:  Vec<Mutex<NodeRecord>>,
    viewer: Arc<GossipAgent>,
    events: Mutex<Vec<Event>>,
}

fn role_of(i: usize) -> Role {
    if i < 4       { Role::Provider { cap_idx: i,     primary: true  } }
    else if i < 8  { Role::Provider { cap_idx: i - 4, primary: false } }
    else           { Role::Requirer { cap_idx: i - 8 } }
}

fn push_event(app: &AppState, msg: String) {
    let mut e = app.events.lock().unwrap();
    e.push(Event { ts_ms: now_ms(), message: msg });
    if e.len() > 100 { e.drain(0..50); }
}

fn state_json(app: &AppState) -> String {
    // Nodes — per-index card payload.
    let nodes: Vec<String> = (0..N_NODES).map(|i| {
        let n = app.nodes[i].lock().unwrap();
        let (role, cap_idx, primary) = match n.role {
            Role::Provider { cap_idx, primary } => ("provider", cap_idx, primary),
            Role::Requirer { cap_idx }          => ("requirer", cap_idx, false),
        };
        let (ns, name) = CAPS[cap_idx];
        format!(
            r#"{{"id":{},"port":{},"role":"{}","primary":{},"ns":"{}","name":"{}","on":{},"status":"{}","flips":{}}}"#,
            i, BASE_PORT + i as u16, role, primary, ns, name, n.on,
            n.last_status.replace('"', "'"), n.flips,
        )
    }).collect();

    // Capability summary: per cap, ask the viewer for demand/providers.
    let caps: Vec<String> = CAPS.iter().enumerate().map(|(ci, &(ns, name))| {
        let filter = CapFilter::new(ns, name);
        let d = app.viewer.demand(&filter);
        let providers: Vec<String> = d.providers.iter()
            .map(|p| format!(r#""{}""#, p))
            .collect();
        let demanders: Vec<String> = d.demanding_nodes.iter()
            .map(|p| format!(r#""{}""#, p))
            .collect();
        format!(
            r#"{{"idx":{},"ns":"{}","name":"{}","providers":[{}],"demanders":[{}],"pressure":{:.3}}}"#,
            ci, ns, name,
            providers.join(","), demanders.join(","),
            d.demand_pressure,
        )
    }).collect();

    let evts: Vec<String> = app.events.lock().unwrap().iter().rev().take(40)
        .map(|e| format!(r#"{{"ts_ms":{},"message":"{}"}}"#, e.ts_ms, e.message.replace('"', "'")))
        .collect();

    format!(
        r#"{{"nodes":[{}],"caps":[{}],"events":[{}]}}"#,
        nodes.join(","),
        caps.join(","),
        evts.join(","),
    )
}

async fn serve_http(app: Arc<AppState>, agents: Arc<Vec<Arc<GossipAgent>>>) {
    let listener = TcpListener::bind(format!("127.0.0.1:{HTTP_PORT}")).await
        .expect("HTTP bind failed");
    eprintln!("╔══════════════════════════════════════════════════╗");
    eprintln!("║  Open → http://127.0.0.1:{HTTP_PORT}                  ║");
    eprintln!("╚══════════════════════════════════════════════════╝");
    loop {
        let Ok((mut stream, _)) = listener.accept().await else { continue };
        let app    = app.clone();
        let agents = agents.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            let req = std::str::from_utf8(&buf[..n]).unwrap_or("");

            if req.contains("GET /cmd") {
                let toggle = req.contains("op=toggle");
                if toggle {
                    if let Some(idx) = extract_param(req, "node=") {
                        if idx < N_NODES {
                            toggle_provider(&app, &agents, idx);
                        }
                    }
                }
                let _ = stream.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\nok"
                ).await;
                return;
            }
            if req.contains("GET /state") {
                let json = state_json(&app);
                let _ = stream.write_all(format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    json.len(), json
                ).as_bytes()).await;
                return;
            }
            let html = include_str!("../docs/capability_market.html");
            let _ = stream.write_all(format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                html.len(), html
            ).as_bytes()).await;
        });
    }
}

fn extract_param(req: &str, key: &str) -> Option<usize> {
    let pos = req.find(key)?;
    let tail = &req[pos + key.len()..];
    let end = tail.find(|c: char| !c.is_ascii_digit()).unwrap_or(tail.len());
    tail[..end].parse().ok()
}

fn toggle_provider(app: &AppState, agents: &[Arc<GossipAgent>], idx: usize) {
    let mut n = app.nodes[idx].lock().unwrap();
    let Role::Provider { cap_idx, .. } = n.role else { return; };
    let (ns, name) = CAPS[cap_idx];
    if n.on {
        // Drop the handle — `advertise_capability` retracts on drop.
        n.cap_handle = None;
        n.on = false;
        drop(n);
        push_event(app, format!("retract node-{idx} ({ns}/{name})"));
    } else {
        let h = agents[idx].advertise_capability(
            Capability::new(ns, name),
            Duration::from_secs(REASSERT_SECS),
        );
        n.cap_handle = Some(h);
        n.on = true;
        drop(n);
        push_event(app, format!("advertise node-{idx} ({ns}/{name})"));
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    raise_fd_limit(4096);

    let ports: Vec<u16> = (0..N_NODES).map(|i| BASE_PORT + i as u16).collect();
    let mut agents: Vec<Arc<GossipAgent>> = Vec::with_capacity(N_NODES);
    for &p in &ports {
        let nid = NodeId::new("127.0.0.1", p)?;
        let mut cfg = GossipConfig::default();
        cfg.bind_address               = "127.0.0.1".to_string();
        cfg.bind_port                  = p;
        cfg.default_ttl                = 10;
        cfg.gossip_shards              = 1;
        cfg.reconnect_backoff_secs     = 1;
        cfg.health_check_max_jitter_ms = 100;
        cfg.bootstrap_peers = ports.iter().filter(|&&pp| pp != p)
            .map(|&pp| NodeId::new("127.0.0.1", pp))
            .collect::<Result<Vec<_>, _>>()?;
        agents.push(Arc::new(GossipAgent::new(nid, cfg)));
    }
    eprintln!("Starting {N_NODES} agents…");
    for a in &agents { a.start().await?; }

    // Build initial node records: primary providers advertise immediately;
    // requirers declare immediately; backup providers stay idle.
    let mut records: Vec<Mutex<NodeRecord>> = Vec::with_capacity(N_NODES);
    for i in 0..N_NODES {
        let role = role_of(i);
        let mut rec = NodeRecord {
            role,
            on: false,
            cap_handle: None,
            req_handle: None,
            last_status: "—".to_string(),
            flips: 0,
        };
        match role {
            Role::Provider { cap_idx, primary } if primary => {
                let (ns, name) = CAPS[cap_idx];
                rec.cap_handle = Some(agents[i].advertise_capability(
                    Capability::new(ns, name),
                    Duration::from_secs(REASSERT_SECS),
                ));
                rec.on = true;
            }
            Role::Requirer { cap_idx } => {
                let (ns, name) = CAPS[cap_idx];
                rec.req_handle = Some(agents[i].declare_requirement(
                    CapFilter::new(ns, name),
                    Duration::from_secs(REASSERT_SECS),
                ));
                rec.on = true;
                rec.last_status = "Unsatisfied".to_string();
            }
            _ => {}
        }
        records.push(Mutex::new(rec));
    }

    let app = Arc::new(AppState {
        nodes:  records,
        viewer: agents[0].clone(),
        events: Mutex::new(Vec::new()),
    });

    // One watch_requirement task per requirer. Updates the node's last_status
    // text whenever the watcher fires (debounced server-side by C2).
    for i in 8..N_NODES {
        let agent_i = agents[i].clone();
        let app_i   = app.clone();
        let cap_idx = i - 8;
        let (ns, name) = CAPS[cap_idx];
        tokio::spawn(async move {
            let mut rx = agent_i.watch_requirement(CapFilter::new(ns, name));
            loop {
                let txt = match &*rx.borrow_and_update() {
                    RequirementStatus::Satisfied { providers } => format!("Satisfied · {} provider(s)", providers.len()),
                    RequirementStatus::Unsatisfied { .. }       => "Unsatisfied".to_string(),
                };
                {
                    let mut n = app_i.nodes[i].lock().unwrap();
                    if n.last_status != txt {
                        push_event(&app_i, format!("node-{i} → {txt}"));
                        n.last_status = txt;
                        n.flips += 1;
                    }
                }
                if rx.changed().await.is_err() { break; }
            }
        });
    }

    let agents_arc = Arc::new(agents.clone());
    tokio::spawn(serve_http(app.clone(), agents_arc));

    time::sleep(Duration::from_millis(SETTLE_MS)).await;
    eprintln!("Capability market live. Toggle providers from the browser.");

    signal::ctrl_c().await?;
    eprintln!("\nShutting down…");
    for a in &agents { a.shutdown().await; }
    Ok(())
}
