//! Circuit Breaker / Watchdog — Layer 2 Quorum Health Monitoring
//!
//! 6 service nodes (ports 54200–54205) emit "heartbeat" signals every second.
//! A supervisor node (port 54206) uses `quorum_persistent` to count how many
//! distinct senders delivered a heartbeat within the last 3 seconds, surviving
//! restarts because the evidence lives in the Layer 1 KV store.
//!
//! When fewer than 4 out of 6 services are alive, the supervisor opens a
//! "circuit breaker". The browser shows individual service liveness, the
//! quorum counter, and circuit state — with Kill/Restart controls.
//!
//! Run:
//!   cargo run --example watchdog
//!
//! Then open http://127.0.0.1:8094

use bytes::Bytes;
use gossip_protocol::{GossipAgent, GossipConfig, NodeId, SignalScope};
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

const N_SERVICES: usize = 6;
const BASE_SERVICE: u16 = 54200;
const SUPERVISOR_PORT: u16 = 54206;
const HTTP_PORT: u16 = 8094;
const SETTLE_MS: u64 = 2_500;
const HEARTBEAT_MS: u64 = 1_000;
const QUORUM_WINDOW_SECS: u64 = 3;
const QUORUM_THRESHOLD: usize = 4; // circuit opens below this

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Clone)]
struct ServiceState {
    emit_count:      u64,
    alive:           bool,  // controlled by Kill/Restart
    last_heartbeat_ms: u64,
}

impl Default for ServiceState {
    fn default() -> Self { Self { emit_count: 0, alive: true, last_heartbeat_ms: 0 } }
}

#[derive(Default, Clone)]
struct SupervisorState {
    quorum_count:  usize,
    circuit_open:  bool,
    check_count:   u64,
}

struct Event {
    ts_ms:   u64,
    message: String,
}

struct AppState {
    services:   Vec<Arc<Mutex<ServiceState>>>,
    supervisor: Arc<Mutex<SupervisorState>>,
    events:     Arc<Mutex<Vec<Event>>>,
}

fn state_json(app: &AppState) -> String {
    let svcs: Vec<String> = app.services.iter().enumerate().map(|(i, ss)| {
        let s = ss.lock().unwrap();
        format!(
            r#"{{"id":{},"port":{},"alive":{},"emit_count":{},"last_heartbeat_ms":{}}}"#,
            i, BASE_SERVICE + i as u16,
            s.alive, s.emit_count, s.last_heartbeat_ms
        )
    }).collect();

    let sup = app.supervisor.lock().unwrap();
    let evts: Vec<String> = app.events.lock().unwrap().iter().rev().take(20).map(|e| {
        format!(r#"{{"ts_ms":{},"message":"{}"}}"#, e.ts_ms, e.message)
    }).collect();

    format!(
        r#"{{"services":[{}],"supervisor":{{"port":{},"quorum_count":{},"threshold":{},"circuit_open":{},"check_count":{}}},"events":[{}]}}"#,
        svcs.join(","),
        SUPERVISOR_PORT, sup.quorum_count, QUORUM_THRESHOLD, sup.circuit_open, sup.check_count,
        evts.join(",")
    )
}

fn push_event(app: &AppState, msg: &str) {
    let mut evts = app.events.lock().unwrap();
    evts.push(Event { ts_ms: now_ms(), message: msg.to_string() });
    if evts.len() > 100 { evts.drain(0..50); }
}

async fn serve_http(app: Arc<AppState>) {
    let listener = TcpListener::bind(format!("127.0.0.1:{HTTP_PORT}")).await
        .expect("HTTP bind failed");
    eprintln!("╔══════════════════════════════════════════════════╗");
    eprintln!("║  Open → http://127.0.0.1:{HTTP_PORT}                  ║");
    eprintln!("╚══════════════════════════════════════════════════╝");
    loop {
        let Ok((mut stream, _)) = listener.accept().await else { continue };
        let app = app.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            let req = std::str::from_utf8(&buf[..n]).unwrap_or("");

            // /cmd?op=kill&node=N  or  /cmd?op=restart&node=N
            if req.contains("GET /cmd") {
                let op_kill    = req.contains("op=kill");
                let op_restart = req.contains("op=restart");
                if let Some(idx) = extract_node_id(req) {
                    if idx < N_SERVICES {
                        let mut s = app.services[idx].lock().unwrap();
                        s.alive = op_restart || !op_kill;
                        let verb = if s.alive { "restart" } else { "kill" };
                        drop(s);
                        push_event(&app, &format!("{verb} service-{idx}"));
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
            let html = include_str!("../docs/watchdog.html");
            let _ = stream.write_all(format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                html.len(), html
            ).as_bytes()).await;
        });
    }
}

fn extract_node_id(req: &str) -> Option<usize> {
    let node_idx = req.find("node=")?;
    let s = &req[node_idx + 5..];
    let end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    s[..end].parse().ok()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    raise_fd_limit(4096);

    let all_ports: Vec<u16> = (0..N_SERVICES).map(|i| BASE_SERVICE + i as u16)
        .chain(std::iter::once(SUPERVISOR_PORT))
        .collect();

    let mut agents: Vec<Arc<GossipAgent>> = Vec::new();
    for &p in &all_ports {
        let nid = NodeId::new("127.0.0.1", p)?;
        let mut cfg = GossipConfig::default();
        cfg.bind_address               = "127.0.0.1".to_string();
        cfg.bind_port                  = p;
        cfg.default_ttl                = 10;
        cfg.reconnect_backoff_secs     = 1;
        cfg.gossip_shards              = 1;
        cfg.health_check_max_jitter_ms = 100;
        cfg.bootstrap_peers = all_ports.iter().filter(|&&pp| pp != p)
            .map(|&pp| NodeId::new("127.0.0.1", pp))
            .collect::<Result<Vec<_>, _>>()?;
        agents.push(Arc::new(GossipAgent::new(nid, cfg)));
    }

    eprintln!("Starting {N_SERVICES} service agents + supervisor…");
    for a in &agents { a.start().await?; }

    let app = Arc::new(AppState {
        services:   (0..N_SERVICES).map(|_| Arc::new(Mutex::new(ServiceState::default()))).collect(),
        supervisor: Arc::new(Mutex::new(SupervisorState::default())),
        events:     Arc::new(Mutex::new(Vec::new())),
    });

    // Service heartbeat tasks
    for i in 0..N_SERVICES {
        let agent      = agents[i].clone();
        let svc_state  = app.services[i].clone();
        tokio::spawn(async move {
            let mut ticker = time::interval(Duration::from_millis(HEARTBEAT_MS));
            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                let alive = svc_state.lock().unwrap().alive;
                if alive {
                    let t = now_ms();
                    let _ = agent.emit("heartbeat", SignalScope::System,
                        Bytes::copy_from_slice(&t.to_le_bytes()));
                    let mut s = svc_state.lock().unwrap();
                    s.emit_count += 1;
                    s.last_heartbeat_ms = t;
                }
            }
        });
    }

    // Supervisor: poll quorum_persistent every 500ms
    let supervisor = agents[N_SERVICES].clone();
    let sup_state  = app.supervisor.clone();
    let events     = app.events.clone();
    tokio::spawn(async move {
        let window = Duration::from_secs(QUORUM_WINDOW_SECS);
        let mut prev_circuit = false;
        loop {
            time::sleep(Duration::from_millis(500)).await;
            let count = supervisor.quorum_persistent("heartbeat", window);
            let circuit_open = count < QUORUM_THRESHOLD;
            {
                let mut s = sup_state.lock().unwrap();
                s.quorum_count = count;
                s.circuit_open = circuit_open;
                s.check_count += 1;
            }
            if circuit_open != prev_circuit {
                let msg = if circuit_open {
                    format!("CIRCUIT OPEN — quorum {count}/{QUORUM_THRESHOLD} (need ≥{QUORUM_THRESHOLD})")
                } else {
                    format!("circuit closed — quorum restored {count}/{N_SERVICES}")
                };
                events.lock().unwrap().push(Event { ts_ms: now_ms(), message: msg });
                prev_circuit = circuit_open;
            }
        }
    });

    let app_srv = app.clone();
    tokio::spawn(serve_http(app_srv));

    time::sleep(Duration::from_millis(SETTLE_MS)).await;
    eprintln!("Watchdog running. Supervisor checks quorum every 500ms.");
    eprintln!("Circuit opens when < {QUORUM_THRESHOLD} of {N_SERVICES} services are alive.");

    signal::ctrl_c().await?;
    eprintln!("\nShutting down…");
    for a in &agents { a.shutdown().await; }
    Ok(())
}
