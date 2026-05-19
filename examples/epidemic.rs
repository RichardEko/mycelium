//! Epidemic Wave — Layer 2 Signal Propagation
//!
//! 16 GossipAgents in a ring topology (ports 54000–54015), split into two
//! groups: "alpha" (nodes 0–7) and "beta" (nodes 8–15).
//!
//! Three signal kinds let you compare scope behaviour live:
//!   sys.pulse   — System scope: forwarded AND acted by all 16 nodes.
//!   alpha.pulse — Group scope:  forwarded by all, acted only by alpha.
//!   beta.pulse  — Group scope:  forwarded by all, acted only by beta.
//!
//! The key Layer 2 invariant — *forwarding is unconditional, acting is
//! filtered* — is visible as the colour wave: every node changes shade as
//! the gossip packet hops past, but only the in-scope nodes light up fully.
//!
//! Run:
//!   cargo run --example epidemic
//!
//! Then open http://127.0.0.1:8092

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

const N: usize = 16;
const BASE_PORT: u16 = 54000;
const HTTP_PORT: u16 = 8092;
const SETTLE_MS: u64 = 2_500;
const AUTO_PULSE_MS: u64 = 6_000;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Default, Clone)]
struct NodeState {
    last_sys_ms:   u64,
    last_alpha_ms: u64,
    last_beta_ms:  u64,
    sys_count:     u64,
    alpha_count:   u64,
    beta_count:    u64,
}

struct AppState {
    nodes:        Vec<Arc<Mutex<NodeState>>>,
    pulse_count:  Mutex<u64>,
    last_emit_ms: Mutex<u64>,
    last_scope:   Mutex<String>,
}

fn state_json(app: &AppState) -> String {
    let pulse_count  = *app.pulse_count.lock().unwrap();
    let last_emit_ms = *app.last_emit_ms.lock().unwrap();
    let last_scope   = app.last_scope.lock().unwrap().clone();
    let nodes: Vec<String> = app.nodes.iter().enumerate().map(|(i, ns)| {
        let s     = ns.lock().unwrap();
        let group = if i < N / 2 { "alpha" } else { "beta" };
        format!(
            r#"{{"id":{},"port":{},"group":"{}","last_sys_ms":{},"last_alpha_ms":{},"last_beta_ms":{},"sys_count":{},"alpha_count":{},"beta_count":{}}}"#,
            i, BASE_PORT + i as u16, group,
            s.last_sys_ms, s.last_alpha_ms, s.last_beta_ms,
            s.sys_count, s.alpha_count, s.beta_count,
        )
    }).collect();
    format!(
        r#"{{"pulse_count":{},"last_emit_ms":{},"last_scope":"{}","n":{},"nodes":[{}]}}"#,
        pulse_count, last_emit_ms, last_scope, N, nodes.join(",")
    )
}

async fn serve_http(app: Arc<AppState>, emitter: Arc<GossipAgent>) {
    let listener = TcpListener::bind(format!("127.0.0.1:{HTTP_PORT}")).await
        .expect("HTTP bind failed");
    eprintln!("╔══════════════════════════════════════════════════╗");
    eprintln!("║  Open → http://127.0.0.1:{HTTP_PORT}                  ║");
    eprintln!("╚══════════════════════════════════════════════════╝");
    loop {
        let Ok((mut stream, _)) = listener.accept().await else { continue };
        let app     = app.clone();
        let emitter = emitter.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            let req = std::str::from_utf8(&buf[..n]).unwrap_or("");

            if req.contains("GET /trigger") {
                let scope_str = if req.contains("scope=alpha") { "alpha" }
                    else if req.contains("scope=beta") { "beta" }
                    else { "sys" };
                fire_pulse(&app, &emitter, scope_str);
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
            let html = include_str!("../docs/epidemic.html");
            let _ = stream.write_all(format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                html.len(), html
            ).as_bytes()).await;
        });
    }
}

fn fire_pulse(app: &AppState, emitter: &GossipAgent, scope: &str) {
    let t = now_ms();
    *app.last_emit_ms.lock().unwrap() = t;
    *app.pulse_count.lock().unwrap() += 1;
    *app.last_scope.lock().unwrap()   = scope.to_string();
    let payload = Bytes::copy_from_slice(&t.to_le_bytes());
    let _ = match scope {
        "alpha" => emitter.emit("alpha.pulse", SignalScope::Group("alpha".into()), payload),
        "beta"  => emitter.emit("beta.pulse",  SignalScope::Group("beta".into()),  payload),
        _       => emitter.emit("sys.pulse",   SignalScope::System,                payload),
    };
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    raise_fd_limit(4096);

    let mut agents: Vec<Arc<GossipAgent>> = Vec::with_capacity(N);
    for i in 0..N {
        let p   = BASE_PORT + i as u16;
        let nid = NodeId::new("127.0.0.1", p)?;
        let mut cfg = GossipConfig::default();
        cfg.bind_address               = "127.0.0.1".to_string();
        cfg.bind_port                  = p;
        cfg.default_ttl                = 20;
        cfg.reconnect_backoff_secs     = 1;
        cfg.gossip_shards              = 1;
        cfg.health_check_max_jitter_ms = 100;
        // Ring: each node connects only to its two immediate neighbours.
        // TTL=20 lets signals cross all 16 hops even through a sparse ring.
        cfg.bootstrap_peers = vec![
            NodeId::new("127.0.0.1", BASE_PORT + ((i + N - 1) % N) as u16)?,
            NodeId::new("127.0.0.1", BASE_PORT + ((i + 1)     % N) as u16)?,
        ];
        cfg.max_peers            = 2;
        cfg.max_forwarding_peers = 2;
        agents.push(Arc::new(GossipAgent::new(nid, cfg)));
    }

    eprintln!("Starting {N} agents in ring topology…");
    for a in &agents { a.start().await?; }

    // Group memberships
    for i in 0..N {
        if i < N / 2 {
            agents[i].join_group("alpha");
        } else {
            agents[i].join_group("beta");
        }
    }

    let app = Arc::new(AppState {
        nodes:        (0..N).map(|_| Arc::new(Mutex::new(NodeState::default()))).collect(),
        pulse_count:  Mutex::new(0),
        last_emit_ms: Mutex::new(0),
        last_scope:   Mutex::new("sys".to_string()),
    });

    // Subscribe each node to all three signal kinds
    for i in 0..N {
        let ns = app.nodes[i].clone();

        let mut sys_rx = agents[i].signal_rx("sys.pulse");
        let ns2 = ns.clone();
        tokio::spawn(async move {
            while sys_rx.recv().await.is_some() {
                let t = now_ms();
                let mut s = ns2.lock().unwrap();
                s.last_sys_ms = t;
                s.sys_count  += 1;
            }
        });

        let mut alpha_rx = agents[i].signal_rx("alpha.pulse");
        let ns3 = ns.clone();
        tokio::spawn(async move {
            while alpha_rx.recv().await.is_some() {
                let t = now_ms();
                let mut s = ns3.lock().unwrap();
                s.last_alpha_ms = t;
                s.alpha_count   += 1;
            }
        });

        let mut beta_rx = agents[i].signal_rx("beta.pulse");
        let ns4 = ns.clone();
        tokio::spawn(async move {
            while beta_rx.recv().await.is_some() {
                let t = now_ms();
                let mut s = ns4.lock().unwrap();
                s.last_beta_ms = t;
                s.beta_count   += 1;
            }
        });
    }

    let app_srv = app.clone();
    let emitter = agents[0].clone();
    tokio::spawn(serve_http(app_srv, emitter.clone()));

    time::sleep(Duration::from_millis(SETTLE_MS)).await;
    eprintln!("Mesh settled. Auto-pulsing every {AUTO_PULSE_MS}ms.");

    // Rotate through sys / alpha / beta automatically
    let pulse_app = app.clone();
    let pulse_agent = emitter.clone();
    tokio::spawn(async move {
        let mut ticker = time::interval(Duration::from_millis(AUTO_PULSE_MS));
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        let scopes = ["sys", "alpha", "beta"];
        let mut idx = 0usize;
        loop {
            ticker.tick().await;
            fire_pulse(&pulse_app, &pulse_agent, scopes[idx % 3]);
            idx += 1;
        }
    });

    signal::ctrl_c().await?;
    eprintln!("\nShutting down…");
    for a in &agents { a.shutdown().await; }
    Ok(())
}
