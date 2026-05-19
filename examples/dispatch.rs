//! Worker Dispatch Pool — Layer 2 Adaptive Routing
//!
//! 8 worker nodes (ports 54100–54107) and 2 dispatcher nodes (54108–54109).
//! Workers receive tasks via Individual-scope "task" signals and process them
//! at varying rates (fast workers: 150 ms/task, slow: 700 ms/task).
//!
//! Each worker calls `manage_opacity("task", …)` to broadcast its load state
//! via Layer I (`sys/load/{id}/task`). Dispatchers call `peer_load()` every
//! 300 ms to read the cluster's load map, then emit the next task to the
//! least-loaded (least-opaque) worker.
//!
//! As workers saturate, their queue fill → 1.0, opacity governor fires
//! BOUNDARY_OPAQUE, and dispatchers automatically route around them.
//!
//! Run:
//!   cargo run --example dispatch
//!
//! Then open http://127.0.0.1:8093

use bytes::Bytes;
use mycelium::{GossipAgent, GossipConfig, NodeId, OpacityHint, SignalScope};
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

const N_WORKERS: usize  = 8;
const N_DISPATCH: usize = 2;
const BASE_WORKER: u16  = 54100;
const BASE_DISPATCH: u16 = 54108;
const HTTP_PORT: u16    = 8093;
const SETTLE_MS: u64    = 2_500;
const DISPATCH_INTERVAL_MS: u64 = 300;

// ms per task for each worker (index 0-7). Workers 4-7 are twice as slow.
const PROCESS_MS: [u64; N_WORKERS] = [150, 180, 160, 170, 700, 750, 680, 720];

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Default, Clone)]
struct WorkerState {
    queue_depth:   usize,
    handled:       u64,
    fill_ratio:    f32,
    is_opaque:     bool,
    last_task_ms:  u64,
}

#[derive(Default, Clone)]
struct DispatchState {
    dispatched:    u64,
    last_target:   Option<usize>,
    last_dispatch_ms: u64,
}

struct AppState {
    workers:    Vec<Arc<Mutex<WorkerState>>>,
    dispatchers: Vec<Arc<Mutex<DispatchState>>>,
    burst_pending: Mutex<bool>,
}

fn state_json(app: &AppState, agents: &[Arc<GossipAgent>]) -> String {
    let workers: Vec<String> = app.workers.iter().enumerate().map(|(i, ws)| {
        let s = ws.lock().unwrap();
        // Read fill_ratio from peer_load as seen by dispatcher 0
        let fill = s.fill_ratio;
        format!(
            r#"{{"id":{},"port":{},"queue":{},"fill":{:.3},"is_opaque":{},"handled":{},"process_ms":{}}}"#,
            i, BASE_WORKER + i as u16,
            s.queue_depth, fill, s.is_opaque, s.handled, PROCESS_MS[i]
        )
    }).collect();

    let dispatchers: Vec<String> = app.dispatchers.iter().enumerate().map(|(i, ds)| {
        let s = ds.lock().unwrap();
        let target = s.last_target.map(|t| t.to_string()).unwrap_or_else(|| "null".to_string());
        format!(
            r#"{{"id":{},"port":{},"dispatched":{},"last_target":{}}}"#,
            i, BASE_DISPATCH + i as u16, s.dispatched, target
        )
    }).collect();

    // peer_load from dispatcher-0's perspective
    let load_entries: Vec<String> = agents[N_WORKERS].peer_load(Duration::from_secs(5))
        .iter()
        .filter(|(_, k, _)| k.as_ref() == "task")
        .map(|(n, _, s)| format!(r#"{{"node":"{}","fill":{:.3},"opaque":{}}}"#, n, s.fill_ratio, s.is_opaque))
        .collect();

    format!(
        r#"{{"workers":[{}],"dispatchers":[{}],"peer_load":[{}]}}"#,
        workers.join(","), dispatchers.join(","), load_entries.join(",")
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
            let mut buf = [0u8; 512];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            let req = std::str::from_utf8(&buf[..n]).unwrap_or("");

            if req.contains("GET /burst") {
                *app.burst_pending.lock().unwrap() = true;
                let _ = stream.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\nok"
                ).await;
                return;
            }
            if req.contains("GET /state") {
                let json = state_json(&app, &agents);
                let _ = stream.write_all(format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    json.len(), json
                ).as_bytes()).await;
                return;
            }
            let html = include_str!("../docs/dispatch.html");
            let _ = stream.write_all(format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                html.len(), html
            ).as_bytes()).await;
        });
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    raise_fd_limit(4096);

    // Build worker agents
    let mut agents: Vec<Arc<GossipAgent>> = Vec::new();
    let all_ports: Vec<u16> = (0..N_WORKERS).map(|i| BASE_WORKER + i as u16)
        .chain((0..N_DISPATCH).map(|i| BASE_DISPATCH + i as u16))
        .collect();

    for i in 0..(N_WORKERS + N_DISPATCH) {
        let p   = all_ports[i];
        let nid = NodeId::new("127.0.0.1", p)?;
        let mut cfg = GossipConfig::default();
        cfg.bind_address               = "127.0.0.1".to_string();
        cfg.bind_port                  = p;
        cfg.default_ttl                = 10;
        cfg.reconnect_backoff_secs     = 1;
        cfg.gossip_shards              = 1;
        cfg.health_check_max_jitter_ms = 100;
        // Full mesh: every node connects to every other node
        cfg.bootstrap_peers = all_ports.iter().filter(|&&pp| pp != p)
            .map(|&pp| NodeId::new("127.0.0.1", pp))
            .collect::<Result<Vec<_>, _>>()?;
        agents.push(Arc::new(GossipAgent::new(nid, cfg)));
    }

    eprintln!("Starting {N_WORKERS} workers + {N_DISPATCH} dispatchers…");
    for a in &agents { a.start().await?; }
    for i in 0..N_WORKERS { agents[i].join_group("workers"); }

    let app = Arc::new(AppState {
        workers:    (0..N_WORKERS).map(|_| Arc::new(Mutex::new(WorkerState::default()))).collect(),
        dispatchers: (0..N_DISPATCH).map(|_| Arc::new(Mutex::new(DispatchState::default()))).collect(),
        burst_pending: Mutex::new(false),
    });

    // Worker tasks: receive "task" signals, track queue, use manage_opacity
    let mut opacity_handles = Vec::new();
    for i in 0..N_WORKERS {
        let worker_state = app.workers[i].clone();
        let agent        = agents[i].clone();
        let process_ms   = PROCESS_MS[i];

        // Opacity governor monitors "task" channel fill
        let _opacity = agent.manage_opacity(
            "task",
            SignalScope::System,
            OpacityHint { threshold: 0.60, hysteresis: 0.20, payload: Bytes::new() },
        );
        opacity_handles.push(_opacity);

        // Task processing loop
        let mut task_rx = agent.signal_rx_with_capacity("task", 12);
        tokio::spawn(async move {
            loop {
                match task_rx.recv().await {
                    Some(_task) => {
                        { let mut s = worker_state.lock().unwrap(); s.queue_depth += 1; }
                        time::sleep(Duration::from_millis(process_ms)).await;
                        { let mut s = worker_state.lock().unwrap(); s.queue_depth = s.queue_depth.saturating_sub(1); s.handled += 1; s.last_task_ms = now_ms(); }
                    }
                    None => break,
                }
            }
        });
    }

    // Poll peer_load to update worker state snapshots (for JSON)
    let app_load = app.clone();
    let dispatcher0 = agents[N_WORKERS].clone();
    tokio::spawn(async move {
        loop {
            time::sleep(Duration::from_millis(300)).await;
            let loads = dispatcher0.peer_load(Duration::from_secs(5));
            for i in 0..N_WORKERS {
                let port_str = format!("127.0.0.1:{}", BASE_WORKER + i as u16);
                if let Some((_, _, ls)) = loads.iter().find(|(n, k, _)| n.as_ref() == port_str && k.as_ref() == "task") {
                    let mut s = app_load.workers[i].lock().unwrap();
                    s.fill_ratio = ls.fill_ratio;
                    s.is_opaque  = ls.is_opaque;
                }
            }
        }
    });

    // Dispatcher tasks: route tasks to least-loaded worker
    for d in 0..N_DISPATCH {
        let app_d = app.clone();
        let dispatcher = agents[N_WORKERS + d].clone();
        let worker_agents: Vec<Arc<GossipAgent>> = (0..N_WORKERS).map(|i| agents[i].clone()).collect();
        tokio::spawn(async move {
            let mut ticker = time::interval(Duration::from_millis(DISPATCH_INTERVAL_MS));
            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;

                // Burst: inject 20 rapid tasks to saturate slow workers
                let burst = {
                    let mut b = app_d.burst_pending.lock().unwrap();
                    if *b && d == 0 { *b = false; true } else { false }
                };

                let n_tasks = if burst { 20 } else { 1 };
                for _ in 0..n_tasks {
                    // Pick least-loaded worker via peer_load
                    let loads = dispatcher.peer_load(Duration::from_secs(5));
                    let target = (0..N_WORKERS).min_by_key(|&i| {
                        let port_str = format!("127.0.0.1:{}", BASE_WORKER + i as u16);
                        let fill = loads.iter()
                            .find(|(n, k, _)| n.as_ref() == port_str && k.as_ref() == "task")
                            .map(|(_, _, ls)| (ls.fill_ratio * 1000.0) as u32)
                            .unwrap_or(0);
                        fill
                    }).unwrap_or(0);

                    let worker_id = worker_agents[target].node_id().clone();
                    let _ = dispatcher.emit("task", SignalScope::Individual(worker_id),
                        Bytes::from_static(b"work"));

                    let mut ds = app_d.dispatchers[d].lock().unwrap();
                    ds.dispatched += 1;
                    ds.last_target = Some(target);
                    ds.last_dispatch_ms = now_ms();
                }
            }
        });
    }

    let agents_arc = Arc::new(agents.clone());
    let app_srv    = app.clone();
    tokio::spawn(serve_http(app_srv, agents_arc));

    time::sleep(Duration::from_millis(SETTLE_MS)).await;
    eprintln!("Dispatching tasks every {DISPATCH_INTERVAL_MS}ms. Fast workers: 0-3, slow: 4-7.");

    signal::ctrl_c().await?;
    eprintln!("\nShutting down…");
    drop(opacity_handles);
    for a in &agents { a.shutdown().await; }
    Ok(())
}
