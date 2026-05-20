//! Emergent Pool — Phase 3g/3h cap-groups + Phase 4 signal_wired_via
//!
//! 20 worker nodes (ports 55100–55119) plus 2 consumers (55120, 55121).
//! Node 0 publishes `cap-group/gpu-pool` whose filter is `compute/gpu`.
//! Any worker that advertises `compute/gpu` auto-joins the emergent group
//! via `watch_capability_group_definitions` and writes a per-member
//! projection under `gcap/gpu-pool/compute/gpu/{self}`.
//!
//! Consumers periodically fire `signal_wired_via` against the same filter.
//! The resolver picks one provider per outgoing signal; the line from
//! consumer to chosen worker flashes in the browser. Toggle a worker's
//! `compute/gpu` advertisement and watch it slide in/out of the pool ring.
//!
//! C3 in action: the worker's membership task is ONE per group, regardless
//! of how many `provides` or `requires` the def carries. The signal-out
//! count on every worker tile is incremented by the per-worker signal
//! receiver, demonstrating that the resolver actually picked it.
//!
//! Run:
//!   cargo run --example emergent_pool
//!
//! Then open http://127.0.0.1:8098
use bytes::Bytes;
use mycelium::{
    CapFilter, Capability, CapabilityGroupDef, CapabilityGroupHandle, CapabilityHandle,
    GossipAgent, GossipConfig, NodeId, WiredEmitOutcome, WiringProvider,
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

const N_WORKERS:  usize = 20;
const N_CONSUMERS: usize = 2;
const BASE_PORT:  u16   = 55100;
const HTTP_PORT:  u16   = 8098;
const SETTLE_MS:  u64   = 3_000;
const REASSERT_SECS: u64 = 30;
const CONSUMER_INTERVAL_MS: u64 = 700;
const GROUP_NAME: &str = "gpu-pool";

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

fn worker_port(i: usize)   -> u16 { BASE_PORT + i as u16 }
fn consumer_port(i: usize) -> u16 { BASE_PORT + N_WORKERS as u16 + i as u16 }

struct WorkerRecord {
    has_cap:        bool,
    cap_handle:     Option<CapabilityHandle>,
    signals_in:     u64,
    last_signal_ms: u64,
}

struct ConsumerRecord {
    signals_sent:    u64,
    last_outcome:    String,
    last_target_id:  Option<usize>,
}

struct FlashRecord {
    consumer_id: usize,
    target_id:   usize,
    ts_ms:       u64,
}

struct Event { ts_ms: u64, message: String }

struct AppState {
    workers:   Vec<Mutex<WorkerRecord>>,
    consumers: Vec<Mutex<ConsumerRecord>>,
    flashes:   Mutex<Vec<FlashRecord>>,
    events:    Mutex<Vec<Event>>,
}

fn push_event(app: &AppState, msg: String) {
    let mut e = app.events.lock().unwrap();
    e.push(Event { ts_ms: now_ms(), message: msg });
    if e.len() > 100 { e.drain(0..50); }
}

fn record_flash(app: &AppState, consumer_id: usize, target_id: usize) {
    let mut f = app.flashes.lock().unwrap();
    f.push(FlashRecord { consumer_id, target_id, ts_ms: now_ms() });
    if f.len() > 60 { f.drain(0..30); }
}

fn state_json(app: &AppState) -> String {
    let workers: Vec<String> = (0..N_WORKERS).map(|i| {
        let w = app.workers[i].lock().unwrap();
        format!(
            r#"{{"id":{},"port":{},"has_cap":{},"signals_in":{},"last_signal_ms":{}}}"#,
            i, worker_port(i), w.has_cap, w.signals_in, w.last_signal_ms,
        )
    }).collect();

    let consumers: Vec<String> = (0..N_CONSUMERS).map(|i| {
        let c = app.consumers[i].lock().unwrap();
        let target = c.last_target_id.map(|n| n as i32).unwrap_or(-1);
        format!(
            r#"{{"id":{},"port":{},"signals_sent":{},"last_outcome":"{}","last_target":{}}}"#,
            i, consumer_port(i), c.signals_sent,
            c.last_outcome.replace('"', "'"),
            target,
        )
    }).collect();

    let cutoff = now_ms().saturating_sub(1_500);
    let flashes: Vec<String> = app.flashes.lock().unwrap().iter()
        .filter(|f| f.ts_ms >= cutoff)
        .map(|f| format!(r#"{{"from":{},"to":{},"ts":{}}}"#, f.consumer_id, f.target_id, f.ts_ms))
        .collect();

    let evts: Vec<String> = app.events.lock().unwrap().iter().rev().take(40)
        .map(|e| format!(r#"{{"ts_ms":{},"message":"{}"}}"#, e.ts_ms, e.message.replace('"', "'")))
        .collect();

    format!(
        r#"{{"workers":[{}],"consumers":[{}],"flashes":[{}],"events":[{}],"group":"{}"}}"#,
        workers.join(","),
        consumers.join(","),
        flashes.join(","),
        evts.join(","),
        GROUP_NAME,
    )
}

async fn serve_http(
    app:     Arc<AppState>,
    workers: Arc<Vec<Arc<GossipAgent>>>,
) {
    let listener = TcpListener::bind(format!("127.0.0.1:{HTTP_PORT}")).await
        .expect("HTTP bind failed");
    eprintln!("╔══════════════════════════════════════════════════╗");
    eprintln!("║  Open → http://127.0.0.1:{HTTP_PORT}                  ║");
    eprintln!("╚══════════════════════════════════════════════════╝");
    loop {
        let Ok((mut stream, _)) = listener.accept().await else { continue };
        let app     = app.clone();
        let workers = workers.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            let req = std::str::from_utf8(&buf[..n]).unwrap_or("");

            if req.contains("GET /cmd") {
                if let Some(idx) = extract_param(req, "node=") {
                    if req.contains("op=toggle") && idx < N_WORKERS {
                        toggle_worker(&app, &workers, idx);
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
            let html = include_str!("../docs/emergent_pool.html");
            let _ = stream.write_all(format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                html.len(), html
            ).as_bytes()).await;
        });
    }
}

fn extract_param(req: &str, key: &str) -> Option<usize> {
    let pos  = req.find(key)?;
    let tail = &req[pos + key.len()..];
    let end  = tail.find(|c: char| !c.is_ascii_digit()).unwrap_or(tail.len());
    tail[..end].parse().ok()
}

fn toggle_worker(app: &AppState, workers: &[Arc<GossipAgent>], idx: usize) {
    let mut w = app.workers[idx].lock().unwrap();
    if w.has_cap {
        w.cap_handle = None;
        w.has_cap = false;
        drop(w);
        push_event(app, format!("worker-{idx} retract compute/gpu"));
    } else {
        let h = workers[idx].advertise_capability(
            Capability::new("compute", "gpu"),
            Duration::from_secs(REASSERT_SECS),
        );
        w.cap_handle = Some(h);
        w.has_cap = true;
        drop(w);
        push_event(app, format!("worker-{idx} advertise compute/gpu (joins pool)"));
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    raise_fd_limit(4096);

    // Build all agents — workers first, then consumers.
    let all_ports: Vec<u16> =
        (0..N_WORKERS).map(worker_port)
        .chain((0..N_CONSUMERS).map(consumer_port))
        .collect();

    let make_cfg = |port: u16, bootstrap: Vec<NodeId>| {
        let mut cfg = GossipConfig::default();
        cfg.bind_address               = "127.0.0.1".to_string();
        cfg.bind_port                  = port;
        cfg.default_ttl                = 10;
        cfg.gossip_shards              = 1;
        cfg.reconnect_backoff_secs     = 1;
        cfg.health_check_max_jitter_ms = 100;
        cfg.bootstrap_peers            = bootstrap;
        cfg
    };

    let mut workers: Vec<Arc<GossipAgent>> = Vec::with_capacity(N_WORKERS);
    let mut consumers: Vec<Arc<GossipAgent>> = Vec::with_capacity(N_CONSUMERS);
    for &p in &all_ports {
        let bootstrap: Vec<NodeId> = all_ports.iter()
            .filter(|&&pp| pp != p)
            .map(|&pp| NodeId::new("127.0.0.1", pp))
            .collect::<Result<Vec<_>, _>>()?;
        let nid = NodeId::new("127.0.0.1", p)?;
        let cfg = make_cfg(p, bootstrap);
        let agent = Arc::new(GossipAgent::new(nid, cfg));
        if (p as usize) < BASE_PORT as usize + N_WORKERS {
            workers.push(agent);
        } else {
            consumers.push(agent);
        }
    }

    eprintln!("Starting {N_WORKERS} workers + {N_CONSUMERS} consumers…");
    for a in workers.iter().chain(consumers.iter()) { a.start().await?; }

    // Initial: roughly half the workers advertise compute/gpu.
    let mut worker_records: Vec<Mutex<WorkerRecord>> = Vec::with_capacity(N_WORKERS);
    for i in 0..N_WORKERS {
        let has = i % 2 == 0;
        let cap_handle = if has {
            Some(workers[i].advertise_capability(
                Capability::new("compute", "gpu"),
                Duration::from_secs(REASSERT_SECS),
            ))
        } else { None };
        worker_records.push(Mutex::new(WorkerRecord {
            has_cap: has,
            cap_handle,
            signals_in: 0,
            last_signal_ms: 0,
        }));
    }
    let consumer_records: Vec<Mutex<ConsumerRecord>> = (0..N_CONSUMERS).map(|_| {
        Mutex::new(ConsumerRecord {
            signals_sent: 0,
            last_outcome: "—".to_string(),
            last_target_id: None,
        })
    }).collect();

    let app = Arc::new(AppState {
        workers:   worker_records,
        consumers: consumer_records,
        flashes:   Mutex::new(Vec::new()),
        events:    Mutex::new(Vec::new()),
    });

    // The emergent group is defined by worker 0 (any node would do). Held in a
    // Box that lives as long as main() so the def's tombstone doesn't fire.
    let group_def = CapabilityGroupDef {
        filter:          CapFilter::new("compute", "gpu"),
        topology_policy: None,
        provides:        vec![Capability::new("compute", "gpu")],
        requires:        vec![],
    };
    let _grp_handle: CapabilityGroupHandle = workers[0].define_capability_group(
        GROUP_NAME,
        group_def,
        Duration::from_secs(REASSERT_SECS),
    );

    // Each worker subscribes to its inbox of `render-job` signals so that when
    // the consumer's signal_wired_via routes to it, we can increment a counter.
    for (i, a) in workers.iter().enumerate() {
        let mut rx = a.signal_rx("render-job");
        let app_w  = app.clone();
        tokio::spawn(async move {
            while let Some(_sig) = rx.recv().await {
                let mut w = app_w.workers[i].lock().unwrap();
                w.signals_in += 1;
                w.last_signal_ms = now_ms();
            }
        });
    }

    // Each consumer fires signals on its own cadence. Stagger the start so the
    // two consumers don't always pick the same provider.
    for (i, a) in consumers.iter().enumerate() {
        let app_c = app.clone();
        let agent = a.clone();
        let stagger = Duration::from_millis(150 * i as u64);
        tokio::spawn(async move {
            time::sleep(stagger).await;
            let mut ticker = time::interval(Duration::from_millis(CONSUMER_INTERVAL_MS));
            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                let filter = CapFilter::new("compute", "gpu");
                let payload = Bytes::from_static(b"render");
                let outcome = agent.signal_wired_via(&filter, "render-job", payload);
                let (text, target) = describe_outcome(&outcome);
                if let Some(t) = target {
                    record_flash(&app_c, i, t);
                }
                let mut c = app_c.consumers[i].lock().unwrap();
                c.signals_sent  += 1;
                c.last_outcome   = text;
                c.last_target_id = target;
            }
        });
    }

    let workers_arc = Arc::new(workers.clone());
    tokio::spawn(serve_http(app.clone(), workers_arc));

    time::sleep(Duration::from_millis(SETTLE_MS)).await;
    eprintln!("Emergent pool live. Toggle individual workers from the browser.");

    signal::ctrl_c().await?;
    eprintln!("\nShutting down…");
    for a in workers.iter().chain(consumers.iter()) { a.shutdown().await; }
    Ok(())
}

/// Reduces a `WiredEmitOutcome` to (status string, optional worker index) for
/// browser display. Picks the first node-style provider as the "highlight";
/// group-style providers are recorded as status-only since the resolver
/// already fanned the signal out via group membership.
fn describe_outcome(out: &WiredEmitOutcome) -> (String, Option<usize>) {
    match out {
        WiredEmitOutcome::Unwired { .. } => ("Unwired — no provider".to_string(), None),
        WiredEmitOutcome::Emitted { providers } if providers.is_empty() =>
            ("Emitted (0 providers)".to_string(), None),
        WiredEmitOutcome::Emitted { providers } => {
            // The resolver returns Node-style entries for direct cap/ matches AND
            // a Group entry for gcap/ matches. We highlight a single member; the
            // group entry's first contributor is good enough.
            let (text, idx) = pick_target(providers);
            (text, idx)
        }
    }
}

fn pick_target(providers: &[WiringProvider]) -> (String, Option<usize>) {
    for p in providers {
        match p {
            WiringProvider::Node { node_id, .. } => {
                if let Some(idx) = node_id_to_worker_idx(node_id) {
                    return (format!("Emitted → worker-{idx} (node)"), Some(idx));
                }
            }
            WiringProvider::Group { contributors, .. } => {
                if let Some(idx) = contributors.iter().find_map(node_id_to_worker_idx) {
                    return (format!("Emitted → worker-{idx} (group)"), Some(idx));
                }
            }
        }
    }
    (format!("Emitted ({} providers)", providers.len()), None)
}

fn node_id_to_worker_idx(nid: &NodeId) -> Option<usize> {
    let s = nid.to_string(); // "127.0.0.1:551xx"
    let port: u16 = s.rsplit(':').next()?.parse().ok()?;
    let idx = port.checked_sub(BASE_PORT)? as usize;
    if idx < N_WORKERS { Some(idx) } else { None }
}
