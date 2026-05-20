//! Locality-Aware Wiring — Phase 6 resolve_with_locality
//!
//! 12 GossipAgents (ports 55200–55211) arranged in a topology tree of
//! `az` × `rack` × `host`. Four nodes advertise `render/job`; one of the
//! others is the active consumer and continuously calls
//! `resolve_with_locality(&filter, PreferShared(0))`. The UI plots every
//! candidate on concentric rings centred on the consumer — closer rings =
//! deeper shared locality prefix.
//!
//! Kill the closest provider and watch the resolver visibly shift to the
//! next-closest ring; bring it back and the choice snaps inward. Click any
//! non-provider node to make it the consumer and see the depth ordering
//! recompute from a different vantage point.
//!
//! Layout (all under `az1`/`az2`):
//!     0: az1/rack0/host0  *provider*       6: az2/rack0/host0  *provider*
//!     1: az1/rack0/host1                   7: az2/rack0/host1
//!     2: az1/rack0/host2  (default consumer)
//!                                          8: az2/rack0/host2
//!     3: az1/rack1/host0                   9: az2/rack1/host0  *provider*
//!     4: az1/rack1/host1  *provider*      10: az2/rack1/host1
//!     5: az1/rack1/host2                  11: az2/rack1/host2
//!
//! Run:
//!   cargo run --example locality_wiring
//!
//! Then open http://127.0.0.1:8099

use mycelium::{
    CapFilter, Capability, CapabilityHandle, GossipAgent, GossipConfig, LocalityPreference,
    NodeId,
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

const N_NODES:    usize = 12;
const BASE_PORT:  u16   = 55200;
const HTTP_PORT:  u16   = 8099;
const SETTLE_MS:  u64   = 2_500;
const REASSERT_SECS: u64 = 30;
const RESOLVE_INTERVAL_MS: u64 = 500;
const DEFAULT_CONSUMER: usize = 2;

// Provider node indices — chosen to land at varying locality depths from
// the default consumer (az1/rack0/host2): node 0 → depth 2, node 4 → depth 1,
// nodes 6 and 9 → depth 0.
const PROVIDER_NODES: &[usize] = &[0, 4, 6, 9];

fn locality_for(idx: usize) -> Vec<String> {
    let az   = if idx < 6 { "az1" } else { "az2" };
    let rack = if (idx % 6) < 3 { "rack0" } else { "rack1" };
    let host = format!("host{}", idx % 3);
    vec![az.to_string(), rack.to_string(), host]
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

struct NodeRecord {
    locality:   Vec<String>,
    has_cap:    bool,
    cap_handle: Option<CapabilityHandle>,
}

struct Candidate {
    node_id: usize,
    depth:   usize,
}

struct ResolutionSnapshot {
    candidates: Vec<Candidate>,
    chosen_id:  Option<usize>,
    last_ms:    u64,
}

struct Event { ts_ms: u64, message: String }

struct AppState {
    nodes:      Vec<Mutex<NodeRecord>>,
    consumer:   Mutex<usize>,
    snapshot:   Mutex<ResolutionSnapshot>,
    events:     Mutex<Vec<Event>>,
}

fn push_event(app: &AppState, msg: String) {
    let mut e = app.events.lock().unwrap();
    e.push(Event { ts_ms: now_ms(), message: msg });
    if e.len() > 100 { e.drain(0..50); }
}

fn state_json(app: &AppState) -> String {
    let consumer_id = *app.consumer.lock().unwrap();
    let nodes: Vec<String> = (0..N_NODES).map(|i| {
        let n = app.nodes[i].lock().unwrap();
        let segs: Vec<String> = n.locality.iter().map(|s| format!(r#""{}""#, s)).collect();
        let role = if i == consumer_id { "consumer" }
                   else if PROVIDER_NODES.contains(&i) { "provider" }
                   else { "peer" };
        format!(
            r#"{{"id":{},"port":{},"locality":[{}],"role":"{}","has_cap":{}}}"#,
            i, BASE_PORT + i as u16, segs.join(","), role, n.has_cap,
        )
    }).collect();

    let snap = app.snapshot.lock().unwrap();
    let cands: Vec<String> = snap.candidates.iter().map(|c| {
        format!(r#"{{"id":{},"depth":{}}}"#, c.node_id, c.depth)
    }).collect();
    let chosen = snap.chosen_id.map(|n| n as i32).unwrap_or(-1);

    let evts: Vec<String> = app.events.lock().unwrap().iter().rev().take(40)
        .map(|e| format!(r#"{{"ts_ms":{},"message":"{}"}}"#, e.ts_ms, e.message.replace('"', "'")))
        .collect();

    format!(
        r#"{{"nodes":[{}],"consumer":{},"candidates":[{}],"chosen":{},"last_ms":{},"events":[{}]}}"#,
        nodes.join(","),
        consumer_id,
        cands.join(","),
        chosen,
        snap.last_ms,
        evts.join(","),
    )
}

async fn serve_http(
    app:    Arc<AppState>,
    agents: Arc<Vec<Arc<GossipAgent>>>,
) {
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
                if let Some(idx) = extract_param(req, "node=") {
                    if req.contains("op=toggle") && PROVIDER_NODES.contains(&idx) {
                        toggle_provider(&app, &agents, idx);
                    } else if req.contains("op=consumer") && idx < N_NODES {
                        *app.consumer.lock().unwrap() = idx;
                        push_event(&app, format!("consumer is now node-{idx}"));
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
            let html = include_str!("../docs/locality_wiring.html");
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

fn toggle_provider(app: &AppState, agents: &[Arc<GossipAgent>], idx: usize) {
    let mut n = app.nodes[idx].lock().unwrap();
    if n.has_cap {
        n.cap_handle = None;
        n.has_cap = false;
        drop(n);
        push_event(app, format!("node-{idx} retract render/job"));
    } else {
        let h = agents[idx].advertise_capability(
            Capability::new("render", "job"),
            Duration::from_secs(REASSERT_SECS),
        );
        n.cap_handle = Some(h);
        n.has_cap = true;
        drop(n);
        push_event(app, format!("node-{idx} advertise render/job"));
    }
}

fn node_id_to_idx(nid: &NodeId) -> Option<usize> {
    let s = nid.to_string();
    let port: u16 = s.rsplit(':').next()?.parse().ok()?;
    let idx = port.checked_sub(BASE_PORT)? as usize;
    if idx < N_NODES { Some(idx) } else { None }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    raise_fd_limit(4096);

    let ports: Vec<u16> = (0..N_NODES).map(|i| BASE_PORT + i as u16).collect();
    let mut agents: Vec<Arc<GossipAgent>> = Vec::with_capacity(N_NODES);
    for (i, &p) in ports.iter().enumerate() {
        let nid = NodeId::new("127.0.0.1", p)?;
        let mut cfg = GossipConfig::default();
        cfg.bind_address               = "127.0.0.1".to_string();
        cfg.bind_port                  = p;
        cfg.default_ttl                = 10;
        cfg.gossip_shards              = 1;
        cfg.reconnect_backoff_secs     = 1;
        cfg.health_check_max_jitter_ms = 100;
        cfg.locality_path              = locality_for(i);
        cfg.bootstrap_peers = ports.iter().filter(|&&pp| pp != p)
            .map(|&pp| NodeId::new("127.0.0.1", pp))
            .collect::<Result<Vec<_>, _>>()?;
        agents.push(Arc::new(GossipAgent::new(nid, cfg)));
    }
    eprintln!("Starting {N_NODES} agents…");
    for a in &agents { a.start().await?; }

    let mut records: Vec<Mutex<NodeRecord>> = Vec::with_capacity(N_NODES);
    for i in 0..N_NODES {
        let is_provider = PROVIDER_NODES.contains(&i);
        let cap_handle = if is_provider {
            Some(agents[i].advertise_capability(
                Capability::new("render", "job"),
                Duration::from_secs(REASSERT_SECS),
            ))
        } else { None };
        records.push(Mutex::new(NodeRecord {
            locality:   locality_for(i),
            has_cap:    is_provider,
            cap_handle,
        }));
    }

    let app = Arc::new(AppState {
        nodes:    records,
        consumer: Mutex::new(DEFAULT_CONSUMER),
        snapshot: Mutex::new(ResolutionSnapshot {
            candidates: Vec::new(),
            chosen_id:  None,
            last_ms:    0,
        }),
        events: Mutex::new(Vec::new()),
    });

    // Resolution task: every interval, ask the current consumer agent to
    // resolve the filter with locality preference. Cache the result for the
    // /state endpoint.
    let app_r = app.clone();
    let agents_r = agents.clone();
    tokio::spawn(async move {
        let filter = CapFilter::new("render", "job");
        let mut ticker = time::interval(Duration::from_millis(RESOLVE_INTERVAL_MS));
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let consumer_id = *app_r.consumer.lock().unwrap();
            let raw = agents_r[consumer_id].resolve_with_locality(
                &filter,
                LocalityPreference::PreferShared(0),
            );
            let mut candidates: Vec<Candidate> = raw.iter()
                .filter_map(|(nid, _cap, depth)| {
                    node_id_to_idx(nid).map(|id| Candidate { node_id: id, depth: *depth })
                })
                .collect();
            // resolve_with_locality already ordered by depth desc — record the
            // first as the canonical "chosen" provider.
            let chosen_id = candidates.first().map(|c| c.node_id);
            // Stable secondary sort by id for predictable rendering at equal depth.
            candidates.sort_by(|a, b| b.depth.cmp(&a.depth).then(a.node_id.cmp(&b.node_id)));
            let prev_chosen = app_r.snapshot.lock().unwrap().chosen_id;
            *app_r.snapshot.lock().unwrap() = ResolutionSnapshot {
                candidates,
                chosen_id,
                last_ms: now_ms(),
            };
            let _ = consumer_id;  // consumer_id surfaced via app.consumer for /state
            if chosen_id != prev_chosen {
                let msg = match chosen_id {
                    Some(id) => format!("resolver → node-{id}"),
                    None     => "resolver → (no provider)".to_string(),
                };
                push_event(&app_r, msg);
            }
        }
    });

    let agents_arc = Arc::new(agents.clone());
    tokio::spawn(serve_http(app.clone(), agents_arc));

    time::sleep(Duration::from_millis(SETTLE_MS)).await;
    eprintln!("Locality wiring live. Kill providers and switch consumers from the browser.");

    signal::ctrl_c().await?;
    eprintln!("\nShutting down…");
    for a in &agents { a.shutdown().await; }
    Ok(())
}
