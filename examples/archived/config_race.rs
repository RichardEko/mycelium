//! Config Propagation Race — Layer III Competing Proposers
//!
//! 9 GossipAgents (ports 54400–54408) simultaneously try to commit different
//! "config epoch" values to the same consensus slot. First ballot to reach
//! quorum commits; the others receive Superseded. Observe how the layer III
//! protocol resolves the race deterministically — only one value wins, and
//! all peers converge to it via anti-entropy.
//!
//! Controls:
//!   /race        — trigger a new race (all nodes propose simultaneously)
//!   /race?winner=N — propose only from node N (guaranteed commit if quorum OK)
//!
//! Run:
//!   cargo run --example config_race
//!
//! Then open http://127.0.0.1:8096

use bytes::Bytes;
use mycelium::{
    ConsensusConfig, ConsensusResult, GossipAgent, GossipConfig, NodeId,
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

const N: usize = 9;
const BASE_PORT: u16 = 54400;
const HTTP_PORT: u16 = 8096;
const SETTLE_MS: u64 = 3_000;
const AUTO_RACE_MS: u64 = 12_000;
const GROUP: &str = "cfg_cluster";
const SLOT: &str  = "config/epoch";

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Default, Clone)]
struct NodeResult {
    proposed_value: String,
    status:         String,   // "committed", "superseded", "timeout", "pending", ""
    ballots_tried:  u32,
    committed_value: Option<String>,
    ts_ms:          u64,
}

struct RaceRecord {
    race_num:    u32,
    started_ms:  u64,
    results:     Vec<NodeResult>,
    winner:      Option<usize>,
    committed:   Option<String>,
    finished:    bool,
}

struct AppState {
    node_results:  Arc<Vec<Arc<Mutex<NodeResult>>>>,
    races:         Arc<Mutex<Vec<RaceRecord>>>,
    race_count:    Arc<Mutex<u32>>,
    race_active:   Arc<Mutex<bool>>,
    committed:     Arc<Mutex<Option<String>>>,
}

fn state_json(app: &AppState) -> String {
    let races       = app.races.lock().unwrap();
    let race_count  = *app.race_count.lock().unwrap();
    let race_active = *app.race_active.lock().unwrap();
    let committed   = app.committed.lock().unwrap().clone();

    let nodes: Vec<String> = app.node_results.iter().enumerate().map(|(i, nr)| {
        let r = nr.lock().unwrap();
        let cv = r.committed_value.as_deref()
            .map(|v| format!(r#""{}""#, v))
            .unwrap_or_else(|| "null".to_string());
        format!(
            r#"{{"id":{},"port":{},"proposed_value":"{}","status":"{}","ballots_tried":{},"committed_value":{}}}"#,
            i, BASE_PORT + i as u16, r.proposed_value, r.status, r.ballots_tried, cv
        )
    }).collect();

    let race_hist: Vec<String> = races.iter().rev().take(8).map(|r| {
        let winner = r.winner.map(|w| w.to_string()).unwrap_or_else(|| "null".to_string());
        let cv = r.committed.as_deref().map(|v| format!(r#""{}""#, v)).unwrap_or_else(|| "null".to_string());
        let results: Vec<String> = r.results.iter().enumerate().map(|(i, nr)| {
            format!(r#"{{"node":{},"status":"{}","ballots":{}}}"#, i, nr.status, nr.ballots_tried)
        }).collect();
        format!(
            r#"{{"race_num":{},"started_ms":{},"winner":{},"committed":{},"finished":{},"results":[{}]}}"#,
            r.race_num, r.started_ms, winner, cv, r.finished, results.join(",")
        )
    }).collect();

    let committed_json = committed.as_deref()
        .map(|v| format!(r#""{}""#, v))
        .unwrap_or_else(|| "null".to_string());

    format!(
        r#"{{"n":{},"race_count":{},"race_active":{},"committed":{},"nodes":[{}],"races":[{}]}}"#,
        N, race_count, race_active, committed_json,
        nodes.join(","), race_hist.join(",")
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

            if req.contains("GET /race") {
                let only_node: Option<usize> = req.find("winner=")
                    .and_then(|p| {
                        let s = &req[p+7..];
                        let end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
                        s[..end].parse().ok()
                    });
                let app2    = app.clone();
                let agents2 = agents.clone();
                tokio::spawn(run_race(app2, agents2, only_node));
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
            let html = include_str!("../docs/config_race.html");
            let _ = stream.write_all(format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                html.len(), html
            ).as_bytes()).await;
        });
    }
}

async fn run_race(app: Arc<AppState>, agents: Arc<Vec<Arc<GossipAgent>>>, only_node: Option<usize>) {
    {
        let mut active = app.race_active.lock().unwrap();
        if *active { return; }
        *active = true;
    }

    let race_num = {
        let mut r = app.race_count.lock().unwrap();
        *r += 1;
        *r
    };

    // Reset node results
    for (i, nr) in app.node_results.iter().enumerate() {
        let epoch = format!("epoch-{}-node-{}", race_num, i);
        let mut r = nr.lock().unwrap();
        *r = NodeResult { proposed_value: epoch, status: "pending".to_string(), ..Default::default() };
    }

    let started_ms = now_ms();
    {
        let mut races = app.races.lock().unwrap();
        races.push(RaceRecord {
            race_num,
            started_ms,
            results: (0..N).map(|i| NodeResult {
                proposed_value: format!("epoch-{}-node-{}", race_num, i),
                ..Default::default()
            }).collect(),
            winner: None,
            committed: None,
            finished: false,
        });
    }

    let cfg = ConsensusConfig {
        phase1_timeout:         Duration::from_millis(1200),
        max_ballots:            3,
        ballot_retry_jitter_ms: 120,
        count_opaque_as_absent: false,
        ..ConsensusConfig::default()
    };

    // Spawn all proposals simultaneously
    let mut handles = Vec::new();
    let proposer_nodes: Vec<usize> = match only_node {
        Some(n) if n < N => vec![n],
        _                => (0..N).collect(),
    };

    for &i in &proposer_nodes {
        let agent  = agents[i].clone();
        let value  = app.node_results[i].lock().unwrap().proposed_value.clone();
        let cfg2   = cfg.clone();
        let nr     = app.node_results[i].clone();
        let app2   = app.clone();
        let committed_arc = app.committed.clone();
        handles.push(tokio::spawn(async move {
            let result = agent.group_propose(
                GROUP, SLOT,
                Bytes::copy_from_slice(value.as_bytes()),
                cfg2,
            ).await;

            let (status, ballots_tried, cv) = match &result {
                ConsensusResult::Committed { ballot, value: v, .. } => {
                    let cv = String::from_utf8_lossy(v).to_string();
                    *committed_arc.lock().unwrap() = Some(cv.clone());
                    ("committed".to_string(), *ballot as u32, Some(cv))
                },
                ConsensusResult::Timeout { ballots_tried, .. } => {
                    ("timeout".to_string(), *ballots_tried, None)
                },
                ConsensusResult::Superseded { ballot, .. } => {
                    let cv = agent.consensus_get(SLOT)
                        .map(|b| String::from_utf8_lossy(&b).to_string());
                    if let Some(ref v) = cv {
                        *committed_arc.lock().unwrap() = Some(v.clone());
                    }
                    ("superseded".to_string(), *ballot as u32, cv)
                },
            };

            {
                let mut r = nr.lock().unwrap();
                r.status          = status.clone();
                r.ballots_tried   = ballots_tried;
                r.committed_value = cv.clone();
                r.ts_ms           = now_ms();
            }

            // Update race record
            {
                let mut races = app2.races.lock().unwrap();
                if let Some(race) = races.last_mut() {
                    let nr2 = &mut race.results[i];
                    nr2.status          = status.clone();
                    nr2.ballots_tried   = ballots_tried;
                    nr2.committed_value = cv.clone();
                    if status == "committed" {
                        race.winner    = Some(i);
                        race.committed = cv;
                    }
                }
            }
        }));
    }

    // Wait for all proposals to settle
    for h in handles { let _ = h.await; }

    // Mark race finished
    {
        let mut races = app.races.lock().unwrap();
        if let Some(race) = races.last_mut() {
            race.finished = true;
        }
    }

    // Clear pending statuses for non-proposers
    for i in 0..N {
        let mut r = app.node_results[i].lock().unwrap();
        if r.status == "pending" {
            r.status = "observer".to_string();
        }
    }

    *app.race_active.lock().unwrap() = false;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    raise_fd_limit(4096);

    let mut agents: Vec<Arc<GossipAgent>> = Vec::with_capacity(N);
    let ports: Vec<u16> = (0..N).map(|i| BASE_PORT + i as u16).collect();

    for i in 0..N {
        let p   = ports[i];
        let nid = NodeId::new("127.0.0.1", p)?;
        let mut cfg = GossipConfig::default();
        cfg.bind_address               = "127.0.0.1".to_string();
        cfg.bind_port                  = p;
        cfg.default_ttl                = 10;
        cfg.reconnect_backoff_secs     = 1;
        cfg.gossip_shards              = 1;
        cfg.health_check_max_jitter_ms = 100;
        cfg.bootstrap_peers = ports.iter().filter(|&&pp| pp != p)
            .map(|&pp| NodeId::new("127.0.0.1", pp))
            .collect::<Result<Vec<_>, _>>()?;
        agents.push(Arc::new(GossipAgent::new(nid, cfg)));
    }

    eprintln!("Starting {N} competing proposer nodes…");
    for a in &agents { a.start().await?; }

    let listener_cfg = ConsensusConfig::default();
    let mut consensus_handles = Vec::new();
    for a in &agents {
        a.join_group(GROUP);
        consensus_handles.push(a.start_consensus_listener(listener_cfg.clone()));
    }

    let app = Arc::new(AppState {
        node_results: Arc::new((0..N).map(|_| Arc::new(Mutex::new(NodeResult::default()))).collect()),
        races:        Arc::new(Mutex::new(Vec::new())),
        race_count:   Arc::new(Mutex::new(0)),
        race_active:  Arc::new(Mutex::new(false)),
        committed:    Arc::new(Mutex::new(None)),
    });

    let agents_arc = Arc::new(agents.clone());
    let app_srv    = app.clone();
    tokio::spawn(serve_http(app_srv, agents_arc.clone()));

    time::sleep(Duration::from_millis(SETTLE_MS)).await;
    eprintln!("Settled. Racing every {AUTO_RACE_MS}ms. Open http://127.0.0.1:{HTTP_PORT}");

    // Auto-race loop
    let app_race   = app.clone();
    let agents_race = agents_arc.clone();
    tokio::spawn(async move {
        time::sleep(Duration::from_millis(1_000)).await; // first race after 1s
        loop {
            let app2    = app_race.clone();
            let agents2 = agents_race.clone();
            tokio::spawn(run_race(app2, agents2, None));
            time::sleep(Duration::from_millis(AUTO_RACE_MS)).await;
        }
    });

    signal::ctrl_c().await?;
    eprintln!("\nShutting down…");
    drop(consensus_handles);
    for a in &agents { a.shutdown().await; }
    Ok(())
}
