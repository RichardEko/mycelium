//! Consensus Ballot Matrix — Layer III Epidemic Two-Phase Voting
//!
//! 7 GossipAgents (ports 54300–54306) all participate as voters.
//! Every 8 seconds, one node is elected to propose a value for the "coordinator"
//! slot via `group_propose`. The ballot matrix shows — live — how many ballots
//! were tried, how many votes the last ballot received, and whether the proposal
//! committed, timed out, or was superseded by another proposer.
//!
//! Run:
//!   cargo run --example consensus_ballot
//!
//! Then open http://127.0.0.1:8095

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

const N: usize = 7;
const BASE_PORT: u16 = 54300;
const HTTP_PORT: u16 = 8095;
const SETTLE_MS: u64 = 3_000;
const PROPOSE_INTERVAL_MS: u64 = 8_000;
const GROUP: &str = "cluster";

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Clone)]
struct ProposalRecord {
    round:             u32,
    proposer_id:       usize,
    slot:              String,
    proposed_value:    String,
    status:            String,
    ballots_tried:     u32,
    votes_last_ballot: usize,
    quorum_required:   usize,
    committed_value:   Option<String>,
    ts_ms:             u64,
}

struct AppState {
    history:          Arc<Mutex<Vec<ProposalRecord>>>,
    committed:        Arc<Mutex<std::collections::HashMap<String, String>>>,
    proposer_idx:     Arc<Mutex<usize>>,
    round:            Arc<Mutex<u32>>,
    in_flight:        Arc<Mutex<bool>>,
}

fn state_json(app: &AppState) -> String {
    let history   = app.history.lock().unwrap();
    let committed = app.committed.lock().unwrap();
    let proposer  = *app.proposer_idx.lock().unwrap();
    let round     = *app.round.lock().unwrap();
    let in_flight = *app.in_flight.lock().unwrap();

    let hist_entries: Vec<String> = history.iter().rev().take(12).map(|r| {
        let cv = r.committed_value.as_deref().map(|v| format!(r#""{}""#, v))
                  .unwrap_or_else(|| "null".to_string());
        format!(
            r#"{{"round":{},"proposer":{},"slot":"{}","proposed":"{}","status":"{}","ballots_tried":{},"votes_last_ballot":{},"quorum_required":{},"committed_value":{},"ts_ms":{}}}"#,
            r.round, r.proposer_id, r.slot, r.proposed_value,
            r.status, r.ballots_tried, r.votes_last_ballot, r.quorum_required,
            cv, r.ts_ms
        )
    }).collect();

    let committed_entries: Vec<String> = committed.iter().map(|(k, v)| {
        format!(r#""{}":"{}""#, k, v)
    }).collect();

    format!(
        r#"{{"n":{},"round":{},"current_proposer":{},"in_flight":{},"history":[{}],"committed":{{{}}}}}"#,
        N, round, proposer, in_flight,
        hist_entries.join(","),
        committed_entries.join(",")
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

            if req.contains("GET /propose") {
                // Force an immediate proposal from a given node
                if let Some(idx_str) = req.find("node=").map(|p| &req[p+5..]) {
                    let end = idx_str.find(|c: char| !c.is_ascii_digit()).unwrap_or(idx_str.len());
                    if let Ok(idx) = idx_str[..end].parse::<usize>() {
                        if idx < N {
                            let app2    = app.clone();
                            let agent   = agents[idx].clone();
                            tokio::spawn(run_proposal(app2, agent, idx, true));
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
            let html = include_str!("../docs/consensus_ballot.html");
            let _ = stream.write_all(format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                html.len(), html
            ).as_bytes()).await;
        });
    }
}

async fn run_proposal(app: Arc<AppState>, agent: Arc<GossipAgent>, idx: usize, forced: bool) {
    {
        let mut inf = app.in_flight.lock().unwrap();
        if *inf && !forced { return; }
        *inf = true;
    }
    let round = {
        let mut r = app.round.lock().unwrap();
        *r += 1;
        *r
    };
    *app.proposer_idx.lock().unwrap() = idx;

    let slot  = "coordinator";
    let value = format!("node-{idx}@round-{round}");

    let cfg = ConsensusConfig {
        phase1_timeout:         Duration::from_millis(1500),
        max_ballots:            4,
        ballot_retry_jitter_ms: 80,
        count_opaque_as_absent: false,
        ..ConsensusConfig::default()
    };

    let result = agent.group_propose(GROUP, slot, Bytes::copy_from_slice(value.as_bytes()), cfg).await;

    let (status, ballots_tried, votes_last, quorum_req, committed_val) = match &result {
        ConsensusResult::Committed { ballot, value: v, .. } => {
            let cv = String::from_utf8_lossy(v).to_string();
            app.committed.lock().unwrap().insert(slot.to_string(), cv.clone());
            ("committed".to_string(), *ballot as u32, N, (N / 2) + 1, Some(cv))
        },
        ConsensusResult::Timeout { ballots_tried, votes_last_ballot, quorum_required, .. } => {
            ("timeout".to_string(), *ballots_tried, *votes_last_ballot, *quorum_required, None)
        },
        ConsensusResult::Superseded { ballot, .. } => {
            let cv = agent.consensus_get(slot)
                .map(|b| String::from_utf8_lossy(&b).to_string());
            if let Some(ref v) = cv {
                app.committed.lock().unwrap().insert(slot.to_string(), v.clone());
            }
            ("superseded".to_string(), *ballot as u32, 0, 0, cv)
        },
    };

    app.history.lock().unwrap().push(ProposalRecord {
        round,
        proposer_id: idx,
        slot: slot.to_string(),
        proposed_value: value,
        status,
        ballots_tried,
        votes_last_ballot: votes_last,
        quorum_required: quorum_req,
        committed_value: committed_val,
        ts_ms: now_ms(),
    });

    *app.in_flight.lock().unwrap() = false;
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

    eprintln!("Starting {N} consensus nodes…");
    for a in &agents { a.start().await?; }

    // All nodes join the cluster group and start listening
    let listener_config = ConsensusConfig::default();
    let mut consensus_handles = Vec::new();
    for a in &agents {
        a.join_group(GROUP);
        consensus_handles.push(a.start_consensus_listener(listener_config.clone()));
    }

    let app = Arc::new(AppState {
        history:      Arc::new(Mutex::new(Vec::new())),
        committed:    Arc::new(Mutex::new(std::collections::HashMap::new())),
        proposer_idx: Arc::new(Mutex::new(0)),
        round:        Arc::new(Mutex::new(0)),
        in_flight:    Arc::new(Mutex::new(false)),
    });

    let agents_arc = Arc::new(agents.clone());
    let app_srv    = app.clone();
    tokio::spawn(serve_http(app_srv, agents_arc.clone()));

    time::sleep(Duration::from_millis(SETTLE_MS)).await;
    eprintln!("Consensus mesh settled. Proposing every {PROPOSE_INTERVAL_MS}ms (round-robin).");

    // Round-robin proposal loop
    let app_prop = app.clone();
    let agents_prop = agents_arc.clone();
    tokio::spawn(async move {
        let mut ticker = time::interval(Duration::from_millis(PROPOSE_INTERVAL_MS));
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        let mut next = 0usize;
        loop {
            ticker.tick().await;
            let idx    = next % N;
            let app2   = app_prop.clone();
            let agent  = agents_prop[idx].clone();
            tokio::spawn(run_proposal(app2, agent, idx, false));
            next += 1;
        }
    });

    signal::ctrl_c().await?;
    eprintln!("\nShutting down…");
    drop(consensus_handles);
    for a in &agents { a.shutdown().await; }
    Ok(())
}
