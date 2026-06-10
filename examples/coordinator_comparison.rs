//! # Coordinator Comparison
//!
//! Empirical companion to Paper 2a (§9, "Empirical Validation in the Computing
//! Domain"). Demonstrates that the same Mycelium substrate can be wired to
//! exhibit either of two interaction patterns — broker-mediated routing or
//! locally-resolved gossip routing — and measures the epistemic-error gap
//! between them as the agent population scales.
//!
//! ## Two modes from one substrate
//!
//! - **MODE=gossip** (the locally-resolved alternative of Paper 2a):
//!   the client reads its own gossip view of worker loads and picks a worker
//!   directly. No coordinator. The decision uses the client's local snapshot,
//!   bounded-stale by ~one gossip round.
//!
//! - **MODE=broker** (the coordinator pattern Paper 1 critiques):
//!   one designated node aggregates worker state and answers `pick_worker`
//!   RPCs. Clients do not read worker load directly; they ask the broker.
//!   Every routing decision is serialised through the broker and uses
//!   the broker's gossip view, with the RPC round-trip added.
//!
//! The substrate is identical in both modes. The only difference is whether
//! the client reads `wkr/*/load` locally or RPCs a broker.
//!
//! ## What we measure
//!
//! For each routing decision:
//!   * `perceived_load` — value the decision was made on
//!   * `true_load`      — worker's actual current load at action time
//!                        (measured by an immediate probe RPC to the selected
//!                        worker right after the decision)
//!   * `decision_us`    — wall-clock microseconds for the decision step
//!   * `is_misroute`    — `true_load >= perceived_load + MISROUTE_GAP`
//!
//! ## CLI
//!
//! ```sh
//! MODE=gossip N=20 DURATION_SECS=15 cargo run --release --example coordinator_comparison
//! MODE=broker N=20 DURATION_SECS=15 cargo run --release --example coordinator_comparison
//! ```

use bytes::Bytes;
use mycelium::{GossipAgent, GossipConfig, NodeId};
use std::{
    sync::{Arc, atomic::{AtomicU32, Ordering}},
    time::{Duration, Instant},
};
use tokio::time::sleep;

// ─────────────────────────────────────────────────────────────────────────────

const LOAD_PREFIX: &str = "wkr/";
const RPC_PICK:    &str = "compare.pick";
const RPC_PROBE:   &str = "compare.probe";
const MISROUTE_GAP: u32 = 3;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let mode = std::env::var("MODE").unwrap_or_else(|_| "gossip".to_string());
    let n: usize = std::env::var("N").unwrap_or_else(|_| "20".to_string()).parse().unwrap();
    let duration_secs: u64 = std::env::var("DURATION_SECS")
        .unwrap_or_else(|_| "15".to_string()).parse().unwrap();
    let decision_rate_hz: u64 = std::env::var("DECISION_RATE_HZ")
        .unwrap_or_else(|_| "50".to_string()).parse().unwrap();
    let load_update_rate_hz: u64 = std::env::var("LOAD_UPDATE_RATE_HZ")
        .unwrap_or_else(|_| "10".to_string()).parse().unwrap();
    let port_base: u16 = std::env::var("PORT_BASE")
        .unwrap_or_else(|_| "29100".to_string()).parse().unwrap();
    let warmup_secs: u64 = std::env::var("WARMUP_SECS")
        .unwrap_or_else(|_| "6".to_string()).parse().unwrap();

    eprintln!("# coordinator_comparison mode={} n={} duration_secs={} \
        decision_rate_hz={} load_update_rate_hz={} warmup_secs={}",
        mode, n, duration_secs, decision_rate_hz, load_update_rate_hz, warmup_secs);

    let cluster = build_cluster(&mode, n, port_base).await;

    // ── Spawn worker load drivers ────────────────────────────────────────────
    // Each worker writes `wkr/{node_id}/load = u32_le` periodically. The value
    // also lives in worker.true_load (atomic) for the probe RPC to read.
    for w in &cluster.workers {
        let w = Arc::clone(w);
        let rate = load_update_rate_hz;
        let node_str = w.agent.node_id().to_string();
        let key = format!("{}{}/load", LOAD_PREFIX, node_str);
        tokio::spawn(async move {
            let mut load: u32 = 0;
            let mut tick = tokio::time::interval(Duration::from_millis(1000 / rate.max(1)));
            loop {
                tick.tick().await;
                // Pseudo-random walk: range 0..40, drift biased slightly upward.
                let delta: i32 = (fastrand::i32(0..10)) - 4;
                load = (load as i32 + delta).clamp(0, 40) as u32;
                w.true_load.store(load, Ordering::Relaxed);
                w.agent.kv().set(key.clone(), Bytes::copy_from_slice(&load.to_le_bytes()));
            }
        });
    }

    // ── Spawn worker probe handlers (returns *true* current load) ───────────
    for w in &cluster.workers {
        let w_arc = Arc::clone(w);
        let mut rx = w.agent.service().rpc_rx(RPC_PROBE);
        tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                let load = w_arc.true_load.load(Ordering::Relaxed);
                w_arc.agent.service().rpc_respond(&req, Bytes::copy_from_slice(&load.to_le_bytes()));
            }
        });
    }

    // ── Spawn broker handler (broker mode only) ─────────────────────────────
    if mode == "broker" {
        let broker = Arc::clone(cluster.broker.as_ref().expect("broker in broker mode"));
        let mut rx = broker.service().rpc_rx(RPC_PICK);
        tokio::spawn(async move {
            loop {
                let Some(req) = rx.recv().await else { break };
                let pick = scan_lowest_load(&broker);
                let reply = match pick {
                    Some((node, load)) => {
                        let mut buf = Vec::with_capacity(32);
                        buf.extend_from_slice(node.to_string().as_bytes());
                        buf.push(b'|');
                        buf.extend_from_slice(load.to_string().as_bytes());
                        Bytes::from(buf)
                    }
                    None => Bytes::from_static(b"NONE"),
                };
                broker.service().rpc_respond(&req, reply);
            }
        });
    }

    // ── Warmup ───────────────────────────────────────────────────────────────
    eprint!("# warmup ");
    let warmup_deadline = Instant::now() + Duration::from_secs(warmup_secs);
    while Instant::now() < warmup_deadline {
        sleep(Duration::from_millis(500)).await;
        let visible = scan_lowest_load(&cluster.client).map(|_| count_workers(&cluster.client)).unwrap_or(0);
        eprint!("{}/{} ", visible, n);
        if visible == n { break; }
    }
    let final_visible = count_workers(&cluster.client);
    let client_peers = cluster.client.peers().len();
    let broker_peers = cluster.broker.as_ref().map(|b| b.peers().len()).unwrap_or(0);
    eprintln!("→ client sees {} workers, {} peers; broker has {} peers",
        final_visible, client_peers, broker_peers);

    // ── Measurement loop ─────────────────────────────────────────────────────
    println!("mode,n,decision_idx,perceived_load,true_load,decision_us,is_misroute");
    let decision_interval = Duration::from_millis(1000 / decision_rate_hz.max(1));
    let deadline = Instant::now() + Duration::from_secs(duration_secs);
    let mut idx: u64 = 0;
    let mut latencies_us: Vec<u64> = Vec::with_capacity(8 * 1024);
    let mut misroute_count: u64 = 0;
    let mut total_staleness: u64 = 0;
    let mut considered: u64 = 0;
    let mut nones: u64 = 0;

    while Instant::now() < deadline {
        let t0 = Instant::now();
        let pick: Option<(NodeId, u32)> = match mode.as_str() {
            "gossip" => scan_lowest_load(&cluster.client),
            "broker" => {
                let broker_node = cluster.broker_node.clone().expect("broker_node");
                let result = cluster.client.service().rpc_call(
                    broker_node, RPC_PICK, Bytes::new(), Duration::from_millis(500),
                ).await;
                match result {
                    Ok(reply) if &reply[..] != b"NONE" => parse_broker_reply(&reply),
                    _ => None,
                }
            }
            other => panic!("unknown MODE={}", other),
        };
        let decision_us = t0.elapsed().as_micros() as u64;
        latencies_us.push(decision_us);

        if let Some((target_node, perceived_load)) = pick {
            let probe = cluster.client.service().rpc_call(
                target_node.clone(), RPC_PROBE, Bytes::new(), Duration::from_millis(300),
            ).await;
            if let Ok(reply) = probe {
                if reply.len() == 4 {
                    let true_load = u32::from_le_bytes(reply[..].try_into().unwrap());
                    let staleness = (true_load as i64 - perceived_load as i64).unsigned_abs();
                    let is_misroute = true_load >= perceived_load + MISROUTE_GAP;
                    if is_misroute { misroute_count += 1; }
                    total_staleness += staleness;
                    considered += 1;
                    println!("{},{},{},{},{},{},{}",
                        mode, n, idx, perceived_load, true_load, decision_us, is_misroute);
                }
            }
        } else {
            nones += 1;
        }

        idx += 1;
        let next = t0 + decision_interval;
        let now = Instant::now();
        if next > now { sleep(next - now).await; }
    }

    // ── Summary ──────────────────────────────────────────────────────────────
    latencies_us.sort_unstable();
    let mean_lat = if !latencies_us.is_empty() {
        latencies_us.iter().sum::<u64>() as f64 / latencies_us.len() as f64
    } else { 0.0 };
    let p50 = percentile(&latencies_us, 0.50);
    let p95 = percentile(&latencies_us, 0.95);
    let p99 = percentile(&latencies_us, 0.99);
    let misroute_rate = if considered > 0 {
        misroute_count as f64 / considered as f64
    } else { 0.0 };
    let mean_staleness = if considered > 0 {
        total_staleness as f64 / considered as f64
    } else { 0.0 };

    let summary = format!(
        "SUMMARY mode={} n={} decisions={} considered={} nones={} \
         mean_us={:.1} p50_us={} p95_us={} p99_us={} \
         mean_staleness={:.3} misroute_rate={:.4}",
        mode, n, idx, considered, nones,
        mean_lat, p50, p95, p99, mean_staleness, misroute_rate
    );
    eprintln!("{}", summary);
    println!("# {}", summary);

    // Shutdown.
    for w in cluster.workers {
        w.agent.shutdown().await;
    }
    if let Some(b) = cluster.broker { b.shutdown().await; }
    cluster.client.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────────────

struct Worker {
    agent:     Arc<GossipAgent>,
    true_load: AtomicU32,
}

struct Cluster {
    workers:     Vec<Arc<Worker>>,
    client:      Arc<GossipAgent>,
    broker:      Option<Arc<GossipAgent>>,
    broker_node: Option<NodeId>,
}

async fn build_cluster(mode: &str, n: usize, port_base: u16) -> Cluster {
    let rendezvous = NodeId::new("127.0.0.1", port_base).expect("port_base");

    let mut workers = Vec::with_capacity(n);
    for i in 0..n {
        let port = port_base + i as u16;
        let id   = NodeId::new("127.0.0.1", port).expect("worker port");
        let mut cfg = GossipConfig::default();
        cfg.bind_port = port;
        cfg.health_check_max_jitter_ms = 50;
        if i != 0 {
            cfg.bootstrap_peers = vec![rendezvous.clone()];
        }
        let agent = Arc::new(GossipAgent::new(id, cfg));
        agent.start().await.expect("worker start");
        workers.push(Arc::new(Worker {
            agent,
            true_load: AtomicU32::new(0),
        }));
    }

    let client_port = port_base + n as u16;
    let client_id   = NodeId::new("127.0.0.1", client_port).expect("client port");
    let mut cfg = GossipConfig::default();
    cfg.bind_port = client_port;
    // Bootstrap client from all workers — guarantees direct peering with every
    // worker so probe RPCs are one hop. Peer-exchange is slower to converge at
    // this scale than just configuring the topology directly.
    cfg.bootstrap_peers = workers.iter()
        .map(|w| w.agent.node_id().clone())
        .collect();
    cfg.health_check_max_jitter_ms = 50;
    let client = Arc::new(GossipAgent::new(client_id.clone(), cfg));
    client.start().await.expect("client start");

    let (broker, broker_node) = if mode == "broker" {
        let broker_port = port_base + n as u16 + 1;
        let broker_id   = NodeId::new("127.0.0.1", broker_port).expect("broker port");
        let mut cfg = GossipConfig::default();
        cfg.bind_port = broker_port;
        // Bootstrap broker with rendezvous AND client — guarantees direct peering
        // between broker and client without relying on peer-exchange latency.
        cfg.bootstrap_peers = vec![rendezvous.clone(), client_id.clone()];
        cfg.health_check_max_jitter_ms = 50;
        let broker = Arc::new(GossipAgent::new(broker_id.clone(), cfg));
        broker.start().await.expect("broker start");
        (Some(broker), Some(broker_id))
    } else {
        (None, None)
    };

    Cluster { workers, client, broker, broker_node }
}

// ─────────────────────────────────────────────────────────────────────────────

/// Scan the local KV view for `wkr/*/load` entries and return the (node_id, load)
/// pair with the lowest load. None if no live load entries are visible.
fn scan_lowest_load(agent: &Arc<GossipAgent>) -> Option<(NodeId, u32)> {
    let entries = agent.kv().scan_prefix(LOAD_PREFIX);
    let mut best: Option<(NodeId, u32)> = None;
    for (key, bytes) in entries {
        // Key shape: wkr/{node_id}/load → strip "wkr/" prefix and "/load" suffix.
        let s = key.as_ref();
        if !s.ends_with("/load") { continue; }
        let middle = &s[LOAD_PREFIX.len()..s.len() - "/load".len()];
        // node_id printed form is host:port
        let Some((host, port_str)) = middle.rsplit_once(':') else { continue };
        let Ok(port) = port_str.parse::<u16>() else { continue };
        let Ok(node) = NodeId::new(host, port) else { continue };
        if bytes.len() != 4 { continue; }
        let load = u32::from_le_bytes(bytes[..].try_into().unwrap());
        match best {
            None => best = Some((node, load)),
            Some((_, current)) if load < current => best = Some((node, load)),
            _ => {}
        }
    }
    best
}

fn count_workers(agent: &Arc<GossipAgent>) -> usize {
    agent.kv().scan_prefix(LOAD_PREFIX)
        .iter()
        .filter(|(k, _)| k.ends_with("/load"))
        .count()
}

/// Parse `b"host:port|load"` into `(NodeId, u32)`.
fn parse_broker_reply(reply: &[u8]) -> Option<(NodeId, u32)> {
    let s = std::str::from_utf8(reply).ok()?;
    let (node_str, load_str) = s.split_once('|')?;
    let (host, port_str) = node_str.rsplit_once(':')?;
    let port: u16 = port_str.parse().ok()?;
    let node = NodeId::new(host, port).ok()?;
    let load: u32 = load_str.parse().ok()?;
    Some((node, load))
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() { return 0; }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}
