//! In-process SWIM scale oracle (WS-B M5 Stage 4 debugging).
//!
//! The Stage-4 finding (`docs/plans/v2-wsb-scale-transport.md`) recorded an
//! unresolved divergence: the SWIM de-pin mechanism flattens seed connections
//! to ~2k **in-process at 50 workers**, but **Docker at 100 nodes does not
//! converge** (`seed_established` stays ≈ N). The candidate next step the
//! finding names is: *"reproduce the divergence in-process at N=100 with
//! synthetic continuous KV churn (fast oracle)."*
//!
//! This module is that fast oracle. It spawns `1 seed + (N-1)` workers **in one
//! process over loopback TCP/UDP**, runs SWIM at configurable (Docker-like)
//! cadences, optionally drives continuous KV churn, and prints a time series of:
//!
//!   * **membership convergence** — the min/median/max of `peers().len()` across
//!     all nodes (how much of the cluster each node has discovered), and
//!   * **seed connection count** — the seed's outbound persistent writers plus
//!     the number of workers that currently hold a writer *to* the seed
//!     (inbound). Their sum is the in-process analogue of `seed_established`.
//!
//! If the seed total stays ≈ N here, the divergence is a *mechanism/scale* bug
//! (fixable in Rust) rather than a Docker-networking artifact — and the
//! membership column tells us whether slow discovery is the binding constraint.
//!
//! Ignored by default (heavy: N agents × ~20 tasks). Run explicitly, e.g.:
//!
//! ```bash
//! SWIM_ORACLE_N=100 SWIM_ORACLE_SECS=150 SWIM_ORACLE_CHURN_MS=500 \
//!   cargo test --lib swim_scale_oracle -- --ignored --nocapture
//! ```

#![cfg(test)]

use super::*;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Bind port 0, read the OS-assigned port, release it. Free for the microseconds
/// before the agent binds it (same trick as the `lib_tests` allocator).
fn alloc_port() -> u16 { crate::test_util::alloc_port() }

/// Count the seed's persistent TCP connection load the way `seed_established`
/// does in Docker: outbound writers the seed itself opened + inbound writers
/// every other node opened *to* the seed.
fn seed_conn_split(agents: &[Arc<GossipAgent>], seed_idx: usize) -> (usize, usize) {
    let seed_id = agents[seed_idx].node_id().clone();
    // Count only LIVE writers. An idle-closed writer's TCP connection is gone, but its map
    // entry lingers (it's reaped lazily on the next get_or_spawn_writer/evict), so a raw
    // `.len()` over-counts dead entries. `/proc/net/tcp` in Docker counts only live
    // ESTABLISHED sockets — `is_live()` is the in-process analogue.
    let outbound = agents[seed_idx]
        .peer_writers
        .pin()
        .iter()
        .filter(|(_, e)| e.is_live())
        .count();
    let inbound = agents
        .iter()
        .enumerate()
        .filter(|(i, a)| {
            *i != seed_idx
                && a.peer_writers
                    .pin()
                    .get(&seed_id)
                    .is_some_and(|e| e.is_live())
        })
        .count();
    (outbound, inbound)
}

/// Distribution of how many peers each node has discovered (membership view size).
fn membership_spread(agents: &[Arc<GossipAgent>]) -> (usize, usize, usize) {
    let mut sizes: Vec<usize> = agents.iter().map(|a| a.peers().len()).collect();
    sizes.sort_unstable();
    let min = *sizes.first().unwrap();
    let max = *sizes.last().unwrap();
    let med = sizes[sizes.len() / 2];
    (min, med, max)
}

/// The fast oracle. See module docs for the env knobs.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "heavy scale harness; run explicitly with --ignored --nocapture"]
async fn swim_scale_oracle() {
    let n = env_u64("SWIM_ORACLE_N", 60) as usize;
    let total_secs = env_u64("SWIM_ORACLE_SECS", 70);
    let churn_ms = env_u64("SWIM_ORACLE_CHURN_MS", 0);
    let probe_ms = env_u64("SWIM_ORACLE_PROBE_MS", 1000);
    let health_s = env_u64("SWIM_ORACLE_HEALTH_S", 10);
    let sample_s = env_u64("SWIM_ORACLE_SAMPLE_S", 5).max(1);

    assert!(n >= 2, "need at least a seed + one worker");

    // Node 0 is the shared seed; workers 1..N bootstrap to it (and only it),
    // mirroring the Docker scale topology where every node knows the seed.
    let mut ports: Vec<u16> = (0..n).map(|_| alloc_port()).collect();
    let seed_idx = 0usize;
    let id = |ports: &[u16], i: usize| NodeId::new("127.0.0.1", ports[i]).unwrap();

    let mk_cfg = |ports: &[u16], i: usize| {
        let mut cfg = GossipConfig::default();
        cfg.bind_port = ports[i];
        cfg.bootstrap_peers =
            if i == seed_idx { vec![] } else { vec![id(ports, seed_idx)] };
        cfg.swim_failure_detector = true;
        cfg.swim_probe_interval_ms = probe_ms;
        cfg.swim_probe_timeout_ms = (probe_ms / 2).max(200);
        cfg.health_check_interval_secs = health_s;
        // Spread first health ticks so 100 nodes don't reconcile in lockstep.
        cfg.health_check_max_jitter_ms = 1000;
        cfg.reconnect_backoff_secs = 1;
        cfg
    };

    eprintln!(
        "# swim_scale_oracle N={n} secs={total_secs} churn_ms={churn_ms} \
         probe_ms={probe_ms} health_s={health_s}"
    );

    let mut agents: Vec<Arc<GossipAgent>> = Vec::with_capacity(n);
    for i in 0..n {
        // Retry on AddrInUse: ephemeral-port reuse / TIME_WAIT residue can collide
        // between alloc and bind, especially on back-to-back 100-node runs. The seed
        // (i=0) is finalised here before any worker reads `ports[seed_idx]`.
        let mut attempt = 0;
        let agent = loop {
            let id_i = id(&ports, i);
            let agent = Arc::new(GossipAgent::new(id_i, mk_cfg(&ports, i)));
            match agent.start().await {
                Ok(()) => break agent,
                Err(e) => {
                    attempt += 1;
                    assert!(attempt < 20, "agent {i} failed to start after {attempt} tries: {e:?}");
                    ports[i] = alloc_port();
                }
            }
        };
        agents.push(agent);
        // Stagger joins slightly so the seed isn't SYN-flooded at t=0 (also
        // closer to a real rollout than a thundering-herd start).
        if i % 10 == 9 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    // Optional continuous KV churn: every node re-advertises a heartbeat key on
    // a fixed cadence. This is the in-process stand-in for the demo's continuous
    // capability-advertisement gossip the finding suspected of keeping
    // forwarding writers warm.
    let mut churn_tasks = Vec::new();
    if churn_ms > 0 {
        for a in &agents {
            let a = Arc::clone(a);
            let key = format!("cap/{}/hb", a.node_id());
            churn_tasks.push(tokio::spawn(async move {
                let mut t = tokio::time::interval(Duration::from_millis(churn_ms));
                loop {
                    t.tick().await;
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;
                    let _ = a.kv().set(key.clone(), now.to_le_bytes().to_vec());
                }
            }));
        }
    }

    // Canary: the seed writes one key now; at the end we count how many nodes have it.
    // Connection flattening is only a win if KV still propagates cluster-wide — this guards
    // against "flattening" by accidentally starving anti-entropy.
    let _ = agents[seed_idx].kv().set("oracle/canary", b"1".to_vec());

    eprintln!("t_s,peers_min,peers_med,peers_max,seed_out,seed_in,seed_total");
    let start = Instant::now();
    loop {
        let elapsed = start.elapsed().as_secs();
        let (pmin, pmed, pmax) = membership_spread(&agents);
        let (sout, sin) = seed_conn_split(&agents, seed_idx);
        eprintln!(
            "{elapsed},{pmin},{pmed},{pmax},{sout},{sin},{}",
            sout + sin
        );
        if elapsed >= total_secs {
            break;
        }
        tokio::time::sleep(Duration::from_secs(sample_s)).await;
    }

    for t in churn_tasks {
        t.abort();
    }

    // Report-only — the assertions are loose so the harness always prints its
    // series. The point of this test is the time series, not a pass/fail gate.
    let (_pmin, pmed, _pmax) = membership_spread(&agents);
    let (sout, sin) = seed_conn_split(&agents, seed_idx);
    let canary_seen = agents.iter().filter(|a| a.kv().get("oracle/canary").is_some()).count();
    eprintln!(
        "# final: peers_med={pmed} seed_out={sout} seed_in={sin} seed_total={} \
         canary_seen={canary_seen}/{n} (N={n})",
        sout + sin
    );
}
