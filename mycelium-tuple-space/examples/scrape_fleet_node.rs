//! Single-node tuple-space launcher for the analytics scraper fleet
//! (TathataSystems/analytics scraper-v3 P-F — see that repo's
//! `docs/dev/plans/scraper-v3-fleet-runbook.md` §1).
//!
//! Boots one GossipAgent with the HTTP gateway and a WAL-backed Primary
//! tuple space, then parks until Ctrl-C. Loopback only.
//!
//! Usage:
//!   cargo run --release -p mycelium-tuple-space --features gateway \
//!       --example scrape_fleet_node -- [bind_port] [http_port] [ns] [wal_path]
//!
//! Defaults: bind 7945, http gateway 7946, ns "scraper-v3", wal "./scrape-fleet.wal".
//! `GossipConfig` env overrides apply — e.g. `GOSSIP_CLUSTER_NAME=<label>` sets the
//! cosmetic cluster label shown on `/stats` and `/metrics` (unset = unlabelled).
//!
//! `worker_timeout_secs` is deliberately 7200 (2 h): fleet items are whole
//! council scrape runs that legitimately take 60–90 min — the library default
//! (300 s) would requeue a council while a worker is still scraping it,
//! producing a concurrent double-run.

use mycelium::{GossipAgent, GossipConfig, NodeId};
use mycelium_tuple_space::{TupleConfig, TupleRole, TupleSpace};
use std::path::PathBuf;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let bind: u16 = args.next().and_then(|s| s.parse().ok()).unwrap_or(7945);
    let http: u16 = args.next().and_then(|s| s.parse().ok()).unwrap_or(7946);
    let ns: String = args.next().unwrap_or_else(|| "scraper-v3".to_string());
    let wal: PathBuf = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./scrape-fleet.wal"));
    // Inflight deadline (5th arg). Default 14400s (4h): a cold first-crawl of a big
    // county council legitimately exceeds 2h (Flintshire ran ~2h40m on 2026-07-17),
    // and the old 7200s default requeued it mid-run → duplicate run + ghost (#726).
    let inflight: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(14400);

    let mut cfg = GossipConfig {
        bind_port: bind,
        http_port: Some(http),
        ..Default::default()
    };
    // Deployment knobs stay out of the source: env overrides (GOSSIP_CLUSTER_NAME
    // et al.) are the startup configuration surface. The cluster label lands on
    // GET /stats and as a `cluster` label on every /metrics series.
    cfg.apply_env_overrides().expect("gossip env overrides");
    let agent = Arc::new(GossipAgent::new(
        NodeId::new("127.0.0.1", bind).expect("node id"),
        cfg,
    ));

    // Routes must be registered before agent.start().
    let ts = TupleSpace::new(
        Arc::clone(&agent),
        TupleConfig {
            namespace: Arc::from(ns.as_str()),
            role: TupleRole::Primary,
            persist: true,
            wal_path: wal.clone(),
            worker_timeout_secs: inflight,
            ..Default::default()
        },
    )
    .await
    .expect("tuple space");
    agent.with_http_routes(Arc::clone(&ts).http_router());
    agent.start().await.expect("agent start");

    println!(
        "scrape-fleet tuple-space node up: ns={ns} gateway=http://127.0.0.1:{http} wal={} inflight-deadline={inflight}s",
        wal.display()
    );
    tokio::signal::ctrl_c().await.ok();
    println!("scrape-fleet node: shutting down");
}
