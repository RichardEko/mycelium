//! Worker SIDECAR node for the analytics scraper fleet (scraper-v3 #718 —
//! full-mesh workers; see that repo's `docs/wiki/dev/ops/fleet-operations.md`).
//!
//! Boots one GossipAgent that JOINS the fleet mesh via a bootstrap peer (the
//! primary started by `scrape_fleet_node`), exposes its own local HTTP gateway,
//! and mounts the tuple space in `TupleRole::Client` — pure producer/worker,
//! never serves, no WAL. Every `/gateway/tuple/*` call against THIS gateway is
//! transparently forwarded to the current primary over capability-discovered
//! mesh RPC (`tuple.{ns}.*`), so the Python worker only ever talks to
//! `127.0.0.1:<its own gateway>` — discovery replaces configuration.
//!
//! The worker process itself (analytics `fleet work`) advertises its
//! `scrape/worker` capability and `ui/label` through this sidecar's gateway
//! (`/gateway/capability/advertise`, `/gateway/kv`), which is what makes each
//! worker a first-class, console-visible mesh member (Ops Console Fleet tab).
//!
//! Usage:
//!   cargo run --release -p mycelium-tuple-space --features gateway \
//!       --example scrape_worker_node -- <bind_port> <http_port> [ns] [join]
//!
//! `GossipConfig` env overrides apply — launch with the same `GOSSIP_CLUSTER_NAME`
//! as the primary to keep the fleet's label consistent (unset = unlabelled).
//!
//! Defaults: ns "scraper-v3", join "127.0.0.1:7945" (the primary's bind port).
//! bind_port/http_port are REQUIRED and must be unique per worker (the
//! controller assigns them; convention: bind 7950+i, http 7960+i).

use mycelium::{GossipAgent, GossipConfig, NodeId};
use mycelium_tuple_space::{TupleConfig, TupleRole, TupleSpace};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let bind: u16 = args
        .next()
        .and_then(|s| s.parse().ok())
        .expect("bind_port (required, unique per worker)");
    let http: u16 = args
        .next()
        .and_then(|s| s.parse().ok())
        .expect("http_port (required, unique per worker)");
    let ns: String = args.next().unwrap_or_else(|| "scraper-v3".to_string());
    let join: String = args.next().unwrap_or_else(|| "127.0.0.1:7945".to_string());

    let (join_host, join_port) = join
        .rsplit_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("join port")))
        .expect("join must be host:port");

    let mut cfg = GossipConfig {
        bind_port: bind,
        http_port: Some(http),
        bootstrap_peers: vec![NodeId::new(&join_host, join_port).expect("join peer id")],
        ..Default::default()
    };
    // Env overrides are the startup configuration surface (see scrape_fleet_node.rs).
    // Launch sidecars with the same GOSSIP_CLUSTER_NAME as the primary so they don't
    // show up unlabelled next to a labelled primary.
    cfg.apply_env_overrides().expect("gossip env overrides");
    let agent = Arc::new(GossipAgent::new(
        NodeId::new("127.0.0.1", bind).expect("node id"),
        cfg,
    ));

    // Routes must be registered before agent.start(). Client role: no WAL,
    // never serves — ops route to the discovered primary.
    let ts = TupleSpace::new(
        Arc::clone(&agent),
        TupleConfig {
            namespace: Arc::from(ns.as_str()),
            role: TupleRole::Client,
            persist: false,
            ..Default::default()
        },
    )
    .await
    .expect("tuple space");
    agent.with_http_routes(Arc::clone(&ts).http_router());
    agent.start().await.expect("agent start");

    println!(
        "scrape-worker sidecar up: ns={ns} gateway=http://127.0.0.1:{http} joined={join} role=client"
    );
    tokio::signal::ctrl_c().await.ok();
    println!("scrape-worker sidecar: shutting down");
}
