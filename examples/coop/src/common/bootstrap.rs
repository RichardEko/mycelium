//! Depot spin-up with consistent config across the suite.
//!
//! Every depot runs with the **gateway** (for the AgentFacts lens + diagnostics) and a **tls**
//! identity (AgentFacts are self-signed, and mTLS gives the cluster its peer identity). In-process
//! multi-node demos share one `cert_dir` so they trust the same auto-generated CA — separate dirs
//! mean separate CAs and the handshake fails.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use mycelium::config::TlsConfig;
use mycelium::{GossipAgent, GossipConfig, NodeId};

use super::facts_lens;

/// Grab `n` **mutually-distinct** OS-assigned free TCP ports on loopback. All `n` listeners are
/// held open at once before any port is returned — otherwise binding `:0`, dropping, and binding
/// `:0` again can hand back the *same* just-freed ephemeral port, so two agents would collide.
/// (A small TOCTOU window remains between this returning and an agent binding; fine for a demo.)
pub fn alloc_ports(n: usize) -> Vec<u16> {
    let listeners: Vec<std::net::TcpListener> = (0..n)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port"))
        .collect();
    listeners
        .iter()
        .map(|l| l.local_addr().expect("local addr").port())
        .collect()
    // listeners drop here, freeing all n distinct ports for the agents to bind.
}

/// Everything needed to bring one depot online.
pub struct DepotOpts {
    /// A constructive node name for logs (e.g. `"depot-camden"`).
    pub name:        String,
    /// Gossip (TCP) port.
    pub gossip_port: u16,
    /// Gateway HTTP port — must differ from `gossip_port`. Serves the AgentFacts lens.
    pub http_port:   u16,
    /// Jurisdiction/zone, published as an AgentFacts field + locality.
    pub zone:        String,
    /// Gossip ports of bootstrap peers (empty = this is a seed).
    pub bootstrap:   Vec<u16>,
    /// Shared auto-CA directory (all depots in one demo must share this).
    pub cert_dir:    PathBuf,
    /// Optional faster health/convergence tick (seconds) for governor demos. `None` = library
    /// default (10 s). When `Some(h)`, `reconnect_backoff` is set to `h.saturating_sub(3)` so the
    /// tuning invariant (`reconnect_backoff + 2 < health`) holds.
    pub health_secs: Option<u64>,
}

/// A running depot.
pub struct Depot {
    pub name:        String,
    pub agent:       Arc<GossipAgent>,
    pub gossip_port: u16,
    pub http_port:   u16,
}

impl Depot {
    /// This depot's `NodeId`.
    pub fn node_id(&self) -> NodeId {
        self.agent.node_id().clone()
    }

    /// Graceful shutdown with a bounded drain.
    pub async fn shutdown(&self) {
        self.agent.shutdown_with_timeout(Duration::from_secs(5)).await;
    }
}

/// Bring one depot online: build config, mount the AgentFacts lens, start, publish baseline facts.
pub async fn spawn_depot(opts: DepotOpts) -> Result<Depot, Box<dyn std::error::Error>> {
    let id = NodeId::new("127.0.0.1", opts.gossip_port)?;

    let mut bootstrap_peers = Vec::with_capacity(opts.bootstrap.len());
    for p in &opts.bootstrap {
        bootstrap_peers.push(NodeId::new("127.0.0.1", *p)?);
    }

    let mut cfg = GossipConfig {
        bind_port: opts.gossip_port,
        http_port: Some(opts.http_port),
        bootstrap_peers,
        tls: Some(TlsConfig { auto_cert_dir: opts.cert_dir.clone(), ..TlsConfig::default() }),
        // Every coop node carries a cluster name (never null on /stats or the `cluster=` metric
        // label) — a "coop" default, overridable with GOSSIP_CLUSTER_NAME so one Prometheus/Grafana
        // can tell environments apart. It is a *label*, not a membership boundary.
        cluster_name: Some(std::env::var("GOSSIP_CLUSTER_NAME").unwrap_or_else(|_| "coop".to_string())),
        ..Default::default()
    };
    if let Some(h) = opts.health_secs {
        cfg.health_check_interval_secs = h.max(1);
        // reconnect_backoff must be >= 1 (0 is rejected); keep it < health where possible.
        cfg.reconnect_backoff_secs = h.saturating_sub(3).max(1);
        cfg.health_check_max_jitter_ms = 50;
    }

    let agent = Arc::new(GossipAgent::new(id, cfg));

    // Mount the lens BEFORE start — extra routes are taken when the gateway server spawns.
    facts_lens::mount(&agent, &opts.zone, opts.http_port);

    agent.start().await?;

    // Baseline facts need a live identity + KV — publish after start.
    facts_lens::publish_baseline(&agent, &opts.zone);

    Ok(Depot {
        name:        opts.name,
        agent,
        gossip_port: opts.gossip_port,
        http_port:   opts.http_port,
    })
}
