mod audit;
mod config;
mod llm;
mod runner;

use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

use mycelium::{
    Capability, CapValue, GossipAgent, GossipConfig, GossipError, NodeId,
    PersistenceConfig, SyncMode, TlsConfig,
};

use config::SkillFile;
use runner::SkillRunner;

#[tokio::main]
async fn main() {
    // Initialise tracing
    #[cfg(feature = "cli")]
    {
        use tracing_subscriber::{EnvFilter, fmt};
        fmt()
            .with_env_filter(EnvFilter::from_default_env()
                .add_directive("skillrunner=info".parse().unwrap())
                .add_directive("mycelium=warn".parse().unwrap()))
            .init();
    }

    let path = parse_skill_arg();

    let sf = match SkillFile::load(&path) {
        Ok(sf) => Arc::new(sf),
        Err(e) => {
            eprintln!("error: failed to load {path}: {e}");
            std::process::exit(1);
        }
    };

    if let Err(e) = run(sf).await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run(sf: Arc<SkillFile>) -> Result<(), Box<dyn std::error::Error>> {
    let node_id = NodeId::new(&sf.node.bind_address, sf.node.bind_port)?;
    let config  = build_gossip_config(&sf)?;
    let builder = GossipAgent::new(node_id, config);
    #[cfg(feature = "a2a")]
    let builder = if sf.node.http_port.is_some() { builder.with_a2a() } else { builder };
    let agent   = Arc::new(builder);

    // Optional OTEL tracer
    #[cfg(feature = "otel")]
    let otel_provider = sf.skill.otel.as_ref()
        .map(|cfg| audit::otel::init_tracer(cfg))
        .transpose()?;

    agent.start().await?;
    tracing::info!(
        "skillrunner: node {} started ({}:{})",
        agent.node_id(),
        sf.node.bind_address,
        sf.node.bind_port,
    );

    // Push input/output schemas to KV for tool discovery by peer skills
    let node_id_str = agent.node_id().to_string();
    let ns   = &sf.capability.ns;
    let name = &sf.capability.name;

    if let Some(ref schema) = sf.capability.input {
        let key = format!("skills/{ns}/{name}/{node_id_str}/input");
        let _ = agent.set(key, Bytes::from(serde_json::to_vec(schema)?));
    }
    if let Some(ref schema) = sf.capability.output {
        let key = format!("skills/{ns}/{name}/{node_id_str}/output");
        let _ = agent.set(key, Bytes::from(serde_json::to_vec(schema)?));
    }

    // Advertise capability on the mesh
    let cap = build_capability(&sf);
    let refresh = Duration::from_secs(sf.capability.ttl_secs);
    let _cap_handle = agent.advertise_capability(cap, refresh);

    tracing::info!(
        "skillrunner: advertising {ns}/{name} (refresh {}s, max_concurrent {:?})",
        sf.capability.ttl_secs,
        sf.capability.policy.as_ref().and_then(|p| p.max_concurrent),
    );

    // Build HTTP client for LLM calls
    let http_client = Arc::new(
        reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()?,
    );

    // Graceful shutdown on ctrl-c / SIGTERM
    let agent_shutdown = Arc::clone(&agent);
    tokio::spawn(async move {
        shutdown_signal().await;
        tracing::info!("skillrunner: shutting down");
        agent_shutdown.shutdown().await;
    });

    // Run the skill invocation loop (blocks until shutdown)
    let skill_runner = SkillRunner {
        agent:  Arc::clone(&agent),
        skill:  Arc::clone(&sf),
        client: http_client,
        #[cfg(feature = "otel")]
        otel:   otel_provider,
    };
    skill_runner.run().await;

    Ok(())
}

#[allow(clippy::field_reassign_with_default)]
fn build_gossip_config(sf: &SkillFile) -> Result<GossipConfig, GossipError> {
    let mut cfg = GossipConfig::default();
    cfg.bind_address = sf.node.bind_address.clone();
    cfg.bind_port    = sf.node.bind_port;
    cfg.http_port    = sf.node.http_port;

    cfg.bootstrap_peers = sf.node.bootstrap_peers.iter()
        .filter_map(|addr| {
            let (ip, port_str) = addr.rsplit_once(':')?;
            let port: u16 = port_str.parse().ok()?;
            NodeId::new(ip, port).ok()
        })
        .collect();

    if let Some(ref p) = sf.node.persistence {
        cfg.persistence = Some(PersistenceConfig {
            base_path:               p.base_path.clone().into(),
            sync_mode:               if p.sync_flush { SyncMode::Flush } else { SyncMode::Async },
            snapshot_wal_threshold:  10_000,
            snapshot_interval_secs:  300,
        });
    }

    if let Some(ref t) = sf.node.tls {
        cfg.tls = Some(TlsConfig {
            cert_pem:     t.cert_pem.as_ref().map(Into::into),
            key_pem:      t.key_pem.as_ref().map(Into::into),
            ca_cert_pem:  t.ca_cert_pem.as_ref().map(Into::into),
            auto_cert_dir: t.auto_cert_dir.as_ref()
                .map(Into::into)
                .unwrap_or_else(|| "./mycelium-tls/".into()),
        });
    }

    Ok(cfg)
}

fn build_capability(sf: &SkillFile) -> Capability {
    let mut cap = Capability::new(sf.capability.ns.as_str(), sf.capability.name.as_str());

    if let Some(ref desc) = sf.capability.description {
        cap = cap.with("description", CapValue::Text(desc.as_str().into()));
    }

    if let Some(ref policy) = sf.capability.policy {
        if !policy.authorized_callers.is_empty() {
            cap = cap.with_authorized_callers(policy.authorized_callers.iter().map(String::as_str));
        }
    }

    // Advertise platform requirements as capability attributes so capability
    // resolution can filter on them (e.g. CapConstraint::Eq "gpu")
    if let Some(ref platform) = sf.capability.platform {
        for req in &platform.requires {
            cap = cap.with(format!("requires.{req}"), CapValue::Bool(true));
        }
    }

    cap
}

fn parse_skill_arg() -> String {
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        if (args[i] == "--skill" || args[i] == "-s") && i + 1 < args.len() {
            return args[i + 1].clone();
        }
        if let Some(val) = args[i].strip_prefix("--skill=") {
            return val.to_string();
        }
        i += 1;
    }
    eprintln!("usage: skillrunner --skill <path/to/skill.toml>");
    std::process::exit(1);
}

async fn shutdown_signal() {
    use tokio::signal;
    let ctrl_c = async { signal::ctrl_c().await.ok(); };

    #[cfg(unix)]
    {
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    ctrl_c.await;
}
