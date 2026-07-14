//! Minimal caller for SkillRunner skills — smoke test and demo driver.
//!
//! Guide chapter: docs/guide/05-skills.md
//!
//! Usage (in two terminals):
//!   Terminal 1: cargo run --bin skillrunner -- --skill examples/skills/hello.skill.toml
//!   Terminal 2: cargo run --example invoke_skill
//!
//! Environment overrides (used by examples/community/invoke.sh to drive the
//! researcher → writer → verifier pipeline through the same caller):
//!   SKILL_CALLER_PORT  caller's own gossip port            (default 7970)
//!   SKILL_NODE_PORT    bootstrap peer's gossip port        (default 7950)
//!   SKILL_CAP          capability to resolve, "ns/name"    (default "llm/hello")
//!   SKILL_PAYLOAD      raw JSON payload for skill.invoke   (default a hello message)

use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;
use mycelium::{CapFilter, GossipAgent, GossipConfig, NodeId};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Caller node; bootstraps off the skillrunner seed on 7950.
    // Use port 7970 to avoid conflicting with community example nodes (7950-7955).
    let caller_port: u16 = std::env::var("SKILL_CALLER_PORT")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(7970);
    let node_port: u16 = std::env::var("SKILL_NODE_PORT")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(7950);
    let cap = std::env::var("SKILL_CAP").unwrap_or_else(|_| "llm/hello".to_string());
    let (ns, name) = cap.split_once('/')
        .ok_or_else(|| format!("SKILL_CAP must be \"ns/name\", got {cap:?}"))?;
    let (ns, name) = (ns.to_string(), name.to_string());
    let payload: Vec<u8> = match std::env::var("SKILL_PAYLOAD") {
        Ok(json) => {
            // Validate early so a typo fails here, not inside the skill.
            let _: serde_json::Value = serde_json::from_str(&json)
                .map_err(|e| format!("SKILL_PAYLOAD is not valid JSON: {e}"))?;
            json.into_bytes()
        }
        Err(_) => serde_json::to_vec(&serde_json::json!({
            "message": "Hello from Mycelium! What is a gossip protocol in one sentence?"
        }))?,
    };

    let node_id   = NodeId::new("127.0.0.1", caller_port)?;
    let skill_node = NodeId::new("127.0.0.1", node_port)?;

    let mut cfg = GossipConfig::default();
    cfg.cluster_name = Some(std::env::var("GOSSIP_CLUSTER_NAME").unwrap_or_else(|_| "invoke-skill".to_string()));
    cfg.bind_port = caller_port;
    cfg.bootstrap_peers = vec![skill_node.clone()];

    let agent = Arc::new(GossipAgent::new(node_id, cfg));
    agent.start().await?;
    println!("caller: started on :{caller_port}, waiting for {ns}/{name} capability...");

    // Poll until the capability appears (up to 15 s)
    let skill_id = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let filter = CapFilter::new(ns.as_str(), name.as_str());
            if let Some((id, _)) = agent.capabilities().resolve(&filter).into_iter().next() {
                return id;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }).await.map_err(|_| format!("timed out waiting for {ns}/{name} capability on mesh"))?;

    println!("caller: found skill on node {skill_id}, invoking...");

    let result = agent.service().rpc_call(
        skill_id,
        "skill.invoke",
        Bytes::from(payload),
        Duration::from_secs(180),   // LLM pipelines (research → write → verify) can be slow
    ).await?;

    let json: serde_json::Value = serde_json::from_slice(&result)
        .unwrap_or(serde_json::Value::String(String::from_utf8_lossy(&result).into_owned()));

    println!("reply: {}", serde_json::to_string_pretty(&json)?);

    // Check audit trail arrived in KV
    tokio::time::sleep(Duration::from_millis(500)).await;
    let audit = agent.kv().scan_prefix("audit/");
    println!("\naudit records on mesh: {}", audit.len());
    for (k, _) in &audit {
        println!("  {k}");
    }

    agent.shutdown().await;
    Ok(())
}
