//! The AgentFacts **lens** — mounted on every depot so any running example can be inspected live
//! at the federation edge. This is selection #3 ("AgentFacts views on every example") implemented
//! as shared infrastructure rather than a standalone demo.
//!
//! - [`mount`] mounts the WS-F edge router on the agent's embedded gateway (`/.well-known/
//!   agent-facts.json` — the self-certified PULL doc — plus the converged CRDT board). Must be
//!   called **before** `agent.start()` (extra routes are consumed at startup).
//! - [`publish_baseline`] publishes a couple of per-field-signed facts (`status`, `zone`) **after**
//!   start, so the intra-domain CRDT board has something to assemble.

use std::sync::Arc;

use mycelium::GossipAgent;
use mycelium_agentfacts::{agent_facts_router, publish_field, FactsOptions};
use serde_json::json;

/// Mount the AgentFacts edge endpoint on `agent`'s gateway. `http_port` is the agent's gateway
/// port (`GossipConfig::http_port`); `zone` becomes the published jurisdiction/locality.
pub fn mount(agent: &Arc<GossipAgent>, zone: &str, http_port: u16) {
    let opts = FactsOptions {
        endpoints: vec![format!("http://127.0.0.1:{http_port}/.well-known/agent-facts.json")],
        locality:  Some(zone.to_string()),
        ttl_secs:  300,
        ..Default::default()
    };
    agent.with_http_routes(agent_facts_router(Arc::clone(agent), opts));
}

/// Publish this depot's baseline self-signed facts onto the intra-domain CRDT board. Call after
/// `start()`. Best-effort (a node without a tls identity simply publishes nothing).
pub fn publish_baseline(agent: &GossipAgent, zone: &str) {
    let _ = publish_field(agent, "status", json!("ready"));
    let _ = publish_field(agent, "zone", json!(zone));
}
