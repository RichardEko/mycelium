//! **The 30-second "hello mesh".** Two embedded agents on loopback share state by gossip —
//! no broker, no coordinator, no config files, no LLM, no features to enable.
//!
//! Run it:
//! ```sh
//! cargo run --example hello_mesh
//! ```
//!
//! What happens: `alpha` writes a key into the gossip KV store; `beta` — which was told only
//! `alpha`'s address — learns the value by gossip and prints it. That's the whole substrate,
//! Layer I: an eventually-consistent key-value store every node shares. Signals, capabilities,
//! consensus, skills and everything else build on exactly this.
//!
//! **Next step:** the FAQ is your map — [`docs/guide/faq.md`](../docs/guide/faq.md).

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mycelium::{GossipAgent, GossipConfig, NodeId};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Two agents on loopback. `beta` bootstraps off `alpha`'s port — that is the *entire*
    // cluster setup: no registry to configure, no broker to run.
    let alpha = start_agent(7801, &[]).await?;
    let beta = start_agent(7802, &[7801]).await?;

    // `alpha` writes into the gossip KV store. The write is local and instant; it then
    // propagates epidemically to every peer that can reach `alpha`.
    // `set` returns `false` only if the write exceeds the size cap (~10 MiB); "hello" is fine.
    let accepted = alpha.kv().set("greeting", Bytes::from_static(b"hello from alpha"));
    assert!(accepted, "the write was within the size cap");
    println!("alpha wrote   greeting = \"hello from alpha\"");

    // `beta` learns it by gossip. The store is *eventually* consistent, so a reader waits for
    // the value to converge rather than getting an instant answer (loopback: well under a second).
    let value = await_key(&beta, "greeting").await?;
    println!(
        "beta  read    greeting = \"{}\"   ← arrived by gossip, no coordinator asked",
        String::from_utf8_lossy(&value),
    );

    alpha.shutdown().await;
    beta.shutdown().await;
    println!("\n✓ two agents, one shared value, zero coordinator — that's Mycelium's Layer I.");
    Ok(())
}

/// Start an agent on `127.0.0.1:{port}`, bootstrapping off the given peer ports.
///
/// The one non-obvious line is `bind_port`: an agent listens on `config.bind_port`, **not** on
/// the `NodeId`'s port — so set both to the same value (a common first-time gotcha).
async fn start_agent(port: u16, peers: &[u16]) -> Result<Arc<GossipAgent>, Box<dyn std::error::Error>> {
    let config = GossipConfig {
        bind_port: port,
        bootstrap_peers: peers.iter().map(|p| NodeId::new("127.0.0.1", *p).unwrap()).collect(),
        cluster_name: Some(std::env::var("GOSSIP_CLUSTER_NAME").unwrap_or_else(|_| "hello-mesh".to_string())),
        ..Default::default()
    };
    let agent = Arc::new(GossipAgent::new(NodeId::new("127.0.0.1", port)?, config));
    agent.start().await?;
    Ok(agent)
}

/// Poll the KV store until `key` appears — gossip is eventually consistent, so a reader waits
/// for convergence (here, up to 5 s) instead of blocking on a coordinator for an answer.
async fn await_key(agent: &GossipAgent, key: &str) -> Result<Bytes, Box<dyn std::error::Error>> {
    for _ in 0..100 {
        if let Some(value) = agent.kv().get(key) {
            return Ok(value);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err("key never converged within 5 s — is the mesh connected?".into())
}
