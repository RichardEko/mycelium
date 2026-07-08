//! **Hello, capability** — broker-less service discovery + RPC in one file. No registry, no LLM.
//!
//! Run it:
//! ```sh
//! cargo run --example hello_capability
//! ```
//!
//! `alpha` **advertises** a capability `math/double` and serves it. `beta` — which was never told
//! alpha's address, only *what it wants* — **resolves** the capability by name, discovers alpha,
//! and **calls** it. That is the whole capability system: nodes advertise what they *do*; callers
//! find providers by *need*, not by address. No broker to run, no registry to write, no config.
//!
//! It's the natural next step after [`hello_mesh`](hello_mesh) (which shows just the shared KV
//! store). **Next:** guide [chapter 02 · Capabilities](../docs/guide/02-capabilities.md), or the
//! [FAQ](../docs/guide/faq.md).

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mycelium::{CapFilter, Capability, GossipAgent, GossipConfig, NodeId};

/// The RPC "kind" alpha serves and beta calls — a string both sides agree on (a capability
/// advertisement says *who provides* `math/double`; the RPC kind is *how you invoke* it).
const DOUBLE: &str = "math.double";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Two agents; `beta` bootstraps off `alpha` (see hello_mesh.rs for the setup details).
    let alpha = start_agent(7811, &[]).await?;
    let beta = start_agent(7812, &[7811]).await?;

    // ── alpha: advertise a capability, and serve the work behind it. ───────────────────────────
    // Advertising publishes "this node provides math/double" into the gossip KV. There is no
    // central registry — the ad simply propagates to peers, and *evaporates* if alpha ever stops
    // refreshing it (liveness is emergent). Hold the returned handle: dropping it retracts the ad.
    let _reg =
        alpha.capabilities().advertise_capability(Capability::new("math", "double"), Duration::from_secs(30));

    // Serve the actual work over RPC: read a number, double it, reply. `rpc_rx` yields incoming
    // requests for the `DOUBLE` kind; `rpc_respond` routes the reply back to the caller.
    let mut requests = alpha.service().rpc_rx(DOUBLE);
    let alpha_serving = Arc::clone(&alpha);
    tokio::spawn(async move {
        while let Some(req) = requests.recv().await {
            let n: u64 = String::from_utf8_lossy(&req.payload()).parse().unwrap_or(0);
            alpha_serving.service().rpc_respond(&req, (n * 2).to_string().into_bytes());
        }
    });

    // ── beta: resolve the capability by NEED, then call whoever provides it. ────────────────────
    // beta was never configured with alpha's address — only that it wants `math/double`. It waits
    // for the advertisement to arrive by gossip, then calls the provider it discovered.
    let provider = resolve_provider(&beta, "math", "double").await?;
    println!("beta discovered  math/double  on {provider}  ← an address it was never configured with");

    let reply = beta
        .service()
        .rpc_call(provider, DOUBLE, Bytes::from_static(b"21"), Duration::from_secs(5))
        .await?;
    println!("beta called      double(21) = {}", String::from_utf8_lossy(&reply));

    alpha.shutdown().await;
    beta.shutdown().await;
    println!("\n✓ one node advertised what it *does*; another found it by *need* and called it — no registry, no broker.");
    Ok(())
}

/// Start an agent on `127.0.0.1:{port}`, bootstrapping off the given peer ports.
async fn start_agent(port: u16, peers: &[u16]) -> Result<Arc<GossipAgent>, Box<dyn std::error::Error>> {
    let config = GossipConfig {
        bind_port: port, // an agent listens on `bind_port`, not the NodeId's port — set both
        bootstrap_peers: peers.iter().map(|p| NodeId::new("127.0.0.1", *p).unwrap()).collect(),
        ..Default::default()
    };
    let agent = Arc::new(GossipAgent::new(NodeId::new("127.0.0.1", port)?, config));
    agent.start().await?;
    Ok(agent)
}

/// Poll capability resolution until a provider of `ns/name` appears — discovery is eventually
/// consistent (the advertisement propagates by gossip), so a caller waits for it to arrive.
async fn resolve_provider(agent: &GossipAgent, ns: &str, name: &str) -> Result<NodeId, Box<dyn std::error::Error>> {
    let filter = CapFilter::new(ns, name);
    for _ in 0..100 {
        if let Some((node, _cap)) = agent.capabilities().resolve(&filter).into_iter().next() {
            return Ok(node);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err("no provider of the capability appeared within 5 s".into())
}
