//! The fleet-reasoning worked example — all three Tier-3 wedges in one in-process mesh.
//!
//! A neighbourhood food-redistribution co-op runs a small agent fleet. Node C is the
//! *coordinator* agent that reasons about surplus-to-pantry matching; nodes A and B are
//! *workers* that serve the `fable-mini` model. The arc:
//!
//! 1. C declares its model dependency before any worker is up → **not ready** (wedge ③).
//! 2. A and B come up serving the model → the dependency resolves (wedge ③).
//! 3. C routes three calls load-aware across A/B, recording a trace (wedges ① + ②).
//! 4. A worker dies mid-run → the next call **fails over** to the survivor (wedge ①).
//! 5. The whole run is replayed and narrated from C's KV view (wedge ②).
//!
//! Run: `cargo run -p mycelium-reason --features llm --example fleet_reasoning`.
//! Exits 0 on success; asserted by `ci_smoke.sh` on the printed markers.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use mycelium::{EchoBackend, GossipAgent, GossipConfig, NodeId, PromptTemplate};
use mycelium_reason::{
    InferenceRouter, ModelProfile, ModelQuery, RouterConfig, TraceRecorder, narrate, replay,
    require_model, serve_model,
};

/// Start one agent, `None` if the bind lost the port race.
async fn try_start(port: u16, boot: Vec<u16>) -> Option<Arc<GossipAgent>> {
    let cfg = GossipConfig {
        bind_port: port,
        bootstrap_peers: boot.into_iter().map(|p| NodeId::new("127.0.0.1", p).unwrap()).collect(),
        ..Default::default()
    };
    let agent = Arc::new(GossipAgent::new(NodeId::new("127.0.0.1", port).unwrap(), cfg));
    agent.start().await.ok().map(|_| agent)
}

/// Start the three-node mesh, retrying the whole trio on fresh ports when a bind loses
/// the bind-:0 race (mutual bootstrap fixes all ports before any agent starts).
async fn start_trio() -> (Arc<GossipAgent>, Arc<GossipAgent>, Arc<GossipAgent>) {
    for _ in 0..16 {
        let free = || mycelium::test_util::alloc_port();
        let (pa, pb, pc) = (free(), free(), free());
        let Some(a) = try_start(pa, vec![pb, pc]).await else { continue };
        let Some(b) = try_start(pb, vec![pa, pc]).await else {
            a.shutdown_with_timeout(Duration::from_secs(5)).await;
            continue;
        };
        match try_start(pc, vec![pa, pb]).await {
            Some(c) => return (a, b, c),
            None => {
                a.shutdown_with_timeout(Duration::from_secs(5)).await;
                b.shutdown_with_timeout(Duration::from_secs(5)).await;
            }
        }
    }
    panic!("could not bind an agent trio after 16 attempts");
}

fn template() -> PromptTemplate {
    PromptTemplate {
        system: "You match surplus food lots to pantry demand.".into(),
        user_template: "{{input}}".into(),
        max_tokens: 128,
        temperature: 0.0,
        metadata: HashMap::new(),
    }
}

#[tokio::main]
async fn main() {
    let (worker_a, worker_b, coord) = start_trio().await;

    // ── (1) Wedge ③: the coordinator declares its dependency before any provider. ──
    let dep = require_model(&coord, "fable-mini", Duration::from_millis(500));
    match dep.await_ready(Duration::from_millis(400)).await {
        Ok(_) => unreachable!("nothing serves the model yet"),
        Err(e) => println!("coordinator: model not ready ({e})"),
    }

    // ── (2) Workers A and B bring the model up; the dependency resolves. ──
    let profile = || ModelProfile {
        model: "fable-mini".into(),
        ctx_window: Some(8192),
        family: Some("fable".into()),
        extra: Vec::new(),
    };
    let _reg_a = serve_model(&worker_a, profile(), template(), Arc::new(EchoBackend)).await.unwrap();
    let _reg_b = serve_model(&worker_b, profile(), template(), Arc::new(EchoBackend)).await.unwrap();
    let providers = dep.await_ready(Duration::from_secs(30)).await.unwrap();
    println!("coordinator: model ready — fable-mini served by {} provider(s)", providers.len());

    // ── (3) Wedges ① + ②: routed calls with a trace. ──
    let trace = TraceRecorder::new(Arc::clone(&coord), "surplus-run-1");
    trace.resume("fable-mini", 400, &providers);
    let router = InferenceRouter::new(
        Arc::clone(&coord),
        RouterConfig { call_timeout: Duration::from_secs(3), ..Default::default() },
    );
    let q = ModelQuery::new("fable-mini");
    let inputs = [
        "match 40kg apples from orchard-7 to pantries",
        "match 12 bread crates from bakery-3 to pantries",
        "match 25kg root vegetables from farm-2 to pantries",
    ];
    let mut last_provider = None;
    for input in inputs {
        let routed = router.call(&q, input, &HashMap::new(), Some(&trace)).await.unwrap();
        println!("coordinator: routed call → {} (attempt {})", routed.provider, routed.attempt);
        last_provider = Some(routed.provider);
    }

    // ── (4) Wedge ① failover: kill the last-used worker; the next call survives. ──
    let dead = last_provider.unwrap();
    let dying = if dead == *worker_a.node_id() { &worker_a } else { &worker_b };
    dying.shutdown_with_timeout(Duration::from_secs(5)).await;
    let routed = router
        .call(&q, "match 8 trays of eggs from coop-1 to pantries", &HashMap::new(), Some(&trace))
        .await
        .unwrap();
    assert_ne!(routed.provider, dead, "the dead worker cannot have answered");
    println!("coordinator: failover — call routed to {} (attempt {})", routed.provider, routed.attempt);

    // ── (5) Wedge ②: replay + narrate the run from the coordinator's KV view. ──
    // 1 resume + (4 route + 4 llm_call successes) minimum; failed attempts add more.
    let events = replay(&coord, "surplus-run-1");
    assert!(events.len() >= 9, "the full run is in the trace (got {})", events.len());
    println!("trace replay: {} events", events.len());
    for line in narrate(&events) {
        println!("  {line}");
    }

    coord.shutdown_with_timeout(Duration::from_secs(5)).await;
    worker_a.shutdown_with_timeout(Duration::from_secs(5)).await;
    worker_b.shutdown_with_timeout(Duration::from_secs(5)).await;
    println!("fleet_reasoning: OK");
}
