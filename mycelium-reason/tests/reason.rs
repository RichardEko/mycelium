//! Cross-node integration: the three wedges + the blob tier over real two-agent meshes.
//!
//! Robustness: assertions are **structural polls on generous timeouts**, never fixed
//! sleeps (testing.md); agents start via [`start_pair`]'s whole-pair retry (the bind-:0
//! TOCTOU flake class, 2026-07-07); every started agent is shut down at test end.
#![allow(clippy::field_reassign_with_default)] // GossipConfig is built the way mycelium's own tests do

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mycelium::{CapEntry, CapFilter, Capability, EchoBackend, GossipAgent, GossipConfig, NodeId, PromptTemplate};
use mycelium_reason::{
    FsBlobStore, InferenceRouter, MeshBlobStore, ModelProfile, ModelQuery, RouterConfig,
    TraceRecorder, replay, require_model, serve_model, spawn_blob_server,
};

/// A free TCP port (bind :0, read it, drop). The drop opens a TOCTOU window against
/// parallel test binaries — which is why agents start via [`start_pair`]'s retry.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// Start one agent, `None` if the bind lost the port race.
async fn try_start(port: u16, boot: Vec<u16>) -> Option<Arc<GossipAgent>> {
    let mut cfg = GossipConfig::default();
    cfg.bind_port = port;
    cfg.bootstrap_peers = boot.into_iter().map(|p| NodeId::new("127.0.0.1", p).unwrap()).collect();
    let agent = Arc::new(GossipAgent::new(NodeId::new("127.0.0.1", port).unwrap(), cfg));
    agent.start().await.ok().map(|_| agent)
}

/// Start a mutually-bootstrapped agent pair, retrying the *whole pair* on fresh ports
/// when a bind loses the `free_port` race (mutual bootstrap means both ports must be
/// fixed before either agent starts, so a per-agent retry can't work).
async fn start_pair() -> (Arc<GossipAgent>, Arc<GossipAgent>) {
    for _ in 0..16 {
        let (pa, pb) = (free_port(), free_port());
        let Some(a) = try_start(pa, vec![pb]).await else { continue };
        match try_start(pb, vec![pa]).await {
            Some(b) => return (a, b),
            None => a.shutdown_with_timeout(Duration::from_secs(5)).await,
        }
    }
    panic!("could not bind an agent pair after 16 attempts");
}

async fn poll_until(mut cond: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cond()
}

fn echo_template() -> PromptTemplate {
    PromptTemplate {
        system: "test".into(),
        user_template: "{{input}}".into(),
        max_tokens: 64,
        temperature: 0.0,
        metadata: HashMap::new(),
    }
}

fn profile(model: &str) -> ModelProfile {
    ModelProfile {
        model: model.into(),
        ctx_window: Some(8192),
        family: Some("fable".into()),
        extra: Vec::new(),
    }
}

/// Wedge ①: both nodes serve; the router answers, and after the winning provider is
/// shut down the next call succeeds via the survivor (failover down the candidate list —
/// possibly through a stale cap entry for the dead node, which is the point).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn route_and_failover_across_providers() {
    let (agent_a, agent_b) = start_pair().await;
    let _reg_a = serve_model(&agent_a, profile("fable-mini"), echo_template(), Arc::new(EchoBackend))
        .await
        .unwrap();
    let _reg_b = serve_model(&agent_b, profile("fable-mini"), echo_template(), Arc::new(EchoBackend))
        .await
        .unwrap();

    let cfg = RouterConfig {
        max_attempts: 3,
        call_timeout: Duration::from_secs(2), // fast per-attempt failover under test
        failover_timeout: Duration::from_secs(1),
        load_max_age: Duration::from_secs(10),
    };
    let router_a = InferenceRouter::new(Arc::clone(&agent_a), cfg.clone());
    let q = ModelQuery::new("fable-mini");

    // Both cap ads have gossiped.
    assert!(
        poll_until(|| router_a.candidates(&q).len() == 2, Duration::from_secs(20)).await,
        "both providers visible to the router"
    );

    let routed = router_a.call(&q, "hello mesh", &HashMap::new(), None).await.unwrap();
    assert_eq!(routed.output, "echo: hello mesh");
    assert_eq!(routed.model_used, "echo");
    let winner = routed.provider.clone();

    // Kill the winner; route again FROM THE SURVIVOR (the winner may have been the
    // router's own node). The survivor serves the model itself, so the call must
    // succeed — via a retry past the dead node if its cap entry is still live.
    let (survivor, dead) = if winner == *agent_a.node_id() {
        (Arc::clone(&agent_b), Arc::clone(&agent_a))
    } else {
        (Arc::clone(&agent_a), Arc::clone(&agent_b))
    };
    dead.shutdown_with_timeout(Duration::from_secs(5)).await;

    let router_s = InferenceRouter::new(Arc::clone(&survivor), cfg);
    let routed2 = router_s.call(&q, "after failover", &HashMap::new(), None).await.unwrap();
    assert_eq!(routed2.output, "echo: after failover");
    assert_ne!(routed2.provider, winner, "the survivor answered, not the dead winner");
    assert!(routed2.attempt <= 2, "at most one failed attempt against the dead node");

    agent_a.shutdown_with_timeout(Duration::from_secs(5)).await;
    agent_b.shutdown_with_timeout(Duration::from_secs(5)).await;
}

/// The liveness filter, tested against exactly the case that matters: a **fresh cap ad
/// for a node that is not a live SWIM member** — what an ungracefully-dead node leaves
/// behind (its `cap/` ad lingers ~90 s, but it is gone from `peers()` at once, and its
/// death may have raced its own cap-tombstone gossip). `resolve()` is liveness-blind, so
/// it returns the ghost; `candidates()` must drop it, leaving only self.
///
/// A graceful in-process `shutdown` cannot reproduce this — shutdown retracts the cap, so
/// `resolve()` alone would drop it and the filter would never be exercised. Here the cap
/// deliberately lingers, so **only** the liveness filter can prune it: this FAILS without
/// the filter (the ghost stays a candidate) and passes with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn liveness_filter_drops_a_non_peer_cap() {
    let agent = {
        let mut started = None;
        for _ in 0..16 {
            if let Some(a) = try_start(free_port(), vec![]).await {
                started = Some(a);
                break;
            }
        }
        started.expect("single agent start")
    };
    let _reg = serve_model(&agent, profile("fable-mini"), echo_template(), Arc::new(EchoBackend))
        .await
        .unwrap();

    // Inject a fresh cap ad for a ghost node that is NOT (and never was) a peer — the
    // ungracefully-dead-node residue. `refresh_interval_ms = 30_000` ⇒ fresh for ~90 s,
    // far longer than this test runs.
    let ghost = NodeId::new("127.0.0.1", 59999).unwrap();
    let entry = CapEntry {
        capability: Capability::new("llm", "fable-mini"),
        refresh_interval_ms: 30_000,
    };
    assert!(
        agent.kv().set(format!("cap/{ghost}/llm/fable-mini"), entry.encode()),
        "ghost cap injected"
    );

    let router = InferenceRouter::new(Arc::clone(&agent), RouterConfig::default());
    let q = ModelQuery::new("fable-mini");

    // resolve() is liveness-blind → it sees BOTH self and the injected ghost.
    assert!(
        poll_until(
            || agent.capabilities().resolve(&CapFilter::new("llm", "fable-mini")).len() == 2,
            Duration::from_secs(5),
        )
        .await,
        "resolve() sees self + the ghost (it is liveness-blind)"
    );

    // candidates() is liveness-filtered → only self; the non-peer ghost is pruned.
    let c = router.candidates(&q);
    assert_eq!(c.len(), 1, "the non-peer ghost is pruned by liveness");
    assert_eq!(c[0].0, *agent.node_id(), "only self remains");

    agent.shutdown_with_timeout(Duration::from_secs(5)).await;
}

/// Wedge ① constraints: a query whose `llm-meta` constraint no provider satisfies
/// yields no candidates, while a satisfiable one keeps them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_constraints_filter_candidates() {
    use mycelium::{CapConstraint, CapValue};

    let (agent_a, agent_b) = start_pair().await;
    let _reg_b = serve_model(&agent_b, profile("fable-mini"), echo_template(), Arc::new(EchoBackend))
        .await
        .unwrap();

    let router = InferenceRouter::new(Arc::clone(&agent_a), RouterConfig::default());
    let plain = ModelQuery::new("fable-mini");
    assert!(
        poll_until(|| !router.candidates(&plain).is_empty(), Duration::from_secs(20)).await,
        "provider visible"
    );

    let mut fits = ModelQuery::new("fable-mini");
    fits.constraints = vec![("ctx_window".into(), CapConstraint::Gte(CapValue::Integer(4096)))];
    assert!(
        poll_until(|| router.candidates(&fits).len() == 1, Duration::from_secs(20)).await,
        "satisfiable constraint keeps the provider"
    );

    let mut too_big = ModelQuery::new("fable-mini");
    too_big.constraints = vec![("ctx_window".into(), CapConstraint::Gte(CapValue::Integer(1_000_000)))];
    assert!(router.candidates(&too_big).is_empty(), "unsatisfiable constraint drops it");

    agent_a.shutdown_with_timeout(Duration::from_secs(5)).await;
    agent_b.shutdown_with_timeout(Duration::from_secs(5)).await;
}

/// Wedge ②: events recorded on both nodes into one run gossip-converge; replay on
/// either node sees all of them, HLC-ordered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trace_records_converge_across_nodes() {
    let (agent_a, agent_b) = start_pair().await;

    let tr_a = TraceRecorder::new(Arc::clone(&agent_a), "run-xnode");
    let tr_b = TraceRecorder::new(Arc::clone(&agent_b), "run-xnode");
    tr_a.tool_call("demand-forecast", true);
    tr_a.record("custom", serde_json::json!({ "step": 1 }));
    tr_b.tool_call("surplus-match", true);
    tr_b.resume("fable-mini", 120, &[agent_b.node_id().clone()]);

    // Node B replays ALL four events (two of them written on A → gossip).
    assert!(
        poll_until(|| replay(&agent_b, "run-xnode").len() == 4, Duration::from_secs(20)).await,
        "all four events visible on node B"
    );
    let events = replay(&agent_b, "run-xnode");
    assert!(events.windows(2).all(|w| w[0].hlc <= w[1].hlc), "HLC-ordered");
    let nodes: std::collections::HashSet<_> = events.iter().map(|e| e.node.clone()).collect();
    assert!(nodes.contains(&agent_a.node_id().to_string()));
    assert!(nodes.contains(&agent_b.node_id().to_string()));
    // The narrative covers every event.
    assert_eq!(mycelium_reason::narrate(&events).len(), 4);

    agent_a.shutdown_with_timeout(Duration::from_secs(5)).await;
    agent_b.shutdown_with_timeout(Duration::from_secs(5)).await;
}

/// Wedge ③: the dependency pends while nothing serves the model, then resolves to the
/// peer once `serve_model` runs there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn require_model_pends_then_resolves() {
    let (agent_a, agent_b) = start_pair().await;

    let dep = require_model(&agent_a, "fable-mini", Duration::from_millis(300));
    assert!(dep.providers().is_empty());
    let pending = dep.await_ready(Duration::from_millis(600)).await;
    assert!(pending.is_err(), "no provider yet → await_ready times out");

    let _reg_b = serve_model(&agent_b, profile("fable-mini"), echo_template(), Arc::new(EchoBackend))
        .await
        .unwrap();

    let providers = dep.await_ready(Duration::from_secs(20)).await.unwrap();
    assert!(
        providers.contains(agent_b.node_id()),
        "the serving peer is among the resolved providers"
    );

    agent_a.shutdown_with_timeout(Duration::from_secs(5)).await;
    agent_b.shutdown_with_timeout(Duration::from_secs(5)).await;
}

/// Blob tier: a blob stored on A is fetched (verified) by B over RPC and write-back
/// cached, so the second get is a local hit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blob_mesh_fetch_verifies_and_caches() {
    let (agent_a, agent_b) = start_pair().await;

    let dir_a = tempfile::tempdir().unwrap();
    let store_a = Arc::new(FsBlobStore::open(dir_a.path()).unwrap());
    let payload = Bytes::from(vec![7u8; 128 * 1024]); // 128 KiB — real but frame-friendly
    let id = store_a.put(&payload).unwrap();
    let _server = spawn_blob_server(&agent_a, Arc::clone(&store_a));

    let dir_b = tempfile::tempdir().unwrap();
    let store_b = Arc::new(FsBlobStore::open(dir_b.path()).unwrap());
    let mesh_b = MeshBlobStore::new(Arc::clone(&agent_b), Arc::clone(&store_b), Duration::from_secs(5));

    // B misses locally until A's blob-cache capability has gossiped; poll the fetch
    // itself (structural: success == the bytes arrived and verified).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut fetched = None;
    while tokio::time::Instant::now() < deadline {
        if let Some(bytes) = mesh_b.get(&id).await {
            fetched = Some(bytes);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(fetched.expect("mesh fetch succeeded").as_ref(), payload.as_ref());
    assert!(store_b.contains(&id), "write-back cached locally");
    // Second get is a local hit (works even with the server gone).
    drop(_server);
    assert_eq!(mesh_b.get(&id).await.unwrap().as_ref(), payload.as_ref());

    // The empty blob (a typed None payload — the checkpointer mints it) is answered
    // from its content address alone: no local file, no provider, still a hit.
    let empty_id = mycelium_reason::BlobId::of(b"");
    assert!(!store_b.contains(&empty_id));
    assert_eq!(mesh_b.get(&empty_id).await.expect("empty blob resolves").len(), 0);

    agent_a.shutdown_with_timeout(Duration::from_secs(5)).await;
    agent_b.shutdown_with_timeout(Duration::from_secs(5)).await;
}
