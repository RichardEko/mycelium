//! Phase-2-remainder · the access broker: the curator's membership gate on store access, driven
//! cross-node. A pinned curator answers a reader's `request_store_access` RPC — granting a `StoreGrant`
//! (the location) to an allowlisted node, denying one that isn't. The grant is a **one-time** handshake;
//! after it the reader opens the store directly (verified: the granted location *is* the reader's store).
#![cfg(feature = "control-plane")]
#![allow(clippy::field_reassign_with_default)]

use std::sync::Arc;
use std::time::Duration;

use mycelium::{GossipAgent, GossipConfig, NodeId};
use mycelium_wiki::{
    AccessError, CuratorBrain, DirectReconciler, FsStore, Membership, StoreGrant, Wiki, WikiConfig,
    WikiRole, WikiStore,
};

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

async fn poll_until(mut cond: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if cond() { return true; }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cond()
}

fn wcfg(role: WikiRole) -> WikiConfig {
    WikiConfig {
        group: "ops".into(), role,
        cap_refresh: Duration::from_millis(300), drain_interval: Duration::from_millis(150),
        lint_interval: Duration::from_secs(5),
    }
}

async fn spawn_agent(port: u16, boot: Vec<u16>) -> Arc<GossipAgent> {
    let mut cfg = GossipConfig::default();
    cfg.bind_port = port;
    cfg.bootstrap_peers = boot.into_iter().map(|p| NodeId::new("127.0.0.1", p).unwrap()).collect();
    let a = Arc::new(GossipAgent::new(NodeId::new("127.0.0.1", port).unwrap(), cfg));
    a.start().await.unwrap();
    a
}

/// Retry the broker handshake past the startup window (curator-ad propagation + handler registration);
/// `Denied` is terminal, transient errors retry until the deadline.
async fn access(w: &Wiki<FsStore>, timeout: Duration) -> Result<StoreGrant, AccessError> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match w.request_store_access().await {
            Ok(g)                    => return Ok(g),
            Err(AccessError::Denied) => return Err(AccessError::Denied),
            Err(e) if tokio::time::Instant::now() >= deadline => return Err(e),
            Err(_)                   => tokio::time::sleep(Duration::from_millis(150)).await,
        }
    }
}

/// A pinned curator (A) with a membership gate + a reader (B) sharing one store dir. `membership_for` is
/// handed B's node-id string so a test can allow/deny exactly B. Returns everything for teardown.
async fn cluster(
    dir: &std::path::Path,
    membership_for: impl FnOnce(&str) -> Membership,
) -> (Arc<GossipAgent>, Arc<Wiki<FsStore>>, Arc<GossipAgent>, Arc<Wiki<FsStore>>) {
    let (pa, pb) = (free_port(), free_port());
    let b_id = NodeId::new("127.0.0.1", pb).unwrap().to_string();

    let agent_a = spawn_agent(pa, vec![pb]).await;
    let store_a = Arc::new(FsStore::open(dir, "ops").unwrap());
    let brain = CuratorBrain::new(Box::new(DirectReconciler)).with_membership(membership_for(&b_id));
    let curator = Wiki::with_brain(Arc::clone(&agent_a), wcfg(WikiRole::Curator), store_a, brain).await;

    let agent_b = spawn_agent(pb, vec![pa]).await;
    let store_b = Arc::new(FsStore::open(dir, "ops").unwrap());
    let reader = Wiki::new(Arc::clone(&agent_b), wcfg(WikiRole::Reader), store_b).await;

    assert!(
        poll_until(|| !agent_a.peers().is_empty() && !agent_b.peers().is_empty(), Duration::from_secs(10)).await,
        "mesh forms",
    );
    (agent_a, curator, agent_b, reader)
}

async fn teardown(agent_a: Arc<GossipAgent>, curator: Arc<Wiki<FsStore>>, agent_b: Arc<GossipAgent>, reader: Arc<Wiki<FsStore>>) {
    curator.shutdown().await;
    reader.shutdown().await;
    agent_a.shutdown_with_timeout(Duration::from_secs(5)).await;
    agent_b.shutdown_with_timeout(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn curator_grants_an_allowlisted_reader_the_store_location() {
    let dir = tempfile::tempdir().unwrap();
    let (agent_a, curator, agent_b, reader) =
        cluster(dir.path(), |b_id| Membership::allow([b_id.to_string()])).await;

    let grant = access(&reader, Duration::from_secs(30)).await.expect("allowlisted reader is granted");
    assert_eq!(grant.location, reader.store().location(), "the grant names the reader's own store — read direct from here");
    assert_eq!(grant.group, "ops");

    teardown(agent_a, curator, agent_b, reader).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn curator_denies_a_reader_outside_the_allowlist() {
    let dir = tempfile::tempdir().unwrap();
    // An allowlist that does NOT contain B (empty) → the gate denies it.
    let (agent_a, curator, agent_b, reader) =
        cluster(dir.path(), |_b_id| Membership::allow(Vec::<String>::new())).await;

    match access(&reader, Duration::from_secs(30)).await {
        Err(AccessError::Denied) => {}
        other => panic!("expected Denied, got {other:?}"),
    }

    teardown(agent_a, curator, agent_b, reader).await;
}
