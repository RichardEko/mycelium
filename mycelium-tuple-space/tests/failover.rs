//! Phase 2 resilience: live replication to a secondary, capability-TTL
//! failure detection, promotion with id preservation, and the emergent Auto
//! election (lowest candidate wins, loser becomes secondary).

use bytes::Bytes;
use mycelium::{GossipAgent, GossipConfig, NodeId};
use mycelium_tuple_space::{TupleConfig, TupleRole, TupleSpace};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

/// A free ephemeral port (kernel-assigned), avoiding the PID-derived fixed-port collisions that
/// flaked under parallel CI / re-runs (lingering TIME_WAIT sockets, two test processes landing on
/// the same `pid % 400` base) — retired here by delegating to the core's bind-verified,
/// process-unique allocator (`mycelium::test_util::alloc_port`, the `test-util` feature).
fn alloc_port() -> u16 {
    mycelium::test_util::alloc_port()
}

/// Two distinct free ports, lower first — so "lowest candidate id wins" stays deterministic
/// without fixed ports.
fn alloc_two_sorted_ports() -> (u16, u16) {
    loop {
        let (a, b) = (alloc_port(), alloc_port());
        if a != b {
            return (a.min(b), a.max(b));
        }
    }
}

async fn start_agent(port: u16, bootstrap: Option<u16>) -> Arc<GossipAgent> {
    let id = NodeId::new("127.0.0.1", port).expect("node id");
    let cfg = GossipConfig {
        bind_port: port,
        health_check_max_jitter_ms: 50,
        bootstrap_peers: bootstrap
            .map(|b| vec![NodeId::new("127.0.0.1", b).expect("bootstrap id")])
            .unwrap_or_default(),
        // Failover hinges on capability-evaporation + ring liveness timing on a small
        // loopback cluster. SWIM (now default-on) swaps in UDP-probe liveness with different
        // eviction timing, making promotion/election non-deterministic in these tests. Pin
        // the legacy path; SWIM-on failover at scale is exercised by the G3 resilience test.
        swim_failure_detector: false,
        ..Default::default()
    };
    let agent = Arc::new(GossipAgent::new(id, cfg));
    agent.start().await.expect("agent start");
    agent
}

async fn wait_peered(agents: &[&Arc<GossipAgent>]) {
    for _ in 0..400 {
        if agents.iter().all(|a| !a.peers().is_empty()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("agents never peered");
}

fn ts_cfg(ns: &str, role: TupleRole) -> TupleConfig {
    TupleConfig {
        namespace: Arc::from(ns),
        role,
        // Fast evaporation so the failure detector fires within test budget:
        // promotion latency ≈ 3 × cap_refresh.
        cap_refresh: Duration::from_millis(300),
        heartbeat_interval: Duration::from_millis(200),
        ..Default::default()
    }
}

/// Primary + secondary + client: items put through the primary survive its
/// death — the secondary's mirror serves them under their ORIGINAL ids, and
/// acked items do not resurrect.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failover_preserves_items_and_ids() {
    let base: u16 = 27400 + (std::process::id() % 400) as u16 * 3;
    let a_primary = start_agent(base, None).await;
    let a_secondary = start_agent(base + 1, Some(base)).await;
    let a_client = start_agent(base + 2, Some(base)).await;
    wait_peered(&[&a_primary, &a_secondary, &a_client]).await;

    let primary = TupleSpace::new(Arc::clone(&a_primary), ts_cfg("fo", TupleRole::Primary))
        .await
        .expect("primary");
    let secondary =
        TupleSpace::new(Arc::clone(&a_secondary), ts_cfg("fo", TupleRole::Secondary))
            .await
            .expect("secondary");
    let client = TupleSpace::new(Arc::clone(&a_client), ts_cfg("fo", TupleRole::Client))
        .await
        .expect("client");

    // Wait until the client can see the primary.
    for _ in 0..100 {
        if client.depth(None).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // 5 items in; ack 2 of them (take + terminal ack).
    let mut ids = HashSet::new();
    for i in 0..5u32 {
        let id = client
            .put("work", Bytes::from(format!("item-{i}")))
            .await
            .expect("put");
        ids.insert(id);
    }
    for _ in 0..2 {
        let (id, _) = client
            .take("work", Duration::from_secs(5))
            .await
            .expect("take");
        client.ack(id).await.expect("ack");
        ids.remove(&id);
    }
    assert_eq!(ids.len(), 3);

    // Replication is fire-and-forget: poll the secondary's local mirror
    // until it converges on exactly the 3 live items.
    for _ in 0..100 {
        let depth: u32 = secondary
            .local_depth(Some("work"))
            .map(|d| d.iter().map(|s| s.depth).sum())
            .unwrap_or(0);
        if depth == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Kill the primary (graceful here; the ad is tombstoned — the same
    // detector path as TTL evaporation, just faster).
    primary.shutdown().await;
    a_primary.shutdown().await;

    // Secondary must promote and serve the 3 surviving items under their
    // original ids.
    for _ in 0..200 {
        if secondary.is_primary() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(secondary.is_primary(), "secondary never promoted");

    // The client's view converges behind the promotion: it may briefly
    // resolve nothing (old ad tombstoned, new ad not yet gossiped) or the
    // dead node. Retry until the new primary serves.
    let mut survived = HashSet::new();
    let mut attempts = 0;
    while survived.len() < 3 {
        match client.take("work", Duration::from_secs(2)).await {
            Ok((id, _)) => {
                survived.insert(id);
            }
            Err(_) => {
                attempts += 1;
                assert!(attempts < 30, "new primary never served the mirrored items");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    assert_eq!(survived, ids, "items must survive under their original ids");

    // Acked items must NOT resurrect.
    let r = client.take("work", Duration::from_millis(300)).await;
    assert!(r.is_err(), "acked items resurrected after failover");

    // New ids issued by the promoted secondary must not collide with old ones.
    let fresh = client
        .put("work", Bytes::from_static(b"post-failover"))
        .await
        .expect("put after failover");
    assert!(
        !survived.contains(&fresh),
        "promoted secondary re-issued an existing id"
    );

    secondary.shutdown().await;
    client.shutdown().await;
    a_client.shutdown().await;
    a_secondary.shutdown().await;
}

/// Two Auto nodes: exactly one becomes primary (the lowest node id), the
/// other becomes secondary — no coordinator, both conclude from the ring.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_election_is_deterministic() {
    let (p_lo, p_hi) = alloc_two_sorted_ports();
    let a1 = start_agent(p_lo, None).await; // lower id → expected primary
    let a2 = start_agent(p_hi, Some(p_lo)).await;
    wait_peered(&[&a1, &a2]).await;

    let ts1 = TupleSpace::new(Arc::clone(&a1), ts_cfg("elect", TupleRole::Auto))
        .await
        .expect("ts1");
    let ts2 = TupleSpace::new(Arc::clone(&a2), ts_cfg("elect", TupleRole::Auto))
        .await
        .expect("ts2");

    for _ in 0..200 {
        let settled = (ts1.is_primary() && ts2.is_secondary())
            || (ts2.is_primary() && ts1.is_secondary());
        if settled {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Lowest node id (base port) must have won.
    assert!(ts1.is_primary(), "lowest candidate id did not win the election");
    assert!(ts2.is_secondary(), "loser did not become secondary");
    assert!(!ts2.is_primary(), "split brain: both candidates promoted");

    // The elected space actually works.
    let id = ts2.put("s", Bytes::from_static(b"x")).await.expect("put");
    let (got, _) = ts2.take("s", Duration::from_secs(5)).await.expect("take");
    assert_eq!(got, id);

    ts1.shutdown().await;
    ts2.shutdown().await;
    a2.shutdown().await;
    a1.shutdown().await;
}

/// The advisory inflight key appears in the gossip KV while an item is
/// claimed and disappears on ack.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inflight_keys_track_claims() {
    let base: u16 = 25400 + (std::process::id() % 400) as u16;
    let agent = start_agent(base, None).await;

    let primary = TupleSpace::new(Arc::clone(&agent), ts_cfg("ifk", TupleRole::Primary))
        .await
        .expect("primary");

    let id = primary
        .put("s", Bytes::from_static(b"x"))
        .await
        .expect("put");
    let (got, _) = primary.take("s", Duration::from_secs(5)).await.expect("take");
    assert_eq!(got, id);

    let key = format!("tuple/inflight/ifk/{id}");
    assert!(
        agent.kv().get(&key).is_some(),
        "inflight key missing while item claimed"
    );

    primary.ack(id).await.expect("ack");
    assert!(
        agent.kv().get(&key).is_none(),
        "inflight key survived the ack"
    );

    primary.shutdown().await;
    agent.shutdown().await;
}
