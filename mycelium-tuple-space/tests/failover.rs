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
            Err(e) => {
                attempts += 1;
                let provs = a_client
                    .capabilities()
                    .resolve(&mycelium::CapFilter::new("tuple", "fo.primary"));
                eprintln!(
                    "[failover diag] attempt {attempts}: take err: {e} | primary-cap providers: {:?} | client peers: {:?}",
                    provs.iter().map(|(n, _)| n.to_string()).collect::<Vec<_>>(),
                    a_client.peers().iter().map(|p| p.to_string()).collect::<Vec<_>>(),
                );
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

/// A secondary that starts before the primary's advertisement has propagated must NOT
/// promote — "evaporated" means *was there, then gone*, and never-seen is startup lag.
/// The pre-fix watch promoted after 2 empty resolves, which on a CPU-starved host created
/// a split-brain primary that never demoted (hosted-CI S13, #150). Once the primary
/// appears, the secondary must stay secondary and serve client ops through it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn secondary_startup_lag_is_not_evaporation() {
    let (pa, pb) = alloc_two_sorted_ports();
    let a1 = start_agent(pa, None).await;
    let a2 = start_agent(pb, Some(pa)).await;
    wait_peered(&[&a1, &a2]).await;

    // Secondary first, with no primary anywhere on the ring.
    let ts2 = TupleSpace::new(Arc::clone(&a2), ts_cfg("lag", TupleRole::Secondary))
        .await
        .expect("ts2");

    // The pre-fix watch promoted after ~2 × cap_refresh; sit past that, inside the
    // orphan grace (10 ticks), and assert no spurious promotion.
    tokio::time::sleep(Duration::from_millis(300 * 4)).await;
    assert!(
        !ts2.is_primary(),
        "secondary promoted on startup propagation lag (never saw a primary)"
    );

    // Primary appears late. The secondary must see it and stay secondary.
    let ts1 = TupleSpace::new(Arc::clone(&a1), ts_cfg("lag", TupleRole::Primary))
        .await
        .expect("ts1");
    tokio::time::sleep(Duration::from_millis(300 * 4)).await;
    assert!(!ts2.is_primary(), "secondary promoted despite a live primary");
    assert!(ts2.is_secondary(), "secondary lost its role");

    // And it functions: client ops through the secondary route to the primary.
    let id = ts2.put("s", Bytes::from_static(b"x")).await.expect("put");
    let (got, _) = ts2.take("s", Duration::from_secs(5)).await.expect("take");
    assert_eq!(got, id);

    ts1.shutdown().await;
    ts2.shutdown().await;
    a2.shutdown().await;
    a1.shutdown().await;
}

/// Bounded availability for the genuine orphan: a secondary whose primary NEVER appears
/// (dead before this process started — nothing to sight) must still promote once the
/// orphan grace (10 × cap_refresh) expires, not wait forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn never_seen_primary_promotes_after_orphan_grace() {
    let (pa, pb) = alloc_two_sorted_ports();
    let a1 = start_agent(pa, None).await;
    let a2 = start_agent(pb, Some(pa)).await;
    wait_peered(&[&a1, &a2]).await;

    let ts2 = TupleSpace::new(Arc::clone(&a2), ts_cfg("orph", TupleRole::Secondary))
        .await
        .expect("ts2");

    // Grace is 10 ticks of cap_refresh (300 ms) ≈ 3 s; poll well past it.
    let mut promoted = false;
    for _ in 0..40 {
        if ts2.is_primary() {
            promoted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    assert!(promoted, "orphaned secondary never promoted (availability hole)");

    // The promoted space serves.
    let id = ts2.put("s", Bytes::from_static(b"x")).await.expect("put");
    let (got, _) = ts2.take("s", Duration::from_secs(5)).await.expect("take");
    assert_eq!(got, id);

    ts2.shutdown().await;
    a2.shutdown().await;
    a1.shutdown().await;
}

/// The succession chain the pinning design claims but nothing executed until now (asked
/// 2026-07-10): A (primary) + B (secondary); A dies; B promotes; a NEW node C joins as
/// secondary of the *promoted* B — with no restart of B — and the pair must fully function:
/// C stays secondary (it sights B), client ops through C route to B, replication reaches C,
/// and when B later dies C promotes (second-generation failover) and serves surviving items
/// under their original ids. Pins are ring-driven, so none of this needs configuration.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn succession_chain_late_secondary_joins_promoted_primary() {
    let (pa, pb) = alloc_two_sorted_ports();
    let pc = loop {
        let p = alloc_port();
        if p != pa && p != pb {
            break p;
        }
    };
    let a1 = start_agent(pa, None).await;
    let a2 = start_agent(pb, Some(pa)).await;
    wait_peered(&[&a1, &a2]).await;

    let ts_a = TupleSpace::new(Arc::clone(&a1), ts_cfg("succ", TupleRole::Primary))
        .await
        .expect("ts_a");
    let ts_b = TupleSpace::new(Arc::clone(&a2), ts_cfg("succ", TupleRole::Secondary))
        .await
        .expect("ts_b");

    // Wait until B can see the primary (cap discovery, same pattern as the other tests).
    for _ in 0..100 {
        if ts_b.depth(None).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Two items in generation 1 (A primary), put THROUGH B (client-op routing).
    let mut ids = HashSet::new();
    for i in 0..2u32 {
        let id = ts_b
            .put("work", Bytes::from(format!("gen1-{i}")))
            .await
            .expect("gen1 put");
        ids.insert(id);
    }
    // Wait for replication to B's mirror so gen-1 items survive A's death.
    for _ in 0..100 {
        let depth: u32 = ts_b
            .local_depth(Some("work"))
            .map(|d| d.iter().map(|s| s.depth).sum())
            .unwrap_or(0);
        if depth == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Generation 2: A dies, B promotes (fast path — B sighted A).
    ts_a.shutdown().await;
    a1.shutdown().await;
    for _ in 0..200 {
        if ts_b.is_primary() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(ts_b.is_primary(), "B never promoted after A died");

    // C joins fresh, bootstrapped to B (A is gone), as secondary — NO restart of B.
    let a3 = start_agent(pc, Some(pb)).await;
    wait_peered(&[&a2, &a3]).await;
    let ts_c = TupleSpace::new(Arc::clone(&a3), ts_cfg("succ", TupleRole::Secondary))
        .await
        .expect("ts_c");

    // C must sight the promoted B and stay secondary (well past the old 2-tick trigger).
    tokio::time::sleep(Duration::from_millis(300 * 4)).await;
    assert!(!ts_c.is_primary(), "C spuriously promoted next to a live promoted B");
    assert!(ts_c.is_secondary(), "C lost its role");

    // Wait until C can see the promoted primary before using it.
    for _ in 0..100 {
        if ts_c.depth(None).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Client ops through C route to the promoted B: two gen-2 items.
    for i in 0..2u32 {
        let id = ts_c
            .put("work", Bytes::from(format!("gen2-{i}")))
            .await
            .expect("gen2 put through C");
        ids.insert(id);
    }
    assert_eq!(ids.len(), 4, "ids must be unique across generations");

    // Replication must reach the late-joined C before B dies.
    for _ in 0..100 {
        let depth: u32 = ts_c
            .local_depth(Some("work"))
            .map(|d| d.iter().map(|s| s.depth).sum())
            .unwrap_or(0);
        if depth == 4 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Generation 3: B dies, C promotes and serves ALL items under original ids.
    ts_b.shutdown().await;
    a2.shutdown().await;
    for _ in 0..200 {
        if ts_c.is_primary() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(ts_c.is_primary(), "C never promoted after B died (second-generation failover)");

    let mut survived = HashSet::new();
    let mut attempts = 0;
    while survived.len() < 4 {
        match ts_c.take("work", Duration::from_secs(2)).await {
            Ok((id, _)) => {
                survived.insert(id);
            }
            Err(_) => {
                attempts += 1;
                assert!(attempts < 30, "promoted C never served all items: got {survived:?} want {ids:?}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    assert_eq!(survived, ids, "items must survive two failovers under original ids");

    ts_c.shutdown().await;
    a3.shutdown().await;
}

/// Run-41 API-finding gate: pipeline ops must wait (bounded) for capability discovery under
/// the DEFAULT config — `BackpressureMode::Raise` means "don't block on a saturated primary",
/// not "race capability gossip". Pre-fix, `put` on a fresh client failed `NoProvider`
/// instantly while `take`/`complete` waited (#154 fixed only the read side; the succession
/// test needed a hand-rolled readiness poll to work around it).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_ops_wait_for_discovery_under_default_config() {
    let (pa, pb) = alloc_two_sorted_ports();
    let a1 = start_agent(pa, None).await;
    let a2 = start_agent(pb, Some(pa)).await;
    wait_peered(&[&a1, &a2]).await;

    let ts1 = TupleSpace::new(Arc::clone(&a1), ts_cfg("disc", TupleRole::Primary))
        .await
        .expect("primary");
    // Fresh client puts IMMEDIATELY — no readiness poll. Default mode (Raise): must still
    // succeed, because discovery-wait is not backpressure.
    let ts2 = TupleSpace::new(Arc::clone(&a2), ts_cfg("disc", TupleRole::Client))
        .await
        .expect("client");
    let id = ts2
        .put("work", Bytes::from_static(b"first"))
        .await
        .expect("immediate put after client creation must ride out discovery");
    let (got, _) = ts2.take("work", Duration::from_secs(5)).await.expect("take");
    assert_eq!(got, id);
    ts2.ack(id).await.expect("ack");

    ts1.shutdown().await;
    ts2.shutdown().await;
    a2.shutdown().await;
    a1.shutdown().await;
}

/// Run-42 falsification probe (resource management): `shutdown` with a take waiter PARKED on
/// an empty stage must complete promptly — a parked oneshot must not wedge the drain. The
/// parked take itself resolves by its own timeout contract (documented at-least-once
/// boundary; a shutdown is not a delivery).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_with_parked_take_waiter_is_prompt() {
    let base = alloc_port();
    let agent = start_agent(base, None).await;
    let ts = TupleSpace::new(Arc::clone(&agent), ts_cfg("park", TupleRole::Primary))
        .await
        .expect("primary");

    // Park a waiter on an empty stage (2 s take timeout bounds the test).
    let ts2 = Arc::clone(&ts);
    let waiter = tokio::spawn(async move { ts2.take("empty", Duration::from_secs(2)).await });
    tokio::time::sleep(Duration::from_millis(200)).await; // let it park

    // Shutdown must complete well under the waiter's timeout.
    let t0 = std::time::Instant::now();
    ts.shutdown().await;
    agent.shutdown_with_timeout(Duration::from_secs(5)).await;
    assert!(
        t0.elapsed() < Duration::from_secs(4),
        "shutdown wedged on a parked take waiter: {:?}",
        t0.elapsed()
    );

    // The parked take resolves (Err) without panicking.
    let r = waiter.await.expect("parked take panicked");
    assert!(r.is_err(), "parked take on an empty stage cannot succeed");
}
