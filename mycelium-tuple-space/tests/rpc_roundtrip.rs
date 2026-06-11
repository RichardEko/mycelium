//! End-to-end RPC: a primary node serving the store and a client node that
//! discovers it via the capability ring and drives the full item lifecycle
//! put → take → complete → ack over real loopback TCP.

use bytes::Bytes;
use mycelium::{GossipAgent, GossipConfig, NodeId};
use mycelium_tuple_space::{TupleConfig, TupleError, TupleRole, TupleSpace};
use std::sync::Arc;
use std::time::Duration;

async fn start_agent(port: u16, bootstrap: Option<u16>) -> Arc<GossipAgent> {
    let id = NodeId::new("127.0.0.1", port).expect("node id");
    // Tight health-check jitter so loopback peer formation is fast (same
    // setting the in-repo examples use for local clusters).
    let cfg = GossipConfig {
        bind_port: port,
        health_check_max_jitter_ms: 50,
        bootstrap_peers: bootstrap
            .map(|b| vec![NodeId::new("127.0.0.1", b).expect("bootstrap id")])
            .unwrap_or_default(),
        ..Default::default()
    };
    let agent = Arc::new(GossipAgent::new(id, cfg));
    agent.start().await.expect("agent start");
    agent
}

/// Structural poll: wait until `client` can resolve the tuple-space primary.
async fn wait_for_primary(ts: &TupleSpace, agent: &GossipAgent) {
    let mut last: Option<TupleError> = None;
    for _ in 0..100 {
        match ts.depth(None).await {
            Ok(_) => return,
            Err(e) => last = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "client never resolved tuple-space primary: last error {:?}, peers {:?}",
        last,
        agent.peers()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rpc_roundtrip_two_nodes() {
    // Per-process ports so consecutive runs never collide on TIME_WAIT sockets.
    let base: u16 = 29400 + (std::process::id() % 500) as u16 * 2;
    let primary_agent = start_agent(base, None).await;
    let client_agent = start_agent(base + 1, Some(base)).await;

    // Form the cluster BEFORE advertising the capability: the first
    // advertisement gossips immediately to connected peers; re-assertion is
    // only every 10 s, which a racing test deadline would sit right on.
    for _ in 0..400 {
        if !primary_agent.peers().is_empty() && !client_agent.peers().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        !client_agent.peers().is_empty(),
        "agents never peered: {:?}",
        client_agent.peers()
    );

    let cfg = TupleConfig {
        namespace: Arc::from("e2e"),
        role: TupleRole::Primary,
        ..Default::default()
    };
    let primary = TupleSpace::new(Arc::clone(&primary_agent), cfg)
        .await
        .expect("primary tuple space");
    assert!(primary.is_primary());

    let cfg = TupleConfig {
        namespace: Arc::from("e2e"),
        role: TupleRole::Client,
        ..Default::default()
    };
    let client = TupleSpace::new(Arc::clone(&client_agent), cfg)
        .await
        .expect("client tuple space");
    assert!(!client.is_primary());

    wait_for_primary(&client, &client_agent).await;

    // put → take round-trips through the primary.
    let id = client
        .put("stage-a", Bytes::from_static(b"hello tuple space"))
        .await
        .expect("put");
    let (got, payload) = client
        .take("stage-a", Duration::from_secs(5))
        .await
        .expect("take");
    assert_eq!(got, id);
    assert_eq!(payload.as_ref(), b"hello tuple space");

    // complete: atomic advance to stage-b.
    let next_id = client
        .complete(id, "stage-b", payload)
        .await
        .expect("complete");
    assert_ne!(next_id, id);

    // depth reflects the advanced item.
    let depths = client.depth(Some("stage-b")).await.expect("depth");
    assert_eq!(depths.len(), 1);
    assert_eq!(depths[0].depth, 1);
    assert_eq!(depths[0].inflight, 0);

    // Drain stage-b and terminally ack.
    let (id_b, _) = client
        .take("stage-b", Duration::from_secs(5))
        .await
        .expect("take stage-b");
    assert_eq!(id_b, next_id);
    client.ack(id_b).await.expect("ack");

    // Double-ack is refused with NotFound.
    assert!(matches!(client.ack(id_b).await, Err(TupleError::NotFound)));

    // Parked remote take: worker parks BEFORE the item exists, producer
    // unblocks it through the hot path.
    let waiter = {
        let c = Arc::clone(&client);
        tokio::spawn(async move { c.take("stage-c", Duration::from_secs(10)).await })
    };
    // Give the take RPC time to arrive and park (the park itself is the
    // condition under test; there is no observable to poll from the client).
    tokio::time::sleep(Duration::from_millis(300)).await;
    let id_c = client
        .put("stage-c", Bytes::from_static(b"direct handoff"))
        .await
        .expect("put stage-c");
    let (got_c, payload_c) = waiter.await.expect("join").expect("parked take");
    assert_eq!(got_c, id_c);
    assert_eq!(payload_c.as_ref(), b"direct handoff");

    // take on an empty stage times out cleanly across RPC.
    let r = client.take("empty", Duration::from_millis(200)).await;
    assert!(matches!(r, Err(TupleError::Timeout)));

    primary.shutdown().await;
    client.shutdown().await;
    client_agent.shutdown().await;
    primary_agent.shutdown().await;
}
