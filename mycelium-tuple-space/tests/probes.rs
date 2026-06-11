//! M2 Run-17 falsification probe (Operational Readiness): the full lifecycle
//! drill. Claimed invariants under test: `TupleSpace::shutdown()` flushes the
//! WAL before returning; a process restart over the same WAL recovers every
//! unacknowledged item (including in-flight, re-queued as abandoned) and no
//! acknowledged item; the gossip agent shuts down cleanly afterwards.

use bytes::Bytes;
use mycelium::{GossipAgent, GossipConfig, NodeId};
use mycelium_tuple_space::{TupleConfig, TupleRole, TupleSpace};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_flushes_wal_and_restart_recovers() {
    let port: u16 = 21400 + (std::process::id() % 400) as u16;
    let wal = std::env::temp_dir().join(format!("mts-probe-{}.wal", std::process::id()));
    let _ = std::fs::remove_file(&wal);

    let ts_cfg = |role| TupleConfig {
        namespace: Arc::from("probe"),
        role,
        persist: true,
        wal_path: wal.clone(),
        // Low checkpoint threshold exercises the periodic sync path too.
        checkpoint_every: 4,
        cap_refresh: Duration::from_millis(300),
        ..Default::default()
    };

    // ── Generation 1: traffic, then shutdown with items in every state ──────
    let mut expected_live: HashSet<u64> = HashSet::new();
    {
        let agent = Arc::new(GossipAgent::new(
            NodeId::new("127.0.0.1", port).expect("node id"),
            GossipConfig {
                bind_port: port,
                health_check_max_jitter_ms: 50,
                ..Default::default()
            },
        ));
        agent.start().await.expect("agent start");
        let ts = TupleSpace::new(Arc::clone(&agent), ts_cfg(TupleRole::Primary))
            .await
            .expect("tuple space");

        // 6 queued items.
        for i in 0..6u32 {
            let id = ts
                .put("work", Bytes::from(format!("item-{i}")))
                .await
                .expect("put");
            expected_live.insert(id);
        }
        // 2 taken-and-acked: must NOT survive.
        for _ in 0..2 {
            let (id, _) = ts.take("work", Duration::from_secs(5)).await.expect("take");
            ts.ack(id).await.expect("ack");
            expected_live.remove(&id);
        }
        // 1 taken, never acked: in-flight at shutdown — MUST survive (re-queued).
        let _ = ts.take("work", Duration::from_secs(5)).await.expect("take inflight");
        // 1 completed onto a second stage: old id replaced by new.
        let (old, payload) = ts.take("work", Duration::from_secs(5)).await.expect("take");
        let new_id = ts.complete(old, "done", payload).await.expect("complete");
        expected_live.remove(&old);
        expected_live.insert(new_id);

        ts.shutdown().await; // claimed: final WAL fsync happens here
        agent.shutdown().await;
    }

    // ── Generation 2: restart over the same WAL ─────────────────────────────
    {
        let agent = Arc::new(GossipAgent::new(
            NodeId::new("127.0.0.1", port + 1).expect("node id"),
            GossipConfig {
                bind_port: port + 1,
                health_check_max_jitter_ms: 50,
                ..Default::default()
            },
        ));
        agent.start().await.expect("agent restart");
        let ts = TupleSpace::new(Arc::clone(&agent), ts_cfg(TupleRole::Primary))
            .await
            .expect("tuple space restart");

        // Drain everything; every live id must reappear exactly once.
        let mut recovered = HashSet::new();
        while let Ok((id, _)) = ts.take("work", Duration::from_millis(300)).await {
            assert!(recovered.insert(id), "id {id} delivered twice after restart");
            ts.ack(id).await.expect("ack recovered");
        }
        match ts.take("done", Duration::from_millis(300)).await {
            Ok((id, _)) => {
                assert!(recovered.insert(id), "id {id} delivered twice after restart");
                ts.ack(id).await.expect("ack done-stage");
            }
            Err(e) => panic!("completed item lost across restart: {e}"),
        }
        assert_eq!(
            recovered, expected_live,
            "restart must recover exactly the unacked items (acked must not resurrect)"
        );

        ts.shutdown().await;
        agent.shutdown().await;
    }
    let _ = std::fs::remove_file(&wal);
}
