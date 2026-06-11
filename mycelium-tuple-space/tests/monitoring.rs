//! Phase 3: sys/tuple metrics keys, the backpressure pheromone with
//! hysteresis, and the producer-side back-off it drives.

use bytes::Bytes;
use mycelium::{GossipAgent, GossipConfig, NodeId};
use mycelium_tuple_space::{TupleConfig, TupleError, TupleRole, TupleSpace};
use std::sync::Arc;
use std::time::Duration;

async fn start_agent(port: u16) -> Arc<GossipAgent> {
    let id = NodeId::new("127.0.0.1", port).expect("node id");
    let cfg = GossipConfig {
        bind_port: port,
        health_check_max_jitter_ms: 50,
        ..Default::default()
    };
    let agent = Arc::new(GossipAgent::new(id, cfg));
    agent.start().await.expect("agent start");
    agent
}

async fn kv_num(agent: &GossipAgent, key: &str) -> Option<u64> {
    agent
        .kv()
        .get(key)
        .and_then(|v| std::str::from_utf8(&v).ok().and_then(|s| s.parse().ok()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_keys_reflect_activity() {
    let base: u16 = 24400 + (std::process::id() % 400) as u16;
    let agent = start_agent(base).await;
    let cfg = TupleConfig {
        namespace: Arc::from("mon"),
        role: TupleRole::Primary,
        cap_refresh: Duration::from_millis(200), // metrics cadence
        ..Default::default()
    };
    let ts = TupleSpace::new(Arc::clone(&agent), cfg).await.expect("ts");

    // 3 puts, 1 take (left in flight), 1 of the takes acked? No: take one,
    // keep it claimed so inflight=1, depth=2.
    for i in 0..3u32 {
        ts.put("s", Bytes::from(format!("{i}"))).await.expect("put");
    }
    ts.take("s", Duration::from_secs(5)).await.expect("take");

    let node = agent.node_id().to_string();
    let sb = format!("sys/tuple/{node}/mon/stage/s");
    // Poll one full metrics cadence.
    for _ in 0..50 {
        if kv_num(&agent, &format!("{sb}/put_total")).await == Some(3) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(kv_num(&agent, &format!("{sb}/put_total")).await, Some(3));
    assert_eq!(kv_num(&agent, &format!("{sb}/take_total")).await, Some(1));
    assert_eq!(kv_num(&agent, &format!("{sb}/depth")).await, Some(2));
    assert_eq!(kv_num(&agent, &format!("{sb}/inflight")).await, Some(1));
    let role = agent.kv().get(&format!("sys/tuple/{node}/mon/role"));
    assert_eq!(role.as_deref(), Some(&b"primary"[..]));

    ts.shutdown().await;
    agent.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pressure_pheromone_hysteresis() {
    let base: u16 = 23400 + (std::process::id() % 400) as u16;
    let agent = start_agent(base).await;
    let cfg = TupleConfig {
        namespace: Arc::from("bp"),
        role: TupleRole::Primary,
        high_watermark: 10,
        cap_refresh: Duration::from_millis(200),
        ..Default::default()
    };
    let ts = TupleSpace::new(Arc::clone(&agent), cfg).await.expect("ts");

    // Saturate to the watermark; the 11th put must raise.
    for i in 0..10u32 {
        ts.put("s", Bytes::from(format!("{i}"))).await.expect("put");
    }
    let r = ts.put("s", Bytes::from_static(b"over")).await;
    assert!(matches!(r, Err(TupleError::Backpressure { .. })));

    // Pheromone appears within one metrics cadence.
    let node = agent.node_id().to_string();
    let pkey = format!("sys/tuple/{node}/bp/pressure/s");
    for _ in 0..50 {
        if agent.kv().get(&pkey).is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(agent.kv().get(&pkey).is_some(), "pressure pheromone never appeared");

    // Drain to 8 (above 70% of 10): pheromone must REMAIN (hysteresis band).
    for _ in 0..2 {
        let (id, _) = ts.take("s", Duration::from_secs(5)).await.expect("take");
        ts.ack(id).await.expect("ack");
    }
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(
        agent.kv().get(&pkey).is_some(),
        "pheromone evaporated inside the hysteresis band"
    );

    // Drain below 7: pheromone must clear.
    for _ in 0..3 {
        let (id, _) = ts.take("s", Duration::from_secs(5)).await.expect("take");
        ts.ack(id).await.expect("ack");
    }
    for _ in 0..50 {
        if agent.kv().get(&pkey).is_none() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        agent.kv().get(&pkey).is_none(),
        "pheromone survived below the hysteresis floor"
    );

    ts.shutdown().await;
    agent.shutdown().await;
}
