//! WS-G / G3 · Phase 5 — the community-microgrid worked example.
//!
//! A neighbourhood energy co-op shares ONE fact pool, no dispatcher. The example shows the
//! blackboard's defining split end to end:
//!
//! - **Reading is unconditional + concurrent (`rd`)**: a forecaster and a tariff agent both observe
//!   every surplus fact — non-destructively, as many readers as you like.
//! - **Consuming a finite fact is competitive + exactly-once (`in`)**: two storage executors race to
//!   claim each surplus; exactly one charges against it, and every surplus is consumed exactly once.
//!
//! Run: `cargo run -p mycelium-blackboard --example microgrid`. Exits 0 on success.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mycelium::{GossipAgent, GossipConfig, NodeId};
use mycelium_blackboard::{Blackboard, BoardConfig, BoardRole, Predicate};
use tokio::sync::Mutex;

const SURPLUS_FACTS: u64 = 12;

#[tokio::main]
async fn main() {
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let agent = Arc::new(GossipAgent::new(
        NodeId::new("127.0.0.1", port).unwrap(),
        GossipConfig { bind_port: port, ..Default::default() },
    ));
    agent.start().await.unwrap();
    let bb = Blackboard::new(
        Arc::clone(&agent),
        BoardConfig { namespace: Arc::from("microgrid"), role: BoardRole::Primary, ..Default::default() },
    )
    .await
    .unwrap();

    // ── The inverter posts finite surplus facts (gossiped to all readers). ──
    for i in 0..SURPLUS_FACTS {
        let attrs = BTreeMap::from([
            ("kind".to_string(), "surplus".to_string()),
            ("feeder".to_string(), "4".to_string()),
            ("kwh".to_string(), format!("{:.1}", 1.0 + i as f64 * 0.3)),
        ]);
        bb.post(attrs, Bytes::from(format!("surplus-{i}"))).await.unwrap();
    }
    println!("inverter: posted {SURPLUS_FACTS} surplus facts on feeder 4");

    // ── Non-destructive readers (rd): forecaster + tariff agent both see the pool. ──
    let surplus = Predicate::new().eq("kind", "surplus");
    let forecaster = bb.read(&surplus).await.unwrap().len();
    let tariff = bb.read(&surplus).await.unwrap().len();
    assert_eq!(forecaster, SURPLUS_FACTS as usize);
    assert_eq!(tariff, SURPLUS_FACTS as usize);
    println!("forecaster: sees {forecaster} surplus facts (shared, non-destructive)");
    println!("tariff agent: sees {tariff} surplus facts (same facts, concurrently)");

    // ── Two storage executors compete (in): each surplus claimed by exactly one. ──
    let claimed_ids: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();
    for name in ["community-battery", "ev-charger"] {
        let (bb2, ids, pred) = (Arc::clone(&bb), Arc::clone(&claimed_ids), surplus.clone());
        handles.push(tokio::spawn(async move {
            let mut mine = 0u64;
            let mut empties = 0;
            loop {
                match bb2.claim(&pred).await.unwrap() {
                    Some(fact) => {
                        // ... charge against the finite surplus ...
                        ids.lock().await.push(fact.id);
                        bb2.ack(fact.id).await.unwrap(); // consumed exactly once
                        mine += 1;
                        empties = 0;
                    }
                    None => {
                        empties += 1;
                        if empties >= 3 {
                            break; // pool drained
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
            }
            println!("executor {name}: charged from {mine} surplus facts");
            mine
        }));
    }

    let mut total = 0u64;
    for h in handles {
        total += h.await.unwrap();
    }

    // ── Verify the exactly-once invariant. ──
    let mut ids = Arc::try_unwrap(claimed_ids).unwrap().into_inner();
    ids.sort_unstable();
    let unique = {
        let mut u = ids.clone();
        u.dedup();
        u.len()
    };
    assert_eq!(total, SURPLUS_FACTS, "every surplus fact is consumed");
    assert_eq!(unique, ids.len(), "no surplus fact is claimed twice");
    assert_eq!(ids.len(), SURPLUS_FACTS as usize, "exactly the posted facts, once each");

    println!(
        "microgrid: all {total} surplus facts consumed exactly once (rd shared, in competitive)"
    );

    bb.shutdown().await;
    agent.shutdown().await;
}
