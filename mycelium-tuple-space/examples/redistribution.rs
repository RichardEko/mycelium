//! Worked example — a food-redistribution sorting pipeline on a tuple space.
//!
//! A community redistribution hub moves donated produce through three stages —
//! `intake` → `sorted` → `routed` — with NO dispatcher assigning work. The
//! example shows the tuple space's defining property end to end:
//!
//! - **Single-copy competitive `take`**: several workers park on the same stage;
//!   each queued item is handed to exactly ONE of them. This is the Linda `in`
//!   primitive — the pipeline's counterpart to the blackboard's `rd`/`in` split.
//! - **Atomic stage advance (`complete`)**: acking an item and posting its
//!   successor to the next stage is ONE WAL record — there is no crash window
//!   between stages, so every donation advances at-most-once per stage and the
//!   pipeline delivers each one exactly once.
//!
//! Workers pull; they are never pushed to. Add or remove a sorter and the
//! throughput changes with no configuration — the queue depth is the only
//! signal. Contrast `mycelium-blackboard`'s `microgrid` example (one shared fact
//! pool, non-destructive reads) with this (a staged buffer, destructive handoff).
//!
//! Run: `cargo run -p mycelium-tuple-space --example redistribution`. Exits 0 on success.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mycelium::{GossipAgent, GossipConfig, NodeId};
use mycelium_tuple_space::{TupleConfig, TupleRole, TupleSpace};
use tokio::sync::Mutex;

const DONATIONS: u64 = 12;

#[tokio::main]
async fn main() {
    let port = mycelium::test_util::alloc_port();
    let agent = Arc::new(GossipAgent::new(
        NodeId::new("127.0.0.1", port).unwrap(),
        GossipConfig { bind_port: port, cluster_name: Some(std::env::var("GOSSIP_CLUSTER_NAME").unwrap_or_else(|_| "redistribution".to_string())), ..Default::default() },
    ));
    agent.start().await.unwrap();
    let ts = TupleSpace::new(
        Arc::clone(&agent),
        TupleConfig {
            namespace: Arc::from("redistribution"),
            role: TupleRole::Primary,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // ── The loading dock posts finite donations onto the first stage. ──
    for i in 0..DONATIONS {
        ts.put("intake", Bytes::from(format!("donation-{i}"))).await.unwrap();
    }
    println!("dock: posted {DONATIONS} donations on `intake`");

    // Each worker drains its stage until it sees `EMPTIES` consecutive timeouts
    // (the stage is empty and no upstream worker is still feeding it).
    const EMPTIES: u32 = 3;
    let poll = Duration::from_millis(150);

    // ── Two sorters compete on `intake`, advancing each item to `sorted`. ──
    // ── Two routers compete on `sorted`, advancing each item to `routed`. ──
    let mut handles = Vec::new();
    for (worker, from, to) in [
        ("sorter-A", "intake", Some("sorted")),
        ("sorter-B", "intake", Some("sorted")),
        ("router-A", "sorted", Some("routed")),
        ("router-B", "sorted", Some("routed")),
    ] {
        let ts2 = Arc::clone(&ts);
        handles.push(tokio::spawn(async move {
            let mut mine = 0u64;
            let mut empties = 0;
            while empties < EMPTIES {
                match ts2.take(from, poll).await {
                    Ok((id, payload)) => {
                        // ... inspect / weigh / label the produce ...
                        // Atomic advance: ack `id` on `from` AND enqueue on `to`.
                        ts2.complete(id, to.unwrap(), payload).await.unwrap();
                        mine += 1;
                        empties = 0;
                    }
                    Err(_) => empties += 1, // Timeout: nothing queued right now
                }
            }
            println!("{worker}: advanced {mine} items {from} → {}", to.unwrap());
            mine
        }));
    }

    // ── The dispatch collector consumes the terminal `routed` stage. ──
    let delivered: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let (ts2, out) = (Arc::clone(&ts), Arc::clone(&delivered));
        handles.push(tokio::spawn(async move {
            let mut n = 0u64;
            let mut empties = 0;
            while empties < EMPTIES {
                match ts2.take("routed", poll).await {
                    Ok((id, payload)) => {
                        out.lock().await.push(String::from_utf8_lossy(&payload).into_owned());
                        ts2.ack(id).await.unwrap(); // terminal: consumed exactly once
                        n += 1;
                        empties = 0;
                    }
                    Err(_) => empties += 1,
                }
            }
            println!("dispatch: delivered {n} items from `routed`");
            n
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // ── Verify the exactly-once invariant across the whole pipeline. ──
    let delivered = Arc::try_unwrap(delivered).unwrap().into_inner();
    let unique: BTreeSet<&String> = delivered.iter().collect();
    assert_eq!(delivered.len(), DONATIONS as usize, "every donation is delivered");
    assert_eq!(unique.len(), delivered.len(), "no donation is delivered twice");
    for i in 0..DONATIONS {
        assert!(unique.contains(&format!("donation-{i}")), "donation-{i} reached dispatch");
    }

    println!(
        "redistribution: all {DONATIONS} donations moved intake → sorted → routed, exactly once \
         (single-copy take, atomic complete)"
    );

    ts.shutdown().await;
    agent.shutdown().await;
}
