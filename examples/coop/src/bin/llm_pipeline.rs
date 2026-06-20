//! Example 08 — **LLM agents coordinating via a tuple space** (pull pipeline + per-worker model).
//!
//! A two-stage donation pipeline where the workers are **LLM agents**. The stages are tuple-space
//! lanes; multiple LLM workers **pull** from a lane when ready, run **their own model** on the item,
//! and **complete** it to the next lane. No dispatcher predicts who does what — coordination is
//! entirely the lanes (generative decoupling + blocking pull), and the LLM call lives *between*
//! `take` and `complete`.
//!
//!   lanes:  classify ──(LLM: tag perishability)──▶ route ──(LLM: pick a kitchen)──▶ done
//!
//!   • `buffer`           — tuple-space Primary (the rendezvous point).
//!   • `seeder`           — puts raw donations into the `classify` lane.
//!   • `agent-a`/`agent-b`— two LLM workers; each loops: take from the deepest pending lane → run
//!                          its model → complete to the next lane. They compete per-lane (pull).
//!
//! The model is an `EchoBackend` stand-in (so CI needs no API key); each worker invokes it directly
//! (`LlmBackend::complete`) — the worker *is* the LLM agent, not a caller of a central skill.
//!
//! Run:  cargo run -p mycelium-coop-examples --bin llm_pipeline

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use coop::common::{alloc_ports, spawn_depot, Donation, DepotOpts};
use mycelium::{EchoBackend, LlmBackend};
use mycelium_tuple_space::{TupleConfig, TupleError, TupleRole, TupleSpace};

const CLASSIFY: &str = "classify";
const ROUTE: &str = "route";
const DONE: &str = "done";
const N: u64 = 6;

async fn wait_until(secs: u64, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    while std::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    cond()
}

/// One LLM worker: loop pulling from `classify` then `route`, running its model between take and
/// complete, until `N` items have reached `done`. Returns how many items this worker advanced.
async fn run_worker(
    name: String,
    ts: Arc<TupleSpace>,
    backend: Arc<dyn LlmBackend>,
    done_count: Arc<AtomicU64>,
) -> u64 {
    let mut advanced = 0u64;
    while done_count.load(Ordering::Acquire) < N {
        // Prefer the later stage so items flow through to `done` rather than piling in `route`.
        if let Ok((id, payload)) = ts.take(ROUTE, Duration::from_millis(300)).await {
            let item = String::from_utf8_lossy(&payload).to_string();
            let r = backend.complete("You assign donations to a community kitchen.",
                &format!("ROUTE: {item}"), 64, 0.0).await;
            let out = r.map(|x| x.output).unwrap_or_else(|_| item);
            if ts.complete(id, DONE, out.into_bytes().into()).await.is_ok() {
                done_count.fetch_add(1, Ordering::AcqRel);
                advanced += 1;
                println!("[{name}] routed an item → done");
            }
            continue;
        }
        match ts.take(CLASSIFY, Duration::from_millis(300)).await {
            Ok((id, payload)) => {
                let item = String::from_utf8_lossy(&payload).to_string();
                let r = backend.complete("You tag a donation's perishability.",
                    &format!("CLASSIFY: {item}"), 64, 0.0).await;
                let out = r.map(|x| x.output).unwrap_or_else(|_| item);
                if ts.complete(id, ROUTE, out.into_bytes().into()).await.is_ok() {
                    advanced += 1;
                    println!("[{name}] classified an item → route");
                }
            }
            Err(TupleError::Timeout) => {
                // Both lanes momentarily empty; small yield, then re-check the done count.
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(_) => break,
        }
    }
    advanced
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ERROR-level: the multi-node tuple space emits benign sys/tuple/ tripwire WARNs (gossip
    // reflection of a node's own metrics key) that would otherwise drown the demo output.
    tracing_subscriber::fmt().with_max_level(tracing::Level::ERROR).init();

    let cert_dir = std::env::temp_dir().join(format!("coop-llmpipe-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&cert_dir);
    let p = alloc_ports(8); // buffer, seeder, agent-a, agent-b

    let ns = Arc::from("pipeline");
    let buffer = spawn_depot(DepotOpts {
        name: "buffer".into(), gossip_port: p[0], http_port: p[1],
        zone: "hub".into(), bootstrap: vec![], cert_dir: cert_dir.clone(), health_secs: None,
    }).await?;
    let _buf_ts = TupleSpace::new(Arc::clone(&buffer.agent), TupleConfig {
        namespace: Arc::clone(&ns), role: TupleRole::Primary, persist: false, ..Default::default()
    }).await?;
    let seed = buffer.gossip_port;
    println!("[buffer] up — tuple-space primary (ns pipeline)");

    let mk = |name: &str, gp: u16, hp: u16| DepotOpts {
        name: name.into(), gossip_port: gp, http_port: hp,
        zone: "depot".into(), bootstrap: vec![seed], cert_dir: cert_dir.clone(), health_secs: None,
    };
    let seeder  = spawn_depot(mk("seeder",  p[2], p[3])).await?;
    let agent_a = spawn_depot(mk("agent-a", p[4], p[5])).await?;
    let agent_b = spawn_depot(mk("agent-b", p[6], p[7])).await?;

    let seeder_ts = TupleSpace::new(Arc::clone(&seeder.agent), TupleConfig {
        namespace: Arc::clone(&ns), role: TupleRole::Client, persist: false, ..Default::default()
    }).await?;
    let a_ts = TupleSpace::new(Arc::clone(&agent_a.agent), TupleConfig {
        namespace: Arc::clone(&ns), role: TupleRole::Client, persist: false, ..Default::default()
    }).await?;
    let b_ts = TupleSpace::new(Arc::clone(&agent_b.agent), TupleConfig {
        namespace: Arc::clone(&ns), role: TupleRole::Client, persist: false, ..Default::default()
    }).await?;
    println!("[seeder|agent-a|agent-b] up — two LLM workers over the pipeline");

    wait_until(20, || !agent_a.agent.peers().is_empty() && !agent_b.agent.peers().is_empty()).await;
    for _ in 0..100 {
        if seeder_ts.depth(None).await.is_ok() && a_ts.depth(None).await.is_ok() && b_ts.depth(None).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // ── seed N raw donations into the classify lane ─────────────────────────────
    for id in 1..=N {
        let d = Donation::new(id, "borough-market", "surplus produce", "southwark");
        seeder_ts.put(CLASSIFY, d.to_bytes()).await?;
    }
    println!("[seeder] put {N} donations into the '{CLASSIFY}' lane");

    // ── both LLM workers drain the pipeline concurrently (pull) ─────────────────
    let done_count = Arc::new(AtomicU64::new(0));
    let backend_a: Arc<dyn LlmBackend> = Arc::new(EchoBackend);
    let backend_b: Arc<dyn LlmBackend> = Arc::new(EchoBackend);
    let wa = tokio::spawn(run_worker("agent-a".into(), Arc::clone(&a_ts), backend_a, Arc::clone(&done_count)));
    let wb = tokio::spawn(run_worker("agent-b".into(), Arc::clone(&b_ts), backend_b, Arc::clone(&done_count)));

    let finished = wait_until(45, || done_count.load(Ordering::Acquire) >= N).await;
    let a_did = wa.await?;
    let b_did = wb.await?;
    assert!(finished, "all {N} donations must flow classify→route→done");

    // ── verify the done lane: N items, each carrying evidence of BOTH LLM passes ─
    let done_depth = a_ts.depth(Some(DONE)).await?.first().map(|s| s.depth).unwrap_or(0);
    println!("[result] {done_depth} items in '{DONE}' — agent-a advanced {a_did}, agent-b advanced {b_did}");
    assert_eq!(done_depth as u64, N, "every donation reached the done lane");
    assert!(a_did > 0 && b_did > 0, "both LLM workers participated (pull = whoever's ready)");

    // Drain one done item and confirm it was processed by two chained LLM passes (nested echoes).
    let (_id, sample) = a_ts.take(DONE, Duration::from_secs(5)).await?;
    let text = String::from_utf8_lossy(&sample);
    assert!(text.contains("ROUTE:") && text.contains("CLASSIFY:"),
        "a done item carries both the classify and route LLM passes — got {text}");
    println!("[result] sample done item shows both LLM stages: route(classify(donation)) ✓");

    println!("\nAll assertions passed — two LLM agents coordinated a multi-stage pipeline purely via the tuple space, no dispatcher.");

    for d in [agent_b, agent_a, seeder, buffer] {
        d.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(&cert_dir);
    Ok(())
}
