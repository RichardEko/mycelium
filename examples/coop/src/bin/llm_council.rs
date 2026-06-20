//! Example 10 — **a council of differentiated LLM agents deliberates** (fan-out · synthesis ·
//! iterative refinement, all on a shared tuple space).
//!
//! The capstone of the LLM-coordination examples. A raw donation **evolves** into an approved
//! distribution plan by passing through three collaboration modes in sequence — and the agents are
//! *differentiated specialists*, each pulling only its own lane. No orchestrator; the tuple space is
//! the only coordination.
//!
//! ```text
//!                          ┌─▶ spec.perish ─▶(perishability)─┐
//! seeder ─▶ assess ─(fan-out)┼─▶ spec.route  ─▶(routing)──────┼─▶ partials ─(synthesizer: join by id)─▶ draft
//!                          └─▶ spec.allergen▶(allergen)──────┘                                          │
//!   approved ◀─[pass]─(critic)◀──── draft ◀─(reviser:+quality)─ revise ◀─[fail]─(critic)◀──────────────┘
//! ```
//!
//!  • **Phase 1 — fan-out to specialists:** a fan-out agent copies the donation into three lanes;
//!    three differentiated agents (perishability / routing / allergen) each pull *their own* lane,
//!    in parallel, and emit a signed-off partial.
//!  • **Phase 2 — fan-in synthesis:** a synthesizer drains `partials`, accumulates them **by
//!    donation id**, and once it holds all three for an id, merges them into a draft plan.
//!  • **Phase 3 — iterative refinement:** a critic scores the draft against a quality bar; on a fail
//!    it sends the item **back to `revise`**; a reviser improves it and sends it **back to `draft`**
//!    — the item cycles until the critic approves.
//!
//! Every role is a real `LlmBackend::complete` call with a role-specific prompt (an `EchoBackend`
//! stand-in, so CI needs no key); the structured decisions are deterministic so the demo asserts
//! cleanly. The draft starts at quality 0.6, the bar is 0.9, each revision adds 0.2 → exactly two
//! refinement cycles (0.6 → 0.8 → 1.0).
//!
//! **Architectural note:** with a *single* synthesizer the fan-in join is done in the synthesizer's
//! own memory (accumulate-by-id after `take`) — fully expressible today. *Competing* synthesizers
//! would each grab fragments of one donation's partial set, which needs keyed-exact-match `take`
//! (ROADMAP M13, Paper 1 §9.4). This demo sits exactly at that boundary and names the line.
//!
//! Run:  cargo run -p mycelium-coop-examples --bin llm_council

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use coop::common::{alloc_ports, spawn_depot, DepotOpts};
use mycelium::{EchoBackend, LlmBackend};
use mycelium_tuple_space::{TupleConfig, TupleError, TupleRole, TupleSpace};
use serde_json::{json, Value};

const ASSESS: &str = "assess";
const L_PERISH: &str = "spec.perish";
const L_ROUTE: &str = "spec.route";
const L_ALLERGEN: &str = "spec.allergen";
const PARTIALS: &str = "partials";
const DRAFT: &str = "draft";
const REVISE: &str = "revise";
const APPROVED: &str = "approved";

const N: u64 = 3;
const QUALITY_BAR: f64 = 0.9;

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

fn parse(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).unwrap_or(Value::Null)
}
fn bytes(v: &Value) -> bytes::Bytes {
    bytes::Bytes::from(serde_json::to_vec(v).unwrap_or_default())
}

/// A role-loop runs until `approved` reaches `N`, taking from `lane` with a short timeout (so it
/// rechecks the stop flag between items). `step` does the agent's work for one item.
async fn agent_loop<F, Fut>(ts: Arc<TupleSpace>, lane: &'static str, approved: Arc<AtomicU64>, step: F)
where
    F: Fn(u64, Value) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    while approved.load(Ordering::Acquire) < N {
        match ts.take(lane, Duration::from_millis(300)).await {
            Ok((id, payload)) => step(id, parse(&payload)).await,
            Err(TupleError::Timeout) => tokio::time::sleep(Duration::from_millis(30)).await,
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}

/// A specialist: call the model with a role prompt, attach a deterministic structured field, and
/// `complete` the item to `partials` carrying the donation id.
async fn specialist(
    ts: &TupleSpace, backend: &Arc<dyn LlmBackend>, id: u64, donation: Value,
    kind: &str, system: &str, field: (&str, Value),
) {
    let did = donation["id"].as_u64().unwrap_or(0);
    let _ = backend.complete(system, &format!("assess: {donation}"), 64, 0.0).await; // the agent "thinks"
    let partial = json!({ "id": did, "kind": kind, field.0: field.1 });
    let _ = ts.complete(id, PARTIALS, bytes(&partial)).await;
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ERROR level: a multi-node tuple space emits benign sys/tuple/ tripwire WARNs (gossip
    // reflection of a node's own metrics key) that would otherwise drown the demo output.
    tracing_subscriber::fmt().with_max_level(tracing::Level::ERROR).init();

    let cert_dir = std::env::temp_dir().join(format!("coop-council-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&cert_dir);
    let p = alloc_ports(8); // buffer, intake, specialists, synthesis

    let ns = Arc::from("council");
    let buffer = spawn_depot(DepotOpts {
        name: "buffer".into(), gossip_port: p[0], http_port: p[1],
        zone: "hub".into(), bootstrap: vec![], cert_dir: cert_dir.clone(), health_secs: Some(2),
    }).await?;
    let _buf_ts = TupleSpace::new(Arc::clone(&buffer.agent), TupleConfig {
        namespace: Arc::clone(&ns), role: TupleRole::Primary, persist: false, ..Default::default()
    }).await?;
    let seed = buffer.gossip_port;
    println!("[buffer] up — tuple-space primary (ns council)");

    let mk = |name: &str, gp: u16, hp: u16| DepotOpts {
        name: name.into(), gossip_port: gp, http_port: hp,
        zone: "depot".into(), bootstrap: vec![seed], cert_dir: cert_dir.clone(), health_secs: Some(2),
    };
    let intake      = spawn_depot(mk("intake",      p[2], p[3])).await?;
    let specialists = spawn_depot(mk("specialists", p[4], p[5])).await?;
    let synthesis   = spawn_depot(mk("synthesis",   p[6], p[7])).await?;

    let client = |a: &Arc<mycelium::GossipAgent>| {
        let a = Arc::clone(a);
        let ns = Arc::clone(&ns);
        async move {
            TupleSpace::new(a, TupleConfig {
                namespace: ns, role: TupleRole::Client, persist: false, ..Default::default()
            }).await
        }
    };
    let intake_ts = client(&intake.agent).await?;
    let spec_ts   = client(&specialists.agent).await?;
    let synth_ts  = client(&synthesis.agent).await?;
    println!("[intake|specialists|synthesis] up — a council of differentiated LLM agents");

    wait_until(25, || !synthesis.agent.peers().is_empty() && !specialists.agent.peers().is_empty()).await;
    let mut ready = false;
    for _ in 0..300 {
        if intake_ts.depth(None).await.is_ok() && spec_ts.depth(None).await.is_ok() && synth_ts.depth(None).await.is_ok() {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(ready, "every council node must reach the tuple-space primary");

    let approved = Arc::new(AtomicU64::new(0));
    let backend: Arc<dyn LlmBackend> = Arc::new(EchoBackend);
    let mut tasks = Vec::new();

    // ── Phase 1 — fan-out agent (intake): assess → three specialist lanes ───────
    {
        let ts = Arc::clone(&intake_ts);
        let approved = Arc::clone(&approved);
        tasks.push(tokio::spawn(agent_loop(Arc::clone(&ts), ASSESS, approved, move |id, donation| {
            let ts = Arc::clone(&ts);
            async move {
                // 1 consumed → 3 produced (fan-out), each carrying the donation.
                let _ = ts.put(L_PERISH, bytes(&donation)).await;
                let _ = ts.put(L_ROUTE, bytes(&donation)).await;
                let _ = ts.put(L_ALLERGEN, bytes(&donation)).await;
                let _ = ts.ack(id).await;
            }
        })));
    }

    // ── Phase 1 — three differentiated specialists (specialists node) ───────────
    for (lane, kind, system, field) in [
        (L_PERISH,   "perish",  "You assess a donation's shelf life.", ("urgency", json!("high"))),
        (L_ROUTE,    "route",   "You pick the nearest community kitchen.", ("kitchen", json!("camden-kitchen"))),
        (L_ALLERGEN, "allergen","You flag dietary constraints.", ("flags", json!(["dairy"]))),
    ] {
        let ts = Arc::clone(&spec_ts);
        let backend = Arc::clone(&backend);
        let approved = Arc::clone(&approved);
        tasks.push(tokio::spawn(agent_loop(Arc::clone(&ts), lane, approved, move |id, donation| {
            let ts = Arc::clone(&ts);
            let backend = Arc::clone(&backend);
            let field = (field.0, field.1.clone());
            async move { specialist(&ts, &backend, id, donation, kind, system, field).await }
        })));
    }

    // ── Phase 2 — synthesizer (synthesis node): join partials by id → draft ─────
    {
        let ts = Arc::clone(&synth_ts);
        let backend = Arc::clone(&backend);
        let approved = Arc::clone(&approved);
        let acc: Arc<tokio::sync::Mutex<HashMap<u64, Value>>> = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        tasks.push(tokio::spawn(agent_loop(Arc::clone(&ts), PARTIALS, approved, move |id, partial| {
            let ts = Arc::clone(&ts);
            let backend = Arc::clone(&backend);
            let acc = Arc::clone(&acc);
            async move {
                let _ = ts.ack(id).await; // terminally remove the partial; we join in memory
                let did = partial["id"].as_u64().unwrap_or(0);
                let mut map = acc.lock().await;
                let entry = map.entry(did).or_insert_with(|| json!({"id": did}));
                if let Some(k) = partial["kind"].as_str() {
                    match k {
                        "perish"   => entry["urgency"] = partial["urgency"].clone(),
                        "route"    => entry["kitchen"] = partial["kitchen"].clone(),
                        "allergen" => entry["flags"]   = partial["flags"].clone(),
                        _ => {}
                    }
                }
                // All three specialist inputs collected → synthesize a draft plan.
                let complete = entry.get("urgency").is_some() && entry.get("kitchen").is_some() && entry.get("flags").is_some();
                if complete {
                    let mut plan = map.remove(&did).unwrap();
                    drop(map);
                    let _ = backend.complete("You synthesize a distribution plan.", &format!("merge: {plan}"), 64, 0.0).await;
                    plan["quality"] = json!(0.6);
                    plan["revisions"] = json!(0);
                    let _ = ts.put(DRAFT, bytes(&plan)).await;
                }
            }
        })));
    }

    // ── Phase 3 — critic (synthesis node): score → approved | revise ────────────
    {
        let ts = Arc::clone(&synth_ts);
        let backend = Arc::clone(&backend);
        let approved_outer = Arc::clone(&approved);
        let approved_step = Arc::clone(&approved);
        tasks.push(tokio::spawn(agent_loop(Arc::clone(&ts), DRAFT, approved_outer, move |id, plan| {
            let ts = Arc::clone(&ts);
            let backend = Arc::clone(&backend);
            let approved_step = Arc::clone(&approved_step);
            async move {
                let _ = backend.complete("You critique a plan against a quality bar.", &format!("review: {plan}"), 64, 0.0).await;
                let q = plan["quality"].as_f64().unwrap_or(0.0);
                if q >= QUALITY_BAR {
                    if ts.complete(id, APPROVED, bytes(&plan)).await.is_ok() {
                        let n = approved_step.fetch_add(1, Ordering::AcqRel) + 1;
                        println!("[critic] approved plan for donation {} (quality {q:.1}, {} revisions) [{n}/{N}]",
                            plan["id"], plan["revisions"]);
                    }
                } else {
                    let _ = ts.complete(id, REVISE, bytes(&plan)).await;
                    println!("[critic] rejected donation {} (quality {q:.1} < {QUALITY_BAR}) → revise", plan["id"]);
                }
            }
        })));
    }

    // ── Phase 3 — reviser (synthesis node): improve quality → back to draft ─────
    {
        let ts = Arc::clone(&synth_ts);
        let backend = Arc::clone(&backend);
        let approved = Arc::clone(&approved);
        tasks.push(tokio::spawn(agent_loop(Arc::clone(&ts), REVISE, approved, move |id, mut plan| {
            let ts = Arc::clone(&ts);
            let backend = Arc::clone(&backend);
            async move {
                let _ = backend.complete("You improve a rejected plan.", &format!("revise: {plan}"), 64, 0.0).await;
                plan["quality"] = json!(plan["quality"].as_f64().unwrap_or(0.0) + 0.2);
                plan["revisions"] = json!(plan["revisions"].as_u64().unwrap_or(0) + 1);
                let _ = ts.complete(id, DRAFT, bytes(&plan)).await; // cycle back to the critic
            }
        })));
    }

    // ── seed N donations and let the council deliberate ─────────────────────────
    for id in 1..=N {
        let d = json!({ "id": id, "items": "12 crates dairy", "origin": "borough-market" });
        intake_ts.put(ASSESS, bytes(&d)).await?;
    }
    println!("[seeder] put {N} donations into '{ASSESS}' — the council convenes\n");

    let finished = wait_until(60, || approved.load(Ordering::Acquire) >= N).await;
    assert!(finished, "all {N} donations must reach an approved plan");

    // ── verify the approved lane: N plans, each fully deliberated + refined ──────
    let mut plans = Vec::new();
    for _ in 0..N {
        let (id, payload) = synth_ts.take(APPROVED, Duration::from_secs(5)).await?;
        synth_ts.ack(id).await?;
        plans.push(parse(&payload));
    }
    println!("\n[result] {} approved plans drained from '{APPROVED}'", plans.len());
    assert_eq!(plans.len() as u64, N, "every donation produced an approved plan");
    for plan in &plans {
        assert!(plan.get("urgency").is_some() && plan.get("kitchen").is_some() && plan.get("flags").is_some(),
            "an approved plan carries all three specialists' contributions — got {plan}");
        assert!(plan["quality"].as_f64().unwrap_or(0.0) >= QUALITY_BAR,
            "an approved plan cleared the quality bar — got {plan}");
        assert!(plan["revisions"].as_u64().unwrap_or(0) >= 2,
            "an approved plan went through the critic↔reviser loop (>=2 revisions) — got {plan}");
    }
    println!("[result] every plan: fanned out to 3 specialists, synthesized, and refined >=2 times ✓");

    println!("\nAll assertions passed — a council of differentiated LLM agents deliberated a shared task (fan-out · synthesis · iterative refinement) purely via the tuple space, no orchestrator.");

    for t in tasks { t.abort(); }
    for d in [synthesis, specialists, intake, buffer] {
        d.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(&cert_dir);
    Ok(())
}
