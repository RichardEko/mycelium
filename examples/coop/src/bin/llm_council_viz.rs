//! Example 10b — **a council of differentiated LLM agents, visualised**: fan-out · synthesis ·
//! iterative refinement, streamed live to a `<canvas>` in the browser.
//!
//! This is the continuous, animated sibling of `llm_council.rs`. Same council, same topology, same
//! differentiated specialists — but instead of seeding a fixed `N` donations, asserting, and
//! exiting, it **seeds a fresh donation every few seconds forever** and streams the deliberation
//! picture to a dashboard at `http://127.0.0.1:8094/`.
//!
//! ```text
//!                          ┌─▶ spec.perish ─▶(perishability)─┐
//! seeder ─▶ assess ─(fan-out)┼─▶ spec.route  ─▶(routing)──────┼─▶ partials ─(synthesizer: join by id)─▶ draft
//!                          └─▶ spec.allergen▶(allergen)──────┘                                          │
//!   approved ◀─[pass]─(critic)◀──── draft ◀─(reviser:+quality)─ revise ◀─[fail]─(critic)◀──────────────┘
//! ```
//!
//! A raw food donation **evolves** into an approved distribution plan by passing through three
//! collaboration modes in sequence — and the agents are *differentiated specialists*, each pulling
//! only its own lane. No orchestrator; the tuple space is the only coordination.
//!
//!  • **Phase 1 — fan-out to specialists:** a fan-out agent copies the donation into three lanes;
//!    three differentiated agents (perishability / routing / allergen) each pull *their own* lane,
//!    in parallel, and emit a signed-off partial.
//!  • **Phase 2 — fan-in synthesis:** a synthesizer drains `partials`, accumulates them **by
//!    donation id**, and once it holds all three for an id, merges them into a draft plan.
//!  • **Phase 3 — iterative refinement:** a critic scores the draft against a quality bar; on a fail
//!    it sends the item **back to `revise`**; a reviser improves it and sends it **back to `draft`**
//!    — the item cycles until the critic approves. Quality ratchets 0.6 → 0.8 → 1.0 (bar 0.9),
//!    exactly two refinement cycles, and you can watch the token climb the bar in the browser.
//!
//! Every role is a real `LlmBackend::complete` call with a role-specific prompt — an `EchoBackend`
//! stand-in, so this needs **no LLM key**. The council instruments each step into a shared snapshot
//! (lane depths, in-flight donations + their stage/quality/revisions, an approved tally) that the
//! `/state` handler serves as JSON; the canvas polls it and animates the DAG.
//!
//! Run:  cargo run -p mycelium-coop-examples --bin llm_council_viz
//! Then open http://127.0.0.1:8094/

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use coop::common::{alloc_ports, spawn_depot, DepotOpts};
use mycelium::{EchoBackend, LlmBackend};
use mycelium_tuple_space::{TupleConfig, TupleError, TupleRole, TupleSpace};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::{signal, time};

const ASSESS: &str = "assess";
const L_PERISH: &str = "spec.perish";
const L_ROUTE: &str = "spec.route";
const L_ALLERGEN: &str = "spec.allergen";
const PARTIALS: &str = "partials";
const DRAFT: &str = "draft";
const REVISE: &str = "revise";
const APPROVED: &str = "approved";

/// The eight lanes the dashboard draws, in DAG order.
const LANES: [&str; 8] = [
    ASSESS, L_PERISH, L_ROUTE, L_ALLERGEN, PARTIALS, DRAFT, REVISE, APPROVED,
];

const QUALITY_BAR: f64 = 0.9;
/// Fixed HTTP port for the dashboard (kept clear of the OS-assigned depot ports).
const HTTP_PORT: u16 = 8094;
/// Seed a fresh donation on this cadence — forever.
const SEED_EVERY: Duration = Duration::from_secs(4);
/// EchoBackend deliberates instantly; each role "thinks" for this long so a browser polling at
/// ~500ms actually catches tokens dwelling at each stage and the refinement loop is watchable.
/// End-to-end (~fan-out + 3 specialists + synth + two critic↔reviser cycles) stays under the seed
/// cadence, so the DAG usually holds one or two donations without ever backlogging.
const THINK: Duration = Duration::from_millis(420);

// ── Shared snapshot served to the browser ─────────────────────────────────
struct DonView {
    stage:     String,
    quality:   f64,
    revisions: u64,
}

struct VizState {
    tick:            u64,
    lanes:           Vec<(String, u32)>,
    in_flight:       BTreeMap<u64, DonView>,
    approved_total:  u64,
    recent_approved: VecDeque<(u64, u64)>, // (id, revisions)
}

impl VizState {
    fn new() -> Self {
        VizState {
            tick: 0,
            lanes: LANES.iter().map(|l| (l.to_string(), 0u32)).collect(),
            in_flight: BTreeMap::new(),
            approved_total: 0,
            recent_approved: VecDeque::new(),
        }
    }

    fn to_json(&self) -> String {
        let lanes: Vec<Value> = self
            .lanes
            .iter()
            .map(|(name, depth)| json!({ "name": name, "depth": depth }))
            .collect();
        // Cap in-flight at the 12 most-recent donation ids (BTreeMap is id-ordered).
        let in_flight: Vec<Value> = self
            .in_flight
            .iter()
            .rev()
            .take(12)
            .map(|(id, d)| {
                json!({
                    "id": id,
                    "stage": d.stage,
                    "quality": d.quality,
                    "revisions": d.revisions,
                })
            })
            .collect();
        let recent: Vec<Value> = self
            .recent_approved
            .iter()
            .rev()
            .take(6)
            .map(|(id, revs)| json!({ "id": id, "revisions": revs }))
            .collect();
        json!({
            "tick": self.tick,
            "lanes": lanes,
            "in_flight": in_flight,
            "approved_total": self.approved_total,
            "recent_approved": recent,
        })
        .to_string()
    }
}

// ── Snapshot mutators (each agent step calls one) ─────────────────────────
fn set_stage(state: &Arc<Mutex<VizState>>, id: u64, stage: &str, quality: f64, revisions: u64) {
    let mut s = state.lock().unwrap();
    let e = s.in_flight.entry(id).or_insert_with(|| DonView {
        stage: stage.to_string(),
        quality,
        revisions,
    });
    e.stage = stage.to_string();
    e.quality = quality;
    e.revisions = revisions;
}

fn approve(state: &Arc<Mutex<VizState>>, id: u64, revisions: u64) {
    let mut s = state.lock().unwrap();
    s.in_flight.remove(&id);
    s.approved_total += 1;
    s.recent_approved.push_back((id, revisions));
    while s.recent_approved.len() > 12 {
        s.recent_approved.pop_front();
    }
}

fn parse(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).unwrap_or(Value::Null)
}
fn bytes(v: &Value) -> bytes::Bytes {
    bytes::Bytes::from(serde_json::to_vec(v).unwrap_or_default())
}

async fn wait_until(secs: u64, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    while std::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        time::sleep(Duration::from_millis(150)).await;
    }
    cond()
}

/// A role-loop runs until the `running` flag drops (ctrl-c), taking from `lane` with a short
/// timeout so it rechecks the flag between items. `step` does the agent's work for one item.
async fn agent_loop<F, Fut>(
    ts: Arc<TupleSpace>,
    lane: &'static str,
    running: Arc<AtomicBool>,
    step: F,
) where
    F: Fn(u64, Value) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    while running.load(Ordering::Acquire) {
        match ts.take(lane, Duration::from_millis(300)).await {
            Ok((id, payload)) => step(id, parse(&payload)).await,
            Err(TupleError::Timeout) => time::sleep(Duration::from_millis(30)).await,
            Err(_) => time::sleep(Duration::from_millis(100)).await,
        }
    }
}

/// A specialist: call the model with a role prompt, attach a deterministic structured field, and
/// `complete` the item to `partials` carrying the donation id.
async fn specialist(
    ts: &TupleSpace,
    backend: &Arc<dyn LlmBackend>,
    id: u64,
    donation: Value,
    kind: &str,
    system: &str,
    field: (&str, Value),
) {
    let did = donation["id"].as_u64().unwrap_or(0);
    let _ = backend.complete(system, &format!("assess: {donation}"), 64, 0.0).await; // "thinks"
    time::sleep(THINK).await; // specialist deliberates — token dwells before the join
    let partial = json!({ "id": did, "kind": kind, field.0: field.1 });
    let _ = ts.complete(id, PARTIALS, bytes(&partial)).await;
}

// ── Minimal HTTP server (mirrors stigmergy_viz.rs) — no deps beyond tokio ──
async fn serve_http(state: Arc<Mutex<VizState>>) {
    let listener = match TcpListener::bind(format!("127.0.0.1:{HTTP_PORT}")).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("HTTP server failed to bind :{HTTP_PORT} — {e}");
            return;
        }
    };
    println!("╔══════════════════════════════════════════════════════╗");
    println!("║  Council dashboard → http://127.0.0.1:{HTTP_PORT}          ║");
    println!("║  fan-out · synthesis · iterative refinement, live      ║");
    println!("╚══════════════════════════════════════════════════════╝");

    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        let st = state.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            let req = std::str::from_utf8(&buf[..n]).unwrap_or("");

            if req.starts_with("OPTIONS") {
                let _ = stream
                    .write_all(
                        b"HTTP/1.1 204 No Content\r\n\
                          Access-Control-Allow-Origin: *\r\n\
                          Connection: close\r\n\r\n",
                    )
                    .await;
                return;
            }

            if req.contains("GET /state") {
                let json = st.lock().unwrap().to_json();
                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                     Content-Type: application/json\r\n\
                     Access-Control-Allow-Origin: *\r\n\
                     Content-Length: {}\r\n\
                     Connection: close\r\n\r\n{}",
                    json.len(),
                    json
                );
                let _ = stream.write_all(response.as_bytes()).await;
            } else {
                let html = include_str!("llm_council_viz.html");
                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                     Content-Type: text/html; charset=utf-8\r\n\
                     Content-Length: {}\r\n\
                     Connection: close\r\n\r\n{}",
                    html.len(),
                    html
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ERROR level: a multi-node tuple space emits benign sys/tuple/ tripwire WARNs (gossip
    // reflection of a node's own metrics key) that would otherwise drown the demo output.
    tracing_subscriber::fmt().with_max_level(tracing::Level::ERROR).init();

    let cert_dir = std::env::temp_dir().join(format!("coop-council-viz-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&cert_dir);
    let p = alloc_ports(8); // buffer, intake, specialists, synthesis

    let ns = Arc::from("council");
    let buffer = spawn_depot(DepotOpts {
        name: "buffer".into(),
        gossip_port: p[0],
        http_port: p[1],
        zone: "hub".into(),
        bootstrap: vec![],
        cert_dir: cert_dir.clone(),
        health_secs: Some(2),
    })
    .await?;
    let buf_ts = TupleSpace::new(
        Arc::clone(&buffer.agent),
        TupleConfig {
            namespace: Arc::clone(&ns),
            role: TupleRole::Primary,
            persist: false,
            ..Default::default()
        },
    )
    .await?;
    let seed = buffer.gossip_port;
    println!("[buffer] up — tuple-space primary (ns council)");
    println!("[ops] buffer gateway is live — point the Ops Console at http://127.0.0.1:{}/  (/stats · /gateway/fleet · /gateway/diagnose)", p[1]);

    let mk = |name: &str, gp: u16, hp: u16| DepotOpts {
        name: name.into(),
        gossip_port: gp,
        http_port: hp,
        zone: "depot".into(),
        bootstrap: vec![seed],
        cert_dir: cert_dir.clone(),
        health_secs: Some(2),
    };
    let intake = spawn_depot(mk("intake", p[2], p[3])).await?;
    let specialists = spawn_depot(mk("specialists", p[4], p[5])).await?;
    let synthesis = spawn_depot(mk("synthesis", p[6], p[7])).await?;

    let client = |a: &Arc<mycelium::GossipAgent>| {
        let a = Arc::clone(a);
        let ns = Arc::clone(&ns);
        async move {
            TupleSpace::new(
                a,
                TupleConfig {
                    namespace: ns,
                    role: TupleRole::Client,
                    persist: false,
                    ..Default::default()
                },
            )
            .await
        }
    };
    let intake_ts = client(&intake.agent).await?;
    let spec_ts = client(&specialists.agent).await?;
    let synth_ts = client(&synthesis.agent).await?;
    println!("[intake|specialists|synthesis] up — a council of differentiated LLM agents");

    wait_until(25, || {
        !synthesis.agent.peers().is_empty() && !specialists.agent.peers().is_empty()
    })
    .await;
    let mut ready = false;
    for _ in 0..300 {
        if intake_ts.depth(None).await.is_ok()
            && spec_ts.depth(None).await.is_ok()
            && synth_ts.depth(None).await.is_ok()
        {
            ready = true;
            break;
        }
        time::sleep(Duration::from_millis(100)).await;
    }
    assert!(ready, "every council node must reach the tuple-space primary");

    // ── Shared snapshot + HTTP dashboard (start early so the browser connects immediately) ──
    let state = Arc::new(Mutex::new(VizState::new()));
    let state_for_server = state.clone();
    tokio::spawn(async move { serve_http(state_for_server).await });

    let running = Arc::new(AtomicBool::new(true));
    let backend: Arc<dyn LlmBackend> = Arc::new(EchoBackend);
    let mut tasks = Vec::new();

    // ── Phase 1 — fan-out agent (intake): assess → three specialist lanes ───────
    {
        let ts = Arc::clone(&intake_ts);
        let running = Arc::clone(&running);
        let state = state.clone();
        tasks.push(tokio::spawn(agent_loop(
            Arc::clone(&ts),
            ASSESS,
            running,
            move |id, donation| {
                let ts = Arc::clone(&ts);
                let state = state.clone();
                async move {
                    let did = donation["id"].as_u64().unwrap_or(0);
                    set_stage(&state, did, "specialists", 0.0, 0);
                    time::sleep(THINK).await; // let the token dwell at the fan-out for the browser
                    // 1 consumed → 3 produced (fan-out), each carrying the donation.
                    let _ = ts.put(L_PERISH, bytes(&donation)).await;
                    let _ = ts.put(L_ROUTE, bytes(&donation)).await;
                    let _ = ts.put(L_ALLERGEN, bytes(&donation)).await;
                    let _ = ts.ack(id).await;
                }
            },
        )));
    }

    // ── Phase 1 — three differentiated specialists (specialists node) ───────────
    for (lane, kind, system, field) in [
        (L_PERISH, "perish", "You assess a donation's shelf life.", ("urgency", json!("high"))),
        (L_ROUTE, "route", "You pick the nearest community kitchen.", ("kitchen", json!("camden-kitchen"))),
        (L_ALLERGEN, "allergen", "You flag dietary constraints.", ("flags", json!(["dairy"]))),
    ] {
        let ts = Arc::clone(&spec_ts);
        let backend = Arc::clone(&backend);
        let running = Arc::clone(&running);
        let state = state.clone();
        tasks.push(tokio::spawn(agent_loop(
            Arc::clone(&ts),
            lane,
            running,
            move |id, donation| {
                let ts = Arc::clone(&ts);
                let backend = Arc::clone(&backend);
                let state = state.clone();
                let field = (field.0, field.1.clone());
                async move {
                    let did = donation["id"].as_u64().unwrap_or(0);
                    set_stage(&state, did, "partials", 0.0, 0);
                    specialist(&ts, &backend, id, donation, kind, system, field).await
                }
            },
        )));
    }

    // ── Phase 2 — synthesizer (synthesis node): join partials by id → draft ─────
    {
        let ts = Arc::clone(&synth_ts);
        let backend = Arc::clone(&backend);
        let running = Arc::clone(&running);
        let state = state.clone();
        let acc: Arc<tokio::sync::Mutex<std::collections::HashMap<u64, Value>>> =
            Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
        tasks.push(tokio::spawn(agent_loop(
            Arc::clone(&ts),
            PARTIALS,
            running,
            move |id, partial| {
                let ts = Arc::clone(&ts);
                let backend = Arc::clone(&backend);
                let acc = Arc::clone(&acc);
                let state = state.clone();
                async move {
                    let _ = ts.ack(id).await; // terminally remove the partial; we join in memory
                    let did = partial["id"].as_u64().unwrap_or(0);
                    let mut map = acc.lock().await;
                    let entry = map.entry(did).or_insert_with(|| json!({ "id": did }));
                    if let Some(k) = partial["kind"].as_str() {
                        match k {
                            "perish" => entry["urgency"] = partial["urgency"].clone(),
                            "route" => entry["kitchen"] = partial["kitchen"].clone(),
                            "allergen" => entry["flags"] = partial["flags"].clone(),
                            _ => {}
                        }
                    }
                    let complete = entry.get("urgency").is_some()
                        && entry.get("kitchen").is_some()
                        && entry.get("flags").is_some();
                    if complete {
                        let mut plan = map.remove(&did).unwrap();
                        drop(map);
                        let _ = backend
                            .complete("You synthesize a distribution plan.", &format!("merge: {plan}"), 64, 0.0)
                            .await;
                        time::sleep(THINK).await; // synthesizing the joined draft
                        plan["quality"] = json!(0.6);
                        plan["revisions"] = json!(0);
                        set_stage(&state, did, "draft", 0.6, 0);
                        let _ = ts.put(DRAFT, bytes(&plan)).await;
                    }
                }
            },
        )));
    }

    // ── Phase 3 — critic (synthesis node): score → approved | revise ────────────
    {
        let ts = Arc::clone(&synth_ts);
        let backend = Arc::clone(&backend);
        let running = Arc::clone(&running);
        let state = state.clone();
        tasks.push(tokio::spawn(agent_loop(
            Arc::clone(&ts),
            DRAFT,
            running,
            move |id, plan| {
                let ts = Arc::clone(&ts);
                let backend = Arc::clone(&backend);
                let state = state.clone();
                async move {
                    let _ = backend
                        .complete("You critique a plan against a quality bar.", &format!("review: {plan}"), 64, 0.0)
                        .await;
                    time::sleep(THINK).await; // critic scores the draft against the bar
                    let did = plan["id"].as_u64().unwrap_or(0);
                    let q = plan["quality"].as_f64().unwrap_or(0.0);
                    let revs = plan["revisions"].as_u64().unwrap_or(0);
                    if q >= QUALITY_BAR {
                        set_stage(&state, did, "approved", q, revs);
                        let _ = ts.complete(id, APPROVED, bytes(&plan)).await;
                    } else {
                        set_stage(&state, did, "revise", q, revs);
                        let _ = ts.complete(id, REVISE, bytes(&plan)).await;
                    }
                }
            },
        )));
    }

    // ── Phase 3 — reviser (synthesis node): improve quality → back to draft ─────
    {
        let ts = Arc::clone(&synth_ts);
        let backend = Arc::clone(&backend);
        let running = Arc::clone(&running);
        let state = state.clone();
        tasks.push(tokio::spawn(agent_loop(
            Arc::clone(&ts),
            REVISE,
            running,
            move |id, mut plan| {
                let ts = Arc::clone(&ts);
                let backend = Arc::clone(&backend);
                let state = state.clone();
                async move {
                    let _ = backend
                        .complete("You improve a rejected plan.", &format!("revise: {plan}"), 64, 0.0)
                        .await;
                    time::sleep(THINK).await; // reviser reworks the plan (+0.2 quality)
                    let did = plan["id"].as_u64().unwrap_or(0);
                    let q = plan["quality"].as_f64().unwrap_or(0.0) + 0.2;
                    let revs = plan["revisions"].as_u64().unwrap_or(0) + 1;
                    plan["quality"] = json!(q);
                    plan["revisions"] = json!(revs);
                    set_stage(&state, did, "draft", q, revs);
                    let _ = ts.complete(id, DRAFT, bytes(&plan)).await; // cycle back to the critic
                }
            },
        )));
    }

    // ── Publisher (synthesis node): drain approved plans → tally + recent list ──
    {
        let ts = Arc::clone(&synth_ts);
        let running = Arc::clone(&running);
        let state = state.clone();
        tasks.push(tokio::spawn(agent_loop(
            Arc::clone(&ts),
            APPROVED,
            running,
            move |id, plan| {
                let ts = Arc::clone(&ts);
                let state = state.clone();
                async move {
                    let _ = ts.ack(id).await;
                    let did = plan["id"].as_u64().unwrap_or(0);
                    let revs = plan["revisions"].as_u64().unwrap_or(0);
                    approve(&state, did, revs);
                    println!(
                        "[approved] donation {did} → distribution plan (quality {:.1}, {revs} revisions)",
                        plan["quality"].as_f64().unwrap_or(0.0)
                    );
                }
            },
        )));
    }

    // ── Background: sample lane depths off the primary store into the snapshot. ──
    let buf_ts_for_depth = Arc::clone(&buf_ts);
    let state_for_depth = state.clone();
    let running_for_depth = Arc::clone(&running);
    tokio::spawn(async move {
        let mut ticker = time::interval(Duration::from_millis(300));
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        while running_for_depth.load(Ordering::Acquire) {
            ticker.tick().await;
            let depths = buf_ts_for_depth.depth(None).await.unwrap_or_default();
            let mut s = state_for_depth.lock().unwrap();
            s.tick += 1;
            for (name, d) in s.lanes.iter_mut() {
                *d = depths
                    .iter()
                    .find(|td| td.stage.as_ref() == name.as_str())
                    .map(|td| td.depth + td.inflight)
                    .unwrap_or(0);
            }
        }
    });

    // ── Seed a fresh donation every few seconds — forever, until ctrl-c. ────────
    println!("\nCouncil running. Open http://127.0.0.1:{HTTP_PORT}/ · ctrl-c to exit.\n");
    let seeder_state = state.clone();
    let mut next_id: u64 = 1;
    loop {
        let items = [
            "12 crates dairy",
            "40kg mixed veg",
            "8 trays bakery",
            "20 crates citrus",
            "15kg cooked meals",
        ];
        let it = items[(next_id as usize - 1) % items.len()];
        let d = json!({ "id": next_id, "items": it, "origin": "borough-market" });
        set_stage(&seeder_state, next_id, "assess", 0.0, 0);
        if intake_ts.put(ASSESS, bytes(&d)).await.is_err() {
            eprintln!("[seeder] failed to seed donation {next_id} — retrying next tick");
        } else {
            println!("[seeder] donation {next_id} ({it}) enters the council");
        }
        next_id += 1;

        tokio::select! {
            _ = time::sleep(SEED_EVERY) => {}
            _ = signal::ctrl_c() => break,
        }
    }

    // ── Shutdown ────────────────────────────────────────────────────────
    println!("\nShutting down…");
    running.store(false, Ordering::Release);
    for t in &tasks {
        t.abort();
    }
    for d in [synthesis, specialists, intake, buffer] {
        d.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(&cert_dir);
    Ok(())
}
