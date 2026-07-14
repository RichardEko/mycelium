//! Live visual showcase of the community-microgrid blackboard — a browser dashboard.
//!
//! Companion to the batch `microgrid` example: same domain (a neighbourhood energy
//! co-op sharing ONE fact pool, no dispatcher), but it runs **forever** and serves a
//! live animated canvas so you can *watch* the blackboard's defining split happen:
//!
//! - **Reading is unconditional + concurrent (`rd`)** — a `forecaster` and a `tariff`
//!   agent both observe the whole surplus pool non-destructively, every tick. Their
//!   "sees N" counters track the shared, concurrent view.
//! - **Consuming a finite fact is competitive + exactly-once (`in`)** — two storage
//!   executors (`community-battery`, `ev-charger`) race to `claim` + `ack` each surplus.
//!   Exactly one charges against it; the dashboard asserts no fact is claimed twice and
//!   shows a green "exactly-once" badge (red if ever violated).
//!
//! An inverter posts a new `surplus` fact (feeder 4, varying kWh) a few times a second,
//! gossiped to all. Posting rate and drain rate fluctuate, so a visible pool builds up
//! and drains — the glowing dots on the feeder bus.
//!
//! Run:
//!   cargo run -p mycelium-blackboard --example microgrid_viz
//!
//! Then open http://127.0.0.1:8091/ — no external dependencies, works offline. Ctrl-C to exit.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use mycelium::{GossipAgent, GossipConfig, NodeId};
use mycelium_blackboard::{Blackboard, BoardConfig, BoardRole, Predicate};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::{signal, time};

const HTTP_PORT: u16 = 8091;
/// The mycelium gateway (for the Ops Console) — served only when built `--features gateway`
/// (this companion crate has default-features off). `/stats` · `/gateway/fleet` · `/gateway/diagnose`.
const OPS_PORT: u16 = 9091;
const RENDER_MS: u64 = 500;
const POST_BASE_MS: u64 = 800;
const READ_MS: u64 = 600;
const POOL_CAP: usize = 40;
const RECENT_CAP: usize = 8;

// ── Shared dashboard snapshot served to the browser ────────────────────
#[derive(Default)]
struct VizState {
    generation: u64,
    posted_total: u64,
    available: usize,
    pool: Vec<(u64, f64)>, // (id, kwh) of currently-claimable facts
    battery_claimed: u64,
    charger_claimed: u64,
    forecaster_sees: usize,
    tariff_sees: usize,
    recent: VecDeque<(u64, f64, String)>, // last claims: (id, kwh, by)
    exactly_once_ok: bool,
}

impl VizState {
    fn to_json(&self) -> String {
        let pool_str: Vec<String> = self
            .pool
            .iter()
            .take(POOL_CAP)
            .map(|(id, kwh)| format!(r#"{{"id":{id},"kwh":{kwh:.2}}}"#))
            .collect();
        let recent_str: Vec<String> = self
            .recent
            .iter()
            .map(|(id, kwh, by)| format!(r#"{{"id":{id},"kwh":{kwh:.2},"by":"{by}"}}"#))
            .collect();
        format!(
            r#"{{"generation":{},"posted_total":{},"available":{},"pool":[{}],"battery_claimed":{},"charger_claimed":{},"forecaster_sees":{},"tariff_sees":{},"recent":[{}],"exactly_once_ok":{}}}"#,
            self.generation,
            self.posted_total,
            self.available,
            pool_str.join(","),
            self.battery_claimed,
            self.charger_claimed,
            self.forecaster_sees,
            self.tariff_sees,
            recent_str.join(","),
            self.exactly_once_ok,
        )
    }
}

// ── Minimal HTTP server — no dependencies beyond tokio ────────────────
// Serves the HTML dashboard at / and the JSON state at /state, same origin
// so there are no CORS or file:// restrictions. Mirrors examples/conway.rs.
async fn serve_http(state: Arc<Mutex<VizState>>) {
    let listener = match TcpListener::bind(format!("127.0.0.1:{HTTP_PORT}")).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("HTTP server failed to bind :{HTTP_PORT} — {e}");
            return;
        }
    };
    eprintln!("╔══════════════════════════════════════════════════╗");
    eprintln!("║  Microgrid blackboard — live dashboard           ║");
    eprintln!("║  Open in browser → http://127.0.0.1:{HTTP_PORT}/      ║");
    eprintln!("╚══════════════════════════════════════════════════╝");

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
                let json = match st.lock() {
                    Ok(g) => g.to_json(),
                    Err(_) => return,
                };
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
                let html = include_str!("microgrid_viz.html");
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

// ── Tiny deterministic PRNG (xorshift64*) — keeps the demo dependency-free ──
fn next_rand(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *state = x;
    x.wrapping_mul(0x2545_F491_4F6C_DD1D)
}
// Uniform-ish f64 in [lo, hi).
fn rand_range(state: &mut u64, lo: f64, hi: f64) -> f64 {
    let r = (next_rand(state) >> 11) as f64 / (1u64 << 53) as f64;
    lo + r * (hi - lo)
}

fn kwh_of(fact_attrs: &BTreeMap<String, String>) -> f64 {
    fact_attrs
        .get("kwh")
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── One agent, one shared blackboard: the whole co-op reasons over it. ──
    let port = mycelium::test_util::alloc_port();
    let agent = Arc::new(GossipAgent::new(
        NodeId::new("127.0.0.1", port)?,
        GossipConfig { bind_port: port, http_port: Some(OPS_PORT), ..Default::default() },
    ));
    agent.start().await?;
    #[cfg(feature = "gateway")]
    eprintln!("║  Ops Console     → http://127.0.0.1:{OPS_PORT}/      ║");
    let bb = Blackboard::new(
        Arc::clone(&agent),
        BoardConfig {
            namespace: Arc::from("microgrid"),
            role: BoardRole::Primary,
            ..Default::default()
        },
    )
    .await?;

    let state = Arc::new(Mutex::new(VizState { exactly_once_ok: true, ..Default::default() }));
    // Every id ever claimed — the exactly-once invariant is "no id appears twice".
    let claimed_ids: Arc<Mutex<HashSet<u64>>> = Arc::new(Mutex::new(HashSet::new()));

    // ── HTTP dashboard — start first so the browser can connect immediately. ──
    {
        let st = state.clone();
        tokio::spawn(async move { serve_http(st).await });
    }
    eprintln!("microgrid: blackboard primary on 127.0.0.1:{port} · dashboard :{HTTP_PORT}");

    let surplus = Predicate::new().eq("kind", "surplus");

    // ── Inverter: posts a fresh surplus fact onto feeder 4 a few times a second. ──
    // Cycles between a BURST phase (posts faster than the two executors drain, so a
    // visible pool builds up) and a LULL phase (posts slowly, letting them catch up).
    {
        let (bb, st) = (Arc::clone(&bb), state.clone());
        tokio::spawn(async move {
            let mut rng = 0x1234_5678_9abc_def0u64;
            let mut phase_ticks = 0u32;
            let mut bursting = true;
            loop {
                let kwh = rand_range(&mut rng, 1.0, 5.0);
                let attrs = BTreeMap::from([
                    ("kind".to_string(), "surplus".to_string()),
                    ("feeder".to_string(), "4".to_string()),
                    ("kwh".to_string(), format!("{kwh:.1}")),
                ]);
                if bb.post(attrs, Bytes::from(format!("surplus-{kwh:.1}kwh"))).await.is_ok()
                    && let Ok(mut g) = st.lock()
                {
                    g.posted_total += 1;
                }
                // ~14 posts per phase, then flip. Burst outruns the drain; lull trails it.
                phase_ticks += 1;
                if phase_ticks >= 14 {
                    phase_ticks = 0;
                    bursting = !bursting;
                }
                let base = if bursting { 250.0 } else { POST_BASE_MS as f64 * 1.6 };
                let ms = (base + rand_range(&mut rng, -120.0, 120.0)).max(120.0) as u64;
                time::sleep(Duration::from_millis(ms)).await;
            }
        });
    }

    // ── Readers: forecaster + tariff both `rd` the pool (non-destructive, concurrent). ──
    for name in ["forecaster", "tariff"] {
        let (bb, st, pred) = (Arc::clone(&bb), state.clone(), surplus.clone());
        tokio::spawn(async move {
            let mut ticker = time::interval(Duration::from_millis(READ_MS));
            loop {
                ticker.tick().await;
                if let Ok(facts) = bb.read(&pred).await
                    && let Ok(mut g) = st.lock()
                {
                    match name {
                        "forecaster" => g.forecaster_sees = facts.len(),
                        _ => g.tariff_sees = facts.len(),
                    }
                }
            }
        });
    }

    // ── Executors: community-battery + ev-charger compete to `claim` + `ack` (exactly-once). ──
    for name in ["community-battery", "ev-charger"] {
        let (bb, st, ids, pred) = (Arc::clone(&bb), state.clone(), Arc::clone(&claimed_ids), surplus.clone());
        tokio::spawn(async move {
            let mut rng = name.bytes().fold(0xf00d_babe_u64, |a, b| a.wrapping_mul(31).wrapping_add(b as u64)) | 1;
            loop {
                match bb.claim(&pred).await {
                    Ok(Some(fact)) => {
                        let kwh = kwh_of(&fact.attributes);
                        // Exactly-once check: an id we've seen before is a violation.
                        let first_time = ids.lock().map(|mut s| s.insert(fact.id)).unwrap_or(true);
                        // ... charge the battery / EV against this finite surplus ...
                        let _ = bb.ack(fact.id).await;
                        if let Ok(mut g) = st.lock() {
                            if !first_time {
                                g.exactly_once_ok = false;
                            }
                            match name {
                                "community-battery" => g.battery_claimed += 1,
                                _ => g.charger_claimed += 1,
                            }
                            g.recent.push_front((fact.id, kwh, name.to_string()));
                            g.recent.truncate(RECENT_CAP);
                        }
                        // Charging takes a moment — lets the pool breathe.
                        let ms = rand_range(&mut rng, 250.0, 900.0) as u64;
                        time::sleep(Duration::from_millis(ms)).await;
                    }
                    // Pool empty right now — wait for the inverter to post more.
                    Ok(None) => time::sleep(Duration::from_millis(180)).await,
                    // Transient error — back off, keep the executor alive.
                    Err(_) => time::sleep(Duration::from_millis(300)).await,
                }
            }
        });
    }

    // ── Render loop: snapshot the live pool for the dashboard every RENDER_MS. ──
    let mut ticker = time::interval(Duration::from_millis(RENDER_MS));
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                // Non-destructive read of the currently-claimable pool.
                let pool: Vec<(u64, f64)> = bb
                    .read(&surplus)
                    .await
                    .map(|facts| facts.iter().map(|f| (f.id, kwh_of(&f.attributes))).collect())
                    .unwrap_or_default();
                if let Ok(mut g) = state.lock() {
                    g.generation += 1;
                    g.available = pool.len();
                    g.pool = pool;
                }
            }
            _ = signal::ctrl_c() => break,
        }
    }

    eprintln!("\nShutting down…");
    bb.shutdown().await;
    agent.shutdown().await;
    Ok(())
}
