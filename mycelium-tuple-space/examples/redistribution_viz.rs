//! Live visual showcase of the food-redistribution sorting pipeline — a browser dashboard.
//!
//! Companion to the batch `redistribution` example: same domain (a community
//! redistribution hub moving donated produce through `intake` → `sorted` →
//! `routed`, with NO dispatcher), but it runs **forever** and serves a live
//! animated canvas so you can *watch* the tuple space's defining property happen:
//!
//! - **Single-copy competitive `take`** — two sorters race on `intake`, two
//!   routers race on `sorted`. Each queued item is handed to exactly ONE worker
//!   (the Linda `in` primitive). Workers pull; they are never pushed to. The
//!   queue depth is the only signal — add or remove a worker and throughput
//!   changes with no configuration.
//! - **Atomic stage advance (`complete`)** — acking an item on one stage and
//!   posting its successor on the next is ONE WAL record. There is no crash
//!   window between stages, so every donation advances at-most-once per stage
//!   and the pipeline delivers each one exactly once. The dashboard asserts no
//!   id is delivered twice and shows a green "exactly-once" badge (red if ever
//!   violated).
//!
//! A loading dock posts fresh `donation` items onto `intake` on a burst/lull
//! cycle, so a visible backlog builds up then drains. A dispatch collector
//! consumes the terminal `routed` stage, delivering each donation exactly once.
//!
//! Run:
//!   cargo run -p mycelium-tuple-space --example redistribution_viz
//!
//! Then open http://127.0.0.1:8093/ — no external dependencies, works offline. Ctrl-C to exit.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use mycelium::{GossipAgent, GossipConfig, NodeId};
use mycelium_tuple_space::{TupleConfig, TupleRole, TupleSpace};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::{signal, time};

const HTTP_PORT: u16 = 8093;
/// The mycelium gateway (for the Ops Console) — served only when built `--features gateway`
/// (this companion crate has default-features off). `/stats` · `/gateway/fleet` · `/gateway/diagnose`.
const OPS_PORT: u16 = 9093;
const RENDER_MS: u64 = 400;
const RECENT_CAP: usize = 8;

// The three pipeline stages, in flow order.
const STAGES: [&str; 3] = ["intake", "sorted", "routed"];
// Workers, in display order: (name, lane). Four competitive workers + the
// terminal dispatch collector.
const WORKERS: [(&str, &str); 5] = [
    ("sorter-A", "sorter"),
    ("sorter-B", "sorter"),
    ("router-A", "router"),
    ("router-B", "router"),
    ("dispatch", "dispatch"),
];

// ── Shared dashboard snapshot served to the browser ────────────────────
#[derive(Clone, Default)]
struct WorkerStat {
    processed: u64,
    active: bool,
}

// One "move" for the recent-activity feed: an item advancing a lane.
#[derive(Clone)]
struct Move {
    id: u64,
    worker: String,
    from: String,
    to: String,
}

struct VizState {
    tick: u64,
    posted_total: u64,
    delivered_total: u64,
    depths: [u32; STAGES.len()],
    // Parallel to WORKERS.
    workers: [WorkerStat; WORKERS.len()],
    recent: VecDeque<Move>,
    exactly_once_ok: bool,
}

impl Default for VizState {
    fn default() -> Self {
        Self {
            tick: 0,
            posted_total: 0,
            delivered_total: 0,
            depths: [0; STAGES.len()],
            workers: Default::default(),
            recent: VecDeque::new(),
            exactly_once_ok: true,
        }
    }
}

impl VizState {
    fn worker_slot(&mut self, name: &str) -> &mut WorkerStat {
        let idx = WORKERS.iter().position(|(n, _)| *n == name).unwrap_or(0);
        &mut self.workers[idx]
    }

    fn push_move(&mut self, mv: Move) {
        self.recent.push_front(mv);
        self.recent.truncate(RECENT_CAP);
    }

    fn to_json(&self) -> String {
        let stages: Vec<String> = STAGES
            .iter()
            .zip(self.depths.iter())
            .map(|(name, depth)| format!(r#"{{"name":"{name}","depth":{depth}}}"#))
            .collect();
        let workers: Vec<String> = WORKERS
            .iter()
            .zip(self.workers.iter())
            .map(|((name, lane), w)| {
                format!(
                    r#"{{"name":"{name}","lane":"{lane}","processed":{},"active":{}}}"#,
                    w.processed, w.active
                )
            })
            .collect();
        let recent: Vec<String> = self
            .recent
            .iter()
            .map(|m| {
                format!(
                    r#"{{"id":{},"worker":"{}","from":"{}","to":"{}"}}"#,
                    m.id, m.worker, m.from, m.to
                )
            })
            .collect();
        format!(
            r#"{{"tick":{},"posted_total":{},"delivered_total":{},"stages":[{}],"workers":[{}],"recent":[{}],"exactly_once_ok":{}}}"#,
            self.tick,
            self.posted_total,
            self.delivered_total,
            stages.join(","),
            workers.join(","),
            recent.join(","),
            self.exactly_once_ok,
        )
    }
}

// ── Minimal HTTP server — no dependencies beyond tokio ────────────────
// Serves the HTML dashboard at / and the JSON state at /state, same origin
// so there are no CORS or file:// restrictions. Mirrors microgrid_viz.rs.
async fn serve_http(state: Arc<Mutex<VizState>>) {
    let listener = match TcpListener::bind(format!("127.0.0.1:{HTTP_PORT}")).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("HTTP server failed to bind :{HTTP_PORT} — {e}");
            return;
        }
    };
    eprintln!("╔══════════════════════════════════════════════════╗");
    eprintln!("║  Redistribution pipeline — live dashboard        ║");
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
                let html = include_str!("redistribution_viz.html");
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
fn rand_range(state: &mut u64, lo: f64, hi: f64) -> f64 {
    let r = (next_rand(state) >> 11) as f64 / (1u64 << 53) as f64;
    lo + r * (hi - lo)
}

// Parse the donation number from a `donation-{n}` payload — the exactly-once key.
fn donation_num(payload: &[u8]) -> u64 {
    std::str::from_utf8(payload)
        .ok()
        .and_then(|s| s.rsplit_once('-'))
        .and_then(|(_, n)| n.parse::<u64>().ok())
        .unwrap_or(u64::MAX)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── One agent, one shared tuple space: the whole hub pulls from it. ──
    let port = mycelium::test_util::alloc_port();
    let agent = Arc::new(GossipAgent::new(
        NodeId::new("127.0.0.1", port)?,
        GossipConfig { bind_port: port, http_port: Some(OPS_PORT), ..Default::default() },
    ));
    agent.start().await?;
    #[cfg(feature = "gateway")]
    eprintln!("║  Ops Console     → http://127.0.0.1:{OPS_PORT}/      ║");
    let ts = TupleSpace::new(
        Arc::clone(&agent),
        TupleConfig {
            namespace: Arc::from("redistribution"),
            role: TupleRole::Primary,
            ..Default::default()
        },
    )
    .await?;

    let state = Arc::new(Mutex::new(VizState::default()));
    // Every donation number ever delivered — exactly-once is "no number twice".
    let delivered_ids: Arc<Mutex<HashSet<u64>>> = Arc::new(Mutex::new(HashSet::new()));
    let running = Arc::new(AtomicBool::new(true));

    // ── HTTP dashboard — start first so the browser can connect immediately. ──
    {
        let st = state.clone();
        tokio::spawn(async move { serve_http(st).await });
    }
    eprintln!("redistribution: tuple-space primary on 127.0.0.1:{port} · dashboard :{HTTP_PORT}");

    // ── Loading dock: posts fresh donations onto `intake` on a burst/lull cycle. ──
    // A BURST posts faster than the sorters drain (a visible backlog builds); a
    // LULL trails them (the backlog drains). Runs forever.
    {
        let (ts, st, run) = (Arc::clone(&ts), state.clone(), Arc::clone(&running));
        tokio::spawn(async move {
            let mut rng = 0x1234_5678_9abc_def0u64;
            let mut n = 0u64;
            let mut phase_ticks = 0u32;
            let mut bursting = true;
            while run.load(Ordering::Relaxed) {
                if ts.put("intake", Bytes::from(format!("donation-{n}"))).await.is_ok() {
                    n += 1;
                    if let Ok(mut g) = st.lock() {
                        g.posted_total += 1;
                    }
                }
                // ~18 posts per phase, then flip. Burst outruns the drain; lull trails it.
                phase_ticks += 1;
                if phase_ticks >= 18 {
                    phase_ticks = 0;
                    bursting = !bursting;
                }
                let base = if bursting { 70.0 } else { 700.0 };
                let ms = (base + rand_range(&mut rng, -35.0, 35.0)).max(45.0) as u64;
                time::sleep(Duration::from_millis(ms)).await;
            }
        });
    }

    // ── Two sorters compete on `intake` → `sorted`; two routers on `sorted` → `routed`. ──
    // Single-copy competitive take + atomic complete: each item goes to exactly
    // one worker and advances in one WAL record.
    let poll = Duration::from_millis(300);
    for (worker, from, to) in [
        ("sorter-A", "intake", "sorted"),
        ("sorter-B", "intake", "sorted"),
        ("router-A", "sorted", "routed"),
        ("router-B", "sorted", "routed"),
    ] {
        let (ts, st, run) = (Arc::clone(&ts), state.clone(), Arc::clone(&running));
        tokio::spawn(async move {
            let mut rng = worker.bytes().fold(0xf00d_babe_u64, |a, b| {
                a.wrapping_mul(31).wrapping_add(b as u64)
            }) | 1;
            while run.load(Ordering::Relaxed) {
                match ts.take(from, poll).await {
                    Ok((id, payload)) => {
                        let num = donation_num(&payload);
                        if let Ok(mut g) = st.lock() {
                            g.worker_slot(worker).active = true;
                        }
                        // ... inspect / weigh / label the produce (takes a moment) ...
                        let work_ms = rand_range(&mut rng, 180.0, 420.0) as u64;
                        time::sleep(Duration::from_millis(work_ms)).await;
                        // Atomic advance: ack `id` on `from` AND enqueue on `to`.
                        if ts.complete(id, to, payload).await.is_ok()
                            && let Ok(mut g) = st.lock()
                        {
                            let w = g.worker_slot(worker);
                            w.processed += 1;
                            w.active = false;
                            g.push_move(Move {
                                id: num,
                                worker: worker.to_string(),
                                from: from.to_string(),
                                to: to.to_string(),
                            });
                        }
                    }
                    // Timeout: nothing queued right now — keep parking.
                    Err(_) => {
                        if let Ok(mut g) = st.lock() {
                            g.worker_slot(worker).active = false;
                        }
                    }
                }
            }
        });
    }

    // ── Dispatch collector: drains the terminal `routed` stage, delivering once. ──
    {
        let (ts, st, ids, run) = (
            Arc::clone(&ts),
            state.clone(),
            Arc::clone(&delivered_ids),
            Arc::clone(&running),
        );
        tokio::spawn(async move {
            let mut rng = 0xdead_beef_u64;
            while run.load(Ordering::Relaxed) {
                match ts.take("routed", poll).await {
                    Ok((id, payload)) => {
                        let num = donation_num(&payload);
                        if let Ok(mut g) = st.lock() {
                            g.worker_slot("dispatch").active = true;
                        }
                        // Exactly-once check: a number we've seen before is a violation.
                        let first_time = ids.lock().map(|mut s| s.insert(num)).unwrap_or(true);
                        // ... hand the crate to the delivery van ...
                        let work_ms = rand_range(&mut rng, 90.0, 240.0) as u64;
                        time::sleep(Duration::from_millis(work_ms)).await;
                        if ts.ack(id).await.is_ok()
                            && let Ok(mut g) = st.lock()
                        {
                            if !first_time {
                                g.exactly_once_ok = false;
                            }
                            g.delivered_total += 1;
                            let w = g.worker_slot("dispatch");
                            w.processed += 1;
                            w.active = false;
                            g.push_move(Move {
                                id: num,
                                worker: "dispatch".to_string(),
                                from: "routed".to_string(),
                                to: "delivered".to_string(),
                            });
                        }
                    }
                    Err(_) => {
                        if let Ok(mut g) = st.lock() {
                            g.worker_slot("dispatch").active = false;
                        }
                    }
                }
            }
        });
    }

    // ── Render loop: snapshot per-stage queue depth for the dashboard. ──
    let mut ticker = time::interval(Duration::from_millis(RENDER_MS));
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                // Per-stage queue depth is the only pipeline signal — poll it.
                let mut depths = [0u32; STAGES.len()];
                if let Ok(all) = ts.depth(None).await {
                    for d in all {
                        if let Some(i) = STAGES.iter().position(|s| **s == *d.stage) {
                            depths[i] = d.depth;
                        }
                    }
                }
                if let Ok(mut g) = state.lock() {
                    g.tick += 1;
                    g.depths = depths;
                }
            }
            _ = signal::ctrl_c() => break,
        }
    }

    eprintln!("\nShutting down…");
    running.store(false, Ordering::Relaxed);
    ts.shutdown().await;
    agent.shutdown().await;
    Ok(())
}
