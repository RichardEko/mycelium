//! Conway's Game of Life on a real 8×8 gossip mesh.
//!
//! 64 GossipAgents run in-process over TCP (ports 52000-52063).
//! Each agent owns one cell. Cell state lives in the KV store and
//! propagates epidemically. A System-scope tick signal drives each
//! agent to read its neighbours' state from its own gossiped view
//! and write its cell's next state — demonstrating eventual consistency.
//!
//! Run:
//!   cargo run --example conway
//!
//! Then open docs/conway.html and switch to "Live (Rust)" to visualise
//! the real agent mesh. Grid state is served as JSON on http://127.0.0.1:8090/state
//!
//! Toroidal edges — the grid wraps on all sides.

use bytes::Bytes;
use gossip_protocol::{GossipAgent, GossipConfig, NodeId, SignalScope};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::{signal, time};

/// Raise the process file-descriptor limit to at least `target`.
/// macOS defaults to 256; 64 agents need ~200+ sockets so we'd hit it immediately.
#[cfg(unix)]
fn raise_fd_limit(target: u64) {
    unsafe {
        let mut rl: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) != 0 { return; }
        if rl.rlim_cur >= target { return; }
        rl.rlim_cur = target.min(rl.rlim_max);
        libc::setrlimit(libc::RLIMIT_NOFILE, &rl);
    }
}
#[cfg(not(unix))]
fn raise_fd_limit(_: u64) {}

const GRID: usize = 8;
const BASE_PORT: u16 = 52000;
const HTTP_PORT: u16 = 8090;
const TICK_MS: u64 = 1_100;
const RENDER_OFFSET_MS: u64 = 400;
const SETTLE_MS: u64 = 5_000;
// Two-phase update: tick (read phase) → WRITE_DELAY_MS → write (write phase).
// All 64 agents read neighbours before any agent writes, preventing race-condition
// pattern corruption when tasks execute concurrently.
const TICK_KIND:       &str = "conway.tick";
const WRITE_KIND:      &str = "conway.write";
const WRITE_DELAY_MS:  u64  = 200;

const GLIDER: &[(usize, usize)] = &[(1, 0), (2, 1), (0, 2), (1, 2), (2, 2)];

// ── Shared grid snapshot served to the browser ────────────────────────
#[derive(Clone)]
struct GridSnapshot {
    generation: u64,
    cells:      [[bool; GRID]; GRID],
    kv_ages:    [[i64; GRID]; GRID], // generation when cell last changed, -1 = never
    live:       usize,
}

impl Default for GridSnapshot {
    fn default() -> Self {
        Self {
            generation: 0,
            cells:   [[false; GRID]; GRID],
            kv_ages: [[-1; GRID]; GRID],
            live:    0,
        }
    }
}

impl GridSnapshot {
    fn to_json(&self) -> String {
        let cells_str: Vec<String> = self.cells.iter()
            .map(|row| {
                let vals: Vec<&str> = row.iter().map(|&v| if v { "true" } else { "false" }).collect();
                format!("[{}]", vals.join(","))
            })
            .collect();

        let ages_str: Vec<String> = self.kv_ages.iter()
            .map(|row| {
                let vals: Vec<String> = row.iter().map(|v| v.to_string()).collect();
                format!("[{}]", vals.join(","))
            })
            .collect();

        format!(
            r#"{{"generation":{},"grid":{},"cells":[{}],"kv_ages":[{}],"live":{}}}"#,
            self.generation, GRID,
            cells_str.join(","),
            ages_str.join(","),
            self.live,
        )
    }
}

// ── Minimal HTTP server — no dependencies beyond tokio ────────────────
// Serves the HTML visualiser at / and the JSON state at /state.
// Serving the page from the same origin as the API avoids all CORS and
// file:// protocol restrictions.
async fn serve_http(snapshot: Arc<Mutex<GridSnapshot>>) {
    let listener = match TcpListener::bind(format!("127.0.0.1:{HTTP_PORT}")).await {
        Ok(l)  => l,
        Err(e) => { eprintln!("HTTP server failed to bind :{HTTP_PORT} — {e}"); return; }
    };
    eprintln!("╔══════════════════════════════════════════════════╗");
    eprintln!("║  Open in browser → http://127.0.0.1:{HTTP_PORT}       ║");
    eprintln!("║  Switch to  Live (Rust)  to see the real mesh    ║");
    eprintln!("╚══════════════════════════════════════════════════╝");

    loop {
        let Ok((mut stream, _)) = listener.accept().await else { continue };
        let snap = snapshot.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            let req = std::str::from_utf8(&buf[..n]).unwrap_or("");

            if req.starts_with("OPTIONS") {
                let _ = stream.write_all(
                    b"HTTP/1.1 204 No Content\r\n\
                      Access-Control-Allow-Origin: *\r\n\
                      Connection: close\r\n\r\n"
                ).await;
                return;
            }

            if req.contains("GET /state") {
                let json = snap.lock().unwrap().to_json();
                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                     Content-Type: application/json\r\n\
                     Access-Control-Allow-Origin: *\r\n\
                     Content-Length: {}\r\n\
                     Connection: close\r\n\r\n{}",
                    json.len(), json
                );
                let _ = stream.write_all(response.as_bytes()).await;
            } else {
                // Serve the visualiser — embedded at compile time from docs/conway.html
                let html = include_str!("../docs/conway.html");
                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                     Content-Type: text/html; charset=utf-8\r\n\
                     Content-Length: {}\r\n\
                     Connection: close\r\n\r\n{}",
                    html.len(), html
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
    }
}

// ── Conway helpers ────────────────────────────────────────────────────
fn port(x: usize, y: usize) -> u16 {
    BASE_PORT + (y * GRID + x) as u16
}

fn cell_key(x: usize, y: usize) -> String {
    format!("cell/{x}/{y}")
}

fn toroidal_neighbours(x: usize, y: usize) -> [(usize, usize); 8] {
    let g = GRID as i32;
    [(-1i32,-1i32),(0,-1),(1,-1),(-1,0),(1,0),(-1,1),(0,1),(1,1)].map(|(dx, dy)| {
        (((x as i32 + dx).rem_euclid(g)) as usize,
         ((y as i32 + dy).rem_euclid(g)) as usize)
    })
}

fn read_alive(agent: &GossipAgent, x: usize, y: usize) -> bool {
    agent.get(&cell_key(x, y))
        .map(|b| b.first() == Some(&1))
        .unwrap_or(false)
}

fn render_terminal(viewer: &GossipAgent, gen: u64, live: usize) {
    print!("\x1b[H");
    println!("  Conway's Life — 8×8 gossip mesh   \x1b[36mgen {gen}\x1b[0m   \x1b[33mlive {live:3}\x1b[0m          ");
    println!("  64 TCP agents · KV epidemic propagation · http://127.0.0.1:{HTTP_PORT}/state\n");
    for y in 0..GRID {
        print!("  ");
        for x in 0..GRID {
            if read_alive(viewer, x, y) {
                print!("\x1b[32m██\x1b[0m");
            } else {
                print!("\x1b[38;5;235m░░\x1b[0m");
            }
        }
        println!();
    }
    println!("\n  ctrl-c to exit                ");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 64 agents × ~3 sockets each comfortably exceeds macOS's default 256 fd limit.
    raise_fd_limit(4096);

    // ── Build agents ──────────────────────────────────────────────────
    let seed_port = port(0, 0);
    let seed_id = NodeId::new("127.0.0.1", seed_port)?;

    let mut seed_cfg = GossipConfig::default();
    seed_cfg.bind_address = "127.0.0.1".to_string();
    seed_cfg.bind_port = seed_port;
    let seed = Arc::new(GossipAgent::new(seed_id.clone(), seed_cfg));

    let mut agents: Vec<Arc<GossipAgent>> = Vec::with_capacity(GRID * GRID);
    for y in 0..GRID {
        for x in 0..GRID {
            if x == 0 && y == 0 {
                agents.push(seed.clone());
                continue;
            }
            let p = port(x, y);
            let nid = NodeId::new("127.0.0.1", p)?;
            let mut cfg = GossipConfig::default();
            cfg.bind_address = "127.0.0.1".to_string();
            cfg.bind_port = p;
            cfg.bootstrap_peers = vec![seed_id.clone()];
            agents.push(Arc::new(GossipAgent::new(nid, cfg)));
        }
    }

    // ── Start ─────────────────────────────────────────────────────────
    eprintln!("Starting {} gossip agents on 127.0.0.1:{}-{}…",
        agents.len(), BASE_PORT, BASE_PORT + (GRID * GRID) as u16 - 1);
    for a in &agents {
        a.start().await?;
    }

    // ── Initial state ─────────────────────────────────────────────────
    for y in 0..GRID {
        for x in 0..GRID {
            let alive = GLIDER.contains(&(x, y));
            let _ = agents[y * GRID + x].set(cell_key(x, y), Bytes::copy_from_slice(&[alive as u8]));
        }
    }

    // ── HTTP viewer — start before settle so the browser can connect immediately
    let snapshot: Arc<Mutex<GridSnapshot>> = Arc::new(Mutex::new(GridSnapshot::default()));
    let snap_for_server = snapshot.clone();
    tokio::spawn(async move { serve_http(snap_for_server).await });

    // ── Settle ────────────────────────────────────────────────────────
    eprintln!("Mesh settling ({SETTLE_MS}ms)…");
    time::sleep(Duration::from_millis(SETTLE_MS)).await;

    // ── Per-cell two-phase handlers ───────────────────────────────────
    // Phase 1 (conway.tick): every agent reads its neighbours' current KV state
    //   and stores the computed next value in a local AtomicU8 — no KV writes yet.
    // Phase 2 (conway.write, WRITE_DELAY_MS later): every agent commits its stored
    //   next value to KV, which then gossips to peers.
    // Separating the phases ensures all reads happen against the previous generation.
    let next_states: Vec<Arc<AtomicU8>> = (0..GRID * GRID)
        .map(|_| Arc::new(AtomicU8::new(0)))
        .collect();

    for y in 0..GRID {
        for x in 0..GRID {
            let agent = agents[y * GRID + x].clone();
            let nbs   = toroidal_neighbours(x, y);
            let next  = next_states[y * GRID + x].clone();
            let mut rx = agent.signal_rx(TICK_KIND);
            tokio::spawn(async move {
                while rx.recv().await.is_some() {
                    let live_count = nbs.iter()
                        .filter(|(nx, ny)| read_alive(&agent, *nx, *ny))
                        .count();
                    let was_alive = read_alive(&agent, x, y);
                    let next_alive = matches!(
                        (was_alive, live_count),
                        (true, 2) | (true, 3) | (false, 3)
                    );
                    next.store(next_alive as u8, Ordering::Release);
                }
            });
        }
    }

    for y in 0..GRID {
        for x in 0..GRID {
            let agent = agents[y * GRID + x].clone();
            let key   = cell_key(x, y);
            let next  = next_states[y * GRID + x].clone();
            let mut rx = agent.signal_rx(WRITE_KIND);
            tokio::spawn(async move {
                while rx.recv().await.is_some() {
                    let v = next.load(Ordering::Acquire);
                    let _ = agent.set(key.clone(), Bytes::copy_from_slice(&[v]));
                }
            });
        }
    }

    // ── Tick driver ───────────────────────────────────────────────────
    let tick_agent = agents[0].clone();
    tokio::spawn(async move {
        let mut ticker = time::interval(Duration::from_millis(TICK_MS));
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let _ = tick_agent.emit(TICK_KIND,  SignalScope::System, Bytes::new());
            time::sleep(Duration::from_millis(WRITE_DELAY_MS)).await;
            let _ = tick_agent.emit(WRITE_KIND, SignalScope::System, Bytes::new());
        }
    });

    // ── Render + snapshot loop ────────────────────────────────────────
    // Reads agent(0,0)'s KV view — after settling it holds the full grid.
    // Tracks kv_ages by observing state changes between render frames.
    print!("\x1b[2J");
    let viewer = agents[0].clone();
    let mut gen = 0u64;
    let mut kv_ages = [[-1i64; GRID]; GRID];
    let mut prev_alive = [[false; GRID]; GRID];

    time::sleep(Duration::from_millis(RENDER_OFFSET_MS)).await;
    let mut render_ticker = time::interval(Duration::from_millis(TICK_MS));
    render_ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = render_ticker.tick() => {
                let mut snap_cells = [[false; GRID]; GRID];
                let mut live = 0usize;

                for y in 0..GRID {
                    for x in 0..GRID {
                        let alive = read_alive(&viewer, x, y);
                        snap_cells[y][x] = alive;
                        if alive { live += 1; }
                        // Track when each cell last changed (drives KV freshness view)
                        if alive != prev_alive[y][x] {
                            kv_ages[y][x] = gen as i64;
                            prev_alive[y][x] = alive;
                        }
                    }
                }

                // Update HTTP snapshot
                {
                    let mut s = snapshot.lock().unwrap();
                    s.generation = gen;
                    s.cells      = snap_cells;
                    s.kv_ages    = kv_ages;
                    s.live       = live;
                }

                render_terminal(&viewer, gen, live);
                gen += 1;
            }
            _ = signal::ctrl_c() => break,
        }
    }

    // ── Shutdown ──────────────────────────────────────────────────────
    eprintln!("\nShutting down…");
    for a in &agents {
        a.shutdown().await;
    }
    Ok(())
}
