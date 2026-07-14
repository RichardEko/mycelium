//! Conway's Game of Life on a real 16×16 gossip mesh.
//!
//! Guide chapter: docs/guide/01-gossip-kv.md
//!
//! 256 GossipAgents run in-process over TCP (ports 52000-52255).
//! Each agent owns one cell. Cell state lives in the KV store and
//! propagates epidemically. A System-scope epoch signal drives each
//! agent to read its neighbours' state from its own gossiped view
//! and write its cell's next state — demonstrating eventual consistency.
//!
//! Coordination: each agent uses a local tokio::time::interval — the
//! standard pattern in distributed systems (Dynamo, Cassandra, Riak all
//! do this). The gossip *epoch signal* was elegant but unreliable: the
//! boundary layer's opacity mechanism sheds signals under load, and
//! eventual consistency cannot guarantee all 256 agents see consistent
//! neighbour state within a single generation boundary. Local timers
//! solve the coordination problem; gossip solves the state-propagation
//! problem. These are different concerns.
//!
//! Run:
//!   cargo run --example conway
//!
//! Then open http://127.0.0.1:8090 and switch to "Live (Rust)".
//!
//! Toroidal edges — the grid wraps on all sides.

use bytes::Bytes;
use mycelium::{GossipAgent, GossipConfig, NodeId};
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

const GRID: usize = 16;
// Below the OS ephemeral port range (macOS starts at 49152, Linux at ~32768) so the 256 fixed
// binds never race a transient outbound connection — and clear of apps that squat ports in the
// 52xxx block (e.g. Roon's UDP 52240, which used to collide with this demo's old 52000 base).
const BASE_PORT: u16 = 40000;
const HTTP_PORT: u16 = 8090;
/// The viewer agent also exposes the mycelium gateway here, so the Ops Console can watch the
/// 256-node mesh (`/stats` · `/gateway/fleet` · `/gateway/diagnose`).
const OPS_PORT: u16 = 9090;
const TICK_MS: u64 = 300;
const RENDER_OFFSET_MS: u64 = 180;
const SETTLE_MS: u64 = 3_000;
// Two-phase update: read phase → WRITE_DELAY_MS → write phase.
// All 256 agents read neighbours before any agent writes. Timer jitter on a
// single Tokio runtime is <1ms, well within the 100ms write delay.
const WRITE_DELAY_MS: u64 = 100;

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
    eprintln!("║  Ops Console     → http://127.0.0.1:{OPS_PORT}       ║");
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
                // Inject the "⚙ Ops Console" back-link, pre-targeted at the viewer node's gateway.
                // Empty (button hidden) unless built `--features gateway`, since the console reads
                // this node through that gateway.
                let console_link = if cfg!(feature = "gateway") {
                    format!(
                        "<a class=\"opsbtn\" href=\"http://127.0.0.1:8099/?target=127.0.0.1:{OPS_PORT}\" \
                         title=\"Open this cluster in the Mycelium Ops Console\">⚙ Ops Console</a>"
                    )
                } else {
                    String::new()
                };
                let html = include_str!("conway.html").replace("__OPS_CONSOLE_LINK__", &console_link);
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
    agent.kv().get(&cell_key(x, y))
        .map(|b| b.first() == Some(&1))
        .unwrap_or(false)
}

fn render_terminal(viewer: &GossipAgent, epoch: u64, live: usize) {
    print!("\x1b[H");
    println!("  Conway's Life — {GRID}×{GRID} gossip mesh   \x1b[36mgen {epoch}\x1b[0m   \x1b[33mlive {live:3}\x1b[0m          ");
    println!("  {} TCP agents · KV epidemic propagation · visualiser http://127.0.0.1:{HTTP_PORT}/  (JSON: /state)\n", GRID*GRID);
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
    // 256 agents × ~3 sockets each comfortably exceeds macOS's default 256 fd limit.
    raise_fd_limit(4096);

    // ── Build agents ──────────────────────────────────────────────────
    // Grid bootstrap topology: each agent's bootstrap_peers = 4 toroidal spatial
    // neighbours. bootstrap_peers are always in the gossip shard's forwarding
    // targets regardless of health-check state, so signals propagate through the
    // grid mesh from the first tick. TTL=20 covers the 16-hop diameter.
    let mut agents: Vec<Arc<GossipAgent>> = Vec::with_capacity(GRID * GRID);
    for y in 0..GRID {
        for x in 0..GRID {
            let p   = port(x, y);
            let nid = NodeId::new("127.0.0.1", p)?;
            let mut cfg = GossipConfig::default();
            cfg.bind_address             = "127.0.0.1".to_string();
            cfg.bind_port                = p;
            // Always name the cluster (never null on /stats); overridable via GOSSIP_CLUSTER_NAME.
            cfg.cluster_name             = Some(std::env::var("GOSSIP_CLUSTER_NAME").unwrap_or_else(|_| "conway".to_string()));
            // The viewer agent (0,0) also serves the mycelium gateway for the Ops Console.
            if p == BASE_PORT { cfg.http_port = Some(OPS_PORT); }
            // In-process demo: 256 localhost agents that never fail, so SWIM's failure detector —
            // and its per-agent UDP bind on the same port — is pointless here. Disabling it drops
            // the UDP bind that otherwise races any app already holding a port in the range.
            cfg.swim_failure_detector    = false;
            cfg.default_ttl              = 20;
            cfg.reconnect_backoff_secs   = 1;
            // Each cell writes once per generation, then epidemic-forwards through the mesh.
            // 256 agents × epidemic-forwarding ≈ 192 msgs/peer-writer in a burst; the
            // default writer_channel_depth (1024) has ample headroom.
            // One shard per agent instead of the default min(CPU, 16). In debug mode the
            // default produces 256 × 16 = 4096 tasks; one shard cuts that to 256 tasks with
            // no measurable latency impact at TICK_MS = 300ms.
            cfg.gossip_shards            = 1;
            // 4-connected toroidal neighbours
            cfg.bootstrap_peers = vec![
                NodeId::new("127.0.0.1", port((x + GRID - 1) % GRID, y))?,  // left
                NodeId::new("127.0.0.1", port((x + 1) % GRID,        y))?,  // right
                NodeId::new("127.0.0.1", port(x, (y + GRID - 1) % GRID))?,  // up
                NodeId::new("127.0.0.1", port(x, (y + 1) % GRID))?,         // down
            ];
            // Fix gossip topology at the 4 bootstrap peers.
            // max_forwarding_peers: limits gossip fan-out to 4 neighbours.
            // max_peers: prevents peer-table growth beyond 4 entries — without this,
            //   piggybacked Ping lists propagate all 256 NodeIds to every agent, the
            //   health monitor opens persistent connections to all of them, and the
            //   process accumulates ~32 000 file descriptors (256×128) which starves
            //   the tokio runtime and makes the HTTP server unresponsive.
            cfg.max_forwarding_peers     = cfg.bootstrap_peers.len();
            cfg.max_peers                = cfg.bootstrap_peers.len();
            // Cap startup jitter to 100ms. Default is up to health_check_interval_secs×500ms
            // (5s) — acceptable for production but makes the demo settle slowly when all 256
            // agents start simultaneously. 100ms spreads first pings without a thundering herd.
            cfg.health_check_max_jitter_ms = 100;
            agents.push(Arc::new(GossipAgent::new(nid, cfg)));
        }
    }
    // ── Start ─────────────────────────────────────────────────────────
    eprintln!("Starting {} gossip agents on 127.0.0.1:{}-{}…",
        agents.len(), BASE_PORT, BASE_PORT + (GRID * GRID) as u16 - 1);
    for a in &agents {
        a.start().await?;
    }
    #[cfg(feature = "gateway")]
    {
        // Self-advertise the viewer agent's browser UI so the Ops Console can offer a live
        // "↗ visualiser" click-through (the `ui/viz` + `ui/label` KV convention). agents[0] is
        // the port==BASE_PORT node — the one that got `http_port = Some(OPS_PORT)`. The reverse
        // link (the "⚙ Ops Console" button on the page) is injected into the HTML in serve_http.
        let _ = agents[0].kv().set("ui/viz", format!("http://127.0.0.1:{HTTP_PORT}/"));
        let _ = agents[0].kv().set("ui/label", "Conway's Life".to_string());
    }

    // ── Initial state ─────────────────────────────────────────────────
    // Each agent writes its OWN cell — distributed writes.
    // Centralised writes (one agent writing all 256 keys) flood its 4 peer-writer
    // channels (depth 64) with 256 messages each, silently dropping 192/256 (75%).
    // Distributed writes produce exactly 1 message per peer-writer: no drops.
    for y in 0..GRID {
        for x in 0..GRID {
            let alive = GLIDER.contains(&(x, y));
            let _ = agents[y * GRID + x].kv().set(cell_key(x, y), Bytes::copy_from_slice(&[alive as u8]));
        }
    }

    // ── HTTP viewer — start before settle so the browser can connect immediately
    let snapshot: Arc<Mutex<GridSnapshot>> = Arc::new(Mutex::new(GridSnapshot::default()));
    let snap_for_server = snapshot.clone();
    tokio::spawn(async move { serve_http(snap_for_server).await });

    // ── Settle ────────────────────────────────────────────────────────
    eprintln!("Mesh settling ({SETTLE_MS}ms)…");
    time::sleep(Duration::from_millis(SETTLE_MS)).await;

    // ── Per-cell epoch tasks ───────────────────────────────────────────
    // Each agent runs its own local timer — the standard pattern in distributed
    // systems. The gossip layer handles state propagation; the timer handles
    // local coordination. Two-phase separation (read → sleep → write) keeps
    // all reads in the same generation before any writes propagate.
    for y in 0..GRID {
        for x in 0..GRID {
            let agent = agents[y * GRID + x].clone();
            let nbs   = toroidal_neighbours(x, y);
            let key   = cell_key(x, y);
            tokio::spawn(async move {
                let mut ticker = time::interval(Duration::from_millis(TICK_MS));
                ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
                loop {
                    ticker.tick().await;
                    // Phase 1: read neighbours from gossiped KV view, compute next state
                    let live_count = nbs.iter()
                        .filter(|(nx, ny)| read_alive(&agent, *nx, *ny))
                        .count();
                    let was_alive = read_alive(&agent, x, y);
                    let next_alive = matches!(
                        (was_alive, live_count),
                        (true, 2) | (true, 3) | (false, 3)
                    );
                    // Phase 2: after delay, write — all reads complete well before any write
                    time::sleep(Duration::from_millis(WRITE_DELAY_MS)).await;
                    let _ = agent.kv().set(key.clone(), Bytes::copy_from_slice(&[next_alive as u8]));
                }
            });
        }
    }

    // ── Render + snapshot loop ────────────────────────────────────────
    // Reads agent(0,0)'s KV view — after settling it holds the full grid.
    // Tracks kv_ages by observing state changes between render frames.
    print!("\x1b[2J");
    let viewer = agents[0].clone();
    let mut epoch = 0u64;
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
                            kv_ages[y][x] = epoch as i64;
                            prev_alive[y][x] = alive;
                        }
                    }
                }

                // Update HTTP snapshot
                {
                    let mut s = snapshot.lock().unwrap();
                    s.generation = epoch;
                    s.cells      = snap_cells;
                    s.kv_ages    = kv_ages;
                    s.live       = live;
                }

                render_terminal(&viewer, epoch, live);
                epoch += 1;
            }
            _ = signal::ctrl_c() => break,
        }
    }

    // ── Shutdown ──────────────────────────────────────────────────────
    eprintln!("\nShutting down…");
    for a in &agents {
        a.shutdown().await;
    }

    // Confirm channel depth was adequate — non-zero means writer_channel_depth
    // should be raised for this cluster size / epoch rate.
    let stats = viewer.system_stats();
    if stats.dropped_frames > 0 {
        eprintln!("dropped_frames: {} — consider raising writer_channel_depth", stats.dropped_frames);
        for (peer, n) in viewer.peer_drop_counts() {
            eprintln!("  {peer}: {n} drops");
        }
    }

    Ok(())
}
