//! Conway's Game of Life: 256-agent gossip mesh + Metal/wgpu compute shader.
//!
//! Architecture:
//!   - 16×16 GossipAgents run over TCP (ports 52100-52355), one per grid tile.
//!   - A 512×512 Conway grid lives in a wgpu storage buffer on the GPU (Metal backend).
//!   - Each tick: a compute shader applies the Conway rule to all 262 144 cells in parallel.
//!   - Each agent reads the live-cell density of its 32×32 tile and writes it as a u8 to KV.
//!   - Density values propagate epidemically; any agent can answer "what fraction of tile (x,y)
//!     is alive?" from its gossiped view — demonstrating eventual-consistency over GPU state.
//!   - The HTTP server at :8091 serves the full 512×512 grid as a flat JSON byte array and
//!     the 16×16 mesh density view, viewable in the browser.
//!
//! Run:
//!   cargo run --example conway_gpu
//!
//! Then open http://127.0.0.1:8091

use bytes::Bytes;
use gossip_protocol::{GossipAgent, GossipConfig, NodeId};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::{signal, time};
use wgpu::util::DeviceExt;

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

// ── Dimensions ────────────────────────────────────────────────────────
const MESH:      usize = 16;           // gossip grid: MESH×MESH agents
const TILE:      usize = 32;           // GPU cells per agent tile
const FULL:      usize = MESH * TILE;  // 512: full GPU grid width/height
const BASE_PORT: u16   = 52100;
const HTTP_PORT: u16   = 8091;
const TICK_MS:   u64   = 100;          // GPU tick — fast because compute is free
const SETTLE_MS: u64   = 3_000;

fn port(x: usize, y: usize) -> u16 { BASE_PORT + (y * MESH + x) as u16 }
fn tile_key(x: usize, y: usize) -> String { format!("tile/{x}/{y}") }

// ── Shared state served to browser ────────────────────────────────────
struct SharedState {
    generation: u64,
    // 512×512 flat grid, row-major, 0=dead 1=alive
    grid:  Vec<u8>,
    // 16×16 density view (0-100 = percent alive in each tile)
    density: [[u8; MESH]; MESH],
}

impl SharedState {
    fn new() -> Self {
        Self {
            generation: 0,
            grid: vec![0u8; FULL * FULL],
            density: [[0u8; MESH]; MESH],
        }
    }

    fn to_json(&self) -> String {
        // Grid as a flat JSON array is large (~260 KB); use a compact run-length encoding.
        // Format: {"generation":N,"full":512,"mesh":16,"tile":32,"rle":[count,val,...],"density":[[...],...]}
        let mut rle: Vec<u32> = Vec::with_capacity(512);
        let mut i = 0;
        let data = &self.grid;
        while i < data.len() {
            let val = data[i];
            let mut run = 1u32;
            while i + (run as usize) < data.len() && data[i + (run as usize)] == val && run < 32767 {
                run += 1;
            }
            rle.push(run);
            rle.push(val as u32);
            i += run as usize;
        }
        let rle_str: Vec<String> = rle.iter().map(|v| v.to_string()).collect();

        let density_str: Vec<String> = self.density.iter().map(|row| {
            let vals: Vec<String> = row.iter().map(|v| v.to_string()).collect();
            format!("[{}]", vals.join(","))
        }).collect();

        format!(
            r#"{{"generation":{},"full":{},"mesh":{},"tile":{},"rle":[{}],"density":[{}]}}"#,
            self.generation, FULL, MESH, TILE,
            rle_str.join(","),
            density_str.join(","),
        )
    }
}

// ── WGSL compute shader — Conway's rule ───────────────────────────────
// One workgroup invocation per cell. Each reads its 8 neighbours (toroidal),
// counts live neighbours, applies the rule, writes next state.
const SHADER: &str = r#"
@group(0) @binding(0) var<storage, read>       cur:  array<u32>;
@group(0) @binding(1) var<storage, read_write>  nxt:  array<u32>;
@group(0) @binding(2) var<uniform>              dims: vec2<u32>;

fn idx(x: u32, y: u32) -> u32 { return y * dims.x + x; }

fn wrap(v: i32, n: u32) -> u32 {
    let m = i32(n);
    return u32(((v % m) + m) % m);
}

@compute @workgroup_size(16, 16)
fn main(@builtin(global_invocation_id) gid: vec3<u32>) {
    let x = gid.x; let y = gid.y;
    if (x >= dims.x || y >= dims.y) { return; }

    var live: u32 = 0u;
    for (var dy: i32 = -1; dy <= 1; dy++) {
        for (var dx: i32 = -1; dx <= 1; dx++) {
            if (dx == 0 && dy == 0) { continue; }
            let nx = wrap(i32(x) + dx, dims.x);
            let ny = wrap(i32(y) + dy, dims.y);
            live += cur[idx(nx, ny)];
        }
    }

    let was = cur[idx(x, y)];
    // Conway: alive with 2-3 neighbours survives; dead with 3 is born
    let born    = (1u - was) * u32(live == 3u);
    let survive = was        * u32(live == 2u || live == 3u);
    nxt[idx(x, y)] = born + survive;
}
"#;

// ── GPU state ─────────────────────────────────────────────────────────
struct GpuConway {
    device:      wgpu::Device,
    queue:       wgpu::Queue,
    pipeline:    wgpu::ComputePipeline,
    bind_groups: [wgpu::BindGroup; 2],
    bufs:        [wgpu::Buffer; 2],
    readback:    wgpu::Buffer,
    _dims_buf:   wgpu::Buffer,
    phase:       usize,          // which buf is "current"
    size:        u64,
}

impl GpuConway {
    async fn new(initial: &[u8]) -> Self {
        let instance = wgpu::Instance::new(wgpu::InstanceDescriptor {
            backends: wgpu::Backends::METAL | wgpu::Backends::VULKAN | wgpu::Backends::DX12,
            ..Default::default()
        });
        let adapter = instance
            .request_adapter(&wgpu::RequestAdapterOptions {
                power_preference: wgpu::PowerPreference::HighPerformance,
                ..Default::default()
            })
            .await
            .expect("no GPU adapter found");

        eprintln!("GPU: {}", adapter.get_info().name);

        let (device, queue) = adapter
            .request_device(&wgpu::DeviceDescriptor {
                label: None,
                required_features: wgpu::Features::empty(),
                required_limits: wgpu::Limits::default(),
                memory_hints: Default::default(),
            }, None)
            .await
            .expect("device creation failed");

        let n = (FULL * FULL) as u64;
        let size = n * 4; // u32 per cell

        // Upload initial state (u8 → u32)
        let init_u32: Vec<u32> = initial.iter().map(|&v| v as u32).collect();
        let init_bytes = bytemuck::cast_slice::<u32, u8>(&init_u32);

        let buf0 = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("grid0"),
            contents: init_bytes,
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
        });
        let buf1 = device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("grid1"),
            size,
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
            mapped_at_creation: false,
        });
        let readback = device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("readback"),
            size,
            usage: wgpu::BufferUsages::COPY_DST | wgpu::BufferUsages::MAP_READ,
            mapped_at_creation: false,
        });

        let dims_data: [u32; 2] = [FULL as u32, FULL as u32];
        let dims_buf = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("dims"),
            contents: bytemuck::cast_slice(&dims_data),
            usage: wgpu::BufferUsages::UNIFORM,
        });

        let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("conway"),
            source: wgpu::ShaderSource::Wgsl(SHADER.into()),
        });

        let bgl = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: None,
            entries: &[
                wgpu::BindGroupLayoutEntry {
                    binding: 0, visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: true },
                        has_dynamic_offset: false, min_binding_size: None,
                    }, count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 1, visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: false },
                        has_dynamic_offset: false, min_binding_size: None,
                    }, count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 2, visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Uniform,
                        has_dynamic_offset: false, min_binding_size: None,
                    }, count: None,
                },
            ],
        });

        let make_bg = |cur: &wgpu::Buffer, nxt: &wgpu::Buffer| {
            device.create_bind_group(&wgpu::BindGroupDescriptor {
                label: None,
                layout: &bgl,
                entries: &[
                    wgpu::BindGroupEntry { binding: 0, resource: cur.as_entire_binding() },
                    wgpu::BindGroupEntry { binding: 1, resource: nxt.as_entire_binding() },
                    wgpu::BindGroupEntry { binding: 2, resource: dims_buf.as_entire_binding() },
                ],
            })
        };
        let bg0 = make_bg(&buf0, &buf1);
        let bg1 = make_bg(&buf1, &buf0);

        let layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: None,
            bind_group_layouts: &[&bgl],
            push_constant_ranges: &[],
        });
        let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some("conway"),
            layout: Some(&layout),
            module: &shader,
            entry_point: "main",
            compilation_options: Default::default(),
            cache: None,
        });

        Self {
            device, queue, pipeline,
            bind_groups: [bg0, bg1],
            bufs: [buf0, buf1],
            readback,
            _dims_buf: dims_buf,
            phase: 0,
            size,
        }
    }

    // Step one generation on GPU, read back results.
    async fn step(&mut self) -> Vec<u8> {
        let p = self.phase;
        let tiles = ((FULL as u32) + 15) / 16;

        let mut enc = self.device.create_command_encoder(&Default::default());
        {
            let mut pass = enc.begin_compute_pass(&Default::default());
            pass.set_pipeline(&self.pipeline);
            pass.set_bind_group(0, &self.bind_groups[p], &[]);
            pass.dispatch_workgroups(tiles, tiles, 1);
        }
        // Copy next buffer → readback
        enc.copy_buffer_to_buffer(&self.bufs[1 - p], 0, &self.readback, 0, self.size);
        self.queue.submit(std::iter::once(enc.finish()));

        // Map and read
        let slice = self.readback.slice(..);
        let (tx, rx) = tokio::sync::oneshot::channel();
        slice.map_async(wgpu::MapMode::Read, move |r| { let _ = tx.send(r); });
        self.device.poll(wgpu::Maintain::Wait);
        rx.await.unwrap().unwrap();

        let data = {
            let view = slice.get_mapped_range();
            let u32s: &[u32] = bytemuck::cast_slice(&view);
            u32s.iter().map(|&v| v as u8).collect::<Vec<u8>>()
        };
        self.readback.unmap();

        self.phase = 1 - p;
        data
    }
}

// ── HTTP server ────────────────────────────────────────────────────────
async fn serve_http(state: Arc<Mutex<SharedState>>) {
    let listener = match TcpListener::bind(format!("127.0.0.1:{HTTP_PORT}")).await {
        Ok(l)  => l,
        Err(e) => { eprintln!("HTTP bind :{HTTP_PORT} failed: {e}"); return; }
    };
    eprintln!("╔══════════════════════════════════════════════════╗");
    eprintln!("║  Open in browser → http://127.0.0.1:{HTTP_PORT}       ║");
    eprintln!("╚══════════════════════════════════════════════════╝");

    loop {
        let Ok((mut stream, _)) = listener.accept().await else { continue };
        let s = state.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            let req = std::str::from_utf8(&buf[..n]).unwrap_or("");

            if req.contains("GET /state") {
                let json = s.lock().unwrap().to_json();
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                     Access-Control-Allow-Origin: *\r\nContent-Length: {}\r\n\
                     Connection: close\r\n\r\n{}",
                    json.len(), json
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            } else {
                let html = include_str!("../docs/conway_gpu.html");
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                    html.len(), html
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            }
        });
    }
}

// ── Initial pattern: R-pentomino in the centre of each tile ──────────
fn initial_grid() -> Vec<u8> {
    let mut g = vec![0u8; FULL * FULL];
    // Place an R-pentomino near the centre of every other tile to get rich dynamics
    let rpento: &[(i32, i32)] = &[(1,0),(2,0),(0,1),(1,1),(1,2)];
    for ty in 0..MESH {
        for tx in 0..MESH {
            if (tx + ty) % 3 != 0 { continue; } // sparse seeding
            let ox = (tx * TILE + TILE / 2) as i32;
            let oy = (ty * TILE + TILE / 2) as i32;
            for &(dx, dy) in rpento {
                let cx = ((ox + dx) as usize).min(FULL - 1);
                let cy = ((oy + dy) as usize).min(FULL - 1);
                g[cy * FULL + cx] = 1;
            }
        }
    }
    g
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    raise_fd_limit(4096);

    // ── GPU init ──────────────────────────────────────────────────────
    let init = initial_grid();
    let mut gpu = GpuConway::new(&init).await;

    // ── Gossip mesh ───────────────────────────────────────────────────
    let seed_port = port(0, 0);
    let seed_id   = NodeId::new("127.0.0.1", seed_port)?;
    let mut seed_cfg = GossipConfig::default();
    seed_cfg.bind_address = "127.0.0.1".to_string();
    seed_cfg.bind_port    = seed_port;
    let seed = Arc::new(GossipAgent::new(seed_id.clone(), seed_cfg));

    let mut agents: Vec<Arc<GossipAgent>> = Vec::with_capacity(MESH * MESH);
    for y in 0..MESH {
        for x in 0..MESH {
            if x == 0 && y == 0 { agents.push(seed.clone()); continue; }
            let p   = port(x, y);
            let nid = NodeId::new("127.0.0.1", p)?;
            let mut cfg = GossipConfig::default();
            cfg.bind_address    = "127.0.0.1".to_string();
            cfg.bind_port       = p;
            cfg.bootstrap_peers            = vec![seed_id.clone()];
            cfg.health_check_max_jitter_ms = 100;
            agents.push(Arc::new(GossipAgent::new(nid, cfg)));
        }
    }

    eprintln!("Starting {} gossip agents (ports {}-{})…",
        agents.len(), BASE_PORT, BASE_PORT + (MESH * MESH) as u16 - 1);
    for a in &agents { a.start().await?; }

    // ── HTTP server ───────────────────────────────────────────────────
    let shared = Arc::new(Mutex::new(SharedState::new()));
    let shared_for_http = shared.clone();
    tokio::spawn(async move { serve_http(shared_for_http).await });

    // ── Settle ────────────────────────────────────────────────────────
    eprintln!("Mesh settling ({SETTLE_MS}ms)…");
    time::sleep(Duration::from_millis(SETTLE_MS)).await;

    // ── Tick driver: GPU step + density gossip ─────────────────────────
    eprintln!("Running. 512×512 GPU grid, {MESH}×{MESH} gossip mesh, {TICK_MS}ms/gen.");

    let mut gen = 0u64;
    let agents_arc = Arc::new(agents.clone());
    let shared_tick = shared.clone();

    let mut ticker = time::interval(Duration::from_millis(TICK_MS));
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                // 1. GPU step
                let grid = gpu.step().await;

                // 2. Compute per-tile densities and write to gossip KV
                let mut density = [[0u8; MESH]; MESH];
                for ty in 0..MESH {
                    for tx in 0..MESH {
                        let mut live = 0u32;
                        for dy in 0..TILE {
                            for dx in 0..TILE {
                                let gx = tx * TILE + dx;
                                let gy = ty * TILE + dy;
                                live += grid[gy * FULL + gx] as u32;
                            }
                        }
                        // density as 0–100 percent
                        let pct = (live * 100 / (TILE * TILE) as u32) as u8;
                        density[ty][tx] = pct;
                        let _ = agents_arc[ty * MESH + tx].set(
                            tile_key(tx, ty),
                            Bytes::copy_from_slice(&[pct]),
                        );
                    }
                }

                // 3. Update shared state for HTTP
                {
                    let mut s = shared_tick.lock().unwrap();
                    s.generation = gen;
                    s.grid       = grid;
                    s.density    = density;
                }
                gen += 1;

                if gen % 50 == 0 {
                    let total_live: u32 = density.iter().flat_map(|r| r.iter())
                        .map(|&d| d as u32).sum::<u32>() * (TILE * TILE / 100) as u32;
                    eprintln!("gen {gen:5}  ~{total_live} live cells");
                }
            }
            _ = signal::ctrl_c() => break,
        }
    }

    eprintln!("\nShutting down…");
    for a in &agents { a.shutdown().await; }

    let stats = agents[0].system_stats();
    if stats.dropped_frames > 0 {
        eprintln!("dropped_frames: {} — consider raising writer_channel_depth", stats.dropped_frames);
        for (peer, n) in agents[0].peer_drop_counts() {
            eprintln!("  {peer}: {n} drops");
        }
    }

    Ok(())
}
