//! MCP Mesh — Adaptive Broker-less AI Tool Routing
//!
//! An interactive playground for the Mycelium MCP bridge. Start with three
//! nodes, then add more from a tool palette, duplicate tools across nodes to
//! see load-balancing, kill providers to watch failover, and restart them to
//! see the mesh self-heal — all with zero reconfiguration.
//!
//! Initial topology (ports 55000–55003):
//!   node-0 · 55000 — weather, ping
//!   node-1 · 55001 — search, calculate
//!   node-2 · 55002 — translate, summarize
//!   node-3 · 55003 — calculate          ← duplicate; balancing visible immediately
//!
//! Run:
//!   cargo run --example mcp_mesh
//!
//! Then open http://127.0.0.1:8099

use bytes::Bytes;
use mycelium::{GossipAgent, GossipConfig, McpToolHandle, NodeId, signal_kind};
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, RwLock,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener, time};

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

const BASE_PORT: u16 = 55000;
const HTTP_PORT: u16 = 8099;
const SETTLE_MS: u64 = 2_500;
const AUTO_CALL_MS: u64 = 6_000;

const VALID_TOOLS: &[&str] = &[
    "weather", "ping", "search", "calculate", "translate", "summarize",
];

/// Initial topology — 4 nodes. node-1 and node-3 both provide "calculate"
/// so load-balancing arcs are visible from the first auto-cycle.
const INITIAL: &[&[&str]] = &[
    &["weather", "ping"],
    &["search", "calculate"],
    &["translate", "summarize"],
    &["calculate"],
];

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

// ── Node slot ─────────────────────────────────────────────────────────────────

struct NodeSlot {
    /// Stable identity — never reused after remove.
    idx:        usize,
    port:       u16,
    /// Which tools this node provides.
    tools:      Vec<&'static str>,
    alive:      Arc<AtomicBool>,
    call_count: Arc<AtomicU64>,
    agent:      Mutex<Option<Arc<GossipAgent>>>,
    handles:    Mutex<Vec<McpToolHandle>>,
}

// ── App state ─────────────────────────────────────────────────────────────────

struct AppState {
    /// Dynamic node list. Always locked briefly; never held across .await.
    nodes:               RwLock<Vec<Arc<NodeSlot>>>,
    /// Monotonically increasing node counter — stable idx for each slot.
    next_idx:            AtomicUsize,
    /// Next gossip port to allocate.
    next_port:           AtomicUsize,
    total_calls:         AtomicU64,
    call_errors:         AtomicU64,
    last_call_ms:        AtomicU64,
    last_call_tool:      Mutex<String>,
    last_call_result:    Mutex<String>,
    /// Array-position of the node that *issued* the last RPC (-1 = unknown).
    last_caller_pos:     Mutex<i64>,
    /// Array-position of the node that *handled* the last RPC (-1 = unknown).
    last_provider_pos:   Mutex<i64>,
    auto_call_idx:       AtomicU64,
}

// ── Tool handlers ─────────────────────────────────────────────────────────────

async fn handle_weather(args: serde_json::Value) -> Result<serde_json::Value, String> {
    let city = args["city"].as_str().unwrap_or("Unknown");
    let temp = 15 + (city.len() % 20) as i32;
    Ok(json!({ "city": city, "temp_c": temp,
                "condition": if temp > 20 { "sunny" } else if temp > 10 { "cloudy" } else { "rainy" } }))
}

async fn handle_ping(args: serde_json::Value) -> Result<serde_json::Value, String> {
    let target = args["host"].as_str().unwrap_or("localhost");
    Ok(json!({ "host": target, "latency_ms": 12, "status": "reachable" }))
}

async fn handle_search(args: serde_json::Value) -> Result<serde_json::Value, String> {
    let q = args["query"].as_str().unwrap_or("");
    Ok(json!({ "query": q, "results": [
        { "title": format!("{q} — Overview"), "url": "https://example.com/1" },
        { "title": format!("{q} — Deep dive"), "url": "https://example.com/2" },
    ]}))
}

async fn handle_calculate(args: serde_json::Value) -> Result<serde_json::Value, String> {
    let expr = args["expression"].as_str().unwrap_or("0");
    let result: f64 = (|| {
        let p: Vec<&str> = expr.split_whitespace().collect();
        if p.len() == 3 {
            let a: f64 = p[0].parse().ok()?;
            let b: f64 = p[2].parse().ok()?;
            match p[1] {
                "+" => Some(a + b), "-" => Some(a - b),
                "*" => Some(a * b), "/" => if b != 0.0 { Some(a / b) } else { None },
                _ => None,
            }
        } else { None }
    })().unwrap_or(f64::NAN);
    Ok(json!({ "expression": expr, "result": result }))
}

async fn handle_translate(args: serde_json::Value) -> Result<serde_json::Value, String> {
    let text = args["text"].as_str().unwrap_or("");
    let lang = args["target_language"].as_str().unwrap_or("es");
    Ok(json!({ "original": text, "translated": format!("[{lang}] {text}"), "language": lang }))
}

async fn handle_summarize(args: serde_json::Value) -> Result<serde_json::Value, String> {
    let text = args["text"].as_str().unwrap_or("");
    let wc   = text.split_whitespace().count();
    let summary = if wc > 8 {
        text.split_whitespace().take(7).collect::<Vec<_>>().join(" ") + "…"
    } else { text.to_string() };
    Ok(json!({ "summary": summary, "original_words": wc }))
}

// ── Agent / tool helpers ──────────────────────────────────────────────────────

fn build_agent(port: u16, peer_ports: &[u16]) -> Arc<GossipAgent> {
    let nid = NodeId::new("127.0.0.1", port).unwrap();
    let mut cfg = GossipConfig::default();
    cfg.bind_address               = "127.0.0.1".to_string();
    cfg.bind_port                  = port;
    cfg.default_ttl                = 20;
    cfg.reconnect_backoff_secs     = 1;
    cfg.gossip_shards              = 1;
    cfg.health_check_max_jitter_ms = 100;
    cfg.max_peers                  = 20;
    cfg.bootstrap_peers = peer_ports.iter()
        .map(|&p| NodeId::new("127.0.0.1", p).unwrap())
        .collect();
    Arc::new(GossipAgent::new(nid, cfg))
}

fn register_tools(agent: &Arc<GossipAgent>, tools: &[&'static str]) -> Vec<McpToolHandle> {
    let mut handles = Vec::new();
    for &tool in tools {
        let h = match tool {
            "weather"   => agent.register_mcp_tool("weather", json!({
                "type": "object", "description": "Current weather for a city",
                "properties": { "city": { "type": "string" } }, "required": ["city"],
            }), |a| async move { handle_weather(a).await }),
            "ping"      => agent.register_mcp_tool("ping", json!({
                "type": "object", "description": "Check host reachability",
                "properties": { "host": { "type": "string" } }, "required": ["host"],
            }), |a| async move { handle_ping(a).await }),
            "search"    => agent.register_mcp_tool("search", json!({
                "type": "object", "description": "Web search",
                "properties": { "query": { "type": "string" } }, "required": ["query"],
            }), |a| async move { handle_search(a).await }),
            "calculate" => agent.register_mcp_tool("calculate", json!({
                "type": "object", "description": "Evaluate a math expression",
                "properties": { "expression": { "type": "string" } }, "required": ["expression"],
            }), |a| async move { handle_calculate(a).await }),
            "translate" => agent.register_mcp_tool("translate", json!({
                "type": "object", "description": "Translate text",
                "properties": {
                    "text": { "type": "string" },
                    "target_language": { "type": "string" },
                }, "required": ["text", "target_language"],
            }), |a| async move { handle_translate(a).await }),
            "summarize" => agent.register_mcp_tool("summarize", json!({
                "type": "object", "description": "Summarize a passage",
                "properties": { "text": { "type": "string" } }, "required": ["text"],
            }), |a| async move { handle_summarize(a).await }),
            _ => continue,
        };
        handles.push(h);
    }
    handles
}

/// Parse `&str` tool names from a query-param value into static refs.
fn parse_tools(s: &str) -> Vec<&'static str> {
    let mut out = Vec::new();
    for part in s.split(',') {
        let part = part.trim();
        if let Some(&s) = VALID_TOOLS.iter().find(|&&v| v == part) {
            if !out.contains(&s) { out.push(s); }
        }
    }
    out
}

fn parse_query_param(req: &str, key: &str) -> Option<String> {
    let line  = req.lines().next()?;
    let needle = format!("{key}=");
    let pos   = line.find(&needle)?;
    let rest  = &line[pos + needle.len()..];
    let end   = rest.find([' ', '&', '\r', '\n']).unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

fn sample_args(tool: &str) -> serde_json::Value {
    match tool {
        "weather"   => json!({ "city": "Tokyo" }),
        "ping"      => json!({ "host": "8.8.8.8" }),
        "search"    => json!({ "query": "gossip protocol" }),
        "calculate" => json!({ "expression": "42 * 7" }),
        "translate" => json!({ "text": "Hello world", "target_language": "es" }),
        "summarize" => json!({ "text": "Mycelium is a broker-less gossip substrate for adaptive AI agent systems." }),
        _           => json!({}),
    }
}

// ── Call tool ─────────────────────────────────────────────────────────────────

/// Scan the KV mesh for a provider of `tool_name`, pick one at random (load
/// balancing when multiple providers exist), issue `rpc_call`, and write the
/// caller/provider array positions into `AppState` for arc animation.
async fn call_tool(
    app:       &AppState,
    tool_name: &str,
    args:      serde_json::Value,
) -> Result<serde_json::Value, String> {
    // Snapshot live agents. Pick caller randomly for a more interesting viz.
    let live: Vec<(u16, Arc<GossipAgent>)> = {
        let nodes = app.nodes.read().unwrap();
        nodes.iter()
            .filter(|s| s.alive.load(Ordering::Relaxed))
            .filter_map(|s| {
                let ag = s.agent.lock().unwrap().clone()?;
                Some((s.port, ag))
            })
            .collect()
    };
    if live.is_empty() { return Err("no live agents".to_string()); }

    // Round-robin caller selection (changes each call).
    let call_n = app.total_calls.load(Ordering::Relaxed) as usize;
    let (caller_port, caller) = live[call_n % live.len()].clone();

    // KV scan: all providers for this tool.
    let prefix  = format!("tools/{tool_name}/");
    let entries = caller.scan_prefix(&prefix);
    if entries.is_empty() {
        return Err(format!("tool not found: {tool_name}"));
    }

    // Random provider pick — uniform across available providers.
    let pick = (now_ms() as usize) % entries.len();
    let (key, _) = &entries[pick];
    let provider_str = key.trim_start_matches(&prefix);
    let provider: NodeId = provider_str.parse()
        .map_err(|e| format!("bad provider id '{provider_str}': {e}"))?;
    let provider_port: u16 = provider.to_socket_addr().port();

    // Record caller / provider positions for arc animation.
    {
        let nodes = app.nodes.read().unwrap();
        let caller_pos   = nodes.iter().position(|s| s.port == caller_port)   .map(|i| i as i64).unwrap_or(-1);
        let provider_pos = nodes.iter().position(|s| s.port == provider_port) .map(|i| i as i64).unwrap_or(-1);
        *app.last_caller_pos.lock().unwrap()   = caller_pos;
        *app.last_provider_pos.lock().unwrap() = provider_pos;
    }

    // Issue the RPC.
    let rpc_req = json!({
        "jsonrpc": "2.0", "id": 1,
        "method":  "tools/call",
        "params":  { "name": tool_name, "arguments": args },
    });
    let reply = caller
        .rpc_call(provider, signal_kind::MCP_INVOKE,
                  Bytes::from(rpc_req.to_string().into_bytes()),
                  Duration::from_secs(5))
        .await
        .map_err(|e| e.to_string())?;

    let resp: serde_json::Value = serde_json::from_slice(&reply).map_err(|e| e.to_string())?;
    if let Some(err) = resp.get("error") {
        return Err(err["message"].as_str().unwrap_or("tool error").to_string());
    }

    // Increment the provider node's call counter.
    {
        let nodes = app.nodes.read().unwrap();
        if let Some(slot) = nodes.iter().find(|s| s.port == provider_port) {
            slot.call_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    Ok(resp["result"].clone())
}

// ── State JSON ────────────────────────────────────────────────────────────────

fn state_json(app: &AppState) -> String {
    let nodes_snap: Vec<Arc<NodeSlot>> = app.nodes.read().unwrap().clone();

    // Scan live tools with provider array-positions.
    let mut tool_map: std::collections::BTreeMap<String, Vec<usize>> = Default::default();
    let scanner = nodes_snap.iter()
        .filter(|s| s.alive.load(Ordering::Relaxed))
        .find_map(|s| s.agent.lock().unwrap().clone());
    if let Some(a) = scanner {
        for (key, _) in a.scan_prefix("tools/") {
            let parts: Vec<&str> = key.splitn(3, '/').collect();
            if parts.len() < 3 { continue; }
            let name = parts[1].to_string();
            let port: u16 = parts[2].split(':').last()
                .and_then(|s| s.parse().ok()).unwrap_or(0);
            if let Some(pos) = nodes_snap.iter().position(|s| s.port == port) {
                tool_map.entry(name).or_default().push(pos);
            }
        }
    }

    let live_tools_json: Vec<String> = tool_map.iter().map(|(name, providers)| {
        let pstr: Vec<String> = providers.iter().map(|i| i.to_string()).collect();
        format!(r#"{{"name":"{}","providers":[{}]}}"#, name, pstr.join(","))
    }).collect();

    let nodes_json: Vec<String> = nodes_snap.iter().enumerate().map(|(pos, s)| {
        let tools_json: Vec<String> = s.tools.iter().map(|t| format!("\"{}\"", t)).collect();
        format!(
            r#"{{"idx":{},"pos":{},"port":{},"alive":{},"call_count":{},"tools":[{}]}}"#,
            s.idx, pos, s.port,
            s.alive.load(Ordering::Relaxed),
            s.call_count.load(Ordering::Relaxed),
            tools_json.join(","),
        )
    }).collect();

    let total       = app.total_calls.load(Ordering::Relaxed);
    let errors      = app.call_errors.load(Ordering::Relaxed);
    let last_ms     = app.last_call_ms.load(Ordering::Relaxed);
    let last_tool   = app.last_call_tool.lock().unwrap().clone();
    let last_result = app.last_call_result.lock().unwrap().clone();
    let caller_pos  = *app.last_caller_pos.lock().unwrap();
    let provider_pos = *app.last_provider_pos.lock().unwrap();

    format!(
        r#"{{"n":{},"total_calls":{},"call_errors":{},"last_call_ms":{},"last_tool":"{}","last_result":{},"last_caller_pos":{},"last_provider_pos":{},"nodes":[{}],"live_tools":[{}]}}"#,
        nodes_snap.len(), total, errors, last_ms,
        last_tool.replace('"', "\\\""),
        last_result,
        caller_pos, provider_pos,
        nodes_json.join(","),
        live_tools_json.join(","),
    )
}

// ── HTTP server ───────────────────────────────────────────────────────────────

async fn start_node_with_retry(port: u16, peer_ports: &[u16], label: &str) -> Option<Arc<GossipAgent>> {
    let agent = build_agent(port, peer_ports);
    for attempt in 0..5u32 {
        if attempt > 0 { time::sleep(Duration::from_millis(300)).await; }
        match agent.start().await {
            Ok(()) => return Some(agent),
            Err(e) => eprintln!("{label} attempt {attempt}: {e}"),
        }
    }
    eprintln!("{label}: failed after 5 attempts");
    None
}

async fn serve_http(app: Arc<AppState>) {
    let listener = TcpListener::bind(format!("127.0.0.1:{HTTP_PORT}")).await
        .expect("HTTP bind failed");
    eprintln!("╔══════════════════════════════════════════════════╗");
    eprintln!("║  Open → http://127.0.0.1:{HTTP_PORT}                  ║");
    eprintln!("╚══════════════════════════════════════════════════╝");
    loop {
        let Ok((mut stream, _)) = listener.accept().await else { continue };
        let app = app.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let n   = stream.read(&mut buf).await.unwrap_or(0);
            let req = std::str::from_utf8(&buf[..n]).unwrap_or("");

            // ── GET /state ─────────────────────────────────────────────────
            if req.starts_with("GET /state") {
                let body = state_json(&app);
                let _ = stream.write_all(format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                     Access-Control-Allow-Origin: *\r\nContent-Length: {}\r\n\
                     Connection: close\r\n\r\n{}", body.len(), body
                ).as_bytes()).await;
                return;
            }

            // ── POST /call?tool=NAME ────────────────────────────────────────
            if req.starts_with("POST /call") {
                let tool = parse_query_param(req, "tool").unwrap_or_default();
                if tool.is_empty() {
                    let _ = stream.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await;
                    return;
                }
                let result = call_tool(&app, &tool, sample_args(&tool)).await;
                app.total_calls.fetch_add(1, Ordering::Relaxed);
                app.last_call_ms.store(now_ms(), Ordering::Relaxed);
                *app.last_call_tool.lock().unwrap() = tool.clone();
                let (body, is_err) = match result {
                    Ok(v)    => (v.to_string(), false),
                    Err(msg) => { app.call_errors.fetch_add(1, Ordering::Relaxed); (json!({"error": msg}).to_string(), true) },
                };
                let _ = is_err; // tracked above
                *app.last_call_result.lock().unwrap() = body.clone();
                let _ = stream.write_all(format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                     Access-Control-Allow-Origin: *\r\nContent-Length: {}\r\n\
                     Connection: close\r\n\r\n{}", body.len(), body
                ).as_bytes()).await;
                return;
            }

            // ── POST /add?tools=T1,T2 ───────────────────────────────────────
            if req.starts_with("POST /add") {
                let tools_str = parse_query_param(req, "tools").unwrap_or_default();
                let tools     = parse_tools(&tools_str);
                if tools.is_empty() {
                    let msg = b"HTTP/1.1 400 Bad Request\r\nContent-Length: 14\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\nno valid tools";
                    let _ = stream.write_all(msg).await;
                    return;
                }
                let peer_ports: Vec<u16> = {
                    let nodes = app.nodes.read().unwrap();
                    nodes.iter().filter(|s| s.alive.load(Ordering::Relaxed)).map(|s| s.port).collect()
                };
                let port = app.next_port.fetch_add(1, Ordering::Relaxed) as u16;
                let idx  = app.next_idx.fetch_add(1, Ordering::Relaxed);
                let label = format!("add node-{idx} (port {port})");
                if let Some(agent) = start_node_with_retry(port, &peer_ports, &label).await {
                    let handles = register_tools(&agent, &tools);
                    let slot = Arc::new(NodeSlot {
                        idx, port, tools,
                        alive:      Arc::new(AtomicBool::new(true)),
                        call_count: Arc::new(AtomicU64::new(0)),
                        agent:      Mutex::new(Some(agent)),
                        handles:    Mutex::new(handles),
                    });
                    app.nodes.write().unwrap().push(slot);
                    let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\nok").await;
                } else {
                    let _ = stream.write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 12\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\nstart failed").await;
                }
                return;
            }

            // ── POST /kill?node=IDX ─────────────────────────────────────────
            if req.starts_with("POST /kill") {
                if let Some(slot) = find_slot_by_idx(&app, req) {
                    slot.handles.lock().unwrap().clear();
                    let agent = slot.agent.lock().unwrap().take();
                    slot.alive.store(false, Ordering::Relaxed);
                    if let Some(a) = agent { a.shutdown().await; }
                }
                let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\nok").await;
                return;
            }

            // ── POST /restart?node=IDX ─────────────────────────────────────
            if req.starts_with("POST /restart") {
                if let Some(slot) = find_slot_by_idx(&app, req) {
                    let port  = slot.port;
                    let tools = slot.tools.clone();
                    let app2  = app.clone();
                    tokio::spawn(async move {
                        let peer_ports: Vec<u16> = {
                            let nodes = app2.nodes.read().unwrap();
                            nodes.iter()
                                .filter(|s| s.alive.load(Ordering::Relaxed) && s.port != port)
                                .map(|s| s.port).collect()
                        };
                        let label = format!("restart port {port}");
                        if let Some(agent) = start_node_with_retry(port, &peer_ports, &label).await {
                            let handles = register_tools(&agent, &tools);
                            *slot.agent.lock().unwrap()   = Some(agent);
                            *slot.handles.lock().unwrap() = handles;
                            slot.alive.store(true, Ordering::Relaxed);
                        }
                    });
                }
                let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\nok").await;
                return;
            }

            // ── POST /remove?node=IDX ──────────────────────────────────────
            if req.starts_with("POST /remove") {
                let stable_idx = parse_query_param(req, "node")
                    .and_then(|s| s.parse::<usize>().ok());
                if let Some(idx) = stable_idx {
                    let slot = {
                        let mut nodes = app.nodes.write().unwrap();
                        let pos = nodes.iter().position(|s| s.idx == idx);
                        pos.map(|p| nodes.remove(p))
                    };
                    if let Some(slot) = slot {
                        slot.handles.lock().unwrap().clear();
                        let agent = slot.agent.lock().unwrap().take();
                        if let Some(a) = agent { a.shutdown().await; }
                    }
                }
                let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\nok").await;
                return;
            }

            // ── GET / → HTML ────────────────────────────────────────────────
            let html = include_str!("../docs/mcp_mesh.html");
            let _ = stream.write_all(format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                html.len(), html
            ).as_bytes()).await;
        });
    }
}

/// Find a `NodeSlot` by its stable `idx` encoded in the request `node=` param.
fn find_slot_by_idx(app: &AppState, req: &str) -> Option<Arc<NodeSlot>> {
    let idx = parse_query_param(req, "node")?.parse::<usize>().ok()?;
    app.nodes.read().unwrap().iter().find(|s| s.idx == idx).cloned()
}

// ── main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    raise_fd_limit(4096);

    let n = INITIAL.len();
    let all_ports: Vec<u16> = (0..n).map(|i| BASE_PORT + i as u16).collect();

    eprintln!("Starting {n} agents…");
    let mut slots = Vec::with_capacity(n);
    for (idx, tools) in INITIAL.iter().enumerate() {
        let port        = BASE_PORT + idx as u16;
        let peer_ports: Vec<u16> = all_ports.iter().copied().filter(|&p| p != port).collect();
        let agent       = build_agent(port, &peer_ports);
        agent.start().await?;
        let tools_vec: Vec<&'static str> = tools.to_vec();
        let handles     = register_tools(&agent, &tools_vec);
        slots.push(Arc::new(NodeSlot {
            idx,
            port,
            tools: tools_vec,
            alive:      Arc::new(AtomicBool::new(true)),
            call_count: Arc::new(AtomicU64::new(0)),
            agent:      Mutex::new(Some(agent)),
            handles:    Mutex::new(handles),
        }));
    }

    let app = Arc::new(AppState {
        nodes:             RwLock::new(slots),
        next_idx:          AtomicUsize::new(n),
        next_port:         AtomicUsize::new(BASE_PORT as usize + n),
        total_calls:       AtomicU64::new(0),
        call_errors:       AtomicU64::new(0),
        last_call_ms:      AtomicU64::new(0),
        last_call_tool:    Mutex::new(String::new()),
        last_call_result:  Mutex::new("null".to_string()),
        last_caller_pos:   Mutex::new(-1),
        last_provider_pos: Mutex::new(-1),
        auto_call_idx:     AtomicU64::new(0),
    });

    tokio::spawn(serve_http(app.clone()));

    time::sleep(Duration::from_millis(SETTLE_MS)).await;
    eprintln!("Mesh settled. Auto-calling tools every {AUTO_CALL_MS}ms.");

    let auto_app = app.clone();
    tokio::spawn(async move {
        let mut ticker = time::interval(Duration::from_millis(AUTO_CALL_MS));
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let idx  = auto_app.auto_call_idx.fetch_add(1, Ordering::Relaxed) as usize;
            let tool = VALID_TOOLS[idx % VALID_TOOLS.len()];
            let args = sample_args(tool);
            let app2 = auto_app.clone();
            let tool_owned = tool.to_string();
            tokio::spawn(async move {
                let result = call_tool(&app2, &tool_owned, args).await;
                app2.total_calls.fetch_add(1, Ordering::Relaxed);
                app2.last_call_ms.store(now_ms(), Ordering::Relaxed);
                *app2.last_call_tool.lock().unwrap() = tool_owned.clone();
                match result {
                    Ok(v)  => *app2.last_call_result.lock().unwrap() = v.to_string(),
                    Err(e) => {
                        app2.call_errors.fetch_add(1, Ordering::Relaxed);
                        *app2.last_call_result.lock().unwrap() = json!({"error": e}).to_string();
                    }
                }
            });
        }
    });

    tokio::signal::ctrl_c().await?;
    eprintln!("\nShutting down…");
    let slots = app.nodes.read().unwrap().clone();
    for slot in &slots {
        slot.handles.lock().unwrap().clear();
        let agent = slot.agent.lock().unwrap().take();
        if let Some(a) = agent { a.shutdown().await; }
    }
    Ok(())
}
