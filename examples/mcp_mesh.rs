//! MCP Mesh — Broker-less AI Tool Routing
//!
//! Five GossipAgents (ports 55000–55004) each register different MCP tools.
//! Any node can discover and invoke any tool through the gossip mesh — there is
//! no registry, no broker, no central router.
//!
//! Tools per node:
//!   node-0 · ports 55000 — weather, ping
//!   node-1 · port  55001 — search
//!   node-2 · port  55002 — calculate
//!   node-3 · port  55003 — translate
//!   node-4 · port  55004 — summarize
//!
//! The demo moment:
//!   1. Kill node-0.  KV tombstones propagate; "weather" and "ping" vanish from the mesh.
//!   2. Call "weather" → JSON-RPC error: tool not found.
//!   3. Restart node-0.  Tools re-register; KV re-propagates.
//!   4. Call "weather" → succeeds again. Zero reconfiguration anywhere.
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
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
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

const N: usize = 5;
const BASE_PORT: u16 = 55000;
const HTTP_PORT: u16 = 8099;
const SETTLE_MS: u64 = 2_500;
const AUTO_CALL_MS: u64 = 8_000;

/// Names of tools registered on each node (index = node index).
const NODE_TOOLS: [&[&str]; N] = [
    &["weather", "ping"],
    &["search"],
    &["calculate"],
    &["translate"],
    &["summarize"],
];

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// All state for one node.
struct NodeSlot {
    port:       u16,
    tools:      &'static [&'static str],
    alive:      Arc<AtomicBool>,
    call_count: Arc<AtomicU64>,
    agent:      Mutex<Option<Arc<GossipAgent>>>,
    handles:    Mutex<Vec<McpToolHandle>>,
}

struct AppState {
    nodes:            Vec<Arc<NodeSlot>>,
    total_calls:      AtomicU64,
    call_errors:      AtomicU64,
    last_call_ms:     AtomicU64,
    last_call_tool:   Mutex<String>,
    last_call_result: Mutex<String>,
    auto_call_idx:    AtomicU64,
}

// ── Tool handler implementations ──────────────────────────────────────────────

async fn handle_weather(args: serde_json::Value) -> Result<serde_json::Value, String> {
    let city = args["city"].as_str().unwrap_or("Unknown");
    let temp = 15 + (city.len() % 20) as i32;
    Ok(json!({
        "city": city,
        "temp_c": temp,
        "condition": if temp > 20 { "sunny" } else if temp > 10 { "cloudy" } else { "rainy" },
    }))
}

async fn handle_ping(args: serde_json::Value) -> Result<serde_json::Value, String> {
    let target = args["host"].as_str().unwrap_or("localhost");
    Ok(json!({ "host": target, "latency_ms": 12, "status": "reachable" }))
}

async fn handle_search(args: serde_json::Value) -> Result<serde_json::Value, String> {
    let query = args["query"].as_str().unwrap_or("");
    Ok(json!({
        "query": query,
        "results": [
            { "title": format!("{query} — Overview"),    "url": "https://example.com/1" },
            { "title": format!("{query} — Deep dive"),   "url": "https://example.com/2" },
            { "title": format!("{query} — Latest news"), "url": "https://example.com/3" },
        ],
    }))
}

async fn handle_calculate(args: serde_json::Value) -> Result<serde_json::Value, String> {
    let expr = args["expression"].as_str().unwrap_or("0");
    // Minimal safe evaluator: only handles "X op Y" patterns.
    let result: f64 = (|| {
        let parts: Vec<&str> = expr.split_whitespace().collect();
        if parts.len() == 3 {
            let a: f64 = parts[0].parse().ok()?;
            let b: f64 = parts[2].parse().ok()?;
            match parts[1] {
                "+" => Some(a + b),
                "-" => Some(a - b),
                "*" => Some(a * b),
                "/" => if b != 0.0 { Some(a / b) } else { None },
                _   => None,
            }
        } else {
            None
        }
    })()
    .unwrap_or(f64::NAN);
    Ok(json!({ "expression": expr, "result": result }))
}

async fn handle_translate(args: serde_json::Value) -> Result<serde_json::Value, String> {
    let text   = args["text"].as_str().unwrap_or("");
    let target = args["target_language"].as_str().unwrap_or("es");
    let translated = match target {
        "es" => format!("[es] {text}"),
        "fr" => format!("[fr] {text}"),
        "de" => format!("[de] {text}"),
        "ja" => format!("[ja] {text}"),
        _    => format!("[{target}] {text}"),
    };
    Ok(json!({ "original": text, "translated": translated, "language": target }))
}

async fn handle_summarize(args: serde_json::Value) -> Result<serde_json::Value, String> {
    let text = args["text"].as_str().unwrap_or("");
    let word_count = text.split_whitespace().count();
    let summary = if word_count > 10 {
        text.split_whitespace().take(8).collect::<Vec<_>>().join(" ") + "…"
    } else {
        text.to_string()
    };
    Ok(json!({ "summary": summary, "original_words": word_count, "reduction_pct": 70 }))
}

// ── Agent factory ─────────────────────────────────────────────────────────────

fn build_agent(idx: usize) -> Arc<GossipAgent> {
    let port = BASE_PORT + idx as u16;
    let nid  = NodeId::new("127.0.0.1", port).unwrap();
    let mut cfg = GossipConfig::default();
    cfg.bind_address               = "127.0.0.1".to_string();
    cfg.bind_port                  = port;
    cfg.default_ttl                = 20;
    cfg.reconnect_backoff_secs     = 1;
    cfg.gossip_shards              = 1;
    cfg.health_check_max_jitter_ms = 100;
    // Full mesh: every node bootstraps to all others.
    cfg.bootstrap_peers = (0..N)
        .filter(|&j| j != idx)
        .map(|j| NodeId::new("127.0.0.1", BASE_PORT + j as u16).unwrap())
        .collect();
    cfg.max_peers = N - 1;
    Arc::new(GossipAgent::new(nid, cfg))
}

fn register_tools(agent: &Arc<GossipAgent>, idx: usize) -> Vec<McpToolHandle> {
    let mut handles = Vec::new();
    for &tool in NODE_TOOLS[idx] {
        let handle = match tool {
            "weather"   => agent.register_mcp_tool("weather", json!({
                "type": "object",
                "description": "Get current weather for a city",
                "properties": { "city": { "type": "string" } },
                "required": ["city"],
            }), |args| async move { handle_weather(args).await }),

            "ping"      => agent.register_mcp_tool("ping", json!({
                "type": "object",
                "description": "Check host reachability",
                "properties": { "host": { "type": "string" } },
                "required": ["host"],
            }), |args| async move { handle_ping(args).await }),

            "search"    => agent.register_mcp_tool("search", json!({
                "type": "object",
                "description": "Search the web for information",
                "properties": { "query": { "type": "string" } },
                "required": ["query"],
            }), |args| async move { handle_search(args).await }),

            "calculate" => agent.register_mcp_tool("calculate", json!({
                "type": "object",
                "description": "Evaluate a mathematical expression",
                "properties": { "expression": { "type": "string" } },
                "required": ["expression"],
            }), |args| async move { handle_calculate(args).await }),

            "translate" => agent.register_mcp_tool("translate", json!({
                "type": "object",
                "description": "Translate text to another language",
                "properties": {
                    "text":            { "type": "string" },
                    "target_language": { "type": "string" },
                },
                "required": ["text", "target_language"],
            }), |args| async move { handle_translate(args).await }),

            "summarize" => agent.register_mcp_tool("summarize", json!({
                "type": "object",
                "description": "Summarize a passage of text",
                "properties": { "text": { "type": "string" } },
                "required": ["text"],
            }), |args| async move { handle_summarize(args).await }),

            _ => continue,
        };
        handles.push(handle);
    }
    handles
}

// ── Sample arguments for auto-cycling ────────────────────────────────────────

fn sample_args(tool: &str) -> serde_json::Value {
    match tool {
        "weather"   => json!({ "city": "Tokyo" }),
        "ping"      => json!({ "host": "8.8.8.8" }),
        "search"    => json!({ "query": "gossip protocol" }),
        "calculate" => json!({ "expression": "42 * 7" }),
        "translate" => json!({ "text": "Hello world", "target_language": "es" }),
        "summarize" => json!({ "text": "Mycelium is a broker-less gossip substrate for AI agent systems that provides epidemic key-value replication and a signal mesh." }),
        _           => json!({}),
    }
}

// ── RPC call helper ───────────────────────────────────────────────────────────

/// Find a provider for `tool_name` in any live agent's KV view, then call it.
async fn call_tool(
    app:       &AppState,
    tool_name: &str,
    args:      serde_json::Value,
) -> Result<serde_json::Value, String> {
    // Pick any live agent to scan the KV store from.
    let caller = app.nodes.iter()
        .filter(|s| s.alive.load(Ordering::Relaxed))
        .find_map(|s| s.agent.lock().unwrap().clone());
    let caller = caller.ok_or_else(|| "no live agents".to_string())?;

    // Scan tools/{name}/ to find a provider node_id.
    let prefix = format!("tools/{tool_name}/");
    let entries = caller.scan_prefix(&prefix);
    if entries.is_empty() {
        return Err(format!("tool not found: {tool_name}"));
    }

    // Parse provider NodeId from key "tools/{name}/{ip}:{port}".
    let (key, _) = &entries[0];
    let provider_str = key.trim_start_matches(&format!("tools/{tool_name}/"));
    let provider: NodeId = provider_str.parse()
        .map_err(|e| format!("invalid provider id '{provider_str}': {e}"))?;

    // Build JSON-RPC request.
    let rpc_req = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": { "name": tool_name, "arguments": args },
    });

    let reply = caller
        .rpc_call(
            provider,
            signal_kind::MCP_INVOKE,
            Bytes::from(rpc_req.to_string().into_bytes()),
            Duration::from_secs(5),
        )
        .await
        .map_err(|e| e.to_string())?;

    let resp: serde_json::Value = serde_json::from_slice(&reply)
        .map_err(|e| e.to_string())?;

    if let Some(err) = resp.get("error") {
        return Err(err["message"].as_str().unwrap_or("tool error").to_string());
    }
    Ok(resp["result"].clone())
}

// ── State JSON ────────────────────────────────────────────────────────────────

fn state_json(app: &AppState) -> String {
    // Collect live tool KV entries for the mesh view.
    let live_tools: Vec<String> = {
        let caller = app.nodes.iter()
            .filter(|s| s.alive.load(Ordering::Relaxed))
            .find_map(|s| s.agent.lock().unwrap().clone());
        match caller {
            None => vec![],
            Some(a) => {
                let entries = a.scan_prefix("tools/");
                let mut seen = std::collections::HashSet::new();
                let mut tools = vec![];
                for (key, _) in entries {
                    // key = "tools/{name}/{ip}:{port}"
                    let parts: Vec<&str> = key.splitn(3, '/').collect();
                    if parts.len() < 3 { continue; }
                    let name = parts[1];
                    if !seen.insert(name.to_string()) { continue; }
                    // Find which node owns it.
                    let provider = parts[2];
                    let port_str = provider.split(':').last().unwrap_or("0");
                    let port: u16 = port_str.parse().unwrap_or(0);
                    let node_idx = if port >= BASE_PORT && port < BASE_PORT + N as u16 {
                        (port - BASE_PORT) as usize
                    } else {
                        continue
                    };
                    tools.push(format!(
                        r#"{{"name":"{}","node":{}}}"#,
                        name, node_idx,
                    ));
                }
                tools
            }
        }
    };

    let nodes: Vec<String> = app.nodes.iter().enumerate().map(|(i, slot)| {
        let alive = slot.alive.load(Ordering::Relaxed);
        let calls = slot.call_count.load(Ordering::Relaxed);
        let tools_json: Vec<String> = slot.tools.iter()
            .map(|t| format!("\"{}\"", t))
            .collect();
        format!(
            r#"{{"id":{},"port":{},"alive":{},"call_count":{},"tools":[{}]}}"#,
            i, slot.port, alive, calls, tools_json.join(","),
        )
    }).collect();

    let total_calls  = app.total_calls.load(Ordering::Relaxed);
    let call_errors  = app.call_errors.load(Ordering::Relaxed);
    let last_call_ms = app.last_call_ms.load(Ordering::Relaxed);
    let last_tool    = app.last_call_tool.lock().unwrap().clone();
    let last_result  = app.last_call_result.lock().unwrap().clone();

    format!(
        r#"{{"n":{},"base_port":{},"total_calls":{},"call_errors":{},"last_call_ms":{},"last_tool":"{}","last_result":{},"nodes":[{}],"live_tools":[{}]}}"#,
        N, BASE_PORT, total_calls, call_errors, last_call_ms,
        last_tool.replace('"', "\\\""),
        last_result,
        nodes.join(","),
        live_tools.join(","),
    )
}

// ── HTTP server ───────────────────────────────────────────────────────────────

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
            let n = stream.read(&mut buf).await.unwrap_or(0);
            let req = std::str::from_utf8(&buf[..n]).unwrap_or("");

            // GET /state
            if req.starts_with("GET /state") {
                let body = state_json(&app);
                let _ = stream.write_all(format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(), body
                ).as_bytes()).await;
                return;
            }

            // POST /call?tool=NAME
            if req.starts_with("POST /call") {
                let tool = req.lines().next()
                    .and_then(|line| {
                        let q = line.find("tool=")?;
                        let rest = &line[q + 5..];
                        let end = rest.find([' ', '&']).unwrap_or(rest.len());
                        Some(rest[..end].to_string())
                    })
                    .unwrap_or_default();

                if tool.is_empty() {
                    let _ = stream.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await;
                    return;
                }

                let args = sample_args(&tool);
                let result = call_tool(&app, &tool, args).await;
                app.total_calls.fetch_add(1, Ordering::Relaxed);
                app.last_call_ms.store(now_ms(), Ordering::Relaxed);
                *app.last_call_tool.lock().unwrap() = tool.clone();

                let (result_json, is_err) = match result {
                    Ok(v)    => (v.to_string(), false),
                    Err(msg) => {
                        app.call_errors.fetch_add(1, Ordering::Relaxed);
                        // Find the node that owns this tool and increment its call counter
                        // even on error (the attempt was made).
                        (json!({"error": msg}).to_string(), true)
                    }
                };

                // Attribute call to the tool's owner node.
                if !is_err {
                    for slot in &app.nodes {
                        if slot.tools.contains(&tool.as_str()) && slot.alive.load(Ordering::Relaxed) {
                            slot.call_count.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                }

                *app.last_call_result.lock().unwrap() = result_json.clone();

                let body = result_json;
                let _ = stream.write_all(format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(), body
                ).as_bytes()).await;
                return;
            }

            // POST /kill?node=N
            if req.starts_with("POST /kill") {
                let idx = parse_node_idx(req);
                if let Some(i) = idx {
                    let slot = &app.nodes[i];
                    // Drop handles first (tombstones KV), then shut down agent.
                    slot.handles.lock().unwrap().clear();
                    let agent = slot.agent.lock().unwrap().take();
                    slot.alive.store(false, Ordering::Relaxed);
                    if let Some(a) = agent {
                        tokio::spawn(async move { a.shutdown().await; });
                    }
                }
                let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\nok").await;
                return;
            }

            // POST /restart?node=N
            if req.starts_with("POST /restart") {
                let idx = parse_node_idx(req);
                if let Some(i) = idx {
                    let app2 = app.clone();
                    tokio::spawn(async move {
                        // Brief delay so any in-flight tombstones propagate first.
                        time::sleep(Duration::from_millis(500)).await;
                        let slot = &app2.nodes[i];
                        let agent = build_agent(i);
                        if agent.start().await.is_ok() {
                            let handles = register_tools(&agent, i);
                            *slot.agent.lock().unwrap()   = Some(agent);
                            *slot.handles.lock().unwrap() = handles;
                            slot.alive.store(true, Ordering::Relaxed);
                        }
                    });
                }
                let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\nok").await;
                return;
            }

            // GET / (and everything else) → HTML
            let html = include_str!("../docs/mcp_mesh.html");
            let _ = stream.write_all(format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                html.len(), html
            ).as_bytes()).await;
        });
    }
}

fn parse_node_idx(req: &str) -> Option<usize> {
    let line = req.lines().next()?;
    let q = line.find("node=")?;
    let rest = &line[q + 5..];
    let end = rest.find([' ', '&']).unwrap_or(rest.len());
    let idx: usize = rest[..end].parse().ok()?;
    if idx < N { Some(idx) } else { None }
}

// ── main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    raise_fd_limit(4096);

    // Build and start all agents.
    let mut agent_arcs: Vec<Arc<GossipAgent>> = Vec::with_capacity(N);
    for i in 0..N {
        agent_arcs.push(build_agent(i));
    }

    eprintln!("Starting {N} agents in full-mesh topology…");
    for a in &agent_arcs { a.start().await?; }

    // Register tools on each agent.
    let mut all_handles: Vec<Vec<McpToolHandle>> = agent_arcs.iter()
        .enumerate()
        .map(|(i, a)| register_tools(a, i))
        .collect();

    // Assemble app state.
    let nodes: Vec<Arc<NodeSlot>> = (0..N).map(|i| {
        Arc::new(NodeSlot {
            port:       BASE_PORT + i as u16,
            tools:      NODE_TOOLS[i],
            alive:      Arc::new(AtomicBool::new(true)),
            call_count: Arc::new(AtomicU64::new(0)),
            agent:      Mutex::new(Some(agent_arcs[i].clone())),
            handles:    Mutex::new(all_handles.remove(0)),
        })
    }).collect();

    let app = Arc::new(AppState {
        nodes,
        total_calls:      AtomicU64::new(0),
        call_errors:      AtomicU64::new(0),
        last_call_ms:     AtomicU64::new(0),
        last_call_tool:   Mutex::new(String::new()),
        last_call_result: Mutex::new("null".to_string()),
        auto_call_idx:    AtomicU64::new(0),
    });

    tokio::spawn(serve_http(app.clone()));

    time::sleep(Duration::from_millis(SETTLE_MS)).await;
    eprintln!("Mesh settled. Auto-calling tools every {AUTO_CALL_MS}ms.");

    // Auto-cycle through all tools.
    let all_tools: Vec<&str> = NODE_TOOLS.iter().flat_map(|ts| ts.iter().copied()).collect();
    let auto_app = app.clone();
    tokio::spawn(async move {
        let mut ticker = time::interval(Duration::from_millis(AUTO_CALL_MS));
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let idx = auto_app.auto_call_idx.fetch_add(1, Ordering::Relaxed) as usize;
            let tool = all_tools[idx % all_tools.len()];
            let args = sample_args(tool);
            let app2 = auto_app.clone();
            let tool_owned = tool.to_string();
            tokio::spawn(async move {
                let result = call_tool(&app2, &tool_owned, args).await;
                app2.total_calls.fetch_add(1, Ordering::Relaxed);
                app2.last_call_ms.store(now_ms(), Ordering::Relaxed);
                *app2.last_call_tool.lock().unwrap() = tool_owned.clone();
                match result {
                    Ok(v)  => {
                        for slot in &app2.nodes {
                            if slot.tools.contains(&tool_owned.as_str()) && slot.alive.load(Ordering::Relaxed) {
                                slot.call_count.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                        }
                        *app2.last_call_result.lock().unwrap() = v.to_string();
                    }
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
    for slot in &app.nodes {
        slot.handles.lock().unwrap().clear();
        let agent = slot.agent.lock().unwrap().take();
        if let Some(a) = agent {
            a.shutdown().await;
        }
    }
    Ok(())
}
