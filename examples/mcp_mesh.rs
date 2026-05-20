//! MCP Mesh — Adaptive Broker-less AI Tool Routing
//!
//! An interactive playground for the Mycelium MCP bridge. Start with four
//! nodes that share a dependency graph of Capabilities and Requirements, then
//! add more from a tool palette, duplicate tools to see load-balancing, kill
//! providers to watch failover, and hover over any node to inspect its live
//! Capability advertisements and Requirement satisfaction.
//!
//! Initial topology (ports 55000–55003):
//!   node-0 · 55000 — weather, ping      provides data/realtime   needs llm/inference
//!   node-1 · 55001 — search, calculate  provides compute/cpu     needs data/realtime
//!   node-2 · 55002 — translate, summ.   provides llm/inference   needs compute/cpu
//!   node-3 · 55003 — calculate          provides compute/gpu     needs mcp/search
//!
//! Kill node-1 → node-2's compute/cpu and node-3's mcp/search both go ✗.
//!
//! Run:
//!   cargo run --example mcp_mesh
//!
//! Then open http://127.0.0.1:8099

use bytes::Bytes;
use mycelium::{
    Capability, CapabilityHandle, CapFilter, CapValue,
    GossipAgent, GossipConfig, McpToolHandle, NodeId, RequirementHandle,
    signal_kind,
};
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

const BASE_PORT: u16  = 55000;
const HTTP_PORT: u16  = 8099;
const SETTLE_MS: u64  = 2_500;
const AUTO_CALL_MS: u64 = 6_000;

const VALID_TOOLS: &[&str] = &[
    "weather", "ping", "search", "calculate", "translate", "summarize",
];

/// (tools, extra_caps, reqs) — extra_caps are in addition to the auto-generated
/// `mcp/{tool}` caps that every tool-bearing node advertises.
const INITIAL: &[(&[&str], &[(&str,&str)], &[(&str,&str)])] = &[
    (&["weather","ping"],        &[("data","realtime")],  &[("llm","inference")]),
    (&["search","calculate"],    &[("compute","cpu")],    &[("data","realtime")]),
    (&["translate","summarize"], &[("llm","inference")],  &[("compute","cpu")]),
    (&["calculate"],             &[("compute","gpu")],    &[("mcp","search")]),
];

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

// ── Node slot ─────────────────────────────────────────────────────────────────

struct NodeSlot {
    idx:         usize,
    port:        u16,
    tools:       Vec<&'static str>,
    /// Extra capability advertisements beyond the auto-generated `mcp/{tool}` set.
    caps_def:    Vec<(String, String)>,
    /// Requirements this node declares.
    reqs_def:    Vec<(String, String)>,
    alive:       Arc<AtomicBool>,
    call_count:  Arc<AtomicU64>,
    agent:       Mutex<Option<Arc<GossipAgent>>>,
    handles:     Mutex<Vec<McpToolHandle>>,
    cap_handles: Mutex<Vec<CapabilityHandle>>,
    req_handles: Mutex<Vec<RequirementHandle>>,
}

// ── App state ─────────────────────────────────────────────────────────────────

struct AppState {
    nodes:               RwLock<Vec<Arc<NodeSlot>>>,
    next_idx:            AtomicUsize,
    next_port:           AtomicUsize,
    total_calls:         AtomicU64,
    call_errors:         AtomicU64,
    last_call_ms:        AtomicU64,
    last_call_tool:      Mutex<String>,
    last_call_result:    Mutex<String>,
    last_caller_pos:     Mutex<i64>,
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
    Ok(json!({ "host": args["host"].as_str().unwrap_or("localhost"), "latency_ms": 12, "status": "reachable" }))
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
                "+" => Some(a+b), "-" => Some(a-b),
                "*" => Some(a*b), "/" => if b!=0.0 { Some(a/b) } else { None },
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
    let wc = text.split_whitespace().count();
    let summary = if wc > 8 { text.split_whitespace().take(7).collect::<Vec<_>>().join(" ") + "…" }
                  else       { text.to_string() };
    Ok(json!({ "summary": summary, "original_words": wc }))
}

// ── Agent / capability helpers ────────────────────────────────────────────────

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
    let mut h = Vec::new();
    for &tool in tools {
        let handle = match tool {
            "weather"   => agent.register_mcp_tool("weather", json!({
                "type":"object","description":"Current weather for a city",
                "properties":{"city":{"type":"string"}},"required":["city"],
            }), |a| async move { handle_weather(a).await }),
            "ping"      => agent.register_mcp_tool("ping", json!({
                "type":"object","description":"Check host reachability",
                "properties":{"host":{"type":"string"}},"required":["host"],
            }), |a| async move { handle_ping(a).await }),
            "search"    => agent.register_mcp_tool("search", json!({
                "type":"object","description":"Web search",
                "properties":{"query":{"type":"string"}},"required":["query"],
            }), |a| async move { handle_search(a).await }),
            "calculate" => agent.register_mcp_tool("calculate", json!({
                "type":"object","description":"Evaluate a math expression",
                "properties":{"expression":{"type":"string"}},"required":["expression"],
            }), |a| async move { handle_calculate(a).await }),
            "translate" => agent.register_mcp_tool("translate", json!({
                "type":"object","description":"Translate text",
                "properties":{"text":{"type":"string"},"target_language":{"type":"string"}},
                "required":["text","target_language"],
            }), |a| async move { handle_translate(a).await }),
            "summarize" => agent.register_mcp_tool("summarize", json!({
                "type":"object","description":"Summarize a passage",
                "properties":{"text":{"type":"string"}},"required":["text"],
            }), |a| async move { handle_summarize(a).await }),
            _ => continue,
        };
        h.push(handle);
    }
    h
}

/// Advertise all capabilities (auto from tools + extras) and declare requirements.
fn register_caps_reqs(
    agent:    &Arc<GossipAgent>,
    tools:    &[&'static str],
    caps_def: &[(String, String)],
    reqs_def: &[(String, String)],
) -> (Vec<CapabilityHandle>, Vec<RequirementHandle>) {
    let cap_interval = Duration::from_secs(60);
    let req_interval = Duration::from_secs(30);
    let mut cap_h = Vec::new();
    let mut req_h = Vec::new();
    for &tool in tools {
        cap_h.push(agent.advertise_capability(
            Capability::new("mcp", tool).with("tool", CapValue::Text(tool.into())),
            cap_interval,
        ));
    }
    for (ns, name) in caps_def {
        cap_h.push(agent.advertise_capability(
            Capability::new(ns.as_str(), name.as_str()),
            cap_interval,
        ));
    }
    for (ns, name) in reqs_def {
        req_h.push(agent.declare_requirement(
            CapFilter::new(ns.as_str(), name.as_str()),
            req_interval,
        ));
    }
    (cap_h, req_h)
}

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
    let line   = req.lines().next()?;
    let needle = format!("{key}=");
    let pos    = line.find(&needle)?;
    let rest   = &line[pos + needle.len()..];
    let end    = rest.find([' ', '&', '\r', '\n']).unwrap_or(rest.len());
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

async fn call_tool(app: &AppState, tool_name: &str, args: serde_json::Value) -> Result<serde_json::Value, String> {
    let live: Vec<(u16, Arc<GossipAgent>)> = {
        let nodes = app.nodes.read().unwrap();
        nodes.iter()
            .filter(|s| s.alive.load(Ordering::Relaxed))
            .filter_map(|s| Some((s.port, s.agent.lock().unwrap().clone()?)))
            .collect()
    };
    if live.is_empty() { return Err("no live agents".to_string()); }

    let call_n = app.total_calls.load(Ordering::Relaxed) as usize;
    let (caller_port, caller) = live[call_n % live.len()].clone();

    let prefix  = format!("tools/{tool_name}/");
    let entries = caller.scan_prefix(&prefix);
    if entries.is_empty() { return Err(format!("tool not found: {tool_name}")); }

    let pick = (now_ms() as usize) % entries.len();
    let (key, _) = &entries[pick];
    let provider_str = key.trim_start_matches(&prefix);
    let provider: NodeId = provider_str.parse()
        .map_err(|e| format!("bad provider id '{provider_str}': {e}"))?;
    let provider_port = provider.to_socket_addr().port();

    {
        let nodes = app.nodes.read().unwrap();
        let cp = nodes.iter().position(|s| s.port == caller_port)   .map(|i| i as i64).unwrap_or(-1);
        let pp = nodes.iter().position(|s| s.port == provider_port) .map(|i| i as i64).unwrap_or(-1);
        *app.last_caller_pos.lock().unwrap()   = cp;
        *app.last_provider_pos.lock().unwrap() = pp;
    }

    let rpc_req = json!({ "jsonrpc":"2.0","id":1,"method":"tools/call",
                           "params":{"name":tool_name,"arguments":args} });
    let reply = caller
        .rpc_call(provider, signal_kind::MCP_INVOKE,
                  Bytes::from(rpc_req.to_string().into_bytes()), Duration::from_secs(5))
        .await.map_err(|e| e.to_string())?;

    let resp: serde_json::Value = serde_json::from_slice(&reply).map_err(|e| e.to_string())?;
    if let Some(err) = resp.get("error") {
        return Err(err["message"].as_str().unwrap_or("tool error").to_string());
    }
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

    // Single cap/ KV scan: build both the satisfied-cap set and a cap→ports map.
    // Using live KV (not static struct fields) ensures killed nodes drop out immediately.
    let (advertised_caps, cap_port_map): (
        std::collections::HashSet<String>,
        std::collections::HashMap<String, Vec<u16>>,
    ) = {
        let scanner = nodes_snap.iter()
            .filter(|s| s.alive.load(Ordering::Relaxed))
            .find_map(|s| s.agent.lock().unwrap().clone());
        match scanner {
            None => Default::default(),
            Some(a) => {
                let mut caps_set: std::collections::HashSet<String> = Default::default();
                let mut port_map: std::collections::HashMap<String, Vec<u16>> = Default::default();
                // key = "cap/{ip:port}/{ns}/{name}" — node_id has no '/'
                for (key, _) in a.scan_prefix("cap/") {
                    let mut parts = key.splitn(4, '/');
                    let _ = parts.next(); // "cap"
                    let node_str = match parts.next() { Some(s) => s, None => continue };
                    let ns   = match parts.next() { Some(s) => s, None => continue };
                    let name = match parts.next() { Some(s) => s, None => continue };
                    if ns == "locality" { continue; }
                    let cap_key = format!("{ns}/{name}");
                    caps_set.insert(cap_key.clone());
                    let port: u16 = node_str.split(':').last()
                        .and_then(|s| s.parse().ok()).unwrap_or(0);
                    if port > 0 {
                        port_map.entry(cap_key).or_default().push(port);
                    }
                }
                (caps_set, port_map)
            }
        }
    };

    // Scan live tools with provider positions.
    let mut tool_map: std::collections::BTreeMap<String, Vec<usize>> = Default::default();
    {
        let scanner = nodes_snap.iter()
            .filter(|s| s.alive.load(Ordering::Relaxed))
            .find_map(|s| s.agent.lock().unwrap().clone());
        if let Some(a) = scanner {
            for (key, _) in a.scan_prefix("tools/") {
                let parts: Vec<&str> = key.splitn(3, '/').collect();
                if parts.len() < 3 { continue; }
                let name = parts[1].to_string();
                let port: u16 = parts[2].split(':').last().and_then(|s| s.parse().ok()).unwrap_or(0);
                if let Some(pos) = nodes_snap.iter().position(|s| s.port == port) {
                    tool_map.entry(name).or_default().push(pos);
                }
            }
        }
    }

    let live_tools_json: Vec<String> = tool_map.iter().map(|(name, providers)| {
        let pstr: Vec<String> = providers.iter().map(|i| i.to_string()).collect();
        format!(r#"{{"name":"{}","providers":[{}]}}"#, name, pstr.join(","))
    }).collect();

    let nodes_json: Vec<String> = nodes_snap.iter().enumerate().map(|(pos, s)| {
        let tools_json: Vec<String> = s.tools.iter().map(|t| format!("\"{}\"", t)).collect();
        // Full cap list: mcp/{tool} for each tool + caps_def extras.
        let mut cap_keys: Vec<String> = s.tools.iter().map(|&t| format!("\"mcp/{t}\"")).collect();
        for (ns, name) in &s.caps_def {
            cap_keys.push(format!("\"{ns}/{name}\""));
        }
        // Requirements with satisfaction.
        let req_json: Vec<String> = s.reqs_def.iter().map(|(ns, name)| {
            let key = format!("{ns}/{name}");
            let sat = advertised_caps.contains(&key);
            // Derive provider_pos from the live KV cap→port map so it tracks
            // reality: killed nodes have tombstoned their caps and drop out here.
            let provider_pos: i64 = cap_port_map.get(&key)
                .and_then(|ports| {
                    ports.iter().find_map(|&port| {
                        nodes_snap.iter().enumerate()
                            .find(|(_, s)| s.port == port)
                            .map(|(p, _)| p as i64)
                    })
                })
                .unwrap_or(-1);
            format!(r#"{{"key":"{key}","satisfied":{sat},"provider_pos":{provider_pos}}}"#)
        }).collect();
        format!(
            r#"{{"idx":{},"pos":{},"port":{},"alive":{},"call_count":{},"tools":[{}],"caps":[{}],"reqs":[{}]}}"#,
            s.idx, pos, s.port,
            s.alive.load(Ordering::Relaxed),
            s.call_count.load(Ordering::Relaxed),
            tools_json.join(","), cap_keys.join(","), req_json.join(","),
        )
    }).collect();

    let total        = app.total_calls.load(Ordering::Relaxed);
    let errors       = app.call_errors.load(Ordering::Relaxed);
    let last_ms      = app.last_call_ms.load(Ordering::Relaxed);
    let last_tool    = app.last_call_tool.lock().unwrap().clone();
    let last_result  = app.last_call_result.lock().unwrap().clone();
    let caller_pos   = *app.last_caller_pos.lock().unwrap();
    let provider_pos = *app.last_provider_pos.lock().unwrap();

    format!(
        r#"{{"n":{},"total_calls":{},"call_errors":{},"last_call_ms":{},"last_tool":"{}","last_result":{},"last_caller_pos":{},"last_provider_pos":{},"nodes":[{}],"live_tools":[{}]}}"#,
        nodes_snap.len(), total, errors, last_ms,
        last_tool.replace('"', "\\\""), last_result,
        caller_pos, provider_pos,
        nodes_json.join(","), live_tools_json.join(","),
    )
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────

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

fn kill_slot(slot: &NodeSlot) {
    slot.handles.lock().unwrap().clear();
    slot.cap_handles.lock().unwrap().clear();
    slot.req_handles.lock().unwrap().clear();
}

fn find_slot_by_idx(app: &AppState, req: &str) -> Option<Arc<NodeSlot>> {
    let idx = parse_query_param(req, "node")?.parse::<usize>().ok()?;
    app.nodes.read().unwrap().iter().find(|s| s.idx == idx).cloned()
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
            let n   = stream.read(&mut buf).await.unwrap_or(0);
            let req = std::str::from_utf8(&buf[..n]).unwrap_or("");

            macro_rules! respond {
                ($body:expr) => {{
                    let b: &[u8] = $body;
                    let _ = stream.write_all(format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n",
                        b.len()
                    ).as_bytes()).await;
                    let _ = stream.write_all(b).await;
                    return;
                }};
            }
            macro_rules! respond_json {
                ($body:expr) => {{
                    let b = $body;
                    let _ = stream.write_all(format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        b.len(), b
                    ).as_bytes()).await;
                    return;
                }};
            }

            if req.starts_with("GET /state") {
                respond_json!(state_json(&app));
            }

            if req.starts_with("POST /call") {
                let tool = parse_query_param(req, "tool").unwrap_or_default();
                if tool.is_empty() { respond!(b""); }
                let result = call_tool(&app, &tool, sample_args(&tool)).await;
                app.total_calls.fetch_add(1, Ordering::Relaxed);
                app.last_call_ms.store(now_ms(), Ordering::Relaxed);
                *app.last_call_tool.lock().unwrap() = tool.clone();
                let body = match result {
                    Ok(v)  => v.to_string(),
                    Err(e) => { app.call_errors.fetch_add(1, Ordering::Relaxed); json!({"error":e}).to_string() },
                };
                *app.last_call_result.lock().unwrap() = body.clone();
                respond_json!(body);
            }

            if req.starts_with("POST /add") {
                let tools_str = parse_query_param(req, "tools").unwrap_or_default();
                let tools     = parse_tools(&tools_str);
                if tools.is_empty() { respond!(b"no valid tools"); }
                let peer_ports: Vec<u16> = {
                    let nodes = app.nodes.read().unwrap();
                    nodes.iter().filter(|s| s.alive.load(Ordering::Relaxed)).map(|s| s.port).collect()
                };
                let port = app.next_port.fetch_add(1, Ordering::Relaxed) as u16;
                let idx  = app.next_idx.fetch_add(1, Ordering::Relaxed);
                let label = format!("add node-{idx} (port {port})");
                let caps_def: Vec<(String,String)> = vec![];
                let reqs_def: Vec<(String,String)> = vec![];
                if let Some(agent) = start_node_with_retry(port, &peer_ports, &label).await {
                    let handles     = register_tools(&agent, &tools);
                    let (cap_h, req_h) = register_caps_reqs(&agent, &tools, &caps_def, &reqs_def);
                    app.nodes.write().unwrap().push(Arc::new(NodeSlot {
                        idx, port, tools, caps_def, reqs_def,
                        alive:       Arc::new(AtomicBool::new(true)),
                        call_count:  Arc::new(AtomicU64::new(0)),
                        agent:       Mutex::new(Some(agent)),
                        handles:     Mutex::new(handles),
                        cap_handles: Mutex::new(cap_h),
                        req_handles: Mutex::new(req_h),
                    }));
                    respond!(b"ok");
                } else {
                    let _ = stream.write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 12\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\nstart failed").await;
                }
                return;
            }

            if req.starts_with("POST /kill") {
                if let Some(slot) = find_slot_by_idx(&app, req) {
                    kill_slot(&slot);
                    let agent = slot.agent.lock().unwrap().take();
                    slot.alive.store(false, Ordering::Relaxed);
                    if let Some(a) = agent { a.shutdown().await; }
                }
                respond!(b"ok");
            }

            if req.starts_with("POST /restart") {
                if let Some(slot) = find_slot_by_idx(&app, req) {
                    let port      = slot.port;
                    let tools     = slot.tools.clone();
                    let caps_def  = slot.caps_def.clone();
                    let reqs_def  = slot.reqs_def.clone();
                    let app2      = app.clone();
                    tokio::spawn(async move {
                        let peer_ports: Vec<u16> = {
                            let nodes = app2.nodes.read().unwrap();
                            nodes.iter()
                                .filter(|s| s.alive.load(Ordering::Relaxed) && s.port != port)
                                .map(|s| s.port).collect()
                        };
                        if let Some(agent) = start_node_with_retry(port, &peer_ports, &format!("restart port {port}")).await {
                            let handles      = register_tools(&agent, &tools);
                            let (cap_h, req_h) = register_caps_reqs(&agent, &tools, &caps_def, &reqs_def);
                            *slot.agent.lock().unwrap()       = Some(agent);
                            *slot.handles.lock().unwrap()     = handles;
                            *slot.cap_handles.lock().unwrap() = cap_h;
                            *slot.req_handles.lock().unwrap() = req_h;
                            slot.alive.store(true, Ordering::Relaxed);
                        }
                    });
                }
                respond!(b"ok");
            }

            if req.starts_with("POST /remove") {
                let stable_idx = parse_query_param(req, "node").and_then(|s| s.parse::<usize>().ok());
                if let Some(idx) = stable_idx {
                    let slot = {
                        let mut nodes = app.nodes.write().unwrap();
                        nodes.iter().position(|s| s.idx == idx).map(|p| nodes.remove(p))
                    };
                    if let Some(slot) = slot {
                        kill_slot(&slot);
                        let agent = slot.agent.lock().unwrap().take();
                        if let Some(a) = agent { a.shutdown().await; }
                    }
                }
                respond!(b"ok");
            }

            let html = include_str!("../docs/mcp_mesh.html");
            let _ = stream.write_all(format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                html.len(), html
            ).as_bytes()).await;
        });
    }
}

// ── main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    raise_fd_limit(4096);

    let n = INITIAL.len();
    let all_ports: Vec<u16> = (0..n).map(|i| BASE_PORT + i as u16).collect();

    eprintln!("Starting {n} agents…");
    let mut slots: Vec<Arc<NodeSlot>> = Vec::with_capacity(n);
    for (idx, &(tools, extra_caps, reqs)) in INITIAL.iter().enumerate() {
        let port       = BASE_PORT + idx as u16;
        let peers: Vec<u16> = all_ports.iter().copied().filter(|&p| p != port).collect();
        let agent      = build_agent(port, &peers);
        agent.start().await?;
        let tools_vec: Vec<&'static str>   = tools.to_vec();
        let caps_def: Vec<(String,String)> = extra_caps.iter().map(|(a,b)| (a.to_string(), b.to_string())).collect();
        let reqs_def: Vec<(String,String)> = reqs.iter().map(|(a,b)| (a.to_string(), b.to_string())).collect();
        let handles      = register_tools(&agent, &tools_vec);
        let (cap_h, req_h) = register_caps_reqs(&agent, &tools_vec, &caps_def, &reqs_def);
        slots.push(Arc::new(NodeSlot {
            idx, port, tools: tools_vec, caps_def, reqs_def,
            alive:       Arc::new(AtomicBool::new(true)),
            call_count:  Arc::new(AtomicU64::new(0)),
            agent:       Mutex::new(Some(agent)),
            handles:     Mutex::new(handles),
            cap_handles: Mutex::new(cap_h),
            req_handles: Mutex::new(req_h),
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
                        *app2.last_call_result.lock().unwrap() = json!({"error":e}).to_string();
                    }
                }
            });
        }
    });

    tokio::signal::ctrl_c().await?;
    eprintln!("\nShutting down…");
    for slot in app.nodes.read().unwrap().iter() {
        kill_slot(slot);
        let agent = slot.agent.lock().unwrap().take();
        if let Some(a) = agent { a.shutdown().await; }
    }
    Ok(())
}
