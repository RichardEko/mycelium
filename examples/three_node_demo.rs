//! Multi-role demo binary — LLM chat cluster and consistency overlay cluster.
//!
//! One binary, six roles selected by `MYCELIUM_ROLE`:
//!
//! ```text
//!   tool-a   ─── weather(city)   — calls wttr.in
//!                web_fetch(url)  — fetches any URL, returns first 2 KB
//!
//!   tool-b   ─── calculate(expr) — safe arithmetic evaluator
//!                wiki(topic)     — calls Wikipedia REST summary API
//!
//!   tool-sf  ─── sf_lookup(query) — SF Encyclopedia (SFE3) scholarly lookup
//!                                   author/series/theme entries; dynamically
//!                                   discovered by the llm node when started
//!
//!   llm      ─── browser chat UI at CHAT_PORT (default 8080)
//!                plans with local Ollama (llama3.2)
//!                routes each tool call to whichever container hosts it
//!
//!   mgmt     ─── read-only management dashboard (JSON + SSE)
//!
//!   node     ─── generic gossip node (KV + signals, no tools or LLM)
//!
//!   overlay  ─── consistency overlay node: consensus voter + HTTP gateway
//!                exposes consistent_set/get, distributed_lock, elect_leader,
//!                append/scan_log/subscribe_log, emit_reliable — all via REST
//!                used by the Python SDK overlay methods and tests/overlay/
//! ```
//!
//! # HTTP endpoints (llm node)
//!
//! | Endpoint      | Description |
//! |---------------|-------------|
//! | `GET /`       | Browser chat UI (HTML) |
//! | `POST /chat`  | Send `{"message":"..."}` — 202 Accepted; planning runs async |
//! | `GET /stream` | SSE stream of `ChatEvent`: Thinking, ToolCall, ToolResult, Assistant, Idle |
//! | `GET /mesh`   | Tool list visible to the planner and current model name |
//!
//! # HTTP endpoints (overlay node — full overlay API)
//!
//! | Endpoint | Method | Description |
//! |---|---|---|
//! | `/gateway/overlay/consistent/set` | POST | Linearizable KV write |
//! | `/gateway/overlay/consistent/get` | GET  | Read committed value |
//! | `/gateway/overlay/lock/acquire`   | POST | Acquire distributed lock |
//! | `/gateway/overlay/lock/{id}`      | DELETE | Release lock guard |
//! | `/gateway/overlay/elect`          | POST | Elect a group leader |
//! | `/gateway/overlay/log/append`     | POST | Append to ordered log stream |
//! | `/gateway/overlay/log/scan`       | GET  | Range scan log stream |
//! | `/gateway/overlay/log/compact`    | POST | Tombstone old log entries |
//! | `/gateway/overlay/log/subscribe`  | GET  | SSE live stream |
//! | `/gateway/overlay/log/group/subscribe` | GET | SSE consumer-group stream |
//! | `/gateway/overlay/emit_reliable`  | POST | Send with explicit ACK |
//!
//! # Environment variables
//!
//! | Variable             | Default                      | Used by       |
//! |----------------------|------------------------------|---------------|
//! | `MYCELIUM_ROLE`      | `tool-a`                     | all           |
//! | `MYCELIUM_PEERS`     | *(comma-sep h:p)*            | all           |
//! | `MYCELIUM_HOSTNAME`  | value of `HOSTNAME` env var  | all           |
//! | `MYCELIUM_PORT`      | `57000`                      | all           |
//! | `MYCELIUM_HTTP_PORT` | `8300`                       | all           |
//! | `OLLAMA_BASE_URL`    | `http://ollama:11434/v1`     | llm           |
//! | `OLLAMA_MODEL`       | `llama3.2`                   | llm           |
//! | `CHAT_PORT`          | `8080`                       | llm           |
//! | `MGMT_PORT`          | `8090`                       | mgmt          |
//!
//! # Docker (recommended)
//! ```sh
//! make test-llm-demo     # interactive — open http://localhost:8080 to chat
//! make test-three-node   # automated test (4 scenarios, real llama3.2)
//! make test-overlay      # overlay cluster: 3 nodes, 3 Python scenarios
//! ```
//!
//! # Local quick start — LLM chat cluster (no Docker)
//! ```sh
//! # terminal 1
//! MYCELIUM_ROLE=tool-a MYCELIUM_PEERS=127.0.0.1:57001,127.0.0.1:57002,127.0.0.1:57003 \
//!   MYCELIUM_PORT=57000 cargo run --example three_node_demo
//!
//! # terminal 2
//! MYCELIUM_ROLE=tool-b MYCELIUM_PEERS=127.0.0.1:57000,127.0.0.1:57002,127.0.0.1:57003 \
//!   MYCELIUM_PORT=57001 cargo run --example three_node_demo
//!
//! # terminal 3 — requires Ollama running on localhost:11434 with llama3.2 pulled
//! MYCELIUM_ROLE=llm MYCELIUM_PEERS=127.0.0.1:57000,127.0.0.1:57001,127.0.0.1:57003 \
//!   MYCELIUM_PORT=57002 OLLAMA_BASE_URL=http://localhost:11434/v1 \
//!   cargo run --example three_node_demo
//! # open http://localhost:8080
//!
//! # terminal 4 — management dashboard (optional)
//! MYCELIUM_ROLE=mgmt MYCELIUM_PEERS=127.0.0.1:57000,127.0.0.1:57001,127.0.0.1:57002 \
//!   MYCELIUM_PORT=57003 cargo run --example three_node_demo
//! # open http://localhost:8090
//!
//! # terminal 5 — SF Encyclopedia tool (start any time; llm discovers it live)
//! MYCELIUM_ROLE=tool-sf MYCELIUM_PEERS=127.0.0.1:57000,127.0.0.1:57001,127.0.0.1:57002 \
//!   MYCELIUM_PORT=57004 cargo run --example three_node_demo
//! # once started, ask the llm about any SF author, series, or theme —
//! # it will automatically prefer sf_lookup over wiki for SF queries
//! ```
//!
//! # Local quick start — overlay cluster (no Docker)
//! ```sh
//! # terminal 1
//! MYCELIUM_ROLE=overlay MYCELIUM_PEERS=127.0.0.1:57001,127.0.0.1:57002 \
//!   MYCELIUM_PORT=57000 MYCELIUM_HTTP_PORT=8300 cargo run --example three_node_demo
//!
//! # terminal 2
//! MYCELIUM_ROLE=overlay MYCELIUM_PEERS=127.0.0.1:57000,127.0.0.1:57002 \
//!   MYCELIUM_PORT=57001 MYCELIUM_HTTP_PORT=8301 cargo run --example three_node_demo
//!
//! # terminal 3
//! MYCELIUM_ROLE=overlay MYCELIUM_PEERS=127.0.0.1:57000,127.0.0.1:57001 \
//!   MYCELIUM_PORT=57002 MYCELIUM_HTTP_PORT=8302 cargo run --example three_node_demo
//!
//! # Python SDK talks to any node's HTTP gateway, e.g.:
//! #   agent = MyceliumAgent("127.0.0.1", 8300)
//! #   agent.consistent_set("cfg/x", b"hello")
//! #   leader = agent.elect_leader("my-group")
//! ```

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    response::sse::{Event, KeepAlive, Sse},
    routing::{get, post},
};
use bytes::Bytes;
use futures_util::StreamExt;
use mycelium::{
    BulkServeHandle, CapabilityHandle, Capability, CapFilter, GossipAgent, GossipConfig,
    MailboxHandle, McpToolHandle, NodeId,
    PersistenceConfig, SignalScope, SyncMode, signal_kind,
};
use mycelium::ConsensusConfig;
#[cfg(feature = "llm")]
use mycelium::{EchoBackend, LlmBackend, PromptSkillHandle, PromptTemplate};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::convert::Infallible;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::time::Duration;
use tokio::sync::{Mutex, broadcast};
use tokio::time;
use tokio_stream::wrappers::BroadcastStream;
use tracing::{error, info, warn};

// ── Constants ──────────────────────────────────────────────────────────────────

const GOSSIP_PORT_DEFAULT: u16 = 57000;
const HTTP_PORT_DEFAULT:   u16 = 8300;
const CHAT_PORT_DEFAULT:   u16 = 8080;
const MGMT_PORT_DEFAULT:   u16 = 8090;
const TOOL_SETTLE_SECS:    u64 = 8;
const MAX_TURNS:           usize = 12;

// ── Chat events (broadcast to all SSE clients) ────────────────────────────────

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ChatEvent {
    UserMessage { content: String },
    Thinking    { content: String },
    ToolCall    { tool: String, node_id: String, args: Value },
    ToolResult  { tool: String, result: Value },
    ToolError   { tool: String, error: String },
    Assistant   { content: String },
    Error       { message: String },
    Idle,
}

// ── Shared state ───────────────────────────────────────────────────────────────

struct AppState {
    agent:   Arc<GossipAgent>,
    cfg:     LlmCfg,
    history: Mutex<Vec<Value>>,
    tx:      broadcast::Sender<ChatEvent>,
    busy:    AtomicBool,
}

// ── LLM config ─────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct LlmCfg {
    base_url: String,
    api_key:  String,
    model:    String,
}

impl LlmCfg {
    fn from_env() -> Self {
        Self {
            base_url: std::env::var("OLLAMA_BASE_URL")
                .unwrap_or_else(|_| "http://ollama:11434/v1".into()),
            api_key:  std::env::var("OLLAMA_API_KEY")
                .unwrap_or_else(|_| "ollama".into()),
            model:    std::env::var("OLLAMA_MODEL")
                .unwrap_or_else(|_| "llama3.2".into()),
        }
    }
}

// ── Startup helpers ────────────────────────────────────────────────────────────

async fn resolve_ip(hostname: &str) -> Result<String, String> {
    use tokio::net::lookup_host;
    let mut addrs = lookup_host(format!("{hostname}:0"))
        .await
        .map_err(|e| format!("DNS lookup for '{hostname}' failed: {e}"))?;
    addrs.next()
        .map(|a| a.ip().to_string())
        .ok_or_else(|| format!("no address resolved for '{hostname}'"))
}

async fn resolve_peers(peer_list: &str) -> Vec<NodeId> {
    let mut out = Vec::new();
    for entry in peer_list.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (host, port) = match entry.rsplit_once(':') {
            Some(p) => p,
            None    => { warn!(peer = entry, "ignoring peer: no port"); continue; }
        };
        let port: u16 = match port.parse() {
            Ok(p)  => p,
            Err(_) => { warn!(peer = entry, "ignoring peer: invalid port"); continue; }
        };
        match resolve_ip(host).await {
            Ok(ip) => match NodeId::new(&ip, port) {
                Ok(nid) => out.push(nid),
                Err(e)  => warn!(peer = entry, "ignoring peer: {e}"),
            },
            Err(e) => warn!(peer = entry, "ignoring peer: {e}"),
        }
    }
    out
}

async fn make_agent(
    my_ip:    &str,
    peers:    Vec<NodeId>,
    port:     u16,
    http_port: Option<u16>,
    data_dir:  Option<std::path::PathBuf>,
) -> Arc<GossipAgent> {
    let nid = NodeId::new(my_ip, port).expect("valid self NodeId");
    let mut cfg = GossipConfig::default();
    cfg.bind_address               = my_ip.to_string();
    cfg.bind_port                  = port;
    cfg.http_port                  = http_port;
    cfg.http_addr                  = "0.0.0.0".to_string();
    cfg.bootstrap_peers            = peers;
    cfg.default_ttl                = 10;
    cfg.reconnect_backoff_secs     = 2;
    cfg.gossip_shards              = 2;
    cfg.health_check_max_jitter_ms = 200;
    if let Some(dir) = data_dir {
        cfg.persistence = Some(PersistenceConfig {
            base_path:              dir,
            // Flush guarantees every WAL entry is fsynced before returning,
            // so data survives an unclean shutdown (SIGTERM with no explicit fsync).
            sync_mode:              SyncMode::Flush,
            snapshot_wal_threshold: 10_000,
            snapshot_interval_secs: 300,
        });
    }
    Arc::new(GossipAgent::new(nid, cfg))
}

// ── Tool handler types ─────────────────────────────────────────────────────────

type BoxFuture<T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'static>>;
type ToolHandler  = Arc<dyn Fn(Value) -> BoxFuture<Result<Value, String>> + Send + Sync + 'static>;

fn register(
    agent:       &Arc<GossipAgent>,
    name:        &str,
    description: &str,
    params:      Value,
    handler:     ToolHandler,
) -> McpToolHandle {
    let schema = json!({ "description": description, "inputSchema": params });
    agent.register_mcp_tool(name, schema, move |args| {
        let h = Arc::clone(&handler);
        Box::pin(async move { h(args).await })
    })
}

// ── Tool implementations ───────────────────────────────────────────────────────

async fn tool_weather(args: Value) -> Result<Value, String> {
    let city = args["city"].as_str().unwrap_or("London").to_string();
    let url  = format!("https://wttr.in/{city}?format=j1");
    let resp = reqwest::Client::new()
        .get(&url)
        .header("User-Agent", "mycelium-demo/0.1")
        .timeout(Duration::from_secs(10))
        .send().await.map_err(|e| format!("weather request failed: {e}"))?
        .json::<Value>().await.map_err(|e| format!("weather parse failed: {e}"))?;
    let current = &resp["current_condition"][0];
    Ok(json!({
        "city":         city,
        "temp_c":       current["temp_C"].as_str().unwrap_or("?"),
        "feels_like_c": current["FeelsLikeC"].as_str().unwrap_or("?"),
        "description":  current["weatherDesc"][0]["value"].as_str().unwrap_or("unknown"),
        "humidity_pct": current["humidity"].as_str().unwrap_or("?"),
        "wind_kmph":    current["windspeedKmph"].as_str().unwrap_or("?"),
    }))
}

async fn tool_web_fetch(args: Value) -> Result<Value, String> {
    let url = args["url"].as_str()
        .ok_or_else(|| "missing url parameter".to_string())?
        .to_string();
    let body = reqwest::Client::new()
        .get(&url)
        .header("User-Agent", "mycelium-demo/0.1")
        .timeout(Duration::from_secs(15))
        .send().await.map_err(|e| format!("fetch failed: {e}"))?
        .text().await.map_err(|e| format!("body read failed: {e}"))?;
    let stripped: String = {
        let mut out = String::with_capacity(body.len().min(4096));
        let mut in_tag = false;
        for c in body.chars() {
            match c {
                '<' => in_tag = true,
                '>' => in_tag = false,
                _ if !in_tag => out.push(c),
                _ => {}
            }
            if out.len() >= 2000 { break; }
        }
        out.split_whitespace().collect::<Vec<_>>().join(" ")
    };
    Ok(json!({ "url": url, "content": stripped }))
}

async fn tool_calculate(args: Value) -> Result<Value, String> {
    let expr = args["expression"].as_str().unwrap_or("").trim().to_string();
    fn eval(tokens: &[&str]) -> Option<f64> {
        if tokens.len() == 3 {
            let a: f64 = tokens[0].parse().ok()?;
            let b: f64 = tokens[2].parse().ok()?;
            return match tokens[1] {
                "+" => Some(a + b),
                "-" => Some(a - b),
                "*" | "×" => Some(a * b),
                "/" | "÷" => if b != 0.0 { Some(a / b) } else { None },
                "^" | "**" => Some(a.powf(b)),
                "%" => Some(a % b),
                _ => None,
            };
        }
        None
    }
    let tokens: Vec<&str> = expr.split_whitespace().collect();
    let result = eval(&tokens)
        .ok_or_else(|| format!("cannot evaluate '{expr}' — expected 'a op b' (e.g. '330 * 1024')"))?;
    Ok(json!({ "expression": expr, "result": result }))
}

async fn tool_wiki(args: Value) -> Result<Value, String> {
    let topic = args["topic"].as_str().unwrap_or("").trim().to_string();
    if topic.is_empty() { return Err("missing topic parameter".into()); }

    let client = reqwest::Client::new();

    // Search for the canonical article title so natural-language queries work.
    let search: Value = client
        .get("https://en.wikipedia.org/w/api.php")
        .query(&[("action","query"),("list","search"),("srsearch",&topic),("format","json"),("srlimit","1")])
        .header("User-Agent", "mycelium-demo/0.1")
        .timeout(Duration::from_secs(10))
        .send().await.map_err(|e| format!("wiki search failed: {e}"))?
        .json().await.map_err(|e| format!("wiki search parse failed: {e}"))?;

    let canonical = search["query"]["search"][0]["title"]
        .as_str()
        .unwrap_or(&topic)
        .replace(' ', "_");

    let summary_url = format!("https://en.wikipedia.org/api/rest_v1/page/summary/{canonical}");
    let resp: Value = client.get(&summary_url)
        .header("User-Agent", "mycelium-demo/0.1")
        .timeout(Duration::from_secs(10))
        .send().await.map_err(|e| format!("wiki request failed: {e}"))?
        .json().await.map_err(|e| format!("wiki parse failed: {e}"))?;

    if resp["type"].as_str() == Some("disambiguation") || resp["extract"].is_null() {
        return Err(format!("'{topic}' is ambiguous or not found — try a more specific title"));
    }
    Ok(json!({
        "title":   resp["title"].as_str().unwrap_or(&topic),
        "summary": resp["extract"].as_str().unwrap_or("(no extract)"),
        "url":     resp["content_urls"]["desktop"]["page"].as_str().unwrap_or(""),
    }))
}

// ── SF Encyclopedia tool ───────────────────────────────────────────────────────

fn sfe_candidate_slugs(query: &str) -> Vec<String> {
    // Strip "by Author" and leading articles, then generate slug variants
    let q = query.to_lowercase();
    let q = if let Some(i) = q.find(" by ") { &q[..i] } else { q.as_str() };
    let q = q.trim();
    let stop = ["the", "a", "an", "of", "in", "and"];
    let words: Vec<&str> = q.split_whitespace().filter(|w| !stop.contains(w)).collect();
    if words.is_empty() { return vec![]; }

    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    let mut push = |s: String| { if seen.insert(s.clone()) { out.push(s); } };

    // Full phrase first
    let fwd = words.join("_");
    push(fwd.clone());
    push(format!("{fwd}_series"));

    // All windows of size 2 and 3 — both forward and reversed.
    // This catches "Dan Simmons Hyperion Cantos" → simmons_dan, hyperion_cantos, etc.
    for size in [2usize, 3, 1] {
        if size > words.len() { continue; }
        for start in 0..=(words.len() - size) {
            let w = &words[start..start + size];
            let f = w.join("_");
            let mut wr = w.to_vec();
            wr.reverse();
            let r = wr.join("_");
            push(f.clone());
            push(format!("{f}_series"));
            push(format!("{f}s"));
            if r != f {
                push(r.clone());
                push(format!("{r}_series"));
            }
        }
    }
    out
}

fn sfe_extract_text(html: &str) -> String {
    // Pull paragraphs from the entryArticle section
    let start = html.find("entryArticle").unwrap_or(0);
    let chunk = &html[start..];
    let mut out = String::new();
    let mut in_tag = false;
    for ch in chunk.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => { in_tag = false; out.push(' '); }
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }
    let out = out
        .replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
        .replace("&nbsp;", " ").replace("&#8217;", "'").replace("&#8216;", "'")
        .replace("&#8220;", "\u{201c}").replace("&#8221;", "\u{201d}")
        .replace("&#8212;", "—").replace("&#8211;", "–").replace("&ndash;", "–");
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

async fn tool_sf_lookup(args: Value) -> Result<Value, String> {
    let query = args["query"].as_str().unwrap_or("").trim().to_string();
    if query.is_empty() { return Err("missing query parameter".into()); }

    let client = reqwest::Client::new();
    let candidates = sfe_candidate_slugs(&query);

    for slug in &candidates {
        let url = format!("https://sf-encyclopedia.com/entry/{slug}");
        let resp = client.get(&url)
            .header("User-Agent", "mycelium-demo/0.1")
            .timeout(Duration::from_secs(12))
            .send().await.map_err(|e| format!("SFE3 request failed: {e}"))?;

        if !resp.status().is_success() { continue; }

        let html = resp.text().await.map_err(|e| format!("SFE3 read failed: {e}"))?;
        if html.contains("Sorry! The page was not found") { continue; }

        let text = sfe_extract_text(&html);
        // Skip the header boilerplate (nav + search form) — content starts after "Tagged:"
        let content = if let Some(i) = text.find("Tagged:") {
            text[i..].splitn(2, '.').nth(1).unwrap_or(&text).trim().to_string()
        } else {
            text
        };
        let excerpt: String = content.chars().take(1800).collect();

        return Ok(json!({
            "source": "SF Encyclopedia (SFE3) — scholarly critical reference",
            "title":  slug.replace('_', " "),
            "url":    url,
            "content": excerpt,
        }));
    }

    Err(format!(
        "No SFE3 entry found for '{}' — tried slugs: {}",
        query,
        candidates.join(", ")
    ))
}

// ── Tool node runners ──────────────────────────────────────────────────────────

async fn run_tool_a(agent: Arc<GossipAgent>, role: &str) {
    let _role_cap = agent.advertise_capability(Capability::new("role", "tool-a"), Duration::from_secs(5));
    let _weather = register(
        &agent, "weather",
        "Get current weather conditions for a city. Input: {\"city\": \"London\"}",
        json!({"type":"object","properties":{"city":{"type":"string","description":"City name"}},"required":["city"]}),
        Arc::new(|args| Box::pin(tool_weather(args))),
    );
    let _fetch = register(
        &agent, "web_fetch",
        "Fetch the text content of any URL. Input: {\"url\": \"https://...\"}",
        json!({"type":"object","properties":{"url":{"type":"string","description":"URL to fetch"}},"required":["url"]}),
        Arc::new(|args| Box::pin(tool_web_fetch(args))),
    );
    info!("[{role}] tools: weather, web_fetch — listening");
    loop { time::sleep(Duration::from_secs(60)).await; }
}

async fn run_tool_b(agent: Arc<GossipAgent>, role: &str) {
    let _role_cap = agent.advertise_capability(Capability::new("role", "tool-b"), Duration::from_secs(5));
    let _calc = register(
        &agent, "calculate",
        "Evaluate a simple arithmetic expression. Input: {\"expression\": \"330 * 1024\"}",
        json!({"type":"object","properties":{"expression":{"type":"string","description":"Expression like '330 * 1024'"}},"required":["expression"]}),
        Arc::new(|args| Box::pin(tool_calculate(args))),
    );
    let _wiki = register(
        &agent, "wiki",
        "Look up a Wikipedia article summary. Input: {\"topic\": \"Eiffel Tower\"}",
        json!({"type":"object","properties":{"topic":{"type":"string","description":"Wikipedia article title"}},"required":["topic"]}),
        Arc::new(|args| Box::pin(tool_wiki(args))),
    );
    info!("[{role}] tools: calculate, wiki — listening");
    loop { time::sleep(Duration::from_secs(60)).await; }
}

async fn run_tool_sf(agent: Arc<GossipAgent>, role: &str) {
    let _role_cap = agent.advertise_capability(Capability::new("role", "tool-sf"), Duration::from_secs(5));
    let _sf = register(
        &agent, "sf_lookup",
        "Look up science fiction and fantasy authors, novels, series, and themes in the \
         SF Encyclopedia (SFE3) — a scholarly critical reference with literary analysis, \
         historical context, and thematic depth well beyond Wikipedia. \
         Prefer this over wiki for any SF/fantasy query. \
         Input: {\"query\": \"Dan Simmons\"} or {\"query\": \"Hyperion Cantos\"}",
        json!({"type":"object","properties":{"query":{"type":"string","description":"Author name, book/series title, or SF theme (e.g. 'cyberpunk', 'time travel')"}},"required":["query"]}),
        Arc::new(|args| Box::pin(tool_sf_lookup(args))),
    );
    info!("[{role}] tools: sf_lookup — listening");
    loop { time::sleep(Duration::from_secs(60)).await; }
}

// ── Mesh helpers ───────────────────────────────────────────────────────────────

fn discover_tools(agent: &GossipAgent) -> Vec<(String, String, Value)> {
    let mut seen: std::collections::HashSet<String> = Default::default();
    let mut tools = Vec::new();
    for (key, schema_bytes) in agent.scan_prefix("tools/") {
        let parts: Vec<&str> = key.splitn(3, '/').collect();
        if parts.len() != 3 { continue; }
        let tool_name    = parts[1].to_string();
        let node_id_str  = parts[2].to_string();
        if !seen.insert(tool_name.clone()) { continue; }
        let Ok(schema) = serde_json::from_slice::<Value>(&schema_bytes) else { continue };
        let input_schema = schema.get("inputSchema").cloned()
            .unwrap_or_else(|| json!({"type":"object","properties":{}}));
        let description = schema["description"].as_str().unwrap_or("").to_string();
        tools.push((tool_name.clone(), node_id_str, json!({
            "type": "function",
            "function": { "name": tool_name, "description": description, "parameters": input_schema }
        })));
    }
    tools
}

fn find_tool_node(agent: &GossipAgent, tool_name: &str) -> Option<String> {
    let entries = agent.scan_prefix(&format!("tools/{tool_name}/"));
    let (key, _) = entries.into_iter().next()?;
    let parts: Vec<&str> = key.splitn(3, '/').collect();
    if parts.len() < 3 { return None; }
    Some(parts[2].to_string())
}

async fn invoke_tool(agent: &GossipAgent, tool_name: &str, args: Value) -> Result<Value, String> {
    let entries = agent.scan_prefix(&format!("tools/{tool_name}/"));
    let (key, _) = entries.into_iter().next()
        .ok_or_else(|| format!("no provider for tool '{tool_name}'"))?;
    let parts: Vec<&str> = key.splitn(3, '/').collect();
    let nid: NodeId = parts[2].parse().map_err(|e: mycelium::GossipError| e.to_string())?;
    let rpc_req = json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": tool_name, "arguments": args }
    });
    info!("[llm] → {tool_name}({args}) via {nid}");
    let reply = agent.rpc_call(
        nid, signal_kind::MCP_INVOKE,
        Bytes::from(rpc_req.to_string()),
        Duration::from_secs(30),
    ).await.map_err(|e| e.to_string())?;
    let resp: Value = serde_json::from_slice(&reply).map_err(|e| e.to_string())?;
    if let Some(err) = resp.get("error") {
        return Err(err["message"].as_str().unwrap_or("tool error").to_string());
    }
    Ok(resp["result"].clone())
}

// ── LLM step ───────────────────────────────────────────────────────────────────

struct ToolCallReq {
    id:   String,
    name: String,
    args: Value,
}

enum LlmStep {
    ToolCalls(Vec<ToolCallReq>),
    Answer(String),
}

async fn llm_step(cfg: &LlmCfg, messages: &[Value], tool_defs: &[Value]) -> Result<LlmStep, String> {
    let mut req_body = json!({ "model": cfg.model, "messages": messages });
    if !tool_defs.is_empty() {
        req_body["tools"]       = json!(tool_defs);
        req_body["tool_choice"] = json!("auto");
    }
    let resp = reqwest::Client::new()
        .post(format!("{}/chat/completions", cfg.base_url))
        .bearer_auth(&cfg.api_key)
        .json(&req_body)
        .timeout(Duration::from_secs(120))
        .send().await.map_err(|e| format!("LLM request failed: {e}"))?
        .json::<Value>().await.map_err(|e| format!("LLM parse failed: {e}"))?;

    if let Some(err) = resp.get("error") {
        return Err(format!("LLM error: {}", err["message"].as_str().unwrap_or("unknown")));
    }
    let msg = &resp["choices"][0]["message"];
    if let Some(tcs) = msg["tool_calls"].as_array() {
        if !tcs.is_empty() {
            let calls: Vec<ToolCallReq> = tcs.iter().filter_map(|tc| {
                let id   = tc["id"].as_str()?.to_string();
                let name = tc["function"]["name"].as_str()?.to_string();
                let args: Value = serde_json::from_str(
                    tc["function"]["arguments"].as_str().unwrap_or("{}"),
                ).unwrap_or(json!({}));
                Some(ToolCallReq { id, name, args })
            }).collect();
            if !calls.is_empty() {
                return Ok(LlmStep::ToolCalls(calls));
            }
        }
    }
    Ok(LlmStep::Answer(msg["content"].as_str().unwrap_or("").to_string()))
}

// ── Planning cycle (spawned per user message) ──────────────────────────────────

async fn planning_cycle(state: Arc<AppState>) {
    // If tool nodes aren't visible yet, wait briefly
    if discover_tools(&state.agent).is_empty() {
        let _ = state.tx.send(ChatEvent::Thinking {
            content: "Waiting for tool nodes to join the mesh...".into(),
        });
        for _ in 0..10u8 {
            time::sleep(Duration::from_secs(2)).await;
            if !discover_tools(&state.agent).is_empty() { break; }
        }
    }

    let tools_info = discover_tools(&state.agent);
    let tool_defs: Vec<Value> = tools_info.iter().map(|(_, _, d)| d.clone()).collect();
    let _ = state.tx.send(ChatEvent::Thinking {
        content: format!("Planning with {} tool(s)...", tools_info.len()),
    });

    let mut messages: Vec<Value> = state.history.lock().await.clone();

    let mut turn = 0usize;
    loop {
        if turn >= MAX_TURNS {
            let _ = state.tx.send(ChatEvent::Error {
                message: format!("Reached max turns ({MAX_TURNS}) without a final answer."),
            });
            break;
        }
        match llm_step(&state.cfg, &messages, &tool_defs).await {
            Err(e) => {
                let _ = state.tx.send(ChatEvent::Error { message: e });
                break;
            }
            Ok(LlmStep::Answer(content)) => {
                messages.push(json!({"role": "assistant", "content": &content}));
                let _ = state.tx.send(ChatEvent::Assistant { content });
                break;
            }
            Ok(LlmStep::ToolCalls(calls)) => {
                let tc_array: Vec<Value> = calls.iter().map(|tc| json!({
                    "id": tc.id, "type": "function",
                    "function": {"name": tc.name, "arguments": tc.args.to_string()}
                })).collect();
                messages.push(json!({"role": "assistant", "content": null, "tool_calls": tc_array}));

                for tc in &calls {
                    let node_id = find_tool_node(&state.agent, &tc.name)
                        .unwrap_or_else(|| "unknown".into());
                    let _ = state.tx.send(ChatEvent::ToolCall {
                        tool: tc.name.clone(), node_id, args: tc.args.clone(),
                    });
                    match invoke_tool(&state.agent, &tc.name, tc.args.clone()).await {
                        Ok(result) => {
                            let _ = state.tx.send(ChatEvent::ToolResult {
                                tool: tc.name.clone(), result: result.clone(),
                            });
                            messages.push(json!({
                                "role": "tool", "tool_call_id": tc.id,
                                "content": result.to_string()
                            }));
                        }
                        Err(e) => {
                            let _ = state.tx.send(ChatEvent::ToolError {
                                tool: tc.name.clone(), error: e.clone(),
                            });
                            messages.push(json!({
                                "role": "tool", "tool_call_id": tc.id,
                                "content": format!("Error: {e}")
                            }));
                        }
                    }
                }
                turn += 1;
            }
        }
    }

    // Persist final conversation state for multi-turn follow-ups
    *state.history.lock().await = messages;
    state.busy.store(false, Ordering::SeqCst);
    let _ = state.tx.send(ChatEvent::Idle);
}

// ── HTTP handlers ──────────────────────────────────────────────────────────────

async fn handle_root() -> Response {
    Html(chat_html()).into_response()
}

#[derive(Deserialize)]
struct ChatReq { message: String }

async fn handle_chat(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ChatReq>,
) -> Response {
    if req.message.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "empty message"}))).into_response();
    }
    if state.busy.swap(true, Ordering::SeqCst) {
        return (StatusCode::CONFLICT, Json(json!({"error": "busy — please wait for the current reply"}))).into_response();
    }
    let content = req.message.trim().to_string();
    state.history.lock().await.push(json!({"role": "user", "content": &content}));
    let _ = state.tx.send(ChatEvent::UserMessage { content });
    tokio::spawn(planning_cycle(Arc::clone(&state)));
    (StatusCode::ACCEPTED, Json(json!({"ok": true}))).into_response()
}

async fn handle_stream(
    State(state): State<Arc<AppState>>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let rx = state.tx.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|msg| async move {
        let event = msg.ok()?;
        let data  = serde_json::to_string(&event).ok()?;
        Some(Ok::<_, Infallible>(Event::default().data(data)))
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn handle_mesh(State(state): State<Arc<AppState>>) -> Json<Value> {
    let tools: Vec<Value> = discover_tools(&state.agent)
        .into_iter()
        .map(|(name, node_id, def)| json!({
            "name":        name,
            "node_id":     node_id,
            "description": def["function"]["description"].as_str().unwrap_or("")
        }))
        .collect();
    Json(json!({ "tools": tools, "model": state.cfg.model }))
}

// ── Chat server ────────────────────────────────────────────────────────────────

async fn run_chat_server(agent: Arc<GossipAgent>, cfg: LlmCfg, chat_port: u16) {
    let _role_cap = agent.advertise_capability(Capability::new("role", "llm"), Duration::from_secs(5));
    info!("[llm] waiting {TOOL_SETTLE_SECS}s for mesh to converge...");
    time::sleep(Duration::from_secs(TOOL_SETTLE_SECS)).await;

    let (tx, _) = broadcast::channel::<ChatEvent>(512);
    let state = Arc::new(AppState {
        agent,
        cfg: cfg.clone(),
        history: Mutex::new(vec![json!({"role": "system", "content":
            "You are a helpful assistant with access to tools for weather lookups, \
             arithmetic calculations, Wikipedia summaries, and web page fetching. \
             Use the available tools whenever they help answer the user's question. \
             Keep answers concise and factual."})]),
        tx,
        busy: AtomicBool::new(false),
    });

    let router = Router::new()
        .route("/",       get(handle_root))
        .route("/chat",   post(handle_chat))
        .route("/stream", get(handle_stream))
        .route("/mesh",   get(handle_mesh))
        .with_state(Arc::clone(&state));

    let addr = format!("0.0.0.0:{chat_port}");
    info!("[llm] Chat UI ready → http://{addr}/  (model: {})", cfg.model);
    let listener = tokio::net::TcpListener::bind(&addr).await
        .expect("bind chat port");
    axum::serve(listener, router).await.expect("serve chat");
}

// ── Management dashboard ───────────────────────────────────────────────────────

struct MgmtState {
    agent: Arc<GossipAgent>,
}

async fn mgmt_handle_root() -> Response {
    Html(mgmt_html()).into_response()
}

async fn mgmt_kv_scan(
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    State(s): State<Arc<MgmtState>>,
) -> Json<Value> {
    let prefix = params.get("prefix").map(|s| s.as_str()).unwrap_or("cap/");
    let entries: Vec<Value> = s.agent.scan_prefix(prefix)
        .into_iter()
        .map(|(k, v)| json!({ "key": k.as_ref(), "bytes": v.len() }))
        .collect();
    Json(json!({ "prefix": prefix, "count": entries.len(), "entries": entries }))
}

async fn mgmt_handle_state(State(s): State<Arc<MgmtState>>) -> Json<Value> {
    let agent = &s.agent;

    // tools/ → which node hosts which tool
    let tools_info = discover_tools(agent);
    let mut node_tools: std::collections::HashMap<String, Vec<String>> = Default::default();
    for (tool_name, node_id_str, _) in &tools_info {
        node_tools.entry(node_id_str.clone()).or_default().push(tool_name.clone());
    }

    // role/* capabilities → map node_id → role label.
    // Use with_max_age so crashed nodes (no tombstone) age out after 30s.
    // Iterate highest-priority roles first and use entry().or_insert() so a
    // node that briefly held two roles (e.g. during a restart race) keeps the
    // first-seen (higher-priority) label rather than the last-inserted one.
    let liveness = Duration::from_secs(30); // 6× the 5s re-advertisement interval
    let mut node_roles: std::collections::HashMap<String, String> = Default::default();
    for role_name in &["mgmt", "tool-a", "tool-b", "llm", "node"] {
        for (nid, _cap) in agent.resolve(&CapFilter::new("role", *role_name).with_max_age(liveness)) {
            node_roles.entry(nid.to_string()).or_insert_with(|| role_name.to_string());
        }
    }

    let my_id = agent.node_id().to_string();
    node_roles.entry(my_id.clone()).or_insert_with(|| "mgmt".into());

    // direct TCP peers this node has open connections to right now
    let tcp_peers: std::collections::HashSet<String> =
        agent.peers().iter().map(|n| n.to_string()).collect();

    // union all known node IDs (from role caps, tool registrations, TCP peers)
    let mut all_ids: std::collections::HashSet<String> = node_roles.keys().cloned().collect();
    all_ids.extend(node_tools.keys().cloned());
    all_ids.extend(tcp_peers.iter().cloned());
    all_ids.insert(my_id.clone());

    let mut nodes: Vec<Value> = all_ids.into_iter().map(|id| {
        let role        = node_roles.get(&id).cloned().unwrap_or_else(|| "unknown".into());
        let tools       = node_tools.get(&id).cloned().unwrap_or_default();
        let tcp_live    = id == my_id || tcp_peers.contains(&id);
        json!({ "id": id, "role": role, "tools": tools, "is_self": id == my_id, "tcp": tcp_live })
    }).collect();
    let order = |r: &str| match r { "tool-a"=>0, "tool-b"=>1, "llm"=>2, "mgmt"=>3, _=>4 };
    nodes.sort_by_key(|n| order(n["role"].as_str().unwrap_or("")));

    Json(json!({
        "nodes":      nodes,
        "tool_count": tools_info.len(),
        "tcp_peers":  tcp_peers.len(),
        "self_id":    my_id,
    }))
}

async fn run_mgmt_server(agent: Arc<GossipAgent>, mgmt_port: u16) {
    let _role_cap = agent.advertise_capability(Capability::new("role", "mgmt"), Duration::from_secs(5));

    let state = Arc::new(MgmtState { agent });
    let router = Router::new()
        .route("/",              get(mgmt_handle_root))
        .route("/health",        get(|| async { StatusCode::OK }))
        .route("/api/state",     get(mgmt_handle_state))
        .route("/api/kv-scan",   get(mgmt_kv_scan))
        .with_state(Arc::clone(&state));

    let addr = format!("0.0.0.0:{mgmt_port}");
    info!("[mgmt] Dashboard: http://{addr}/");
    let listener = tokio::net::TcpListener::bind(&addr).await.expect("bind mgmt port");
    axum::serve(listener, router).await.expect("serve mgmt");
}

// ── Test node role ─────────────────────────────────────────────────────────────

struct NodeState {
    agent:         Arc<GossipAgent>,
    mailbox_count: Arc<std::sync::atomic::AtomicUsize>,
    _bulk_handle:  BulkServeHandle,
    _mbox_handle:  MailboxHandle,
    _role_cap:     CapabilityHandle,
}

async fn node_scatter(State(s): State<Arc<NodeState>>) -> Json<Value> {
    let peers = s.agent.peers();
    if peers.is_empty() {
        return Json(json!({ "ok": false, "reason": "no peers", "responders": 0 }));
    }
    let targets = peers.clone();
    match s.agent.scatter_gather(
        targets,
        "echo-scatter",
        Bytes::from_static(b"ping"),
        Duration::from_secs(5),
        1,
    ).await {
        Ok(results) => Json(json!({ "ok": true, "responders": results.len() })),
        Err(e)      => Json(json!({ "ok": false, "reason": e.to_string(), "responders": 0 })),
    }
}

async fn node_bulk_echo_peer(State(s): State<Arc<NodeState>>) -> Json<Value> {
    // Only bulk-call node-role peers — other roles (mgmt, etc.) don't register bulk_serve.
    let self_id = s.agent.node_id().clone();
    let liveness = std::time::Duration::from_secs(30);
    let Some(target) = s.agent
        .resolve(&CapFilter::new("role", "node").with_max_age(liveness))
        .into_iter()
        .map(|(nid, _)| nid)
        .find(|nid| *nid != self_id)
    else {
        return Json(json!({ "ok": false, "reason": "no node-role peers" }));
    };
    let payload = Bytes::from(vec![b'x'; 4096]);
    match s.agent.bulk_call(target.clone(), "echo-bulk", payload, Duration::from_secs(10)).await {
        Ok(result) => Json(json!({ "ok": true, "target": target.to_string(), "echoed_size": result.len() })),
        Err(e)     => Json(json!({ "ok": false, "reason": e.to_string() })),
    }
}

/// Serves staged bulk payloads to bulk_serve targets.
async fn node_bulk_fetch(
    Path(corr_id): Path<String>,
    State(s):      State<Arc<NodeState>>,
) -> impl axum::response::IntoResponse {
    let nonce = match u64::from_str_radix(corr_id.trim_start_matches("0x"), 16) {
        Ok(n)  => n,
        Err(_) => return (StatusCode::BAD_REQUEST, vec![]).into_response(),
    };
    match s.agent.bulk_staging_get(nonce) {
        Some(bytes) => (StatusCode::OK, bytes.to_vec()).into_response(),
        None        => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn node_deliver_to_self(State(s): State<Arc<NodeState>>) -> Json<Value> {
    let self_id = s.agent.node_id().clone();
    let ok = s.agent.deliver_event(&self_id, "test-mailbox", Bytes::from_static(b"hello-mailbox"));
    Json(json!({ "ok": ok }))
}

async fn node_mailbox_count(State(s): State<Arc<NodeState>>) -> Json<Value> {
    let count = s.mailbox_count.load(std::sync::atomic::Ordering::Acquire);
    Json(json!({ "count": count }))
}

async fn node_kv_get(
    State(s): State<Arc<NodeState>>,
    Path(key): Path<String>,
) -> Response {
    match s.agent.get(&key) {
        Some(val) => (StatusCode::OK, val.to_vec()).into_response(),
        None      => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn node_kv_put(
    State(s): State<Arc<NodeState>>,
    Path(key): Path<String>,
    body: String,
) -> StatusCode {
    let _ = s.agent.set_async(key, Bytes::from(body.into_bytes())).await;
    StatusCode::NO_CONTENT
}

async fn node_emit(
    State(s): State<Arc<NodeState>>,
    Path(kind): Path<String>,
    body: String,
) -> StatusCode {
    let _ = s.agent.emit(kind.as_str(), SignalScope::System, Bytes::from(body.into_bytes()));
    StatusCode::ACCEPTED
}

/// Builds node-role axum routes and wires up background handlers.
///
/// Returns a `Router<()>` ready to be passed to [`GossipAgent::with_http_routes`].
/// Must be called before [`GossipAgent::start`].
fn init_node_routes(agent: Arc<GossipAgent>) -> axum::Router {
    let role_cap = agent.advertise_capability(Capability::new("role", "node"), Duration::from_secs(5));

    // Record test.signal arrivals under a per-hostname key so each node's
    // reception can be queried independently in integration tests.
    let hostname  = std::env::var("HOSTNAME").unwrap_or_else(|_| agent.node_id().to_string());
    let sig_key   = format!("sig-received/{}", hostname);
    let mut sig_rx = agent.signal_rx("test.signal");
    let sig_agent  = Arc::clone(&agent);
    tokio::spawn(async move {
        while let Some(sig) = sig_rx.recv().await {
            let _ = sig_agent.set(sig_key.clone(), sig.payload.clone());
        }
    });

    // Register an echo-scatter responder so scatter_gather works from peers.
    let sc_agent = Arc::clone(&agent);
    tokio::spawn(async move {
        let mut rx = sc_agent.signal_rx("echo-scatter");
        while let Some(req) = rx.recv().await {
            let req = mycelium::RpcRequest::from(req);
            sc_agent.rpc_respond(&req, req.payload());
        }
    });

    let bulk_handle = agent.bulk_serve("echo-bulk", |_sender, payload| async move { payload });

    let mailbox_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (mbox_handle, mut mbox_rx) = agent.open_mailbox("test-mailbox", 64);
    let mc = Arc::clone(&mailbox_count);
    tokio::spawn(async move {
        while mbox_rx.recv().await.is_some() {
            mc.fetch_add(1, std::sync::atomic::Ordering::Release);
        }
    });

    let state = Arc::new(NodeState {
        agent,
        mailbox_count,
        _bulk_handle: bulk_handle,
        _mbox_handle: mbox_handle,
        _role_cap:    role_cap,
    });

    // /health and /ready are omitted — the embedded gateway already provides them.
    // /bulk/{corr_id} is also omitted — bulk_staging_handler in the gateway is equivalent.
    Router::new()
        .route("/kv/{*key}",         get(node_kv_get).put(node_kv_put))
        .route("/emit/{kind}",       post(node_emit))
        .route("/scatter",           post(node_scatter))
        .route("/bulk-echo-peer",    post(node_bulk_echo_peer))
        .route("/deliver-to-self",   post(node_deliver_to_self))
        .route("/mailbox-count",     get(node_mailbox_count))
        .with_state(state)
}

async fn run_node(_agent: Arc<GossipAgent>, role: &str) {
    info!("[{role}] HTTP routes registered in embedded gateway");
    // All work runs in the background tasks and axum handlers registered via init_node_routes.
    loop { tokio::time::sleep(Duration::from_secs(60)).await; }
}

// ── Inline chat UI ─────────────────────────────────────────────────────────────

fn chat_html() -> &'static str {
    r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Mycelium Chat</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',system-ui,sans-serif;background:#0f0f1a;color:#e2e8f0;height:100vh;display:flex;flex-direction:column}
header{background:#1a1a2e;border-bottom:1px solid #2d2d4e;padding:12px 20px;display:flex;align-items:center;gap:12px;flex-shrink:0}
header h1{font-size:1.05rem;font-weight:600;color:#a78bfa}
#mesh-info{font-size:0.72rem;color:#64748b;margin-left:auto;text-align:right;line-height:1.4}
#chat{flex:1;overflow-y:auto;padding:16px 20px;display:flex;flex-direction:column;gap:10px}
.bubble{max-width:76%;padding:10px 14px;border-radius:14px;line-height:1.6;font-size:0.88rem;white-space:pre-wrap;word-break:break-word}
.bubble.user{align-self:flex-end;background:#4c1d95;color:#ede9fe;border-bottom-right-radius:4px}
.bubble.assistant{align-self:flex-start;background:#1e293b;color:#e2e8f0;border-bottom-left-radius:4px}
.bubble.thinking{align-self:flex-start;background:#0f172a;color:#475569;font-style:italic;border:1px dashed #2d2d4e;border-bottom-left-radius:4px;font-size:0.8rem}
.bubble.error-msg{align-self:center;background:#2d1a1a;color:#f87171;border:1px solid #7f1d1d;font-size:0.82rem;text-align:center;border-radius:8px}
.tool-card{align-self:flex-start;background:#0d1117;border:1px solid #21262d;border-radius:10px;padding:10px 14px;font-size:0.78rem;max-width:84%;font-family:monospace}
.tool-name{color:#79c0ff;font-weight:700;font-size:0.82rem}
.tool-node{color:#6e7681;font-size:0.68rem;margin-top:2px}
.tool-args{color:#adbac7;margin-top:6px;opacity:0.85;white-space:pre-wrap}
.tool-result{color:#56d364;margin-top:6px;white-space:pre-wrap}
.tool-err{color:#f85149;margin-top:6px}
#input-area{background:#1a1a2e;border-top:1px solid #2d2d4e;padding:12px 16px;display:flex;gap:8px;flex-shrink:0}
#msg{flex:1;background:#0d1117;border:1px solid #2d2d4e;border-radius:10px;padding:10px 14px;color:#e2e8f0;font-size:0.88rem;outline:none;resize:none;font-family:inherit;line-height:1.4;max-height:140px;overflow-y:auto}
#msg:focus{border-color:#6d28d9}
#msg::placeholder{color:#475569}
#msg:disabled,#send:disabled{opacity:0.45;cursor:not-allowed}
#send{background:#6d28d9;color:#ede9fe;border:none;border-radius:10px;padding:10px 22px;cursor:pointer;font-size:0.88rem;transition:background .15s;white-space:nowrap;align-self:flex-end}
#send:hover:not(:disabled){background:#7c3aed}
::-webkit-scrollbar{width:5px}
::-webkit-scrollbar-track{background:#0f0f1a}
::-webkit-scrollbar-thumb{background:#2d2d4e;border-radius:3px}
</style>
</head>
<body>
<header>
  <h1>&#127812; Mycelium Chat</h1>
  <div id="mesh-info">connecting…</div>
</header>
<div id="chat"></div>
<div id="input-area">
  <textarea id="msg" rows="1" placeholder="Ask anything — weather, maths, Wikipedia, web…"></textarea>
  <button id="send">Send</button>
</div>
<script>
(function(){
var chat=document.getElementById('chat');
var msg=document.getElementById('msg');
var send=document.getElementById('send');
var lastCard=null;

function esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}

function addBubble(cls,text){
  var d=document.createElement('div');
  d.className='bubble '+cls;
  d.textContent=text;
  chat.appendChild(d);
  chat.scrollTop=chat.scrollHeight;
  return d;
}

function addCard(tool,nodeId,args){
  var d=document.createElement('div');
  d.className='tool-card';
  d.innerHTML='<div class="tool-name">&#9889; '+esc(tool)+'()</div>'
    +'<div class="tool-node">node: '+esc(nodeId)+'</div>'
    +'<div class="tool-args">'+esc(JSON.stringify(args,null,2))+'</div>';
  chat.appendChild(d);
  chat.scrollTop=chat.scrollHeight;
  return d;
}

function appendToCard(card,cls,content){
  var d=document.createElement('div');
  d.className=cls;
  d.textContent=content;
  card.appendChild(d);
  chat.scrollTop=chat.scrollHeight;
}

function setBusy(v){
  msg.disabled=v;
  send.disabled=v;
  if(!v){msg.focus();}
}

var es=new EventSource('/stream');
es.onmessage=function(e){
  var ev=JSON.parse(e.data);
  if(ev.type==='user_message'){addBubble('user',ev.content);lastCard=null;}
  else if(ev.type==='thinking'){addBubble('thinking',ev.content);}
  else if(ev.type==='tool_call'){lastCard=addCard(ev.tool,ev.node_id,ev.args);}
  else if(ev.type==='tool_result'){if(lastCard)appendToCard(lastCard,'tool-result',JSON.stringify(ev.result,null,2));}
  else if(ev.type==='tool_error'){if(lastCard)appendToCard(lastCard,'tool-err','Error: '+ev.error);}
  else if(ev.type==='assistant'){addBubble('assistant',ev.content);lastCard=null;}
  else if(ev.type==='error'){addBubble('error-msg',ev.message);}
  else if(ev.type==='idle'){setBusy(false);}
};
es.onerror=function(){
  document.getElementById('mesh-info').textContent='stream disconnected — reload to reconnect';
};

function doSend(){
  var text=msg.value.trim();
  if(!text||send.disabled)return;
  msg.value='';
  msg.style.height='auto';
  setBusy(true);
  fetch('/chat',{
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({message:text})
  }).then(function(r){
    if(!r.ok){r.json().then(function(j){addBubble('error-msg',j.error||'Send failed');setBusy(false);});}
  }).catch(function(e){addBubble('error-msg','Network error: '+e);setBusy(false);});
}

send.addEventListener('click',doSend);
msg.addEventListener('keydown',function(e){
  if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();doSend();}
});
msg.addEventListener('input',function(){
  this.style.height='auto';
  this.style.height=Math.min(this.scrollHeight,140)+'px';
});

fetch('/mesh').then(function(r){return r.json();}).then(function(d){
  var names=d.tools.map(function(t){return t.name;}).join(', ');
  document.getElementById('mesh-info').textContent=
    d.tools.length+' tools: '+names+'\nmodel: '+d.model;
}).catch(function(){});
})();
</script>
</body>
</html>"##
}

fn mgmt_html() -> &'static str {
    r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Mycelium — Mesh Dashboard</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',system-ui,sans-serif;background:#0f0f1a;color:#e2e8f0;min-height:100vh}
header{background:#1a1a2e;border-bottom:1px solid #2d2d4e;padding:14px 24px;display:flex;align-items:center;gap:12px}
header h1{font-size:1.05rem;font-weight:700;color:#a78bfa}
#status{font-size:0.75rem;color:#64748b;margin-left:auto}
main{max-width:900px;margin:0 auto;padding:24px 20px}
h2{font-size:0.78rem;font-weight:600;color:#475569;text-transform:uppercase;letter-spacing:.08em;margin-bottom:12px}
#nodes{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:14px;margin-bottom:32px}
.node-card{background:#1e293b;border:1px solid #2d2d4e;border-radius:12px;padding:16px}
.node-card.self{border-color:#4c1d95}
.role-badge{display:inline-block;font-size:0.7rem;font-weight:700;padding:2px 9px;border-radius:99px;margin-bottom:10px;text-transform:uppercase;letter-spacing:.06em}
.role-tool-a{background:#0f3460;color:#60a5fa}
.role-tool-b{background:#0f3450;color:#34d399}
.role-llm{background:#3b0764;color:#c084fc}
.role-mgmt{background:#1e3a5f;color:#f59e0b}
.role-unknown{background:#1e293b;color:#64748b}
.node-id{font-family:monospace;font-size:0.72rem;color:#475569;word-break:break-all;margin-bottom:8px}
.tool-list{display:flex;flex-wrap:wrap;gap:4px;margin-top:6px}
.tool-chip{font-size:0.68rem;background:#0d1117;border:1px solid #21262d;color:#79c0ff;border-radius:6px;padding:2px 7px;font-family:monospace}
.no-tools{font-size:0.75rem;color:#334155;font-style:italic}
.self-label{font-size:0.68rem;color:#f59e0b;margin-top:8px}
#summary{background:#1e293b;border:1px solid #2d2d4e;border-radius:10px;padding:14px 18px;display:flex;gap:28px;margin-bottom:24px;flex-wrap:wrap}
.stat{display:flex;flex-direction:column;gap:2px}
.stat-val{font-size:1.4rem;font-weight:700;color:#a78bfa;line-height:1}
.stat-label{font-size:0.72rem;color:#64748b}
.chat-link{display:inline-block;margin-top:16px;background:#6d28d9;color:#ede9fe;border-radius:8px;padding:9px 20px;text-decoration:none;font-size:0.85rem;transition:background .15s}
.chat-link:hover{background:#7c3aed}
::-webkit-scrollbar{width:5px}::-webkit-scrollbar-track{background:#0f0f1a}::-webkit-scrollbar-thumb{background:#2d2d4e;border-radius:3px}
</style>
</head>
<body>
<header>
  <h1>&#127812; Mycelium Mesh Dashboard</h1>
  <div id="status">connecting…</div>
</header>
<main>
  <div id="summary">
    <div class="stat"><div class="stat-val" id="s-nodes">—</div><div class="stat-label">Nodes (gossip)</div></div>
    <div class="stat"><div class="stat-val" id="s-tcp">—</div><div class="stat-label">TCP peers (live)</div></div>
    <div class="stat"><div class="stat-val" id="s-tools">—</div><div class="stat-label">Tools</div></div>
    <div class="stat"><div class="stat-val" id="s-refresh">—</div><div class="stat-label">Last refresh</div></div>
  </div>
  <h2>Active Nodes</h2>
  <div id="nodes"><div style="color:#475569;font-size:0.85rem">Loading…</div></div>
  <a href="http://localhost:8080" target="_blank" class="chat-link">&#128172; Open Chat UI</a>
</main>
<script>
(function(){
var ROLE_LABELS={'tool-a':'tool-a','tool-b':'tool-b','llm':'llm','mgmt':'mgmt','unknown':'?'};
function esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function pad2(n){return n<10?'0'+n:String(n);}
function fmtTime(d){return pad2(d.getHours())+':'+pad2(d.getMinutes())+':'+pad2(d.getSeconds());}
function roleClass(r){var m={'tool-a':'role-tool-a','tool-b':'role-tool-b','llm':'role-llm','mgmt':'role-mgmt'};return m[r]||'role-unknown';}

async function refresh(){
  try{
    var r=await fetch('/api/state');
    if(!r.ok)throw new Error('status '+r.status);
    var d=await r.json();
    document.getElementById('s-nodes').textContent=d.nodes.length;
    document.getElementById('s-tcp').textContent=d.tcp_peers+' / '+(d.nodes.length-1);
    document.getElementById('s-tools').textContent=d.tool_count;
    document.getElementById('s-refresh').textContent=fmtTime(new Date());
    var allTcp=d.tcp_peers>=(d.nodes.length-1);
    document.getElementById('status').textContent=(allTcp?'&#10003;':'&#9888;')+' '
      +d.tcp_peers+'/'+(d.nodes.length-1)+' TCP peers connected · refreshes every 3s';

    var grid=document.getElementById('nodes');
    grid.innerHTML=d.nodes.map(function(n){
      var tools=n.tools.length
        ?n.tools.map(function(t){return '<span class="tool-chip">'+esc(t)+'</span>';}).join('')
        :'<span class="no-tools">no tools</span>';
      var tcpDot=n.is_self?''
        :(n.tcp?'<span style="color:#56d364;font-size:0.7rem;">&#11044; TCP</span>'
               :'<span style="color:#f85149;font-size:0.7rem;">&#11044; TCP</span>');
      var self=n.is_self?'<div class="self-label">&#9654; this node</div>':'';
      return '<div class="node-card'+(n.is_self?' self':'')+'"><span class="role-badge '+roleClass(n.role)+'">'+esc(n.role)+'</span>'
        +tcpDot
        +'<div class="node-id">'+esc(n.id)+'</div>'
        +'<div class="tool-list">'+tools+'</div>'
        +self+'</div>';
    }).join('');
  }catch(e){
    document.getElementById('status').textContent='&#9888; offline — retrying';
  }
}
refresh();
setInterval(refresh,3000);
})();
</script>
</body>
</html>"##
}

// ── main ───────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("RUST_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let role = std::env::var("MYCELIUM_ROLE").unwrap_or_else(|_| "tool-a".to_string());
    let port: u16 = std::env::var("MYCELIUM_PORT")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(GOSSIP_PORT_DEFAULT);
    let http_port: u16 = std::env::var("MYCELIUM_HTTP_PORT")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(HTTP_PORT_DEFAULT);
    let chat_port: u16 = std::env::var("CHAT_PORT")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(CHAT_PORT_DEFAULT);
    let mgmt_port: u16 = std::env::var("MGMT_PORT")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(MGMT_PORT_DEFAULT);
    let peer_list = std::env::var("MYCELIUM_PEERS").unwrap_or_default();

    let hostname = std::env::var("MYCELIUM_HOSTNAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| "127.0.0.1".to_string());
    let my_ip = if hostname.parse::<std::net::IpAddr>().is_ok() {
        hostname.clone()
    } else {
        match resolve_ip(&hostname).await {
            Ok(ip) => ip,
            Err(e) => {
                warn!("Could not resolve own hostname '{hostname}': {e} — using 127.0.0.1");
                "127.0.0.1".to_string()
            }
        }
    };

    let data_dir = std::env::var("MYCELIUM_DATA_DIR").ok().map(std::path::PathBuf::from);

    let peers = resolve_peers(&peer_list).await;
    info!(role=%role, my_ip=%my_ip, port, http_port, peers=peers.len(), "starting");
    let agent = make_agent(&my_ip, peers, port, Some(http_port), data_dir).await;

    // For the node role, register application routes into the embedded gateway before start.
    if role == "node" {
        agent.set_bulk_serving_port(http_port);
        let extra = init_node_routes(Arc::clone(&agent));
        agent.with_http_routes(extra);
    }

    agent.start().await.expect("agent start");
    info!("[{role}] node_id={}", agent.node_id());

    // For the `node` role, register a test/echo prompt skill backed by EchoBackend so
    // integration scenario 12 can verify cross-node KV propagation and invocation.
    #[cfg(feature = "llm")]
    let _llm_skill_handle: Option<PromptSkillHandle> = if role == "node" {
        let template = PromptTemplate {
            system: "Mycelium test echo skill.".into(),
            user_template: "{{input}}".into(),
            max_tokens: 64,
            temperature: 0.0,
            metadata: std::collections::HashMap::new(),
        };
        let backend: std::sync::Arc<dyn LlmBackend> = std::sync::Arc::new(EchoBackend);
        match Arc::clone(&agent).register_prompt_skill("test", "echo", template, backend).await {
            Ok(h)  => { info!("[node] test/echo prompt skill registered"); Some(h) }
            Err(e) => { warn!("[node] failed to register prompt skill: {e}"); None }
        }
    } else {
        None
    };

    match role.as_str() {
        "tool-a" => run_tool_a(agent, &role).await,
        "tool-b" => run_tool_b(agent, &role).await,
        "tool-sf" => run_tool_sf(agent, &role).await,
        "llm"    => run_chat_server(agent, LlmCfg::from_env(), chat_port).await,
        "mgmt"   => run_mgmt_server(agent, mgmt_port).await,
        "node"    => run_node(agent, &role).await,
        "overlay" => {
            let _consensus = agent.start_consensus_listener(ConsensusConfig::default());
            info!("[overlay] consensus listener started; HTTP gateway ready on :{http_port}");
            loop { tokio::time::sleep(std::time::Duration::from_secs(60)).await; }
        }
        other    => {
            error!("Unknown MYCELIUM_ROLE='{other}' — expected tool-a, tool-b, llm, mgmt, node, or overlay");
            std::process::exit(1);
        }
    }
    Ok(())
}
