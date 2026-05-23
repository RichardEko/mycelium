//! Three-node real-world demo — Docker edition.
//!
//! One binary, three roles selected by `MYCELIUM_ROLE`:
//!
//! ```
//!   tool-a  ─── weather(city)   — calls wttr.in
//!               web_fetch(url)  — fetches any URL, returns first 2 KB
//!
//!   tool-b  ─── calculate(expr) — safe arithmetic evaluator
//!               wiki(topic)     — calls Wikipedia REST summary API
//!
//!   llm     ─── discovers all tools over gossip, plans with a real LLM,
//!               routes each tool call to whichever container hosts it
//! ```
//!
//! # Environment variables
//!
//! | Variable              | Default                              | Used by      |
//! |-----------------------|--------------------------------------|--------------|
//! | `MYCELIUM_ROLE`       | *(required)*                         | all          |
//! | `MYCELIUM_PEERS`      | *(required, comma-sep host:port)*    | all          |
//! | `MYCELIUM_HOSTNAME`   | value of `HOSTNAME` env var          | all          |
//! | `MYCELIUM_PORT`       | `57000`                              | all          |
//! | `MYCELIUM_HTTP_PORT`  | `8300`                               | all          |
//! | `OPENAI_BASE_URL`     | `https://api.openai.com/v1`          | llm          |
//! | `OPENAI_API_KEY`      | *(required for llm role)*            | llm          |
//! | `OPENAI_MODEL`        | `gpt-4o-mini`                        | llm          |
//! | `DEMO_TASK`           | see below                            | llm          |
//!
//! # Quick start (local, no Docker)
//! ```sh
//! # terminal 1
//! MYCELIUM_ROLE=tool-a MYCELIUM_PEERS=127.0.0.1:57001,127.0.0.1:57002 \
//!   MYCELIUM_PORT=57000 cargo run --example three_node_demo
//!
//! # terminal 2
//! MYCELIUM_ROLE=tool-b MYCELIUM_PEERS=127.0.0.1:57000,127.0.0.1:57002 \
//!   MYCELIUM_PORT=57001 cargo run --example three_node_demo
//!
//! # terminal 3
//! MYCELIUM_ROLE=llm MYCELIUM_PEERS=127.0.0.1:57000,127.0.0.1:57001 \
//!   MYCELIUM_PORT=57002 OPENAI_API_KEY=sk-... cargo run --example three_node_demo
//! ```

use bytes::Bytes;
use mycelium::{GossipAgent, GossipConfig, McpToolHandle, NodeId, signal_kind};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::time;
use tracing::{error, info, warn};

// ── Constants ─────────────────────────────────────────────────────────────────

const GOSSIP_PORT_DEFAULT: u16 = 57000;
const HTTP_PORT_DEFAULT:   u16 = 8300;
const TOOL_SETTLE_SECS:    u64 = 8;   // wait for mesh convergence before planning
const MAX_TURNS:           usize = 10;
const DEFAULT_TASK: &str =
    "What is the current weather in London? \
     Also, how tall is the Eiffel Tower in metres according to Wikipedia? \
     Finally, what is that height multiplied by 1024?";

// ── Startup helpers ───────────────────────────────────────────────────────────

/// Resolve a hostname to its first IPv4/v6 address string.
/// In Docker containers this turns service names like "tool-a" into IPs.
async fn resolve_ip(hostname: &str) -> Result<String, String> {
    use tokio::net::lookup_host;
    let mut addrs = lookup_host(format!("{hostname}:0"))
        .await
        .map_err(|e| format!("DNS lookup for '{hostname}' failed: {e}"))?;
    addrs.next()
        .map(|a| a.ip().to_string())
        .ok_or_else(|| format!("no address resolved for '{hostname}'"))
}

/// Resolve a comma-separated list of `host:port` peer strings into `NodeId`s,
/// skipping any that fail (peers may not be up yet; reconnect handles it).
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

/// Build and start a GossipAgent from the standard env vars.
async fn make_agent(
    my_ip:    &str,
    peers:    Vec<NodeId>,
    port:     u16,
    http_port: u16,
) -> Arc<GossipAgent> {
    let nid = NodeId::new(my_ip, port).expect("valid self NodeId");
    let mut cfg = GossipConfig::default();
    cfg.bind_address               = my_ip.to_string();
    cfg.bind_port                  = port;
    cfg.http_port                  = Some(http_port);
    cfg.http_addr                  = "0.0.0.0".to_string();
    cfg.bootstrap_peers            = peers;
    cfg.default_ttl                = 10;
    cfg.reconnect_backoff_secs     = 2;
    cfg.gossip_shards              = 2;
    cfg.health_check_max_jitter_ms = 200;
    let agent = Arc::new(GossipAgent::new(nid, cfg));
    agent.start().await.expect("agent start");
    agent
}

// ── Tool handler types ────────────────────────────────────────────────────────

type BoxFuture<T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'static>>;
type ToolHandler = Arc<dyn Fn(Value) -> BoxFuture<Result<Value, String>> + Send + Sync + 'static>;

fn register(agent: &Arc<GossipAgent>, name: &str, description: &str, params: Value, handler: ToolHandler) -> McpToolHandle {
    let schema = json!({ "description": description, "inputSchema": params });
    agent.register_mcp_tool(name, schema, move |args| {
        let h = Arc::clone(&handler);
        Box::pin(async move { h(args).await })
    })
}

// ── Real tool implementations ─────────────────────────────────────────────────

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
        "city":        city,
        "temp_c":      current["temp_C"].as_str().unwrap_or("?"),
        "feels_like_c": current["FeelsLikeC"].as_str().unwrap_or("?"),
        "description": current["weatherDesc"][0]["value"].as_str().unwrap_or("unknown"),
        "humidity_pct": current["humidity"].as_str().unwrap_or("?"),
        "wind_kmph":   current["windspeedKmph"].as_str().unwrap_or("?"),
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

    // Strip HTML tags with a simple pass; return first 2 KB
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
    let result = eval(&tokens).ok_or_else(|| {
        format!("cannot evaluate '{expr}' — expected 'a op b' (e.g. '330 * 1024')")
    })?;

    Ok(json!({ "expression": expr, "result": result }))
}

async fn tool_wiki(args: Value) -> Result<Value, String> {
    let topic = args["topic"].as_str().unwrap_or("").trim().to_string();
    if topic.is_empty() { return Err("missing topic parameter".into()); }

    // Wikipedia REST summary API — no auth required
    let encoded = topic.replace(' ', "_");
    let url = format!("https://en.wikipedia.org/api/rest_v1/page/summary/{encoded}");
    let resp = reqwest::Client::new()
        .get(&url)
        .header("User-Agent", "mycelium-demo/0.1")
        .timeout(Duration::from_secs(10))
        .send().await.map_err(|e| format!("wiki request failed: {e}"))?
        .json::<Value>().await.map_err(|e| format!("wiki parse failed: {e}"))?;

    if resp["type"].as_str() == Some("disambiguation") || resp["extract"].is_null() {
        return Err(format!("'{topic}' is ambiguous or not found — try a more specific title"));
    }
    Ok(json!({
        "title":   resp["title"].as_str().unwrap_or(&topic),
        "summary": resp["extract"].as_str().unwrap_or("(no extract)"),
        "url":     resp["content_urls"]["desktop"]["page"].as_str().unwrap_or(""),
    }))
}

// ── Tool-node runners ─────────────────────────────────────────────────────────

async fn run_tool_a(agent: Arc<GossipAgent>, role: &str) {
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
    info!("[{role}] registered tools: weather, web_fetch — listening");
    // Block forever (tools are served by the MCP signal handler in the background)
    loop { time::sleep(Duration::from_secs(60)).await; }
}

async fn run_tool_b(agent: Arc<GossipAgent>, role: &str) {
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
    info!("[{role}] registered tools: calculate, wiki — listening");
    loop { time::sleep(Duration::from_secs(60)).await; }
}

// ── LLM planning loop ─────────────────────────────────────────────────────────

struct LlmCfg {
    base_url: String,
    api_key:  String,
    model:    String,
}

impl LlmCfg {
    fn from_env() -> Self {
        Self {
            base_url: std::env::var("OPENAI_BASE_URL")
                .unwrap_or_else(|_| "https://api.openai.com/v1".into()),
            api_key:  std::env::var("OPENAI_API_KEY").unwrap_or_default(),
            model:    std::env::var("OPENAI_MODEL")
                .unwrap_or_else(|_| "gpt-4o-mini".into()),
        }
    }
}

fn discover_tools(agent: &GossipAgent) -> Vec<(String, NodeId, Value)> {
    let mut seen: std::collections::HashSet<String> = Default::default();
    let mut tools = Vec::new();
    for (key, schema_bytes) in agent.scan_prefix("tools/") {
        let parts: Vec<&str> = key.splitn(3, '/').collect();
        if parts.len() != 3 { continue; }
        let tool_name = parts[1].to_string();
        if !seen.insert(tool_name.clone()) { continue; }
        let Ok(nid) = parts[2].parse::<NodeId>() else { continue };
        let Ok(schema) = serde_json::from_slice::<Value>(&schema_bytes) else { continue };
        let input_schema = schema.get("inputSchema").cloned()
            .unwrap_or_else(|| json!({"type":"object","properties":{}}));
        let description = schema["description"].as_str().unwrap_or("").to_string();
        tools.push((tool_name.clone(), nid, json!({
            "type": "function",
            "function": { "name": tool_name, "description": description, "parameters": input_schema }
        })));
    }
    tools
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

    info!("[llm] → {tool_name}({args})  via {nid}");

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

async fn llm_step(
    cfg:      &LlmCfg,
    messages: &[Value],
    tools:    &[(String, NodeId, Value)],
) -> Result<Option<(String, Value)>, String> {
    let tool_defs: Vec<Value> = tools.iter().map(|(_, _, d)| d.clone()).collect();
    let resp = reqwest::Client::new()
        .post(format!("{}/chat/completions", cfg.base_url))
        .bearer_auth(&cfg.api_key)
        .json(&json!({
            "model":       cfg.model,
            "messages":    messages,
            "tools":       tool_defs,
            "tool_choice": "auto",
        }))
        .timeout(Duration::from_secs(60))
        .send().await.map_err(|e| format!("LLM request failed: {e}"))?
        .json::<Value>().await.map_err(|e| format!("LLM response parse failed: {e}"))?;

    if let Some(err) = resp.get("error") {
        return Err(format!("LLM API error: {}", err["message"].as_str().unwrap_or("unknown")));
    }

    let choice = resp["choices"].get(0).ok_or("empty choices from LLM")?;
    if choice["finish_reason"].as_str() == Some("tool_calls") {
        let tc   = &choice["message"]["tool_calls"][0];
        let name = tc["function"]["name"].as_str().unwrap_or("").to_string();
        let args: Value = serde_json::from_str(
            tc["function"]["arguments"].as_str().unwrap_or("{}"),
        ).unwrap_or(json!({}));
        Ok(Some((name, args)))
    } else {
        Ok(None)
    }
}

async fn run_llm(agent: Arc<GossipAgent>, cfg: LlmCfg, task: String) {
    // Wait for tool nodes to join the mesh
    info!("[llm] waiting {TOOL_SETTLE_SECS}s for tool nodes to advertise...");
    time::sleep(Duration::from_secs(TOOL_SETTLE_SECS)).await;

    loop {
        let tools = discover_tools(&agent);
        if tools.is_empty() {
            warn!("[llm] no tools discovered yet, retrying in 3s...");
            time::sleep(Duration::from_secs(3)).await;
            continue;
        }
        let tool_names: Vec<&str> = tools.iter().map(|(n, _, _)| n.as_str()).collect();
        info!("[llm] discovered {} tools: {}", tools.len(), tool_names.join(", "));
        break;
    }

    let tools = discover_tools(&agent);
    info!("[llm] task: {task}");

    let mut messages = vec![
        json!({"role":"system","content":
            "You are a helpful assistant. Use the available tools to answer the user's question \
             step by step. Stop calling tools once you have all the information needed."}),
        json!({"role":"user","content": task}),
    ];

    let mut turn = 0usize;
    loop {
        if turn >= MAX_TURNS {
            error!("[llm] reached max turns ({MAX_TURNS}) — aborting");
            break;
        }

        match llm_step(&cfg, &messages, &tools).await {
            Err(e) => {
                error!("[llm] LLM error: {e}");
                break;
            }
            Ok(None) => {
                // LLM is done — extract and print the final answer
                let final_msg = match llm_step_final_content(&cfg, &messages).await {
                    Ok(content) => content,
                    Err(e) => { error!("[llm] final message error: {e}"); break; }
                };
                info!("[llm] ✓ DONE after {turn} tool call(s)");
                println!("\n╔═══════════════════════════════════════════════╗");
                println!("║  TASK:  {task}");
                println!("╠═══════════════════════════════════════════════╣");
                println!("║  ANSWER:\n");
                for line in final_msg.lines() {
                    println!("  {line}");
                }
                println!("╚═══════════════════════════════════════════════╝\n");
                break;
            }
            Ok(Some((tool_name, args))) => {
                match invoke_tool(&agent, &tool_name, args.clone()).await {
                    Err(e) => {
                        error!("[llm] ← tool error from {tool_name}: {e}");
                        messages.push(json!({"role":"assistant","content":null,"tool_calls":[{
                            "id":"c0","type":"function",
                            "function":{"name":tool_name,"arguments":args.to_string()}
                        }]}));
                        messages.push(json!({"role":"tool","tool_call_id":"c0",
                            "content":format!("Error: {e}")}));
                    }
                    Ok(result) => {
                        info!("[llm] ← {} result: {}", tool_name,
                            serde_json::to_string_pretty(&result).unwrap_or_default());
                        messages.push(json!({"role":"assistant","content":null,"tool_calls":[{
                            "id":"c0","type":"function",
                            "function":{"name":tool_name,"arguments":args.to_string()}
                        }]}));
                        messages.push(json!({"role":"tool","tool_call_id":"c0",
                            "content":result.to_string()}));
                    }
                }
                turn += 1;
            }
        }
    }
}

// A follow-up call without tools to get the final text answer.
async fn llm_step_final_content(cfg: &LlmCfg, messages: &[Value]) -> Result<String, String> {
    let resp = reqwest::Client::new()
        .post(format!("{}/chat/completions", cfg.base_url))
        .bearer_auth(&cfg.api_key)
        .json(&json!({ "model": cfg.model, "messages": messages }))
        .timeout(Duration::from_secs(60))
        .send().await.map_err(|e| format!("LLM final request failed: {e}"))?
        .json::<Value>().await.map_err(|e| format!("LLM final parse failed: {e}"))?;

    Ok(resp["choices"][0]["message"]["content"]
        .as_str()
        .unwrap_or("(no response)")
        .to_string())
}

// ── main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialise tracing — uses RUST_LOG env var (default: info)
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("RUST_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let role = std::env::var("MYCELIUM_ROLE")
        .unwrap_or_else(|_| "tool-a".to_string());
    let port: u16 = std::env::var("MYCELIUM_PORT")
        .ok().and_then(|v| v.parse().ok())
        .unwrap_or(GOSSIP_PORT_DEFAULT);
    let http_port: u16 = std::env::var("MYCELIUM_HTTP_PORT")
        .ok().and_then(|v| v.parse().ok())
        .unwrap_or(HTTP_PORT_DEFAULT);
    let peer_list = std::env::var("MYCELIUM_PEERS").unwrap_or_default();

    // Resolve own IP — Docker containers set HOSTNAME to the service name
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

    let peers = resolve_peers(&peer_list).await;

    info!(
        role = %role,
        my_ip = %my_ip,
        port,
        http_port,
        peers = peers.len(),
        "starting"
    );

    let agent = make_agent(&my_ip, peers, port, http_port).await;
    info!("[{role}] node started — node_id={} — gateway http://0.0.0.0:{http_port}",
          agent.node_id());

    match role.as_str() {
        "tool-a" => run_tool_a(agent, &role).await,
        "tool-b" => run_tool_b(agent, &role).await,
        "llm"    => {
            let cfg  = LlmCfg::from_env();
            if cfg.api_key.is_empty() {
                error!("OPENAI_API_KEY is not set — LLM role requires it");
                std::process::exit(1);
            }
            let task = std::env::var("DEMO_TASK")
                .unwrap_or_else(|_| DEFAULT_TASK.to_string());
            run_llm(agent, cfg, task).await;
        }
        other => {
            error!("Unknown MYCELIUM_ROLE='{other}' — expected tool-a, tool-b, or llm");
            std::process::exit(1);
        }
    }

    Ok(())
}
