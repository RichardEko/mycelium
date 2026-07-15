//! # `wiki_council_viz` — a live chat over a fleet of wiki-grounded council specialists.
//!
//! A neighbourhood council's decisions live in one **shared wiki**. Four **specialists** answer from
//! it, each an expert in a slice: **Transport**, **Energy**, **Planning**, and a cross-cutting
//! **Budget** analyst who reads the £ figures across every decision. Ask a question in the browser and
//! it **fans out** to the relevant specialists — each answers *grounded in the wiki, with citations* —
//! and a **synthesizer** merges them into one cited reply.
//!
//! Each specialist is a **data-plane reader**: it opens the shared store and reads directly — no node,
//! no curator, no service (the node-independence the wiki architecture intends). In a distributed
//! deployment each specialist is a separate mesh agent advertising its `domain` as a capability, and
//! the question routes by capability rather than being dispatched; here they run in one process so the
//! whole fleet is watchable in one dashboard. The answering LLM is a deterministic **grounded mock**
//! (no key, no network) — every claim is verbatim from the wiki, so the demo is honest offline.
//!
//! ```text
//! cargo run -p mycelium-wiki --example wiki_council_viz --features gateway   # → http://127.0.0.1:8095/
//! ```

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use mycelium::{GossipAgent, GossipConfig, NodeId};
use mycelium_wiki::{FsStore, Wiki, WikiConfig, WikiRole, WikiStore};

const HTTP_PORT: u16 = 8095;
/// The Mycelium gateway port (distinct from the dashboard) — lets the Ops Console target this node.
const OPS_PORT: u16 = 9095;
const GROUP: &str = "council";

// ── the reader's grounding: retrieval (reused from wiki_chat) ────────────────────

/// A retrieved wiki section with its page, for citation.
#[derive(Clone)]
struct Hit {
    page:    String,
    heading: String,
    body:    String,
    score:   usize,
}

const STOPWORDS: &[&str] = &[
    "the", "was", "were", "what", "which", "about", "for", "and", "that", "this", "with", "did",
    "does", "has", "have", "are", "how", "why", "when", "who", "our", "you", "your", "council",
];

fn tokens(s: &str) -> Vec<String> {
    s.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| w.len() >= 3 && !STOPWORDS.contains(w))
        .map(str::to_string)
        .collect()
}

/// Keyword-overlap retrieval over the whole shared corpus.
fn retrieve(store: &FsStore, question: &str, k: usize) -> Vec<Hit> {
    let q = tokens(question);
    let mut hits: Vec<Hit> = Vec::new();
    for path in store.list_pages().unwrap_or_default() {
        let Some(page) = store.read(&path).ok().flatten() else { continue };
        for s in page.sections {
            let hay = tokens(&format!("{} {}", s.heading, s.body));
            let score = q.iter().filter(|t| hay.contains(*t)).count();
            if score > 0 {
                hits.push(Hit { page: path.clone(), heading: s.heading, body: s.body, score });
            }
        }
    }
    hits.sort_by_key(|h| std::cmp::Reverse(h.score));
    hits.truncate(k);
    hits
}

// ── the specialist fleet ─────────────────────────────────────────────────────────

struct Specialist {
    name:     &'static str,
    emoji:    &'static str,
    color:    &'static str,
    /// Domain keywords; a hit "belongs" to this specialist if it mentions any of them.
    keywords: &'static [&'static str],
    /// Cross-cutting analysts (Budget) engage on any hit carrying a £ figure, not just a keyword.
    money:    bool,
}

const FLEET: &[Specialist] = &[
    Specialist { name: "Transport", emoji: "🚲", color: "#58a6ff", money: false,
        keywords: &["bike", "lane", "bus", "route", "parking", "road", "travel", "cycle",
                    "pedestrian", "station", "permit", "traffic", "riverside", "mobility"] },
    Specialist { name: "Energy", emoji: "⚡", color: "#d29922", money: false,
        keywords: &["solar", "energy", "heat", "pump", "charging", "charge", "grid", "power",
                    "electric", "array", "tariff", "renewable", "kilowatt", "oakfield"] },
    Specialist { name: "Planning", emoji: "🏛", color: "#3fb950", money: false,
        keywords: &["library", "hours", "allotment", "zoning", "market", "square", "land",
                    "building", "depot", "plot", "recycling", "opening", "waste"] },
    Specialist { name: "Budget", emoji: "💷", color: "#a78bfa", money: true,
        keywords: &["budget", "cost", "fund", "funded", "spend", "grant", "allocation",
                    "saving", "capital", "reallocated", "pounds"] },
];

/// Assign a decision to its single best-matching **content** specialist (most keyword overlap), so a
/// doc that brushes several domains' words can't pull the whole fleet in. `None` = no content owner.
fn owner(hit: &Hit) -> Option<usize> {
    let toks = tokens(&format!("{} {}", hit.heading, hit.body));
    let mut best: Option<(usize, usize)> = None;
    for (i, spec) in FLEET.iter().enumerate() {
        if spec.money {
            continue;
        }
        let count = spec.keywords.iter().filter(|k| toks.iter().any(|t| t == **k)).count();
        if count > 0 && best.is_none_or(|(_, c)| count > c) {
            best = Some((i, count));
        }
    }
    best.map(|(i, _)| i)
}

fn first_sentence(body: &str) -> String {
    let b = body.trim().replace('\n', " ");
    match b.find(". ") {
        Some(i) => b[..=i].trim().to_string(),
        None => b,
    }
}

/// Pull the "£… (source)"-style figures a Budget analyst would cite.
fn pounds(hits: &[&Hit]) -> Vec<String> {
    let mut out = Vec::new();
    for h in hits {
        let b = h.body.replace('\n', " ");
        let mut rest = b.as_str();
        while let Some(p) = rest.find('£') {
            let after = &rest[p..];
            let end = after
                .char_indices()
                .find(|(i, c)| *i > 0 && !c.is_ascii_digit() && *c != ',' && *c != '£')
                .map(|(i, _)| i)
                .unwrap_or(after.len());
            let amount = &after[..end];
            if amount.len() > 1 {
                out.push(format!("{amount} — {}", h.heading));
            }
            rest = &after[end..];
        }
    }
    out
}

struct Contribution {
    specialist: &'static str,
    emoji:      &'static str,
    color:      &'static str,
    answer:     String,
    citations:  Vec<(String, String)>, // (page, heading)
}

struct Turn {
    question:      String,
    contributions: Vec<Contribution>,
    synthesis:     String,
}

/// Fan out one question to the fleet, ground each engaged specialist, and synthesize.
fn run_council(store: &FsStore, question: &str) -> Turn {
    let all = retrieve(store, question, 8);
    // Engage only on the strongest-matching tier: a single shared word (a stray "cost" or "street")
    // shouldn't pull an off-topic specialist in. Ties (a genuinely multi-decision question) survive.
    let top = all.first().map(|h| h.score).unwrap_or(0);
    let hits: Vec<Hit> = all.into_iter().filter(|h| top > 0 && h.score >= top).collect();
    let mut contributions = Vec::new();

    for (si, spec) in FLEET.iter().enumerate() {
        let eng: Vec<&Hit> = if spec.money {
            hits.iter().filter(|h| h.body.contains('£')).collect()
        } else {
            hits.iter().filter(|h| owner(h) == Some(si)).collect()
        };
        if eng.is_empty() {
            continue;
        }
        let answer = if spec.money {
            let figs = pounds(&eng);
            if figs.is_empty() {
                continue;
            }
            format!("The cost on record: {}.", figs.join("; "))
        } else {
            first_sentence(&eng[0].body)
        };
        let citations = eng
            .iter()
            .take(2)
            .map(|h| (h.page.clone(), h.heading.clone()))
            .collect();
        contributions.push(Contribution {
            specialist: spec.name,
            emoji:      spec.emoji,
            color:      spec.color,
            answer,
            citations,
        });
    }

    let synthesis = match contributions.len() {
        0 => "No specialist holds a record of that. Ask about transport, energy, planning, or the \
              budget of a specific decision."
            .to_string(),
        1 => format!("{} answered, grounded in the wiki.", contributions[0].specialist),
        n => {
            let who = contributions
                .iter()
                .map(|c| c.specialist)
                .collect::<Vec<_>>()
                .join(" + ");
            format!(
                "Synthesized from {n} specialists ({who}): each cited its own wiki record above — \
                 together they give the decision and its cost."
            )
        }
    };

    Turn { question: question.to_string(), contributions, synthesis }
}

// ── viz state ────────────────────────────────────────────────────────────────────

#[derive(Default)]
struct CouncilState {
    thread: Vec<Turn>,
    /// per-specialist: how many turns it has engaged (for the fleet activity view).
    engaged: BTreeMap<&'static str, u64>,
}

fn esc(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"").replace(['\n', '\t'], " ")
}

fn state_json(st: &CouncilState) -> String {
    let fleet = FLEET
        .iter()
        .map(|s| {
            format!(
                r#"{{"name":"{}","emoji":"{}","color":"{}","engaged":{}}}"#,
                s.name, s.emoji, s.color, st.engaged.get(s.name).copied().unwrap_or(0)
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let thread = st
        .thread
        .iter()
        .map(|t| {
            let contribs = t
                .contributions
                .iter()
                .map(|c| {
                    let cites = c
                        .citations
                        .iter()
                        .map(|(p, h)| format!(r#"{{"page":"{}","heading":"{}"}}"#, esc(p), esc(h)))
                        .collect::<Vec<_>>()
                        .join(",");
                    format!(
                        r#"{{"specialist":"{}","emoji":"{}","color":"{}","answer":"{}","citations":[{}]}}"#,
                        c.specialist, c.emoji, c.color, esc(&c.answer), cites
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(
                r#"{{"question":"{}","contributions":[{}],"synthesis":"{}"}}"#,
                esc(&t.question), contribs, esc(&t.synthesis)
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(r#"{{"fleet":[{fleet}],"thread":[{thread}]}}"#)
}

fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' if i + 2 < b.len() => {
                let hex = std::str::from_utf8(&b[i + 1..i + 3]).ok();
                if let Some(v) = hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                    out.push(v);
                    i += 3;
                    continue;
                }
                out.push(b[i]);
                i += 1;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).to_string()
}

// ── HTTP dashboard ─────────────────────────────────────────────────────────────────

async fn serve_http(store: Arc<FsStore>, state: Arc<Mutex<CouncilState>>) {
    let listener = match TcpListener::bind(format!("127.0.0.1:{HTTP_PORT}")).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("HTTP server failed to bind :{HTTP_PORT} — {e}");
            return;
        }
    };
    loop {
        let Ok((mut stream, _)) = listener.accept().await else { continue };
        let store = Arc::clone(&store);
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let mut buf = vec![0u8; 8192];
            let n = match stream.read(&mut buf).await {
                Ok(n) if n > 0 => n,
                _ => return,
            };
            let req = String::from_utf8_lossy(&buf[..n]);
            let line = req.lines().next().unwrap_or("");
            let target = line.split_whitespace().nth(1).unwrap_or("/");

            let (ctype, body): (&str, String) = if target.starts_with("/ask") {
                // GET /ask?q=<url-encoded question> → run the council, append the turn, return state.
                let q = target
                    .split_once("?q=")
                    .map(|(_, v)| pct_decode(v))
                    .unwrap_or_default();
                if !q.trim().is_empty() {
                    let turn = run_council(&store, q.trim());
                    let mut st = state.lock().unwrap();
                    for c in &turn.contributions {
                        *st.engaged.entry(c.specialist).or_insert(0) += 1;
                    }
                    st.thread.push(turn);
                }
                ("application/json", state_json(&state.lock().unwrap()))
            } else if target.starts_with("/state") {
                ("application/json", state_json(&state.lock().unwrap()))
            } else {
                // Inject the "⚙ Ops Console" back-link (only with the gateway, which the console reads).
                let console = if cfg!(feature = "gateway") {
                    format!(
                        "<a class=\"opsbtn\" href=\"http://127.0.0.1:8099/?target=127.0.0.1:{OPS_PORT}\" \
                         title=\"Open this cluster in the Mycelium Ops Console\">⚙ Ops Console</a>"
                    )
                } else {
                    String::new()
                };
                ("text/html; charset=utf-8",
                 include_str!("wiki_council_viz.html").replace("__OPS_CONSOLE_LINK__", &console))
            };

            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {ctype}\r\nAccess-Control-Allow-Origin: *\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes()).await;
        });
    }
}

// ── import (the control-plane writer, reused from wiki_chat) ──────────────────────

fn parse_doc(stem: &str, text: &str) -> (String, String) {
    let text = text.trim_start();
    if let Some(rest) = text.strip_prefix("# ") {
        if let Some((h, body)) = rest.split_once('\n') {
            return (h.trim().to_string(), body.trim().to_string());
        }
        return (rest.trim().to_string(), String::new());
    }
    (stem.replace(['-', '_'], " "), text.to_string())
}

async fn import(store_dir: &Path, corpus: &Path) -> Arc<FsStore> {
    let port = mycelium::test_util::alloc_port();
    let cfg = GossipConfig {
        bind_port: port,
        cluster_name: Some(std::env::var("GOSSIP_CLUSTER_NAME").unwrap_or_else(|_| "wiki-council".into())),
        ..Default::default()
    };
    let agent = Arc::new(GossipAgent::new(NodeId::new("127.0.0.1", port).unwrap(), cfg));
    agent.start().await.unwrap();
    let store = Arc::new(FsStore::open(store_dir, GROUP).unwrap());
    let wcfg = WikiConfig {
        group: GROUP.into(),
        role: WikiRole::Curator,
        cap_refresh: Duration::from_millis(500),
        drain_interval: Duration::from_millis(100),
        lint_interval: Duration::from_secs(5),
    };
    let wiki = Wiki::new(Arc::clone(&agent), wcfg, Arc::clone(&store)).await;

    let mut expected = Vec::new();
    for entry in std::fs::read_dir(corpus).expect("corpus dir") {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()).is_none_or(|e| e != "md") {
            continue;
        }
        let stem = path.file_stem().unwrap().to_string_lossy().to_string();
        let text = std::fs::read_to_string(&path).unwrap();
        let (heading, body) = parse_doc(&stem, &text);
        let page = format!("decisions/{stem}");
        let sid = wiki.new_section_id(&page);
        wiki.propose(&page, sid, heading, body, BTreeMap::new());
        expected.push(page);
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let done = expected
            .iter()
            .all(|p| store.read(p).ok().flatten().is_some_and(|pg| !pg.sections.is_empty()));
        if done || tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    println!("imported {} council decisions into the shared wiki", expected.len());
    agent.shutdown_with_timeout(Duration::from_secs(5)).await;
    store
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let corpus = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "mycelium-wiki/examples/corpus/council".to_string());
    let store_dir = std::env::temp_dir().join(format!("wiki-council-{}", std::process::id()));

    // Control plane: a curator imports the corpus into the shared store (single writer of record).
    let store = import(&store_dir, Path::new(&corpus)).await;

    // A gateway-carrying node so the Ops Console can target this demo + discover its UI.
    let gwport = OPS_PORT;
    let cfg = GossipConfig {
        bind_port: mycelium::test_util::alloc_port(),
        cluster_name: Some(std::env::var("GOSSIP_CLUSTER_NAME").unwrap_or_else(|_| "wiki-council".into())),
        http_port: Some(gwport),
        ..Default::default()
    };
    let gw = Arc::new(GossipAgent::new(NodeId::new("127.0.0.1", cfg.bind_port)?, cfg));
    gw.start().await?;
    #[cfg(feature = "gateway")]
    {
        let _ = gw.kv().set("ui/viz", format!("http://127.0.0.1:{HTTP_PORT}/"));
        let _ = gw.kv().set("ui/label", "Council specialists".to_string());
    }

    let state = Arc::new(Mutex::new(CouncilState::default()));
    tokio::spawn(serve_http(Arc::clone(&store), Arc::clone(&state)));

    println!("╔══════════════════════════════════════════════════════╗");
    println!("║  Wiki Council — specialists over a shared wiki        ║");
    println!("║  Open in browser → http://127.0.0.1:{HTTP_PORT}/         ║");
    #[cfg(feature = "gateway")]
    println!("║  Ops Console     → point it at 127.0.0.1:{gwport}       ║");
    println!("╚══════════════════════════════════════════════════════╝");
    println!("Try: \"what was decided about the Elm Street bike lane, and what did it cost?\"");

    tokio::signal::ctrl_c().await?;
    let _ = std::fs::remove_dir_all(&store_dir);
    Ok(())
}
