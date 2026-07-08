//! # `wiki_chat` — the worked example (Phase 5): import documents, then chat grounded in the wiki.
//!
//! A **reusable template for both driving use cases** — the same binary serves UC1 (an organisation
//! "twin": a domain canon a chat agent answers from) and UC2 (a community council: decisions a resident
//! navigates by chat). Point it at a different corpus directory; nothing else changes.
//!
//! It demonstrates **both planes** of the wiki, each used the way the architecture intends:
//! - `import` is a **writer** → it runs the **control plane**: a curator drains its proposals and
//!   applies them to the store (the single writer of record).
//! - `ask` / `chat` are **readers** → they use the **data plane directly**: a reader opens the store
//!   and reads in parallel, with **no node and no curator** — the node-independence that makes the wiki
//!   a store, not a service. The only thing a reader needs besides the store is an LLM to phrase the
//!   answer.
//!
//! ## Usage
//! ```text
//! wiki_chat import --store DIR --group G --corpus examples/corpus/council
//! wiki_chat ask    --store DIR --group G [--mock] "what was decided about the Elm Street bike lane?"
//! wiki_chat chat   --store DIR --group G [--mock]            # interactive REPL
//! ```
//! `--mock` uses a deterministic in-process backend (no network) that echoes the retrieved context —
//! used by `ci_smoke.sh`. Without it, an OpenAI-compatible endpoint is read from `WIKI_CHAT_LLM_URL` /
//! `WIKI_CHAT_LLM_KEY` / `WIKI_CHAT_LLM_MODEL`.
#![allow(clippy::field_reassign_with_default)] // GossipConfig is built the way mycelium's own tests do

use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use mycelium::{EchoBackend, GossipAgent, GossipConfig, LlmBackend, LlmError, LlmResult, NodeId, OpenAiBackend};
use mycelium_wiki::{FsStore, Wiki, WikiConfig, WikiRole, WikiStore};

// ── the reader's grounding: retrieval + prompt (the reusable chat core) ─────────

/// A retrieved section with its page, for citation.
struct Hit {
    page:    String,
    heading: String,
    body:    String,
    score:   usize,
}

const STOPWORDS: &[&str] = &[
    "the", "was", "were", "what", "which", "about", "for", "and", "that", "this", "with", "did",
    "does", "has", "have", "are", "how", "why", "when", "who", "our", "you", "your",
];

fn tokens(s: &str) -> Vec<String> {
    s.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| w.len() >= 3 && !STOPWORDS.contains(w))
        .map(str::to_string)
        .collect()
}

/// Keyword-overlap retrieval over the whole corpus — the *authoritative-specific* layer's job is
/// structured/lexical recall of the exact curated text, not embedding similarity (that is RAG's role).
fn retrieve(store: &FsStore, question: &str, k: usize) -> Vec<Hit> {
    let q = tokens(question);
    let mut hits: Vec<Hit> = Vec::new();
    for path in store.list_pages().unwrap_or_default() {
        let Some(page) = store.read(&path).ok().flatten() else { continue };
        for s in page.sections {
            let hay = tokens(&format!("{} {}", s.heading, s.body));
            let score = q.iter().filter(|t| hay.contains(t)).count();
            if score > 0 {
                hits.push(Hit { page: path.clone(), heading: s.heading, body: s.body, score });
            }
        }
    }
    hits.sort_by_key(|h| std::cmp::Reverse(h.score));
    hits.truncate(k);
    hits
}

fn ground_prompt(hits: &[Hit], question: &str) -> String {
    let mut ctx = String::from("WIKI CONTEXT:\n");
    if hits.is_empty() {
        ctx.push_str("(no matching sections)\n");
    }
    for h in hits {
        ctx.push_str(&format!("## {} ({})\n{}\n\n", h.heading, h.page, h.body.trim()));
    }
    format!("{ctx}\nQUESTION: {question}\n\nAnswer using only the context above, and cite the section heading(s):")
}

const SYSTEM: &str = "You are an assistant for a group's shared wiki. Answer the user's question using \
    ONLY the provided wiki context. Cite the section heading(s) you used. If the context does not cover \
    the question, say so plainly — do not guess.";

// ── the mock backend (deterministic CI grounding) ───────────────────────────────

/// A no-network backend that returns the grounded context verbatim, so `ci_smoke.sh` can assert an
/// imported fact reaches the answer. Real deployments use [`OpenAiBackend`] instead.
struct GroundedMock;

#[async_trait::async_trait]
impl LlmBackend for GroundedMock {
    async fn complete(&self, _system: &str, user: &str, _max: u32, _temp: f32) -> Result<LlmResult, LlmError> {
        let context = user.split("QUESTION:").next().unwrap_or("").trim();
        Ok(LlmResult {
            output:      format!("[mock LLM — grounded in the wiki]\n{context}"),
            model_used:  "grounded-mock".into(),
            tokens_used: 0,
        })
    }
}

fn backend(mock: bool) -> Arc<dyn LlmBackend> {
    if mock {
        return Arc::new(GroundedMock);
    }
    // A real OpenAI-compatible endpoint from the environment; falls back to EchoBackend if unset so the
    // example never panics for want of a key (the answer is then trivial but the pipeline runs).
    match std::env::var("WIKI_CHAT_LLM_KEY") {
        Ok(key) if !key.is_empty() => {
            let url = std::env::var("WIKI_CHAT_LLM_URL").unwrap_or_else(|_| "https://api.openai.com/v1".into());
            let model = std::env::var("WIKI_CHAT_LLM_MODEL").unwrap_or_else(|_| "gpt-4o-mini".into());
            Arc::new(OpenAiBackend::new(url, key, model))
        }
        _ => {
            eprintln!("(no WIKI_CHAT_LLM_KEY set — using EchoBackend; pass --mock for grounded output)");
            Arc::new(EchoBackend)
        }
    }
}

// ── import (the control-plane writer) ───────────────────────────────────────────

/// Parse a document into (heading, body): a leading `# Heading` becomes the heading, the rest the body;
/// otherwise the filename stem is the heading and the whole file the body.
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

async fn import(store_dir: &Path, group: &str, corpus: &Path) {
    // A pinned curator (no election wait — this is a single-node importer) over the shared store.
    let port = free_port();
    let mut cfg = GossipConfig::default();
    cfg.bind_port = port;
    let agent = Arc::new(GossipAgent::new(NodeId::new("127.0.0.1", port).unwrap(), cfg));
    agent.start().await.unwrap();
    let store = Arc::new(FsStore::open(store_dir, group).unwrap());
    let wcfg = WikiConfig {
        group: group.into(), role: WikiRole::Curator,
        cap_refresh: Duration::from_millis(500), drain_interval: Duration::from_millis(100),
        lint_interval: Duration::from_secs(5),
    };
    let wiki = Wiki::new(Arc::clone(&agent), wcfg, Arc::clone(&store)).await;

    let mut expected: Vec<String> = Vec::new();
    for entry in std::fs::read_dir(corpus).expect("corpus dir") {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()).is_none_or(|e| e != "md" && e != "txt") { continue; }
        let stem = path.file_stem().unwrap().to_string_lossy().to_string();
        let text = std::fs::read_to_string(&path).unwrap();
        let (heading, body) = parse_doc(&stem, &text);
        let page = format!("decisions/{stem}");
        let sid = wiki.new_section_id(&page);
        let attrs = BTreeMap::from([
            ("source".to_string(), stem.clone()),
            ("group".to_string(), group.to_string()),
        ]);
        wiki.propose(&page, sid, heading, body, attrs);
        expected.push(page);
        println!("proposed  {}", path.display());
    }

    // Wait for the curator to drain + apply every proposal to the shared store (single writer of
    // record). Poll the store directly — the reader's view is the source of truth.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let done = expected.iter().all(|p| store.read(p).ok().flatten().is_some_and(|pg| !pg.sections.is_empty()));
        if done { break; }
        if tokio::time::Instant::now() >= deadline { eprintln!("import: timed out waiting for curator apply"); break; }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    println!("imported {} document(s) into group '{}' at {}", expected.len(), group, store.location());
    agent.shutdown_with_timeout(Duration::from_secs(5)).await;
}

// ── ask / chat (the data-plane reader) ──────────────────────────────────────────

/// Answer one question: retrieve from the store directly (no node), ground, and phrase with the LLM.
async fn answer(store: &FsStore, backend: &Arc<dyn LlmBackend>, question: &str) -> String {
    let hits = retrieve(store, question, 3);
    let prompt = ground_prompt(&hits, question);
    match backend.complete(SYSTEM, &prompt, 512, 0.2).await {
        Ok(r)  => r.output,
        Err(e) => format!("(LLM error: {e})"),
    }
}

async fn ask(store_dir: &Path, group: &str, question: &str, mock: bool) {
    // A reader touches only the data plane: open the store and read. No agent, no curator.
    let store = FsStore::open(store_dir, group).unwrap();
    let backend = backend(mock);
    println!("{}", answer(&store, &backend, question).await);
}

async fn chat(store_dir: &Path, group: &str, mock: bool) {
    let store = FsStore::open(store_dir, group).unwrap();
    let backend = backend(mock);
    println!("wiki chat — group '{group}'. Ask a question (empty line to quit).");
    let stdin = std::io::stdin();
    loop {
        print!("> ");
        std::io::stdout().flush().ok();
        let mut line = String::new();
        if stdin.read_line(&mut line).unwrap_or(0) == 0 { break; }
        let q = line.trim();
        if q.is_empty() { break; }
        println!("{}\n", answer(&store, &backend, q).await);
    }
}

// ── plumbing ─────────────────────────────────────────────────────────────────────

fn free_port() -> u16 {
    mycelium::test_util::alloc_port()
}

/// Minimal flag parsing: `--key value` pairs + `--mock`, plus a trailing positional (the question).
fn arg(args: &[String], key: &str) -> Option<String> {
    args.iter().position(|a| a == key).and_then(|i| args.get(i + 1)).cloned()
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let cmd = args.first().cloned().unwrap_or_default();
    let store_dir = arg(&args, "--store").unwrap_or_else(|| "/tmp/wiki-chat-store".into());
    let group = arg(&args, "--group").unwrap_or_else(|| "council".into());
    let mock = args.iter().any(|a| a == "--mock");

    match cmd.as_str() {
        "import" => {
            let corpus = arg(&args, "--corpus").expect("import needs --corpus DIR");
            import(Path::new(&store_dir), &group, Path::new(&corpus)).await;
        }
        "ask" => {
            let question = args.last().filter(|q| !q.starts_with("--")).cloned().expect("ask needs a question");
            ask(Path::new(&store_dir), &group, &question, mock).await;
        }
        "chat" => chat(Path::new(&store_dir), &group, mock).await,
        other => {
            eprintln!("usage: wiki_chat <import|ask|chat> --store DIR --group G [--corpus DIR] [--mock] [question]");
            eprintln!("unknown command: {other:?}");
            std::process::exit(2);
        }
    }
}
