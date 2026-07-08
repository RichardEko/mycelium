//! The deploy/reheal flagship node (rung 6, **echo variant**) — extends `reason_node`
//! into the one story that beats commodity checkpoint stores on non-commodity terms:
//! *a LangGraph graph's model dependency follows it across a node failure.*
//!
//! Two roles, chosen by env, share one binary so a Python driver (or a shell) can start
//! a two-node mesh and drive the whole choreography:
//!
//! - `SERVE_MODEL=1` (node **A**): serve `MODEL` as an [`EchoBackend`] prompt skill AND
//!   publish the "model artifact" — a content-addressed blob — advertising its id in KV so
//!   a peer knows *what to fetch*. A also runs [`spawn_blob_server`], so the blob is
//!   mesh-fetchable.
//! - `REHEAL=1` (node **B**): does **not** serve at start. A background task declares the
//!   demand ([`require_model`] → a gossiped `req/`), then structurally polls for A's KV
//!   advert, fetches the artifact blob over the mesh ([`MeshBlobStore::get`] — a real
//!   cross-node fetch with SHA-256 verify), and on arrival **bridges** it into a live
//!   prompt skill via [`serve_model`]. The model is now mesh-invocable on B, so once A
//!   dies the graph's routed inference lands on B.
//!
//! ## The echo-fixture honesty caveat
//!
//! The "model artifact" here is a tiny byte string, and "serving" it is
//! `serve_model(EchoBackend)` — the output is `echo: {input}`, deterministic and
//! wasmtime-free (so this stays in `mycelium-reason` examples, off the Provisioner path).
//! What is **real** is the seam: `require_model` → gossiped demand → mesh blob fetch +
//! content-address verify → the `serve_model` bridge → routed resume. The real variant
//! (a later PR) streams actual GGUF weights via `model_deploy`'s `BlobRuntime` and serves
//! them through a local-Ollama backend — same seam, real bytes. This node proves the
//! wiring deterministically in CI; it does not pretend the blob is a neural network.
//!
//! Env vars: `BIND_PORT`, `HTTP_PORT`, `BLOB_DIR` (all required), `BOOTSTRAP` (optional
//! `host:port`), `MODEL` (default `reheal-demo`), and one of `SERVE_MODEL=1` / `REHEAL=1`.
//!
//! Run (node A, then node B):
//! ```text
//! SERVE_MODEL=1 BIND_PORT=7301 HTTP_PORT=8301 BLOB_DIR=/tmp/reheal-a \
//!   cargo run -p mycelium-reason --features llm,gateway --example reheal_node
//! REHEAL=1 BIND_PORT=7302 HTTP_PORT=8302 BOOTSTRAP=127.0.0.1:7301 BLOB_DIR=/tmp/reheal-b \
//!   cargo run -p mycelium-reason --features llm,gateway --example reheal_node
//! ```

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use mycelium::{EchoBackend, GossipAgent, GossipConfig, LlmBackend, NodeId, PromptTemplate};
use mycelium_reason::{
    BlobId, FsBlobStore, MeshBlobStore, ModelProfile, TraceRecorder, reason_router, require_model,
    serve_model, spawn_blob_server,
};

/// A required env var, parsed; panics with a usable message when absent or malformed.
fn required<T: std::str::FromStr>(name: &str) -> T {
    let raw = std::env::var(name).unwrap_or_else(|_| panic!("{name} is required (see module doc)"));
    raw.parse().unwrap_or_else(|_| panic!("{name}={raw} did not parse"))
}

/// True when `name` is set to a truthy value (`1` / `true`).
fn flag(name: &str) -> bool {
    matches!(std::env::var(name).as_deref(), Ok("1") | Ok("true"))
}

/// The deterministic echo template both roles serve — output is `echo: {input}`.
fn echo_template() -> PromptTemplate {
    PromptTemplate {
        system: "You are a deterministic echo used by the reheal flagship.".into(),
        user_template: "{{input}}".into(),
        max_tokens: 512,
        temperature: 0.0,
        metadata: HashMap::new(),
    }
}

/// The advertised profile for `model` — the metadata half of the served skill.
fn echo_profile(model: &str) -> ModelProfile {
    ModelProfile {
        model: model.to_string(),
        ctx_window: Some(8192),
        family: Some("echo".into()),
        extra: Vec::new(),
    }
}

/// KV key under which node A advertises the artifact blob id for `model` (hex).
fn blob_advert_key(model: &str) -> String {
    format!("models/{model}/blob")
}

#[tokio::main]
async fn main() {
    let bind_port: u16 = required("BIND_PORT");
    let http_port: u16 = required("HTTP_PORT");
    let blob_dir: String = required("BLOB_DIR");
    let model = std::env::var("MODEL").unwrap_or_else(|_| "reheal-demo".into());
    let serve_role = flag("SERVE_MODEL");
    let reheal_role = flag("REHEAL");

    let bootstrap_peers = match std::env::var("BOOTSTRAP") {
        Ok(peer) => {
            let (host, port) = peer.rsplit_once(':').expect("BOOTSTRAP must be host:port");
            let port: u16 = port.parse().expect("BOOTSTRAP port did not parse");
            vec![NodeId::new(host, port).expect("BOOTSTRAP host:port invalid")]
        }
        Err(_) => Vec::new(),
    };
    let cfg = GossipConfig {
        bind_port,
        http_port: Some(http_port),
        bootstrap_peers,
        ..Default::default()
    };

    let agent = Arc::new(GossipAgent::new(
        NodeId::new("127.0.0.1", bind_port).expect("BIND_PORT invalid"),
        cfg,
    ));
    let store = Arc::new(FsBlobStore::open(&blob_dir).expect("BLOB_DIR must be creatable"));

    // Routes must be mounted BEFORE start — with_http_routes silently ignores late calls.
    agent.with_http_routes(reason_router(Arc::clone(&agent), Arc::clone(&store)));
    agent.start().await.expect("agent start failed");

    // Both roles serve blobs to peers (the mesh fetch behind GET /gateway/reason/blob/{id}
    // and MeshBlobStore::get). Node A is where the artifact actually lives; B write-back
    // caches it once fetched, so B keeps serving the blob after A dies too.
    let _blobs = spawn_blob_server(&agent, Arc::clone(&store));

    // Node A: serve the model now, and publish the artifact so B can discover + fetch it.
    // Held for the process lifetime (dropping ModelReg retracts the skill).
    let mut _served: Option<mycelium_reason::ModelReg> = None;
    if serve_role {
        _served = Some(
            serve_model(&agent, echo_profile(&model), echo_template(), Arc::new(EchoBackend))
                .await
                .expect("serve_model failed"),
        );

        // The "model artifact": a content-addressed blob. Echo fixture — a byte string, not
        // real weights; the real variant puts a streamed GGUF here (see the module doc).
        let artifact = format!("{model} weights v1 (echo fixture)").into_bytes();
        let blob_id = store.put(&artifact).expect("artifact put failed");
        // Advertise WHAT to fetch (the id), not the bytes: KV gossips everywhere and is
        // size-gated; payloads travel the blob tier. B reads this key to learn the id.
        let _ = agent.kv().set(blob_advert_key(&model), blob_id.to_hex().into_bytes());
        println!(
            "serve: model {model} live + artifact published (blob {}, {} bytes)",
            blob_id.to_hex(),
            artifact.len(),
        );
    }

    // Node B: the reheal task. Declares demand, structurally polls for A's advert, fetches
    // the artifact over the mesh, then bridges it into a live skill via serve_model.
    let reheal_task = if reheal_role {
        let agent = Arc::clone(&agent);
        let store = Arc::clone(&store);
        let model = model.clone();
        let blob_dir = blob_dir.clone();
        Some(tokio::spawn(async move {
            reheal(agent, store, model, blob_dir).await;
        }))
    } else {
        None
    };

    // Readiness: the printed marker means the gateway actually answers /health, not merely
    // that start() returned — the Python driver polls for it.
    let health = format!("http://127.0.0.1:{http_port}/health");
    let http = reqwest::Client::new();
    for attempt in 0.. {
        if http.get(&health).send().await.is_ok_and(|r| r.status().is_success()) {
            break;
        }
        assert!(attempt < 100, "gateway never answered /health");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    println!("reheal node ready on {http_port}");

    // Park until SIGTERM (driver cleanup) or ctrl-c (local shells).
    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("SIGTERM handler");
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await.ok();

    if let Some(task) = reheal_task {
        task.abort();
    }
    agent.shutdown_with_timeout(Duration::from_secs(5)).await;
    println!("reheal node on {http_port}: shut down");
}

/// The reheal choreography (node B). Runs as a background task for the process lifetime:
/// once the model is bridged it parks holding the RAII handles, so the skill stays live
/// (and A can die) until the task is aborted at shutdown.
async fn reheal(agent: Arc<GossipAgent>, store: Arc<FsBlobStore>, model: String, blob_dir: String) {
    // The real demand signal: a gossiped `req/{node}/llm/{model}` requirement. Held for the
    // task's life — dropping it retracts the demand.
    let _dep = require_model(&agent, &model, Duration::from_secs(5));
    let mesh = MeshBlobStore::new(Arc::clone(&agent), Arc::clone(&store), Duration::from_secs(10));
    let trace = TraceRecorder::new(Arc::clone(&agent), format!("reheal-{model}"));
    let started = std::time::Instant::now();

    // Structural poll (~300 ms) — the assertion is "the advert appeared AND the blob
    // fetched", never a fixed sleep. Bounded generously so a wedged mesh eventually gives
    // up with a visible warning rather than spinning forever.
    let deadline = started + Duration::from_secs(120);
    let advert = blob_advert_key(&model);
    let reg: mycelium_reason::ModelReg = loop {
        // The id gossips in via A's KV advert; the bytes travel the blob tier separately.
        let id = agent.kv().get(&advert).and_then(|hex| {
            std::str::from_utf8(&hex).ok().and_then(BlobId::from_hex)
        });
        // REAL cross-node fetch: local miss → RPC to A's blob server → SHA-256 verify
        // against the content address → write-back cache into B's store.
        if let Some(id) = id
            && let Some(bytes) = mesh.get(&id).await
        {
            let n = bytes.len();
            // A local marker: proof-of-install on disk (the real variant writes the
            // activated model dir here). Best-effort — the skill is the real signal.
            let marker = Path::new(&blob_dir).join(format!("rehealed-{model}.marker"));
            let _ = std::fs::write(&marker, format!("{n} bytes from mesh"));

            // The BRIDGE: the fetched artifact becomes a live, mesh-invocable prompt skill
            // on B. Echo fixture — EchoBackend, not the fetched bytes as a real model (see
            // the module doc); the seam is what is exercised.
            let reg = serve_model(
                &agent,
                echo_profile(&model),
                echo_template(),
                Arc::new(EchoBackend) as Arc<dyn LlmBackend>,
            )
            .await
            .expect("bridge serve_model failed");

            let waited_ms = started.elapsed().as_millis() as u64;
            trace.record(
                "reheal",
                serde_json::json!({
                    "model": model,
                    "bytes": n,
                    "waited_ms": waited_ms,
                    "detail": format!("rehealed {model} from mesh, {n} bytes, now serving"),
                }),
            );
            println!("reheal: model {model} installed from mesh ({n} bytes) — now serving");
            break reg;
        }
        if std::time::Instant::now() >= deadline {
            eprintln!("reheal: gave up awaiting the {model} artifact after 120s");
            return;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    };

    // Keep the skill (and the demand) alive for the process lifetime. Parking here holds
    // `reg` and `_dep`; the task is aborted at shutdown, dropping both.
    let _reg = reg;
    std::future::pending::<()>().await;
}
