//! Example M — **deploy a real LLM model through the artifact library** (manual demo).
//!
//! The proof the artifact library's Blob path is real end to end: a genuine GGUF model file
//! travels CI → durable library → catalogue → resource-checked self-election → **streamed
//! chunked pull with live loading-tier percent** → placement → **activation into Ollama** →
//! probe-gated capability → and finally an app node **generates real tokens** through the
//! deployed model. Nothing is simulated: the percent ticks are real bytes, the activation is
//! a real `ollama create`, and the story at the end came out of the model that was just
//! deployed.
//!
//! Because it needs a local Ollama daemon and a model file, this demo is **manual** — it is
//! deliberately NOT in `ci_smoke.sh`.
//!
//! # Requirements
//!
//! - `ollama` installed and the daemon running (`ollama serve` or the desktop app).
//! - A GGUF file. Any small one works; the 19 MB TinyStories model is perfect for the
//!   co-op storyteller framing:
//!
//!   ```sh
//!   curl -L -o /tmp/stories15M-q4_0.gguf \
//!     https://huggingface.co/ggml-org/models/resolve/main/tinyllamas/stories15M-q4_0.gguf
//!   ```
//!
//! # Run
//!
//! ```sh
//! MODEL_GGUF=/tmp/stories15M-q4_0.gguf \
//!   cargo run -p mycelium-coop-examples --features wasm --bin model_deploy
//! ```
//!
//! Optional: `OLLAMA_URL` (default `http://localhost:11434`).
//!
//! # Why the pull is a *direct store read*, not a mesh RPC
//!
//! Design §5 (`docs/design/artifact-library.md`): large artifacts pull **direct from the
//! durable store, per node** — the mesh `artifact.fetch` RPC rides the gossip frame
//! (10 MiB cap) and is for typical WASM components. A model blob streams from the library
//! via ranged reads, which is also what makes the loading-tier percent honest.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use coop::common::{alloc_ports, spawn_depot, DepotOpts};
use ed25519_dalek::SigningKey;
use mycelium::{CapFilter, CapValue, Capability, LlmBackend, OpenAiBackend};
use mycelium_wasm_host::{
    spawn_librarian, ArtifactKind, BlobRuntime, FsLibrarySource, InstallableCatalog,
    InstallableEntry, LibrarianConfig, Manifest, Provisioner, WasmHost, MANIFEST_FILE,
};

const MODEL_NAME: &str = "coop-storyteller";
const CAP_NS: &str = "llm";
const CAP_NAME: &str = "storyteller";

async fn wait_until(secs: u64, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    while std::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    cond()
}

fn preflight() -> Result<std::path::PathBuf, String> {
    let gguf = std::env::var("MODEL_GGUF").map_err(|_| {
        "MODEL_GGUF is not set.\n\
         Point it at any GGUF file, e.g.:\n\
         \n  curl -L -o /tmp/stories15M-q4_0.gguf \\\n    \
         https://huggingface.co/ggml-org/models/resolve/main/tinyllamas/stories15M-q4_0.gguf\n\
         \n  MODEL_GGUF=/tmp/stories15M-q4_0.gguf cargo run -p mycelium-coop-examples \\\n    \
         --features wasm --bin model_deploy"
            .to_string()
    })?;
    let path = std::path::PathBuf::from(&gguf);
    if !path.exists() {
        return Err(format!("MODEL_GGUF points at {gguf}, which does not exist"));
    }
    let ok = std::process::Command::new("ollama")
        .arg("list")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if !ok {
        return Err("`ollama list` failed — install Ollama and start the daemon (`ollama serve`)".into());
    }
    Ok(path)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(tracing::Level::ERROR).init();

    let gguf_path = match preflight() {
        Ok(p) => p,
        Err(msg) => {
            eprintln!("model_deploy is a manual demo and needs a local model + Ollama:\n\n{msg}");
            std::process::exit(1);
        }
    };
    let ollama_url =
        std::env::var("OLLAMA_URL").unwrap_or_else(|_| "http://localhost:11434".into());

    let cert_dir = std::env::temp_dir().join(format!("coop-model-{}", std::process::id()));
    let base = std::env::temp_dir().join(format!("coop-model-deploy-{}", std::process::id()));
    let (lib_dir, models_dir) = (base.join("library"), base.join("models"));
    let _ = std::fs::remove_dir_all(&cert_dir);
    let _ = std::fs::remove_dir_all(&base);
    let p = alloc_ports(6);

    // ── Phase 0 — CI publishes the model into the durable library ────────────────
    // Runtime read (no build-time embedding), content-addressed store, and a SIGNED entry:
    // kind=Blob, real cost hints, and the resource footprint (mem ≈ 4× file size — a loaded
    // llama-arch model wants weights + KV-cache/session overhead). The publisher key never
    // touches a node; Manifest::append_entry is the one-call CI publish step.
    let model_bytes = std::fs::read(&gguf_path)?;
    let model_size = model_bytes.len() as u64;
    let library = Arc::new(FsLibrarySource::open(&lib_dir)?);
    let artifact_id = library.store(&model_bytes)?;
    drop(model_bytes);
    let publisher_key = SigningKey::from_bytes(&[44u8; 32]);
    let publisher_pub = publisher_key.verifying_key().to_bytes();
    let entry = InstallableEntry::new(Capability::new(CAP_NS, CAP_NAME), artifact_id)
        .with_kind(ArtifactKind::Blob)
        .with_cost(model_size, 30)
        .with_requirements(/* disk */ model_size, /* mem */ model_size.saturating_mul(4))
        .signed_by(&publisher_key);
    Manifest::append_entry(&lib_dir.join(MANIFEST_FILE), entry)?;
    println!(
        "[ci] model published to the library: {} ({:.1} MB, artifact {})",
        gguf_path.display(),
        model_size as f64 / 1e6,
        &artifact_id.to_hex()[..12]
    );

    // ── librarian: serves the library + syncs manifest → catalogue ───────────────
    let librarian = spawn_depot(DepotOpts {
        name: "librarian".into(), gossip_port: p[0], http_port: p[1],
        zone: "hub".into(), bootstrap: vec![], cert_dir: cert_dir.clone(), health_secs: Some(2),
    }).await?;
    let seed = librarian.gossip_port;
    let _librarian_role = spawn_librarian(
        Arc::clone(&librarian.agent),
        Arc::clone(&library) as Arc<_>,
        LibrarianConfig {
            manifest_path: lib_dir.join(MANIFEST_FILE),
            publisher: publisher_pub,
            sync_interval: Duration::from_millis(500),
        },
    );
    println!("[librarian] up — manifest → catalogue sync running");

    // ── model-host: the node that will self-elect to run the model ───────────────
    let mk = |name: &str, gp: u16, hp: u16| DepotOpts {
        name: name.into(), gossip_port: gp, http_port: hp,
        zone: "depot".into(), bootstrap: vec![seed], cert_dir: cert_dir.clone(), health_secs: Some(2),
    };
    let host = spawn_depot(mk("model-host", p[2], p[3])).await?;
    let app = spawn_depot(mk("app", p[4], p[5])).await?;
    wait_until(20, || !host.agent.peers().is_empty() && !app.agent.peers().is_empty()).await;

    // The catalogue entry gossips in; only then can the provisioner resolve it.
    let filter = CapFilter::new(CAP_NS, CAP_NAME);
    assert!(
        wait_until(30, || {
            InstallableCatalog::from_kv(&host.agent.kv()).resolve_best(&filter).is_some()
        }).await,
        "the model's catalogue entry reaches the model-host"
    );
    let catalog = InstallableCatalog::from_kv(&host.agent.kv());
    println!("[model-host] found {CAP_NS}/{CAP_NAME} in the catalogue (signed, kind=Blob)");

    // Activation = a real `ollama create` from the placed file; the probe reads a health
    // bit the activation sets (probes must be cheap — they run on the provisioner tick).
    let activated = Arc::new(AtomicBool::new(false));
    let act_flag = Arc::clone(&activated);
    let probe_flag = Arc::clone(&activated);
    let runtime = BlobRuntime::new(&models_dir)
        .with_chunk_bytes(1 << 20) // 1 MiB chunks → real loading-tier percent
        .with_activation(move |placed| {
            let modelfile = placed.with_extension("Modelfile");
            std::fs::write(&modelfile, format!("FROM {}\n", placed.display()))
                .map_err(|e| e.to_string())?;
            let out = std::process::Command::new("ollama")
                .args(["create", MODEL_NAME, "-f"])
                .arg(&modelfile)
                .output()
                .map_err(|e| format!("ollama create: {e}"))?;
            if !out.status.success() {
                return Err(format!(
                    "ollama create failed: {}",
                    String::from_utf8_lossy(&out.stderr)
                ));
            }
            act_flag.store(true, Ordering::SeqCst);
            Ok(())
        })
        .with_probe(move |placed| placed.exists() && probe_flag.load(Ordering::SeqCst));

    // Direct store pull (design §5): the model streams from the durable library via ranged
    // reads — the mesh RPC's 10 MiB frame cap is for WASM-sized artifacts, not models.
    let mut prov = Provisioner::new(
        Arc::clone(&host.agent),
        Arc::new(WasmHost::new()?),
        catalog,
        Arc::new(FsLibrarySource::open(&lib_dir)?) as Arc<_>,
        1.0,
    );
    prov.register_runtime(Arc::new(runtime));
    prov.supervise(filter.clone(), 1);
    // The default resource policy is live here: this machine's real free memory/disk are
    // checked against the entry's declared footprint before the node elects.

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let ticker = tokio::spawn(async move {
        loop {
            if *stop_rx.borrow() {
                break;
            }
            prov.provision_round();
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        prov
    });
    println!("[model-host] provisioner ticking — self-election is resource-checked (real probe, real numbers)");

    // ── the app watches the REAL loading tier, then the capability appear ────────
    let loading_filter = CapFilter::new(CAP_NS, "loading");
    let mut last_pct: i64 = -1;
    let deadline = std::time::Instant::now() + Duration::from_secs(180);
    let deployed = loop {
        if std::time::Instant::now() > deadline {
            break false;
        }
        for (_node, cap) in app.agent.capabilities().resolve(&loading_filter) {
            if let Some(CapValue::Integer(pct)) = cap.attributes.get("pct")
                && *pct != last_pct
            {
                last_pct = *pct;
                println!("[app] {CAP_NS}/loading … {pct}% (real bytes, streamed from the library)");
            }
        }
        if !app.agent.capabilities().resolve(&filter).is_empty() {
            break true;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    };
    assert!(deployed, "the model capability must appear (pull + place + ollama create)");
    println!("[app] {CAP_NS}/{CAP_NAME} is live — placed, activated into Ollama, probe-gated");

    // ── the proof: generate real tokens through the deployed model ───────────────
    let backend = OpenAiBackend::new(format!("{}/v1", ollama_url.trim_end_matches('/')), "ollama", MODEL_NAME);
    let story = backend
        .complete(
            "You are the food co-op's newsletter storyteller.",
            "Once upon a time, on the night of the great surplus-bread rescue,",
            64,
            0.8,
        )
        .await?;
    println!("\n[app] the deployed model speaks:\n      “{}”\n", story.output.trim());
    assert!(!story.output.trim().is_empty(), "the deployed model generated real tokens");
    assert_eq!(story.model_used.split(':').next(), Some(MODEL_NAME),
        "the tokens came from the model we deployed, not a pre-existing one");

    println!(
        "All assertions passed — a real LLM model ({:.1} MB GGUF) was published to the durable \
         library, discovered via the signed catalogue, resource-checked, streamed with live \
         progress, placed, activated into Ollama, probe-gated — and generated real tokens.",
        model_size as f64 / 1e6
    );

    // ── cleanup ───────────────────────────────────────────────────────────────────
    let _ = stop_tx.send(true);
    let _ = ticker.await;
    let _ = std::process::Command::new("ollama").args(["rm", MODEL_NAME]).output();
    for d in [app, host, librarian] {
        d.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(&cert_dir);
    let _ = std::fs::remove_dir_all(&base);
    Ok(())
}
