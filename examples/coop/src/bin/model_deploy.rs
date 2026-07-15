//! Example M — **deploy a real LLM model through the artifact library** (manual demo).
//!
//! The proof the artifact library's Blob path is real end to end — with **both halves of a
//! model deployment governed**: the **weights** (a genuine GGUF) *and* the **profile** (the
//! deployment configuration — system prompt, parameters) travel the library as two signed,
//! content-addressed artifacts. The profile references the weights **by content address**
//! (`FROM artifact:{hex}`), and activation resolves that reference against the local
//! placement dir — no paths, no node names, no dependency resolver: a profile that activates
//! before its weights are placed simply fails and retries (restart ≡ provisioning is the
//! ordering mechanism, preserving the M15 one-hop contract).
//!
//! Flow: CI → durable library → catalogue → resource-checked self-election → **streamed
//! chunked pull with live loading-tier percent** → placement → profile **resolution +
//! activation into Ollama** → probe-gated capability → an app node **generates real tokens**
//! under the governed profile. Nothing is simulated: the percent ticks are real bytes, the
//! activation is a real `ollama create`, the SYSTEM prompt Ollama runs is asserted to be the
//! one that arrived in the signed profile, and the story at the end came out of the model
//! that was just deployed.
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
//!
//! ## Loads
//! - **Content** — a real GGUF LLM model (genuine weights, not a stub)
//! - **Type** — `ArtifactKind::Blob`
//! - **From** — library → gossip catalogue → resource-checked election → chunked mesh pull (live percent) → placement → ollama create → probe-gated
//!
//! - **Content** — the model's deployment profile (system prompt + parameters)
//! - **Type** — `profile artifact (references the weights by content address)`
//! - **From** — library → catalogue → resolved against the placed weights

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use coop::common::{alloc_ports, announce_loads, spawn_depot, DepotOpts, Loads};
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

const LOADS: &[Loads] = &[
    Loads {
        content: "a real GGUF LLM model (genuine weights, not a stub)",
        kind: "ArtifactKind::Blob",
        from: "library → gossip catalogue → resource-checked election → chunked mesh pull \
               (live percent) → placement → ollama create → probe-gated",
    },
    Loads {
        content: "the model's deployment profile (system prompt + parameters)",
        kind: "profile artifact (references the weights by content address)",
        from: "library → catalogue → resolved against the placed weights",
    },
];

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    announce_loads(LOADS);
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

    // ── Phase 0 — CI publishes TWO governed artifacts into the durable library ──
    // 1. The WEIGHTS: the GGUF, runtime-read (no build-time embedding), content-addressed,
    //    signed, with the real resource footprint (mem ≈ 4× file size — a loaded llama-arch
    //    model wants weights + KV-cache/session overhead).
    // 2. The PROFILE: the deployment configuration (system prompt, parameters) is an
    //    artifact too — signed, versioned by content address like everything else. It
    //    references the weights BY CONTENT ADDRESS (`FROM artifact:{hex}`): no path, no
    //    node-name — activation resolves the reference against the local placement dir.
    // The publisher key never touches a node; Manifest::append_entry is the CI publish step.
    let model_bytes = std::fs::read(&gguf_path)?;
    let model_size = model_bytes.len() as u64;
    let library = Arc::new(FsLibrarySource::open(&lib_dir)?);
    let weights_id = library.store(&model_bytes)?;
    drop(model_bytes);
    let weights_hex = weights_id.to_hex();

    let profile_text = format!(
        "# coop-storyteller deployment profile — a governed artifact, signed like the weights.\n\
         FROM artifact:{weights_hex}\n\
         SYSTEM \"\"\"You are the food co-op's newsletter storyteller. Every tale celebrates rescued surplus food.\"\"\"\n\
         PARAMETER temperature 0.8\n"
    );
    let profile_id = library.store(profile_text.as_bytes())?;
    let profile_hex = profile_id.to_hex();

    let publisher_key = SigningKey::from_bytes(&[44u8; 32]);
    let publisher_pub = publisher_key.verifying_key().to_bytes();
    let manifest_path = lib_dir.join(MANIFEST_FILE);
    Manifest::append_entry(
        &manifest_path,
        InstallableEntry::new(Capability::new(CAP_NS, "storyteller-weights"), weights_id)
            .with_kind(ArtifactKind::Blob)
            .with_cost(model_size, 30)
            .with_requirements(/* disk */ model_size, /* mem */ model_size.saturating_mul(4))
            .signed_by(&publisher_key),
    )?;
    Manifest::append_entry(
        &manifest_path,
        InstallableEntry::new(Capability::new(CAP_NS, CAP_NAME), profile_id)
            .with_kind(ArtifactKind::Blob)
            .with_cost(profile_text.len() as u64, 5)
            .with_requirements(profile_text.len() as u64, 0)
            .signed_by(&publisher_key),
    )?;
    println!(
        "[ci] published weights ({:.1} MB, artifact {}) + profile ({} B, artifact {}) — the profile \
         references the weights by content address",
        model_size as f64 / 1e6,
        &weights_hex[..12],
        profile_text.len(),
        &profile_hex[..12]
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

    // Both catalogue entries gossip in; only then can the provisioner resolve them.
    let filter = CapFilter::new(CAP_NS, CAP_NAME);
    let weights_filter = CapFilter::new(CAP_NS, "storyteller-weights");
    assert!(
        wait_until(30, || {
            let cat = InstallableCatalog::from_kv(&host.agent.kv());
            cat.resolve_best(&filter).is_some() && cat.resolve_best(&weights_filter).is_some()
        }).await,
        "both catalogue entries (weights + profile) reach the model-host"
    );
    let catalog = InstallableCatalog::from_kv(&host.agent.kv());
    println!("[model-host] found weights + profile in the catalogue (both signed, kind=Blob)");

    // One Blob runtime hosts both artifacts. Content-addressed placement means the closures
    // know exactly which file they were handed — dispatch is by hash, no sniffing:
    //   · the WEIGHTS need no activation (placement is the whole job; default-probe = exists);
    //   · the PROFILE's activation resolves its `FROM artifact:{hex}` reference to the placed
    //     weights path and runs the real `ollama create`. If the weights aren't placed yet the
    //     activation FAILS — and that is the ordering mechanism: the reservation drops and a
    //     later round retries (restart ≡ provisioning; no dependency resolver, M15 one-hop).
    // The probe reads a health bit the activation sets (probes are cheap-under-lock).
    let activated = Arc::new(AtomicBool::new(false));
    let act_flag = Arc::clone(&activated);
    let probe_flag = Arc::clone(&activated);
    let (act_profile_hex, probe_profile_hex) = (profile_hex.clone(), profile_hex.clone());
    let runtime = BlobRuntime::new(&models_dir)
        .with_chunk_bytes(1 << 20) // 1 MiB chunks → real loading-tier percent
        .with_activation(move |placed| {
            if placed.file_name().and_then(|n| n.to_str()) != Some(act_profile_hex.as_str()) {
                return Ok(()); // the weights: placement is the whole install
            }
            // The profile: resolve the content-address reference against the placement dir.
            let text = std::fs::read_to_string(placed).map_err(|e| e.to_string())?;
            let referenced_hex = text
                .lines()
                .find_map(|l| l.strip_prefix("FROM artifact:"))
                .ok_or("profile has no `FROM artifact:` reference")?
                .trim()
                .to_string();
            let weights_path = placed.with_file_name(&referenced_hex);
            if !weights_path.exists() {
                return Err(format!(
                    "referenced weights artifact {} not placed yet — retrying next round",
                    &referenced_hex[..12]
                ));
            }
            let resolved = text.replace(
                &format!("artifact:{referenced_hex}"),
                &weights_path.display().to_string(),
            );
            let modelfile = placed.with_extension("Modelfile");
            std::fs::write(&modelfile, resolved).map_err(|e| e.to_string())?;
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
        .with_probe(move |placed| {
            let is_profile =
                placed.file_name().and_then(|n| n.to_str()) == Some(probe_profile_hex.as_str());
            placed.exists() && (!is_profile || probe_flag.load(Ordering::SeqCst))
        });

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
    prov.supervise(weights_filter.clone(), 1);
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
    assert!(deployed, "the model capability must appear (pull + place + resolve profile + ollama create)");
    println!("[app] {CAP_NS}/{CAP_NAME} is live — weights placed, profile resolved + activated into Ollama, probe-gated");

    // The GOVERNED profile is what Ollama is actually running: its SYSTEM prompt (signed
    // into the catalogue, not hardcoded on any node) must be live in the created model.
    let show = std::process::Command::new("ollama").args(["show", MODEL_NAME]).output()?;
    let show_text = String::from_utf8_lossy(&show.stdout).to_string();
    assert!(
        show_text.contains("newsletter storyteller"),
        "the deployed PROFILE's system prompt is live in Ollama (governed config, not node-local hardcoding)"
    );
    println!("[app] `ollama show {MODEL_NAME}` carries the profile's SYSTEM prompt — the config that arrived is the config that runs");

    // ── the proof: generate real tokens through the deployed model ───────────────
    // The system prompt is deliberately NOT passed here — it came from the deployed profile.
    let backend = OpenAiBackend::new(format!("{}/v1", ollama_url.trim_end_matches('/')), "ollama", MODEL_NAME);
    let story = backend
        .complete(
            "",
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
        "All assertions passed — a real LLM ({:.1} MB GGUF weights) AND its deployment profile \
         (system prompt + parameters, referencing the weights by content address) travelled the \
         durable library as two signed artifacts, were resource-checked, streamed with live \
         progress, resolved + activated into Ollama, probe-gated — and generated real tokens \
         under the governed profile.",
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
