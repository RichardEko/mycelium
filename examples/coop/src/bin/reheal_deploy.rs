//! Example M+ — **the deploy/reheal flagship, real-model half** (manual demo).
//!
//! The one story that beats a commodity checkpoint store on non-commodity terms, told with a
//! *real* neural network instead of the CI echo fixture: **a governed GGUF model reheals onto
//! the surviving node and generates real tokens through routed inference after the origin dies.**
//!
//! It is the composition of the two proven demos it sits between:
//!   · [`model_deploy`](model_deploy) — the artifact-library machinery: a GGUF and its
//!     deployment profile travel the durable library as two signed, content-addressed
//!     artifacts (the profile references the weights by content address, `FROM artifact:{hex}`),
//!     are resource-checked, streamed with a live loading-tier percent, and activated into
//!     Ollama by a [`Provisioner`] + [`BlobRuntime`]. Reused here almost verbatim.
//!   · `mycelium-reason`'s `reheal_node` (echo variant) — the *seam* a model dependency follows
//!     across a node failure: demand → a provider elects → [`serve_model`] bridges it into a
//!     mesh-invocable prompt skill → routed inference lands on the survivor. That demo proves
//!     the wiring deterministically with `EchoBackend`; **this** one swaps in real weights.
//!
//! What is genuinely *new* over `model_deploy` (a reviewer should diff against it and look here):
//!
//! 1. **Two provider depots** (origin **A**, survivor **B**), each running a `Provisioner` that
//!    `supervise(profile_filter, 1)`s the model. `supervise(min=1)` — keep exactly one live
//!    provider across the fleet — **is the reheal mechanism**: when the origin dies its
//!    presence advertisement clears, the live count drops below 1, and the survivor elects and
//!    installs. No orchestrator; the desired-state invariant self-heals (M14).
//! 2. **The `serve_model` bridge.** The provisioner installs the profile and advertises a
//!    *presence* capability (`llm/{MODEL}-deploy`) — but that is not `LLM_INVOKE`-routable. So on
//!    each node, once *its* activation has run `ollama create`, a per-node bridge task calls
//!    [`serve_model`] to register the **routable** skill `llm/{MODEL}` backed by a local-Ollama
//!    [`OpenAiBackend`]. The activation closure hands the bridge its node-unique Ollama model
//!    name over an mpsc; the bridge holds the returned `ModelReg` for life.
//! 3. **Routed inference** through `mycelium-reason`'s [`InferenceRouter`] (no gateway, no
//!    Python) — the app node routes `llm/{MODEL}` and asserts real tokens come back, once from
//!    the origin and again from the survivor after the origin is killed.
//!
//! ## The honest single-machine shared-Ollama caveat  (read this)
//!
//! On one host, A and B share **one** local Ollama daemon. So each node must `ollama create`
//! under its **own** name (`{MODEL_NAME}-{gossip_port}`): the survivor's activation is then a
//! *genuine fresh creation from the bytes it streamed*, and its `serve_model` backend points at
//! its own Ollama model — not a model the origin happened to leave behind. What follows the
//! thread across the node death on a single machine is therefore **the streamed GGUF bytes + the
//! Mycelium routable capability `llm/{MODEL}`** (node-unique Ollama name behind each). For the
//! true multi-machine story — a second physical Ollama — give each node its own daemon; the code
//! is identical, only `OLLAMA_URL` differs per node. Nothing here is simulated: the percent ticks
//! are real bytes, each activation is a real `ollama create`, and both stories came out of a
//! model that was just deployed onto the node that answered.
//!
//! Because it needs a local Ollama daemon and a GGUF, this demo is **manual** — deliberately NOT
//! in `ci_smoke.sh` (exactly like `model_deploy`).
//!
//! # Requirements
//!
//! - `ollama` installed and the daemon running (`ollama serve` or the desktop app).
//! - A GGUF file. The 19 MB TinyStories model is perfect for the co-op storyteller framing:
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
//!   cargo run -p mycelium-coop-examples --features wasm --bin reheal_deploy
//! ```
//!
//! Optional: `OLLAMA_URL` (default `http://localhost:11434`).
//!
//! ## Loads
//! - **Content** — a governed GGUF model — a real neural network, not the CI echo fixture
//! - **Type** — `ArtifactKind::Blob (weights) + deployment profile`
//! - **From** — streamed GGUF bytes + profile over the mesh; reheals onto a standby node via peer pull after the origin node dies (reuses the model_deploy machinery)

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use coop::common::{alloc_ports, announce_loads, spawn_depot, Depot, DepotOpts, Loads};
use ed25519_dalek::SigningKey;
use mycelium::{CapFilter, CapValue, Capability, LlmBackend, OpenAiBackend, PromptTemplate};
use mycelium_reason::{
    InferenceRouter, ModelProfile, ModelQuery, ModelReg, RouterConfig, serve_model,
};
use mycelium_wasm_host::{
    spawn_librarian, ArtifactKind, BlobRuntime, FsLibrarySource, InstallableCatalog,
    InstallableEntry, LibrarianConfig, Manifest, Provisioner, WasmHost, MANIFEST_FILE,
};
use tokio::sync::mpsc;

// The Ollama *base* model name. Each node creates under its own `{MODEL_NAME}-{gossip_port}`
// (see the shared-Ollama caveat in the module doc) so its activation is a genuine fresh create.
const MODEL_NAME: &str = "coop-storyteller";
const CAP_NS: &str = "llm";
// The **routable** skill `serve_model` registers and the `InferenceRouter` resolves: `llm/storyteller`.
const MODEL: &str = "storyteller";
// The **presence** capability the Provisioner advertises on install and `supervise`s — DISTINCT
// from `MODEL` so the two do not collide on the same `cap/{node}/llm/…` key (the provisioner's
// installed-presence ad vs. the bridge's serve_model skill would otherwise LWW-churn each other).
const DEPLOY_CAP: &str = "storyteller-deploy";
const WEIGHTS_CAP: &str = "storyteller-weights";

/// Structural poll (never a fixed sleep as an assertion) — mirrors `model_deploy::wait_until`.
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

/// Preflight — verbatim from `model_deploy` (only the binary name in the hint differs).
fn preflight() -> Result<std::path::PathBuf, String> {
    let gguf = std::env::var("MODEL_GGUF").map_err(|_| {
        "MODEL_GGUF is not set.\n\
         Point it at any GGUF file, e.g.:\n\
         \n  curl -L -o /tmp/stories15M-q4_0.gguf \\\n    \
         https://huggingface.co/ggml-org/models/resolve/main/tinyllamas/stories15M-q4_0.gguf\n\
         \n  MODEL_GGUF=/tmp/stories15M-q4_0.gguf cargo run -p mycelium-coop-examples \\\n    \
         --features wasm --bin reheal_deploy"
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

/// A live provider node (origin or survivor): its provisioner ticker + its serve_model bridge.
/// Both tasks hold RAII handles (the `Provisioner` owns the presence caps; the bridge owns the
/// `ModelReg`) — stopping/aborting them **tombstones** those caps, which is how "killing the
/// origin" removes its providership so the survivor's `supervise(min=1)` sees 0 and elects.
struct Provider {
    /// The node-unique Ollama model name (`{MODEL_NAME}-{gossip_port}`) — for `ollama rm` cleanup.
    ollama_name: String,
    /// Signals the ticker loop to stop; awaiting the join drops the `Provisioner` (tombstoning
    /// the presence caps while the agent is still alive — a fast, clean departure).
    stop_tx: tokio::sync::watch::Sender<bool>,
    ticker: tokio::task::JoinHandle<()>,
    /// Parks holding the `ModelReg`; aborting it drops the skill (tombstoning `llm/{MODEL}`).
    bridge: tokio::task::JoinHandle<()>,
}

impl Provider {
    /// Graceful teardown while the agent is still up, so tombstones gossip out (fast reheal).
    /// A *hard* crash would instead leave the caps to age out of the freshness window (~90 s);
    /// the reheal watch below is bounded at 180 s, which covers either path.
    async fn kill(self) {
        self.bridge.abort();
        let _ = self.bridge.await; // drops ModelReg → tombstones llm/{MODEL} + llm-meta/{MODEL}
        let _ = self.stop_tx.send(true);
        let _ = self.ticker.await; // drops Provisioner → tombstones the presence caps
    }
}

/// Build a provider node: one `BlobRuntime` (node-unique activation name) hosting both artifacts,
/// a bridge task, and a `Provisioner` supervising weights + profile at min=1. Structurally
/// identical to `model_deploy`'s single host, with the bridge as the only novel seam.
///
/// The catalogue snapshot (`InstallableCatalog::from_kv`) is point-in-time, so the caller must
/// already have waited for both entries to gossip into this node's KV.
fn build_provider(
    depot: &Depot,
    lib_dir: &std::path::Path,
    models_dir: std::path::PathBuf,
    ollama_url: &str,
    profile_hex: &str,
) -> Result<Provider, Box<dyn std::error::Error>> {
    let agent = Arc::clone(&depot.agent);
    // Node-unique Ollama name (shared-daemon honesty — see the module doc caveat).
    let ollama_name = format!("{MODEL_NAME}-{}", depot.gossip_port);

    // The bridge channel: the activation closure (sync, runs inside provision_round) hands the
    // node-unique Ollama name to the bridge task (async) exactly once `ollama create` succeeds.
    let (bridge_tx, mut bridge_rx) = mpsc::unbounded_channel::<String>();

    // ── One Blob runtime hosts both artifacts — copied from model_deploy, with two changes:
    //    · the `ollama create` name is node-unique (`ollama_name`);
    //    · on activation success the closure signals the bridge (`bridge_tx.send`).
    let activated = Arc::new(AtomicBool::new(false));
    let act_flag = Arc::clone(&activated);
    let probe_flag = Arc::clone(&activated);
    let act_profile_hex = profile_hex.to_string();
    let probe_profile_hex = profile_hex.to_string();
    let act_ollama_name = ollama_name.clone();
    let runtime = BlobRuntime::new(&models_dir)
        .with_chunk_bytes(1 << 20) // 1 MiB chunks → real loading-tier percent
        .with_activation(move |placed| {
            if placed.file_name().and_then(|n| n.to_str()) != Some(act_profile_hex.as_str()) {
                return Ok(()); // the weights: placement is the whole install
            }
            // The profile: resolve its `FROM artifact:{hex}` reference against THIS node's
            // placement dir (identical to model_deploy — the ordering mechanism is unchanged:
            // a profile that activates before its weights are placed fails and retries).
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
            // Node-unique create: the survivor's is a genuine fresh model from ITS streamed bytes.
            let out = std::process::Command::new("ollama")
                .args(["create", &act_ollama_name, "-f"])
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
            // Signal the bridge to serve the routable skill. Best-effort: if a probe hiccup
            // re-activates, the extra send is ignored (the bridge takes only the first).
            let _ = bridge_tx.send(act_ollama_name.clone());
            Ok(())
        })
        .with_probe(move |placed| {
            let is_profile =
                placed.file_name().and_then(|n| n.to_str()) == Some(probe_profile_hex.as_str());
            placed.exists() && (!is_profile || probe_flag.load(Ordering::SeqCst))
        });

    // ── The bridge task: on the activation's signal, register the ROUTABLE skill `llm/{MODEL}`
    //    backed by this node's local Ollama model, and hold the ModelReg for life. The system
    //    prompt is deliberately EMPTY — the governed profile's SYSTEM prompt is already baked
    //    into the created Ollama model, exactly as in model_deploy's direct call.
    let bridge_agent = Arc::clone(&agent);
    let bridge_ollama_url = ollama_url.to_string();
    let bridge = tokio::spawn(async move {
        let Some(ollama_model) = bridge_rx.recv().await else {
            return; // sender dropped without activating (node torn down before it won)
        };
        let backend: Arc<dyn LlmBackend> = Arc::new(OpenAiBackend::new(
            format!("{}/v1", bridge_ollama_url.trim_end_matches('/')),
            "ollama",
            ollama_model,
        ));
        let profile = ModelProfile {
            model: MODEL.to_string(),
            ctx_window: Some(8192),
            family: Some("llama".into()),
            extra: Vec::new(),
        };
        let template = PromptTemplate {
            system: String::new(), // governed SYSTEM lives in the deployed Ollama model
            user_template: "{{input}}".into(),
            max_tokens: 64,
            temperature: 0.8,
            metadata: HashMap::new(),
        };
        let reg: ModelReg = match serve_model(&bridge_agent, profile, template, backend).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("bridge serve_model failed: {e}");
                return;
            }
        };
        // Hold the skill (and its caps) alive until the task is aborted at kill/cleanup.
        let _reg = reg;
        std::future::pending::<()>().await;
    });

    // ── The Provisioner: direct-store pull (design §5) of both artifacts, supervised at min=1.
    let catalog = InstallableCatalog::from_kv(&agent.kv());
    let mut prov = Provisioner::new(
        Arc::clone(&agent),
        Arc::new(WasmHost::new()?),
        catalog,
        Arc::new(FsLibrarySource::open(lib_dir)?) as Arc<_>,
        1.0,
    );
    prov.register_runtime(Arc::new(runtime));
    // supervise(min=1): exactly one live provider of each across the fleet. When the origin dies
    // and its presence ad clears, these drop below 1 on the survivor → the survivor elects.
    prov.supervise(CapFilter::new(CAP_NS, WEIGHTS_CAP), 1);
    prov.supervise(CapFilter::new(CAP_NS, DEPLOY_CAP), 1);

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let ticker = tokio::spawn(async move {
        loop {
            if *stop_rx.borrow() {
                break;
            }
            prov.provision_round();
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        // `prov` drops here → its held presence caps tombstone (the reheal trigger on kill).
    });

    Ok(Provider { ollama_name, stop_tx, ticker, bridge })
}

/// Route one inference through the app's `InferenceRouter` and print the tokens.
async fn route_story(
    router: &InferenceRouter,
) -> Result<mycelium_reason::Routed, mycelium_reason::RouteError> {
    let q = ModelQuery::new(MODEL);
    router
        .call(
            &q,
            "Once upon a time, on the night of the great surplus-bread rescue,",
            &HashMap::new(),
            None,
        )
        .await
}

const LOADS: &[Loads] = &[Loads {
    content: "a governed GGUF model — a real neural network, not the CI echo fixture",
    kind: "ArtifactKind::Blob (weights) + deployment profile",
    from: "streamed GGUF bytes + profile over the mesh; reheals onto a standby node via peer \
           pull after the origin node dies (reuses the model_deploy machinery)",
}];

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    announce_loads(LOADS);
    tracing_subscriber::fmt().with_max_level(tracing::Level::ERROR).init();

    let gguf_path = match preflight() {
        Ok(p) => p,
        Err(msg) => {
            eprintln!("reheal_deploy is a manual demo and needs a local model + Ollama:\n\n{msg}");
            std::process::exit(1);
        }
    };
    let ollama_url =
        std::env::var("OLLAMA_URL").unwrap_or_else(|_| "http://localhost:11434".into());

    let cert_dir = std::env::temp_dir().join(format!("coop-reheal-{}", std::process::id()));
    let base = std::env::temp_dir().join(format!("coop-reheal-deploy-{}", std::process::id()));
    let lib_dir = base.join("library");
    let _ = std::fs::remove_dir_all(&cert_dir);
    let _ = std::fs::remove_dir_all(&base);
    let p = alloc_ports(8);

    // ── Phase 0 — CI publishes the two governed artifacts (verbatim from model_deploy) ──
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
        // The WEIGHTS installable: presence cap `llm/storyteller-weights`.
        InstallableEntry::new(Capability::new(CAP_NS, WEIGHTS_CAP), weights_id)
            .with_kind(ArtifactKind::Blob)
            .with_cost(model_size, 30)
            .with_requirements(/* disk */ model_size, /* mem */ model_size.saturating_mul(4))
            .signed_by(&publisher_key),
    )?;
    Manifest::append_entry(
        &manifest_path,
        // The PROFILE installable: presence cap `llm/storyteller-deploy` (NOT the routable
        // `llm/storyteller` — that one comes from the serve_model bridge, so the keys don't collide).
        InstallableEntry::new(Capability::new(CAP_NS, DEPLOY_CAP), profile_id)
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

    // ── librarian: serves the library + syncs manifest → catalogue (verbatim) ─────
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

    // ── origin (A), survivor (B), app: three depots bootstrapping the librarian ───
    let mk = |name: &str, gp: u16, hp: u16| DepotOpts {
        name: name.into(), gossip_port: gp, http_port: hp,
        zone: "depot".into(), bootstrap: vec![seed], cert_dir: cert_dir.clone(), health_secs: Some(2),
    };
    let origin = spawn_depot(mk("origin", p[2], p[3])).await?;
    let survivor = spawn_depot(mk("survivor", p[4], p[5])).await?;
    let app = spawn_depot(mk("app", p[6], p[7])).await?;
    let origin_id = origin.node_id();
    let survivor_id = survivor.node_id();
    wait_until(20, || {
        !origin.agent.peers().is_empty()
            && !survivor.agent.peers().is_empty()
            && !app.agent.peers().is_empty()
    })
    .await;

    // Both catalogue entries must gossip into EACH provider before its provisioner snapshots the
    // catalog (`from_kv` is point-in-time). Wait on both nodes.
    let weights_filter = CapFilter::new(CAP_NS, WEIGHTS_CAP);
    let deploy_filter = CapFilter::new(CAP_NS, DEPLOY_CAP);
    let both_resolve = |agent: &Arc<mycelium::GossipAgent>| {
        let cat = InstallableCatalog::from_kv(&agent.kv());
        cat.resolve_best(&deploy_filter).is_some() && cat.resolve_best(&weights_filter).is_some()
    };
    assert!(
        wait_until(30, || both_resolve(&origin.agent) && both_resolve(&survivor.agent)).await,
        "both catalogue entries reach both provider nodes"
    );
    println!("[origin+survivor] both found weights + profile in the catalogue (signed, kind=Blob)");

    let models_dir = |gp: u16| base.join(format!("models-{gp}"));

    // ── Bring the ORIGIN up first and let it win the single-provider election ─────
    // We start the origin's provisioner, then structurally wait until it is the sole live
    // provider serving `llm/{MODEL}`, BEFORE starting the survivor. This deterministically makes
    // the origin the initial server and the survivor a genuine dormant standby — so the reheal
    // (step 8) is unambiguous. (A plain `supervise(min=1)` with `self_elect_p = 1.0` across two
    // eager nodes could otherwise race and have BOTH install at once, which would not exercise a
    // real post-death election. Staggering also faithfully models "the origin came up first".)
    let origin_prov =
        build_provider(&origin, &lib_dir, models_dir(origin.gossip_port), &ollama_url, &profile_hex)?;
    let origin_ollama = origin_prov.ollama_name.clone();
    println!("[origin] provisioner ticking — resource-checked self-election (real probe, real numbers)");

    // Watch the REAL loading tier, then wait for the routable skill to go live on the origin.
    let loading_filter = CapFilter::new(CAP_NS, "loading");
    let model_filter = CapFilter::new(CAP_NS, MODEL);
    let mut last_pct: i64 = -1;
    let deadline = std::time::Instant::now() + Duration::from_secs(180);
    let origin_live = loop {
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
        // The routable `llm/{MODEL}` appears only after: pull → place → resolve profile →
        // ollama create → the bridge's serve_model. That is the whole real path.
        if !app.agent.capabilities().resolve(&model_filter).is_empty() {
            break true;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    };
    assert!(origin_live, "the routable model skill must go live on the origin");

    // Confirm exactly WHO serves it (the origin), and route real tokens through it.
    let router = InferenceRouter::new(Arc::clone(&app.agent), RouterConfig::default());
    let first = route_story(&router).await?;
    println!("\n[app] routed to {} (attempt {}), the deployed model speaks:\n      “{}”\n",
        first.provider, first.attempt, first.output.trim());
    assert!(!first.output.trim().is_empty(), "the origin generated real tokens");
    assert!(
        first.model_used.starts_with(MODEL_NAME),
        "the tokens came from the deployed model (got model_used={})",
        first.model_used
    );
    assert_eq!(
        first.provider, origin_id,
        "the origin is the initial provider (staggered single-election)"
    );
    println!("[app] origin {} serves llm/{MODEL} and generated real tokens", first.provider);

    // ── Now start the SURVIVOR as a dormant standby (it sees the origin's presence → min=1
    //    already satisfied → it does not install, until the origin dies). ─────────────
    let survivor_prov = build_provider(
        &survivor, &lib_dir, models_dir(survivor.gossip_port), &ollama_url, &profile_hex,
    )?;
    let survivor_ollama = survivor_prov.ollama_name.clone();
    println!("[survivor] provisioner ticking — dormant standby (supervise sees the origin, min=1 met)");
    // Confirm the survivor stays dormant while the origin is alive: over a short observation
    // window the live-provider count for llm/{MODEL} must never exceed 1 (min=1 already met).
    // The polling loop itself is the check — not a fixed sleep used as an assertion.
    let obs_deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < obs_deadline {
        assert!(
            app.agent.capabilities().resolve(&model_filter).len() <= 1,
            "survivor must stay dormant while the origin serves (supervise min=1 already met)"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // ── Kill the origin — the node currently serving llm/{MODEL} ──────────────────
    let killed = origin_id.clone();
    println!("\n[chaos] killing the origin {killed} — the node serving llm/{MODEL}");
    origin_prov.kill().await; // tombstones the origin's presence + skill caps (fast, clean)
    origin.shutdown().await;

    // ── Reheal: supervise(min=1) on the survivor now sees 0 providers of llm/{MODEL}-deploy →
    //    the survivor elects → streams the GGUF (real ranged-read percent) → ollama create
    //    (survivor's node-unique name) → the bridge serve_models llm/{MODEL} again. Watch the
    //    loading tier. Bounded at 180 s (covers even the freshness-window path if a hard crash
    //    had skipped the tombstones). ────────────────────────────────────────────────
    println!("[survivor] origin gone — waiting for the survivor to elect + reheal the model…");
    last_pct = -1;
    let deadline = std::time::Instant::now() + Duration::from_secs(180);
    let rehealed = loop {
        if std::time::Instant::now() > deadline {
            break false;
        }
        for (_node, cap) in app.agent.capabilities().resolve(&loading_filter) {
            if let Some(CapValue::Integer(pct)) = cap.attributes.get("pct")
                && *pct != last_pct
            {
                last_pct = *pct;
                println!("[app] {CAP_NS}/loading … {pct}% (survivor streaming the GGUF afresh)");
            }
        }
        // A provider that is BOTH live and not the killed origin.
        let live_provider = app
            .agent
            .capabilities()
            .resolve(&model_filter)
            .into_iter()
            .map(|(n, _)| n)
            .find(|n| n != &killed);
        if live_provider.is_some() {
            break true;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    };
    assert!(rehealed, "the survivor must reheal llm/{MODEL} after the origin dies");
    println!("[survivor] rehealed — llm/{MODEL} is live again on the survivor");

    // ── The proof: route + generate real tokens AGAIN — now from the survivor ─────
    let second = route_story(&router).await?;
    println!("\n[app] routed to {} (attempt {}), the REHEALED model speaks:\n      “{}”\n",
        second.provider, second.attempt, second.output.trim());
    assert!(!second.output.trim().is_empty(), "the survivor generated real tokens");
    assert_ne!(second.provider, killed, "the answer came from a DIFFERENT node than the killed origin");
    assert_eq!(second.provider, survivor_id, "the survivor is the new provider");
    assert!(
        second.model_used.starts_with(MODEL_NAME),
        "the rehealed tokens came from the deployed model (got model_used={})",
        second.model_used
    );

    println!(
        "All assertions passed — a real LLM ({:.1} MB GGUF) was governed as a signed artifact, \
         served on the origin through routed inference, and after the origin was KILLED the \
         survivor elected on a bare `supervise(min=1)` invariant, streamed the weights afresh, \
         `ollama create`d them under its own name, and generated real tokens through the same \
         routable capability `llm/{MODEL}`. The model followed the thread across the node death.",
        model_size as f64 / 1e6
    );

    // ── cleanup ───────────────────────────────────────────────────────────────────
    survivor_prov.kill().await;
    // Both node-unique Ollama models (origin's + survivor's) — see the shared-daemon caveat.
    // The origin's Provider was consumed by `kill()`, so we saved its name earlier.
    for name in [origin_ollama.as_str(), survivor_ollama.as_str()] {
        let _ = std::process::Command::new("ollama").args(["rm", name]).output();
    }
    for d in [app, survivor, librarian] {
        d.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(&cert_dir);
    let _ = std::fs::remove_dir_all(&base);
    Ok(())
}
