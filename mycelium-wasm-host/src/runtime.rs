//! Artifact runtimes — the kind-dispatch axis of install (`docs/design/artifact-library.md` §4).
//!
//! "Install" is not one operation. A WASM component is pulled → verified → **instantiated**
//! (`WasmHost`) and served over RPC; a model blob is pulled → verified → **placed** for a
//! node-local runtime to consume, its capability probe-gated on that runtime answering. The
//! [`ArtifactRuntime`] trait owns the *how* per [`ArtifactKind`]; the `Provisioner` owns the
//! *when* (the demand/presence/shed convergence loops, which are kind-agnostic) and dispatches
//! through its runtime registry. A node registers runtimes only for the kinds it can host —
//! **eligibility is node-local truth** (no GPU → no model runtime → this node never self-elects
//! for model artifacts; some other node does).

use std::sync::Arc;

use mycelium::GossipAgent;

use crate::artifact::{ArtifactKind, ArtifactSource};
use crate::catalog::InstallableEntry;
use crate::host::{HostState, Instance, WasmHost, WasmHostError};

/// The RPC `kind` an inbound caller uses to invoke the hosted capability `ns/name`. A caller
/// resolves the capability to a provider node, then `rpc_call(provider, cap_invoke_kind(ns, name),
/// payload, timeout)`; the hosting runtime's serve loop routes it to the component's `handle`
/// export.
pub fn cap_invoke_kind(namespace: &str, name: &str) -> String {
    format!("cap.invoke/{namespace}/{name}")
}

/// Install-progress callback: `(bytes_fetched, bytes_total)`. `bytes_total = 0` means unknown.
/// For large blob pulls this drives the *real* loading-tier advertisement (the percent the
/// `llm_agent` example used to simulate); small WASM installs report a single completion tick.
pub type ProgressFn = Arc<dyn Fn(u64, u64) + Send + Sync>;

/// What a runtime needs from the node to bring an artifact live.
#[derive(Clone)]
pub struct RuntimeCtx {
    pub agent: Arc<GossipAgent>,
}

/// Why an install failed. Wraps the failing stage's message; the provisioner logs it, drops the
/// reservation, and lets a later round retry (restart ≡ provisioning).
#[derive(Debug)]
pub struct InstallError(pub String);

impl std::fmt::Display for InstallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "install failed: {}", self.0)
    }
}

impl std::error::Error for InstallError {}

impl From<WasmHostError> for InstallError {
    fn from(e: WasmHostError) -> Self {
        Self(e.to_string())
    }
}

/// A live installation — the node-level lifecycle handle a runtime returns from
/// [`ArtifactRuntime::install`].
pub trait Installed: Send {
    /// Is the capability actually servable right now? For a WASM component: the serve task
    /// lives. For a placed model: the local runtime answers (the probe-gating hook).
    fn probe(&self) -> bool;

    /// Cooperative teardown: stop serving and clean up (abort tasks, delete placed bytes).
    fn uninstall(self: Box<Self>);
}

/// How one [`ArtifactKind`] is installed and torn down on this node.
#[async_trait::async_trait]
pub trait ArtifactRuntime: Send + Sync {
    /// The artifact kind this runtime installs.
    fn kind(&self) -> ArtifactKind;

    /// Pull (from `source`), verify, and bring the artifact live. May be long-running (a
    /// multi-GB blob) — the provisioner runs it as a background task against an `Installing`
    /// reservation, never blocking the provision tick. The runtime must have a live inbound
    /// path (serve handler, probing runtime) *before* returning: the provisioner advertises
    /// the declared-provide on success, and a resolvable capability must always have a
    /// receiver behind it.
    async fn install(
        &self,
        entry: InstallableEntry,
        source: Arc<dyn ArtifactSource + Send + Sync>,
        ctx: RuntimeCtx,
        progress: ProgressFn,
    ) -> Result<Box<dyn Installed>, InstallError>;
}

/// Serve loop for one hosted WASM capability: owns the component [`Instance`] (wasmtime stores
/// are single-threaded, so one task per instance serialises calls) and answers each inbound RPC
/// by invoking the component's `handle` export and replying with its output.
async fn serve_loop(
    agent: Arc<GossipAgent>,
    mut instance: Instance,
    mut rx: mycelium::RpcRequestRx,
) {
    while let Some(req) = rx.recv().await {
        let payload = req.payload().to_vec();
        // NB: wasm execution is synchronous and blocks this task for its duration — fine for
        // short handlers; long-running components want fuel/epoch limits + spawn_blocking (follow-up).
        let result: Vec<u8> = match instance.invoke("invoke", payload) {
            Ok(Ok(out)) => out,
            Ok(Err(e)) => format!("component-error: {e}").into_bytes(),
            Err(e) => format!("host-error: {e}").into_bytes(),
        };
        agent.service().rpc_respond(&req, result);
    }
}

/// The [`ArtifactKind::WasmComponent`] runtime — the original install path (pull + verify +
/// instantiate + serve), relocated behind the trait unchanged. `WasmHost` is the engine *inside*
/// this runtime, not the definition of install.
pub struct WasmComponentRuntime {
    host: Arc<WasmHost>,
}

impl WasmComponentRuntime {
    pub fn new(host: Arc<WasmHost>) -> Self {
        Self { host }
    }
}

#[async_trait::async_trait]
impl ArtifactRuntime for WasmComponentRuntime {
    fn kind(&self) -> ArtifactKind {
        ArtifactKind::WasmComponent
    }

    async fn install(
        &self,
        entry: InstallableEntry,
        source: Arc<dyn ArtifactSource + Send + Sync>,
        ctx: RuntimeCtx,
        progress: ProgressFn,
    ) -> Result<Box<dyn Installed>, InstallError> {
        let state = HostState::new(
            ctx.agent.node_id().clone(),
            entry.provides.namespace.clone(),
            ctx.agent.kv(),
            ctx.agent.mesh(),
        );
        // Components are small (well under the mesh frame cap): pull + verify + instantiate in
        // one step. Chunked/ranged pulls with incremental progress are the blob runtime's job.
        let instance = self.host.provision(&*source, &entry.artifact, state)?;
        progress(entry.size_bytes, entry.size_bytes);

        // Register the inbound serve handler *before* returning, so the advertisement the
        // provisioner makes on success always finds a live RPC receiver.
        let invoke_kind = cap_invoke_kind(&entry.provides.namespace, &entry.provides.name);
        let rx = ctx.agent.service().rpc_rx(invoke_kind);
        let serve = tokio::spawn(serve_loop(Arc::clone(&ctx.agent), instance, rx));
        Ok(Box::new(WasmInstalled { serve }))
    }
}

/// A live, serving WASM component installation.
struct WasmInstalled {
    serve: tokio::task::JoinHandle<()>,
}

impl Installed for WasmInstalled {
    fn probe(&self) -> bool {
        !self.serve.is_finished()
    }

    fn uninstall(self: Box<Self>) {
        self.serve.abort();
    }
}
