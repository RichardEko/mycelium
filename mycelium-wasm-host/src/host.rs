//! The component host: the wasmtime [`Engine`] plus [`HostState`] — the per-component context
//! that the WIT imports map onto.
//!
//! [`HostState`]'s scoped operations (`kv_get`/`kv_set`/`kv_delete`/`emit`) are the
//! enforcement-bearing host surface. They are defined and tested here against a **live node**
//! first; the `bindgen!`-generated `Host` trait impls (the canonical-ABI wiring) delegate
//! straight to them, so the host→substrate mapping is proven before the wasm plumbing lands.

use std::sync::Arc;

use bytes::Bytes;
use mycelium::{KvHandle, MeshHandle, NodeId, SignalScope};
use wasmtime::component::{Component, HasSelf, Linker};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use mycelium::CapFilter;

use crate::artifact::{verify_artifact, ArtifactId, ArtifactSource};
use crate::catalog::InstallableCatalog;
use crate::confine::{confine_key, ConfinementError};

/// Generated host/guest bindings for [`wit/host.wit`]. Wrapped in a private module so the
/// generated `mycelium::host::*` paths don't collide with the `mycelium` *crate* dependency.
mod bindings {
    wasmtime::component::bindgen!({
        world: "capability-component",
        path: "wit/host.wit",
    });
}

// The component's request/response records (the capability export's ABI types).
pub use bindings::exports::mycelium::host::capability::{Request, Response};

// ── WIT import impls: the canonical-ABI host functions delegate to HostState's tested,
//    enforcement-bearing scoped operations. Confinement failures fail *closed* (deny + warn);
//    the WIT signatures carry no error channel, so an escaping guest is silently refused. ──

impl bindings::mycelium::host::kv::Host for HostState {
    fn get(&mut self, key: String) -> Option<Vec<u8>> {
        match self.kv_get(&key) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(component = %self.namespace, %key, %e, "denied component kv.get");
                None
            }
        }
    }
    fn set(&mut self, key: String, value: Vec<u8>) {
        if let Err(e) = self.kv_set(&key, value) {
            tracing::warn!(component = %self.namespace, %key, %e, "denied component kv.set");
        }
    }
    fn delete(&mut self, key: String) {
        if let Err(e) = self.kv_delete(&key) {
            tracing::warn!(component = %self.namespace, %key, %e, "denied component kv.delete");
        }
    }
}

impl bindings::mycelium::host::mesh::Host for HostState {
    fn emit(&mut self, kind: String, payload: Vec<u8>) {
        HostState::emit(self, &kind, payload);
    }
}

impl bindings::mycelium::host::log::Host for HostState {
    fn info(&mut self, message: String) {
        tracing::info!(component = %self.namespace, "{message}");
    }
    fn warn(&mut self, message: String) {
        tracing::warn!(component = %self.namespace, "{message}");
    }
}

// Gives the WASI host implementation access to this component's restricted WASI context.
impl WasiView for HostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView { ctx: &mut self.wasi, table: &mut self.table }
    }
}

/// Per-component host context carried in the wasmtime `Store`. Holds the component's identity
/// (node + capability namespace) and the **scoped** Mycelium handles its imports map onto.
pub struct HostState {
    node_id:   NodeId,
    namespace: Arc<str>,
    kv:        KvHandle,
    mesh:      MeshHandle,
    // A *restricted, deny-by-default* WASI context: std-based guests import wasi:* from libc
    // init, so the host must provide it — but with no filesystem, network, env, or inherited
    // stdio. The guest's only real doors remain our scoped kv/mesh/log imports.
    wasi:      WasiCtx,
    table:     ResourceTable,
}

impl HostState {
    /// Build the context for a component instance providing capability `namespace` on `node_id`.
    pub fn new(node_id: NodeId, namespace: impl Into<Arc<str>>, kv: KvHandle, mesh: MeshHandle) -> Self {
        Self {
            node_id,
            namespace: namespace.into(),
            kv,
            mesh,
            wasi: WasiCtxBuilder::new().build(), // deny-by-default: no fs/net/env/stdio
            table: ResourceTable::new(),
        }
    }

    /// The capability namespace this component provides (its confinement scope).
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    // ── Scoped host operations (what the WIT imports map onto) ────────────────

    /// `kv.get` — read a component-relative key from the component's confined subtree.
    pub fn kv_get(&mut self, key: &str) -> Result<Option<Vec<u8>>, ConfinementError> {
        let abs = confine_key(&self.node_id, &self.namespace, key)?;
        Ok(self.kv.get(&abs).map(|b| b.to_vec()))
    }

    /// `kv.set` — write a component-relative key. Out-of-subtree keys are refused.
    pub fn kv_set(&mut self, key: &str, value: Vec<u8>) -> Result<(), ConfinementError> {
        let abs = confine_key(&self.node_id, &self.namespace, key)?;
        let _ = self.kv.set(abs, Bytes::from(value));
        Ok(())
    }

    /// `kv.delete` — tombstone a component-relative key.
    pub fn kv_delete(&mut self, key: &str) -> Result<(), ConfinementError> {
        let abs = confine_key(&self.node_id, &self.namespace, key)?;
        let _ = self.kv.delete(abs);
        Ok(())
    }

    /// `mesh.emit` — broadcast a signal into the mesh (`System` scope for v0).
    pub fn emit(&mut self, kind: &str, payload: Vec<u8>) {
        let _ = self.mesh.emit(kind.to_string(), SignalScope::Cluster, Bytes::from(payload));
    }
}

/// The component host: a shared wasmtime [`Engine`] configured for the Component Model. Cheap
/// to clone-share across instantiations; one per node is plenty.
pub struct WasmHost {
    engine:        Engine,
    /// Opt-in fuel budget granted to each `invoke` call. `Some(n)` ⇒ a component that runs past
    /// `n` wasm instructions **traps** instead of hanging the serve task (deterministic). `None` ⇒
    /// unmetered (the engine doesn't even enable fuel — zero overhead).
    fuel_per_call: Option<u64>,
}

/// Errors from host setup / instantiation / invocation.
#[derive(Debug)]
pub enum WasmHostError {
    /// The wasmtime engine could not be created with the requested config.
    Engine(String),
    /// The artifact bytes are not a valid component, or linking/instantiation failed.
    Instantiate(String),
    /// Calling the component's `handle` export trapped or failed at the ABI.
    Invoke(String),
    /// No source held the requested artifact.
    Fetch(String),
    /// Fetched bytes did not match the requested content address.
    Verify(String),
}

impl std::fmt::Display for WasmHostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Engine(e) => write!(f, "wasm engine init failed: {e}"),
            Self::Instantiate(e) => write!(f, "component instantiation failed: {e}"),
            Self::Invoke(e) => write!(f, "component invocation failed: {e}"),
            Self::Fetch(e) => write!(f, "artifact fetch failed: {e}"),
            Self::Verify(e) => write!(f, "artifact verification failed: {e}"),
        }
    }
}

impl std::error::Error for WasmHostError {}

impl WasmHost {
    /// Create a host with a Component-Model-enabled engine (no fuel metering — zero overhead).
    pub fn new() -> Result<Self, WasmHostError> {
        Self::build(None)
    }

    /// Create a host that grants each `invoke` a fuel budget of `fuel_per_call` wasm instructions.
    /// A component exceeding it traps (`WasmHostError::Invoke`) rather than hanging the serve task —
    /// the safety bound recommended for serving untrusted components.
    pub fn with_fuel_per_call(fuel_per_call: u64) -> Result<Self, WasmHostError> {
        Self::build(Some(fuel_per_call))
    }

    fn build(fuel_per_call: Option<u64>) -> Result<Self, WasmHostError> {
        let mut cfg = Config::new();
        cfg.wasm_component_model(true);
        if fuel_per_call.is_some() {
            cfg.consume_fuel(true);
        }
        let engine = Engine::new(&cfg).map_err(|e| WasmHostError::Engine(e.to_string()))?;
        Ok(Self { engine, fuel_per_call })
    }

    /// The shared engine (components are instantiated against it).
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Compile `component_bytes` and instantiate it against `state` (the component's scoped host
    /// context). The returned [`Instance`] is ready to [`invoke`](Instance::invoke). The host's
    /// scoped imports (kv/mesh/log) are wired into the linker here; the guest can reach the node
    /// *only* through them.
    pub fn instantiate(&self, component_bytes: &[u8], state: HostState) -> Result<Instance, WasmHostError> {
        let component = Component::new(&self.engine, component_bytes)
            .map_err(|e| WasmHostError::Instantiate(e.to_string()))?;
        let mut linker: Linker<HostState> = Linker::new(&self.engine);
        // Restricted WASI (std-based guests link wasi:* at init) + our scoped host imports.
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker)
            .map_err(|e| WasmHostError::Instantiate(e.to_string()))?;
        bindings::CapabilityComponent::add_to_linker::<_, HasSelf<HostState>>(&mut linker, |s| s)
            .map_err(|e| WasmHostError::Instantiate(e.to_string()))?;
        let mut store = Store::new(&self.engine, state);
        // Component instantiation (libc/WASI init) runs guest code; give it unlimited fuel so the
        // per-call budget bounds only `invoke`, not start-up. (No-op when fuel is disabled.)
        if self.fuel_per_call.is_some() {
            store.set_fuel(u64::MAX).map_err(|e| WasmHostError::Instantiate(e.to_string()))?;
        }
        let world = bindings::CapabilityComponent::instantiate(&mut store, &component, &linker)
            .map_err(|e| WasmHostError::Instantiate(e.to_string()))?;
        Ok(Instance { store, world, fuel_per_call: self.fuel_per_call })
    }

    /// **Pull + verify + instantiate** — the M12 mechanism end to end. Fetch the artifact for
    /// `id` from `source`, **verify** the bytes against the content address *before* handing them
    /// to the engine (the source is untrusted), then instantiate against `state`. A node trusts
    /// the catalog that gave it `id`, not the bytes' origin.
    pub fn provision(
        &self,
        source: &(impl ArtifactSource + ?Sized),
        id: &ArtifactId,
        state: HostState,
    ) -> Result<Instance, WasmHostError> {
        let bytes = source
            .fetch(id)
            .ok_or_else(|| WasmHostError::Fetch(format!("no source holds artifact {id}")))?;
        verify_artifact(&bytes, id).map_err(|e| WasmHostError::Verify(e.to_string()))?;
        self.instantiate(&bytes, state)
    }

    /// **The full autonomic step (M15 → M12):** resolve `filter` against `catalog` to pick the
    /// best installable artifact, then pull + verify + instantiate it from `source`. `Ok(None)`
    /// means *no catalog entry satisfies the requirement* (the loop simply does not fire — not an
    /// error). This is the one-shot "a requirement appeared; become a provider of it" path; the
    /// standing demand-watch loop that calls it is the provisioner agent (an app-layer concern).
    pub fn provision_for(
        &self,
        catalog: &InstallableCatalog,
        filter: &CapFilter,
        source: &(impl ArtifactSource + ?Sized),
        state: HostState,
    ) -> Result<Option<Instance>, WasmHostError> {
        match catalog.resolve_best(filter) {
            Some(entry) => self.provision(source, &entry.artifact, state).map(Some),
            None => Ok(None),
        }
    }
}

/// A live, instantiated capability component plus its per-instance store. The capability is
/// invoked by calling its `handle` export ([`invoke`](Self::invoke)).
pub struct Instance {
    store:         Store<HostState>,
    world:         bindings::CapabilityComponent,
    fuel_per_call: Option<u64>,
}

impl Instance {
    /// Invoke the component's capability `handle(kind, payload)` export. The outer `Result` is a
    /// host/ABI failure (trap — incl. fuel exhaustion); the inner `Result` is the component's own
    /// success payload or its returned error string.
    pub fn invoke(&mut self, kind: &str, payload: Vec<u8>) -> Result<Result<Vec<u8>, String>, WasmHostError> {
        // Refuel per call so each invocation gets the full budget (and a runaway component traps
        // instead of hanging the serve task).
        if let Some(budget) = self.fuel_per_call {
            self.store.set_fuel(budget).map_err(|e| WasmHostError::Invoke(e.to_string()))?;
        }
        let req = Request { kind: kind.to_string(), payload };
        let resp = self
            .world
            .mycelium_host_capability()
            .call_handle(&mut self.store, &req)
            .map_err(|e| WasmHostError::Invoke(e.to_string()))?;
        Ok(match resp.error {
            Some(e) => Err(e),
            None => Ok(resp.payload),
        })
    }

    /// Access the underlying host state (e.g. for diagnostics).
    pub fn state(&self) -> &HostState {
        self.store.data()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alloc_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
    }

    async fn live_agent() -> std::sync::Arc<mycelium::GossipAgent> {
        // Retry on bind failure: alloc_port() has a TOCTOU window, so under parallel tests two
        // agents can race for the same freed port (AddrInUse). A fresh port per attempt removes
        // the flake deterministically.
        for _ in 0..16 {
            let port = alloc_port();
            let id = NodeId::new("127.0.0.1", port).unwrap();
            let cfg = mycelium::GossipConfig { bind_port: port, ..Default::default() };
            let agent = std::sync::Arc::new(mycelium::GossipAgent::new(id, cfg));
            if agent.start().await.is_ok() {
                return agent;
            }
        }
        panic!("could not bind a gossip port after 16 attempts");
    }

    #[test]
    fn wasm_host_engine_builds() {
        let host = WasmHost::new().expect("engine");
        // The engine is component-model-capable; a no-op smoke that the dep + config are sane.
        let _ = host.engine();
    }

    #[tokio::test]
    async fn provision_fetches_and_verifies_before_instantiating() {
        use crate::artifact::{ArtifactId, ArtifactSource, InMemorySource};
        use bytes::Bytes;

        let agent = live_agent().await;
        let host = WasmHost::new().expect("engine");
        let st = |a: &mycelium::GossipAgent| HostState::new(a.node_id().clone(), "nlp", a.kv(), a.mesh());

        // Unknown id → Fetch error (never reaches verify/instantiate).
        let empty = InMemorySource::new();
        let missing = ArtifactId::of(b"never stored");
        assert!(matches!(host.provision(&empty, &missing, st(&agent)), Err(WasmHostError::Fetch(_))));

        // Stored, content-addressed bytes pass verify, then fail instantiate (not a real
        // component) — proving fetch+verify ran and instantiate was reached.
        let mut src = InMemorySource::new();
        let id = src.insert(Bytes::from_static(b"\0asm-but-not-a-component"));
        assert!(matches!(host.provision(&src, &id, st(&agent)), Err(WasmHostError::Instantiate(_))));

        // A source that returns bytes not matching the id → Verify error (bytes never instantiated).
        struct LyingSource;
        impl ArtifactSource for LyingSource {
            fn fetch(&self, _id: &ArtifactId) -> Option<Bytes> {
                Some(Bytes::from_static(b"substituted bytes"))
            }
        }
        let claimed = ArtifactId::of(b"what the catalog promised");
        assert!(matches!(host.provision(&LyingSource, &claimed, st(&agent)), Err(WasmHostError::Verify(_))));

        agent.shutdown().await;
    }

    #[tokio::test]
    async fn instantiate_rejects_non_component_bytes() {
        // The instantiate path is wired (linker + bindgen + Store); feeding it garbage proves it
        // reaches Component::new and fails cleanly rather than mis-linking. A positive end-to-end
        // with a real guest component is the M12 follow-up (needs the wasm guest toolchain).
        let agent = live_agent().await;
        let host = WasmHost::new().expect("engine");
        let state = HostState::new(agent.node_id().clone(), "nlp", agent.kv(), agent.mesh());
        let err = host.instantiate(b"not a wasm component", state);
        assert!(matches!(err, Err(WasmHostError::Instantiate(_))));
        agent.shutdown().await;
    }

    #[tokio::test]
    async fn scoped_kv_round_trips_into_the_component_subtree_on_a_live_node() {
        let agent = live_agent().await;
        let mut state = HostState::new(agent.node_id().clone(), "nlp", agent.kv(), agent.mesh());

        // A component-relative write/read round-trips...
        state.kv_set("state/cursor", b"42".to_vec()).unwrap();
        assert_eq!(state.kv_get("state/cursor").unwrap(), Some(b"42".to_vec()));

        // ...and it actually landed under the confined comp/ subtree (not cap/).
        let abs = format!("comp/{}/nlp/state/cursor", agent.node_id());
        assert_eq!(agent.kv().get(&abs).map(|b| b.to_vec()), Some(b"42".to_vec()));

        // delete tombstones it.
        state.kv_delete("state/cursor").unwrap();
        assert_eq!(state.kv_get("state/cursor").unwrap(), None);

        agent.shutdown().await;
    }

    #[tokio::test]
    async fn a_component_cannot_escape_its_subtree() {
        let agent = live_agent().await;
        let mut state = HostState::new(agent.node_id().clone(), "nlp", agent.kv(), agent.mesh());

        // Traversal / absolute escapes are refused at the host boundary (prevention, not detection).
        assert!(state.kv_set("../evil", b"x".to_vec()).is_err());
        assert!(state.kv_get("/etc/secret").is_err());
        assert!(state.kv_set("a/../../cap/poison", b"x".to_vec()).is_err());

        // None of the refused writes reached the store.
        assert!(agent.kv().get(&format!("cap/{}/poison", agent.node_id())).is_none());

        agent.shutdown().await;
    }
}
