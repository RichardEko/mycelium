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

/// Per-component host context carried in the wasmtime `Store`. Holds the component's identity
/// (node + capability namespace) and the **scoped** Mycelium handles its imports map onto.
pub struct HostState {
    node_id:   NodeId,
    namespace: Arc<str>,
    kv:        KvHandle,
    mesh:      MeshHandle,
}

impl HostState {
    /// Build the context for a component instance providing capability `namespace` on `node_id`.
    pub fn new(node_id: NodeId, namespace: impl Into<Arc<str>>, kv: KvHandle, mesh: MeshHandle) -> Self {
        Self { node_id, namespace: namespace.into(), kv, mesh }
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
        let _ = self.mesh.emit(kind.to_string(), SignalScope::System, Bytes::from(payload));
    }
}

/// The component host: a shared wasmtime [`Engine`] configured for the Component Model. Cheap
/// to clone-share across instantiations; one per node is plenty.
pub struct WasmHost {
    engine: Engine,
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
}

impl std::fmt::Display for WasmHostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Engine(e) => write!(f, "wasm engine init failed: {e}"),
            Self::Instantiate(e) => write!(f, "component instantiation failed: {e}"),
            Self::Invoke(e) => write!(f, "component invocation failed: {e}"),
        }
    }
}

impl std::error::Error for WasmHostError {}

impl WasmHost {
    /// Create a host with a Component-Model-enabled engine.
    pub fn new() -> Result<Self, WasmHostError> {
        let mut cfg = Config::new();
        cfg.wasm_component_model(true);
        let engine = Engine::new(&cfg).map_err(|e| WasmHostError::Engine(e.to_string()))?;
        Ok(Self { engine })
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
        bindings::CapabilityComponent::add_to_linker::<_, HasSelf<HostState>>(&mut linker, |s| s)
            .map_err(|e| WasmHostError::Instantiate(e.to_string()))?;
        let mut store = Store::new(&self.engine, state);
        let world = bindings::CapabilityComponent::instantiate(&mut store, &component, &linker)
            .map_err(|e| WasmHostError::Instantiate(e.to_string()))?;
        Ok(Instance { store, world })
    }
}

/// A live, instantiated capability component plus its per-instance store. The capability is
/// invoked by calling its `handle` export ([`invoke`](Self::invoke)).
pub struct Instance {
    store: Store<HostState>,
    world: bindings::CapabilityComponent,
}

impl Instance {
    /// Invoke the component's capability `handle(kind, payload)` export. The outer `Result` is a
    /// host/ABI failure (trap); the inner `Result` is the component's own success payload or its
    /// returned error string.
    pub fn invoke(&mut self, kind: &str, payload: Vec<u8>) -> Result<Result<Vec<u8>, String>, WasmHostError> {
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
        let port = alloc_port();
        let id = NodeId::new("127.0.0.1", port).unwrap();
        let cfg = mycelium::GossipConfig { bind_port: port, ..Default::default() };
        let agent = std::sync::Arc::new(mycelium::GossipAgent::new(id, cfg));
        agent.start().await.unwrap();
        agent
    }

    #[test]
    fn wasm_host_engine_builds() {
        let host = WasmHost::new().expect("engine");
        // The engine is component-model-capable; a no-op smoke that the dep + config are sane.
        let _ = host.engine();
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
