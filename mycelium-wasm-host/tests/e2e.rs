//! End-to-end: a real WASM guest component (`tests/fixtures/echo_component.wasm`, built from
//! `tests/fixtures/echo-component/`) is instantiated against a live node and invoked. This proves
//! the host⇄component boundary actually *executes* — the guest's `kv` import crosses into the
//! node's store, confined to its subtree — not merely that the wiring type-checks.
//!
//! The `.wasm` is committed so CI needs no wasm toolchain; regenerate with the fixture's `build.sh`.

use mycelium::{CapFilter, Capability, GossipAgent, GossipConfig, NodeId};
use mycelium_wasm_host::{
    ArtifactId, HostState, InMemorySource, InstallableCatalog, InstallableEntry, WasmHost,
};
use std::sync::Arc;

const ECHO_COMPONENT: &[u8] = include_bytes!("fixtures/echo_component.wasm");

fn alloc_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

async fn live_agent() -> Arc<GossipAgent> {
    // Retry on bind failure (alloc_port has a TOCTOU window; parallel tests can race for a freed
    // port). A fresh port per attempt removes the AddrInUse flake.
    for _ in 0..16 {
        let port = alloc_port();
        let id = NodeId::new("127.0.0.1", port).unwrap();
        let cfg = GossipConfig { bind_port: port, ..Default::default() };
        let agent = Arc::new(GossipAgent::new(id, cfg));
        if agent.start().await.is_ok() {
            return agent;
        }
    }
    panic!("could not bind a gossip port after 16 attempts");
}

#[tokio::test]
async fn echo_component_executes_and_its_kv_import_crosses_the_confined_boundary() {
    let agent = live_agent().await;
    let host = WasmHost::new().expect("engine");
    let state = HostState::new(agent.node_id().clone(), "nlp", agent.kv(), agent.mesh());

    let mut instance = host.instantiate(ECHO_COMPONENT, state).expect("instantiate real component");

    // The guest stores the payload via kv.set, reads it back, and echoes it.
    let out = instance
        .invoke("greet", b"hello from the host".to_vec())
        .expect("no host/ABI trap")
        .expect("guest returned success");
    assert_eq!(out, b"hello from the host", "component echoed the payload");

    // The guest's kv.set actually crossed into the node's store — confined to comp/{node}/nlp/.
    let abs = format!("comp/{}/nlp/last-input", agent.node_id());
    assert_eq!(
        agent.kv().get(&abs).map(|b| b.to_vec()),
        Some(b"hello from the host".to_vec()),
        "guest write landed in the confined component subtree"
    );

    agent.shutdown().await;
}

#[tokio::test]
async fn provision_pulls_verifies_and_runs_a_real_component() {
    let agent = live_agent().await;
    let host = WasmHost::new().expect("engine");

    // Publish the component into a content-addressed source, then provision by id.
    let mut catalog = InMemorySource::new();
    let id: ArtifactId = catalog.insert(ECHO_COMPONENT.to_vec());

    let state = HostState::new(agent.node_id().clone(), "vision", agent.kv(), agent.mesh());
    let mut instance = host.provision(&catalog, &id, state).expect("provision (fetch+verify+instantiate)");

    let out = instance.invoke("ping", b"42".to_vec()).expect("no trap").expect("guest ok");
    assert_eq!(out, b"42");

    agent.shutdown().await;
}

#[tokio::test]
async fn provision_for_resolves_a_requirement_then_pulls_and_runs_the_match() {
    // The full M15→M12 path: a requirement (CapFilter) is resolved against an installable
    // catalog to pick an ArtifactId, which is then pulled + verified + instantiated.
    let agent = live_agent().await;
    let host = WasmHost::new().expect("engine");

    // The artifact bytes live in a content-addressed source...
    let mut source = InMemorySource::new();
    let id: ArtifactId = source.insert(ECHO_COMPONENT.to_vec());

    // ...and the catalog declares what installing it would provide, keyed by that id.
    let mut catalog = InstallableCatalog::new();
    catalog.add(InstallableEntry::new(Capability::new("text", "echo"), id).with_cost(52_266, 1));

    let state = HostState::new(agent.node_id().clone(), "text", agent.kv(), agent.mesh());

    // Resolve "I need text/echo" → pick the artifact → pull + verify + instantiate.
    let mut instance = host
        .provision_for(&catalog, &CapFilter::new("text", "echo"), &source, state)
        .expect("provision_for ok")
        .expect("a catalog entry satisfied the requirement");
    let out = instance.invoke("run", b"resolved!".to_vec()).expect("no trap").expect("guest ok");
    assert_eq!(out, b"resolved!");

    // A requirement nothing provides → Ok(None), no error (the loop simply doesn't fire).
    let state2 = HostState::new(agent.node_id().clone(), "text", agent.kv(), agent.mesh());
    let none = host
        .provision_for(&catalog, &CapFilter::new("audio", "transcribe"), &source, state2)
        .expect("provision_for ok");
    assert!(none.is_none(), "unsatisfiable requirement yields Ok(None)");

    agent.shutdown().await;
}
