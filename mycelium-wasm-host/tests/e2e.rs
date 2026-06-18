//! End-to-end: a real WASM guest component (`tests/fixtures/echo_component.wasm`, built from
//! `tests/fixtures/echo-component/`) is instantiated against a live node and invoked. This proves
//! the host⇄component boundary actually *executes* — the guest's `kv` import crosses into the
//! node's store, confined to its subtree — not merely that the wiring type-checks.
//!
//! The `.wasm` is committed so CI needs no wasm toolchain; regenerate with the fixture's `build.sh`.

use mycelium::{GossipAgent, GossipConfig, NodeId};
use mycelium_wasm_host::{ArtifactId, HostState, InMemorySource, WasmHost};
use std::sync::Arc;

const ECHO_COMPONENT: &[u8] = include_bytes!("fixtures/echo_component.wasm");

fn alloc_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

async fn live_agent() -> Arc<GossipAgent> {
    let port = alloc_port();
    let id = NodeId::new("127.0.0.1", port).unwrap();
    let cfg = GossipConfig { bind_port: port, ..Default::default() };
    let agent = Arc::new(GossipAgent::new(id, cfg));
    agent.start().await.unwrap();
    agent
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
