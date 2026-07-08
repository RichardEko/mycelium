//! Integration for the policy-audit **verification tool** — real tls agents.
//!
//! After a Tier-C denial (an unauthorized caller is stopped at the provider gate and the denial is
//! sealed), `prove_denials` must reconstruct the provider's chain and attest the stop:
//!   - the chain verifies, the denial is sealed, and it names the unauthorized caller + guarded kind;
//!   - a neutral observer node proves it identically (the chain gossips fleet-wide);
//!   - the *authorized* caller — admitted, never denied — yields an empty proof (the negative control).
//!
//! Structural polls on generous timeouts, never fixed sleeps; every agent is shut down.
#![cfg(feature = "compliance")]
#![allow(clippy::field_reassign_with_default)] // GossipConfig is built the way mycelium's own tests do

use std::sync::Arc;
use std::time::Duration;

use mycelium::{GossipAgent, GossipConfig, NodeId, TlsConfig};
use mycelium_guardrails::{apply, guarded_rpc_serve, prove_denials, Policy};

const KIND: &str = "agent.tool.invoke";

/// The core's bind-verified, process-unique loopback allocator (`mycelium::test_util::alloc_port`,
/// the `test-util` feature) — retires the old bind-:0-and-drop TOCTOU flake class.
fn free_port() -> u16 {
    mycelium::test_util::alloc_port()
}

async fn try_start(port: u16, boot: Vec<u16>, cert_dir: &std::path::Path) -> Option<Arc<GossipAgent>> {
    let mut cfg = GossipConfig::default();
    cfg.bind_port = port;
    cfg.bootstrap_peers = boot.into_iter().map(|p| NodeId::new("127.0.0.1", p).unwrap()).collect();
    cfg.reconnect_backoff_secs = 1;
    cfg.health_check_interval_secs = 1;
    cfg.tls = Some(TlsConfig { auto_cert_dir: cert_dir.to_path_buf(), ..TlsConfig::default() });
    let agent = Arc::new(GossipAgent::new(NodeId::new("127.0.0.1", port).unwrap(), cfg));
    agent.start().await.ok().map(|_| agent)
}

async fn start_mesh(n: usize, cert_dir: &std::path::Path) -> Vec<Arc<GossipAgent>> {
    for _ in 0..16 {
        let ports: Vec<u16> = (0..n).map(|_| free_port()).collect();
        let mut agents = Vec::with_capacity(n);
        let mut ok = true;
        for (i, &p) in ports.iter().enumerate() {
            let boot = if i == 0 { vec![] } else { vec![ports[0]] };
            match try_start(p, boot, cert_dir).await {
                Some(a) => agents.push(a),
                None => {
                    ok = false;
                    break;
                }
            }
        }
        if ok {
            return agents;
        }
        for a in agents {
            a.shutdown_with_timeout(Duration::from_secs(5)).await;
        }
    }
    panic!("could not bind a {n}-agent tls mesh after 16 attempts");
}

async fn poll_until(mut cond: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cond()
}

fn cert_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("myc-verify-{tag}-{}", free_port()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// After a Tier-C denial, `prove_denials` reconstructs the provider's chain and proves the stop —
/// from the provider itself and from a neutral observer — and finds nothing for the authorized caller.
#[tokio::test]
async fn prove_denials_reconstructs_the_sealed_denial() {
    let dir = cert_dir("prove");
    // 0 = provider, 1 = authorized caller, 2 = unauthorized caller, 3 = neutral observer.
    let agents = start_mesh(4, &dir).await;
    let (provider, ok_caller, no_caller, observer) = (
        Arc::clone(&agents[0]),
        Arc::clone(&agents[1]),
        Arc::clone(&agents[2]),
        Arc::clone(&agents[3]),
    );

    assert!(
        poll_until(|| agents.iter().all(|a| a.peers().len() >= 3), Duration::from_secs(30)).await,
        "the 4-node mesh forms"
    );

    // The provider guards KIND with authorized_callers = [ok_caller node-id].
    let policy = Policy::new().authorized_callers([ok_caller.node_id().to_string()]);
    let applied = apply(policy, &provider).await;
    let _guard = guarded_rpc_serve(&applied, KIND, move |agent, req| async move {
        agent.service().rpc_respond(&req, req.payload());
    });

    // Unauthorized caller invokes → denied and sealed.
    let denied = no_caller
        .service()
        .rpc_call(provider.node_id().clone(), KIND, b"payload".to_vec(), Duration::from_secs(10))
        .await
        .expect("unauthorized call gets an error reply");
    assert!(String::from_utf8_lossy(&denied).contains("unauthorized"), "the caller is refused");

    // Authorized caller invokes → admitted (so its own chain holds no denial).
    let ok = ok_caller
        .service()
        .rpc_call(provider.node_id().clone(), KIND, b"echo-me".to_vec(), Duration::from_secs(10))
        .await
        .expect("authorized call gets a reply");
    assert_eq!(&ok[..], b"echo-me", "the authorized caller is admitted");

    let no_id = no_caller.node_id().to_string();
    let ok_id = ok_caller.node_id().to_string();

    // The provider proves the stop from its own chain.
    assert!(
        poll_until(
            || {
                let p = prove_denials(&provider, provider.node_id(), Some(&no_id));
                p.chain_verified && !p.denials.is_empty()
            },
            Duration::from_secs(20),
        )
        .await,
        "the provider's verified chain carries the sealed denial"
    );
    let proof = prove_denials(&provider, provider.node_id(), Some(&no_id));
    assert!(proof.chain_verified, "the chain verifies");
    assert_eq!(&proof.provider, provider.node_id());
    assert!(proof.verify_error.is_none());
    assert!(!proof.denials.is_empty(), "the denial is sealed");
    let d = &proof.denials[0];
    assert_eq!(d.caller, no_id, "the sealed principal is the unauthorized caller");
    assert_eq!(d.target, KIND, "the sealed target is the guarded kind");
    // seq are non-decreasing (sorted).
    assert!(proof.denials.windows(2).all(|w| w[0].seq <= w[1].seq), "denials are seq-sorted");

    // A neutral observer proves it identically — the chain gossips fleet-wide.
    assert!(
        poll_until(
            || {
                let p = prove_denials(&observer, provider.node_id(), Some(&no_id));
                p.chain_verified && !p.denials.is_empty()
            },
            Duration::from_secs(20),
        )
        .await,
        "a neutral observer reconstructs the same verified denial"
    );

    // Negative: the authorized caller was never denied — an empty proof (still a verified chain).
    let none = prove_denials(&provider, provider.node_id(), Some(&ok_id));
    assert!(none.chain_verified, "the chain still verifies");
    assert!(none.denials.is_empty(), "the authorized caller has no sealed denial");

    for a in agents {
        a.shutdown_with_timeout(Duration::from_secs(5)).await;
    }
    let _ = std::fs::remove_dir_all(&dir);
}
