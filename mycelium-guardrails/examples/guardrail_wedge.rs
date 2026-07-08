//! The guardrail **wedge**, made runnable — "an agent is structurally stopped; here is the
//! cryptographic proof."
//!
//! A neighbourhood microgrid co-op runs a small agent fleet. One node **provides** a governed tool
//! (`agent.tool.invoke`) behind a Tier-C `authorized_callers` gate. Two peers try to invoke it:
//! one is **not** on the allowlist and is structurally stopped at the provider's gate (its denial
//! sealed into the tamper-evident audit chain); one **is** and is admitted. Then a *third*
//! observer node — holding no special role — reconstructs the provider's chain and prints the
//! cryptographic proof the unauthorized agent was stopped.
//!
//! Honest framing (binding #3): the proof attests the provider *tamper-evidently sealed stopping*
//! the unauthorized caller — provable-stopping. It is **not** a global "could not have done Y
//! anywhere" claim: the chain is per-node, and only gated capabilities seal denials.
//!
//! tls is required because audit records are Ed25519-signed and `req.sender()` is only trustworthy
//! under the tls identity — so the sealed principal is the *verified* caller, not a self-asserted
//! string.
//!
//! Run: `cargo run -p mycelium-guardrails --features compliance --example guardrail_wedge`.
//! Exits 0 on success (prints `WEDGE OK`); asserted by `ci_smoke.sh` on the printed markers.
#![allow(clippy::field_reassign_with_default)] // GossipConfig is built the way mycelium's own tests do

use std::sync::Arc;
use std::time::Duration;

use mycelium::{GossipAgent, GossipConfig, NodeId, TlsConfig};
use mycelium_guardrails::{apply, guarded_rpc_serve, narrate_proof, prove_denials, Policy};

const KIND: &str = "agent.tool.invoke";

/// A free TCP port (bind :0, read it, drop) — reused across a retrying mesh start, so the
/// drop-then-rebind TOCTOU window is covered by the whole-set retry.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// Start one tls agent sharing `cert_dir` (shared CA), `None` if the bind lost the port race.
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

/// Start `n` mutually-bootstrapped tls agents, retrying the whole set on a lost `free_port` race.
/// Node 0 is the boot anchor; the rest bootstrap to it. Shared `cert_dir` = shared CA.
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

/// Poll `cond` until true or `timeout` elapses — structural, never a fixed sleep.
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

#[tokio::main]
async fn main() {
    // ── (1) A 4-node tls mesh: provider, authorized, unauthorized, and a neutral observer. ──
    let dir = std::env::temp_dir().join(format!("myc-guardrail-wedge-{}", free_port()));
    let _ = std::fs::remove_dir_all(&dir);
    let agents = start_mesh(4, &dir).await;
    let (provider, authorized, unauthorized, observer) = (
        Arc::clone(&agents[0]),
        Arc::clone(&agents[1]),
        Arc::clone(&agents[2]),
        Arc::clone(&agents[3]),
    );

    assert!(
        poll_until(|| agents.iter().all(|a| a.peers().len() >= 3), Duration::from_secs(30)).await,
        "the 4-node mesh forms"
    );
    println!("mesh: 4 tls nodes formed (provider, authorized, unauthorized, observer)");

    // ── (2) The provider declares and applies its policy, and shows the strength report. ──
    let policy = Policy::new()
        .act_within_groups(["microgrid-ops"]) // Tier A — the receiver boundary.
        .deny_tools(["shell"]) // Tier B — a self-imposed transition guard.
        .authorized_callers([authorized.node_id().to_string()]); // Tier C — the hard gate.
    let applied = apply(policy.clone(), &provider).await;

    println!("provider: policy strength report (the legibility — which clause is which guarantee):");
    for clause in policy.strength_report() {
        println!("  · {} [{}] — {}", clause.name, clause.tier.label(), clause.detail);
    }

    // The Tier-C gate in front of the tool handler: authorized callers reach it; unauthorized
    // callers are dropped with a sealed `Invoke`/`Denied` and an error reply.
    let _guard = guarded_rpc_serve(&applied, KIND, move |agent, req| async move {
        // The governed tool: acknowledge the authorized invocation.
        agent.service().rpc_respond(&req, b"tool-ack".to_vec());
    });

    // ── (3) The unauthorized agent invokes → structurally stopped at the provider gate. ──
    let denied = unauthorized
        .service()
        .rpc_call(provider.node_id().clone(), KIND, b"do-something".to_vec(), Duration::from_secs(10))
        .await
        .expect("unauthorized call still gets a reply (an error), not a timeout");
    assert!(
        String::from_utf8_lossy(&denied).contains("unauthorized"),
        "the unauthorized caller is refused: {}",
        String::from_utf8_lossy(&denied)
    );
    println!("✗ unauthorized agent structurally stopped at the provider gate");

    // ── (4) The authorized agent invokes → admitted, handler runs. ──
    let ok = authorized
        .service()
        .rpc_call(provider.node_id().clone(), KIND, b"do-something".to_vec(), Duration::from_secs(10))
        .await
        .expect("authorized call gets a reply");
    assert_eq!(&ok[..], b"tool-ack", "the authorized caller's handler runs");
    println!("✓ authorized agent admitted");

    // ── (5) The proof: a THIRD observer reconstructs the provider's chain and proves it. ──
    // The audit chain gossips fleet-wide, so any node can prove the denial — not only the provider.
    let unauthorized_id = unauthorized.node_id().to_string();
    assert!(
        poll_until(
            || {
                let p = prove_denials(&observer, provider.node_id(), Some(&unauthorized_id));
                p.chain_verified && !p.denials.is_empty()
            },
            Duration::from_secs(30),
        )
        .await,
        "the observer sees the provider's verified chain with the sealed denial"
    );

    let proof = prove_denials(&observer, provider.node_id(), Some(&unauthorized_id));
    assert!(proof.chain_verified, "the provider's chain verifies");
    assert!(!proof.denials.is_empty(), "the denial is sealed");
    let denial = &proof.denials[0];
    assert_eq!(denial.caller, unauthorized_id, "the sealed principal is the unauthorized caller");
    assert_eq!(denial.target, KIND, "the sealed target is the guarded kind");

    println!("proof (reconstructed by a neutral observer node — the chain is readable fleet-wide):");
    for line in narrate_proof(&proof) {
        println!("{line}");
    }

    // Negative control: the authorized caller was never denied — an empty proof, honestly.
    let authorized_id = authorized.node_id().to_string();
    let no_proof = prove_denials(&observer, provider.node_id(), Some(&authorized_id));
    assert!(no_proof.denials.is_empty(), "the authorized caller has no sealed denial");
    println!(
        "control: authorized caller {} has 0 sealed denials (as it should — it was admitted)",
        authorized_id
    );

    // ── (6) Deterministic teardown. ──
    for a in agents {
        a.shutdown_with_timeout(Duration::from_secs(5)).await;
    }
    let _ = std::fs::remove_dir_all(&dir);

    println!("WEDGE OK");
}
