//! Cross-node integration for the self-imposed, tier-labelled policy — real tls agents.
//!
//! Two scenarios:
//!   1. `apply` joins the declared Tier-A groups (a group-scoped signal now reaches the node) and
//!      installs the Tier-B AgentPolicy (a `denied_tools` transition is rejected).
//!   2. The Tier-C wedge in miniature: a provider guards a served RPC kind with
//!      `authorized_callers`; an unauthorized caller is denied *and* the denial is sealed into the
//!      provider's tamper-evident audit chain (verified principal) — and the chain still verifies.
//!      An authorized caller is admitted and its handler runs.
//!
//! Structural polls on generous timeouts, never fixed sleeps; every agent is shut down.
#![cfg(feature = "compliance")]
#![allow(clippy::field_reassign_with_default)] // GossipConfig is built the way mycelium's own tests do

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use mycelium::{
    AuditAction, AuditOutcome, ExecutionState, GossipAgent, GossipConfig, NodeId, PolicyViolation,
    SignalScope, TlsConfig,
};
use mycelium_guardrails::{apply, guarded_rpc_serve, Policy};

/// A free TCP port (bind :0, read it, drop). The drop opens a TOCTOU window against parallel
/// test binaries — which is why agents start via a retry, never a bare unwrap.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// Start one tls agent sharing `cert_dir` (so the mesh shares a CA), `None` if the bind lost the
/// port race.
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
    let dir = std::env::temp_dir().join(format!("myc-guardrails-{tag}-{}", free_port()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Scenario 1 — Tier A (boundary join makes a group-scoped signal reach the node) + Tier B (a
/// `denied_tools` transition is rejected with `ToolDenied`).
#[tokio::test]
async fn apply_joins_groups_and_installs_agent_policy() {
    let dir = cert_dir("apply");
    let agents = start_mesh(2, &dir).await;
    let (node, peer) = (Arc::clone(&agents[0]), Arc::clone(&agents[1]));

    assert!(
        poll_until(|| !node.peers().is_empty() && !peer.peers().is_empty(), Duration::from_secs(20)).await,
        "mesh forms"
    );

    // Subscribe on the node BEFORE joining, so the receiver exists when the group signal arrives.
    let mut rx = node.mesh().signal_rx("guardrails.ping");

    let policy = Policy::new()
        .act_within_groups(["ops"])
        .deny_tools(["shell"]);
    let applied = apply(policy, &node).await;

    // Tier A: the node published grp/ops membership; the peer must observe it before a
    // Group("ops")-scoped emit can be admission-routed to the node.
    assert!(
        poll_until(|| peer.mesh().group_members("ops").iter().any(|m| m == node.node_id()), Duration::from_secs(20)).await,
        "the peer sees the node's grp/ops membership"
    );

    // The peer emits a group-scoped signal; the node (a member) acts on it — the boundary admits it.
    let _ = peer.mesh().emit("guardrails.ping", SignalScope::Group("ops".into()), b"hi".to_vec());
    let got = tokio::time::timeout(Duration::from_secs(20), rx.recv()).await;
    assert!(matches!(got, Ok(Some(_))), "the joined node receives the group-scoped signal");

    // Tier B: the installed AgentPolicy rejects a denied tool at the state transition.
    let sm = applied.state_machine();
    let verdict = sm.transition(ExecutionState::Invoking { tool: "shell".into() }).await;
    assert!(
        matches!(&verdict, Err(PolicyViolation::ToolDenied(t)) if t == "shell"),
        "a denied-tool transition is rejected: {verdict:?}"
    );

    for a in agents {
        a.shutdown_with_timeout(Duration::from_secs(5)).await;
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario 2 — the Tier-C hard-prevention wedge in miniature: an unauthorized caller is denied
/// and the denial is provably sealed; an authorized caller is admitted.
#[tokio::test]
async fn tier_c_denies_unauthorized_caller_and_seals_the_denial() {
    let dir = cert_dir("tierc");
    // 0 = provider, 1 = authorized caller, 2 = unauthorized caller.
    let agents = start_mesh(3, &dir).await;
    let (provider, ok_caller, no_caller) =
        (Arc::clone(&agents[0]), Arc::clone(&agents[1]), Arc::clone(&agents[2]));

    assert!(
        poll_until(|| agents.iter().all(|a| a.peers().len() >= 2), Duration::from_secs(30)).await,
        "the 3-node mesh forms"
    );

    // The provider guards RPC kind `guarded.echo` with authorized_callers = [ok_caller node-id].
    let policy = Policy::new().authorized_callers([ok_caller.node_id().to_string()]);
    let applied = apply(policy, &provider).await;

    let handled = Arc::new(AtomicBool::new(false));
    let handled_c = Arc::clone(&handled);
    let _guard = guarded_rpc_serve(&applied, "guarded.echo", move |agent, req| {
        let handled = Arc::clone(&handled_c);
        async move {
            handled.store(true, Ordering::SeqCst);
            agent.service().rpc_respond(&req, req.payload());
        }
    });

    // Structural poll: both callers must have registered the provider as a peer (routing works) —
    // covered by the mesh-forms poll above. Also poll that each caller learns the provider's
    // identity so caller_authorized can resolve the sender — implicit under tls once peered.

    // Unauthorized caller invokes → gets a denial reply, handler never ran.
    let denied_reply = no_caller
        .service()
        .rpc_call(provider.node_id().clone(), "guarded.echo", b"payload".to_vec(), Duration::from_secs(10))
        .await;
    let denied_bytes = denied_reply.expect("unauthorized call still gets a reply (an error), not a timeout");
    assert!(
        String::from_utf8_lossy(&denied_bytes).contains("unauthorized"),
        "unauthorized caller is refused: {}",
        String::from_utf8_lossy(&denied_bytes)
    );
    assert!(!handled.load(Ordering::SeqCst), "the handler must not run for an unauthorized caller");

    // (a)+(b): a Denied audit record with the unauthorized caller as principal is sealed in the
    // provider's own chain.
    let no_id = no_caller.node_id().to_string();
    assert!(
        poll_until(
            || provider.audit_stream(provider.node_id()).iter().any(|r| {
                r.record.action == AuditAction::Invoke
                    && r.record.outcome == AuditOutcome::Denied
                    && r.record.principal == no_id
                    && r.record.target == "guarded.echo"
            }),
            Duration::from_secs(20),
        )
        .await,
        "an Invoke/Denied record naming the unauthorized caller is sealed in the provider's chain"
    );

    // (c): the provider's audit chain still verifies end-to-end.
    provider
        .audit_verify(provider.node_id())
        .expect("the provider's audit chain verifies after sealing the denial");

    // The authorized caller is admitted: handler runs and echoes the payload back.
    let ok_reply = ok_caller
        .service()
        .rpc_call(provider.node_id().clone(), "guarded.echo", b"echo-me".to_vec(), Duration::from_secs(10))
        .await;
    let ok_bytes = ok_reply.expect("authorized call gets a reply");
    assert_eq!(&ok_bytes[..], b"echo-me", "the authorized caller's handler echoes the payload");
    assert!(handled.load(Ordering::SeqCst), "the handler ran for the authorized caller");

    for a in agents {
        a.shutdown_with_timeout(Duration::from_secs(5)).await;
    }
    let _ = std::fs::remove_dir_all(&dir);
}
