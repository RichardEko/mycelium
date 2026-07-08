//! The guardrail **fleet**, made runnable — all three strength tiers *actually firing* in one
//! constructive-domain co-op, not merely declared.
//!
//! A neighbourhood **surplus-food-rescue / community-energy co-op** runs a small agent fleet across
//! two regions. Where the [`guardrail_wedge`](guardrail_wedge) example proves the hard-prevention
//! tier (Tier C) end-to-end, this one composes the whole policy surface and shows **each tier
//! observably fire**:
//!
//! - **Tier A — boundary drop (self-imposed structural prevention).** A `north` agent that has only
//!   joined `region-north` **structurally never acts** on a `region-south`-scoped dispatch — the
//!   signal is dropped at its admission boundary, before any handler, so its `signal_rx` never sees
//!   it; a `region-north` dispatch *is* admitted. Honest note (the chapter says this): Tier A is
//!   self-imposed — this demonstrates an *honest* node declining to act outside its region; a
//!   *malicious* node could ignore its own boundary. That is exactly why Tier C exists.
//! - **Tier B — denied tool blocked at the state transition (self-imposed, transition-level).** A
//!   `planner` agent whose policy denies `wire_transfer` is refused at its own
//!   `→ Invoking{tool:"wire_transfer"}` transition (`PolicyViolation::ToolDenied`), while an allowed
//!   tool (`match_surplus`) transitions fine.
//! - **Tier C — unauthorized caller rejected at the provider gate + sealed and proven (hard
//!   prevention).** A `settlement` provider guards `coop.settle` with `authorized_callers`; a
//!   `rogue` node is rejected at the gate and the denial is **sealed** into the provider's
//!   tamper-evident audit chain, while the `coordinator` is admitted. A neutral observer then
//!   reconstructs the chain and prints the cryptographic proof.
//!
//! Why one `recv` timeout is a legitimate assertion for the Tier-A *drop* (and not a fixed-sleep
//! smell): a boundary drop is a **non-event** — an admission-dropped signal never enters the node's
//! channel at all, so no amount of waiting can surface it. We first prove the flood path is *live*
//! (a `region-north` dispatch reaches `north`), then assert the `region-south` dispatch does **not**
//! arrive within a bounded, CI-generous window, then bracket it with a *second* `region-north`
//! dispatch that **does** arrive — so the missing signal is provably a drop, not slow gossip.
//!
//! tls is required because the Tier-C audit records are Ed25519-signed and `req.sender()` is only
//! trustworthy under the tls identity — so the sealed principal is the *verified* caller.
//!
//! Run: `cargo run -p mycelium-guardrails --features compliance --example guardrail_fleet`.
//! Exits 0 on success (prints `FLEET OK`); asserted by `ci_smoke.sh` on the printed markers.
#![allow(clippy::field_reassign_with_default)] // GossipConfig is built the way mycelium's own tests do

use std::sync::Arc;
use std::time::Duration;

use mycelium::{
    ExecutionState, GossipAgent, GossipConfig, NodeId, PolicyViolation, SignalScope, TlsConfig,
};
use mycelium_guardrails::{apply, guarded_rpc_serve, narrate_proof, prove_denials, Policy};

const DISPATCH: &str = "coop.dispatch";
const SETTLE: &str = "coop.settle";

/// A free TCP port (bind :0, read it, drop) — reused across a retrying mesh start, so the
/// drop-then-rebind TOCTOU window is covered by the whole-set retry.
fn free_port() -> u16 {
    mycelium::test_util::alloc_port()
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

/// Deterministic teardown: shut down every agent, then remove the shared cert dir. Called on
/// every exit path so a mid-run panic never leaks a live agent or a temp dir.
async fn teardown(agents: Vec<Arc<GossipAgent>>, dir: &std::path::Path) {
    for a in agents {
        a.shutdown_with_timeout(Duration::from_secs(5)).await;
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::main]
async fn main() {
    // ── The co-op fleet: coordinator, two regional agents, a planner, a settlement provider, and a
    //    rogue would-be caller — six tls nodes sharing one CA. ──
    let dir = std::env::temp_dir().join(format!("myc-guardrail-fleet-{}", free_port()));
    let _ = std::fs::remove_dir_all(&dir);
    let agents = start_mesh(6, &dir).await;
    let (coordinator, north, south, planner, settlement, rogue) = (
        Arc::clone(&agents[0]),
        Arc::clone(&agents[1]),
        Arc::clone(&agents[2]),
        Arc::clone(&agents[3]),
        Arc::clone(&agents[4]),
        Arc::clone(&agents[5]),
    );

    assert!(
        poll_until(|| agents.iter().all(|a| a.peers().len() >= 5), Duration::from_secs(45)).await,
        "the 6-node co-op mesh forms"
    );
    println!("mesh: 6 tls nodes formed (coordinator, north, south, planner, settlement, rogue)");

    // ══ Act 1 — Tier A: the boundary drops an out-of-region dispatch (structural, observable). ══
    //
    // `north` acts only within region-north; `south` only within region-south. A region-south
    // dispatch is structurally invisible to north (dropped at admission, before any handler), while
    // a region-north dispatch is admitted. Honest note: Tier A is *self-imposed* — this is an honest
    // north node declining to act outside its region; a malicious node could ignore its own
    // boundary (which is why the hard gate, Tier C, exists).
    let _north_applied = apply(Policy::new().act_within_groups(["region-north"]), &north).await;
    let _south_applied = apply(Policy::new().act_within_groups(["region-south"]), &south).await;

    // Subscribe on north BEFORE any emit, so the receiver exists when an admitted dispatch arrives.
    let mut north_rx = north.mesh().signal_rx(DISPATCH);

    // The coordinator must observe north's region-north membership before a Group-scoped dispatch
    // can be admission-routed to it — a structural precondition, polled (never a fixed sleep).
    assert!(
        poll_until(
            || coordinator.mesh().group_members("region-north").iter().any(|m| m == north.node_id()),
            Duration::from_secs(30),
        )
        .await,
        "the coordinator sees north's region-north membership"
    );

    // (positive #1) A region-north dispatch reaches north — this proves the flood path is LIVE, so
    // the subsequent negative (a bounded wait that receives nothing) is a genuine boundary drop and
    // not merely slow gossip.
    let _ = coordinator.mesh().emit(DISPATCH, SignalScope::Group("region-north".into()), b"north-surplus-1".to_vec());
    let first = tokio::time::timeout(Duration::from_secs(30), north_rx.recv()).await;
    assert!(
        matches!(&first, Ok(Some(sig)) if sig.payload.as_ref() == b"north-surplus-1"),
        "north admits the region-north dispatch (its own region): {first:?}"
    );

    // Drain anything else buffered so the negative window starts from a known-empty channel.
    while north_rx.try_recv().is_ok() {}

    // (negative) A region-south dispatch: north is NOT a member, so admission drops it at the
    // boundary — it never enters north's channel. A bounded, CI-generous wait that times out is a
    // legitimate assertion here precisely because the drop is a *non-event*: an admission-dropped
    // signal cannot be surfaced by waiting longer, only a delayed-but-admitted one could — and we
    // just proved (positive #1) that admitted region dispatches arrive fast.
    let _ = coordinator.mesh().emit(DISPATCH, SignalScope::Group("region-south".into()), b"south-surplus".to_vec());
    let dropped = tokio::time::timeout(Duration::from_secs(3), north_rx.recv()).await;
    assert!(
        dropped.is_err(),
        "north structurally IGNORES the region-south dispatch (boundary drop): unexpectedly got {dropped:?}"
    );

    // (positive #2, the bracket) A second region-north dispatch, emitted AFTER the region-south one,
    // DOES arrive — so the missing south dispatch is provably a boundary drop, not a channel that
    // simply went quiet. The channel was live the whole time; only the out-of-region signal was
    // dropped.
    let _ = coordinator.mesh().emit(DISPATCH, SignalScope::Group("region-north".into()), b"north-surplus-2".to_vec());
    let second = tokio::time::timeout(Duration::from_secs(30), north_rx.recv()).await;
    assert!(
        matches!(&second, Ok(Some(sig)) if sig.payload.as_ref() == b"north-surplus-2"),
        "north still admits region-north dispatches after the drop (bracket): {second:?}"
    );
    // And the dropped south dispatch never sneaks in behind it.
    assert!(
        north_rx.try_recv().is_err(),
        "the region-south dispatch never appears in north's channel — it was dropped, not delayed"
    );
    println!("✓ Tier A: region-north agent structurally ignored a region-south signal (boundary drop)");

    // ══ Act 2 — Tier B: a denied tool is blocked at the agent's own state transition. ══
    //
    // The planner's policy denies `wire_transfer` and budgets tool calls. The guard fires at the
    // agent's `→ Invoking{tool}` transition — self-imposed and transition-level (a side effect not
    // preceded by a policed transition would not be caught; the chapter states this honestly).
    let planner_applied = apply(
        Policy::new().deny_tools(["wire_transfer"]).tool_budget(5),
        &planner,
    )
    .await;
    let sm = planner_applied.state_machine();

    // A normal working step: Idle → Planning is fine.
    sm.transition(ExecutionState::Planning)
        .await
        .expect("planner may plan");

    // An allowed tool transitions fine — the guard is a deny-list, not a lock-down.
    sm.transition(ExecutionState::Invoking { tool: "match_surplus".into() })
        .await
        .expect("the allowed tool 'match_surplus' is invocable");

    // The denied tool is refused at the transition with ToolDenied — Tier B firing.
    let verdict = sm.transition(ExecutionState::Invoking { tool: "wire_transfer".into() }).await;
    assert!(
        matches!(&verdict, Err(PolicyViolation::ToolDenied(t)) if t == "wire_transfer"),
        "the denied tool 'wire_transfer' is blocked at the transition: {verdict:?}"
    );
    println!("✓ Tier B: agent blocked from the denied tool 'wire_transfer' at its state transition");

    // ══ Act 3 — Tier C: an unauthorized caller is rejected at the provider gate, sealed, proven. ══
    //
    // The settlement provider hard-gates `coop.settle` to the coordinator only. A rogue node is
    // rejected at the provider's own gate and the denial is sealed into its tamper-evident chain;
    // the coordinator is admitted. Then a neutral observer reconstructs the proof.
    let policy = Policy::new()
        .act_within_groups(["region-north", "region-south"]) // Tier A — settles for both regions.
        .deny_tools(["shell"]) // Tier B — a self-imposed transition guard.
        .authorized_callers([coordinator.node_id().to_string()]); // Tier C — the hard gate.
    let settle_applied = apply(policy.clone(), &settlement).await;

    // The legibility: print which clause compiles to which guarantee tier (all three on show).
    println!("settlement: policy strength report (which clause is which guarantee — the legibility):");
    for clause in policy.strength_report() {
        println!("  · {} [{}] — {}", clause.name, clause.tier.label(), clause.detail);
    }

    // The Tier-C gate in front of the settlement handler: authorized callers reach it; unauthorized
    // callers are dropped with a sealed `Invoke`/`Denied` and an error reply.
    let _guard = guarded_rpc_serve(&settle_applied, SETTLE, move |agent, req| async move {
        agent.service().rpc_respond(&req, b"settled".to_vec());
    });

    // The rogue node invokes → structurally stopped at the provider gate (error reply, not a hang).
    let rogue_reply = rogue
        .service()
        .rpc_call(settlement.node_id().clone(), SETTLE, b"pay-me".to_vec(), Duration::from_secs(10))
        .await
        .expect("the rogue call still gets a reply (an error), not a timeout");
    assert!(
        String::from_utf8_lossy(&rogue_reply).contains("unauthorized"),
        "the rogue caller is refused: {}",
        String::from_utf8_lossy(&rogue_reply)
    );

    // The coordinator invokes → admitted, handler runs.
    let coord_reply = coordinator
        .service()
        .rpc_call(settlement.node_id().clone(), SETTLE, b"settle-north".to_vec(), Duration::from_secs(10))
        .await
        .expect("the coordinator's authorized call gets a reply");
    assert_eq!(&coord_reply[..], b"settled", "the authorized coordinator's settlement handler runs");

    // The proof: a neutral observer (the planner — no special role) reconstructs the provider's
    // chain and proves the rogue denial. The audit chain gossips fleet-wide, so any node can prove
    // it — not only the provider.
    let rogue_id = rogue.node_id().to_string();
    assert!(
        poll_until(
            || {
                let p = prove_denials(&planner, settlement.node_id(), Some(&rogue_id));
                p.chain_verified && !p.denials.is_empty()
            },
            Duration::from_secs(30),
        )
        .await,
        "the observer sees the provider's verified chain with the sealed rogue denial"
    );
    let proof = prove_denials(&planner, settlement.node_id(), Some(&rogue_id));
    assert!(proof.chain_verified, "the provider's chain verifies");
    assert_eq!(proof.denials.len(), 1, "exactly one sealed rogue denial");
    assert_eq!(proof.denials[0].caller, rogue_id, "the sealed principal is the rogue caller");
    assert_eq!(proof.denials[0].target, SETTLE, "the sealed target is the guarded settlement kind");

    println!("proof (reconstructed by a neutral observer node — the chain is readable fleet-wide):");
    for line in narrate_proof(&proof) {
        println!("{line}");
    }

    // Negative control: the authorized coordinator was never denied — an empty proof, honestly.
    let coord_id = coordinator.node_id().to_string();
    let no_proof = prove_denials(&planner, settlement.node_id(), Some(&coord_id));
    assert!(no_proof.denials.is_empty(), "the authorized coordinator has no sealed denial");
    println!("✓ Tier C: unauthorized settlement invocation rejected + sealed; proof reconstructed");

    // ── Deterministic teardown, then the success marker. ──
    teardown(agents, &dir).await;
    println!("FLEET OK");
}
