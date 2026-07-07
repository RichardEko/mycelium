//! Cross-node control-plane test — curator election, the single-writer apply against a shared
//! (node-independent) store, and ring-failover. Two in-process agents share one `FsStore` directory.
#![allow(clippy::field_reassign_with_default)] // GossipConfig is built the way mycelium's own tests do
//!
//! Robustness note: election/failover are asserted with **structural polls on generous timeouts**,
//! never fixed sleeps — the capability-ring failure detector is timing-sensitive under CI load. The
//! election settles on a fixed window, so a lost gossip race could once leave two nodes self-elected
//! with no recovery (this flaked `curator_elects_…` once in CI). The curator **sentinel** now makes
//! that self-healing — "lowest id wins" applied continuously, not just at election — so convergence
//! to a single curator is guaranteed, not merely probable; `dual_curators_reconcile_to_a_single_writer`
//! is its canary.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use mycelium::{GossipAgent, GossipConfig, NodeId};
use mycelium_wiki::{FsStore, LintKind, Wiki, WikiConfig, WikiRole};

/// A free TCP port (bind :0, read it, drop). The drop opens a TOCTOU window against parallel
/// test binaries — which is why agents start via [`start_pair`]'s retry, never a bare unwrap
/// (an `AddrInUse` here flaked this file in CI, 2026-07-07).
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// Start one agent, `None` if the bind lost the port race.
async fn try_start(port: u16, boot: Vec<u16>) -> Option<Arc<GossipAgent>> {
    let mut cfg = GossipConfig::default();
    cfg.bind_port = port;
    cfg.bootstrap_peers = boot.into_iter().map(|p| NodeId::new("127.0.0.1", p).unwrap()).collect();
    let agent = Arc::new(GossipAgent::new(NodeId::new("127.0.0.1", port).unwrap(), cfg));
    agent.start().await.ok().map(|_| agent)
}

/// Start a mutually-bootstrapped agent pair, retrying the *whole pair* on fresh ports when a
/// bind loses the `free_port` race (mutual bootstrap means both ports must be fixed before
/// either agent starts, so a per-agent retry can't work). Same idiom as the wasm-host tests.
async fn start_pair() -> (Arc<GossipAgent>, Arc<GossipAgent>) {
    for _ in 0..16 {
        let (pa, pb) = (free_port(), free_port());
        let Some(a) = try_start(pa, vec![pb]).await else { continue };
        match try_start(pb, vec![pa]).await {
            Some(b) => return (a, b),
            None => a.shutdown_with_timeout(Duration::from_secs(5)).await,
        }
    }
    panic!("could not bind an agent pair after 16 attempts");
}

async fn poll_until(mut cond: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if cond() { return true; }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cond()
}

fn attrs(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

/// Build a wiki over a shared store dir on an already-started agent.
async fn wiki_on(agent: &Arc<GossipAgent>, dir: &std::path::Path, role: WikiRole) -> Arc<Wiki<FsStore>> {
    let store = Arc::new(FsStore::open(dir, "ops").unwrap());
    let wcfg = WikiConfig {
        group: "ops".into(),
        role,
        cap_refresh: Duration::from_millis(400),
        drain_interval: Duration::from_millis(150),
        lint_interval: Duration::from_millis(300),
    };
    Wiki::new(Arc::clone(agent), wcfg, store).await
}

/// Read `page`'s first section body from a wiki, if present.
fn first_body(w: &Wiki<FsStore>, page: &str) -> Option<String> {
    w.read(page).ok().flatten().and_then(|p| p.sections.first().map(|s| s.body.clone()))
}

/// Split-brain reconciliation (the root cause of the earlier `curator_elects_…` CI flake): the
/// election settles on a fixed window, so a lost gossip race can leave two nodes self-elected. Force
/// that state directly — two **forced** curators against one store — and assert the sentinel makes
/// the higher-id one step down, so exactly one curator survives and stays. Before the step-down guard
/// both stayed curator forever and this XOR never held.
#[tokio::test]
async fn dual_curators_reconcile_to_a_single_writer() {
    let dir = tempfile::tempdir().unwrap();
    let (agent_a, agent_b) = start_pair().await;
    let wiki_a = wiki_on(&agent_a, dir.path(), WikiRole::Curator).await;
    let wiki_b = wiki_on(&agent_b, dir.path(), WikiRole::Curator).await;

    assert!(poll_until(|| !agent_a.peers().is_empty() && !agent_b.peers().is_empty(), Duration::from_secs(10)).await,
        "mesh forms");
    // Both begin as curator (forced). The sentinel reconciles: lowest id wins, the other resigns.
    assert!(poll_until(|| wiki_a.is_curator() ^ wiki_b.is_curator(), Duration::from_secs(30)).await,
        "the split-brain reconciles to exactly one curator");
    // …and it stays reconciled: the resigned node must NOT re-elect while the winner advertises. If
    // any poll over the next ~1.5s observes a non-XOR state, this returns true and the assert fails.
    let regressed = poll_until(|| !(wiki_a.is_curator() ^ wiki_b.is_curator()), Duration::from_millis(1500)).await;
    assert!(!regressed, "exactly-one-curator holds stably after reconciliation");

    agent_a.shutdown_with_timeout(Duration::from_secs(5)).await;
    agent_b.shutdown_with_timeout(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn curator_elects_applies_and_fails_over_against_the_same_store() {
    let dir = tempfile::tempdir().unwrap();
    let (agent_a, agent_b) = start_pair().await;
    let wiki_a = wiki_on(&agent_a, dir.path(), WikiRole::Auto).await;
    let wiki_b = wiki_on(&agent_b, dir.path(), WikiRole::Auto).await;

    // The ring forms and exactly one node elects itself curator.
    assert!(poll_until(|| !agent_a.peers().is_empty() && !agent_b.peers().is_empty(), Duration::from_secs(10)).await,
        "mesh forms");
    assert!(poll_until(|| wiki_a.is_curator() ^ wiki_b.is_curator(), Duration::from_secs(30)).await,
        "exactly one curator elected");

    // Propose a section from node A; the curator (whoever it is) drains it and writes the shared
    // store — so BOTH nodes, reading the same store directly, observe it.
    let page = "incidents/cert-rotation";
    let sid = wiki_a.new_section_id(page);
    wiki_a.propose(page, sid, "Symptoms", "gateway 503s", attrs(&[("node", "e_rl_rk")]));
    assert!(poll_until(|| first_body(&wiki_b, page).as_deref() == Some("gateway 503s"), Duration::from_secs(30)).await,
        "the curator applied the proposal to the shared store; the other node reads it directly");

    // Kill the curator. The survivor's ring-watch promotes it (failover), and it resumes against the
    // SAME store — nothing transferred.
    let (survivor, dead_agent) = if wiki_a.is_curator() {
        (Arc::clone(&wiki_b), Arc::clone(&agent_a))
    } else {
        (Arc::clone(&wiki_a), Arc::clone(&agent_b))
    };
    dead_agent.shutdown_with_timeout(Duration::from_secs(5)).await;

    assert!(poll_until(|| survivor.is_curator(), Duration::from_secs(30)).await,
        "the survivor promotes to curator after the old one evaporates");

    // A new proposal is applied by the new curator, against the same store.
    let sid2 = survivor.new_section_id("incidents/cert-rotation");
    survivor.propose("incidents/cert-rotation", sid2, "Resolution", "rolled the cert back", attrs(&[]));
    assert!(poll_until(|| {
        survivor.read("incidents/cert-rotation").ok().flatten()
            .map(|p| p.sections.iter().any(|s| s.body == "rolled the cert back")).unwrap_or(false)
    }, Duration::from_secs(30)).await, "the promoted curator applies against the same store");

    // The curator's periodic lint loop runs the group-function health check over the shared store:
    // propose a section with a dead cross-link and the promoted curator surfaces it (advisory only).
    let sid3 = survivor.new_section_id("incidents/cert-rotation");
    survivor.propose("incidents/cert-rotation", sid3, "Refs", "see [[incidents/does-not-exist]]", attrs(&[]));
    assert!(poll_until(|| {
        survivor.last_lint().findings.iter().any(|f| f.kind == LintKind::DeadCrossLink)
    }, Duration::from_secs(30)).await, "the curator's lint loop reports the dead cross-link");

    // Cleanup — shutting down an already-dead agent is harmless.
    agent_a.shutdown_with_timeout(Duration::from_secs(5)).await;
    agent_b.shutdown_with_timeout(Duration::from_secs(5)).await;
}
