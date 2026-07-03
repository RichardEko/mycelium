//! Cross-node control-plane test — curator election, the single-writer apply against a shared
//! (node-independent) store, and ring-failover. Two in-process agents share one `FsStore` directory.
#![allow(clippy::field_reassign_with_default)] // GossipConfig is built the way mycelium's own tests do
//!
//! Robustness note: election/failover are asserted with **structural polls on generous timeouts**,
//! never fixed sleeps — the capability-ring failure detector is timing-sensitive under CI load (a
//! sibling companion's election test flaked exactly this way), and the guaranteed outcome only needs
//! patience, not a tighter bound.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use mycelium::{GossipAgent, GossipConfig, NodeId};
use mycelium_wiki::{FsStore, LintKind, Wiki, WikiConfig, WikiRole};

/// A free TCP port (bind :0, read it, drop). Good enough for an in-process test cluster.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
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

async fn spawn(port: u16, boot: Vec<u16>, dir: &std::path::Path) -> (Arc<GossipAgent>, Arc<Wiki<FsStore>>) {
    let mut cfg = GossipConfig::default();
    cfg.bind_port = port;
    cfg.bootstrap_peers = boot.into_iter().map(|p| NodeId::new("127.0.0.1", p).unwrap()).collect();
    let agent = Arc::new(GossipAgent::new(NodeId::new("127.0.0.1", port).unwrap(), cfg));
    agent.start().await.unwrap();
    let store = Arc::new(FsStore::open(dir, "ops").unwrap());
    let wcfg = WikiConfig {
        group: "ops".into(),
        role: WikiRole::Auto,
        cap_refresh: Duration::from_millis(400),
        drain_interval: Duration::from_millis(150),
        lint_interval: Duration::from_millis(300),
    };
    let wiki = Wiki::new(Arc::clone(&agent), wcfg, store).await;
    (agent, wiki)
}

/// Read `page`'s first section body from a wiki, if present.
fn first_body(w: &Wiki<FsStore>, page: &str) -> Option<String> {
    w.read(page).ok().flatten().and_then(|p| p.sections.first().map(|s| s.body.clone()))
}

#[tokio::test]
async fn curator_elects_applies_and_fails_over_against_the_same_store() {
    let dir = tempfile::tempdir().unwrap();
    let (pa, pb) = (free_port(), free_port());
    let (agent_a, wiki_a) = spawn(pa, vec![pb], dir.path()).await;
    let (agent_b, wiki_b) = spawn(pb, vec![pa], dir.path()).await;

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
