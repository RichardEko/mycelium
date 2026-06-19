//! Example 01 — **actor ↔ LLM via the mailbox**.
//!
//! Three depots in the Food-Rescue Co-op:
//!   • `kitchen-router` hosts a `routing/suggest` Prompt Skill (an `EchoBackend` stand-in, so no
//!     model / API key is needed) — the co-op's routing brain.
//!   • `depot-triage` is the **actor**: it opens a `triage.ask` mailbox, and for each donation it
//!     receives it consults the router skill and delivers the answer back to the asker.
//!   • `depot-intake` receives donations and asks triage to route each by **delivering an event to
//!     triage's mailbox** — no coupling beyond the target `NodeId` + a `kind` string.
//!
//! Triage drains its mailbox in **HLC-causal order**, calls the router (a genuine cross-node RPC),
//! and **delivers the answer back** to intake's `triage.reply` mailbox. Actor-style messaging —
//! addressed, ordered, durable within the gossip TTL window — built entirely on Layer I (KV) + HLC
//! ordering. No broker, no actor registry, no explicit lifecycle.
//!
//! Every depot also serves the AgentFacts lens on the gateway port printed at startup; while this
//! runs:  curl http://127.0.0.1:<printed-port>/.well-known/agent-facts.json
//!
//! Run:  cargo run -p mycelium-coop-examples --bin mailbox_llm

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use coop::common::{alloc_ports, spawn_depot, Donation, DepotOpts};
use mycelium::{CapFilter, EchoBackend, LlmBackend, PromptTemplate};

const N_DONATIONS: u64 = 3;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(tracing::Level::WARN).init();

    let cert_dir = std::env::temp_dir().join(format!("coop-mailbox-llm-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&cert_dir);

    // Six mutually-distinct OS-assigned ports (3 depots × gossip+http) so re-runs never collide.
    let p = alloc_ports(6);

    // ── kitchen-router: the LLM skill host (seed) ───────────────────────────────
    let router = spawn_depot(DepotOpts {
        name: "kitchen-router".into(),
        gossip_port: p[0], http_port: p[1],
        zone: "camden".into(),
        bootstrap: vec![],
        cert_dir: cert_dir.clone(),
    })
    .await?;
    println!("[{}] up — gossip :{} · facts http://127.0.0.1:{}/.well-known/agent-facts.json",
        router.name, router.gossip_port, router.http_port);

    // EchoBackend renders the template and echoes the input — deterministic, CI-safe, no model.
    let template = PromptTemplate {
        system: "You are the co-op's food-routing assistant.".into(),
        user_template: "Route this donation to the nearest community kitchen: {{input}}".into(),
        max_tokens: 128,
        temperature: 0.0,
        metadata: HashMap::new(),
    };
    let backend: Arc<dyn LlmBackend> = Arc::new(EchoBackend);
    let _skill = router.agent.llm()
        .register_prompt_skill("routing", "suggest", template, backend)
        .await?;
    println!("[{}] registered skill routing/suggest (EchoBackend)", router.name);

    // ── depot-triage: the mailbox actor ─────────────────────────────────────────
    let triage = spawn_depot(DepotOpts {
        name: "depot-triage".into(),
        gossip_port: p[2], http_port: p[3],
        zone: "islington".into(),
        bootstrap: vec![router.gossip_port],
        cert_dir: cert_dir.clone(),
    })
    .await?;
    println!("[{}] up — gossip :{}", triage.name, triage.gossip_port);

    // Triage drains its inbound mailbox: for each ask, consult the router and reply to the sender.
    let (_ask_mbox, mut ask_rx) = triage.agent.service().open_mailbox("triage.ask", 64);
    let triage_agent = Arc::clone(&triage.agent);
    let processor = tokio::spawn(async move {
        while let Some(ev) = ask_rx.recv().await {
            let Some(donation) = Donation::from_bytes(&ev.payload) else { continue };
            let advice = triage_agent.llm()
                .call_prompt_skill("routing", "suggest", &donation.summary(), HashMap::new(), Duration::from_secs(5))
                .await;
            let reply = match advice {
                Ok(out) => format!("[{}] {}", donation.id, out),
                Err(e)  => format!("[{}] triage error: {e}", donation.id),
            };
            triage_agent.service().deliver_event(&ev.sender, "triage.reply", reply.into_bytes());
        }
    });

    // ── depot-intake: receives donations, asks triage, collects replies ─────────
    let intake = spawn_depot(DepotOpts {
        name: "depot-intake".into(),
        gossip_port: p[4], http_port: p[5],
        zone: "hackney".into(),
        bootstrap: vec![router.gossip_port],
        cert_dir: cert_dir.clone(),
    })
    .await?;
    println!("[{}] up — gossip :{}", intake.name, intake.gossip_port);

    // Wait for the cluster to form structurally (CLAUDE.md convention — not a fixed sleep): the
    // routing skill must be visible to triage, and every node must have ≥1 peer so the skill RPC's
    // Individual-scoped frame can be delivered (or flood-relayed). Gating only on capability
    // visibility races the RPC ahead of peering ("Individual-scoped frame dropped: no peers").
    let triage_id = triage.node_id();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let skill_visible = !triage.agent.capabilities()
                .resolve(&CapFilter::new("routing", "suggest")).is_empty();
            let peered = !router.agent.peers().is_empty()
                && !triage.agent.peers().is_empty()
                && !intake.agent.peers().is_empty();
            if skill_visible && peered {
                return;
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    })
    .await
    .map_err(|_| "timed out: routing/suggest + peering never converged")?;
    println!("[{}] cluster peered; routing/suggest visible to triage", intake.name);

    // Open the reply mailbox BEFORE delivering, so replies are caught as they arrive.
    let (_reply_mbox, mut reply_rx) = intake.agent.service().open_mailbox("triage.reply", 64);

    // Deliver N donations to triage's mailbox — each an event addressed by NodeId + kind only.
    for id in 1..=N_DONATIONS {
        let donation = Donation::new(id, "borough-market", "12 crates mixed veg", "southwark");
        let queued = intake.agent.service().deliver_event(&triage_id, "triage.ask", donation.to_bytes());
        println!("[{}] → asked triage to route {}  (queued={queued})", intake.name, donation.summary());
    }

    // Collect the replies — at-least-once within the TTL window, delivered in HLC order.
    let mut replies = Vec::new();
    for _ in 0..N_DONATIONS {
        match tokio::time::timeout(Duration::from_secs(10), reply_rx.recv()).await {
            Ok(Some(ev)) => {
                let text = String::from_utf8_lossy(&ev.payload).to_string();
                println!("[{}] ← triage replied: {text}", intake.name);
                replies.push((ev.hlc_ts, text));
            }
            Ok(None) => break,
            Err(_)   => return Err("timed out waiting for a triage reply".into()),
        }
    }

    // ── Assertions ──────────────────────────────────────────────────────────────
    assert_eq!(replies.len() as u64, N_DONATIONS, "every donation got a routed reply");
    assert!(
        replies.iter().all(|(_, t)| t.contains("12 crates mixed veg")),
        "the skill output echoes the donation it routed"
    );
    let mut ordered = replies.clone();
    ordered.sort_by_key(|(ts, _)| *ts);
    assert_eq!(ordered, replies, "replies were delivered in causal (HLC) order");
    println!("\nAll assertions passed — {N_DONATIONS} donations routed via the mailbox, in order.");

    drop(_ask_mbox);
    processor.abort();
    intake.shutdown().await;
    triage.shutdown().await;
    router.shutdown().await;
    let _ = std::fs::remove_dir_all(&cert_dir);
    Ok(())
}
