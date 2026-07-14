//! **Distributed lock** — mutual exclusion across a broker-less mesh, with a fencing token.
//!
//! Run it:
//! ```sh
//! cargo run --example distributed_lock
//! ```
//!
//! Three nodes contend for one lock, `warehouse/bay-3`, via `agent.consensus().locks()`. Exactly
//! one holds it at a time — the others *wait* (blocking `lock()`), then take their turn when it's
//! released. Each holder does a short "exclusive job" and stamps a shared resource with its
//! **fencing token** ([`LockGuard::token`]); the resource **rejects any token lower than the
//! highest it has seen**, so even a stale holder whose lease lapsed can't corrupt it.
//!
//! Why a fencing token and not just the lock? A leased lock can't promise you *still* hold it at
//! the instant you touch the resource (a pause can outlast your lease). The monotonic token, +
//! the resource rejecting stale ones, is what makes it actually safe — the discipline every
//! correct distributed lock needs (Kleppmann). See [module docs](mycelium::LockService) and guide
//! [chapter 04 · Consensus](../docs/guide/04-consensus.md#the-distributed-lock-service).
//!
//! **When NOT to use a lock:** to hand *work items* to workers, use a work queue
//! (`mycelium-tuple-space`), not one lock — a lock serialises, a queue distributes.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use mycelium::{GossipAgent, GossipConfig, NodeId};

/// A shared resource that only accepts *fenced* writes: it remembers the highest token it has
/// seen and refuses anything lower. This is the holder-side half of the lock's safety contract.
#[derive(Default)]
struct FencedResource {
    highest_token: AtomicU64,
    writes:        AtomicU64,
    rejected:      AtomicU64,
}

impl FencedResource {
    /// Returns `true` if the write was accepted (token ≥ highest seen), `false` if fenced out.
    fn write(&self, token: u64, who: &str) -> bool {
        // Monotonic guard: reject a token below the highest committed so far.
        let mut cur = self.highest_token.load(Ordering::Acquire);
        loop {
            if token < cur {
                self.rejected.fetch_add(1, Ordering::Relaxed);
                println!("    ✗ {who} write FENCED OUT (token {token} < {cur})");
                return false;
            }
            match self.highest_token.compare_exchange_weak(cur, token, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_)   => break,
                Err(observed) => cur = observed,
            }
        }
        self.writes.fetch_add(1, Ordering::Relaxed);
        println!("    ✓ {who} wrote under token {token}");
        true
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── Three nodes on loopback, peered into one mesh ────────────────────────────────────────
    let base = 7400;
    let ports = [base, base + 1, base + 2];
    let mut agents = Vec::new();
    for (i, &port) in ports.iter().enumerate() {
        let bootstrap: Vec<NodeId> = if i == 0 { vec![] }
            else { vec![NodeId::new("127.0.0.1", base)?] };
        let cfg = GossipConfig { bind_port: port, bootstrap_peers: bootstrap, cluster_name: Some(std::env::var("GOSSIP_CLUSTER_NAME").unwrap_or_else(|_| "lock-demo".to_string())), ..Default::default() };
        let agent = Arc::new(GossipAgent::new(NodeId::new("127.0.0.1", port)?, cfg));
        agent.start().await?;
        // Every node that should vote on the lock needs a consensus listener.
        std::mem::forget(agent.consensus().start_consensus_listener(Default::default()));
        agents.push(agent);
    }
    // Let the mesh peer up.
    for _ in 0..60 {
        if agents.iter().all(|a| a.peers().len() >= 2) { break; }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    println!("mesh formed: 3 nodes contending for lock `warehouse/bay-3`\n");

    let resource = Arc::new(FencedResource::default());

    // ── All three race for the same lock; the winner does exclusive work, the rest wait ──────
    let mut tasks = Vec::new();
    for (i, agent) in agents.iter().cloned().enumerate() {
        let who = format!("node-{i}");
        let resource = Arc::clone(&resource);
        tasks.push(tokio::spawn(async move {
            let locks = agent.consensus().locks();
            // Each node makes two attempts at the exclusive job.
            for round in 0..2 {
                // Block up to 30 s for the lock; hold it for at most 15 s.
                match locks.lock("warehouse/bay-3", Duration::from_secs(15), Duration::from_secs(30)).await {
                    Ok(guard) => {
                        println!("[{who}] acquired (round {round}, fencing token = {})", guard.token);
                        // Exclusive section: stamp the resource with our fencing token.
                        resource.write(guard.token, &who);
                        tokio::time::sleep(Duration::from_millis(300)).await; // simulate work
                        guard.release(); // explicit release (drop would also do it)
                        println!("[{who}] released\n");
                    }
                    Err(e) => println!("[{who}] gave up waiting: {e}"),
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }));
    }
    for t in tasks { let _ = t.await; }

    // Every legitimate holder wrote under a strictly higher token than the last (mutual exclusion
    // + HLC monotonicity), so none were wrongly rejected.
    println!("resource: {} writes accepted (all under increasing tokens), highest token = {}",
        resource.writes.load(Ordering::Relaxed),
        resource.highest_token.load(Ordering::Relaxed));

    // ── The fence in action: a straggler whose lease lapsed attempts a LATE write ────────────
    // This is the case a lock alone cannot prevent — a paused holder waking up after its lease
    // expired. Its token is now below the highest the resource has committed, so it is refused.
    println!("\na straggler (an old holder, paused past its lease) attempts a late write:");
    let stale_token = 1; // any token below the current highest
    let accepted = resource.write(stale_token, "straggler");
    assert!(!accepted, "the fence MUST reject a stale token");

    println!("\nmutual exclusion held (one holder at a time), and the fencing token refused the \
             stale late write — which is what makes the lock actually safe.");

    for a in agents { a.shutdown().await; }
    Ok(())
}
