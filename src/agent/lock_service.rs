//! # Distributed lock service
//!
//! An ergonomic layer over [`ConsensusHandle::distributed_lock`](super::ConsensusHandle::distributed_lock):
//! **blocking acquire** (wait out contention instead of failing immediately), a **scoped
//! critical section** ([`with_lock`](LockService::with_lock)), and the guidance to use them
//! correctly. Every method returns the same consensus-backed [`LockGuard`] — mutually exclusive
//! cluster-wide, leased, with a monotonic fencing token — so a `LockService` lock and a raw
//! `distributed_lock` on the same *name* contend against each other (they share the
//! `lock/{name}` consensus slot).
//!
//! ```no_run
//! # async fn ex(agent: std::sync::Arc<mycelium::GossipAgent>) -> Result<(), Box<dyn std::error::Error>> {
//! use std::time::Duration;
//! let locks = agent.consensus().locks();
//!
//! // Scoped critical section — acquire, run, release (the recommended form):
//! locks.with_lock("shard-7", Duration::from_secs(30), Duration::from_secs(10), |guard| async move {
//!     let _fence = guard.token; // stamp resource writes with this (see the fencing-token note)
//!     // … exclusive work; the lock is released when this returns (or on panic/error) …
//! }).await?;
//! # Ok(()) }
//! ```
//!
//! ## Which lock primitive? (read this before choosing)
//!
//! | You want… | Use | Why |
//! |---|---|---|
//! | **Exclusive access to a named resource**, willing to wait for it | [`LockService::lock`] / [`with_lock`](LockService::with_lock) | Blocks (bounded) until the lock is free; releases deterministically |
//! | **Best-effort try-lock** — take it now or move on | [`LockService::try_lock`] or [`distributed_lock`](super::ConsensusHandle::distributed_lock) | Immediate `Superseded` if held; no waiting |
//! | **Elect one leader / owner** for a group | [`elect_leader`](super::ConsensusHandle::elect_leader) | A lock whose *value* is the winner's id, read back by everyone |
//! | **Hand each *work item* to exactly one of many workers** | `mycelium-tuple-space` (`take`) | A lock serialises; a work queue *distributes*. Do **not** build a queue out of one lock |
//! | **One active consumer of an ordered log** (consumer group) | `KvHandle::subscribe_log_group` / the gateway | Leased single-active claim + private offset — the log-consumer pattern, not a mutex |
//! | **Agree a value under contention** (config, a decision) | [`consistent_set`](super::ConsensusHandle::consistent_set) | You want the agreed *value*, not exclusion |
//!
//! ## The leased-lock discipline (the one thing to get right)
//!
//! This is a **leased** lock: you hold it for `ttl`, then it **auto-expires** (the consensus
//! commit carries a lease). Expiry is the safety net — if a holder crashes or hangs, the lease
//! lapses and someone else can acquire, so the cluster never wedges on a dead holder. The
//! consequence, which you **must** design for (Kleppmann's fencing-token argument):
//!
//! > A leased lock cannot guarantee you *still* hold it at the moment you touch the resource — a
//! > GC pause or slow syscall can outlast your lease. **The fencing token is the real
//! > protection:** stamp every write to the protected resource with [`LockGuard::token`] (a
//! > per-name monotonically increasing token drawn from the **commit HLC** — not the consensus
//! > ballot, which can regress under gossip lag; see the #164 finding + the monotonicity test)
//! > and have the resource **reject a token lower than the highest it has seen.** Then a stale
//! > holder's late write is refused.
//!
//! Practically: pick `ttl` comfortably larger than your critical section, keep the section short,
//! and fence the resource. Coarse-grained by design — a consensus round per acquire (~1 s to let
//! the commit converge) — so this suits leader election, shard/config ownership, migrations; it
//! is **not** for high-rate fine-grained locking.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use super::{ConsensusHandle, ConsistencyError, LockGuard, TaskCtx};

/// Ergonomic distributed-lock service — see the [module docs](self) for when to use it.
///
/// Obtain one with [`ConsensusHandle::locks`](super::ConsensusHandle::locks)
/// (`agent.consensus().locks()`). Cheap to clone/hold; all state is the shared agent.
#[derive(Clone)]
pub struct LockService {
    pub(super) ctx: Arc<TaskCtx>,
}

impl LockService {
    fn consensus(&self) -> ConsensusHandle {
        ConsensusHandle { ctx: Arc::clone(&self.ctx) }
    }

    /// Try to acquire `name` **once**, immediately. Returns [`ConsistencyError::Superseded`] if
    /// another holder currently owns it (no waiting). Thin wrapper over
    /// [`distributed_lock`](super::ConsensusHandle::distributed_lock).
    pub async fn try_lock(
        &self,
        name: &str,
        ttl:  Duration,
    ) -> Result<LockGuard, ConsistencyError> {
        self.consensus().distributed_lock(name, ttl).await
    }

    /// Acquire `name`, **waiting up to `wait`** for a current holder to release or its lease to
    /// lapse. Retries with exponential backoff on contention; returns the guard as soon as this
    /// node wins, or [`ConsistencyError::Superseded`] if `wait` elapses while still contended.
    /// `wait == 0` is exactly [`try_lock`](Self::try_lock).
    ///
    /// `ttl` is the acquired lock's lease (see the [leased-lock discipline](self)); it is
    /// independent of `wait` (how long you are willing to queue for it).
    pub async fn lock(
        &self,
        name: &str,
        ttl:  Duration,
        wait: Duration,
    ) -> Result<LockGuard, ConsistencyError> {
        let deadline = tokio::time::Instant::now() + wait;
        // Each acquire already spends ~1 s converging; a short extra backoff on contention avoids
        // two waiters lock-stepping on the same slot without inflating latency.
        let mut backoff = Duration::from_millis(200);
        loop {
            match self.consensus().distributed_lock(name, ttl).await {
                Ok(guard) => return Ok(guard),
                // Held elsewhere — wait for it to free, up to the deadline.
                Err(ConsistencyError::Superseded) => {
                    let now = tokio::time::Instant::now();
                    if now >= deadline {
                        return Err(ConsistencyError::Superseded);
                    }
                    let nap = backoff.min(deadline.saturating_duration_since(now));
                    tokio::time::sleep(nap).await;
                    backoff = (backoff * 2).min(Duration::from_secs(2));
                }
                // Timeout / topology errors are not "someone holds it" — surface them.
                Err(other) => return Err(other),
            }
        }
    }

    /// Acquire `name` (blocking up to `wait`), run `f`, then release — the **recommended** form,
    /// because release is guaranteed on every exit path (return, `?`, panic unwinding drops the
    /// guard). Returns `f`'s output.
    ///
    /// Keep the closure's runtime **shorter than `ttl`** (or fence the resource with
    /// [`LockGuard::token`]) — the lock is leased and can lapse mid-section under a long pause;
    /// see the [leased-lock discipline](self).
    pub async fn with_lock<F, Fut, T>(
        &self,
        name: &str,
        ttl:  Duration,
        wait: Duration,
        f:    F,
    ) -> Result<T, ConsistencyError>
    where
        F:   FnOnce(&LockGuard) -> Fut,
        Fut: Future<Output = T>,
    {
        let guard = self.lock(name, ttl, wait).await?;
        let out = f(&guard).await;
        drop(guard); // explicit: release before returning
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use crate::{GossipAgent, GossipConfig, NodeId};
    use crate::consensus::ConsensusConfig;
    use std::sync::Arc;
    use std::time::Duration;

    fn alloc_port() -> u16 { crate::test_util::alloc_port() }

    async fn node(port: u16, peers: &[u16]) -> Arc<GossipAgent> {
        let cfg = GossipConfig {
            bind_address: "127.0.0.1".parse().unwrap(),
            bind_port: port,
            bootstrap_peers: peers.iter().map(|p| NodeId::new("127.0.0.1", *p).unwrap()).collect(),
            ..GossipConfig::default()
        };
        let a = Arc::new(GossipAgent::new(NodeId::new("127.0.0.1", port).unwrap(), cfg));
        a.start().await.unwrap();
        std::mem::forget(a.consensus().start_consensus_listener(ConsensusConfig::default()));
        a
    }

    /// `lock` waits out a current holder and acquires once it frees.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn lock_blocks_then_acquires_when_freed() {
        let p1 = alloc_port();
        let p2 = alloc_port();
        let a = node(p1, &[p2]).await;
        let b = node(p2, &[p1]).await;
        for _ in 0..40 {
            if !a.peers().is_empty() && !b.peers().is_empty() { break; }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // A holds "res".
        let held = a.consensus().locks().try_lock("res", Duration::from_secs(60)).await
            .expect("A acquires");

        // B blocks for it with a generous wait; release A shortly after.
        let bl = b.consensus().locks();
        let waiter = tokio::spawn(async move {
            bl.lock("res", Duration::from_secs(60), Duration::from_secs(20)).await
        });
        tokio::time::sleep(Duration::from_millis(500)).await;
        held.release();

        let g = waiter.await.unwrap();
        assert!(g.is_ok(), "B never acquired after A released: {:?}", g.err());
        std::mem::forget(g);
        a.shutdown().await;
        b.shutdown().await;
    }

    /// `with_lock` runs the section then releases — a later acquire succeeds.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn with_lock_releases_after_section() {
        let a = node(alloc_port(), &[]).await;
        let locks = a.consensus().locks();
        let ran = locks.with_lock("w", Duration::from_secs(60), Duration::from_secs(10), |g| {
            let token = g.token;
            async move { token }
        }).await.expect("with_lock");
        assert!(ran > 0, "closure did not observe a fencing token");
        // Released → re-acquire succeeds.
        let g = locks.try_lock("w", Duration::from_secs(60)).await;
        assert!(g.is_ok(), "lock not released after with_lock: {:?}", g.err());
        std::mem::forget(g);
        a.shutdown().await;
    }

    /// `lock` returns Superseded when the holder never releases within `wait`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn lock_times_out_while_held() {
        let a = node(alloc_port(), &[]).await;
        let locks = a.consensus().locks();
        let _held = locks.try_lock("t", Duration::from_secs(60)).await.expect("hold");
        // Same node contends (distinct guard); held throughout a short wait → Superseded.
        let r = locks.lock("t", Duration::from_secs(60), Duration::from_secs(3)).await;
        assert!(matches!(r, Err(crate::ConsistencyError::Superseded)),
            "expected Superseded on a held lock, got {r:?}");
        std::mem::forget(_held);
        a.shutdown().await;
    }

    /// The fencing-token contract: successive acquisitions of the same lock get **strictly
    /// increasing** tokens. (The consensus ballot regressed here under gossip lag — the #164
    /// example finding — which is why the token is the commit's HLC.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fencing_token_is_monotonic_across_acquisitions() {
        let a = node(alloc_port(), &[]).await;
        let locks = a.consensus().locks();
        let mut last = 0u64;
        for i in 0..4 {
            let g = locks.try_lock("mono", Duration::from_secs(60)).await
                .unwrap_or_else(|e| panic!("acquire {i} failed: {e:?}"));
            assert!(g.token > last, "token {} not > previous {} (acquire {i})", g.token, last);
            last = g.token;
            g.release();
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        a.shutdown().await;
    }
}
