//! The in-memory board store (WS-G / G3 · Phase 1) — the pure claim-by-predicate primitive.
//!
//! `BoardStore` is the content-plane analogue of `mycelium-tuple-space`'s `TupleStore`, but where
//! the tuple space routes by lane *position*, the board routes by *content*: a [`Predicate`] over
//! fact attributes. It embodies the **exactly-once-effect discipline** (single-owner claim,
//! idempotent terminal ack, bounded in-flight with crash-requeue) documented in
//! `docs/design/exactly-once-effect.md` — this crate is that contract's *second* real user.
//!
//! Unlike the tuple space's blocking `take`, `claim` is **non-blocking**: a competitive claim either
//! wins a matching fact now or returns `None` (the loser's empty claim). There are no parked
//! waiters — readiness is expressed by re-claiming, not by blocking.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;

use crate::{BlackboardError, Fact, Predicate};

/// An in-flight (claimed-but-not-terminal) fact and when it was claimed (for the deadline sweep).
struct Inflight {
    fact: Fact,
    claimed_at: Instant,
}

/// All mutable state under one lock — eliminates any TOCTOU between the predicate scan and the
/// claim, which is what makes a claim atomically single-owner.
struct BoardInner {
    /// Claimable facts, id-ordered so `claim` resolves ties to the oldest matching fact (FIFO-fair
    /// across the content predicate).
    available: BTreeMap<u64, Fact>,
    /// Claimed facts awaiting `ack` (terminal) or `release` / deadline-requeue (back to available).
    inflight: HashMap<u64, Inflight>,
}

/// Live depth snapshot for one board.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoardDepth {
    /// Facts currently claimable.
    pub available: u64,
    /// Facts currently claimed and awaiting a terminal ack.
    pub inflight: u64,
}

/// Cumulative counters for one board (the `sys/bb/{node}/{board}/…` posture).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoardStats {
    pub posted: u64,
    pub claimed: u64,
    pub acked: u64,
    pub released: u64,
    pub requeued: u64,
}

/// The pure, in-memory board: typed facts with non-destructive [`read`](Self::read) and competitive
/// destructive [`claim`](Self::claim). Cheap to construct and exercise without a cluster.
pub struct BoardStore {
    inner: Mutex<BoardInner>,
    next_id: AtomicU64,
    posted: AtomicU64,
    claimed: AtomicU64,
    acked: AtomicU64,
    released: AtomicU64,
    requeued: AtomicU64,
}

impl Default for BoardStore {
    fn default() -> Self {
        Self::new()
    }
}

impl BoardStore {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(BoardInner {
                available: BTreeMap::new(),
                inflight: HashMap::new(),
            }),
            next_id: AtomicU64::new(0),
            posted: AtomicU64::new(0),
            claimed: AtomicU64::new(0),
            acked: AtomicU64::new(0),
            released: AtomicU64::new(0),
            requeued: AtomicU64::new(0),
        }
    }

    /// Post a fact (Linda `out`). **Non-destructive** — it joins the claimable pool and, in the
    /// agent-backed board (later phases), gossips to every reader. Returns the fact id.
    pub fn post(&self, attributes: BTreeMap<String, String>, payload: Bytes) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.inner
            .lock()
            .available
            .insert(id, Fact { id, attributes, payload });
        self.posted.fetch_add(1, Ordering::Relaxed);
        id
    }

    /// Apply a fact under its ORIGINAL id (replication / WAL replay — later phases), fencing
    /// `next_id` past it so a promoted secondary never re-issues a live id.
    pub fn post_with_id(&self, id: u64, attributes: BTreeMap<String, String>, payload: Bytes) {
        self.next_id.fetch_max(id + 1, Ordering::Relaxed);
        self.inner
            .lock()
            .available
            .insert(id, Fact { id, attributes, payload });
        self.posted.fetch_add(1, Ordering::Relaxed);
    }

    /// **Non-destructive read** (Linda `rd`): every currently-claimable fact matching `predicate`.
    /// Concurrent and shared — many readers see the same fact. A claimed (in-flight) fact is *not*
    /// returned: it is being consumed by its owner, and reappears only if released or re-queued.
    pub fn read(&self, predicate: &Predicate) -> Vec<Fact> {
        self.inner
            .lock()
            .available
            .values()
            .filter(|f| predicate.matches(&f.attributes))
            .cloned()
            .collect()
    }

    /// **Competitive destructive claim** (Linda `in`): atomically move the oldest claimable fact
    /// matching `predicate` into in-flight and return it; `None` if none match. The whole operation
    /// holds one lock, so two racing claims can never both win the same fact — exactly one gets it,
    /// the other sees it already gone. The returned [`Fact::id`] is the claim handle for
    /// [`ack`](Self::ack) / [`release`](Self::release).
    pub fn claim(&self, predicate: &Predicate) -> Option<Fact> {
        let mut g = self.inner.lock();
        let id = g
            .available
            .iter()
            .find(|(_, f)| predicate.matches(&f.attributes))
            .map(|(id, _)| *id)?;
        let fact = g.available.remove(&id).expect("just found");
        g.inflight.insert(id, Inflight { fact: fact.clone(), claimed_at: Instant::now() });
        drop(g);
        self.claimed.fetch_add(1, Ordering::Relaxed);
        Some(fact)
    }

    /// **Terminal ack**: the claimed fact was consumed — it is gone for good. The exactly-once dedup
    /// point: a duplicate ack is a `NotFound`, never a second effect.
    pub fn ack(&self, id: u64) -> Result<(), BlackboardError> {
        match self.inner.lock().inflight.remove(&id) {
            Some(_) => {
                self.acked.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            None => Err(BlackboardError::NotFound),
        }
    }

    /// **Release**: abandon a claim — return the fact to the claimable pool (the loser/abort path,
    /// e.g. an executor that decided not to act). Re-readable and re-claimable afterwards.
    pub fn release(&self, id: u64) -> Result<(), BlackboardError> {
        let mut g = self.inner.lock();
        match g.inflight.remove(&id) {
            Some(Inflight { fact, .. }) => {
                g.available.insert(id, fact);
                drop(g);
                self.released.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            None => Err(BlackboardError::NotFound),
        }
    }

    /// **Crash-requeue** (at-least-once): in-flight claims older than `timeout` return to the
    /// claimable pool, so a claimer that dropped mid-work does not strand the fact. Returns the
    /// re-queued ids. (Phase 2 drives this on a cadence with the WAL; the mechanism lives here.)
    pub fn requeue_expired(&self, timeout: Duration) -> Vec<u64> {
        let mut g = self.inner.lock();
        let expired: Vec<u64> = g
            .inflight
            .iter()
            .filter(|(_, i)| i.claimed_at.elapsed() >= timeout)
            .map(|(id, _)| *id)
            .collect();
        for id in &expired {
            if let Some(Inflight { fact, .. }) = g.inflight.remove(id) {
                g.available.insert(*id, fact);
            }
        }
        if !expired.is_empty() {
            self.requeued.fetch_add(expired.len() as u64, Ordering::Relaxed);
        }
        expired
    }

    /// Live depth (claimable + in-flight).
    pub fn depth(&self) -> BoardDepth {
        let g = self.inner.lock();
        BoardDepth {
            available: g.available.len() as u64,
            inflight: g.inflight.len() as u64,
        }
    }

    /// Cumulative counters.
    pub fn stats(&self) -> BoardStats {
        BoardStats {
            posted: self.posted.load(Ordering::Relaxed),
            claimed: self.claimed.load(Ordering::Relaxed),
            acked: self.acked.load(Ordering::Relaxed),
            released: self.released.load(Ordering::Relaxed),
            requeued: self.requeued.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn surplus(feeder: &str, kwh: &str) -> BTreeMap<String, String> {
        BTreeMap::from([
            ("kind".to_string(), "surplus".to_string()),
            ("feeder".to_string(), feeder.to_string()),
            ("kwh".to_string(), kwh.to_string()),
        ])
    }

    #[test]
    fn predicate_equality_and_presence() {
        let f = surplus("4", "3.2");
        assert!(Predicate::new().eq("kind", "surplus").matches(&f));
        assert!(Predicate::new().eq("feeder", "4").present("kwh").matches(&f));
        assert!(!Predicate::new().eq("feeder", "9").matches(&f), "wrong value");
        assert!(!Predicate::new().present("price").matches(&f), "absent attr");
        assert!(Predicate::new().matches(&f), "empty predicate matches all");
    }

    // ── G-G3.1: competitive exactly-once claim ───────────────────────────────

    #[test]
    fn read_is_non_destructive_claim_is_destructive() {
        let store = BoardStore::new();
        store.post(surplus("4", "3.2"), Bytes::from("payload"));
        let pred = Predicate::new().eq("kind", "surplus");

        // Many non-destructive reads all see the fact.
        assert_eq!(store.read(&pred).len(), 1);
        assert_eq!(store.read(&pred).len(), 1, "read does not consume");
        assert_eq!(store.depth().available, 1);

        // A claim removes it from the claimable pool (and from read).
        let claimed = store.claim(&pred).expect("one matching fact");
        assert_eq!(claimed.payload.as_ref(), b"payload");
        assert_eq!(store.read(&pred).len(), 0, "claimed fact is no longer readable");
        assert_eq!(store.depth(), BoardDepth { available: 0, inflight: 1 });
    }

    #[test]
    fn two_claims_over_one_finite_fact_exactly_one_wins() {
        let store = BoardStore::new();
        store.post(surplus("4", "3.2"), Bytes::from("the surplus"));
        let pred = Predicate::new().eq("kind", "surplus");

        // Two executors race for the single finite fact (sequential — the lock serialises them).
        let a = store.claim(&pred);
        let b = store.claim(&pred);
        assert!(a.is_some() ^ b.is_some(), "exactly one claim wins; the loser gets None");
        assert!(b.is_none(), "the second claimer sees the fact already gone");
        assert_eq!(store.stats().claimed, 1);
    }

    #[test]
    fn concurrent_claimers_never_double_claim() {
        // The same property under real contention: N threads claim against one fact.
        let store = Arc::new(BoardStore::new());
        store.post(surplus("4", "3.2"), Bytes::from("x"));
        let pred = Predicate::new().eq("kind", "surplus");

        let wins = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut handles = Vec::new();
        for _ in 0..16 {
            let (s, p, w) = (Arc::clone(&store), pred.clone(), Arc::clone(&wins));
            handles.push(std::thread::spawn(move || {
                if s.claim(&p).is_some() {
                    w.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(wins.load(Ordering::Relaxed), 1, "exactly one of 16 racers claims the single fact");
        assert_eq!(store.depth().inflight, 1);
    }

    #[test]
    fn release_returns_fact_to_claimable() {
        let store = BoardStore::new();
        let id = store.post(surplus("4", "3.2"), Bytes::from("x"));
        let pred = Predicate::new().eq("kind", "surplus");

        let claimed = store.claim(&pred).unwrap();
        assert_eq!(claimed.id, id);
        assert_eq!(store.read(&pred).len(), 0);

        store.release(claimed.id).unwrap();
        assert_eq!(store.read(&pred).len(), 1, "released fact is claimable again");
        assert!(store.claim(&pred).is_some(), "and re-claimable");
        assert_eq!(store.stats().released, 1);
    }

    #[test]
    fn ack_is_terminal_and_idempotent() {
        let store = BoardStore::new();
        store.post(surplus("4", "3.2"), Bytes::from("x"));
        let pred = Predicate::new().eq("kind", "surplus");

        let claimed = store.claim(&pred).unwrap();
        store.ack(claimed.id).unwrap();
        assert_eq!(store.depth(), BoardDepth { available: 0, inflight: 0 }, "acked fact is gone");
        // Duplicate ack is a no-op error, never a second effect (the dedup point).
        assert_eq!(store.ack(claimed.id), Err(BlackboardError::NotFound));
        assert_eq!(store.read(&pred).len(), 0, "acked fact does not return");
    }

    #[test]
    fn requeue_expired_returns_inflight_claims() {
        let store = BoardStore::new();
        store.post(surplus("4", "3.2"), Bytes::from("x"));
        let pred = Predicate::new().eq("kind", "surplus");

        let claimed = store.claim(&pred).unwrap();
        // Nothing expired yet at a long timeout.
        assert!(store.requeue_expired(Duration::from_secs(3600)).is_empty());
        // A zero timeout treats the live claim as expired → re-queued.
        let requeued = store.requeue_expired(Duration::from_millis(0));
        assert_eq!(requeued, vec![claimed.id]);
        assert_eq!(store.read(&pred).len(), 1, "the abandoned claim is claimable again");
        assert_eq!(store.stats().requeued, 1);
    }

    #[test]
    fn claim_resolves_to_oldest_matching_fact() {
        let store = BoardStore::new();
        let first = store.post(surplus("4", "1.0"), Bytes::from("a"));
        store.post(surplus("4", "2.0"), Bytes::from("b"));
        // Of two matching facts, the oldest (lowest id) is claimed first (FIFO-fair).
        let got = store.claim(&Predicate::new().eq("feeder", "4")).unwrap();
        assert_eq!(got.id, first);
    }
}
