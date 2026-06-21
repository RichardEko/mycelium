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
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;

use crate::wal::{WalRecord, WalWriter};
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
    /// `Some` for a persistent (WAL-backed) board; `None` for a transient one.
    wal: Option<WalWriter>,
}

impl Default for BoardStore {
    fn default() -> Self {
        Self::transient()
    }
}

impl BoardStore {
    /// A transient (in-memory, no WAL) board. Cheap; for tests and non-durable boards.
    pub fn transient() -> Self {
        Self::with_wal(None)
    }

    /// A WAL-backed board: replays `path` (creating it if absent), recovering all live (claimable)
    /// facts and fencing `next_id`. `checkpoint_every` appends between `fdatasync`s.
    pub fn persistent(path: impl AsRef<Path>, checkpoint_every: u64) -> Result<Self, BlackboardError> {
        let (wal, live, max_id) = WalWriter::open(path.as_ref(), checkpoint_every)?;
        let store = Self::with_wal(Some(wal));
        store.next_id.store(max_id.map_or(0, |m| m + 1), Ordering::Relaxed);
        {
            let mut g = store.inner.lock();
            for (id, attributes, payload) in live {
                g.available.insert(id, Fact { id, attributes, payload });
                store.posted.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(store)
    }

    fn with_wal(wal: Option<WalWriter>) -> Self {
        Self {
            inner: Mutex::new(BoardInner { available: BTreeMap::new(), inflight: HashMap::new() }),
            next_id: AtomicU64::new(0),
            posted: AtomicU64::new(0),
            claimed: AtomicU64::new(0),
            acked: AtomicU64::new(0),
            released: AtomicU64::new(0),
            requeued: AtomicU64::new(0),
            wal,
        }
    }

    /// Post a fact (Linda `out`). **Non-destructive** — it joins the claimable pool and, in the
    /// agent-backed board (later phases), gossips to every reader. Returns the fact id.
    pub fn post(&self, attributes: BTreeMap<String, String>, payload: Bytes) -> Result<u64, BlackboardError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if let Some(wal) = &self.wal {
            wal.append(&WalRecord::Post { id, attributes: attributes.clone(), payload: payload.clone() })?;
        }
        self.inner.lock().available.insert(id, Fact { id, attributes, payload });
        self.posted.fetch_add(1, Ordering::Relaxed);
        Ok(id)
    }

    /// Apply a fact under its ORIGINAL id (replication / WAL replay — later phases), fencing
    /// `next_id` past it so a promoted secondary never re-issues a live id.
    pub fn post_with_id(&self, id: u64, attributes: BTreeMap<String, String>, payload: Bytes) -> Result<(), BlackboardError> {
        self.next_id.fetch_max(id + 1, Ordering::Relaxed);
        if let Some(wal) = &self.wal {
            wal.append(&WalRecord::Post { id, attributes: attributes.clone(), payload: payload.clone() })?;
        }
        self.inner.lock().available.insert(id, Fact { id, attributes, payload });
        self.posted.fetch_add(1, Ordering::Relaxed);
        Ok(())
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
    pub fn claim(&self, predicate: &Predicate) -> Result<Option<Fact>, BlackboardError> {
        let mut g = self.inner.lock();
        let Some(id) = g
            .available
            .iter()
            .find(|(_, f)| predicate.matches(&f.attributes))
            .map(|(id, _)| *id)
        else {
            return Ok(None);
        };
        let fact = g.available.remove(&id).expect("just found");
        g.inflight.insert(id, Inflight { fact: fact.clone(), claimed_at: Instant::now() });
        drop(g); // release before WAL I/O
        if let Some(wal) = &self.wal {
            wal.append(&WalRecord::Claim { id })?;
        }
        self.claimed.fetch_add(1, Ordering::Relaxed);
        Ok(Some(fact))
    }

    /// **Terminal ack**: the claimed fact was consumed — it is gone for good. The exactly-once dedup
    /// point: a duplicate ack is a `NotFound`, never a second effect.
    pub fn ack(&self, id: u64) -> Result<(), BlackboardError> {
        let removed = self.inner.lock().inflight.remove(&id).is_some();
        if !removed {
            return Err(BlackboardError::NotFound);
        }
        if let Some(wal) = &self.wal {
            wal.append(&WalRecord::Ack { id })?;
        }
        self.acked.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// **Release**: abandon a claim — return the fact to the claimable pool (the loser/abort path,
    /// e.g. an executor that decided not to act). Re-readable and re-claimable afterwards.
    pub fn release(&self, id: u64) -> Result<(), BlackboardError> {
        {
            let mut g = self.inner.lock();
            match g.inflight.remove(&id) {
                Some(Inflight { fact, .. }) => { g.available.insert(id, fact); }
                None => return Err(BlackboardError::NotFound),
            }
        }
        if let Some(wal) = &self.wal {
            wal.append(&WalRecord::Release { id })?;
        }
        self.released.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// True once the WAL has accumulated enough acked records to be worth compacting.
    pub fn wants_compaction(&self) -> bool {
        self.wal.as_ref().is_some_and(WalWriter::wants_compaction)
    }

    /// Rewrite the WAL to hold only live (claimable + in-flight) facts. Both replay as claimable, so
    /// each is written as a `Post` record. No-op on a transient board.
    pub fn compact(&self) -> Result<(), BlackboardError> {
        let Some(wal) = &self.wal else { return Ok(()) };
        let live: Vec<WalRecord> = {
            let g = self.inner.lock();
            g.available
                .values()
                .map(|f| (f.id, &f.attributes, &f.payload))
                .chain(g.inflight.values().map(|i| (i.fact.id, &i.fact.attributes, &i.fact.payload)))
                .map(|(id, attrs, payload)| WalRecord::Post {
                    id,
                    attributes: attrs.clone(),
                    payload: payload.clone(),
                })
                .collect()
        };
        wal.compact(&live)?;
        Ok(())
    }

    /// The WAL compaction epoch (0 for a transient board) — for tests / future replay cursors.
    pub fn wal_epoch(&self) -> u64 {
        self.wal.as_ref().map_or(0, WalWriter::epoch)
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

    fn claim_one(store: &BoardStore, pred: &Predicate) -> Fact {
        store.claim(pred).unwrap().expect("a matching fact")
    }

    fn temp_wal(name: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!("mbb-wal-{}-{}.log", std::process::id(), name));
        let _ = std::fs::remove_file(&p);
        p
    }

    // ── G-G3.1: competitive exactly-once claim ───────────────────────────────

    #[test]
    fn read_is_non_destructive_claim_is_destructive() {
        let store = BoardStore::transient();
        store.post(surplus("4", "3.2"), Bytes::from("payload")).unwrap();
        let pred = Predicate::new().eq("kind", "surplus");

        // Many non-destructive reads all see the fact.
        assert_eq!(store.read(&pred).len(), 1);
        assert_eq!(store.read(&pred).len(), 1, "read does not consume");
        assert_eq!(store.depth().available, 1);

        // A claim removes it from the claimable pool (and from read).
        let claimed = claim_one(&store, &pred);
        assert_eq!(claimed.payload.as_ref(), b"payload");
        assert_eq!(store.read(&pred).len(), 0, "claimed fact is no longer readable");
        assert_eq!(store.depth(), BoardDepth { available: 0, inflight: 1 });
    }

    #[test]
    fn two_claims_over_one_finite_fact_exactly_one_wins() {
        let store = BoardStore::transient();
        store.post(surplus("4", "3.2"), Bytes::from("the surplus")).unwrap();
        let pred = Predicate::new().eq("kind", "surplus");

        // Two executors race for the single finite fact (sequential — the lock serialises them).
        let a = store.claim(&pred).unwrap();
        let b = store.claim(&pred).unwrap();
        assert!(a.is_some() ^ b.is_some(), "exactly one claim wins; the loser gets None");
        assert!(b.is_none(), "the second claimer sees the fact already gone");
        assert_eq!(store.stats().claimed, 1);
    }

    #[test]
    fn concurrent_claimers_never_double_claim() {
        // The same property under real contention: N threads claim against one fact.
        let store = Arc::new(BoardStore::transient());
        store.post(surplus("4", "3.2"), Bytes::from("x")).unwrap();
        let pred = Predicate::new().eq("kind", "surplus");

        let wins = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut handles = Vec::new();
        for _ in 0..16 {
            let (s, p, w) = (Arc::clone(&store), pred.clone(), Arc::clone(&wins));
            handles.push(std::thread::spawn(move || {
                if s.claim(&p).unwrap().is_some() {
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
        let store = BoardStore::transient();
        let id = store.post(surplus("4", "3.2"), Bytes::from("x")).unwrap();
        let pred = Predicate::new().eq("kind", "surplus");

        let claimed = claim_one(&store, &pred);
        assert_eq!(claimed.id, id);
        assert_eq!(store.read(&pred).len(), 0);

        store.release(claimed.id).unwrap();
        assert_eq!(store.read(&pred).len(), 1, "released fact is claimable again");
        assert!(store.claim(&pred).unwrap().is_some(), "and re-claimable");
        assert_eq!(store.stats().released, 1);
    }

    #[test]
    fn ack_is_terminal_and_idempotent() {
        let store = BoardStore::transient();
        store.post(surplus("4", "3.2"), Bytes::from("x")).unwrap();
        let pred = Predicate::new().eq("kind", "surplus");

        let claimed = claim_one(&store, &pred);
        store.ack(claimed.id).unwrap();
        assert_eq!(store.depth(), BoardDepth { available: 0, inflight: 0 }, "acked fact is gone");
        // Duplicate ack is a no-op error, never a second effect (the dedup point).
        assert!(matches!(store.ack(claimed.id), Err(BlackboardError::NotFound)));
        assert_eq!(store.read(&pred).len(), 0, "acked fact does not return");
    }

    #[test]
    fn requeue_expired_returns_inflight_claims() {
        let store = BoardStore::transient();
        store.post(surplus("4", "3.2"), Bytes::from("x")).unwrap();
        let pred = Predicate::new().eq("kind", "surplus");

        let claimed = claim_one(&store, &pred);
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
        let store = BoardStore::transient();
        let first = store.post(surplus("4", "1.0"), Bytes::from("a")).unwrap();
        store.post(surplus("4", "2.0"), Bytes::from("b")).unwrap();
        // Of two matching facts, the oldest (lowest id) is claimed first (FIFO-fair).
        let got = claim_one(&store, &Predicate::new().eq("feeder", "4"));
        assert_eq!(got.id, first);
    }

    // ── G-G3.2: WAL durability (Phase 2) ─────────────────────────────────────

    #[test]
    fn posted_facts_survive_wal_replay() {
        let path = temp_wal("post-replay");
        let pred = Predicate::new().eq("kind", "surplus");
        {
            let store = BoardStore::persistent(&path, 1).unwrap();
            store.post(surplus("4", "1.0"), Bytes::from("a")).unwrap();
            store.post(surplus("4", "2.0"), Bytes::from("b")).unwrap();
        }
        // Reopen — both facts replay as claimable, ids fenced.
        let store = BoardStore::persistent(&path, 1).unwrap();
        assert_eq!(store.read(&pred).len(), 2);
        let fresh = store.post(surplus("4", "3.0"), Bytes::from("c")).unwrap();
        assert!(fresh >= 2, "next_id fenced past replayed ids");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn claimed_but_unacked_fact_requeues_on_replay() {
        // The worked-example "winner drops mid-charge" path: a claim with no ack is at-least-once —
        // on restart the fact returns to claimable so another executor finishes the work.
        let path = temp_wal("claim-crash");
        let pred = Predicate::new().eq("kind", "surplus");
        {
            let store = BoardStore::persistent(&path, 1).unwrap();
            store.post(surplus("4", "3.2"), Bytes::from("finite surplus")).unwrap();
            let _claimed = claim_one(&store, &pred); // claimed, never acked → "crash"
            assert_eq!(store.read(&pred).len(), 0, "claimed in this run");
        }
        let store = BoardStore::persistent(&path, 1).unwrap();
        let recovered = store.read(&pred);
        assert_eq!(recovered.len(), 1, "the unacked claim re-queues as claimable");
        assert_eq!(recovered[0].payload.as_ref(), b"finite surplus");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn acked_fact_does_not_resurrect_on_replay() {
        let path = temp_wal("ack-replay");
        let pred = Predicate::new().eq("kind", "surplus");
        {
            let store = BoardStore::persistent(&path, 1).unwrap();
            store.post(surplus("4", "3.2"), Bytes::from("x")).unwrap();
            let claimed = claim_one(&store, &pred);
            store.ack(claimed.id).unwrap();
        }
        let store = BoardStore::persistent(&path, 1).unwrap();
        assert_eq!(store.read(&pred).len(), 0, "an acked fact is gone for good across replay");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn compaction_preserves_live_facts_and_drops_acked() {
        let path = temp_wal("compact");
        let pred = Predicate::new().eq("kind", "surplus");
        let store = BoardStore::persistent(&path, 1000).unwrap();
        // Post 4, ack 2 (claim+ack), leave 2 claimable.
        for i in 0..4 {
            store.post(surplus("4", &i.to_string()), Bytes::from(format!("p{i}"))).unwrap();
        }
        for _ in 0..2 {
            let c = claim_one(&store, &pred);
            store.ack(c.id).unwrap();
        }
        let epoch_before = store.wal_epoch();
        store.compact().unwrap();
        assert_eq!(store.wal_epoch(), epoch_before + 1, "compaction bumps the epoch");
        drop(store);

        let store = BoardStore::persistent(&path, 1000).unwrap();
        assert_eq!(store.read(&pred).len(), 2, "the 2 live facts survive compaction; the 2 acked are gone");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn refuses_a_newer_wal_format() {
        // A future-version header must be refused, not silently truncated.
        let path = temp_wal("future-version");
        let mut bytes = b"MBBWAL".to_vec();
        bytes.extend_from_slice(&999u16.to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();
        assert!(BoardStore::persistent(&path, 1).is_err());
        let _ = std::fs::remove_file(&path);
    }
}
