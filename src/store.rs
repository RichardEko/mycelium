use crate::framing::GossipUpdate;
use ahash::RandomState;
use bytes::Bytes;
use papaya::Operation;
use std::sync::{Arc, OnceLock};
use tokio::sync::watch;
use tracing::warn;

static KEY_POOL: OnceLock<papaya::HashMap<Arc<str>, Arc<str>>> = OnceLock::new();

fn key_pool() -> &'static papaya::HashMap<Arc<str>, Arc<str>> {
    KEY_POOL.get_or_init(papaya::HashMap::new)
}

/// Returns the current number of entries in the intern pool.
/// Zero if `intern_keys = false` and no key has ever been interned.
/// The pool grows with the number of **distinct keys ever observed** — including
/// keys that have since been tombstoned and GC'd from the store — and is never
/// trimmed. Monitor this in `SystemStats::intern_pool_size` on long-running
/// clusters with high key churn; disable interning with `GossipConfig::intern_keys
/// = false` if the pool grows without bound.
pub(crate) fn intern_pool_len() -> usize {
    KEY_POOL.get().map_or(0, |p| p.len())
}

/// Process-wide key interning pool. Returns a shared `Arc<str>` for a given
/// key string — after the first call for a key, all subsequent calls return
/// the same allocation. This eliminates one heap allocation per received
/// gossip message for workloads with a bounded key set.
///
/// `max_keys`: when > 0, new keys are not inserted once the pool reaches that size;
/// the caller receives its own `Arc<str>` clone instead. Keys already in the pool
/// at the time the cap is hit continue to be shared. Set `max_keys = 0` for no cap.
pub(crate) fn intern_key(key: Arc<str>, max_keys: usize) -> Arc<str> {
    let pool = key_pool();
    let guard = pool.pin();
    if let Some(existing) = guard.get(&*key) {
        return existing.clone();
    }
    // Pool cap: return without inserting when the limit is reached.
    if max_keys > 0 && pool.len() >= max_keys {
        return key;
    }
    // `slot` is taken inside the compute callback. papaya may retry the callback
    // on CAS contention; the second call must not unwrap an already-taken slot.
    let mut slot = Some(key.clone());
    match guard.compute(key.clone(), |existing| match existing {
        Some(_) => papaya::Operation::Abort(()),
        None => match slot.take() {
            Some(v) => papaya::Operation::Insert(v),
            None    => papaya::Operation::Abort(()),
        },
    }) {
        papaya::Compute::Inserted(_, v) => v.clone(),
        _ => guard.get(&*key).cloned().unwrap_or(key),
    }
}

/// A store entry with timestamp for LWW conflict resolution.
/// `data: None` = tombstone; kept until it ages out to prevent key resurrection.
#[derive(Clone, Debug)]
pub(crate) struct StoreEntry {
    pub(crate) data: Option<Bytes>,
    pub(crate) timestamp: u64,
}

/// Computes a stable XOR-hash of all live (key, timestamp) pairs in the store.
///
/// Uses a fixed-seed `AHasher` so the value is identical across processes for the
/// same set of entries. Tombstones are excluded: they are not part of the live data
/// set and GC'd at different times on different nodes, which would cause spurious
/// mismatches. Returns 0 only when the store is empty; 0 is the "no digest" sentinel
/// in `WireMessage::StateRequest` so an empty store still triggers a full snapshot.
/// In practice a non-empty store almost never XORs to 0 (probability < 1 in 2^64).
pub(crate) fn store_hash(store: &papaya::HashMap<Arc<str>, StoreEntry>) -> u64 {
    static SEED: OnceLock<RandomState> = OnceLock::new();
    let rs = SEED.get_or_init(|| RandomState::with_seeds(1, 2, 3, 4));
    let guard = store.pin();
    let mut combined: u64 = 0;
    for (k, v) in guard.iter() {
        if v.data.is_some() {
            combined ^= rs.hash_one(k.as_bytes()) ^ v.timestamp;
        }
    }
    combined
}

/// Applies `update` using last-write-wins. Returns `true` if the store changed.
/// Tombstones win on equal timestamps; plain data requires a strictly newer timestamp.
/// Uses papaya `compute` for a single atomic CAS — no separate read then write.
pub(crate) fn apply_to_store(store: &papaya::HashMap<Arc<str>, StoreEntry>, update: &GossipUpdate) -> bool {
    let ts          = update.timestamp;
    let is_tombstone = update.is_tombstone;
    // Clone value once outside the callback (O(1) Bytes refcount bump).
    // The callback may be retried by papaya on CAS contention; capturing `val`
    // outside avoids re-cloning from `update` on every retry iteration.
    let val = if is_tombstone { None } else { Some(update.value.clone()) };

    let guard = store.pin();
    let result = guard.compute(update.key.clone(), |existing| {
        match existing {
            None => Operation::Insert(StoreEntry { data: val.clone(), timestamp: ts }),
            Some((_, curr)) => {
                let wins = if is_tombstone { ts >= curr.timestamp } else { ts > curr.timestamp };
                if wins {
                    Operation::Insert(StoreEntry { data: val.clone(), timestamp: ts })
                } else {
                    Operation::Abort(())
                }
            }
        }
    });

    matches!(result, papaya::Compute::Inserted(..) | papaya::Compute::Updated { .. })
}

/// Applies `update` to the store, then notifies any subscriber watching that key.
///
/// When `max_store_entries > 0` and the store's current size meets or exceeds that
/// limit, live (non-tombstone) writes are silently dropped. Tombstone writes are
/// always accepted — they reduce the live count and must propagate.
pub(crate) fn apply_and_notify(
    store:             &papaya::HashMap<Arc<str>, StoreEntry>,
    subs:              &papaya::HashMap<Arc<str>, watch::Sender<Option<Bytes>>>,
    update:            &GossipUpdate,
    max_store_entries: usize,
) {
    if max_store_entries > 0 && !update.is_tombstone && store.len() >= max_store_entries {
        warn!(
            key = %update.key,
            cap = max_store_entries,
            "KV store cap reached; live write dropped",
        );
        return;
    }
    if apply_to_store(store, update) {
        let guard = subs.pin();
        if let Some(tx) = guard.get(&update.key) {
            if tx.is_closed() {
                // Conditional remove: only evict if the entry is still the closed sender.
                guard.compute(update.key.clone(), |existing| match existing {
                    Some((_, tx)) if tx.is_closed() => Operation::Remove,
                    _ => Operation::Abort(()),
                });
            } else {
                let val = if update.is_tombstone { None } else { Some(update.value.clone()) };
                let _ = tx.send(val);
            }
        }
    }
}
