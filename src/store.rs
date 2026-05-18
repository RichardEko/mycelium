use crate::framing::GossipUpdate;
use ahash::RandomState;
use bytes::Bytes;
use papaya::Operation;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, OnceLock,
};
use tokio::sync::watch;
use tracing::warn;

/// Secondary index for O(1) bucket + O(k) prefix scan.
///
/// Maps the first path segment of a key (e.g. `"grp"`, `"load"`, `"svc"`) to
/// the set of live full keys under that segment. Only live (non-tombstone) keys
/// are tracked; tombstoned keys are removed.
///
/// Updated atomically in [`apply_and_notify`] whenever the store changes. Allows
/// [`GossipAgent::scan_prefix`] to skip the full store and iterate only the
/// matching bucket — O(|bucket|) instead of O(|store|).
pub(crate) type PrefixIndex = papaya::HashMap<Arc<str>, Arc<papaya::HashMap<Arc<str>, ()>>>;

#[inline]
fn prefix_seg(key: &str) -> Option<&str> {
    key.find('/').map(|i| &key[..i])
}

/// Inserts `key` into the `seg` bucket, creating the bucket if absent.
pub(crate) fn prefix_index_insert(index: &PrefixIndex, key: Arc<str>) {
    let Some(seg) = prefix_seg(&key) else { return };
    let guard = index.pin();
    if let Some(bucket) = guard.get(seg) {
        bucket.pin().insert(key, ());
        return;
    }
    // Create and install a new bucket, tolerating a concurrent racer.
    let new_bucket: Arc<papaya::HashMap<Arc<str>, ()>> = Arc::new(papaya::HashMap::new());
    guard.compute(Arc::from(seg), |existing| match existing {
        Some(_) => papaya::Operation::Abort(()),
        None    => papaya::Operation::Insert(new_bucket.clone()),
    });
    // After compute, the bucket definitely exists (ours or the racer's).
    if let Some(bucket) = guard.get(seg) {
        bucket.pin().insert(key, ());
    }
}

/// Removes `key` from the `seg` bucket (no-op if absent).
pub(crate) fn prefix_index_remove(index: &PrefixIndex, key: &Arc<str>) {
    let Some(seg) = prefix_seg(key) else { return };
    if let Some(bucket) = index.pin().get(seg) {
        bucket.pin().remove(key.as_ref());
    }
}

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

/// Fixed-seed hash state used by both `store_hash` and `apply_and_notify` so the
/// incremental accumulator and the full-scan produce identical values.
static HASH_SEED: OnceLock<RandomState> = OnceLock::new();
fn hash_seed() -> &'static RandomState {
    HASH_SEED.get_or_init(|| RandomState::with_seeds(1, 2, 3, 4))
}

/// O(1) store hash read — returns the value of the incremental XOR accumulator.
///
/// The accumulator is maintained by `apply_and_notify` on every live write or
/// tombstone. Use this in production; `store_hash` (full scan) is kept for tests
/// and one-shot re-seeding after a snapshot import.
pub(crate) fn store_hash_acc(acc: &AtomicU64) -> u64 {
    acc.load(Ordering::Relaxed)
}

/// Computes a stable XOR-hash of all live (key, timestamp) pairs in the store.
///
/// Uses a fixed-seed `AHasher` so the value is identical across processes for the
/// same set of entries. Tombstones are excluded: they are not part of the live data
/// set and GC'd at different times on different nodes, which would cause spurious
/// mismatches. Returns 0 only when the store is empty; 0 is the "no digest" sentinel
/// in `WireMessage::StateRequest` so an empty store still triggers a full snapshot.
/// In practice a non-empty store almost never XORs to 0 (probability < 1 in 2^64).
///
/// Used in tests to seed a fresh accumulator and verify accumulator correctness.
/// Production code uses [`store_hash_acc`] instead.
#[cfg(test)]
pub(crate) fn store_hash(store: &papaya::HashMap<Arc<str>, StoreEntry>) -> u64 {
    let rs = hash_seed();
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

/// Applies `update` to the store, maintains the prefix index, then notifies any
/// subscriber watching that key.
///
/// When `max_store_entries > 0` and the store's current size meets or exceeds that
/// limit, live (non-tombstone) writes are silently dropped. Tombstone writes are
/// always accepted — they reduce the live count and must propagate.
///
/// `hash_acc` is the incremental XOR accumulator maintained in the caller's
/// `GossipAgent` or `ConnContext`. Updated in O(1) per call; readable via
/// [`store_hash_acc`].
pub(crate) fn apply_and_notify(
    store:             &papaya::HashMap<Arc<str>, StoreEntry>,
    subs:              &papaya::HashMap<Arc<str>, watch::Sender<Option<Bytes>>>,
    update:            &GossipUpdate,
    max_store_entries: usize,
    prefix_index:      &PrefixIndex,
    hash_acc:          &AtomicU64,
) {
    if max_store_entries > 0 && !update.is_tombstone && store.len() >= max_store_entries {
        warn!(
            key = %update.key,
            cap = max_store_entries,
            "KV store cap reached; live write dropped",
        );
        return;
    }
    // Read the current entry before the CAS for incremental hash maintenance.
    // A concurrent update between this read and the CAS can leave the accumulator
    // transiently off by one — acceptable since any mismatch triggers a full snapshot.
    let old_entry = store.pin().get(&*update.key).cloned();
    if apply_to_store(store, update) {
        // Maintain the incremental XOR hash.
        let key_hash = hash_seed().hash_one(update.key.as_bytes());
        if let Some(ref old) = old_entry {
            if old.data.is_some() {
                hash_acc.fetch_xor(key_hash ^ old.timestamp, Ordering::Relaxed);
            }
        }
        if !update.is_tombstone {
            hash_acc.fetch_xor(key_hash ^ update.timestamp, Ordering::Relaxed);
        }
        // Maintain the secondary prefix index.
        if update.is_tombstone {
            prefix_index_remove(prefix_index, &update.key);
        } else {
            prefix_index_insert(prefix_index, update.key.clone());
        }
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
