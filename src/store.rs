use crate::framing::GossipUpdate;
use crate::locality::LocalityPath;
use crate::node_id::NodeId;
use ahash::RandomState;
use bytes::Bytes;
use papaya::Operation;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, OnceLock,
};
use tokio::sync::watch;
use tracing::warn;

/// Layer II watch-channel map. Maps a key to a `watch::Sender` that fires whenever
/// the key's value changes in the store. Written by `GossipAgent::subscribe` (Layer II)
/// and notified by `apply_and_notify` (Layer I/II bridge). Co-located in `KvState`
/// because subscriptions share KvState's lifetime and are always distributed together —
/// separating them would require threading a second Arc through every context struct.
pub(crate) type KvSubscriptions = Arc<papaya::HashMap<Arc<str>, watch::Sender<Option<Bytes>>>>;

/// Bundled KV-path state shared across connection handlers, consensus tasks,
/// and opacity governors.
///
/// Replacing the five individual Arc fields with a single `Arc<KvState>` reduces
/// the blast-radius of future additions: new KV fields require only one struct
/// change and one construction-site change rather than edits in every intermediate
/// context struct (`ListenerContext`, `ConnContext`, `ConsensusEngine`, etc.).
///
/// `subscriptions` is a Layer II concern co-located here for lifecycle convenience.
/// See [`KvSubscriptions`] for the full rationale.
pub(crate) struct KvState {
    pub store:             Arc<papaya::HashMap<Arc<str>, StoreEntry>>,
    /// Layer II watch channels. See [`KvSubscriptions`] for design notes.
    pub subscriptions:     KvSubscriptions,
    pub prefix_index:      Arc<PrefixIndex>,
    /// Secondary index for O(1) cap/req lookups by (namespace, name).
    /// Outer key: `"{seg}/{ns}/{name}"` (e.g. `"cap/compute/text-gen"`).
    /// Inner key: the full store key (`"cap/{node}/{ns}/{name}"`).
    /// Maintained alongside `prefix_index` in `apply_and_notify`.
    pub cap_ns_index:      Arc<PrefixIndex>,
    pub hash_acc:          Arc<AtomicU64>,
    pub dropped_frames:    Arc<AtomicU64>,
    pub max_store_entries: usize,
    /// Monotonic counter bumped whenever a `grp/` key is written or tombstoned.
    /// `cached_group_members` uses this to detect remote membership changes without
    /// scanning the store — the cached roster is stale if the counter has advanced.
    pub grp_generation:    Arc<AtomicU64>,
    /// Push-based prefix watch channels. `apply_and_notify` increments the `u64` counter
    /// for any registered prefix that matches a changed key. Watchers use `changed().await`
    /// rather than polling. Created lazily via `GossipAgent::subscribe_prefix`.
    pub prefix_watchers: Arc<papaya::HashMap<Arc<str>, Arc<watch::Sender<u64>>>>,
    /// Per-subscriber prefix watch channels with per-entry predicates.
    /// `apply_and_notify` wakes an entry only if `update.key.starts_with(prefix)`
    /// AND `predicate(&update.key)` returns `true`. Keyed by a monotonic id
    /// allocated from [`KvState::next_pred_watcher_id`]; one entry per
    /// registration (no sharing).
    pub prefix_predicate_watchers: Arc<papaya::HashMap<u64, PrefixPredicateWatcher>>,
    /// Monotonic id allocator for [`KvState::prefix_predicate_watchers`].
    pub next_pred_watcher_id: Arc<AtomicU64>,
    /// Cache of peer `LocalityPath`s populated by `apply_and_notify` from
    /// `cap/{node_id}/locality/self` writes. Used on hot gossip-forwarding paths
    /// (locality-aware fan-out scoring) without re-decoding the KV entry per message.
    pub peer_localities: Arc<papaya::HashMap<NodeId, LocalityPath>>,
}

/// One per-subscriber predicate registration in [`KvState::prefix_predicate_watchers`].
/// `prefix` gates the cheap `starts_with` first; `predicate` runs only when the
/// prefix matches and is allowed to be more expensive.
pub struct PrefixPredicateWatcher {
    pub prefix:    Arc<str>,
    pub predicate: Arc<dyn Fn(&str) -> bool + Send + Sync>,
    pub tx:        Arc<watch::Sender<u64>>,
}

impl KvState {
    /// Constructs a new, empty `KvState` wrapped in an `Arc`.
    ///
    /// All sub-Arcs are created here so callers own a single `Arc<KvState>` rather
    /// than building five independent Arcs and threading them separately.
    pub(crate) fn new(max_store_entries: usize) -> Arc<Self> {
        Arc::new(Self {
            store:             Arc::new(papaya::HashMap::new()),
            subscriptions:     Arc::new(papaya::HashMap::new()),
            prefix_index:      Arc::new(PrefixIndex::new()),
            cap_ns_index:      Arc::new(PrefixIndex::new()),
            hash_acc:          Arc::new(AtomicU64::new(0)),
            dropped_frames:    Arc::new(AtomicU64::new(0)),
            max_store_entries,
            grp_generation:    Arc::new(AtomicU64::new(0)),
            prefix_watchers:           Arc::new(papaya::HashMap::new()),
            prefix_predicate_watchers: Arc::new(papaya::HashMap::new()),
            next_pred_watcher_id:      Arc::new(AtomicU64::new(0)),
            peer_localities:           Arc::new(papaya::HashMap::new()),
        })
    }
}

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
    // Pre-insert the key so it lands atomically with bucket creation when we win.
    let new_bucket: Arc<papaya::HashMap<Arc<str>, ()>> = Arc::new(papaya::HashMap::new());
    new_bucket.pin().insert(key.clone(), ());
    let result = guard.compute(Arc::from(seg), |existing| match existing {
        Some(_) => papaya::Operation::Abort(()),
        None    => papaya::Operation::Insert(new_bucket.clone()),
    });
    // Concurrent racer installed their bucket first; insert into theirs.
    if let papaya::Compute::Aborted(_) = result {
        if let Some(bucket) = guard.get(seg) {
            bucket.pin().insert(key, ());
        }
    }
}

/// Removes `key` from the `seg` bucket (no-op if absent).
pub(crate) fn prefix_index_remove(index: &PrefixIndex, key: &Arc<str>) {
    let Some(seg) = prefix_seg(key) else { return };
    if let Some(bucket) = index.pin().get(seg) {
        bucket.pin().remove(key.as_ref());
    }
}

/// Extracts the cap-ns identity key from a full `cap/` or `req/` store key.
/// `cap/{node}/{ns}/{name}` → `"cap/{ns}/{name}"` (and similarly for `req/`).
/// Returns `None` for keys with a different prefix or malformed shape.
pub(crate) fn cap_ns_index_key(key: &str) -> Option<Arc<str>> {
    let mut parts = key.splitn(4, '/');
    let seg  = parts.next()?;
    if seg != "cap" && seg != "req" { return None; }
    let _node = parts.next()?;
    let ns   = parts.next()?;
    let name = parts.next()?;
    Some(Arc::from(format!("{seg}/{ns}/{name}").as_str()))
}

/// Inserts `inner_key` into the `outer` bucket of `index`, creating the bucket if absent.
pub(crate) fn index_bucket_insert(index: &PrefixIndex, outer: Arc<str>, inner: Arc<str>) {
    let guard = index.pin();
    if let Some(bucket) = guard.get(outer.as_ref()) {
        bucket.pin().insert(inner, ());
        return;
    }
    let new_bucket: Arc<papaya::HashMap<Arc<str>, ()>> = Arc::new(papaya::HashMap::new());
    new_bucket.pin().insert(inner.clone(), ());
    let outer_clone = outer.clone();
    let result = guard.compute(outer, |existing| match existing {
        Some(_) => papaya::Operation::Abort(()),
        None    => papaya::Operation::Insert(new_bucket.clone()),
    });
    if let papaya::Compute::Aborted(_) = result {
        if let Some(bucket) = guard.get(outer_clone.as_ref()) {
            bucket.pin().insert(inner, ());
        }
    }
}

/// Removes `inner_key` from the `outer` bucket (no-op if absent).
pub(crate) fn index_bucket_remove(index: &PrefixIndex, outer: &str, inner: &str) {
    if let Some(bucket) = index.pin().get(outer) {
        bucket.pin().remove(inner);
    }
}

static KEY_POOL: OnceLock<papaya::HashMap<Arc<str>, Arc<str>>> = OnceLock::new();

fn key_pool() -> &'static papaya::HashMap<Arc<str>, Arc<str>> {
    KEY_POOL.get_or_init(papaya::HashMap::new)
}

/// Returns the current number of entries in the intern pool.
/// Zero if `intern_keys = false` and no key has ever been interned.
/// Returns the current number of entries in the intern pool.
pub(crate) fn intern_pool_len() -> usize {
    KEY_POOL.get().map_or(0, |p| p.len())
}

/// Evicts pool entries that have no external holders (pool-only `Arc` references).
///
/// Iterates the pool and removes any entry whose value `Arc::strong_count` is 1 —
/// meaning only the pool itself holds the allocation and no caller is currently
/// using it. Stops once the pool shrinks to `target` entries.
///
/// Called from the GC task when `intern_max_keys > 0` and `pool_len > intern_max_keys`,
/// allowing the pool to reclaim unused keys rather than simply refusing new inserts.
pub(crate) fn shrink_intern_pool(target: usize) {
    let pool = key_pool();
    if pool.len() <= target { return; }
    let guard = pool.pin();
    let candidates: Vec<Arc<str>> = guard
        .iter()
        .filter_map(|(k, v)| {
            // strong_count == 1 means only the pool holds this Arc — safe to evict.
            if Arc::strong_count(v) == 1 { Some(k.clone()) } else { None }
        })
        .take(pool.len().saturating_sub(target))
        .collect();
    for key in candidates {
        guard.compute(key.clone(), |existing| match existing {
            // Re-check inside compute: abort if someone grabbed the Arc since we sampled.
            Some((_, v)) if Arc::strong_count(v) == 1 => papaya::Operation::Remove,
            _ => papaya::Operation::Abort(()),
        });
        if pool.len() <= target { break; }
    }
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
    // Fast path: already interned.
    if let Some(existing) = guard.get(&*key) {
        return existing.clone();
    }
    // Pool cap: skip insertion once the limit is reached.
    if max_keys > 0 && pool.len() >= max_keys {
        return key;
    }
    // Slow path: CAS-insert. The callback may retry on contention; each attempt
    // clones `key` cheaply (O(1) Arc refcount bump), so no Option-slot trick is needed.
    match guard.compute(key.clone(), |existing| match existing {
        Some(_) => papaya::Operation::Abort(()),
        None    => papaya::Operation::Insert(key.clone()),
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
#[cfg(test)]
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
/// When `kv.max_store_entries > 0` and the store's current size meets or exceeds that
/// limit, live (non-tombstone) writes are silently dropped. Tombstone writes are
/// always accepted — they reduce the live count and must propagate.
///
/// The incremental XOR accumulator in `kv.hash_acc` is updated atomically with the
/// store CAS: the old entry's live timestamp is captured *inside* the `compute`
/// callback, eliminating the TOCTOU window that existed when the old entry was read
/// before the CAS in a separate step. The callback resets its capture on each retry
/// so the final (successful) invocation always leaves the correct value.
///
/// Callers construct the [`GossipUpdate`] via
/// [`crate::framing::make_gossip_update`], which is the canonical write-side
/// factory for every higher layer — see that function's doc comment for the
/// placement rationale and the layers it serves.
pub(crate) fn apply_and_notify(kv: &KvState, update: &GossipUpdate) {
    if kv.max_store_entries > 0 && !update.is_tombstone && kv.store.len() >= kv.max_store_entries {
        warn!(
            key = %update.key,
            cap = kv.max_store_entries,
            "KV store cap reached; live write dropped",
        );
        return;
    }

    let ts           = update.timestamp;
    let is_tombstone = update.is_tombstone;
    let val = if is_tombstone { None } else { Some(update.value.clone()) };

    // Capture the old live timestamp inside the compute callback so there is no
    // TOCTOU window between reading the old entry and performing the CAS.
    let mut old_ts_if_live: Option<u64> = None;

    let changed = {
        let guard = kv.store.pin();
        let result = guard.compute(update.key.clone(), |existing| {
            old_ts_if_live = None; // reset on each CAS retry
            match existing {
                None => Operation::Insert(StoreEntry { data: val.clone(), timestamp: ts }),
                Some((_, curr)) => {
                    let wins = if is_tombstone { ts >= curr.timestamp } else { ts > curr.timestamp };
                    if wins {
                        if curr.data.is_some() {
                            old_ts_if_live = Some(curr.timestamp);
                        }
                        Operation::Insert(StoreEntry { data: val.clone(), timestamp: ts })
                    } else {
                        Operation::Abort(())
                    }
                }
            }
        });
        matches!(result, papaya::Compute::Inserted(..) | papaya::Compute::Updated { .. })
    };

    if changed {
        // Maintain the incremental XOR hash.
        let key_hash = hash_seed().hash_one(update.key.as_bytes());
        if let Some(old_ts) = old_ts_if_live {
            kv.hash_acc.fetch_xor(key_hash ^ old_ts, Ordering::Relaxed);
        }
        if !is_tombstone {
            kv.hash_acc.fetch_xor(key_hash ^ ts, Ordering::Relaxed);
        }
        // Bump the group-roster generation counter so cached_group_members knows
        // to re-fetch when any peer joins or leaves a group.
        if update.key.starts_with("grp/") {
            kv.grp_generation.fetch_add(1, Ordering::Relaxed);
        }
        // (Capability/requirement watchers use `prefix_watchers` below, not a
        // generation counter — a previous design held a `cap_generation` here
        // but it had no readers and was removed.)
        // Maintain the peer_localities cache from cap/{node_id}/locality/self entries.
        // Locality is treated as a capability for KV-path uniformity but is also cached
        // in decoded form so hot gossip-forwarding paths don't re-decode per message.
        if let Some(rest) = update.key.strip_prefix("cap/") {
            if let Some(node_id_str) = rest.strip_suffix("/locality/self") {
                if !node_id_str.contains('/') {
                    if let Ok(node_id) = node_id_str.parse::<NodeId>() {
                        let guard = kv.peer_localities.pin();
                        if is_tombstone {
                            guard.remove(&node_id);
                        } else {
                            match LocalityPath::decode(&update.value) {
                                Some(loc) => { guard.insert(node_id, loc); }
                                None      => warn!(
                                    key = %update.key,
                                    "malformed LocalityPath — peer sent bytes under cap/*/locality/self that did not decode",
                                ),
                            }
                        }
                    }
                }
            }
        }
        // Maintain the secondary prefix index.
        if is_tombstone {
            prefix_index_remove(&kv.prefix_index, &update.key);
        } else {
            prefix_index_insert(&kv.prefix_index, update.key.clone());
        }
        // Maintain cap_ns_index for cap/ and req/ keys.
        if let Some(identity) = cap_ns_index_key(&update.key) {
            if is_tombstone {
                index_bucket_remove(&kv.cap_ns_index, &identity, &update.key);
            } else {
                index_bucket_insert(&kv.cap_ns_index, identity, update.key.clone());
            }
        }
        let subs_guard = kv.subscriptions.pin();
        if let Some(tx) = subs_guard.get(&update.key) {
            if tx.is_closed() {
                subs_guard.compute(update.key.clone(), |existing| match existing {
                    Some((_, tx)) if tx.is_closed() => Operation::Remove,
                    _ => Operation::Abort(()),
                });
            } else {
                let notif = if is_tombstone { None } else { Some(update.value.clone()) };
                let _ = tx.send(notif);
            }
        }
        // Notify any prefix watchers whose registered prefix matches the changed key.
        // Closed senders are evicted lazily to avoid unbounded growth from churn.
        let prefix_guard = kv.prefix_watchers.pin();
        let mut to_evict: Vec<Arc<str>> = Vec::new();
        for (prefix, tx) in prefix_guard.iter() {
            if update.key.starts_with(prefix.as_ref()) {
                if tx.is_closed() {
                    to_evict.push(prefix.clone());
                } else {
                    tx.send_modify(|n| *n = n.wrapping_add(1));
                }
            }
        }
        for p in to_evict {
            prefix_guard.compute(p, |existing| match existing {
                Some((_, tx)) if tx.is_closed() => Operation::Remove,
                _ => Operation::Abort(()),
            });
        }
        // Notify per-subscriber predicate watchers. starts_with is the cheap
        // gate; predicate is run only when the prefix matches.
        let pred_guard = kv.prefix_predicate_watchers.pin();
        let mut pred_evict: Vec<u64> = Vec::new();
        for (id, w) in pred_guard.iter() {
            if w.tx.is_closed() {
                pred_evict.push(*id);
                continue;
            }
            if update.key.starts_with(w.prefix.as_ref()) && (w.predicate)(&update.key) {
                w.tx.send_modify(|n| *n = n.wrapping_add(1));
            }
        }
        for id in pred_evict {
            pred_guard.compute(id, |existing| match existing {
                Some((_, w)) if w.tx.is_closed() => Operation::Remove,
                _ => Operation::Abort(()),
            });
        }
    }
}

/// Returns all live (non-tombstone) key-value pairs whose key starts with `prefix`.
///
/// Uses the prefix index for O(|bucket|) access when the first path segment is
/// known; falls back to a full O(|store|) scan for unknown prefixes.
///
/// Exposed as a free function so modules that hold only `Arc<KvState>` (e.g. HTTP
/// handlers) can perform prefix scans without going through `GossipAgent`.
pub(crate) fn scan_kv_prefix(kv: &KvState, prefix: &str) -> Vec<(Arc<str>, Bytes)> {
    let seg         = prefix.find('/').map_or(prefix, |i| &prefix[..i]);
    let store_guard = kv.store.pin();
    let idx_guard   = kv.prefix_index.pin();
    if let Some(bucket) = idx_guard.get(seg) {
        bucket.pin().iter()
            .filter_map(|(key, _)| {
                if !key.starts_with(prefix) { return None; }
                let entry = store_guard.get(key.as_ref())?;
                let data  = entry.data.clone()?;
                Some((key.clone(), data))
            })
            .collect()
    } else {
        store_guard.iter()
            .filter(|(k, v)| v.data.is_some() && k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.data.clone().unwrap()))
            .collect()
    }
}
