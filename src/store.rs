use crate::framing::GossipUpdate;
use crate::locality::LocalityPath;
use crate::node_id::NodeId;
use ahash::RandomState;
use bytes::Bytes;
use papaya::Operation;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex, OnceLock, PoisonError,
};
use tokio::sync::watch;
use tracing::warn;

/// Layer II watch-channel map. Maps a key to a `watch::Sender` that fires whenever
/// the key's value changes in the store. Written by `GossipAgent::subscribe` (Layer II)
/// and notified by `apply_and_notify` (Layer I/II bridge). Co-located in `KvState`
/// because subscriptions share KvState's lifetime and are always distributed together —
/// separating them would require threading a second Arc through every context struct.
pub(crate) type KvSubscriptions = Arc<papaya::HashMap<Arc<str>, watch::Sender<Option<Bytes>>>>;

/// Pure Layer I storage bundle — everything the gossip KV store needs without
/// any Layer II signalling concerns.
///
/// Wrapped inside [`KvState`], which adds the Layer II `subscriptions` field.
/// All existing callers reach these fields through `KvState`'s `Deref`
/// implementation, so no call sites need to change when the two concerns are
/// discussed separately.
pub(crate) struct KvStore {
    pub store:             Arc<papaya::HashMap<Arc<str>, StoreEntry>>,
    pub prefix_index:      Arc<PrefixIndex>,
    /// Striped locks serialising secondary-index reconciliation per key hash.
    /// The store CAS in [`apply_and_notify`] is lock-free, so two winning
    /// writers to the same key can reach the index-maintenance step in the
    /// opposite order of their CASes; the stripe lock + store re-read makes
    /// the final index state always match the final store state. Never held
    /// across `await` (apply_and_notify is synchronous); no other lock is
    /// acquired while a stripe is held.
    pub index_stripes:     Arc<[Mutex<()>; INDEX_STRIPES]>,
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
    /// Per-key durability trackers installed by `set_with_min_acks`. Each key
    /// holds a copy-on-write *list* so concurrent same-key callers coexist;
    /// `apply_and_notify` calls `observe(sender, timestamp)` on every tracker
    /// in the list for every incoming update. Mutate only via
    /// `kv_quorum::{install_tracker, remove_tracker}`.
    pub quorum_trackers: Arc<papaya::HashMap<Arc<str>, crate::agent::kv_quorum::TrackerList>>,
}

/// Bundled KV-path state shared across connection handlers, consensus tasks,
/// and opacity governors.
///
/// Holds a [`KvStore`] (Layer I) plus `subscriptions` (Layer II watch channels).
/// The `Deref<Target = KvStore>` impl means all callers can reach `kv_state.store`,
/// `kv_state.dropped_frames`, etc. without knowing about the split — no call-site
/// changes required.
///
/// `apply_and_notify` is the **single Layer I/II crossing point**: it writes to
/// `KvStore` and then notifies both `KvStore::prefix_watchers` (Layer I push
/// channels) and `KvState::subscriptions` (Layer II watch channels). All other
/// code treats Layer I and Layer II as independent concerns.
///
/// ## papaya pin() guard invariant
///
/// All papaya maps are accessed through a *pinned epoch guard* (`map.pin()`).
/// Guards **must not be held across `await` points** — holding one suspends
/// the papaya epoch-reclamation collector, causing unbounded memory growth.
/// Every call site in this module and in `connection.rs` follows this rule:
/// pin, do the synchronous work, drop the guard, then await.  Reviewers: if
/// you add an `await` inside a block that already holds a `pin()` guard,
/// extract the result first and drop the guard before awaiting.
pub(crate) struct KvState {
    /// Layer I storage (accessed via Deref).
    pub kv_store: KvStore,
    /// Layer II watch channels. See [`KvSubscriptions`] for design notes.
    pub subscriptions: KvSubscriptions,
}

impl std::ops::Deref for KvState {
    type Target = KvStore;
    #[inline(always)]
    fn deref(&self) -> &KvStore { &self.kv_store }
}

/// One per-subscriber predicate registration in [`KvStore::prefix_predicate_watchers`].
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
            kv_store: KvStore {
                store:             Arc::new(papaya::HashMap::new()),
                prefix_index:      Arc::new(PrefixIndex::new()),
                index_stripes:     Arc::new(std::array::from_fn(|_| Mutex::new(()))),
                cap_ns_index:      Arc::new(PrefixIndex::new()),
                hash_acc:          Arc::new(AtomicU64::new(0)),
                dropped_frames:    Arc::new(AtomicU64::new(0)),
                max_store_entries,
                grp_generation:    Arc::new(AtomicU64::new(0)),
                prefix_watchers:           Arc::new(papaya::HashMap::new()),
                prefix_predicate_watchers: Arc::new(papaya::HashMap::new()),
                next_pred_watcher_id:      Arc::new(AtomicU64::new(0)),
                peer_localities:           Arc::new(papaya::HashMap::new()),
                quorum_trackers:           Arc::new(papaya::HashMap::new()),
            },
            subscriptions: Arc::new(papaya::HashMap::new()),
        })
    }
}

/// Secondary index for O(1) bucket + O(k) prefix scan.
///
/// Maps the first path segment of a key (e.g. `"grp"`, `"load"`, `"svc"`) to
/// the set of live full keys under that segment. Only live (non-tombstone) keys
/// are tracked; tombstoned keys are removed.
///
/// Reconciled in [`apply_and_notify`] under a [`KvStore::index_stripes`] lock:
/// after a winning store CAS, the writer re-reads the stored entry and sets
/// index membership to match it, so concurrent writers to the same key cannot
/// leave the index diverged from the store (M2 Run-18 finding). Allows
/// [`GossipAgent::scan_prefix`] to skip the full store and iterate only the
/// matching bucket — O(|bucket|) instead of O(|store|).
pub(crate) type PrefixIndex = papaya::HashMap<Arc<str>, Arc<papaya::HashMap<Arc<str>, ()>>>;

/// Stripe count for [`KvStore::index_stripes`]. Power of two; selected by
/// key hash, so contention is per-colliding-key, not global.
pub(crate) const INDEX_STRIPES: usize = 64;

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
    new_bucket.pin().insert(Arc::clone(&key), ());
    let result = guard.compute(Arc::from(seg), |existing| match existing {
        Some(_) => papaya::Operation::Abort(()),
        None    => papaya::Operation::Insert(Arc::clone(&new_bucket)),
    });
    // Concurrent racer installed their bucket first; insert into theirs.
    if let papaya::Compute::Aborted(_) = result
        && let Some(bucket) = guard.get(seg) {
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
    new_bucket.pin().insert(Arc::clone(&inner), ());
    let outer_clone = Arc::clone(&outer);
    let result = guard.compute(outer, |existing| match existing {
        Some(_) => papaya::Operation::Abort(()),
        None    => papaya::Operation::Insert(Arc::clone(&new_bucket)),
    });
    if let papaya::Compute::Aborted(_) = result
        && let Some(bucket) = guard.get(outer_clone.as_ref()) {
            bucket.pin().insert(inner, ());
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
            if Arc::strong_count(v) == 1 { Some(Arc::clone(k)) } else { None }
        })
        .take(pool.len().saturating_sub(target))
        .collect();
    for key in candidates {
        guard.compute(Arc::clone(&key), |existing| match existing {
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
        return Arc::clone(existing);
    }
    // Pool cap: skip insertion once the limit is reached.
    if max_keys > 0 && pool.len() >= max_keys {
        return key;
    }
    // Slow path: CAS-insert. The callback may retry on contention; each attempt
    // clones `key` cheaply (O(1) Arc refcount bump), so no Option-slot trick is needed.
    match guard.compute(Arc::clone(&key), |existing| match existing {
        Some(_) => papaya::Operation::Abort(()),
        None    => papaya::Operation::Insert(Arc::clone(&key)),
    }) {
        papaya::Compute::Inserted(_, v) => Arc::clone(v),
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

/// LWW comparison: does the incoming write replace the current entry?
///
/// - Different timestamps: strictly newer wins (unchanged).
/// - Equal timestamps, incoming tombstone: tombstone wins (unchanged — deletes
///   are never resurrected by a concurrent same-timestamp data write).
/// - Equal timestamps, current entry is a tombstone: data loses (unchanged).
/// - Equal timestamps, **data vs data: lexicographically greater value wins.**
///   This tiebreak is deterministic and order-independent, so two nodes that
///   apply the same pair of concurrent equal-timestamp writes in opposite
///   orders converge to the same value. Without it they diverge permanently —
///   and undetectably, because the anti-entropy digest hashes (key, timestamp)
///   only and is identical on both sides. Equal timestamps are reachable in
///   practice: two writers in the same wall-clock millisecond whose HLCs have
///   not yet observed each other both stamp `(ms, logical=0)`.
#[inline]
fn lww_wins(incoming_ts: u64, incoming_tombstone: bool, incoming_val: &Option<Bytes>, curr: &StoreEntry) -> bool {
    if incoming_ts != curr.timestamp {
        return incoming_ts > curr.timestamp;
    }
    if incoming_tombstone { return true; }
    match (&curr.data, incoming_val) {
        (None, _)          => false,    // tie against a tombstone: data never resurrects
        (Some(c), Some(v)) => v > c,    // deterministic concurrent-data tiebreak
        (Some(_), None)    => false,    // unreachable: !tombstone ⇒ val is Some
    }
}

/// Applies `update` using last-write-wins. Returns `true` if the store changed.
/// Conflict resolution is [`lww_wins`] — see its doc for the equal-timestamp rules.
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
    let result = guard.compute(Arc::clone(&update.key), |existing| {
        match existing {
            None => Operation::Insert(StoreEntry { data: val.clone(), timestamp: ts }),
            Some((_, curr)) => {
                if lww_wins(ts, is_tombstone, &val, curr) {
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
        let result = guard.compute(Arc::clone(&update.key), |existing| {
            old_ts_if_live = None; // reset on each CAS retry
            match existing {
                None => Operation::Insert(StoreEntry { data: val.clone(), timestamp: ts }),
                Some((_, curr)) => {
                    if lww_wins(ts, is_tombstone, &val, curr) {
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
        // Release: pairs with Acquire in the gossip-loop cache reader (tasks.rs).
        // Guarantees that when the reader observes the new gen value, the grp/ KV
        // write that caused the bump is already visible in the papaya store.
        if update.key.starts_with("grp/") {
            kv.grp_generation.fetch_add(1, Ordering::Release);
        }
        // (Capability/requirement watchers use `prefix_watchers` below, not a
        // generation counter — a previous design held a `cap_generation` here
        // but it had no readers and was removed.)
        //
        // ── Secondary-structure reconcile (prefix index, cap_ns_index,
        //    peer_localities) ─────────────────────────────────────────────
        // The store CAS above is lock-free, so two winning writers to the
        // same key can reach this point in the opposite order of their
        // CASes. Deriving index maintenance from `update` (insert for data,
        // remove for tombstone) let the later-arriving thread undo the
        // earlier one's index op while the store kept the earlier value — a
        // live key permanently invisible to scan_prefix, unrepaired by
        // anti-entropy (M2 Run-18 finding; regression test:
        // `prefix_index_consistent_under_tombstone_insert_race`).
        //
        // Reconcile instead: under a per-key stripe lock, re-read the stored
        // entry and set membership in every secondary structure to match it.
        // Each winner's CAS happens-before its own lock section and lock
        // sections are totally ordered, so the LAST lock-holder reads a
        // store state that includes every winning CAS — the final index
        // state always matches the final store state.
        {
            let stripe = &kv.index_stripes[(key_hash as usize) % INDEX_STRIPES];
            let _stripe_guard = stripe.lock().unwrap_or_else(PoisonError::into_inner);
            let current_live: Option<Bytes> = kv.store.pin()
                .get(update.key.as_ref())
                .and_then(|e| e.data.clone());

            if current_live.is_some() {
                prefix_index_insert(&kv.prefix_index, Arc::clone(&update.key));
            } else {
                prefix_index_remove(&kv.prefix_index, &update.key);
            }

            if let Some(identity) = cap_ns_index_key(&update.key) {
                if current_live.is_some() {
                    index_bucket_insert(&kv.cap_ns_index, identity, Arc::clone(&update.key));
                } else {
                    index_bucket_remove(&kv.cap_ns_index, &identity, &update.key);
                }
            }

            // peer_localities cache from cap/{node_id}/locality/self entries.
            // Locality is treated as a capability for KV-path uniformity but is
            // also cached in decoded form so hot gossip-forwarding paths don't
            // re-decode per message. Decoded from the STORED value, not
            // `update.value`, so the cache reflects whichever write won.
            if let Some(rest) = update.key.strip_prefix("cap/")
                && let Some(node_id_str) = rest.strip_suffix("/locality/self")
                    && !node_id_str.contains('/')
                        && let Ok(node_id) = node_id_str.parse::<NodeId>() {
                            let guard = kv.peer_localities.pin();
                            match current_live.as_ref() {
                                None => { guard.remove(&node_id); }
                                Some(stored) => match LocalityPath::decode(stored) {
                                    Some(loc) => { guard.insert(node_id, loc); }
                                    None      => warn!(
                                        key = %update.key,
                                        "malformed LocalityPath — peer sent bytes under cap/*/locality/self that did not decode",
                                    ),
                                },
                            }
                        }
        }
        let subs_guard = kv.subscriptions.pin();
        if let Some(tx) = subs_guard.get(&update.key) {
            if tx.is_closed() {
                subs_guard.compute(Arc::clone(&update.key), |existing| match existing {
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
                    to_evict.push(Arc::clone(prefix));
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
        // Notify all in-flight set_with_min_acks waiters tracking this key
        // (concurrent same-key callers each hold their own tracker).
        if let Some(trackers) = kv.quorum_trackers.pin().get(&update.key) {
            for tracker in trackers.iter() {
                tracker.observe(update.sender, update.timestamp);
            }
        }
        #[cfg(feature = "metrics")]
        metrics::gauge!("gossip_store_entries").set(kv.store.len() as f64);
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
                Some((Arc::clone(key), data))
            })
            .collect()
    } else {
        store_guard.iter()
            .filter(|(k, v)| v.data.is_some() && k.starts_with(prefix))
            .map(|(k, v)| (Arc::clone(k), v.data.clone().expect("filtered by is_some above")))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framing::GossipUpdate;
    use bytes::Bytes;

    #[test]
    fn lww_newer_wins() {
        let store: papaya::HashMap<Arc<str>, StoreEntry> = papaya::HashMap::new();
        apply_to_store(&store, &GossipUpdate {
            sender: 0, key: "k".into(),
            value: Bytes::from_static(b"old"),
            timestamp: 100, nonce: 1, ttl: 1, is_tombstone: false,
        });
        apply_to_store(&store, &GossipUpdate {
            sender: 0, key: "k".into(),
            value: Bytes::from_static(b"new"),
            timestamp: 200, nonce: 2, ttl: 1, is_tombstone: false,
        });
        assert_eq!(store.pin().get("k").unwrap().data, Some(Bytes::from_static(b"new")));
    }

    #[test]
    fn lww_stale_ignored() {
        let store: papaya::HashMap<Arc<str>, StoreEntry> = papaya::HashMap::new();
        apply_to_store(&store, &GossipUpdate {
            sender: 0, key: "k".into(),
            value: Bytes::from_static(b"new"),
            timestamp: 200, nonce: 1, ttl: 1, is_tombstone: false,
        });
        apply_to_store(&store, &GossipUpdate {
            sender: 0, key: "k".into(),
            value: Bytes::from_static(b"old"),
            timestamp: 100, nonce: 2, ttl: 1, is_tombstone: false,
        });
        assert_eq!(store.pin().get("k").unwrap().data, Some(Bytes::from_static(b"new")));
    }

    #[test]
    fn lww_tombstone_wins_tie() {
        let store: papaya::HashMap<Arc<str>, StoreEntry> = papaya::HashMap::new();
        apply_to_store(&store, &GossipUpdate {
            sender: 0, key: "k".into(),
            value: Bytes::from_static(b"v"),
            timestamp: 100, nonce: 1, ttl: 1, is_tombstone: false,
        });
        apply_to_store(&store, &GossipUpdate {
            sender: 0, key: "k".into(),
            value: Bytes::new(),
            timestamp: 100, nonce: 2, ttl: 1, is_tombstone: true,
        });
        assert_eq!(store.pin().get("k").unwrap().data, None, "tombstone must win equal-timestamp tie");
    }

    #[test]
    fn lww_equal_timestamp_concurrent_data_converges() {
        // Regression test for the M2 Run-16 probe finding: two writers, same
        // key, identical HLC timestamps (possible: same wall ms, logical 0, no
        // prior observation), different values, applied in opposite orders on
        // two nodes. Before the `lww_wins` data-vs-data tiebreak, each node
        // kept its first-applied value — permanent divergence, invisible to
        // anti-entropy because the digest hashes (key, timestamp) only.
        let w_a = GossipUpdate {
            sender: 1, key: "k".into(), value: Bytes::from_static(b"from-a"),
            timestamp: 100, nonce: 1, ttl: 1, is_tombstone: false,
        };
        let w_b = GossipUpdate {
            sender: 2, key: "k".into(), value: Bytes::from_static(b"from-b"),
            timestamp: 100, nonce: 2, ttl: 1, is_tombstone: false,
        };
        let node1: papaya::HashMap<Arc<str>, StoreEntry> = papaya::HashMap::new();
        let node2: papaya::HashMap<Arc<str>, StoreEntry> = papaya::HashMap::new();
        apply_to_store(&node1, &w_a); apply_to_store(&node1, &w_b);
        apply_to_store(&node2, &w_b); apply_to_store(&node2, &w_a);
        let v1 = node1.pin().get("k").unwrap().data.clone();
        let v2 = node2.pin().get("k").unwrap().data.clone();
        assert_eq!(v1, v2, "both nodes must agree regardless of apply order");
        assert_eq!(
            v1, Some(Bytes::from_static(b"from-b")),
            "tiebreak is deterministic: lexicographically greater value wins",
        );
        // The digests agree too — and now agree on the same underlying value.
        assert_eq!(store_hash(&node1), store_hash(&node2));
    }

    #[test]
    fn lww_data_does_not_resurrect_after_tombstone_tie() {
        let store: papaya::HashMap<Arc<str>, StoreEntry> = papaya::HashMap::new();
        apply_to_store(&store, &GossipUpdate {
            sender: 0, key: "k".into(),
            value: Bytes::new(),
            timestamp: 100, nonce: 1, ttl: 1, is_tombstone: true,
        });
        apply_to_store(&store, &GossipUpdate {
            sender: 0, key: "k".into(),
            value: Bytes::from_static(b"v"),
            timestamp: 100, nonce: 2, ttl: 1, is_tombstone: false,
        });
        assert_eq!(store.pin().get("k").unwrap().data, None, "same-timestamp data must not resurrect tombstone");
    }

    /// Regression gate for the M2 Run-18 finding (dim 9).
    ///
    /// `apply_and_notify` used to maintain the prefix index *after* the store
    /// CAS, derived from the update being applied. If a tombstone (lower ts)
    /// and a live insert (higher ts) raced on the same key, both CAS'd in ts
    /// order, but the tombstone thread's `prefix_index_remove` could land
    /// after the insert thread's `prefix_index_insert` — the store held a
    /// live key the index had lost, `scan_prefix` silently missed it, and
    /// anti-entropy never repaired it (re-applying the same (key, ts) loses
    /// LWW and never touches the index). Reproduced at 86 of 100 000 racing
    /// rounds on 2026-06-11 (Apple Silicon).
    ///
    /// Fixed by the stripe-locked reconcile in `apply_and_notify`: index
    /// membership is re-derived from the stored entry under
    /// `KvStore::index_stripes`, so the final index state always matches the
    /// final store state.
    #[test]
    fn prefix_index_consistent_under_tombstone_insert_race() {
        use std::sync::Barrier;
        let kv = KvState::new(0);
        let rounds: u64 = 100_000;
        let keys: Vec<Arc<str>> =
            (0..rounds).map(|i| Arc::from(format!("race/k{i}").as_str())).collect();
        let barrier = Barrier::new(2);
        std::thread::scope(|s| {
            s.spawn(|| {
                for (i, key) in keys.iter().enumerate() {
                    barrier.wait();
                    apply_and_notify(&kv, &GossipUpdate {
                        sender: 1, key: Arc::clone(key), value: Bytes::new(),
                        timestamp: 100, nonce: 2 * i as u64 + 1, ttl: 1, is_tombstone: true,
                    });
                }
            });
            s.spawn(|| {
                for (i, key) in keys.iter().enumerate() {
                    barrier.wait();
                    apply_and_notify(&kv, &GossipUpdate {
                        sender: 2, key: Arc::clone(key), value: Bytes::from_static(b"v"),
                        timestamp: 200, nonce: 2 * i as u64 + 2, ttl: 1, is_tombstone: false,
                    });
                }
            });
        });
        // The live write (ts 200) beats the tombstone (ts 100) in either CAS
        // order, so every key must be live in the store…
        let store_guard = kv.store.pin();
        let idx_guard = kv.prefix_index.pin();
        let bucket = idx_guard.get("race").expect("race bucket exists");
        let bucket_guard = bucket.pin();
        let mut lost = Vec::new();
        for key in &keys {
            assert!(
                store_guard.get(key.as_ref()).is_some_and(|e| e.data.is_some()),
                "store must converge to the live ts-200 write for {key}"
            );
            // …and every live key must still be visible to scan_prefix.
            if !bucket_guard.contains_key(key.as_ref()) {
                lost.push(Arc::clone(key));
            }
        }
        assert!(
            lost.is_empty(),
            "{} of {rounds} live keys lost from the prefix index by the \
             tombstone/insert index race (first: {})",
            lost.len(), lost[0],
        );
    }

    /// M2 Run-19 perf probe: the Run-18 stripe-locked index reconcile added a
    /// mutex acquisition + store re-read to every winning write. This smoke
    /// bounds the cost: single-thread distinct-key throughput and 8-thread
    /// worst-case stripe contention (64 hot keys) must both stay far above
    /// any realistic gossip ingest rate. Run explicitly:
    /// `cargo test --release --lib -- --ignored apply_and_notify_throughput_smoke --nocapture`
    #[test]
    #[ignore = "perf smoke; run explicitly with --release --ignored --nocapture"]
    fn apply_and_notify_throughput_smoke() {
        let kv = KvState::new(0);

        let n = 200_000u64;
        let keys: Vec<Arc<str>> =
            (0..n).map(|i| Arc::from(format!("perf/k{i}").as_str())).collect();
        let t0 = std::time::Instant::now();
        for (i, k) in keys.iter().enumerate() {
            apply_and_notify(&kv, &GossipUpdate {
                sender: 1, key: Arc::clone(k), value: Bytes::from_static(b"v"),
                timestamp: 100, nonce: i as u64 + 1, ttl: 1, is_tombstone: false,
            });
        }
        let single = n as f64 / t0.elapsed().as_secs_f64();

        // Worst-case contention: 8 threads, 64 hot keys, strictly rising
        // timestamps so every write wins its CAS and runs the reconcile.
        let m = 100_000u64;
        let hot: Vec<Arc<str>> =
            (0..64).map(|i| Arc::from(format!("hot/k{i}").as_str())).collect();
        let ts_base = AtomicU64::new(1_000);
        let t1 = std::time::Instant::now();
        std::thread::scope(|s| {
            for _ in 0..8 {
                s.spawn(|| {
                    for j in 0..m {
                        let ts = ts_base.fetch_add(1, Ordering::Relaxed);
                        let k = &hot[(j as usize) % hot.len()];
                        apply_and_notify(&kv, &GossipUpdate {
                            sender: 1, key: Arc::clone(k), value: Bytes::from_static(b"v"),
                            timestamp: ts, nonce: ts, ttl: 1, is_tombstone: false,
                        });
                    }
                });
            }
        });
        let contended = (8 * m) as f64 / t1.elapsed().as_secs_f64();

        eprintln!(
            "apply_and_notify: {single:.0}/s single-thread distinct keys; \
             {contended:.0}/s 8-thread 64-hot-key contention"
        );
        assert!(single > 100_000.0, "single-thread throughput {single:.0}/s below floor");
        assert!(contended > 100_000.0, "contended throughput {contended:.0}/s below floor");
    }

    /// M2 Run-18 race-family sweep: concurrent `set_with_min_acks` callers on
    /// the SAME key must coexist. The previous single-slot tracker map let the
    /// second caller overwrite the first tracker, and the first caller's
    /// unconditional cleanup then deleted the second's — both could time out
    /// spuriously despite the acks arriving.
    #[test]
    fn concurrent_quorum_trackers_coexist_and_remove_only_self() {
        use crate::agent::kv_quorum::{install_tracker, remove_tracker, QuorumAckTracker};
        let kv = KvState::new(0);
        let key: Arc<str> = Arc::from("q/k");
        let (t1, rx1) = QuorumAckTracker::new(100, 1);
        let (t2, rx2) = QuorumAckTracker::new(100, 1);
        install_tracker(&kv.quorum_trackers, Arc::clone(&key), &t1);
        install_tracker(&kv.quorum_trackers, Arc::clone(&key), &t2);

        // One inbound peer update acks BOTH in-flight callers.
        apply_and_notify(&kv, &GossipUpdate {
            sender: 7, key: Arc::clone(&key), value: Bytes::from_static(b"v1"),
            timestamp: 150, nonce: 1, ttl: 1, is_tombstone: false,
        });
        assert_eq!(*rx1.borrow(), 1, "first caller sees the ack");
        assert_eq!(*rx2.borrow(), 1, "second caller sees the ack");

        // First caller completes: removes ONLY its own tracker.
        remove_tracker(&kv.quorum_trackers, &key, &t1);
        apply_and_notify(&kv, &GossipUpdate {
            sender: 8, key: Arc::clone(&key), value: Bytes::from_static(b"v2"),
            timestamp: 151, nonce: 2, ttl: 1, is_tombstone: false,
        });
        assert_eq!(*rx2.borrow(), 2, "surviving caller keeps receiving acks");
        assert_eq!(*rx1.borrow(), 1, "removed tracker is no longer observed");

        remove_tracker(&kv.quorum_trackers, &key, &t2);
        assert!(
            kv.quorum_trackers.pin().get(&key).is_none(),
            "entry drops when the last tracker is removed"
        );
    }

    /// Concurrent-stress coverage for `apply_and_notify` beyond the single
    /// tombstone/insert pair (M2 Run-18 improvement target #2): 8 threads of
    /// random insert/tombstone churn with colliding timestamps over a shared
    /// keyspace spanning plain, `grp/`, `cap/{node}/{ns}/{name}`, and
    /// `cap/{node}/locality/self` keys. Afterwards every secondary structure
    /// — prefix index, `cap_ns_index`, `peer_localities` — must agree with
    /// the store in BOTH directions for every key.
    #[test]
    fn secondary_structures_consistent_under_concurrent_churn() {
        let kv = KvState::new(0);

        let plain: Vec<Arc<str>> =
            (0..40).map(|i| Arc::from(format!("stress/k{i}").as_str())).collect();
        let grp: Vec<Arc<str>> =
            (0..20).map(|i| Arc::from(format!("grp/g{i}/m").as_str())).collect();
        // All cap keys share one cap_ns identity bucket ("cap/ns/skill") so
        // bucket insert/remove contend on the same inner map.
        let cap: Vec<Arc<str>> = (0..20)
            .map(|i| Arc::from(format!("cap/127.0.0.1:{}/ns/skill", 9000 + i).as_str()))
            .collect();
        let loc: Vec<Arc<str>> = (0..8)
            .map(|i| Arc::from(format!("cap/127.0.0.1:{}/locality/self", 9500 + i).as_str()))
            .collect();
        let keys: Vec<Arc<str>> =
            plain.iter().chain(&grp).chain(&cap).chain(&loc).cloned().collect();
        let loc_payload = LocalityPath::new(["eu", "az1"]).encode();

        let threads = 8;
        let ops_per_thread = 30_000u64;
        let nonce_base = AtomicU64::new(1);
        std::thread::scope(|s| {
            for _ in 0..threads {
                s.spawn(|| {
                    let mut rng = fastrand::Rng::new();
                    for _ in 0..ops_per_thread {
                        let key = &keys[rng.usize(..keys.len())];
                        let is_tombstone = rng.u8(..10) < 4;
                        // Small timestamp range so concurrent writers collide
                        // on ties and on either side of each other constantly.
                        let ts = rng.u64(1..64);
                        let value = if is_tombstone {
                            Bytes::new()
                        } else if key.ends_with("/locality/self") {
                            loc_payload.clone()
                        } else {
                            Bytes::from(format!("v{ts}"))
                        };
                        apply_and_notify(&kv, &GossipUpdate {
                            sender: 1,
                            key: Arc::clone(key),
                            value,
                            timestamp: ts,
                            nonce: nonce_base.fetch_add(1, Ordering::Relaxed),
                            ttl: 1,
                            is_tombstone,
                        });
                    }
                });
            }
        });

        let store_guard = kv.store.pin();
        let idx_guard = kv.prefix_index.pin();
        let ns_guard = kv.cap_ns_index.pin();
        let loc_guard = kv.peer_localities.pin();
        for key in &keys {
            let live = store_guard.get(key.as_ref()).is_some_and(|e| e.data.is_some());
            let seg = key.split('/').next().unwrap();
            let in_prefix = idx_guard
                .get(seg)
                .is_some_and(|b| b.pin().contains_key(key.as_ref()));
            assert_eq!(
                in_prefix, live,
                "prefix index diverged from store for {key} (live={live})"
            );
            if let Some(identity) = cap_ns_index_key(key) {
                let in_ns = ns_guard
                    .get(identity.as_ref())
                    .is_some_and(|b| b.pin().contains_key(key.as_ref()));
                assert_eq!(
                    in_ns, live,
                    "cap_ns_index diverged from store for {key} (live={live})"
                );
            }
            if let Some(node_id_str) = key
                .strip_prefix("cap/")
                .and_then(|r| r.strip_suffix("/locality/self"))
            {
                let node_id: NodeId = node_id_str.parse().unwrap();
                assert_eq!(
                    loc_guard.contains_key(&node_id), live,
                    "peer_localities diverged from store for {key} (live={live})"
                );
            }
        }
    }
}

#[cfg(test)]
mod prop_tests {
    use super::*;
    use crate::framing::GossipUpdate;
    use bytes::Bytes;
    use proptest::prelude::*;

    /// Build an update with a unique nonce derived from the timestamp so repeated
    /// application of the same logical event is idempotent in the seen-set.
    fn update(ts: u64, is_tombstone: bool) -> GossipUpdate {
        GossipUpdate {
            sender: 0,
            key: Arc::from("k"),
            value: if is_tombstone { Bytes::new() } else { Bytes::from_static(b"v") },
            timestamp: ts,
            nonce: ts,
            ttl: 1,
            is_tombstone,
        }
    }

    proptest! {
        /// Convergence: applying the same set of updates in any order produces identical state.
        /// Restricted to distinct timestamps because same-timestamp data (non-tombstone) writes
        /// are not commutative — the first one applied wins, by design.
        #[test]
        fn lww_convergence_any_order(
            mut pairs in prop::collection::vec((1u64..=10_000u64, any::<bool>()), 1..=10usize)
        ) {
            // Enforce distinct timestamps to keep the property commutative.
            pairs.dedup_by_key(|(ts, _)| *ts);
            let store_a: papaya::HashMap<Arc<str>, StoreEntry> = papaya::HashMap::new();
            let store_b: papaya::HashMap<Arc<str>, StoreEntry> = papaya::HashMap::new();
            for (ts, is_tomb) in &pairs {
                apply_to_store(&store_a, &update(*ts, *is_tomb));
            }
            for (ts, is_tomb) in pairs.iter().rev() {
                apply_to_store(&store_b, &update(*ts, *is_tomb));
            }
            let a = store_a.pin().get("k").map(|e| (e.timestamp, e.data.is_none()));
            let b = store_b.pin().get("k").map(|e| (e.timestamp, e.data.is_none()));
            prop_assert_eq!(a, b, "LWW must converge regardless of application order");
        }

        /// The winning entry is always the one with the highest timestamp.
        /// On a tie the tombstone wins regardless of application order.
        #[test]
        fn lww_winner_is_max_timestamp(
            ts_a: u64, is_tomb_a: bool, ts_b: u64, is_tomb_b: bool
        ) {
            let store: papaya::HashMap<Arc<str>, StoreEntry> = papaya::HashMap::new();
            apply_to_store(&store, &update(ts_a, is_tomb_a));
            apply_to_store(&store, &update(ts_b, is_tomb_b));
            let entry = store.pin().get("k").cloned().unwrap();
            if ts_a > ts_b {
                prop_assert_eq!(entry.timestamp, ts_a);
                prop_assert_eq!(entry.data.is_none(), is_tomb_a);
            } else if ts_b > ts_a {
                prop_assert_eq!(entry.timestamp, ts_b);
                prop_assert_eq!(entry.data.is_none(), is_tomb_b);
            } else {
                // Equal timestamps: tombstone beats data.
                prop_assert_eq!(entry.timestamp, ts_a);
                if is_tomb_a || is_tomb_b {
                    prop_assert!(entry.data.is_none(), "tombstone must win equal-timestamp tie");
                }
            }
        }
    }
}
