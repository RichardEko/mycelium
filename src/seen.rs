/// Seen-set split into independent shards to reduce CAS contention when many
/// connection-handler tasks insert nonces concurrently.
///
/// Each nonce is routed to a shard by its low bits. Nonces are random u64s so
/// the distribution is uniform. Shards are independent `papaya::HashMap`s —
/// readers and writers on different shards never share a cache line or a hazard
/// pointer epoch.
pub(crate) struct ShardedSeen {
    shards: Box<[papaya::HashMap<u64, u64>]>,
    /// `n_shards - 1`; shard selection is `nonce & mask` (cheap bitwise AND).
    mask: usize,
}

impl ShardedSeen {
    /// Create a new `ShardedSeen`. `n` is rounded up to the nearest power of two.
    pub(crate) fn new(n: usize) -> Self {
        let n = n.max(1).next_power_of_two();
        Self {
            shards: (0..n)
                .map(|_| papaya::HashMap::new())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            mask: n - 1,
        }
    }

    #[inline]
    fn shard(&self, nonce: u64) -> &papaya::HashMap<u64, u64> {
        &self.shards[(nonce as usize) & self.mask]
    }

    /// Records `nonce` with receive timestamp `ts`.
    /// Returns `true` if the nonce was **already present** (duplicate — caller should drop).
    ///
    /// Named `is_duplicate` rather than `insert` to avoid confusion with the Rust std
    /// convention where `insert` returns `true` for *new* insertions.
    #[inline]
    pub(crate) fn is_duplicate(&self, nonce: u64, ts: u64) -> bool {
        self.shard(nonce).pin().insert(nonce, ts).is_some()
    }

    /// Total number of entries across all shards.
    pub(crate) fn len(&self) -> usize {
        self.shards.iter().map(|s| s.len()).sum()
    }

    /// Tick-level eviction. Removes nonces whose receive timestamp is at or
    /// before the chosen cutoff. If the total entry count exceeds `max_entries`
    /// the more aggressive `half_window` cutoff is used; otherwise `seen_cutoff`.
    ///
    /// Returns `true` if the set **still** exceeds `max_entries` after eviction,
    /// signalling that the caller should run `emergency_trim`.
    pub(crate) fn evict(&self, max_entries: usize, seen_cutoff: u64, half_window: u64) -> bool {
        let len_before = self.len();
        let cutoff = if len_before > max_entries { half_window } else { seen_cutoff };
        let removed = self.evict_below(cutoff);
        len_before.saturating_sub(removed) > max_entries
    }

    /// Emergency trim: remove all entries with timestamp at or before `cutoff`.
    /// Called when normal eviction still leaves the set over the size limit.
    pub(crate) fn emergency_trim(&self, cutoff: u64) {
        self.evict_below(cutoff);
    }

    fn evict_below(&self, cutoff: u64) -> usize {
        let mut removed = 0usize;
        for shard in self.shards.iter() {
            let guard = shard.pin();
            // Two-pass scan: papaya does not expose mutable iteration, so stale keys
            // must be collected into a Vec before removal. At max_seen_entries = 100k
            // this allocates O(max_seen_entries / n_shards) u64s per GC tick.
            // TODO: upstream a papaya `drain_if` / `retain` API to eliminate this Vec.
            let stale: Vec<u64> = guard
                .iter()
                .filter_map(|(&k, &v)| if v <= cutoff { Some(k) } else { None })
                .collect();
            for key in stale {
                // Atomic check-and-remove: only evict if the stored timestamp is still
                // ≤ cutoff. A concurrent is_duplicate() that refreshed the timestamp to a
                // newer value aborts the remove rather than evicting a live entry.
                let result = guard.compute(key, |existing| match existing {
                    Some((_, &v)) if v <= cutoff => papaya::Operation::Remove,
                    _ => papaya::Operation::Abort(()),
                });
                if matches!(result, papaya::Compute::Removed(..)) {
                    removed += 1;
                }
            }
        }
        removed
    }
}
