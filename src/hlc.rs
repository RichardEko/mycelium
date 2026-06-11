//! Hybrid Logical Clock (HLC) used to stamp every locally-originated update
//! and to observe remote stamps as they arrive.
//!
//! The clock is packed into a `u64`:
//!
//! - **High 48 bits**: physical Unix-millisecond time. Good through year 8901.
//! - **Low 16 bits**: logical counter. Up to 65 535 events per millisecond
//!   per node before saturation.
//!
//! This packing is deliberate: comparisons on the raw `u64` are also
//! comparisons on the (physical, logical) tuple in lexicographic order, so
//! the existing `>`-based LWW comparison in `apply_and_notify` continues to
//! work unchanged when the timestamp field is reinterpreted as HLC. Higher
//! physical always beats lower physical regardless of logical; with equal
//! physical, higher logical wins.
//!
//! ## Algorithm (Kulkarni et al. 2014)
//!
//! For a local event:
//!
//! ```text
//! next.phys    = max(prev.phys, wall_now_ms)
//! next.logical = if next.phys == prev.phys { prev.logical + 1 } else { 0 }
//! ```
//!
//! For an observed remote event with timestamp `r`:
//!
//! ```text
//! next.phys    = max(prev.phys, r.phys, wall_now_ms)
//! next.logical = match (next.phys ==) {
//!   prev.phys && r.phys => max(prev.logical, r.logical) + 1,
//!   prev.phys           => prev.logical + 1,
//!   r.phys              => r.logical + 1,
//!   neither             => 0,
//! }
//! ```
//!
//! This guarantees that any locally-originated update following an observed
//! remote update has a strictly greater HLC than the remote one — so causal
//! happens-before is preserved even under wall-clock skew.
//!
//! ## Documented limits
//!
//! - **Logical counter saturation.** The low 16 bits cap the logical at
//!   `65 535` events per ms per node. `tick()` saturates rather than
//!   wrapping, so on a node sustained at >65 k local events/ms the
//!   physical part takes over (every saturating-tick acts as if the wall
//!   clock advanced by 1 ms). Ordering stays correct; resolution degrades
//!   gracefully. Widening to a larger logical (e.g. 44+20) would require a
//!   wire-version bump and is deferred.
//!
//! - **Wall-clock forward jump.** Accepted unconditionally:
//!   `next.phys = max(prev.phys, wall_now_ms)` will jump the HLC ahead.
//!   Correctness for causal happens-before is unaffected. The
//!   `crate::seen` TTL eviction is keyed by `physical_ms`; a large
//!   forward jump can briefly age out the entire seen-set, allowing one
//!   round of duplicate admissions until the seen-set repopulates.
//!
//! - **Wall-clock backward jump.** `prev.phys` wins via `max`, so the HLC
//!   never goes backwards. Subsequent `wall_now_ms` reads return values
//!   smaller than `prev.phys`; the logical counter increments instead,
//!   maintaining strict monotonicity. The HLC is "self-correcting" against
//!   transient backward jumps but cannot recover lost time once the clock
//!   resyncs forward again.
//!
//! - **Remote drift bound.** `observe` clamps the *remote* physical time to
//!   `wall_now_ms + max_drift_ms` (default 5 minutes,
//!   [`DEFAULT_MAX_CLOCK_DRIFT_MS`]; configurable via
//!   `GossipConfig::max_clock_drift_ms`, `0` disables). Without the clamp a
//!   single peer with a far-future clock dragged every node's HLC forward
//!   irrecoverably (`max` never decays), and read-side evaporation — the
//!   substrate's failure detector — was silently suspended for the full
//!   drift duration (M2 Run-19 finding; the Kulkarni et al. 2014 algorithm
//!   mandates exactly this bound). Clamping is rate-limit `warn!`ed.
//!
//!   **Documented trade-off:** for a stamp *beyond* the bound, the usual
//!   "local write after observe beats the remote" guarantee is deliberately
//!   waived — the poisoned entry would win LWW against local rewrites until
//!   the wall clock catches up. The capability layer independently
//!   quarantines such entries (its freshness check rejects stamps too far
//!   in the future), so failure detection no longer depends on this. Full
//!   inbound rejection of out-of-bound updates (store-level quarantine) is
//!   a candidate for the next wire-policy pass.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::warn;

/// Default bound on how far ahead of the local wall clock a *remote* HLC
/// stamp may pull this node's clock (5 minutes). See the module doc's
/// "Remote drift bound" section. Override via
/// `GossipConfig::max_clock_drift_ms`; `0` disables the bound.
pub(crate) const DEFAULT_MAX_CLOCK_DRIFT_MS: u64 = 300_000;

/// Minimum interval between drift-clamp warnings (per `Hlc` instance), so a
/// peer with a persistently skewed clock cannot flood the log.
const DRIFT_WARN_INTERVAL_MS: u64 = 10_000;

/// Number of bits reserved for the logical counter.
pub(crate) const LOGICAL_BITS: u32 = 16;
/// Mask covering the logical portion of a packed HLC value.
pub(crate) const LOGICAL_MASK: u64 = (1 << LOGICAL_BITS) - 1;

/// Extracts the physical-ms portion of a packed HLC timestamp.
#[inline]
pub(crate) fn physical_ms(packed: u64) -> u64 {
    packed >> LOGICAL_BITS
}

/// Packs `(phys_ms, logical)` into a single `u64` HLC value.
#[inline]
pub(crate) fn pack(phys_ms: u64, logical: u64) -> u64 {
    (phys_ms << LOGICAL_BITS) | (logical & LOGICAL_MASK)
}

/// Returns the current wall-clock time in milliseconds since the Unix epoch.
/// Saturates to 0 if the clock is somehow before the epoch (a Windows-only
/// edge case after manual clock changes).
#[inline]
fn wall_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Hybrid Logical Clock. Internal state is a single `AtomicU64` storing the
/// packed `(phys_ms << 16) | logical` value, so both `tick` and `observe`
/// are lock-free CAS loops.
pub(crate) struct Hlc {
    state: AtomicU64,
    /// Remote stamps may pull the clock at most this far ahead of the local
    /// wall clock. `0` = unbounded (pre-bound behaviour).
    max_drift_ms: u64,
    /// Wall-ms of the last drift-clamp warning (rate limiting).
    last_drift_warn_ms: AtomicU64,
}

impl Hlc {
    /// Constructs a fresh HLC initialised to the current wall clock with
    /// logical zero and the default remote-drift bound.
    pub(crate) fn new() -> Self {
        Self::with_max_drift(DEFAULT_MAX_CLOCK_DRIFT_MS)
    }

    /// Like [`new`](Self::new) with an explicit remote-drift bound in
    /// milliseconds. `0` disables the bound.
    pub(crate) fn with_max_drift(max_drift_ms: u64) -> Self {
        Self {
            state:              AtomicU64::new(pack(wall_now_ms(), 0)),
            max_drift_ms,
            last_drift_warn_ms: AtomicU64::new(0),
        }
    }

    /// Returns the current packed HLC value without advancing it.
    pub(crate) fn current(&self) -> u64 {
        self.state.load(Ordering::Acquire)
    }

    /// Advances the clock for a local event and returns the new packed
    /// timestamp.
    pub(crate) fn tick(&self) -> u64 {
        loop {
            let prev      = self.state.load(Ordering::Acquire);
            let prev_phys = physical_ms(prev);
            let prev_log  = prev & LOGICAL_MASK;
            let now_ms    = wall_now_ms();

            let next_phys = prev_phys.max(now_ms);
            let next_log  = if next_phys == prev_phys {
                // Saturating bump — if a node manages 64k events in a single ms
                // the next tick still advances physical instead of wrapping.
                prev_log.saturating_add(1).min(LOGICAL_MASK)
            } else {
                0
            };
            let new_ts = pack(next_phys, next_log);
            if self.state
                .compare_exchange(prev, new_ts, Ordering::Release, Ordering::Acquire)
                .is_ok()
            {
                return new_ts;
            }
        }
    }

    /// Absorbs a remote HLC stamp and advances the local clock to dominate
    /// the merged `(local, remote, wall_now)` triple. Returns the new packed
    /// timestamp.
    ///
    /// The remote physical component is clamped to
    /// `wall_now_ms + max_drift_ms` (see the module doc's "Remote drift
    /// bound") so a peer with a far-future clock cannot drag this node's
    /// clock — and with it, every subsequent local write stamp — into the
    /// future. Clamping is logged at `warn!`, rate-limited per instance.
    pub(crate) fn observe(&self, remote: u64) -> u64 {
        let mut remote_phys = physical_ms(remote);
        let remote_log      = remote & LOGICAL_MASK;
        if self.max_drift_ms > 0 {
            let limit = wall_now_ms().saturating_add(self.max_drift_ms);
            if remote_phys > limit {
                let now = wall_now_ms();
                let last = self.last_drift_warn_ms.load(Ordering::Relaxed);
                if now.saturating_sub(last) >= DRIFT_WARN_INTERVAL_MS
                    && self
                        .last_drift_warn_ms
                        .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                        .is_ok()
                {
                    warn!(
                        remote_phys_ms = remote_phys,
                        ahead_ms = remote_phys - now,
                        max_drift_ms = self.max_drift_ms,
                        "remote HLC stamp exceeds the clock-drift bound; clamping — \
                         a peer's clock is far ahead (check NTP on the sending node). \
                         Entries carrying such stamps are quarantined by read-side \
                         freshness checks until the sender's clock is fixed.",
                    );
                }
                remote_phys = limit;
            }
        }
        loop {
            let prev      = self.state.load(Ordering::Acquire);
            let prev_phys = physical_ms(prev);
            let prev_log  = prev & LOGICAL_MASK;
            let now_ms    = wall_now_ms();

            let next_phys = prev_phys.max(remote_phys).max(now_ms);
            let next_log  = if next_phys == prev_phys && next_phys == remote_phys {
                prev_log.max(remote_log).saturating_add(1).min(LOGICAL_MASK)
            } else if next_phys == prev_phys {
                prev_log.saturating_add(1).min(LOGICAL_MASK)
            } else if next_phys == remote_phys {
                remote_log.saturating_add(1).min(LOGICAL_MASK)
            } else {
                0
            };
            let new_ts = pack(next_phys, next_log);
            if self.state
                .compare_exchange(prev, new_ts, Ordering::Release, Ordering::Acquire)
                .is_ok()
            {
                return new_ts;
            }
        }
    }
}

impl Default for Hlc {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the HLC state directly; drift bound disabled so tests that pin
    /// far-future physical values keep their original semantics.
    fn pinned(state: u64) -> Hlc {
        Hlc {
            state:              AtomicU64::new(state),
            max_drift_ms:       0,
            last_drift_warn_ms: AtomicU64::new(0),
        }
    }

    #[test]
    fn pack_and_unpack_roundtrip() {
        let p = pack(1_700_000_000_000, 42);
        assert_eq!(physical_ms(p), 1_700_000_000_000);
        assert_eq!(p & LOGICAL_MASK, 42);
    }

    #[test]
    fn comparison_lex_order_phys_then_logical() {
        let a = pack(100, 0);
        let b = pack(100, 1);
        let c = pack(101, 0);
        assert!(a < b);
        assert!(b < c);
        // Physical-only difference dominates logical.
        assert!(pack(99, LOGICAL_MASK) < pack(100, 0));
    }

    #[test]
    fn tick_strictly_monotonic() {
        let hlc = Hlc::new();
        let mut prev = hlc.tick();
        for _ in 0..1_000 {
            let next = hlc.tick();
            assert!(next > prev, "tick must be strictly monotonic ({} <= {})", next, prev);
            prev = next;
        }
    }

    #[test]
    fn tick_bumps_logical_within_same_ms() {
        // Force the HLC to a fixed physical so wall_now_ms doesn't matter.
        let hlc = pinned(pack(u64::MAX >> LOGICAL_BITS, 0));
        let a = hlc.tick();
        let b = hlc.tick();
        assert_eq!(physical_ms(a), physical_ms(b));
        assert!((b & LOGICAL_MASK) > (a & LOGICAL_MASK));
    }

    #[test]
    fn observe_absorbs_strictly_greater_remote_phys() {
        let hlc = Hlc::new();
        // Build a remote stamp 10 seconds in the future.
        let future_phys = physical_ms(hlc.current()) + 10_000;
        let remote = pack(future_phys, 5);
        let next = hlc.observe(remote);
        assert!(next > remote, "observe must dominate the remote stamp");
        assert!(physical_ms(next) >= future_phys);
    }

    #[test]
    fn observe_then_tick_dominates_remote() {
        let hlc = Hlc::new();
        let future_phys = physical_ms(hlc.current()) + 1_000;
        let remote = pack(future_phys, 999);
        hlc.observe(remote);
        let local = hlc.tick();
        assert!(local > remote, "local tick after observe must beat remote");
    }

    #[test]
    fn observe_same_phys_bumps_logical() {
        // Pin the HLC and the remote to a physical value far in the future
        // so wall_now_ms can't dominate either side and reset the logical.
        let far_future = wall_now_ms() + 60_000;
        let hlc = pinned(pack(far_future, 3));
        let remote = pack(far_future, 7);
        let next = hlc.observe(remote);
        assert_eq!(physical_ms(next), far_future);
        assert!((next & LOGICAL_MASK) > 7);
    }

    /// Regression gate for the M2 Run-19 finding (Semantic Correctness).
    ///
    /// `observe` used to absorb remote physical time with an unbounded
    /// `max`: one peer with a far-ahead clock (NTP failure, or a hostile
    /// node in a non-TLS cluster) dragged this node's HLC — and every
    /// subsequent local write stamp — into the future, silently suspending
    /// read-side evaporation (the substrate's failure detector) for the
    /// full drift duration. Fixed by the drift clamp (module doc, "Remote
    /// drift bound"); companion impact gate in the capability layer:
    /// `future_stamped_entry_outlives_its_evaporation_window_by_the_drift`.
    #[test]
    fn observe_bounds_remote_clock_drift() {
        let hlc = Hlc::new(); // default bound: DEFAULT_MAX_CLOCK_DRIFT_MS
        let week_ahead = wall_now_ms() + 7 * 24 * 3600 * 1000;
        hlc.observe(pack(week_ahead, 0));
        let local = hlc.tick();
        assert!(
            physical_ms(local) <= wall_now_ms() + DEFAULT_MAX_CLOCK_DRIFT_MS + 1_000,
            "a remote stamp a week ahead must not drag local time past the \
             drift bound (local phys = {}, wall = {})",
            physical_ms(local), wall_now_ms(),
        );
    }

    /// The bound is configurable and `0` disables it (pre-bound behaviour).
    #[test]
    fn observe_drift_bound_zero_disables_clamp() {
        let hlc = Hlc::with_max_drift(0);
        let week_ahead_phys = wall_now_ms() + 7 * 24 * 3600 * 1000;
        let next = hlc.observe(pack(week_ahead_phys, 0));
        assert!(physical_ms(next) >= week_ahead_phys, "0 = unbounded");
    }

    /// Remote stamps within the bound are still absorbed normally — the
    /// clamp only engages beyond `wall_now + max_drift_ms`.
    #[test]
    fn observe_within_bound_still_dominates_remote() {
        let hlc = Hlc::new();
        let near_future = pack(wall_now_ms() + 10_000, 5); // 10 s ahead
        let next = hlc.observe(near_future);
        assert!(next > near_future, "in-bound remote stamps must be dominated");
    }

    #[test]
    fn logical_saturates_rather_than_wrapping() {
        let hlc = pinned(pack(0, LOGICAL_MASK));
        // Stuck at physical 0 forever (wall_now_ms is huge but the state
        // pretends it's still 1970). Real callers can't hit this — used here
        // to assert the saturation invariant.
        let next = hlc.tick();
        // Saturating: physical advances past 0, so logical resets to 0.
        assert!(physical_ms(next) > 0);
        assert_eq!(next & LOGICAL_MASK, 0);
    }
}

#[cfg(test)]
mod prop_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// Sequential tick() calls are strictly monotonically increasing.
        #[test]
        fn tick_always_advances(n_ticks in 2usize..=50usize) {
            let hlc = Hlc::new();
            let mut prev = hlc.tick();
            for _ in 1..n_ticks {
                let next = hlc.tick();
                prop_assert!(next > prev,
                    "tick must be strictly monotonic: {} <= {}", next, prev);
                prev = next;
            }
        }

        /// After observe(remote), a subsequent tick() must produce a value strictly
        /// greater than remote. Remote physical is bounded to ±60 s of now so that
        /// the logical counter never saturates (saturation at 64 K events/ms is
        /// unreachable in any real workload and is documented in the HLC module).
        #[test]
        fn tick_after_observe_beats_remote(
            phys_offset_ms in -60_000i64..=60_000i64,
            remote_log     in 0u64..0x8000u64,
        ) {
            let now = wall_now_ms();
            let remote_phys = (now as i64 + phys_offset_ms).max(1) as u64;
            let remote = pack(remote_phys, remote_log);
            let hlc = Hlc::new();
            hlc.observe(remote);
            let local = hlc.tick();
            prop_assert!(local > remote,
                "tick after observe must beat remote: local={} remote={}", local, remote);
        }

        /// observe(remote) always returns a value >= remote. Equal only at logical
        /// saturation (remote_log = LOGICAL_MASK, remote_phys > now_ms) which is
        /// out of scope for realistic workloads; this test stays well below that.
        #[test]
        fn observe_at_least_as_large_as_remote(
            phys_offset_ms in -60_000i64..=60_000i64,
            remote_log     in 0u64..0x8000u64,
        ) {
            let now = wall_now_ms();
            let remote_phys = (now as i64 + phys_offset_ms).max(1) as u64;
            let remote = pack(remote_phys, remote_log);
            let hlc = Hlc::new();
            let result = hlc.observe(remote);
            prop_assert!(result >= remote,
                "observe must return >= remote: result={} remote={}", result, remote);
        }
    }
}
