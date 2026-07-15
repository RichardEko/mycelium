//! loom concurrency-model-checker SPIKE — the exactly-once init guard.
//!
//! ── What this models ────────────────────────────────────────────────────────
//! The `spawned: AtomicBool` **exactly-once init guard** used in production by
//! `mycelium/src/agent/capability_ops.rs` (`FilterOpacityRegistry::spawned`) and
//! consumed at `mycelium/src/agent/capability_handle.rs:265`:
//!
//! ```ignore
//! if !registry.spawned.swap(true, Ordering::AcqRel) {
//!     // ... spawn the ONE consolidated opacity watcher task ...
//! }
//! ```
//!
//! The guard's contract: no matter how many threads race through
//! `declare_requirement` concurrently, **exactly one** must win the flag and run
//! the one-time setup (spawn the background task). Production uses the atomic
//! read-modify-write `swap`; this spike models the equivalent `compare_exchange`
//! claim (both are correct) and contrasts it with the subtly-broken
//! `load`-then-`store` an author reaches for when they forget the check-and-set
//! must be a single atomic step.
//!
//! ── Why ─────────────────────────────────────────────────────────────────────
//! "read, then act on a stale read" is the project's recurring race family — the
//! same class the papaya `compute` retry-safety rule guards against, and the one
//! the calibration ledger keeps re-recording. A stress test surfaces it only by
//! luck: the buggy interleaving is a narrow window. loom explores *every* thread
//! interleaving exhaustively and deterministically, so the bug becomes a
//! guaranteed, reproducible failure with a printed schedule.
//!
//! This is a SELF-CONTAINED SPIKE, not a retrofit. It re-models the pattern with
//! loom's instrumented atomics. Production papaya/tokio code is not
//! loom-instrumentable and is intentionally NOT touched.
//!
//! ── Why a separate crate ────────────────────────────────────────────────────
//! `--cfg loom` is global to a build. tokio gates its `net`/`fs` modules behind
//! `#![cfg(not(loom))]`, so any crate that links tokio (e.g. `mycelium-core`)
//! FAILS to compile under `--cfg loom`. This crate is deliberately tokio-free so
//! loom can actually run. See `mycelium-core/Cargo.toml` for the pointer here.
//!
//! ── How to run ──────────────────────────────────────────────────────────────
//! The whole file is `#![cfg(loom)]`; a normal build/test never compiles it.
//!
//!   # CAS guard is exactly-once — MUST PASS:
//!   RUSTFLAGS="--cfg loom" cargo test -p loom-spike --release \
//!       --test once_guard once_guard_cas_is_exactly_once
//!
//!   # broken load-then-store — MUST FAIL under loom (it prints the interleaving
//!   # where both threads load `false` and both store → 2 winners). `#[ignore]`d
//!   # so the default loom run stays green; run it explicitly to SEE the catch:
//!   RUSTFLAGS="--cfg loom" cargo test -p loom-spike --release \
//!       --test once_guard once_guard_load_then_store_RACES -- --ignored
//!
//! Exploration here is tiny (2 threads, 1–2 atomics); bound it if ever needed
//! with `LOOM_MAX_PREEMPTIONS=3`.
#![cfg(loom)]

use loom::sync::Arc;
use loom::sync::atomic::Ordering::{AcqRel, Acquire, Release};
use loom::sync::atomic::{AtomicBool, AtomicUsize};

/// CORRECT: two threads race to claim the guard via a single atomic
/// `compare_exchange(false → true)`. Exactly one observes `Ok` and wins the
/// one-time init; the loser sees `Err(true)`. loom explores every interleaving
/// and the invariant holds in all of them, so this test PASSES.
#[test]
fn once_guard_cas_is_exactly_once() {
    loom::model(|| {
        let guard = Arc::new(AtomicBool::new(false));

        let g1 = Arc::clone(&guard);
        let t1 = loom::thread::spawn(move || g1.compare_exchange(false, true, AcqRel, Acquire).is_ok());

        let g2 = Arc::clone(&guard);
        let t2 = loom::thread::spawn(move || g2.compare_exchange(false, true, AcqRel, Acquire).is_ok());

        let won1 = t1.join().unwrap();
        let won2 = t2.join().unwrap();

        // XOR: exactly one thread won the init — never zero, never both.
        assert!(
            won1 ^ won2,
            "exactly-once violated: won1={won1}, won2={won2} (expected exactly one winner)"
        );
        // And the flag is durably latched for everyone who follows.
        assert!(guard.load(Acquire), "guard must be latched true after init");
    });
}

/// BROKEN: the same guard as a non-atomic `load`-then-`store` —
/// `if !guard.load() { guard.store(true); winner += 1 }`. A stress test would
/// pass almost always. loom finds the interleaving where BOTH threads load
/// `false` before either stores, so BOTH run the "one-time" init and
/// `winners == 2`, tripping the assert and printing the offending schedule.
///
/// `#[ignore]`d so the default loom run stays green.
// run to SEE loom catch the race:
//   RUSTFLAGS="--cfg loom" cargo test -p loom-spike --release \
//       --test once_guard once_guard_load_then_store_RACES -- --ignored
#[test]
#[ignore]
#[allow(non_snake_case)]
fn once_guard_load_then_store_RACES() {
    loom::model(|| {
        let guard = Arc::new(AtomicBool::new(false));
        let winners = Arc::new(AtomicUsize::new(0));

        let claim = {
            let guard = Arc::clone(&guard);
            let winners = Arc::clone(&winners);
            move || {
                // NON-atomic check-then-act: the whole bug.
                if !guard.load(Acquire) {
                    guard.store(true, Release);
                    winners.fetch_add(1, AcqRel);
                }
            }
        };

        let c1 = claim.clone();
        let t1 = loom::thread::spawn(c1);
        let t2 = loom::thread::spawn(claim);

        t1.join().unwrap();
        t2.join().unwrap();

        // Fails under the both-load-false interleaving loom is guaranteed to
        // discover: winners == 2, not 1.
        assert_eq!(
            winners.load(Acquire),
            1,
            "load-then-store is not exactly-once: two threads both ran the init"
        );
    });
}
