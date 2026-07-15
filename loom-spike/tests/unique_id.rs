//! loom concurrency-model-checker SPIKE — the unique-ID allocator.
//!
//! ── What this models ────────────────────────────────────────────────────────
//! The monotonic `fetch_add` **unique-ID allocator** used in production to hand
//! every predicate-watcher registration a distinct id. Two live call sites in
//! `mycelium-core`:
//!
//! ```ignore
//! // mycelium-core/src/ops.rs:237  (register_prefix_predicate_watcher)
//! let id = ctx.kv_state.next_pred_watcher_id.fetch_add(1, Ordering::Relaxed);
//! ctx.kv_state.prefix_predicate_watchers.pin().insert(id, entry);
//!
//! // mycelium-core/src/kv_handle.rs:183  (KvHandle::watch_prefix_predicate)
//! let id = self.ctx.kv_state.next_pred_watcher_id.fetch_add(1, Ordering::Relaxed);
//! ```
//!
//! The allocator's contract: no matter how many threads register watchers
//! concurrently, **every id is unique** — two watchers must never collide on the
//! same `papaya` map key (a collision silently overwrites one watcher's channel).
//! Production uses the atomic read-modify-write `fetch_add`; this spike contrasts
//! it with the subtly-broken non-atomic `load`-then-`store` an author reaches for
//! when they forget the increment must be a single atomic step.
//!
//! ── Why ─────────────────────────────────────────────────────────────────────
//! "read the counter, then act on the stale read" is the project's recurring race
//! family — the same class the papaya `compute` retry-safety rule guards against,
//! and the one the calibration ledger keeps re-recording. A stress test surfaces a
//! dup only by luck: the buggy interleaving is a narrow window. loom explores
//! *every* thread interleaving exhaustively and deterministically, so the
//! collision becomes a guaranteed, reproducible failure with a printed schedule.
//!
//! This is a SELF-CONTAINED SPIKE, not a retrofit. It re-models the pattern with
//! loom's instrumented atomics; production papaya/tokio code is not
//! loom-instrumentable and is intentionally NOT touched. See `once_guard.rs` for
//! the sibling model and the "why a separate crate" note.
//!
//! ── How to run ──────────────────────────────────────────────────────────────
//! The whole file is `#![cfg(loom)]`; a normal build/test never compiles it.
//!
//!   # fetch_add allocator hands out unique ids — MUST PASS:
//!   RUSTFLAGS="--cfg loom" cargo test -p loom-spike --release \
//!       --test unique_id unique_id_fetch_add_is_unique
//!
//!   # broken load-then-store — MUST FAIL under loom (it prints the interleaving
//!   # where two threads both load N and both return N → duplicate id). `#[ignore]`d
//!   # so the default loom run stays green; run it explicitly to SEE the catch:
//!   RUSTFLAGS="--cfg loom" cargo test -p loom-spike --release \
//!       --test unique_id unique_id_load_then_store_COLLIDES -- --ignored
//!
//! Exploration here is tiny (2–3 threads, 1 counter); bound it if ever needed
//! with `LOOM_MAX_PREEMPTIONS=3`.
#![cfg(loom)]

use std::collections::HashSet;

use loom::sync::Arc;
use loom::sync::atomic::AtomicUsize;
use loom::sync::atomic::Ordering::Relaxed;

/// CORRECT: three threads race to claim an id via a single atomic
/// `fetch_add(1)`. Each read-modify-write is indivisible, so every thread
/// observes and returns a distinct prior value. loom explores every
/// interleaving; the ids are unique in all of them, so this test PASSES.
#[test]
fn unique_id_fetch_add_is_unique() {
    loom::model(|| {
        const N: usize = 3;
        let next = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..N)
            .map(|_| {
                let next = Arc::clone(&next);
                // The production allocation: one atomic read-modify-write.
                loom::thread::spawn(move || next.fetch_add(1, Relaxed))
            })
            .collect();

        let ids: HashSet<usize> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // No two threads received the same id in ANY interleaving loom explored.
        assert_eq!(
            ids.len(),
            N,
            "id allocator handed out a duplicate: got ids {ids:?} from {N} threads"
        );
    });
}

/// BROKEN: the same allocator as a non-atomic `load`-then-`store` —
/// `let id = next.load(); next.store(id + 1); id`. A stress test would pass
/// almost always. loom finds the interleaving where BOTH threads load the same
/// value before either stores, so BOTH return that id and the set collapses to
/// one element — tripping the assert and printing the offending schedule.
///
/// `#[ignore]`d so the default loom run stays green.
// run to SEE loom catch the collision:
//   RUSTFLAGS="--cfg loom" cargo test -p loom-spike --release \
//       --test unique_id unique_id_load_then_store_COLLIDES -- --ignored
#[test]
#[ignore]
#[allow(non_snake_case)]
fn unique_id_load_then_store_COLLIDES() {
    loom::model(|| {
        const N: usize = 2;
        let next = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..N)
            .map(|_| {
                let next = Arc::clone(&next);
                loom::thread::spawn(move || {
                    // NON-atomic read-modify-write: the whole bug. The window
                    // between load and store is where a second thread reads the
                    // same value.
                    let id = next.load(Relaxed);
                    next.store(id + 1, Relaxed);
                    id
                })
            })
            .collect();

        let ids: HashSet<usize> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Fails under the both-load-same interleaving loom is guaranteed to
        // discover: the two ids collide, so the set holds only 1 element.
        assert_eq!(
            ids.len(),
            N,
            "load-then-store is not a unique allocator: two threads got the same id"
        );
    });
}
