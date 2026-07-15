//! loom concurrency-model-checker SPIKE — publish-then-observe (release/acquire).
//!
//! ── What this models ────────────────────────────────────────────────────────
//! The **publish-a-value-then-flag-it-ready** handshake used in production so a
//! reader that observes the "ready" flag is guaranteed to also observe the data
//! published before it. The live pair in `mycelium-core` / `mycelium`:
//!
//! ```ignore
//! // WRITER — mycelium-core/src/kv_persist.rs:57  (run_kv_persist_task, first tick)
//! apply_and_notify(&ctx.kv_state, &update);                       // publish the state
//! ctx.soft_state_advertised.store(true, Ordering::Release);       // ...then flag it ready
//!
//! // READER — src/agent/introspect.rs:125  (Node::is_ready)
//! self.task_ctx.soft_state_advertised.load(Ordering::Acquire)     // sees flag ⇒ sees state
//! ```
//!
//! The contract: once a caller observes `is_ready() == true`, the initial
//! advertisement is guaranteed already applied — a `Release` store paired with an
//! `Acquire` load establishes the happens-before edge that carries the data write
//! with it. This spike models a data cell + ready flag and contrasts the correct
//! `Release`/`Acquire` pairing with the all-`Relaxed` version that drops the edge.
//!
//! ── Why ─────────────────────────────────────────────────────────────────────
//! Missing-ordering bugs are invisible on x86 (strongly ordered) and to stress
//! tests, but real on weak-memory hardware and under compiler reordering. loom
//! models the C11 weak-memory relation directly, so it can expose a reader that
//! sees the flag set while still reading a *stale* data cell — the exact hazard
//! the `Release`/`Acquire` pair exists to prevent.
//!
//! ── Does loom actually catch the relaxed version? (honest outcome) ───────────
//! YES. Under `Relaxed`/`Relaxed`, loom's memory model admits the execution where
//! the reader observes `flag == true` but still loads the *old* data value, so the
//! `#[ignore]`d broken test below FAILS with a printed schedule — concrete, in-repo
//! proof loom surfaces the missing-ordering bug, not just the missing-CAS one.
//!
//! This is a SELF-CONTAINED SPIKE, not a retrofit. See `once_guard.rs` for the
//! sibling model and the "why a separate crate" note.
//!
//! ── How to run ──────────────────────────────────────────────────────────────
//! The whole file is `#![cfg(loom)]`; a normal build/test never compiles it.
//!
//!   # Release/Acquire handshake — MUST PASS (reader that sees the flag sees data):
//!   RUSTFLAGS="--cfg loom" cargo test -p loom-spike --release \
//!       --test publish publish_release_acquire_observes_data
//!
//!   # all-Relaxed — MUST FAIL under loom (it prints the weak-memory execution
//!   # where the flag is seen but the data is stale). `#[ignore]`d so the default
//!   # loom run stays green; run it explicitly to SEE the catch:
//!   RUSTFLAGS="--cfg loom" cargo test -p loom-spike --release \
//!       --test publish publish_relaxed_MISSES_ORDERING -- --ignored
//!
//! Exploration here is tiny (2 threads, 2 atomics); bound it if ever needed
//! with `LOOM_MAX_PREEMPTIONS=3`.
#![cfg(loom)]

use loom::sync::Arc;
use loom::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use loom::sync::atomic::{AtomicBool, AtomicUsize};

/// The sentinel a reader sees when it looked before the writer flagged ready.
const UNPUBLISHED: usize = 0;
/// The value the writer publishes before flagging ready.
const PUBLISHED: usize = 42;

/// CORRECT: writer publishes `data` then `flag.store(true, Release)`; reader
/// does `flag.load(Acquire)` and, if it sees `true`, reads `data`. The
/// Release/Acquire pair establishes happens-before, so any reader that observes
/// the flag is guaranteed to observe the published data. loom explores every
/// interleaving AND weak-memory execution; the invariant holds in all of them,
/// so this test PASSES.
#[test]
fn publish_release_acquire_observes_data() {
    loom::model(|| {
        let data = Arc::new(AtomicUsize::new(UNPUBLISHED));
        let flag = Arc::new(AtomicBool::new(false));

        let (wd, wf) = (Arc::clone(&data), Arc::clone(&flag));
        let writer = loom::thread::spawn(move || {
            wd.store(PUBLISHED, Relaxed); // publish the value...
            wf.store(true, Release); // ...then flag it ready (the release edge)
        });

        let (rd, rf) = (Arc::clone(&data), Arc::clone(&flag));
        let reader = loom::thread::spawn(move || {
            // If we see the flag (acquire edge), we MUST see the published data.
            if rf.load(Acquire) {
                Some(rd.load(Relaxed))
            } else {
                None
            }
        });

        writer.join().unwrap();
        let observed = reader.join().unwrap();

        // Seeing the flag ready but a stale data cell would violate the
        // happens-before the Release/Acquire pair guarantees.
        if let Some(v) = observed {
            assert_eq!(
                v, PUBLISHED,
                "reader saw flag=ready but data was stale ({v}) — release/acquire edge lost"
            );
        }
    });
}

/// BROKEN: the same handshake with `Relaxed` on BOTH the flag store and the flag
/// load. Relaxed carries no ordering, so the data write and the flag write may be
/// observed out of order. loom's weak-memory model finds the execution where the
/// reader observes `flag == true` while still reading the OLD data cell
/// (`UNPUBLISHED`), tripping the assert and printing the offending schedule.
///
/// Confirmed: loom DOES surface this — see the module "honest outcome" note.
/// `#[ignore]`d so the default loom run stays green.
// run to SEE loom catch the missing ordering:
//   RUSTFLAGS="--cfg loom" cargo test -p loom-spike --release \
//       --test publish publish_relaxed_MISSES_ORDERING -- --ignored
#[test]
#[ignore]
#[allow(non_snake_case)]
fn publish_relaxed_MISSES_ORDERING() {
    loom::model(|| {
        let data = Arc::new(AtomicUsize::new(UNPUBLISHED));
        let flag = Arc::new(AtomicBool::new(false));

        let (wd, wf) = (Arc::clone(&data), Arc::clone(&flag));
        let writer = loom::thread::spawn(move || {
            wd.store(PUBLISHED, Relaxed);
            wf.store(true, Relaxed); // NO release: the edge is gone.
        });

        let (rd, rf) = (Arc::clone(&data), Arc::clone(&flag));
        let reader = loom::thread::spawn(move || {
            if rf.load(Relaxed) {
                // NO acquire.
                Some(rd.load(Relaxed))
            } else {
                None
            }
        });

        writer.join().unwrap();
        let observed = reader.join().unwrap();

        // Fails under the reorder loom discovers: flag seen true, data still stale.
        if let Some(v) = observed {
            assert_eq!(
                v, PUBLISHED,
                "relaxed publish is not ordered: reader saw flag=ready with stale data ({v})"
            );
        }
    });
}
