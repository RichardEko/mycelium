//! loom concurrency-model-checker SPIKE — bounded append to a published list.
//!
//! ── What this models ────────────────────────────────────────────────────────
//! The **event-driven fan-out activation**: a newly-learned peer is appended to
//! the published forwarding set (a `tokio::sync::watch` in production — modeled
//! here as a lock-protected slot, which is what a watch channel is internally).
//! The live sites in `mycelium-core`:
//!
//! ```ignore
//! // mycelium-core/src/connection.rs — TCP Ping arm, sender_is_new
//! // mycelium-core/src/swim.rs       — ApplyEffect::BecameAlive
//! peer_list_tx.send_if_modified(|current| {           // ONE lock hold: read,
//!     if current.contains(&sender) { return false; }  // decide, and write are
//!     /* bounded append */                            // a single atomic RMW
//! });
//! ```
//!
//! The original code was the two-lock-holds version:
//!
//! ```ignore
//! let current = peer_list_tx.borrow().clone();        // lock #1: read
//! if !current.contains(&sender) {
//!     let mut next = current.to_vec();
//!     next.push(sender);
//!     let _ = peer_list_tx.send(next.into());         // lock #2: write
//! }
//! ```
//!
//! ── Why ─────────────────────────────────────────────────────────────────────
//! Two connection handlers race through the append when two peers dial a fresh
//! seed at once — its normal startup. With the read and the write under separate
//! lock holds, both handlers can snapshot the same list and the second `send`
//! silently overwrites the first's peer: a **lost update** that left the losing
//! peer unsendable until the health monitor's first reconcile (found live,
//! 2026-07-21 — the mailbox_llm cold-start investigation; the calibration
//! ledger's "act on a stale read" family, on a watch channel instead of papaya).
//!
//! ── Does loom actually catch the broken version? (honest outcome) ───────────
//! YES. loom explores the schedule where both threads clone the empty snapshot
//! before either writes, so the `#[ignore]`d twin below FAILS with a printed
//! schedule — in-repo proof the model covers this call-site pattern. Had this
//! model existed before 2026-07-21, the original `borrow()+send()` shape would
//! have been checkable the day it was written. (The *cap-sizing* logic bug fixed
//! the same day is NOT modeled here — it was an omission, deterministic in every
//! schedule; model checking cannot find logic that is missing.)
//!
//! ── How to run ──────────────────────────────────────────────────────────────
//! The whole file is `#![cfg(loom)]`; a normal build/test never compiles it.
//!
//!   # send_if_modified shape — MUST PASS (no schedule loses an append):
//!   RUSTFLAGS="--cfg loom" cargo test -p loom-spike --release \
//!       --test bounded_append append_single_hold_never_loses_a_peer
//!
//!   # borrow-then-send shape — MUST FAIL under loom (prints the lost-update
//!   # schedule). `#[ignore]`d so the default loom run stays green:
//!   RUSTFLAGS="--cfg loom" cargo test -p loom-spike --release \
//!       --test bounded_append append_borrow_then_send_LOSES_UPDATE -- --ignored
#![cfg(loom)]

use loom::sync::{Arc, Mutex};

/// CORRECT — the `send_if_modified` shape: read-decide-write in ONE lock hold.
/// Two threads each append their own id; every schedule must end with both
/// present. loom passes this in all interleavings.
#[test]
fn append_single_hold_never_loses_a_peer() {
    loom::model(|| {
        let slot: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));

        let handles: Vec<_> = [1usize, 2usize]
            .into_iter()
            .map(|me| {
                let slot = Arc::clone(&slot);
                loom::thread::spawn(move || {
                    let mut cur = slot.lock().unwrap();
                    if !cur.contains(&me) {
                        cur.push(me);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        let final_list = slot.lock().unwrap();
        assert!(
            final_list.contains(&1) && final_list.contains(&2),
            "single-hold append lost a peer: {final_list:?}"
        );
    });
}

/// BROKEN — the original `borrow()` + `send()` shape: snapshot under one lock
/// hold, write back under another. loom finds the schedule where both threads
/// snapshot the empty list and the second write clobbers the first — the exact
/// production lost-update. `#[ignore]`d: run explicitly to SEE the catch.
#[test]
#[ignore]
#[allow(non_snake_case)]
fn append_borrow_then_send_LOSES_UPDATE() {
    loom::model(|| {
        let slot: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));

        let handles: Vec<_> = [1usize, 2usize]
            .into_iter()
            .map(|me| {
                let slot = Arc::clone(&slot);
                loom::thread::spawn(move || {
                    let snapshot = slot.lock().unwrap().clone(); // borrow()
                    if !snapshot.contains(&me) {
                        let mut next = snapshot;
                        next.push(me);
                        *slot.lock().unwrap() = next; // send()
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        let final_list = slot.lock().unwrap();
        assert!(
            final_list.contains(&1) && final_list.contains(&2),
            "borrow-then-send lost a peer: {final_list:?}"
        );
    });
}
