//! SPIKE crate — see `tests/once_guard.rs` for the loom model. This lib is
//! intentionally empty; the crate exists only to host the loom integration test
//! in a tokio-free dependency tree (tokio's `#![cfg(not(loom))]` gating makes it
//! impossible to run loom inside any crate that links tokio, e.g. mycelium-core).
