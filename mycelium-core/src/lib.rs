//! # mycelium-core — the Mycelium substrate (Layers I + II)
//!
//! This crate carries the broker-less substrate: the gossip transport, the
//! last-write-wins KV store (HLC-ordered, anti-entropy synced), and the
//! signal/boundary mesh. It has **no concept of agreement, coordination, or
//! workflow** — those (consensus, capabilities, services, gateway) live in the
//! full [`mycelium`](https://docs.rs/mycelium) crate, which depends on this one.
//!
//! The split (ROADMAP §v2.0 M1) lets bare-metal / embedded callers depend on the
//! substrate without pulling in the Layer III+ dependency tree, and draws the crate
//! boundary at the documented II↔III seam. The substrate never references a
//! higher-layer type (the inverted-dependency invariant — see `docs/philosophy.html`).
//!
//! Extraction is staged (`docs/plans/v2-m1-mycelium-core.md`); this is Stage 3a —
//! the leaf modules. Subsequent stages move the interdependent transport cluster.

pub mod error;
pub mod hlc;
pub mod seen;
