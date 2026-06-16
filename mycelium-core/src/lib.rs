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

pub mod config;
pub mod connection;
pub mod context;
pub mod error;
pub mod framing;
pub mod hlc;
pub mod kv_persist;
pub mod locality;
pub mod node_id;
pub mod ops;
pub mod persistence;
pub mod schema_handle;
pub mod seen;
pub mod signal;
pub mod store;
pub mod stream;
pub mod tls;
pub mod writer;

// Root-level re-exports of the substrate's primary types, so `crate::NodeId`,
// `crate::GossipConfig`, `crate::CoreCtx`, … resolve both inside this crate and
// (via `mycelium`'s re-exports) unchanged in the full crate.
pub use config::GossipConfig;
pub use context::{CoreCtx, ReplyInterceptor};
pub use framing::MAX_FRAME_BYTES;
pub use node_id::NodeId;
pub use schema_handle::{SchemaError, SchemaHandle, SchemaPublishResult};
pub use store::{QuorumObserver, QuorumTrackerList};
