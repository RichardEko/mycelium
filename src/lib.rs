//! # mycelium — gossip substrate for adaptive AI agent systems
//!
//! An embedded, broker-less library that provides two primitives:
//!
//! - **Layer 1 — KV store**: epidemic last-write-wins state propagation over TCP.
//!   Every agent holds a eventually-consistent view of the full cluster's key-value state.
//! - **Layer 2 — Signal mesh**: ephemeral scoped events that flood the cluster epidemically.
//!   Each agent holds a local [`Boundary`](signal::Boundary) (its receptor set) that decides
//!   whether it *acts* on an incoming signal — forwarding is always unconditional.
//!
//! Higher layers build Actor/Event systems, async RPC, and MCP AI tool routing on top.
//! Each agent chooses its own payload serialisation; the substrate routes by signal `kind`
//! string and carries opaque [`bytes::Bytes`].
//!
//! ## Quick start
//!
//! ```rust,no_run
//! use mycelium::{GossipAgent, GossipConfig, NodeId, SignalScope, signal_kind};
//! use bytes::Bytes;
//! use std::{sync::Arc, time::Duration};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let node_id = NodeId::new("127.0.0.1", 7946)?;
//!     let mut config = GossipConfig::default();
//!     config.bootstrap_peers = vec![NodeId::new("127.0.0.1", 7947)?];
//!
//!     let agent = Arc::new(GossipAgent::new(node_id, config));
//!     agent.start().await?;
//!
//!     // Layer 1 — KV state
//!     agent.set("load/self", Bytes::from_static(b"queue=0"));
//!     let val = agent.get("load/self");
//!
//!     // Layer 2 — signals
//!     agent.join_group("nlp");
//!     agent.emit(signal_kind::INVOKE, SignalScope::Group("nlp".into()), Bytes::new());
//!
//!     agent.shutdown().await;
//!     Ok(())
//! }
//! ```
//!
//! See [`GossipAgent`] for the full API. See [`GossipConfig`] for all tunable parameters.
//! See [ROADMAP.md](https://github.com/RichardEko/mycelium/blob/main/ROADMAP.md) for the
//! layer-by-layer architecture and higher-layer design.

#![forbid(unsafe_code)]

pub mod capability;
pub mod config;
pub mod error;
pub mod signal;

mod agent;
mod connection;
mod consensus;
mod framing;
mod hlc;
mod locality;
mod node_id;
mod seen;
mod store;
mod writer;

pub use agent::{GossipAgent, SystemStats};
pub use capability::{
    CapConstraint, CapFilter, CapRanking, CapValue, Capability, CapabilityEvent,
    CapabilityGroupDef, CapabilityGroupHandle, CapabilityHandle,
    DemandStatus, RankingOrder, RequirementHandle, RequirementStatus,
    WiredEmitOutcome, WiringProvider, WiringStatus,
};
pub use config::{GossipConfig, GroupTopologyPolicy, TopologyEnforcement};
pub use locality::LocalityPreference;
pub use consensus::{ConsensusConfig, ConsensusHandle, ConsensusResult, consensus_kind, consensus_ns};
pub use error::GossipError;
pub use node_id::NodeId;
pub use signal::{
    AdvertiseHandle, OpacityHandle, OpacityHint, OpacityState,
    Signal, SignalScope, WatchHandle, signal_kind, kv_ns,
};

// ─── Tests ────────────────────────────────────────────────────────────────────


#[cfg(test)]
mod lib_tests;
