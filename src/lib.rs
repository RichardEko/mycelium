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
//!
//! ## KV namespace ownership
//!
//! The KV store is the single substrate; higher layers own dedicated key
//! prefixes. Higher layers write directly to their prefix via
//! `make_gossip_update` + `apply_and_notify`. This is intentional and not a
//! layer violation: ownership is documented, encoding is shared, and no
//! foreign writer ever touches another layer's prefix.
//!
//! | Prefix                              | Owner / purpose                                              |
//! |-------------------------------------|--------------------------------------------------------------|
//! | `grp/{group}/{node}`                | Signal Mesh — group membership                               |
//! | `sys/load/{node}/{kind}`            | Signal Mesh — opacity (load + auto-opacity composition)      |
//! | `sys/load/{node}/req/{ns}/{name}`   | Phase 3 requirement opacity (composes via `is_self_opaque`)  |
//! | `sys/load/{node}/group-req/{g}/{i}` | Group-requirement opacity; written by the emergent-group membership task when a `CapabilityGroupDef::requires` filter is unsatisfied |
//! | `sys/quorum/{kind}/{sender}`        | Persistent quorum evidence                                   |
//! | `sys/topology-override/{group}`     | Consensus — operator escape hatch (value: `b"true"`)         |
//! | `consensus/committed/{slot}`        | Consensus — committed slot state                             |
//! | `consensus/ballot/{slot}`           | Consensus — ballot tracking                                  |
//! | `consensus/trust/{group}/{node}`    | Consensus — trust slices                                     |
//! | `cap/{node}/{ns}/{name}`            | Node-level capability advertisements                         |
//! | `cap/{node}/locality/self`          | Locality (also a capability — single namespace, single shape)|
//! | `req/{node}/{ns}/{name}`            | Node-level requirement declarations                          |
//! | `cap-group/{group}`                 | Emergent capability-group definitions                        |
//! | `gcap/{group}/{ns}/{name}/{contrib}`| Group-level capability projections                           |
//! | `mailbox/{target}/{kind}/{hlc_hex}` | Service Patterns — event mailbox entries (value: `sender_len(2LE) | sender_bytes | payload`) |
//! | `tools/{name}/{node}`              | Layer IV MCP tool registrations (value: JSON Schema bytes)   |
//! | `agent/{node}/state`               | Layer V agent state machine — current state string (gossips to mesh) |
//! | `agent/{node}/policy`              | Layer V serialised AgentPolicy (readable by monitors/supervisors) |
//! | `agent/{node}/task/{id}/turn`      | Layer V turn counter for `max_turns` enforcement              |
//! | `agent/{node}/task/{id}/calls`     | Layer V tool-call counter for `tool_budget` enforcement       |
//! | `agent/{node}/provision/{item}/error` | Last provisioning failure — written by the **application** provisioning handler, not the substrate |
//! | `sys/identity/{node}`              | mTLS — 32-byte Ed25519 verifying key; written at startup by TLS-enabled nodes |
//! | `cap/{node}/llm/inference`         | LLM backend capability (model, context, backend, endpoint attrs) |
//! | `cap/{node}/llm/installable`       | LLM models that can be pulled (model, size_gb, est_mins attrs) |
//! | `cap/{node}/llm/loading`           | LLM model pull in progress (model, progress 0–100 attrs)     |
//! | `cap/{node}/{ns}/installable`      | Any dynamically provisionable software capability             |
//! | `cap/{node}/{ns}/loading`          | Provisioning in progress; `progress` attr 0–100              |
//!
//! Layer-III writes that read or write KV (consensus engine,
//! `sys/topology-override` reads) are documented at their call sites as
//! deliberate escape hatches, not layer violations — the consensus engine
//! owns the `consensus/` prefix and reads `sys/topology-override` as a
//! policy input, both of which are explicitly part of its namespace
//! contract.
//!
//! ## Durability contract
//!
//! Gossip is the only replication mechanism; there is no quorum acknowledgement.
//! For a key to survive a full-cluster restart, **at least one node that holds
//! the key must have `PersistenceConfig` set** so the WAL survives process exit.
//! Nodes without persistence recover via anti-entropy from live peers; they
//! cannot contribute to full-cluster recovery.
//!
//! Capability and soft-state keys (`cap/`, `sys/load/`, `grp/`, `req/`) are
//! **not** WAL-persisted by design — they regenerate via `advertise_capability`
//! within seconds of reconnection. Hard-state application keys (`test/`,
//! `agent/`, `consensus/`, `tools/`) should be written via `set_async` on at
//! least one persistent node per write if restart durability is required.

#![forbid(unsafe_code)]

pub mod capability;
pub mod capability_config;
pub mod config;
pub mod error;
pub mod mesh_manifest;
pub mod signal;

mod agent;
mod connection;
mod consensus;
mod framing;
mod hlc;
mod locality;
mod node_id;
mod persistence;
mod seen;
mod store;
mod stream;
mod tls;
mod writer;

pub use agent::{
    AgentPolicy, ExecutionState, AgentStateMachine, PolicyViolation,
    BulkError, BulkServeHandle,
    GossipAgent, MailboxHandle, McpClientHandle, McpError, McpToolHandle,
    MeshEvent, RpcError, RpcRequest, RpcRequestRx, ScatterError, ScatterResult, SystemStats,
    AckResult, ConsistencyError, LockGuard, LogEntry, QuorumError, ShardError,
};
pub use capability::{
    CallerContext, CapConstraint, CapFilter, CapRanking, CapValue, Capability, CapabilityEvent,
    CapabilityGroupDef, CapabilityGroupHandle, CapabilityHandle,
    DemandStatus, RankingOrder, RequirementHandle, RequirementStatus,
    WiredEmitOutcome, WiringProvider, WiringStatus,
};
pub use capability_config::{
    CapabilityProbeEntry, NodeCapabilityConfig, ProbeEvent, ProbeState,
    TomlCapValue, run_capability_probes,
};
pub use mesh_manifest::{
    GroupManifest, GroupStatus, MeshManifest, MeshMeta, MeshStatus,
    manifest_keys, semver_gt,
};
pub use config::{GossipConfig, GroupTopologyPolicy, PersistenceConfig, SyncMode, TlsConfig, TopologyEnforcement};
pub use locality::LocalityPreference;
pub use consensus::{ConsensusConfig, ConsensusHandle, ConsensusResult, consensus_kind, consensus_ns};
pub use error::GossipError;
pub use node_id::NodeId;
pub use signal::{
    AdvertiseHandle, OpacityHandle, OpacityHint, OpacityState,
    Signal, SignalScope, WatchHandle, signal_kind, kv_ns,
};

/// Re-exports for the cargo-fuzz harness under `fuzz/`. Gated by the
/// `fuzz-internals` cargo feature so normal builds do not widen the
/// public API. The functions here wrap internal `pub(crate)` decoders
/// (`WireMessage`, `Capability::decode`, …) into `&[u8] -> _` calls that
/// fuzz targets can hammer directly.
///
/// **Not stable.** Any item here can move or change shape between
/// patch releases; if you depend on these from outside `fuzz/`, expect
/// breakage.
#[cfg(feature = "fuzz-internals")]
pub mod fuzz_internals {
    use bincode::serde::decode_from_slice;
    use bytes::Bytes;

    /// Attempts to decode `data` as a `WireMessage` using the same bincode
    /// configuration as the live decoder. Returns whether decoding succeeded;
    /// the actual message is discarded.
    pub fn wire_message_decode(data: &[u8]) -> bool {
        let cfg = crate::framing::bincode_cfg();
        decode_from_slice::<crate::framing::WireMessage, _>(data, cfg).is_ok()
    }

    pub fn capability_decode(bytes: &[u8]) -> bool {
        crate::Capability::decode(bytes).is_some()
    }
    pub fn cap_filter_decode(bytes: &[u8]) -> bool {
        crate::CapFilter::decode(bytes).is_some()
    }
    pub fn capability_group_def_decode(bytes: &[u8]) -> bool {
        crate::CapabilityGroupDef::decode(bytes).is_some()
    }
    pub fn locality_path_decode(bytes: &[u8]) -> bool {
        crate::locality::LocalityPath::decode(bytes).is_some()
    }
    pub fn load_state_decode(bytes: &[u8]) -> bool {
        let b = Bytes::copy_from_slice(bytes);
        crate::signal::decode_load_state(&b).is_some()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────


#[cfg(test)]
mod lib_tests;
