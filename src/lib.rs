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
//!     config.bind_port = 7946;   // the agent listens on `bind_port`, not the NodeId's port — set both
//!     config.bootstrap_peers = vec![NodeId::new("127.0.0.1", 7947)?];
//!
//!     let agent = Arc::new(GossipAgent::new(node_id, config));
//!     agent.start().await?;
//!
//!     // Layer 1 — KV state
//!     let _ = agent.kv().set("load/self", Bytes::from_static(b"queue=0"));
//!     let val = agent.kv().get("load/self");
//!
//!     // Layer 2 — signals
//!     agent.mesh().join_group("nlp");
//!     agent.mesh().emit(signal_kind::INVOKE, SignalScope::Group("nlp".into()), Bytes::new());
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
//! **Building a use case on top of Mycelium?** Start with
//! [Building on Mycelium](https://github.com/RichardEko/mycelium/blob/main/docs/guide/building-on-mycelium.md)
//! — the integrator contract (dependency, public-API-only rule, reserved KV prefixes, the
//! invariants to respect, and a copyable `CLAUDE.md` snippet), then the
//! [FAQ](https://github.com/RichardEko/mycelium/blob/main/docs/guide/faq.md).
//!
//! ## Crate layout — `mycelium` vs `mycelium-core`
//!
//! This crate is the **full runtime** (Layers I + II + III: gossip KV, signal mesh,
//! consensus, capabilities, services, the HTTP/MCP/A2A gateway, and TLS/RBAC/audit) and is
//! what most users want. The Layers I + II substrate alone — gossip KV + signal/boundary mesh,
//! with no Axum/gateway and roughly a third of the dependency tree — lives in the separate
//! [`mycelium-core`](https://crates.io/crates/mycelium-core) crate, which `mycelium`
//! re-exports and depends on. Depend on `mycelium-core` directly only for a minimal embed that
//! needs last-write-wins KV propagation and the scoped event mesh but not RPC, consensus, the
//! capability system, or the gateway. The crate boundary makes the inverted-dependency
//! invariant a compile-time guarantee (the substrate cannot reference the layers above it).
//! You can also trim *this* crate toward the core with `default-features = false` (drops the
//! gateway) and `--features gateway` without `consensus` (drops the agreement layer); a
//! consensus-disabled node still forwards PROPOSE/VOTE/COMMIT, it just never acts. The split
//! landed in v2.0 M1 — see [ROADMAP.md](https://github.com/RichardEko/mycelium/blob/main/ROADMAP.md)
//! §v2.0 Milestones for the rationale.
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
//! | `consensus/lease/{slot}`            | Consensus — epoch-lease window (u64 LE ms); written when `ConsensusConfig::committed_lease_secs` is set; expiry is evaluated read-side |
//! | `consensus/trust/{group}/{node}`    | Consensus — trust slices                                     |
//! | `cap/{node}/{ns}/{name}`            | Node-level capability advertisements                         |
//! | `cap/{node}/locality/self`          | Locality (also a capability — single namespace, single shape)|
//! | `req/{node}/{ns}/{name}`            | Node-level requirement declarations                          |
//! | `cap-group/{group}`                 | Emergent capability-group definitions                        |
//! | `gcap/{group}/{ns}/{name}/{contrib}`| Group-level capability projections                           |
//! | `mailbox/{target}/{kind}/{hlc_hex}` | Service Patterns — event mailbox entries (value: `sender_len(2LE) | sender_bytes | payload`) |
//! | `schemas/{schema_id}`              | Schema registry — authoritative JSON Schema bytes for a capability `schema_id`; written via `publish_schema`; gossip-propagated and WAL-persisted |
//! | `tools/{name}/{node}`              | Layer IV MCP tool registrations (value: JSON Schema bytes)   |
//! | `agent/{node}/state`               | Layer V agent state machine — current state string (gossips to mesh) |
//! | `agent/{node}/policy`              | Layer V serialised AgentPolicy (readable by monitors/supervisors) |
//! | `agent/{node}/task/{id}/turn`      | Layer V turn counter for `max_turns` enforcement              |
//! | `agent/{node}/task/{id}/calls`     | Layer V tool-call counter for `tool_budget` enforcement       |
//! | `agent/{node}/provision/{item}/error` | Last provisioning failure — written by the **application** provisioning handler, not the substrate |
//! | `sys/identity/{node}`              | mTLS — 32-byte Ed25519 verifying key; written at startup by TLS-enabled nodes |
//! | `cap/{node}/llm/inference`         | LLM backend capability (model, context, backend, endpoint attrs) |
//! | `cap/{node}/llm/installable`       | LLM models that can be pulled (model, size_gb, est_mins attrs) |
//! | `cap/{node}/llm/loading`           | LLM model pull in progress; the shipped provisioner writes a `pct` (0–100) attr (the `llm_agent` example's *simulated* pull uses `progress`) |
//! | `cap/{node}/{ns}/installable`      | Any dynamically provisionable software capability             |
//! | `cap/{node}/{ns}/loading`          | Provisioning in progress; `pct` attr 0–100, written by `mycelium-wasm-host`'s `Provisioner` (read by `mycelium-reason`'s `ModelDependency::loading_progress`) |
//! | `svc/{kind}/{node}`                | Persistent capability advertisements (`advertise_persistent`; tombstoned on handle drop) |
//! | `log/{stream}/{hlc_hex}`           | Append-only KV log entries (`KvHandle::append`)              |
//! | `clog/{…}`                         | Consumer positions for group log subscription (`subscribe_log_group`) |
//! | `lock/{name}`                      | Distributed lock state (JSON holder/token/expiry; tombstoned on `LockGuard` drop) |
//! | `prompts/{ns}/{name}`              | LLM prompt templates (`llm` feature; configuration — the `cap/` entry is the presence heartbeat) |
//! | `skills/{ns}/{name}/{node}/input\|output` | SkillRunner skill registrations (signal-kind routing for `skill.invoke`) |
//! | `installable/{ns}/{name}/{hex}`    | `mycelium-wasm-host` — the artifact catalogue (encoded `InstallableEntry`: kind, content address, cost hints, signed resource requirements) |
//! | `comp/{node}/{ns}/…`               | `mycelium-wasm-host` — confined component KV (a WASM guest's scoped subtree; the host's enforcement point) |
//! | `wiki/{group}/proposal/{id}`       | `mycelium-wiki` companion — evaporating edit proposals (drained by the curator) |
//! | `tuple/inflight/{ns}/{id}`         | `mycelium-tuple-space` companion — advisory in-flight claim (JSON value; expiry is read-side, swept by the primary) |
//! | `sys/tuple/{node}/{ns}/…`          | `mycelium-tuple-space` companion — monitoring counters, role, and the backpressure pheromone (`…/pressure/{stage}`) |
//! | `cap/{node}/llm/{model}`           | `mycelium-reason` companion — a served model *is a prompt skill* (`serve_model` → `register_prompt_skill`); the presence cap for `llm/{model}` (template in `prompts/`) |
//! | `cap/{node}/llm-meta/{model}`      | `mycelium-reason` companion — the parallel **attributed** model-metadata ad (`ctx_window`, `family`, extras); separate cap so attribute updates don't LWW-churn the skill's own persist task |
//! | `cap/{node}/reason/blob-cache`     | `mycelium-reason` companion — this node serves content-addressed blobs (RPC `reason.blob.fetch`); `MeshBlobStore` discovers providers via this cap |
//! | `log/reason/{run_id}/{node}/{hlc}` | `mycelium-reason` companion — fleet-reasoning trace substreams (one per writer; a shared stream would collide same-ms HLC keys — `TraceRecorder`/`replay`) |
//! | `ckpt/{thread}/{ns}/{id}`          | `langgraph-checkpoint-mycelium` — LangGraph checkpoint **index** rows (metadata inline; payloads in the blob tier). Written by the Python saver via the gateway KV endpoint |
//! | `ckptw/{thread}/{ns}/{id}/{task}/{idx}` | `langgraph-checkpoint-mycelium` — LangGraph pending-write index rows (one blob per write) |
//!
//! Layer-III writes that read or write KV (consensus engine,
//! `sys/topology-override` reads) are documented at their call sites as
//! deliberate escape hatches, not layer violations — the consensus engine
//! owns the `consensus/` prefix and reads `sys/topology-override` as a
//! policy input, both of which are explicitly part of its namespace
//! contract.
//!
//! **Ownership is promise-strength, not mechanism-strength.** The substrate
//! does not enforce this table: any node can write any key, and LWW will
//! accept it. Higher layers' invariants (e.g. "committed slots are
//! commit-once") are therefore exactly as strong as every node's compliance
//! with this contract — by design, since teaching Layer I to enforce a
//! higher layer's law would invert the dependency that makes it the
//! foundation. Violations are made *legible* instead: the consensus
//! listener's commit-conflict tripwire ([`SystemStats::commit_conflicts`])
//! detects and refuses to endorse conflicting commits.
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
//!
//! ## Speech act patterns (FIPA-ACL → Mycelium)
//!
//! The table below maps the seven FIPA-ACL performatives to idiomatic Mycelium
//! primitives. Use this as a vocabulary bridge when porting FIPA-based or A2A
//! interaction protocols to the gossip substrate.
//!
//! | FIPA-ACL performative      | Mycelium primitive                                                          | Notes                                                                                      |
//! |----------------------------|-----------------------------------------------------------------------------|--------------------------------------------------------------------------------------------|
//! | `INFORM`                   | [`emit`] / [`emit_ordered`]                                                 | Epidemic broadcast; no acknowledgement. `emit_ordered` adds causal HLC sequencing.        |
//! | `REQUEST`                  | [`rpc_call`] / [`rpc_respond`]                                              | Point-to-point call with correlation nonce; awaits reply or timeout.                       |
//! | `QUERY-IF` / `QUERY-REF`   | [`resolve`] / [`watch_capabilities`]                                        | Snapshot or live-streaming capability satisfaction check.                                  |
//! | `PROPOSE`                  | [`group_propose`] / [`system_propose`] / [`cross_group_propose`]            | Epidemic two-phase voting; `GroupQuorum` controls the acceptance fraction per group.       |
//! | `AGREE` / `REFUSE`         | [`rpc_respond`] or [`emit`] with `SignalScope::Individual`                  | Point-to-point reply to a specific RPC request or correlation nonce.                       |
//! | `SUBSCRIBE`                | [`signal_rx`] / [`signal_rx_from`] / [`watch_capabilities`]                 | Push channel; `signal_rx_from` restricts delivery to trusted senders.                     |
//! | `CFP` (call-for-proposals) | [`advertise_capability`] + [`declare_requirement`]                          | Providers advertise; consumers declare needs. Emergent groups form without a coordinator.  |
//!
//! [`emit`]: GossipAgent::emit
//! [`emit_ordered`]: GossipAgent::emit_ordered
//! [`rpc_call`]: GossipAgent::rpc_call
//! [`rpc_respond`]: GossipAgent::rpc_respond
//! [`resolve`]: GossipAgent::resolve
//! [`watch_capabilities`]: GossipAgent::watch_capabilities
//! [`group_propose`]: GossipAgent::group_propose
//! [`system_propose`]: GossipAgent::system_propose
//! [`cross_group_propose`]: GossipAgent::cross_group_propose
//! [`signal_rx`]: GossipAgent::signal_rx
//! [`signal_rx_from`]: GossipAgent::signal_rx_from
//! [`advertise_capability`]: GossipAgent::advertise_capability
//! [`declare_requirement`]: GossipAgent::declare_requirement

#![deny(unsafe_code)]
#![warn(clippy::clone_on_ref_ptr)]

pub mod capability;
pub mod capability_config;
pub mod mesh_manifest;

// Layers I+II substrate live in the `mycelium-core` crate (ROADMAP §v2.0 M1, complete).
// Re-exported here so existing `crate::store::…`, `crate::signal::…`, `crate::config::…`,
// `crate::CoreCtx`, etc. keep resolving unchanged across the crate boundary, and the
// public `mycelium::{config, signal, error}` API surface is preserved.
pub use mycelium_core::{config, error, signal};
pub(crate) use mycelium_core::{
    connection, framing, hlc, locality, node_id, persistence, seen, store, stream, tls, writer,
};

mod agent;
pub mod schema_evolution;
#[cfg(feature = "consensus")]
mod consensus;

pub use agent::{
    AgentPolicy, ExecutionState, AgentStateMachine, PolicyViolation,
    BulkError, BulkServeHandle,
    GossipAgent, MailboxHandle, McpError, McpToolHandle, McpHandle,
    MeshEvent, RpcError, RpcRequest, RpcRequestRx, ScatterError, ScatterResult, SystemStats,
    AckResult, CapabilitiesHandle, LogEntry,
    KvHandle, KvQuorumExt, MeshHandle, QuorumError, ServiceHandle, ShardError,
    SchemaError, SchemaHandle, SchemaPublishResult,
};
// Layer III consensus + the consistency overlay built on it (v2 M2 feature gate).
#[cfg(feature = "consensus")]
pub use agent::{ConsensusHandle, ConsistencyError, LockGuard};
// WS-C M9: self-managing-metabolism config tuner + governance.
pub use agent::{accept_all, clamped, reject_all, ConfigPolicy, CONFIG_PREFIX};
pub use agent::{
    GovernIntent, GovernorSnapshot, HotParam, ParamDirective, ParamSnapshot, Ratchet,
    GOVERN_FLEET_KEY, GOVERN_INTENT_TTL_MS,
};
// Elastic group sizing (Track 2a).
pub use agent::{MembershipAction, MembershipIntent, MEMBERSHIP_INTENT_TTL_MS, MEMBERSHIP_PREFIX};
// Legible Emergence — fleet diagnostics as data (localize · explain · diagnose). `GroupStatus`
// stays crate-internal (bare name is the mesh type); reach it via `FleetSnapshot.governed_groups`.
pub use agent::{
    FleetDiagnosis, FleetSnapshot, Finding, Severity, StoreConvergence, ThrottleEdge, ViewConfidence,
};
#[cfg(feature = "gateway")]
pub use agent::McpClientHandle;
#[cfg(feature = "llm")]
pub use agent::{PromptTemplate, PromptSkillError, PromptSkillHandle, LlmBackend, LlmResult, LlmError, OpenAiBackend, EchoBackend, LlmHandle};
#[cfg(feature = "compliance")]
pub use agent::{role_key, RoleClaim, SignedRoleClaim, ROLE_PREFIX};
#[cfg(feature = "compliance")]
pub use agent::{
    audit_key, audit_stream_prefix, verify_chain, verify_chain_keys, verify_stream_from_genesis,
    AuditAction, AuditOutcome, AuditRecord, AuditVerifyError, SignedAuditRecord, AUDIT_PREFIX,
};
#[cfg(feature = "compliance")]
pub use agent::{
    leaf_hash, merkle_root, verify_inclusion, ProofStep, RevocationEvent, SignedRevocation,
    REVOCATION_PREFIX,
};
#[cfg(feature = "compliance")]
pub use agent::OidcConfig;
pub use capability::{
    CallerContext, CapConstraint, CapEntry, CapFilter, CapRanking, CapValue, Capability, CapabilityEvent,
    CapabilityGroupDef, CapabilityGroupHandle, CapabilityReg,
    DemandStatus, RankingOrder, RequirementHandle, RequirementStatus,
    WiredEmitOutcome, WiringProvider, WiringStatus,
};
pub use capability_config::{
    CapabilityProbeEntry, NodeCapabilityConfig, ProbeEvent, ProbeState, TomlCapValue,
};
#[cfg(feature = "gateway")]
pub use capability_config::run_capability_probes;
pub use mesh_manifest::{
    GroupManifest, GroupStatus, MeshManifest, MeshMeta, MeshStatus,
    manifest_keys, semver_gt,
};
pub use config::{EgressPolicy, GatewayToken, GossipConfig, GroupTopologyPolicy, PersistenceConfig, SyncMode, TlsConfig, TopologyEnforcement};
pub use persistence::DataAtRestCipher;
pub use locality::LocalityPreference;
#[cfg(feature = "consensus")]
pub use consensus::{ConsensusConfig, ConsensusListenerHandle, ConsensusResult, GroupQuorum, consensus_kind, consensus_ns};
pub use mycelium_core::error::GossipError;
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
    use bytes::Bytes;

    /// Attempts to decode `data` as a `WireMessage` using the live hand-rolled
    /// codec (`mycelium_core::codec::decode_wire`). Returns whether decoding
    /// succeeded; the actual message is discarded.
    pub fn wire_message_decode(data: &[u8]) -> bool {
        mycelium_core::codec::decode_wire(data).is_ok()
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

/// test-only: a bind-verified, process-unique loopback port allocator for companion
/// integration tests — retires the `free_port` TOCTOU flake class.
#[cfg(feature = "test-util")]
pub use test_util::alloc_port;

// ─── Tests ────────────────────────────────────────────────────────────────────


// Public under `test-util` (feature-gated, test-only) so companion integration tests can call
// `mycelium::test_util::alloc_port()`; a plain module for the core's own `#[cfg(test)]` build.
#[cfg(any(test, feature = "test-util"))]
pub mod test_util;

#[cfg(test)]
mod lib_tests;

#[cfg(test)]
mod swim_oracle_tests;
