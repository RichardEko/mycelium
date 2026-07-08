//! # mycelium-reason — the Tier-3 reasoning differentiators on Mycelium's public API
//!
//! LLM-authoring DX companion for Mycelium (v3.0). The strategy and its code-verified
//! bindings live in `docs/plans/mycelium-reason.md`; this crate is the **build** tier —
//! the three wedges nothing without the mesh can offer:
//!
//! - **① Capability-routed inference** ([`InferenceRouter`], [`serve_model`]) — route each
//!   call to a healthy model-serving node with no central proxy. Capability *resolution*
//!   is load-blind, so this is a real routing layer: resolve → drop opaque nodes →
//!   rank by pheromone fill → fail over down the candidate list.
//! - **② Fleet-reasoning traces** ([`TraceRecorder`], [`replay`], [`narrate`]) — causal,
//!   HLC-ordered, gossip-replicated records of why the whole fleet reasoned as it did,
//!   replayable from any node; optionally anchored into the WS2 audit chain
//!   (`compliance` feature).
//! - **③ Artifact-aware resume** ([`require_model`]) — a resumed graph's model
//!   dependencies follow it: declare the requirement, structurally await a provider,
//!   surface install progress. Demand half only; the install half is `model_deploy`.
//!
//! Plus the **content-addressed blob tier** ([`FsBlobStore`], [`MeshBlobStore`],
//! [`spawn_blob_server`]) and gateway routes ([`reason_router`], feature `gateway`) that
//! the Python LangGraph checkpointer consumes: metadata gossips in KV, payloads stay in
//! the blob tier and are fetched (verified) from whichever peer holds them.
//!
//! ## The substrate-native frame
//!
//! The same coordinator-free properties that make *coordination* resilient make
//! *reasoning* resilient: inference routed with no central proxy, tamper-evidenced causal
//! traces of the fleet's thinking, and threads whose model dependencies follow them
//! across nodes. A single-process framework structurally cannot offer these; this crate
//! composes all of them from Mycelium's public API alone (the companion-crate contract).
//!
//! ## The model-is-a-prompt-skill convention
//!
//! A served model **is a prompt skill**: capability `llm/{model-id}` via
//! `register_prompt_skill` (the `model_deploy` precedent), plus a parallel *attributed*
//! metadata ad `llm-meta/{model-id}` (ctx window, family, extras) — parallel because
//! re-advertising the same `(node, ns, name)` with attributes would LWW-churn against
//! the skill's own persist task. Both retract together via [`ModelReg`].
//!
//! ## Namespaces claimed
//!
//! - capabilities: `llm-meta/*` (model metadata ads) · `reason/blob-cache` (blob providers)
//! - RPC kinds: `reason.blob.fetch`
//! - log streams: `reason/*` — one substream per writer (KV keys
//!   `log/reason/{run_id}/{node}/…`; a shared stream would collide same-millisecond
//!   HLCs across writers — see `trace`'s module doc)

mod blob;
#[cfg(feature = "gateway")]
mod http;
mod resume;
mod route;
mod trace;

pub use blob::{
    BLOB_FETCH_KIND, BlobId, BlobServerHandle, FsBlobStore, MAX_BLOB_BYTES, MeshBlobStore,
    spawn_blob_server,
};
#[cfg(feature = "gateway")]
pub use http::reason_router;
pub use resume::{ModelDependency, ResumeError, require_model};
#[cfg(feature = "llm")]
pub use route::{ModelProfile, ModelReg, serve_model};
pub use route::{InferenceRouter, ModelQuery, RouteError, Routed, RouterConfig};
pub use trace::{TraceEvent, TraceRecorder, narrate, replay};
