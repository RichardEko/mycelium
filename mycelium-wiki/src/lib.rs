//! # mycelium-wiki — a group-scoped, LLM-curated wiki (control plane / data plane)
//!
//! The **maintained-meaning / authoritative-specific** knowledge canon for a group of agents — the
//! durable, curated sibling of the tuple space's ephemeral pull and the blackboard's competitive
//! claim. It **composes** with an external metrics/structure store (Postgres) and RAG (background),
//! joined by a shared id namespace — it is the *specific/authoritative* layer, not a replacement for
//! either.
//!
//! **Architecture (control plane / data plane).** The corpus is **not** in gossiped KV. It lives in a
//! **node-independent, pluggable store** (a shared filesystem dir / S3 bucket / doc store — see the
//! [`WikiStore`] trait). A group node runs a **curator** service that serialises writes (single writer
//! of record, so concurrent same-section edits need no CRDT), runs the LLM ingest/lint, and **brokers
//! access** (store location + a scoped read grant → group membership is the gate); group agents
//! **read the store directly, in parallel**. Mycelium is the control plane — curator election +
//! ring-failover, the store-location advertisement, the small evaporating proposal queue in KV, and
//! the MCP tool — never the storage. This is the wiki pattern's native shape (files + an LLM curator +
//! direct reads, as Mycelium's own `docs/wiki/` works). Plan + design:
//! `docs/plans/mycelium-wiki.md`, `docs/design/wiki-concurrent-edit.md`.
//!
//! ## Phase 1 (this release) — the data plane
//!
//! The pluggable backing store, deliberately **Mycelium-agnostic** (the store is plain infrastructure;
//! the control plane arrives in Phase 2 behind the `control-plane` feature):
//! - the [`WikiStore`] trait — `read` / `query` / `write_page` / `list_pages` / `location`;
//! - the record model — [`Section`] (heading + body + join-key/scope [`Section::attributes`]),
//!   [`Manifest`] (order, written last), [`Page`], [`Predicate`] (structured attribute filter, not
//!   similarity search), and stable opaque [`mint_section_id`];
//! - [`FsStore`] — a filesystem-directory reference implementation (atomic per-object writes,
//!   manifest-last for torn-read safety, manifest-authoritative reads). An `S3Store` is a parallel impl.

mod model;
mod store;
mod fs;
#[cfg(feature = "control-plane")]
mod agent;

pub use model::{
    mint_section_id, Manifest, Page, Predicate, Section, SectionId, SectionRef, WikiError,
};
pub use store::WikiStore;
pub use fs::FsStore;

/// The Mycelium **control plane** (Phase 2) — the curator role, election + ring-failover, the
/// evaporating proposal queue, and the single-writer apply. Behind the `control-plane` feature so the
/// data plane above stays Mycelium-agnostic.
#[cfg(feature = "control-plane")]
pub use agent::{Wiki, WikiConfig, WikiRole};
