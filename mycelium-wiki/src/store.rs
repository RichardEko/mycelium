//! The **data-plane interface** — the pluggable backing store a group's wiki lives in. Deliberately
//! substrate-agnostic: an `FsStore` (this crate), an `S3Store`, or a doc-store all implement it, and
//! the Mycelium control plane (Phase 2) drives whichever the group is configured with.
//!
//! **Concurrency contract:** the store does not serialise writers itself — that is the *curator's*
//! job (exactly one writer of record per group). [`write_page`](WikiStore::write_page) must therefore
//! only assume a single concurrent writer, but it **must** stay torn-read-safe for concurrent
//! *readers*: write section objects first and the manifest last, so a reader entering via the
//! manifest never observes a half-applied edit.

use std::collections::BTreeMap;

use crate::model::{Page, Predicate, Section, SectionRef, WikiError};

/// A group's wiki backing store. All methods are `&self` — a store handle is shared; a curator holds
/// the write side, readers hold read handles to the same underlying store.
pub trait WikiStore: Send + Sync {
    /// A stable locator for this store (a path / bucket-prefix / URI) — what the curator advertises so
    /// group agents can reach it directly for reads.
    fn location(&self) -> String;

    /// Read a page: its manifest joined with the live section bodies it references, in render order.
    /// `None` if the page has no manifest. Sections present on the store but **not** referenced by the
    /// manifest are invisible here (that is what makes a torn multi-section write unobservable).
    fn read(&self, page: &str) -> Result<Option<Page>, WikiError>;

    /// Find sections matching `predicate` across all pages (structured attribute filter — the
    /// scope/id query, not similarity search). Only manifest-referenced sections are considered.
    fn query(&self, predicate: &Predicate) -> Result<Vec<SectionRef>, WikiError>;

    /// **Curator write** (single writer of record): replace a page with `sections` + page
    /// `attributes`. Writes the section objects first and the manifest **last**; removes sections no
    /// longer referenced. A brand-new page is created; an existing one is fully replaced.
    fn write_page(
        &self, page: &str, sections: &[Section], attributes: &BTreeMap<String, String>,
    ) -> Result<(), WikiError>;

    /// List the paths of all pages that currently have a manifest.
    fn list_pages(&self) -> Result<Vec<String>, WikiError>;
}
