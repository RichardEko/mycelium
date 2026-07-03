//! The **curator's reconcile** (Phase 3) — how a batch of proposals for one section is combined with
//! the section's current text into its new curated form. The single-writer curator calls this once per
//! target section per drain (see [`crate::agent`]), so a same-section conflict is resolved by *one*
//! writer holding *all* the proposals — no CRDT, no lost update.
//!
//! Two implementations:
//! - [`DirectReconciler`] (always available) — a deterministic, **lossless append-merge**: no LLM, so
//!   the corpus grows uncurated but nothing a proposer wrote is dropped. This is the honest fallback,
//!   and it is *idempotent* (re-applying an already-merged proposal is a no-op), which is what lets the
//!   curator crash mid-drain and safely re-drain.
//! - [`LlmReconciler`] (feature `llm`) — a real 3-way merge: the LLM curates the **prose** (resolve
//!   conflicts, drop redundancy, keep meaning) while heading/attributes stay code-controlled. On any
//!   backend error it falls back to the append-merge — an LLM outage degrades curation, never writes.

use std::collections::BTreeMap;

use crate::model::{Section, SectionId};

/// One proposed edit to a single section — the reconcile input (the wire proposal, minus its routing).
#[derive(Debug, Clone)]
pub struct ProposalEdit {
    pub heading:    String,
    pub body:       String,
    pub attributes: BTreeMap<String, String>,
    /// The proposing node (provenance; an LLM reconciler may weigh it, the append-merge ignores it).
    pub author:     String,
}

/// The curated result for one section — what the curator writes back.
#[derive(Debug, Clone)]
pub struct Reconciled {
    pub heading:    String,
    pub body:       String,
    pub attributes: BTreeMap<String, String>,
}

/// How the curator merges a batch of proposals for one section. Called **only** by the single-writer
/// curator, one section at a time, with every pending proposal for that section in hand.
#[async_trait::async_trait]
pub trait Reconciler: Send + Sync {
    /// Merge `proposals` (all targeting `section` on `page`) against `current` (the section as it
    /// stands in the store, or `None` for a new section) into its new curated form.
    async fn reconcile(
        &self,
        page:      &str,
        section:   &SectionId,
        current:   Option<&Section>,
        proposals: &[ProposalEdit],
    ) -> Reconciled;
}

// ── heading / attribute merge (structural, shared by both reconcilers) ──────────

/// The new heading: the last proposal's (last-writer-wins), or the current one if the batch is empty.
fn merge_heading(current: Option<&Section>, proposals: &[ProposalEdit]) -> String {
    proposals.last().map(|p| p.heading.clone())
        .or_else(|| current.map(|s| s.heading.clone()))
        .unwrap_or_default()
}

/// Attributes are join-keys / scope tags, not prose — merge them structurally (last proposal wins per
/// key), never via the LLM. Starts from the current section's attributes.
fn merge_attributes(current: Option<&Section>, proposals: &[ProposalEdit]) -> BTreeMap<String, String> {
    let mut attrs = current.map(|s| s.attributes.clone()).unwrap_or_default();
    for p in proposals {
        for (k, v) in &p.attributes { attrs.insert(k.clone(), v.clone()); }
    }
    attrs
}

// ── the always-available fallback ───────────────────────────────────────────────

/// Deterministic, lossless append-merge — the no-LLM curator. Appends each proposal body that is not
/// already present to the current text; **idempotent** (a re-drained proposal is already contained, so
/// nothing is appended twice), which is what makes crash-mid-drain safe.
#[derive(Debug, Clone, Copy, Default)]
pub struct DirectReconciler;

impl DirectReconciler {
    /// The pure merge, exposed so [`LlmReconciler`] can reuse it as its own error fallback.
    fn merge(current: Option<&Section>, proposals: &[ProposalEdit]) -> Reconciled {
        let mut body = current.map(|s| s.body.clone()).unwrap_or_default();
        for p in proposals {
            let piece = p.body.trim();
            // Idempotency + dedup: skip a body already contained (a crash-replayed or duplicate edit).
            if piece.is_empty() || body.contains(piece) { continue; }
            if body.is_empty() { body = piece.to_string(); }
            else               { body.push_str("\n\n"); body.push_str(piece); }
        }
        Reconciled {
            heading:    merge_heading(current, proposals),
            body,
            attributes: merge_attributes(current, proposals),
        }
    }
}

#[async_trait::async_trait]
impl Reconciler for DirectReconciler {
    async fn reconcile(
        &self, _page: &str, _section: &SectionId, current: Option<&Section>, proposals: &[ProposalEdit],
    ) -> Reconciled {
        Self::merge(current, proposals)
    }
}

// ── the LLM curator (feature `llm`) ─────────────────────────────────────────────

/// A real 3-way merge via a [`mycelium::LlmBackend`]. The LLM curates the section **body**; the heading
/// and attributes are merged structurally (code-controlled — the model does not get to invent join
/// keys). Any backend error → [`DirectReconciler`]'s append-merge, so an LLM outage degrades curation
/// but never blocks a write or loses an edit.
#[cfg(feature = "llm")]
pub struct LlmReconciler {
    backend:    std::sync::Arc<dyn mycelium::LlmBackend>,
    max_tokens: u32,
}

#[cfg(feature = "llm")]
impl LlmReconciler {
    pub fn new(backend: std::sync::Arc<dyn mycelium::LlmBackend>) -> Self {
        Self { backend, max_tokens: 2048 }
    }
    pub fn with_max_tokens(mut self, n: u32) -> Self { self.max_tokens = n; self }

    /// The curator's system prompt — merge, don't rewrite; preserve meaning; body only.
    const SYSTEM: &'static str = "You are the curator of a group's shared wiki. You are given the \
        current text of one section and one or more proposed edits to it. Merge the proposals into the \
        current text: resolve conflicts in favour of the most specific and recent information, remove \
        redundancy, and preserve every distinct fact. Do not invent content. Return ONLY the merged \
        section body as plain prose — no preamble, no headings, no commentary.";

    fn user_prompt(section: &SectionId, current: Option<&Section>, proposals: &[ProposalEdit]) -> String {
        let mut s = String::new();
        s.push_str(&format!("SECTION: {section}\n\nCURRENT TEXT:\n"));
        s.push_str(current.map(|c| c.body.as_str()).unwrap_or("(new section — no current text)"));
        s.push_str("\n\nPROPOSED EDITS:\n");
        for (i, p) in proposals.iter().enumerate() {
            s.push_str(&format!("--- proposal {} (from {}) ---\n{}\n", i + 1, p.author, p.body));
        }
        s.push_str("\nReturn the merged body:");
        s
    }
}

#[cfg(feature = "llm")]
#[async_trait::async_trait]
impl Reconciler for LlmReconciler {
    async fn reconcile(
        &self, _page: &str, section: &SectionId, current: Option<&Section>, proposals: &[ProposalEdit],
    ) -> Reconciled {
        let user = Self::user_prompt(section, current, proposals);
        match self.backend.complete(Self::SYSTEM, &user, self.max_tokens, 0.2).await {
            Ok(r) => Reconciled {
                heading:    merge_heading(current, proposals),
                body:       r.output.trim().to_string(),
                attributes: merge_attributes(current, proposals),
            },
            Err(e) => {
                tracing::warn!(section = %section, error = %e, "wiki: LLM reconcile failed — append-merge fallback");
                DirectReconciler::merge(current, proposals)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sec(id: &str, body: &str) -> Section {
        Section { id: id.into(), heading: "H".into(), body: body.into(), attributes: BTreeMap::new() }
    }
    fn edit(body: &str) -> ProposalEdit {
        ProposalEdit { heading: "H".into(), body: body.into(), attributes: BTreeMap::new(), author: "n1".into() }
    }

    #[tokio::test]
    async fn direct_new_section_takes_the_proposal_body() {
        let r = DirectReconciler.reconcile("p", &"s1".into(), None, &[edit("first fact")]).await;
        assert_eq!(r.body, "first fact");
    }

    #[tokio::test]
    async fn direct_appends_distinct_edits_without_loss() {
        let cur = sec("s1", "alpha");
        let r = DirectReconciler
            .reconcile("p", &"s1".into(), Some(&cur), &[edit("beta"), edit("gamma")]).await;
        assert_eq!(r.body, "alpha\n\nbeta\n\ngamma", "every distinct edit is preserved (lossless)");
    }

    #[tokio::test]
    async fn direct_is_idempotent_on_replay() {
        // A crash-replayed proposal (body already in the current text) must not be appended twice.
        let cur = sec("s1", "alpha\n\nbeta");
        let r = DirectReconciler.reconcile("p", &"s1".into(), Some(&cur), &[edit("beta")]).await;
        assert_eq!(r.body, "alpha\n\nbeta", "re-applying a contained edit is a no-op");
    }

    #[tokio::test]
    async fn direct_merges_attributes_last_wins() {
        let mut cur = sec("s1", "x");
        cur.attributes.insert("scope".into(), "old".into());
        let mut e = edit("y");
        e.attributes.insert("scope".into(), "new".into());
        e.attributes.insert("unit".into(), "eng".into());
        let r = DirectReconciler.reconcile("p", &"s1".into(), Some(&cur), &[e]).await;
        assert_eq!(r.attributes.get("scope").map(String::as_str), Some("new"));
        assert_eq!(r.attributes.get("unit").map(String::as_str), Some("eng"));
    }

    // The LLM path is wired to a real backend: with mycelium's EchoBackend (returns "echo: {user}")
    // we prove the curator calls the backend and writes its output as the section body — deterministic,
    // no network. `--features llm`.
    #[cfg(feature = "llm")]
    #[tokio::test]
    async fn llm_reconciler_uses_the_backend_output_as_the_body() {
        use std::sync::Arc;
        let recon = LlmReconciler::new(Arc::new(mycelium::EchoBackend));
        let cur = sec("s1", "current text");
        let out = recon.reconcile("p", &"s1".into(), Some(&cur), &[edit("please merge this")]).await;
        assert!(out.body.starts_with("echo: "), "the backend's completion becomes the body: {}", out.body);
        assert!(out.body.contains("please merge this"), "the proposal reached the prompt");
        assert!(out.body.contains("current text"), "the current text reached the prompt (3-way)");
    }
}
