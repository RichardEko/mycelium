//! The curator's periodic **lint** (Phase 3) — the *group function* that keeps the corpus healthy,
//! generalising the repo's own `/wiki-lint`. Two tiers:
//! - [`structural_lint`] — always on, deterministic, no LLM: **dead cross-links** and **empty
//!   sections**, computed purely from the pages the curator already reads.
//! - [`SemanticLinter`] (impl behind feature `llm`) — cross-section **self-consistency**: the UC1 org
//!   twin must not assert contradictory facts in different sections. An LLM pass over the corpus.
//!
//! Findings are **advisory** — the curator surfaces them ([`crate::Wiki::last_lint`] + a warn log); it
//! never deletes or rewrites on their basis. This is Mycelium's *detection, not prevention* stance
//! applied to meaning: a group function reports drift, it does not silently "fix" the group's words.

use crate::model::{Page, SectionId};

/// What a lint finding is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LintKind {
    /// A `[[target]]` reference to a page (or `page#section`) that does not exist in the corpus.
    DeadCrossLink,
    /// A section whose body is blank — a curated corpus should not carry empty sections.
    EmptySection,
    /// (LLM) two sections assert contradictory facts.
    SemanticConflict,
}

/// One lint finding — advisory, located to a page (and section, when it is section-local).
#[derive(Debug, Clone)]
pub struct LintFinding {
    pub kind:    LintKind,
    pub page:    String,
    pub section: Option<SectionId>,
    pub detail:  String,
}

/// The result of a lint pass.
#[derive(Debug, Clone, Default)]
pub struct LintReport {
    pub findings: Vec<LintFinding>,
}

impl LintReport {
    pub fn is_clean(&self) -> bool { self.findings.is_empty() }
    pub fn len(&self) -> usize { self.findings.len() }
    pub fn is_empty(&self) -> bool { self.findings.is_empty() }
    /// Findings of one kind.
    pub fn of_kind(&self, kind: LintKind) -> impl Iterator<Item = &LintFinding> {
        self.findings.iter().filter(move |f| f.kind == kind)
    }
}

/// Extract `[[target]]` cross-link targets from a body. A target is `page/path` or
/// `page/path#sectionid` (the wiki's own `[[…]]` convention). Whitespace around the target is trimmed.
fn cross_links(body: &str) -> impl Iterator<Item = &str> {
    body.split("[[").skip(1).filter_map(|rest| rest.split_once("]]").map(|(t, _)| t.trim()))
}

/// The deterministic structural pass: dead cross-links + empty sections, over the whole corpus. Pure —
/// no store, no LLM, no clock — so it is fully unit-testable and cheap to run every tick.
pub fn structural_lint(pages: &[Page]) -> LintReport {
    use std::collections::{BTreeMap, BTreeSet};

    // Corpus index: page path → the set of section ids that exist on it (for anchor checks).
    let mut index: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for p in pages {
        let ids = index.entry(p.path.as_str()).or_default();
        for s in &p.sections { ids.insert(s.id.as_ref()); }
    }

    let mut findings = Vec::new();
    for p in pages {
        for s in &p.sections {
            if s.body.trim().is_empty() {
                findings.push(LintFinding {
                    kind: LintKind::EmptySection, page: p.path.clone(), section: Some(s.id.clone()),
                    detail: format!("section {:?} has an empty body", s.heading),
                });
            }
            for link in cross_links(&s.body) {
                let (tgt_page, tgt_sec) = match link.split_once('#') {
                    Some((pg, sec)) => (pg, Some(sec)),
                    None            => (link, None),
                };
                let resolves = match index.get(tgt_page) {
                    None      => false,
                    Some(ids) => tgt_sec.is_none_or(|sec| ids.contains(sec)),
                };
                if !resolves {
                    findings.push(LintFinding {
                        kind: LintKind::DeadCrossLink, page: p.path.clone(), section: Some(s.id.clone()),
                        detail: format!("dead cross-link [[{link}]]"),
                    });
                }
            }
        }
    }
    LintReport { findings }
}

/// The optional **semantic** lint pass — cross-section self-consistency. A trait (dyn-safe, like
/// [`crate::Reconciler`]) so the [`crate::Wiki`] holds `Option<Box<dyn SemanticLinter>>` regardless of
/// whether the `llm` feature is on; the only implementation, [`LlmSemanticLinter`], is `llm`-gated.
#[async_trait::async_trait]
pub trait SemanticLinter: Send + Sync {
    /// Inspect the whole corpus for cross-section contradictions. A pass that cannot run (e.g. LLM
    /// outage) returns an empty vec — lint is best-effort, never blocking.
    async fn lint(&self, pages: &[Page]) -> Vec<LintFinding>;
}

/// A cross-section self-consistency check via a [`mycelium::LlmBackend`]: render the corpus, ask the
/// model for contradictions, one finding per reported line. A backend error yields no findings — a lint
/// outage is silent, never a failure (detection is advisory).
#[cfg(feature = "llm")]
pub struct LlmSemanticLinter {
    backend:    std::sync::Arc<dyn mycelium::LlmBackend>,
    max_tokens: u32,
}

#[cfg(feature = "llm")]
impl LlmSemanticLinter {
    pub fn new(backend: std::sync::Arc<dyn mycelium::LlmBackend>) -> Self {
        Self { backend, max_tokens: 1024 }
    }
    pub fn with_max_tokens(mut self, n: u32) -> Self { self.max_tokens = n; self }

    const SYSTEM: &'static str = "You are auditing a group's shared wiki for self-consistency. You are \
        given every section. Report only pairs of sections that assert CONTRADICTORY facts — not \
        stylistic differences, not incompleteness. Output one contradiction per line as \
        `page-a §id-a vs page-b §id-b: <what contradicts>`. If there are none, output exactly NONE.";

    fn render_corpus(pages: &[Page]) -> String {
        let mut s = String::new();
        for p in pages {
            for sec in &p.sections {
                s.push_str(&format!("[{} §{}] {}\n{}\n\n", p.path, sec.id, sec.heading, sec.body));
            }
        }
        s
    }
}

#[cfg(feature = "llm")]
#[async_trait::async_trait]
impl SemanticLinter for LlmSemanticLinter {
    async fn lint(&self, pages: &[Page]) -> Vec<LintFinding> {
        let corpus = Self::render_corpus(pages);
        match self.backend.complete(Self::SYSTEM, &corpus, self.max_tokens, 0.0).await {
            Ok(r) => r.output.lines()
                .map(str::trim)
                .filter(|l| !l.is_empty() && !l.eq_ignore_ascii_case("none"))
                .map(|l| LintFinding {
                    kind: LintKind::SemanticConflict, page: String::new(), section: None,
                    detail: l.to_string(),
                })
                .collect(),
            Err(e) => {
                tracing::warn!(error = %e, "wiki: semantic lint failed — skipped this pass");
                Vec::new()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use crate::model::Section;

    fn page(path: &str, sections: Vec<Section>) -> Page {
        Page { path: path.into(), attributes: BTreeMap::new(), sections }
    }
    fn sec(id: &str, body: &str) -> Section {
        Section { id: id.into(), heading: "H".into(), body: body.into(), attributes: BTreeMap::new() }
    }

    #[test]
    fn dead_cross_link_is_flagged() {
        let pages = vec![page("guide/intro", vec![sec("s1", "see [[guide/missing]] for more")])];
        let r = structural_lint(&pages);
        assert_eq!(r.of_kind(LintKind::DeadCrossLink).count(), 1);
    }

    #[test]
    fn resolving_cross_link_and_anchor_are_clean() {
        let pages = vec![
            page("guide/intro", vec![sec("s1", "see [[guide/adv]] and [[guide/adv#s2]]")]),
            page("guide/adv",   vec![sec("s2", "advanced")]),
        ];
        let r = structural_lint(&pages);
        assert!(r.of_kind(LintKind::DeadCrossLink).next().is_none(), "both links resolve");
    }

    #[test]
    fn dead_anchor_on_a_real_page_is_flagged() {
        let pages = vec![
            page("guide/intro", vec![sec("s1", "see [[guide/adv#nope]]")]),
            page("guide/adv",   vec![sec("s2", "advanced")]),
        ];
        let r = structural_lint(&pages);
        assert_eq!(r.of_kind(LintKind::DeadCrossLink).count(), 1, "page exists but the anchor does not");
    }

    #[test]
    fn empty_section_is_flagged() {
        let pages = vec![page("p", vec![sec("s1", "   \n  ")])];
        let r = structural_lint(&pages);
        assert_eq!(r.of_kind(LintKind::EmptySection).count(), 1);
    }

    #[test]
    fn a_clean_corpus_reports_nothing() {
        let pages = vec![page("p", vec![sec("s1", "content, no links")])];
        assert!(structural_lint(&pages).is_clean());
    }

    // The LLM semantic pass is wired to a real backend: EchoBackend echoes the corpus, so a non-empty
    // corpus yields at least one reported "contradiction" — proving the backend is consulted and its
    // output parsed into findings. `--features llm`.
    #[cfg(feature = "llm")]
    #[tokio::test]
    async fn semantic_linter_consults_the_backend() {
        use std::sync::Arc;
        let linter = LlmSemanticLinter::new(Arc::new(mycelium::EchoBackend));
        let pages = vec![page("p", vec![sec("s1", "the sky is green")])];
        let findings = linter.lint(&pages).await;
        assert!(!findings.is_empty(), "the backend was consulted and its lines parsed");
        assert!(findings.iter().all(|f| f.kind == LintKind::SemanticConflict));
    }
}
