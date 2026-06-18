//! M15 — **the selection step**: resolve a requirement against a catalog of *installable*
//! artifacts and pick which to pull, rather than pulling by hard-coded name (the OBR-resolver
//! role OSGi has and the live `resolve` does not — it matches against the *running* set).
//!
//! ## One hop, not a constraint solver (design contract)
//!
//! This resolves only **install-time artifact selection**: which artifact's *declared-provide*
//! satisfies a requirement. It deliberately does **not** compute a transitive install closure —
//! *service/capability* dependencies (skill A calls skill B) are **runtime-mesh-resolved** by the
//! live resolver, re-resolved on every relevant change, never frozen into a deployment set. A
//! component imports the mesh (see the WIT world), so anything that is a *call* bottoms out at the
//! mesh, not here. Going deeper would link-time-bind what the mesh already resolves at call time.
//! This is the architecture declining OSGi's NP-hard part because the mesh dissolved it.
//!
//! The match itself is the **same `CapFilter::matches`** the live resolver uses — pointed at the
//! `Capability` an artifact *would* provide once installed, instead of a live `cap/` entry.

use mycelium::{CapFilter, Capability};

use crate::artifact::ArtifactId;

/// One installable artifact in the catalog: the [`Capability`] it would provide once installed,
/// plus the content address ([`ArtifactId`]) to pull. The declared-provide is a full `Capability`
/// so the live resolver's `CapFilter::matches` works unchanged against not-yet-installed artifacts.
// `Capability` is `PartialEq` but not `Eq`, so neither is this.
#[derive(Clone, Debug, PartialEq)]
pub struct InstallableEntry {
    /// What this artifact would advertise once installed (the resolver-matchable declared-provide).
    pub provides:          Capability,
    /// Content address of the artifact bytes to pull (hand to `WasmHost::provision`).
    pub artifact:          ArtifactId,
    /// Optional ranking hints (0 = unknown): bytes to pull, estimated install seconds.
    pub size_bytes:        u64,
    pub est_install_secs:  u64,
}

impl InstallableEntry {
    /// A catalog entry for `provides`, pulled from `artifact`.
    pub fn new(provides: Capability, artifact: ArtifactId) -> Self {
        Self { provides, artifact, size_bytes: 0, est_install_secs: 0 }
    }

    /// Attach ranking hints used by [`InstallableCatalog::resolve_best`].
    pub fn with_cost(mut self, size_bytes: u64, est_install_secs: u64) -> Self {
        self.size_bytes = size_bytes;
        self.est_install_secs = est_install_secs;
        self
    }
}

/// A catalog of installable artifacts to resolve requirements against.
///
/// v0 is an in-memory set (an embedder populates it, or it is built from the cluster's gossiped
/// `installable` entries — that population path is the integration follow-up). The resolver logic
/// is identical regardless of how the catalog was filled.
#[derive(Default, Clone)]
pub struct InstallableCatalog {
    entries: Vec<InstallableEntry>,
}

impl InstallableCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a catalog entry.
    pub fn add(&mut self, entry: InstallableEntry) {
        self.entries.push(entry);
    }

    /// All catalog entries.
    pub fn entries(&self) -> &[InstallableEntry] {
        &self.entries
    }

    /// **The M15 selection step.** Every catalog entry whose declared-provide matches `filter` —
    /// the same `CapFilter::matches` the live resolver uses, pointed at not-yet-installed artifacts.
    pub fn resolve(&self, filter: &CapFilter) -> Vec<&InstallableEntry> {
        self.entries.iter().filter(|e| filter.matches(&e.provides)).collect()
    }

    /// Resolve, then pick the single best candidate to pull: lowest `size_bytes`, then lowest
    /// `est_install_secs` (cheapest to bring live). `None` if nothing satisfies the requirement.
    /// One-shot "what should this node pull to satisfy `filter`?".
    pub fn resolve_best(&self, filter: &CapFilter) -> Option<&InstallableEntry> {
        self.resolve(filter)
            .into_iter()
            .min_by_key(|e| (e.size_bytes, e.est_install_secs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cap(ns: &str, name: &str) -> Capability {
        Capability::new(ns, name)
    }
    fn art(seed: &[u8]) -> ArtifactId {
        ArtifactId::of(seed)
    }

    #[test]
    fn resolve_matches_declared_provides_by_filter() {
        let mut cat = InstallableCatalog::new();
        cat.add(InstallableEntry::new(cap("llm", "summarize"), art(b"summarizer.wasm")));
        cat.add(InstallableEntry::new(cap("vision", "detect"), art(b"detector.wasm")));

        let hits = cat.resolve(&CapFilter::new("llm", "summarize"));
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].provides.name.as_ref(), "summarize");

        // A requirement nothing in the catalog provides resolves to empty (loop simply won't fire).
        assert!(cat.resolve(&CapFilter::new("audio", "transcribe")).is_empty());
    }

    #[test]
    fn resolve_best_picks_the_cheapest_candidate() {
        let mut cat = InstallableCatalog::new();
        cat.add(InstallableEntry::new(cap("llm", "summarize"), art(b"big")).with_cost(9_000, 120));
        cat.add(InstallableEntry::new(cap("llm", "summarize"), art(b"small")).with_cost(1_000, 30));
        cat.add(InstallableEntry::new(cap("llm", "summarize"), art(b"mid")).with_cost(1_000, 90));

        let best = cat.resolve_best(&CapFilter::new("llm", "summarize")).unwrap();
        assert_eq!(best.artifact, art(b"small"), "lowest size, then lowest est time");

        assert!(cat.resolve_best(&CapFilter::new("nope", "nope")).is_none());
    }
}
