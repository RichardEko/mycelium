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

use mycelium::{CapFilter, Capability, KvHandle};

use crate::artifact::ArtifactId;

/// KV prefix owned by the cluster-wide installable catalog: `installable/{ns}/{name}/{artifact-hex}`.
/// Entries are gossiped like any KV value, so any node can publish an installable artifact and every
/// provisioner can build its catalog from the cluster view.
pub const INSTALLABLE_PREFIX: &str = "installable/";

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

    /// Serialize for gossip: `[32B artifact][8B size LE][8B est LE][Capability bytes]`. Built on
    /// the public `Capability::encode` (no internal codec), so it stays a pure public-API consumer.
    pub fn encode(&self) -> Vec<u8> {
        let cap = self.provides.encode();
        let mut out = Vec::with_capacity(48 + cap.len());
        out.extend_from_slice(self.artifact.as_bytes());
        out.extend_from_slice(&self.size_bytes.to_le_bytes());
        out.extend_from_slice(&self.est_install_secs.to_le_bytes());
        out.extend_from_slice(&cap);
        out
    }

    /// Inverse of [`encode`](Self::encode).
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 48 {
            return None;
        }
        let mut id = [0u8; 32];
        id.copy_from_slice(&bytes[0..32]);
        let size_bytes = u64::from_le_bytes(bytes[32..40].try_into().ok()?);
        let est_install_secs = u64::from_le_bytes(bytes[40..48].try_into().ok()?);
        let provides = Capability::decode(&bytes[48..])?;
        Some(Self { provides, artifact: ArtifactId::from_bytes(id), size_bytes, est_install_secs })
    }

    /// The KV key this entry is published under (`installable/{ns}/{name}/{artifact-hex}`).
    pub fn kv_key(&self) -> String {
        format!(
            "{INSTALLABLE_PREFIX}{}/{}/{}",
            self.provides.namespace,
            self.provides.name,
            self.artifact.to_hex()
        )
    }
}

/// Publish an installable artifact to the cluster-wide catalog (gossiped KV). Any node may do this;
/// every provisioner that builds its catalog via [`InstallableCatalog::from_kv`] then sees it.
pub fn publish_installable(kv: &KvHandle, entry: &InstallableEntry) -> bool {
    kv.set(entry.kv_key(), entry.encode())
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

    /// Build a catalog from the cluster's gossiped installable entries (the `installable/` KV
    /// prefix). This is the real, cluster-wide catalog: it reflects whatever any node has published
    /// via [`publish_installable`], decoded from the local gossip view.
    pub fn from_kv(kv: &KvHandle) -> Self {
        let mut cat = Self::new();
        for (_key, bytes) in kv.scan_prefix(INSTALLABLE_PREFIX) {
            if let Some(entry) = InstallableEntry::decode(&bytes) {
                cat.add(entry);
            }
        }
        cat
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
    fn entry_encode_decode_round_trips() {
        let e = InstallableEntry::new(cap("llm", "summarize"), art(b"x.wasm")).with_cost(4096, 42);
        let back = InstallableEntry::decode(&e.encode()).expect("decode");
        assert_eq!(back.provides.namespace.as_ref(), "llm");
        assert_eq!(back.provides.name.as_ref(), "summarize");
        assert_eq!(back.artifact, e.artifact);
        assert_eq!(back.size_bytes, 4096);
        assert_eq!(back.est_install_secs, 42);
        assert!(InstallableEntry::decode(b"too short").is_none());
    }

    #[tokio::test]
    async fn from_kv_builds_the_catalog_from_gossiped_entries() {
        use mycelium::{GossipAgent, GossipConfig, NodeId};
        use std::sync::Arc;

        let agent = {
            let mut a = None;
            for _ in 0..16 {
                let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
                let id = NodeId::new("127.0.0.1", port).unwrap();
                let cfg = GossipConfig { bind_port: port, ..Default::default() };
                let agent = Arc::new(GossipAgent::new(id, cfg));
                if agent.start().await.is_ok() {
                    a = Some(agent);
                    break;
                }
            }
            a.expect("bind")
        };

        // Publish two installable artifacts to the cluster catalog.
        super::publish_installable(
            &agent.kv(),
            &InstallableEntry::new(cap("llm", "summarize"), art(b"sum")).with_cost(10, 1),
        );
        super::publish_installable(
            &agent.kv(),
            &InstallableEntry::new(cap("vision", "detect"), art(b"det")),
        );

        // Build the catalog from the gossiped KV view and resolve against it.
        let cat = InstallableCatalog::from_kv(&agent.kv());
        assert_eq!(cat.entries().len(), 2);
        let hit = cat.resolve_best(&CapFilter::new("llm", "summarize")).expect("resolved");
        assert_eq!(hit.artifact, art(b"sum"));
        assert_eq!(hit.size_bytes, 10);
        assert!(cat.resolve(&CapFilter::new("audio", "x")).is_empty());

        agent.shutdown().await;
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
