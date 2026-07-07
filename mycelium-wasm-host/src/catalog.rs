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

use crate::artifact::{ArtifactId, ArtifactKind};

/// KV prefix owned by the cluster-wide installable catalog: `installable/{ns}/{name}/{artifact-hex}`.
/// Entries are gossiped like any KV value, so any node can publish an installable artifact and every
/// provisioner can build its catalog from the cluster view.
pub const INSTALLABLE_PREFIX: &str = "installable/";

/// Format version of the [`InstallableEntry`] encoding. A decoder rejects versions it does not
/// know (an entry from the future is invisible, and counted by the caller — detection, not
/// prevention). Clean-slate v1 per `docs/design/artifact-library.md` §4.1 — the catalogue had no
/// field deployments when the kind axis landed, so there is no pre-version format to accept.
pub const ENTRY_FORMAT_VERSION: u8 = 1;

/// Publisher-declared **resource requirements** for hosting an artifact — what installing it
/// actually consumes on a node, as opposed to the *ranking hints* (`size_bytes` is transfer
/// cost). `0` = undeclared (no check). A node's provisioner refuses to self-elect for an entry
/// whose requirements exceed its headroom (`docs/design/artifact-library.md` §4.4); only the
/// publisher knows these numbers (a 4 GB quantised model can want 5 GB+ of RAM activated), so
/// they travel in the entry — **inside the provenance signature**: a requirement is an
/// install-safety claim, and a tampered one (mem lowered to 0) would make trusting nodes OOM
/// themselves.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ResourceRequirements {
    /// Bytes of disk the placed artifact occupies at the runtime's placement root.
    pub disk_bytes: u64,
    /// Bytes of memory hosting the artifact consumes once live (instance / activated model).
    pub mem_bytes:  u64,
}

/// One installable artifact in the catalog: the [`Capability`] it would provide once installed,
/// the [`ArtifactKind`] that selects *how* a node installs it, plus the content address
/// ([`ArtifactId`]) to pull. The declared-provide is a full `Capability` so the live resolver's
/// `CapFilter::matches` works unchanged against not-yet-installed artifacts.
// `Capability` is `PartialEq` but not `Eq`, so neither is this.
#[derive(Clone, Debug, PartialEq)]
pub struct InstallableEntry {
    /// What this artifact would advertise once installed (the resolver-matchable declared-provide).
    pub provides:          Capability,
    /// How a node installs it: instantiate (`WasmComponent`) vs place-and-probe (`Blob`).
    pub kind:              ArtifactKind,
    /// Content address of the artifact bytes to pull (hand to `WasmHost::provision`).
    pub artifact:          ArtifactId,
    /// Optional ranking hints (0 = unknown): bytes to pull, estimated install seconds.
    pub size_bytes:        u64,
    pub est_install_secs:  u64,
    /// Publisher-declared hosting requirements (0 = undeclared). Signed — see
    /// [`ResourceRequirements`].
    pub requires:          ResourceRequirements,
    /// Optional **provenance**: an Ed25519 signature by a publisher over the *entry* — version,
    /// kind, content address, and declared-provide (see [`provenance_message`]). Empty = unsigned.
    /// The content hash gives *integrity* (the bytes are what the catalog named); this gives
    /// *provenance* (a trusted publisher vouched for these bytes **as** this capability and kind —
    /// a signed artifact cannot be re-labeled under a different capability or kind and still
    /// verify). Cost hints stay outside the signature: they are ranking hints, not security claims.
    ///
    /// [`provenance_message`]: Self::provenance_message
    pub signer:            Vec<u8>, // 32-byte verifying key, or empty
    pub signature:         Vec<u8>, // 64-byte signature over provenance_message(), or empty
}

impl InstallableEntry {
    /// A catalog entry for `provides`, pulled from `artifact`. Defaults to
    /// [`ArtifactKind::WasmComponent`]; see [`with_kind`](Self::with_kind).
    pub fn new(provides: Capability, artifact: ArtifactId) -> Self {
        Self {
            provides,
            kind: ArtifactKind::WasmComponent,
            artifact,
            size_bytes: 0,
            est_install_secs: 0,
            requires: ResourceRequirements::default(),
            signer: Vec::new(),
            signature: Vec::new(),
        }
    }

    /// Set the artifact kind (install dispatch — `docs/design/artifact-library.md` §4).
    pub fn with_kind(mut self, kind: ArtifactKind) -> Self {
        self.kind = kind;
        self
    }

    /// Attach ranking hints used by [`InstallableCatalog::resolve_best`].
    pub fn with_cost(mut self, size_bytes: u64, est_install_secs: u64) -> Self {
        self.size_bytes = size_bytes;
        self.est_install_secs = est_install_secs;
        self
    }

    /// Declare hosting requirements (disk at the placement root, memory once live). Call
    /// **before** [`signed_by`](Self::signed_by) — requirements are inside the signature.
    pub fn with_requirements(mut self, disk_bytes: u64, mem_bytes: u64) -> Self {
        self.requires = ResourceRequirements { disk_bytes, mem_bytes };
        self
    }

    /// The domain-separated message provenance signs: binds the signature to the whole entry —
    /// format version, kind, content address, **hosting requirements**, and declared-provide
    /// capability. Signing only the content address would let anyone re-publish a trusted
    /// publisher's artifact under a different capability or kind with provenance still
    /// verifying; leaving requirements unsigned would let a tampered entry (mem lowered to 0)
    /// make trusting nodes install something that OOMs them. Cost *hints* stay outside — they
    /// are ranking inputs, not safety claims.
    fn provenance_message(&self) -> Vec<u8> {
        let cap = self.provides.encode();
        let mut m = Vec::with_capacity(24 + 2 + 32 + 16 + cap.len());
        m.extend_from_slice(b"mycelium-installable-v1");
        m.push(ENTRY_FORMAT_VERSION);
        m.push(self.kind.as_u8());
        m.extend_from_slice(self.artifact.as_bytes());
        m.extend_from_slice(&self.requires.disk_bytes.to_le_bytes());
        m.extend_from_slice(&self.requires.mem_bytes.to_le_bytes());
        m.extend_from_slice(&cap);
        m
    }

    /// Sign the entry with `signing_key`, attaching publisher provenance. A provisioner
    /// configured with the matching verifying key (see `Provisioner::require_provenance`) then only
    /// installs artifacts a trusted publisher vouched for. Call **after** `with_kind` /
    /// `with_requirements` — the signature covers the kind, requirements, and declared-provide
    /// (not the cost hints).
    pub fn signed_by(mut self, signing_key: &ed25519_dalek::SigningKey) -> Self {
        use ed25519_dalek::Signer;
        let sig = signing_key.sign(&self.provenance_message());
        self.signer = signing_key.verifying_key().to_bytes().to_vec();
        self.signature = sig.to_bytes().to_vec();
        self
    }

    /// Verify provenance: the entry is signed, the signer is in `trusted`, and the signature is a
    /// valid Ed25519 signature over [`provenance_message`](Self::provenance_message). An unsigned
    /// entry is **not** trusted; neither is a signed entry whose kind, artifact, or
    /// declared-provide was altered after signing.
    pub fn verify_provenance(&self, trusted: &[[u8; 32]]) -> bool {
        use ed25519_dalek::{Signature, Verifier, VerifyingKey};
        let (Ok(signer), Ok(sig_bytes)) =
            (<[u8; 32]>::try_from(self.signer.as_slice()), <[u8; 64]>::try_from(self.signature.as_slice()))
        else {
            return false;
        };
        if !trusted.contains(&signer) {
            return false;
        }
        let Ok(vk) = VerifyingKey::from_bytes(&signer) else { return false };
        vk.verify(&self.provenance_message(), &Signature::from_bytes(&sig_bytes)).is_ok()
    }

    /// Serialize for gossip: `[1B version][1B kind][32B artifact][8B size][8B est]
    /// [8B req_disk][8B req_mem][1B signed][32B signer + 64B sig if signed][Capability bytes]`.
    /// Built on the public `Capability::encode` (no internal codec).
    pub fn encode(&self) -> Vec<u8> {
        let cap = self.provides.encode();
        let signed = self.signer.len() == 32 && self.signature.len() == 64;
        let mut out = Vec::with_capacity(67 + if signed { 96 } else { 0 } + cap.len());
        out.push(ENTRY_FORMAT_VERSION);
        out.push(self.kind.as_u8());
        out.extend_from_slice(self.artifact.as_bytes());
        out.extend_from_slice(&self.size_bytes.to_le_bytes());
        out.extend_from_slice(&self.est_install_secs.to_le_bytes());
        out.extend_from_slice(&self.requires.disk_bytes.to_le_bytes());
        out.extend_from_slice(&self.requires.mem_bytes.to_le_bytes());
        out.push(signed as u8);
        if signed {
            out.extend_from_slice(&self.signer);
            out.extend_from_slice(&self.signature);
        }
        out.extend_from_slice(&cap);
        out
    }

    /// Inverse of [`encode`](Self::encode). `None` for an unknown format version or kind byte —
    /// a node never guesses at an entry it cannot fully name (the caller counts the rejection).
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 67 || bytes[0] != ENTRY_FORMAT_VERSION {
            return None;
        }
        let kind = ArtifactKind::from_u8(bytes[1])?;
        let mut id = [0u8; 32];
        id.copy_from_slice(&bytes[2..34]);
        let size_bytes = u64::from_le_bytes(bytes[34..42].try_into().ok()?);
        let est_install_secs = u64::from_le_bytes(bytes[42..50].try_into().ok()?);
        let requires = ResourceRequirements {
            disk_bytes: u64::from_le_bytes(bytes[50..58].try_into().ok()?),
            mem_bytes:  u64::from_le_bytes(bytes[58..66].try_into().ok()?),
        };
        let signed = bytes[66] == 1;
        let mut off = 67;
        let (signer, signature) = if signed {
            if bytes.len() < off + 96 {
                return None;
            }
            let signer = bytes[off..off + 32].to_vec();
            let signature = bytes[off + 32..off + 96].to_vec();
            off += 96;
            (signer, signature)
        } else {
            (Vec::new(), Vec::new())
        };
        let provides = Capability::decode(&bytes[off..])?;
        Some(Self {
            provides,
            kind,
            artifact: ArtifactId::from_bytes(id),
            size_bytes,
            est_install_secs,
            requires,
            signer,
            signature,
        })
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

/// Conventional filename of a library's manifest inside its blob directory. Not 64-hex, so
/// `FsLibrarySource::list` never mistakes it for a blob.
pub const MANIFEST_FILE: &str = "manifest";

/// The library's own catalogue file — what makes a blob directory **self-describing**
/// (`docs/design/artifact-library.md` §2). One [`InstallableEntry`] per line, lowercase hex of
/// the canonical [`InstallableEntry::encode`] — signatures included, because entries are signed
/// when *added to the library* (by CI holding the publisher key), so librarian nodes serve
/// pre-signed entries and never hold signing keys.
///
/// The manifest is the **library's source of truth**; the gossiped `installable/` catalogue is
/// the *cluster's view* of it. A librarian keeps the view current by diffing manifest revisions
/// ([`Manifest::diff`]) — publishing added entries, tombstoning removed ones — scoped to its own
/// publisher's entries, so a library removal never clobbers another publisher's announcement.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Manifest {
    entries: Vec<InstallableEntry>,
}

impl Manifest {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_entries(entries: Vec<InstallableEntry>) -> Self {
        Self { entries }
    }

    pub fn entries(&self) -> &[InstallableEntry] {
        &self.entries
    }

    pub fn add(&mut self, entry: InstallableEntry) {
        self.entries.push(entry);
    }

    /// Render to the line-hex format (one encoded entry per line).
    pub fn render(&self) -> String {
        let mut out = String::new();
        for e in &self.entries {
            for b in e.encode() {
                use std::fmt::Write;
                let _ = write!(out, "{b:02x}");
            }
            out.push('\n');
        }
        out
    }

    /// Parse the line-hex format. A malformed line is an **error**, not a skip — the manifest is
    /// the library's source of truth, and silent corruption is exactly what must be detected.
    pub fn parse(text: &str) -> Result<Self, ManifestError> {
        let mut entries = Vec::new();
        for (lineno, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let bytes = hex_line(line).ok_or(ManifestError::BadLine(lineno + 1))?;
            let entry =
                InstallableEntry::decode(&bytes).ok_or(ManifestError::BadLine(lineno + 1))?;
            entries.push(entry);
        }
        Ok(Self { entries })
    }

    /// Load from `path`. A missing file is an empty manifest (a new library); a malformed one is
    /// an error.
    pub fn load(path: &std::path::Path) -> Result<Self, ManifestError> {
        match std::fs::read_to_string(path) {
            Ok(text) => Self::parse(&text),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::new()),
            Err(e) => Err(ManifestError::Io(e)),
        }
    }

    /// Save to `path`, complete-or-absent (temp write + rename — a concurrent reader never
    /// observes a torn manifest).
    pub fn save(&self, path: &std::path::Path) -> Result<(), ManifestError> {
        let dir = path.parent().unwrap_or_else(|| std::path::Path::new("."));
        static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let tmp = dir.join(format!(
            ".tmp-manifest-{}-{}",
            std::process::id(),
            TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        ));
        std::fs::write(&tmp, self.render()).map_err(ManifestError::Io)?;
        std::fs::rename(&tmp, path).map_err(ManifestError::Io)
    }

    /// The CI **publish step**: load the manifest at `path` (missing = new library), add
    /// `entry` — replacing any existing row with the same KV key (`{ns}/{name}/{artifact-hex}`)
    /// — and save it back (torn-read-safe). Together with `FsLibrarySource::store` this is the
    /// whole publish operation; a running librarian picks the change up on its next reconcile
    /// pass. Single-publisher discipline: concurrent publishers race the load-modify-save
    /// (whole-file last-write-wins) — serialize publish jobs per library.
    pub fn append_entry(
        path: &std::path::Path,
        entry: InstallableEntry,
    ) -> Result<(), ManifestError> {
        let mut m = Self::load(path)?;
        let key = entry.kv_key();
        if let Some(existing) = m.entries.iter_mut().find(|e| e.kv_key() == key) {
            *existing = entry;
        } else {
            m.entries.push(entry);
        }
        m.save(path)
    }

    /// Diff two manifest revisions into the librarian's sync actions:
    /// `(to_publish, to_tombstone)`. An entry is *published* when its KV key is new **or** its
    /// encoded value changed (LWW republish overwrites in place); *tombstoned* when its key
    /// disappeared. Keys are [`InstallableEntry::kv_key`].
    pub fn diff(old: &Manifest, new: &Manifest) -> (Vec<InstallableEntry>, Vec<InstallableEntry>) {
        use std::collections::HashMap;
        let old_by_key: HashMap<String, &InstallableEntry> =
            old.entries.iter().map(|e| (e.kv_key(), e)).collect();
        let new_keys: std::collections::HashSet<String> =
            new.entries.iter().map(|e| e.kv_key()).collect();

        let to_publish = new
            .entries
            .iter()
            .filter(|e| old_by_key.get(&e.kv_key()).is_none_or(|o| o.encode() != e.encode()))
            .cloned()
            .collect();
        let to_tombstone = old
            .entries
            .iter()
            .filter(|e| !new_keys.contains(&e.kv_key()))
            .cloned()
            .collect();
        (to_publish, to_tombstone)
    }
}

/// Why a [`Manifest`] failed to load or save.
#[derive(Debug)]
pub enum ManifestError {
    /// Line `n` (1-indexed) is not a hex-encoded, decodable [`InstallableEntry`].
    BadLine(usize),
    Io(std::io::Error),
}

impl std::fmt::Display for ManifestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadLine(n) => write!(f, "manifest line {n} is not a valid installable entry"),
            Self::Io(e) => write!(f, "manifest io error: {e}"),
        }
    }
}

impl std::error::Error for ManifestError {}

/// Decode one even-length lowercase/uppercase hex line.
fn hex_line(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(s.len() / 2);
    for i in (0..b.len()).step_by(2) {
        let hi = hex_nibble(b[i])?;
        let lo = hex_nibble(b[i + 1])?;
        out.push((hi << 4) | lo);
    }
    Some(out)
}

fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
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
    fn provenance_signs_and_verifies_against_trusted_keys() {
        use ed25519_dalek::SigningKey;
        let sk = SigningKey::from_bytes(&[7u8; 32]);
        let vk = sk.verifying_key().to_bytes();
        let entry = InstallableEntry::new(cap("llm", "summarize"), art(b"a.wasm")).signed_by(&sk);

        assert!(entry.verify_provenance(&[vk]), "trusted signer accepted");

        let other = SigningKey::from_bytes(&[8u8; 32]).verifying_key().to_bytes();
        assert!(!entry.verify_provenance(&[other]), "untrusted signer rejected");

        let unsigned = InstallableEntry::new(cap("llm", "summarize"), art(b"a.wasm"));
        assert!(!unsigned.verify_provenance(&[vk]), "unsigned entry is never trusted");

        // A signature is over the content address — pointing the entry at different bytes breaks it.
        let mut tampered = entry.clone();
        tampered.artifact = art(b"evil.wasm");
        assert!(!tampered.verify_provenance(&[vk]), "swapped artifact fails the signature");
    }

    #[test]
    fn signed_entry_encode_decode_round_trips() {
        use ed25519_dalek::SigningKey;
        let sk = SigningKey::from_bytes(&[9u8; 32]);
        let vk = sk.verifying_key().to_bytes();
        let e = InstallableEntry::new(cap("v", "d"), art(b"x")).with_cost(10, 2).signed_by(&sk);
        let back = InstallableEntry::decode(&e.encode()).expect("decode signed");
        assert_eq!(back.signer, e.signer);
        assert_eq!(back.signature, e.signature);
        assert_eq!(back.size_bytes, 10);
        assert!(back.verify_provenance(&[vk]), "provenance survives encode/decode");
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

    #[test]
    fn kind_round_trips_and_defaults_to_wasm_component() {
        use crate::artifact::ArtifactKind;
        let e = InstallableEntry::new(cap("llm", "weights"), art(b"model.onnx"));
        assert_eq!(e.kind, ArtifactKind::WasmComponent, "default kind");

        let blob = e.with_kind(ArtifactKind::Blob);
        let back = InstallableEntry::decode(&blob.encode()).expect("decode blob entry");
        assert_eq!(back.kind, ArtifactKind::Blob);
        assert_eq!(back.provides.name.as_ref(), "weights");
    }

    #[test]
    fn decode_rejects_unknown_version_and_unknown_kind() {
        let good = InstallableEntry::new(cap("v", "d"), art(b"x")).encode();

        let mut wrong_version = good.clone();
        wrong_version[0] = ENTRY_FORMAT_VERSION + 1;
        assert!(InstallableEntry::decode(&wrong_version).is_none(), "future version invisible");

        let mut wrong_kind = good;
        wrong_kind[1] = 0xEE;
        assert!(InstallableEntry::decode(&wrong_kind).is_none(), "unknown kind invisible");
    }

    #[test]
    fn provenance_binds_the_whole_entry_not_just_the_bytes() {
        use crate::artifact::ArtifactKind;
        use ed25519_dalek::SigningKey;
        let sk = SigningKey::from_bytes(&[11u8; 32]);
        let vk = sk.verifying_key().to_bytes();
        let entry = InstallableEntry::new(cap("llm", "summarize"), art(b"a.wasm"))
            .with_kind(ArtifactKind::WasmComponent)
            .signed_by(&sk);
        assert!(entry.verify_provenance(&[vk]));

        // Re-labeling a signed artifact under a different capability must break provenance…
        let mut relabeled = entry.clone();
        relabeled.provides = cap("admin", "root-shell");
        assert!(!relabeled.verify_provenance(&[vk]), "capability re-label fails the signature");

        // …and so must flipping the kind (install method) after signing.
        let mut rekinded = entry.clone();
        rekinded.kind = ArtifactKind::Blob;
        assert!(!rekinded.verify_provenance(&[vk]), "kind flip fails the signature");

        // Cost hints are outside the signature (ranking hints, not security claims).
        let mut rehinted = entry;
        rehinted.size_bytes = 999;
        assert!(rehinted.verify_provenance(&[vk]), "cost hints may be updated without re-signing");
    }

    #[test]
    fn requirements_round_trip_and_are_bound_by_provenance() {
        use ed25519_dalek::SigningKey;
        let sk = SigningKey::from_bytes(&[17u8; 32]);
        let vk = sk.verifying_key().to_bytes();

        let entry = InstallableEntry::new(cap("llm", "weights"), art(b"model"))
            .with_kind(crate::artifact::ArtifactKind::Blob)
            .with_requirements(4_000_000_000, 5_000_000_000)
            .signed_by(&sk);

        // Round trip through the wire encoding.
        let back = InstallableEntry::decode(&entry.encode()).expect("decode");
        assert_eq!(back.requires.disk_bytes, 4_000_000_000);
        assert_eq!(back.requires.mem_bytes, 5_000_000_000);
        assert!(back.verify_provenance(&[vk]), "requirements survive encode/decode signed");

        // Requirements are safety claims: lowering them after signing breaks provenance.
        let mut starved = entry.clone();
        starved.requires.mem_bytes = 0;
        assert!(!starved.verify_provenance(&[vk]), "tampered mem requirement fails the signature");
        let mut shrunk = entry;
        shrunk.requires.disk_bytes = 1;
        assert!(!shrunk.verify_provenance(&[vk]), "tampered disk requirement fails the signature");
    }

    fn scratch_manifest_path(tag: &str) -> std::path::PathBuf {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "mycelium-manifest-{tag}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(MANIFEST_FILE)
    }

    #[test]
    fn manifest_saves_loads_and_preserves_signatures() {
        use ed25519_dalek::SigningKey;
        let sk = SigningKey::from_bytes(&[13u8; 32]);
        let vk = sk.verifying_key().to_bytes();

        let mut m = Manifest::new();
        m.add(InstallableEntry::new(cap("route", "optimize"), art(b"opt")).with_cost(10, 1).signed_by(&sk));
        m.add(InstallableEntry::new(cap("llm", "weights"), art(b"w"))
            .with_kind(crate::artifact::ArtifactKind::Blob));

        let path = scratch_manifest_path("roundtrip");
        m.save(&path).expect("save");
        let back = Manifest::load(&path).expect("load");
        assert_eq!(back, m, "line-hex round trip is lossless");
        assert!(back.entries()[0].verify_provenance(&[vk]), "signature survives the manifest");

        // A missing manifest is an empty (new) library; a corrupt one is an error, not a skip.
        let missing = Manifest::load(&path.with_file_name("nonexistent")).expect("missing = empty");
        assert!(missing.entries().is_empty());
        std::fs::write(&path, "deadbeef-not-an-entry\n").unwrap();
        assert!(matches!(Manifest::load(&path), Err(ManifestError::BadLine(1))));

        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    #[test]
    fn manifest_append_entry_is_the_one_call_publish_step() {
        use crate::artifact::ArtifactKind;
        let path = scratch_manifest_path("append");

        // First publish into a not-yet-existing manifest (a new library).
        let v1 = InstallableEntry::new(cap("llm", "weights"), art(b"model-v1"))
            .with_kind(ArtifactKind::Blob)
            .with_requirements(4_000, 5_000);
        Manifest::append_entry(&path, v1.clone()).expect("first publish");
        let m = Manifest::load(&path).unwrap();
        assert_eq!(m.entries().len(), 1);
        assert_eq!(m.entries()[0].requires.mem_bytes, 5_000, "footprint travels in the manifest");

        // A second, different artifact appends…
        let other = InstallableEntry::new(cap("route", "optimize"), art(b"opt"));
        Manifest::append_entry(&path, other).expect("second publish");
        assert_eq!(Manifest::load(&path).unwrap().entries().len(), 2);

        // …while re-publishing the same key (same capability + artifact, new metadata)
        // replaces in place instead of duplicating.
        let v1_retuned = v1.with_requirements(4_000, 6_000);
        Manifest::append_entry(&path, v1_retuned).expect("republish");
        let m = Manifest::load(&path).unwrap();
        assert_eq!(m.entries().len(), 2, "same kv_key replaces, never duplicates");
        let row = m.entries().iter().find(|e| e.provides.name.as_ref() == "weights").unwrap();
        assert_eq!(row.requires.mem_bytes, 6_000);

        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    /// M2 Run-38 falsification probe (Test Architecture), kept as a permanent property test:
    /// the entry decoder and manifest parser must be TOTAL over hostile input — every
    /// truncation of a valid encoding, and a spray of single-byte corruptions, must return
    /// None/Err, never panic, never misdecode into a different-but-valid entry silently
    /// accepted by provenance.
    #[test]
    fn decode_and_manifest_parse_are_total_over_adversarial_bytes() {
        use ed25519_dalek::SigningKey;
        let sk = SigningKey::from_bytes(&[3u8; 32]);
        let vk = sk.verifying_key().to_bytes();
        let entry = InstallableEntry::new(cap("llm", "weights"), art(b"payload"))
            .with_kind(crate::artifact::ArtifactKind::Blob)
            .with_cost(123, 4)
            .with_requirements(1_000, 2_000)
            .signed_by(&sk);
        let good = entry.encode();

        // Every truncation: no panic, and never a decode that still passes provenance.
        for cut in 0..good.len() {
            if let Some(d) = InstallableEntry::decode(&good[..cut]) {
                assert!(!d.verify_provenance(&[vk]),
                    "a truncated encoding must never verify as the publisher's entry (cut={cut})");
            }
        }
        // Single-byte corruption at every offset: no panic; if it still decodes, provenance
        // must reject it (the signature covers version/kind/artifact/requirements/capability;
        // only the unsigned cost-hint bytes may mutate and still verify — by design).
        const HINTS: std::ops::Range<usize> = 34..50; // size_bytes + est_install_secs
        for i in 0..good.len() {
            let mut bad = good.clone();
            bad[i] ^= 0xA5;
            if let Some(d) = InstallableEntry::decode(&bad)
                && d.verify_provenance(&[vk])
            {
                assert!(HINTS.contains(&i),
                    "corruption at offset {i} decoded AND passed provenance outside the unsigned hint bytes");
            }
        }
        // Manifest lines: odd-length hex, non-hex, and corrupted-entry lines all error (never
        // panic, never silently skip).
        for text in ["abc\n", "zz zz\n", "deadbeef\n"] {
            assert!(Manifest::parse(text).is_err(), "malformed line must be an error: {text:?}");
        }
        let mut hexline = String::new();
        for b in &good { use std::fmt::Write; let _ = write!(hexline, "{b:02x}"); }
        hexline.replace_range(0..2, "ff"); // unknown format version
        assert!(Manifest::parse(&format!("{hexline}\n")).is_err(),
            "an undecodable entry line is an error, not a skip");
    }

    #[test]
    fn manifest_diff_yields_publish_and_tombstone_sets() {
        let a = InstallableEntry::new(cap("route", "optimize"), art(b"opt-v1"));
        let b = InstallableEntry::new(cap("llm", "weights"), art(b"w"));
        let c = InstallableEntry::new(cap("vision", "detect"), art(b"d"));
        let old = Manifest::from_entries(vec![a.clone(), b.clone()]);

        // c added, a's hints changed in place (same key), b removed.
        let a_rehinted = a.clone().with_cost(5_000, 2);
        let new = Manifest::from_entries(vec![a_rehinted.clone(), c.clone()]);

        let (publish, tombstone) = Manifest::diff(&old, &new);
        let publish_keys: Vec<String> = publish.iter().map(|e| e.kv_key()).collect();
        assert!(publish_keys.contains(&c.kv_key()), "new entry published");
        assert!(publish_keys.contains(&a_rehinted.kv_key()), "changed entry republished (LWW)");
        assert_eq!(publish.len(), 2);
        assert_eq!(tombstone.len(), 1);
        assert_eq!(tombstone[0].kv_key(), b.kv_key(), "removed entry tombstoned");

        // Identical revisions are a no-op sync.
        let (p2, t2) = Manifest::diff(&new, &new);
        assert!(p2.is_empty() && t2.is_empty());
    }
}
