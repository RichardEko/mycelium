//! The **librarian** — a node fronting a durable artifact library for the cluster
//! (`docs/design/artifact-library.md` §2, §6). It does three things: **serves** the library's
//! blobs over the `artifact.fetch` RPC, **advertises** the `artifact/librarian` capability so
//! pullers can *discover* a holder through the capability ring (no hardcoded provider lists),
//! and **reconciles** the gossiped `installable/` catalogue to the library's manifest — the
//! library's source of truth.
//!
//! A librarian is a **role, not a daemon** (invariant L3): any node holding a durable
//! [`ArtifactSource`] can take it, several may front the same store, none is elected or special,
//! and no read ever *requires* one — peers that have pulled an artifact serve the same
//! content-verified bytes. Failover transfers nothing: a new librarian resumes against the same
//! store and manifest.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use mycelium::{CapFilter, Capability, CapabilityReg, GossipAgent};

use crate::artifact::ArtifactSource;
use crate::catalog::{publish_installable, InstallableEntry, Manifest, INSTALLABLE_PREFIX};
use crate::mesh_source::serve_artifacts;

/// Capability namespace/name a librarian advertises. One advertisement per serving node — never
/// per artifact (per-hash ads would flood the capability namespace; misses cost one RPC and
/// `pull_artifact` handles them cleanly).
pub const LIBRARIAN_NS: &str = "artifact";
pub const LIBRARIAN_NAME: &str = "librarian";

/// The filter a puller resolves to discover librarian nodes — see
/// `MeshArtifactSource::resolving`.
pub fn librarian_filter() -> CapFilter {
    CapFilter::new(LIBRARIAN_NS, LIBRARIAN_NAME)
}

/// How often the librarian capability advertisement re-asserts.
const LIBRARIAN_ADVERTISE_INTERVAL: Duration = Duration::from_secs(5);

/// Configuration for [`spawn_librarian`].
pub struct LibrarianConfig {
    /// Path to the library's manifest ([`Manifest`] line-hex format). Updating the manifest is
    /// the *library's* concern (CI adds a blob + a signed entry row); the librarian only reads.
    pub manifest_path: PathBuf,
    /// The library's publisher key (Ed25519 verifying key). Catalogue reconciliation is
    /// **scoped to entries signed by this key**: the librarian publishes and tombstones only its
    /// own publisher's announcements — a library removal never clobbers another publisher's
    /// entry (design §2). Manifest rows signed by other keys are ignored.
    pub publisher: [u8; 32],
    /// Manifest poll / reconcile interval.
    pub sync_interval: Duration,
}

/// A running librarian. Dropping it stops serving, retracts the capability advertisement, and
/// stops the manifest sync. Already-published catalogue entries persist (they are ordinary KV)
/// — the next librarian, on any node, resumes against the same store + manifest.
pub struct LibrarianHandle {
    _cap:  CapabilityReg,
    serve: tokio::task::JoinHandle<()>,
    sync:  tokio::task::JoinHandle<()>,
}

impl Drop for LibrarianHandle {
    fn drop(&mut self) {
        self.serve.abort();
        self.sync.abort();
    }
}

/// Take the librarian role: serve `source` to the cluster, advertise `artifact/librarian`, and
/// keep the gossiped catalogue reconciled to the manifest on `cfg.sync_interval`.
pub fn spawn_librarian(
    agent: Arc<GossipAgent>,
    source: Arc<dyn ArtifactSource + Send + Sync>,
    cfg: LibrarianConfig,
) -> LibrarianHandle {
    let serve = serve_artifacts(Arc::clone(&agent), source);
    let cap = agent.capabilities().advertise_capability(
        Capability::new(LIBRARIAN_NS, LIBRARIAN_NAME),
        LIBRARIAN_ADVERTISE_INTERVAL,
    );
    let sync = tokio::spawn(async move {
        loop {
            sync_once(&agent, &cfg);
            tokio::time::sleep(cfg.sync_interval).await;
        }
    });
    LibrarianHandle { _cap: cap, serve, sync }
}

/// One reconcile pass: **manifest → KV**, idempotent and self-healing. The "old" revision is the
/// *live KV view* filtered to this publisher's entries (not remembered state), so a restarted
/// librarian repairs anything that changed while it was down — removals are re-tombstoned, LWW
/// drift is re-published — with no state carried across restarts.
fn sync_once(agent: &Arc<GossipAgent>, cfg: &LibrarianConfig) {
    let manifest = match Manifest::load(&cfg.manifest_path) {
        Ok(m) => m,
        Err(e) => {
            // Unreadable/corrupt manifest: skip the pass, keep serving. Detection, not
            // prevention — the catalogue keeps its last-good state until the manifest heals.
            tracing::warn!(%e, path = ?cfg.manifest_path,
                "librarian manifest unreadable — skipping reconcile pass");
            return;
        }
    };

    let kv = agent.kv();
    let ours = |e: &InstallableEntry| e.signer.as_slice() == cfg.publisher.as_slice();

    // The cluster's current view of this publisher's announcements (undecodable entries —
    // unknown version/kind, foreign publishers — are simply not ours to manage).
    let in_kv: Vec<InstallableEntry> = kv
        .scan_prefix(INSTALLABLE_PREFIX)
        .into_iter()
        .filter_map(|(_key, bytes)| InstallableEntry::decode(&bytes))
        .filter(ours)
        .collect();
    let in_manifest: Vec<InstallableEntry> =
        manifest.entries().iter().filter(|e| ours(e)).cloned().collect();

    let (publish, tombstone) =
        Manifest::diff(&Manifest::from_entries(in_kv), &Manifest::from_entries(in_manifest));
    for entry in &publish {
        publish_installable(&kv, entry);
    }
    for entry in &tombstone {
        let _ = kv.delete(entry.kv_key());
    }
    if !publish.is_empty() || !tombstone.is_empty() {
        metrics::counter!("mycelium_artifact_librarian_published_total")
            .increment(publish.len() as u64);
        metrics::counter!("mycelium_artifact_librarian_tombstoned_total")
            .increment(tombstone.len() as u64);
        tracing::info!(published = publish.len(), tombstoned = tombstone.len(),
            "librarian reconciled the catalogue to the manifest");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsLibrarySource;
    use crate::catalog::{InstallableCatalog, MANIFEST_FILE};
    use crate::mesh_source::MeshArtifactSource;
    use ed25519_dalek::SigningKey;
    use mycelium::{Capability, NodeId};

    fn alloc_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
    }

    async fn agent(port: u16, bootstrap: Option<u16>) -> Arc<GossipAgent> {
        let id = NodeId::new("127.0.0.1", port).unwrap();
        let cfg = mycelium::GossipConfig {
            bind_port: port,
            bootstrap_peers: bootstrap
                .map(|b| vec![NodeId::new("127.0.0.1", b).unwrap()])
                .unwrap_or_default(),
            // Anti-entropy rides the health tick; a cross-node test that reads a fresh KV
            // write needs a fast sweep (the rotation demo's lesson — default is 10 s).
            health_check_interval_secs: 2,
            ..Default::default()
        };
        let a = Arc::new(GossipAgent::new(id, cfg));
        a.start().await.expect("agent start");
        a
    }

    fn scratch_library(tag: &str) -> std::path::PathBuf {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "mycelium-librarian-{tag}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        ))
    }

    #[tokio::test]
    async fn reconciles_manifest_to_catalogue_scoped_to_its_own_signer() {
        let a = agent(alloc_port(), None).await;
        let dir = scratch_library("reconcile");
        let lib = Arc::new(FsLibrarySource::open(&dir).unwrap());
        let manifest_path = dir.join(MANIFEST_FILE);

        let ours_key = SigningKey::from_bytes(&[21u8; 32]);
        let theirs_key = SigningKey::from_bytes(&[22u8; 32]);

        // Library holds one blob, announced by a signed manifest row.
        let id = lib.store(b"deployable bytes").unwrap();
        let entry = InstallableEntry::new(Capability::new("route", "optimize"), id)
            .signed_by(&ours_key);
        Manifest::from_entries(vec![entry.clone()]).save(&manifest_path).unwrap();

        // Another publisher's entry already lives in the catalogue — not ours to manage.
        let foreign = InstallableEntry::new(
            Capability::new("vision", "detect"),
            crate::ArtifactId::of(b"someone else's artifact"),
        )
        .signed_by(&theirs_key);
        assert!(publish_installable(&a.kv(), &foreign));

        let _librarian = spawn_librarian(
            Arc::clone(&a),
            Arc::clone(&lib) as Arc<_>,
            LibrarianConfig {
                manifest_path: manifest_path.clone(),
                publisher: ours_key.verifying_key().to_bytes(),
                sync_interval: Duration::from_millis(100),
            },
        );

        // The manifest row reaches the catalogue.
        let filter = CapFilter::new("route", "optimize");
        let mut published = false;
        for _ in 0..60 {
            if InstallableCatalog::from_kv(&a.kv()).resolve_best(&filter).is_some() {
                published = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(published, "manifest entry is published to the gossiped catalogue");

        // Remove it from the manifest → the librarian tombstones it…
        Manifest::new().save(&manifest_path).unwrap();
        let mut tombstoned = false;
        for _ in 0..60 {
            if InstallableCatalog::from_kv(&a.kv()).resolve_best(&filter).is_none() {
                tombstoned = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(tombstoned, "removed manifest entry is tombstoned from the catalogue");

        // …while the foreign publisher's entry is untouched (signature-scoped sync).
        let foreign_still = InstallableCatalog::from_kv(&a.kv())
            .resolve_best(&CapFilter::new("vision", "detect"))
            .is_some();
        assert!(foreign_still, "another publisher's entry survives our library's removal");

        a.shutdown().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_puller_provisions_knowing_only_the_catalogue() {
        // The step-3 acceptance test: node B discovers the entry via the gossiped catalogue and
        // the *holder* via the librarian capability — no hardcoded provider node-ids anywhere.
        let a_port = alloc_port();
        let a = agent(a_port, None).await;
        let b = agent(alloc_port(), Some(a_port)).await;
        for _ in 0..80 {
            if !a.peers().is_empty() && !b.peers().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let dir = scratch_library("resolve");
        let lib = Arc::new(FsLibrarySource::open(&dir).unwrap());
        let manifest_path = dir.join(MANIFEST_FILE);
        let key = SigningKey::from_bytes(&[23u8; 32]);

        let id = lib.store(b"library-served artifact").unwrap();
        let entry =
            InstallableEntry::new(Capability::new("route", "optimize"), id).signed_by(&key);
        Manifest::from_entries(vec![entry]).save(&manifest_path).unwrap();

        let _librarian = spawn_librarian(
            Arc::clone(&a),
            Arc::clone(&lib) as Arc<_>,
            LibrarianConfig {
                manifest_path,
                publisher: key.verifying_key().to_bytes(),
                sync_interval: Duration::from_millis(100),
            },
        );

        // B: discover the entry via the catalogue (gossiped KV — arrives on the anti-entropy
        // sweep, so the poll window must cover at least one tick).
        let filter = CapFilter::new("route", "optimize");
        let mut found = None;
        for _ in 0..160 {
            if let Some(e) = InstallableCatalog::from_kv(&b.kv()).resolve_best(&filter) {
                found = Some(e.clone());
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let entry = found.expect("B discovers the entry through the gossiped catalogue");
        assert!(entry.verify_provenance(&[key.verifying_key().to_bytes()]));

        // B: discover the holder via the librarian capability and pull — verified on arrival.
        let mesh = MeshArtifactSource::resolving(
            Arc::clone(&b),
            librarian_filter(),
            Duration::from_secs(2),
        );
        let mut pulled = false;
        for _ in 0..40 {
            if mesh.prefetch(&entry.artifact).await {
                pulled = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(pulled, "B pulls from a librarian it discovered — no hardcoded provider");
        assert_eq!(mesh.fetch(&entry.artifact).as_deref(), Some(&b"library-served artifact"[..]));

        a.shutdown().await;
        b.shutdown().await;
        std::fs::remove_dir_all(&dir).ok();
    }
}
