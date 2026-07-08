//! Content-addressed payload tier — the storage half the LangGraph checkpointer consumes.
//!
//! Metadata gossips everywhere (KV); **payloads do not** — they live in a content-addressed
//! store and are fetched from whichever peer holds them. A blob's id *is* its SHA-256, so
//! every fetch (disk or mesh) is verified against the address and dedup across immutable
//! checkpoints falls out for free.
//!
//! v1 limit, stated honestly: one blob ≤ [`MAX_BLOB_BYTES`] (a single-frame RPC reply);
//! chunked transfer via `ServiceHandle::bulk_call` is the named follow-up. `FsBlobStore`
//! copies the artifact library's `FsLibrarySource` semantics (temp-write + rename,
//! verify-on-read) and swaps out for the extracted artifact-library crate when that ships —
//! it is re-implemented here only because `mycelium-wasm-host` carries wasmtime
//! unconditionally.

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mycelium::{CapFilter, Capability, CapabilityReg, GossipAgent};
use sha2::{Digest, Sha256};
use tracing::warn;

/// Hard per-blob ceiling: a blob must fit one RPC reply frame (KV/signal frames are
/// size-gated at ~9.94 MiB; 8 MiB leaves envelope headroom).
pub const MAX_BLOB_BYTES: usize = 8 * 1024 * 1024;

/// RPC kind for peer blob fetch: 32-byte id in, blob bytes (or empty for miss) out.
pub const BLOB_FETCH_KIND: &str = "reason.blob.fetch";

/// Capability advertised by nodes running [`spawn_blob_server`].
const BLOB_CAP_NS: &str = "reason";
const BLOB_CAP_NAME: &str = "blob-cache";

// ── Identity ─────────────────────────────────────────────────────────────────

/// Content address of a blob — its SHA-256. Equality of ids is equality of bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BlobId(pub [u8; 32]);

impl BlobId {
    /// The content address of `bytes`.
    pub fn of(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    /// Lowercase 64-hex form (also the on-disk filename).
    pub fn to_hex(&self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Parse a 64-hex string; `None` on wrong length or non-hex.
    pub fn from_hex(s: &str) -> Option<Self> {
        if s.len() != 64 {
            return None;
        }
        let mut out = [0u8; 32];
        for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
            let hi = (chunk[0] as char).to_digit(16)?;
            let lo = (chunk[1] as char).to_digit(16)?;
            out[i] = ((hi << 4) | lo) as u8;
        }
        Some(Self(out))
    }
}

impl fmt::Display for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

// ── Local store ──────────────────────────────────────────────────────────────

/// Filesystem content-addressed blob store. One file per blob, named by its hex id.
///
/// No locks: writes are **complete-or-absent** (uniquely-named temp file + rename — the
/// `FsLibrarySource` discipline), so a concurrent reader never observes a partial blob,
/// and reads verify the hash so a corrupted-on-disk blob is a miss, never bad data.
pub struct FsBlobStore {
    dir: PathBuf,
}

impl FsBlobStore {
    /// Open a blob directory, creating it (and parents) if absent.
    pub fn open(dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    fn path_of(&self, id: &BlobId) -> PathBuf {
        self.dir.join(id.to_hex())
    }

    /// Store `bytes`, returning their content address. Idempotent — storing bytes the
    /// store already holds is a no-op returning the same id. Rejects blobs over
    /// [`MAX_BLOB_BYTES`] with `InvalidInput` (they could never travel the mesh).
    pub fn put(&self, bytes: &[u8]) -> std::io::Result<BlobId> {
        if bytes.len() > MAX_BLOB_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("blob is {} bytes; the single-frame ceiling is {MAX_BLOB_BYTES}", bytes.len()),
            ));
        }
        let id = BlobId::of(bytes);
        let path = self.path_of(&id);
        if path.exists() {
            return Ok(id);
        }
        static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let tmp = self.dir.join(format!(
            ".tmp-{}-{}-{}",
            id.to_hex(),
            std::process::id(),
            TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        ));
        std::fs::write(&tmp, bytes)?;
        std::fs::rename(&tmp, &path)?;
        Ok(id)
    }

    /// Read the blob for `id`, verifying the content address. A hash mismatch (disk
    /// corruption, partial legacy write) is a miss — `None` + a warning, never bad bytes.
    pub fn get(&self, id: &BlobId) -> Option<Bytes> {
        let bytes = std::fs::read(self.path_of(id)).ok()?;
        if BlobId::of(&bytes) != *id {
            warn!(id = %id, "blob failed content verification on read — treating as absent");
            return None;
        }
        Some(Bytes::from(bytes))
    }

    /// Whether a file for `id` exists (unverified presence check).
    pub fn contains(&self, id: &BlobId) -> bool {
        self.path_of(id).exists()
    }
}

// ── Mesh serving ─────────────────────────────────────────────────────────────

/// RAII handle for a running blob server. Dropping it retracts the `reason/blob-cache`
/// capability and aborts the fetch loop.
pub struct BlobServerHandle {
    _cap: CapabilityReg,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for BlobServerHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Advertise this node as a blob provider and serve [`BLOB_FETCH_KIND`] RPCs against
/// `store`. Reply is the blob bytes, or **empty bytes** for a miss / malformed id (the
/// caller verifies the hash anyway, so empty is unambiguous — no valid blob is empty
/// with a non-empty id... a zero-length blob's id is the SHA-256 of "" and callers
/// verify, so even that degenerate case round-trips correctly).
pub fn spawn_blob_server(agent: &Arc<GossipAgent>, store: Arc<FsBlobStore>) -> BlobServerHandle {
    let cap = agent
        .capabilities()
        .advertise_capability(Capability::new(BLOB_CAP_NS, BLOB_CAP_NAME), Duration::from_secs(30));
    let service = agent.service();
    let mut rx = service.rpc_rx(BLOB_FETCH_KIND);
    let task = tokio::spawn(async move {
        while let Some(req) = rx.recv().await {
            let payload = req.payload();
            let reply = <[u8; 32]>::try_from(payload.as_ref())
                .ok()
                .and_then(|id| store.get(&BlobId(id)))
                .unwrap_or_else(Bytes::new);
            service.rpc_respond(&req, reply);
        }
    });
    BlobServerHandle { _cap: cap, task }
}

// ── Mesh fetching ────────────────────────────────────────────────────────────

/// Local-first, mesh-fallback blob store: `get` serves a local hit, else asks each
/// `reason/blob-cache` provider in turn, verifies the reply against the content
/// address, and write-back caches it locally.
#[derive(Clone)]
pub struct MeshBlobStore {
    agent: Arc<GossipAgent>,
    local: Arc<FsBlobStore>,
    fetch_timeout: Duration,
}

impl MeshBlobStore {
    pub fn new(agent: Arc<GossipAgent>, local: Arc<FsBlobStore>, fetch_timeout: Duration) -> Self {
        Self { agent, local, fetch_timeout }
    }

    /// The local tier (write-back cache target).
    pub fn local(&self) -> &Arc<FsBlobStore> {
        &self.local
    }

    /// Store locally. Peers fetch it from here once this node runs [`spawn_blob_server`].
    pub fn put(&self, bytes: &[u8]) -> std::io::Result<BlobId> {
        self.local.put(bytes)
    }

    /// Local hit, else fetch from mesh providers. Every mesh reply is verified against
    /// the content address before being cached or returned — providers are untrusted.
    pub async fn get(&self, id: &BlobId) -> Option<Bytes> {
        if let Some(bytes) = self.local.get(id) {
            return Some(bytes);
        }
        let providers = self.agent.capabilities().resolve(&CapFilter::new(BLOB_CAP_NS, BLOB_CAP_NAME));
        let me = self.agent.node_id().clone();
        for (node, _) in providers {
            if node == me {
                continue; // self is the local tier, already missed
            }
            let reply = self
                .agent
                .service()
                .rpc_call(node.clone(), BLOB_FETCH_KIND, Bytes::copy_from_slice(&id.0), self.fetch_timeout)
                .await;
            match reply {
                Ok(bytes) if !bytes.is_empty() && BlobId::of(&bytes) == *id => {
                    if let Err(e) = self.local.put(&bytes) {
                        warn!(id = %id, error = %e, "write-back cache of mesh blob failed");
                    }
                    return Some(bytes);
                }
                Ok(bytes) if !bytes.is_empty() => {
                    warn!(id = %id, provider = %node, "mesh blob failed content verification — trying next provider");
                }
                _ => {} // miss or RPC error → next provider
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (tempfile::TempDir, FsBlobStore) {
        let dir = tempfile::tempdir().unwrap();
        let s = FsBlobStore::open(dir.path()).unwrap();
        (dir, s)
    }

    #[test]
    fn roundtrip_and_idempotency() {
        let (_d, s) = store();
        let id = s.put(b"payload").unwrap();
        assert_eq!(id, BlobId::of(b"payload"));
        assert_eq!(s.get(&id).unwrap().as_ref(), b"payload");
        assert!(s.contains(&id));
        // Idempotent: same bytes, same id, still one file.
        assert_eq!(s.put(b"payload").unwrap(), id);
    }

    #[test]
    fn hex_roundtrip() {
        let id = BlobId::of(b"x");
        assert_eq!(BlobId::from_hex(&id.to_hex()), Some(id));
        assert_eq!(BlobId::from_hex("zz"), None);
        assert_eq!(BlobId::from_hex(&"g".repeat(64)), None);
        assert_eq!(format!("{id}"), id.to_hex());
    }

    #[test]
    fn oversize_rejected() {
        let (_d, s) = store();
        let big = vec![0u8; MAX_BLOB_BYTES + 1];
        let err = s.put(&big).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        // The exact ceiling is accepted.
        assert!(s.put(&vec![0u8; MAX_BLOB_BYTES]).is_ok());
    }

    #[test]
    fn corruption_returns_none() {
        let (_d, s) = store();
        let id = s.put(b"honest bytes").unwrap();
        // Corrupt the file behind the store's back.
        std::fs::write(s.path_of(&id), b"tampered").unwrap();
        assert!(s.get(&id).is_none(), "verify-on-read catches the tamper");
        assert!(s.contains(&id), "contains() is presence-only, unverified");
    }
}
