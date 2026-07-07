//! Content-addressed artifact identity, verification, and fetch sourcing — the **pull + verify**
//! front of the M12 mechanism (the loop's "pull + verify + instantiate"; see plan §E.1).
//!
//! An artifact's identity **is** its SHA-256 digest. That is the integrity guarantee: a node
//! resolves a requirement to an [`ArtifactId`] from a catalog it trusts, fetches the bytes from
//! *any* source (the source is **untrusted** — bytes are verified after fetch, before
//! instantiation), and a hash match proves the bytes are exactly what the catalog named. This is
//! why §E.4.4's bulk-fetch question is not a trust question: content-addressing makes the
//! transport interchangeable. A signed-provenance layer (Ed25519 over the id) is a follow-up;
//! content-hash integrity is the load-bearing v0 guarantee.

use std::collections::HashMap;

use bytes::Bytes;
use sha2::{Digest, Sha256};

/// Content address of an artifact: its SHA-256 digest. `Display`/`FromStr` use lowercase hex.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ArtifactId([u8; 32]);

impl ArtifactId {
    /// The content address of `bytes` (their SHA-256 digest).
    pub fn of(bytes: &[u8]) -> Self {
        let mut h = Sha256::new();
        h.update(bytes);
        Self(h.finalize().into())
    }

    /// Parse a 64-char lowercase/uppercase hex digest.
    pub fn from_hex(s: &str) -> Result<Self, ArtifactIdError> {
        if s.len() != 64 {
            return Err(ArtifactIdError::Length(s.len()));
        }
        let mut out = [0u8; 32];
        for (i, byte) in out.iter_mut().enumerate() {
            let hi = hex_val(s.as_bytes()[i * 2])?;
            let lo = hex_val(s.as_bytes()[i * 2 + 1])?;
            *byte = (hi << 4) | lo;
        }
        Ok(Self(out))
    }

    /// Construct from raw digest bytes (e.g. parsed from a catalog key).
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The 32 raw digest bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Lowercase hex form.
    pub fn to_hex(&self) -> String {
        use std::fmt::Write;
        let mut s = String::with_capacity(64);
        for b in &self.0 {
            let _ = write!(s, "{b:02x}");
        }
        s
    }
}

fn hex_val(c: u8) -> Result<u8, ArtifactIdError> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(ArtifactIdError::NotHex(c as char)),
    }
}

impl std::fmt::Display for ArtifactId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl std::fmt::Debug for ArtifactId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ArtifactId({})", self.to_hex())
    }
}

impl std::str::FromStr for ArtifactId {
    type Err = ArtifactIdError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

/// Why an [`ArtifactId`] could not be parsed from a string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtifactIdError {
    /// Not 64 hex characters.
    Length(usize),
    /// A non-hex character was found.
    NotHex(char),
}

impl std::fmt::Display for ArtifactIdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Length(n) => write!(f, "artifact id must be 64 hex chars, got {n}"),
            Self::NotHex(c) => write!(f, "artifact id has non-hex char {c:?}"),
        }
    }
}

impl std::error::Error for ArtifactIdError {}

/// The fetched bytes did not hash to the expected content address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyError {
    pub expected: ArtifactId,
    pub actual:   ArtifactId,
}

impl std::fmt::Display for VerifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "artifact hash mismatch: expected {}, got {}", self.expected, self.actual)
    }
}

impl std::error::Error for VerifyError {}

/// Verify `bytes` hash to `expected`. The integrity check for content-addressed pull: run it
/// **before** instantiation so corrupt or substituted bytes never reach the wasm engine.
pub fn verify_artifact(bytes: &[u8], expected: &ArtifactId) -> Result<(), VerifyError> {
    let actual = ArtifactId::of(bytes);
    if actual == *expected {
        Ok(())
    } else {
        Err(VerifyError { expected: *expected, actual })
    }
}

/// What a deployable artifact *is* — the runtime-dispatch axis of install
/// (`docs/design/artifact-library.md` §4). A [`WasmComponent`](Self::WasmComponent) is pulled,
/// verified, and **instantiated** (`WasmHost`); a [`Blob`](Self::Blob) is pulled, verified, and
/// **placed** for a node-local runtime to consume (model weights, a data pack), with its
/// capability advertisement probe-gated on that runtime answering. The kind travels in the
/// catalogue entry; a node without a registered runtime for a kind simply never self-elects to
/// install it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ArtifactKind {
    /// A WASM component: instantiate in the sandboxed host and serve its `handle` export.
    WasmComponent,
    /// Opaque bytes a node-local runtime consumes from disk (LLM/ONNX weights, data packs).
    Blob,
}

impl ArtifactKind {
    /// The kind's wire byte (catalogue-entry encoding).
    pub const fn as_u8(self) -> u8 {
        match self {
            Self::WasmComponent => 0,
            Self::Blob => 1,
        }
    }

    /// Inverse of [`as_u8`](Self::as_u8); `None` for an unknown kind byte (the decoder rejects
    /// the entry — a node never guesses how to install something it can't name).
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::WasmComponent),
            1 => Some(Self::Blob),
            _ => None,
        }
    }
}

/// A place artifact bytes can be fetched from by content address. The transport is pluggable and
/// **untrusted** (content-addressing verifies after fetch): mesh bulk, an OCI/Warg registry, a
/// local cache, etc. Shipped implementations: [`InMemorySource`] (embedding/tests),
/// [`FsLibrarySource`] (the durable library directory), and `MeshArtifactSource` (peer pull over
/// the `artifact.fetch` RPC, prefetched into a verified cache).
pub trait ArtifactSource {
    /// Fetch the bytes for `id`, or `None` if this source does not hold it.
    fn fetch(&self, id: &ArtifactId) -> Option<Bytes>;
}

/// An in-memory artifact source — for embedding and tests. Keys are derived from the bytes, so a
/// stored artifact is always self-consistent.
#[derive(Default)]
pub struct InMemorySource {
    by_id: HashMap<ArtifactId, Bytes>,
}

impl InMemorySource {
    pub fn new() -> Self {
        Self::default()
    }

    /// Store `bytes`, returning their content address.
    pub fn insert(&mut self, bytes: impl Into<Bytes>) -> ArtifactId {
        let bytes = bytes.into();
        let id = ArtifactId::of(&bytes);
        self.by_id.insert(id, bytes);
        id
    }
}

impl ArtifactSource for InMemorySource {
    fn fetch(&self, id: &ArtifactId) -> Option<Bytes> {
        self.by_id.get(id).cloned()
    }
}

/// A durable, filesystem-backed artifact library: a directory of content-addressed blobs, each
/// stored under its 64-hex [`ArtifactId`] filename. This is the durable **origin tier** of the
/// artifact-library design (`docs/design/artifact-library.md` §2): it survives process
/// restarts, is shareable via a mounted volume, and becomes cluster-servable by handing it to
/// `serve_artifacts`. The library's *self-description* (which capability each blob provides,
/// cost, provenance) lives in its manifest — see `catalog::Manifest`; non-hex filenames (the
/// manifest, temp files) are ignored by [`list`](Self::list) and unreachable via `fetch`.
pub struct FsLibrarySource {
    dir: std::path::PathBuf,
}

impl FsLibrarySource {
    /// Open a library directory, creating it (and parents) if absent.
    pub fn open(dir: impl Into<std::path::PathBuf>) -> std::io::Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    /// The library directory.
    pub fn dir(&self) -> &std::path::Path {
        &self.dir
    }

    fn blob_path(&self, id: &ArtifactId) -> std::path::PathBuf {
        self.dir.join(id.to_hex())
    }

    /// Store `bytes` as a content-addressed blob, returning their [`ArtifactId`].
    ///
    /// The write is **complete-or-absent**: bytes land in a uniquely-named temp file and are
    /// renamed into place only after the full write, so a concurrent reader never observes a
    /// partial blob (the same manifest-last discipline as the wiki's `FsStore`). Idempotent —
    /// storing bytes the library already holds is a no-op returning the same id.
    pub fn store(&self, bytes: &[u8]) -> std::io::Result<ArtifactId> {
        let id = ArtifactId::of(bytes);
        let path = self.blob_path(&id);
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

    /// Remove the blob for `id`, returning whether it was present.
    pub fn remove(&self, id: &ArtifactId) -> std::io::Result<bool> {
        match std::fs::remove_file(self.blob_path(id)) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// All artifact ids currently held — files whose name parses as a 64-hex content address.
    /// The manifest, temp files, and anything else are skipped.
    pub fn list(&self) -> std::io::Result<Vec<ArtifactId>> {
        let mut ids = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            if let Some(name) = entry.file_name().to_str()
                && let Ok(id) = ArtifactId::from_hex(name)
            {
                ids.push(id);
            }
        }
        Ok(ids)
    }
}

impl ArtifactSource for FsLibrarySource {
    /// Read the blob for `id`; `None` if absent or unreadable. Like every source, the library
    /// is *untrusted* — callers verify the bytes against the content address after fetch, so a
    /// corrupted-on-disk blob is detected at the pull/provision boundary, not trusted here.
    fn fetch(&self, id: &ArtifactId) -> Option<Bytes> {
        std::fs::read(self.blob_path(id)).ok().map(Bytes::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_is_stable_and_hex_round_trips() {
        let id = ArtifactId::of(b"hello component");
        assert_eq!(id, ArtifactId::of(b"hello component"));
        assert_ne!(id, ArtifactId::of(b"other"));
        let hex = id.to_hex();
        assert_eq!(hex.len(), 64);
        assert_eq!(ArtifactId::from_hex(&hex).unwrap(), id);
        assert_eq!(hex.parse::<ArtifactId>().unwrap(), id);
    }

    #[test]
    fn from_hex_rejects_bad_input() {
        assert_eq!(ArtifactId::from_hex("abc"), Err(ArtifactIdError::Length(3)));
        let mut bad = "a".repeat(64);
        bad.replace_range(0..1, "z");
        assert_eq!(ArtifactId::from_hex(&bad), Err(ArtifactIdError::NotHex('z')));
    }

    #[test]
    fn verify_passes_on_match_and_fails_on_tamper() {
        let bytes = b"the real artifact";
        let id = ArtifactId::of(bytes);
        assert!(verify_artifact(bytes, &id).is_ok());

        let tampered = b"the real artifacX";
        let err = verify_artifact(tampered, &id).unwrap_err();
        assert_eq!(err.expected, id);
        assert_ne!(err.actual, id);
    }

    #[test]
    fn in_memory_source_round_trips_and_misses_unknown() {
        let mut src = InMemorySource::new();
        let id = src.insert(Bytes::from_static(b"artifact bytes"));
        assert_eq!(src.fetch(&id).as_deref(), Some(&b"artifact bytes"[..]));

        let unknown = ArtifactId::of(b"never stored");
        assert!(src.fetch(&unknown).is_none());

        // What a source returns always re-verifies against the id it was fetched by.
        let fetched = src.fetch(&id).unwrap();
        assert!(verify_artifact(&fetched, &id).is_ok());
    }

    #[test]
    fn artifact_kind_wire_bytes_round_trip_and_reject_unknown() {
        assert_eq!(ArtifactKind::from_u8(ArtifactKind::WasmComponent.as_u8()),
                   Some(ArtifactKind::WasmComponent));
        assert_eq!(ArtifactKind::from_u8(ArtifactKind::Blob.as_u8()), Some(ArtifactKind::Blob));
        assert_eq!(ArtifactKind::from_u8(0xFF), None);
    }

    /// Fresh, unique library dir under the system temp dir (no tempfile dep in this crate).
    fn scratch_lib_dir(tag: &str) -> std::path::PathBuf {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "mycelium-fs-library-{tag}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        ))
    }

    #[test]
    fn fs_library_stores_fetches_and_survives_reopen() {
        let dir = scratch_lib_dir("roundtrip");
        let lib = FsLibrarySource::open(&dir).expect("open");
        let id = lib.store(b"component bytes").expect("store");
        assert_eq!(id, ArtifactId::of(b"component bytes"), "content address is the identity");
        assert_eq!(lib.fetch(&id).as_deref(), Some(&b"component bytes"[..]));
        assert!(verify_artifact(&lib.fetch(&id).unwrap(), &id).is_ok());

        // Idempotent re-store, unknown miss.
        assert_eq!(lib.store(b"component bytes").unwrap(), id);
        assert!(lib.fetch(&ArtifactId::of(b"never stored")).is_none());

        // Durability: a fresh handle on the same directory (≈ process restart) still serves it.
        drop(lib);
        let reopened = FsLibrarySource::open(&dir).expect("reopen");
        assert_eq!(reopened.fetch(&id).as_deref(), Some(&b"component bytes"[..]));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn fs_library_lists_only_content_addressed_blobs_and_removes() {
        let dir = scratch_lib_dir("list");
        let lib = FsLibrarySource::open(&dir).expect("open");
        let a = lib.store(b"artifact a").unwrap();
        let b = lib.store(b"artifact b").unwrap();
        // Non-blob files (a manifest, a stray temp file) are invisible to list().
        std::fs::write(dir.join("manifest"), b"not a blob").unwrap();
        std::fs::write(dir.join(".tmp-leftover"), b"crashed writer residue").unwrap();

        let mut listed = lib.list().unwrap();
        listed.sort_by_key(|id| id.to_hex());
        let mut expect = vec![a, b];
        expect.sort_by_key(|id| id.to_hex());
        assert_eq!(listed, expect);

        assert!(lib.remove(&a).unwrap(), "removing a held blob reports true");
        assert!(!lib.remove(&a).unwrap(), "removing an absent blob reports false");
        assert!(lib.fetch(&a).is_none());
        assert_eq!(lib.fetch(&b).as_deref(), Some(&b"artifact b"[..]));

        std::fs::remove_dir_all(&dir).ok();
    }
}
