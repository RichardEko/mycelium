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

/// A place artifact bytes can be fetched from by content address. The transport is pluggable and
/// **untrusted** (content-addressing verifies after fetch): mesh bulk, an OCI/Warg registry, a
/// local cache, etc. The mesh-bulk source is a follow-up (§E.4.4); v0 ships in-memory + local-dir.
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
}
