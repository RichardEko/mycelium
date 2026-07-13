//! The wiki record model — pages, sections, the manifest, and the query predicate. Serde-serialised
//! into the pluggable store; substrate-agnostic (no Mycelium types here).

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// A **stable, opaque** section id — minted once at section creation and never changed, so "edit
/// section X" and "rename/move section X" stay independent. Not derived from the heading or body.
pub type SectionId = Arc<str>;

/// One section: an editable heading + prose body, plus structured **attributes** — the *join keys*
/// (the shared id namespace with the external metrics store, e.g. `node = e_rl_rk`) and cross-cutting
/// *scope tags* (`topic = accountability`, `issue = climate`). Attributes are **not** typed facets
/// for computation (those live in the metrics store); they are how the meaning layer is located and
/// joined to the structure layer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Section {
    pub id:      SectionId,
    pub heading: String,
    pub body:    String,
    #[serde(default)]
    pub attributes: BTreeMap<String, String>,
}

/// Page-level metadata: the section render `order` + page attributes. The curator writes this
/// **last** (manifest-last), so a direct reader entering via the manifest never observes a
/// half-applied multi-section edit — the store's per-object atomicity does the rest.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub order: Vec<SectionId>,
    #[serde(default)]
    pub attributes: BTreeMap<String, String>,
}

/// A page as read: the manifest joined with its live section bodies, in render order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Page {
    pub path:       String,
    pub attributes: BTreeMap<String, String>,
    pub sections:   Vec<Section>,
}

/// A lightweight [`query`](crate::WikiStore::query) hit — which section on which page matched, without
/// the full-page join.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SectionRef {
    pub page:       String,
    pub id:         SectionId,
    pub heading:    String,
    pub attributes: BTreeMap<String, String>,
}

/// An attribute predicate for `query`: a section matches when it carries **every** `(key, value)`
/// pair (all-of / AND). Empty predicate matches all. Mirrors the blackboard's attribute predicate;
/// the retrieval is structured filter, **not** embedding similarity (that is RAG's job).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Predicate {
    pub equals: BTreeMap<String, String>,
}

impl Predicate {
    pub fn new() -> Self { Self::default() }
    /// Add a required `key == value` constraint (builder style).
    pub fn with(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.equals.insert(key.into(), value.into());
        self
    }
    /// Does `attributes` satisfy every constraint?
    pub fn matches(&self, attributes: &BTreeMap<String, String>) -> bool {
        self.equals.iter().all(|(k, v)| attributes.get(k) == Some(v))
    }
}

/// Errors from a [`WikiStore`](crate::WikiStore).
#[derive(Debug)]
pub enum WikiError {
    Io(std::io::Error),
    Serde(serde_json::Error),
    /// A page path that escapes the store root (`..`, absolute) — rejected.
    BadPath(String),
    /// A compare-and-swap write lost the race: the object's on-store version moved since the
    /// `expected` version the caller read (or the object already exists for an `expected = None`
    /// create). **Not an error to log-and-drop** — the caller must re-read the committed state and
    /// re-apply (idempotently), so a concurrent writer (e.g. a transient split-brain curator) can never
    /// silently clobber a landed edit. The store is at-least-once/never-lose without a distributed lock;
    /// exactly-once *effect* comes from the caller's idempotent reconcile.
    Conflict,
}

impl std::fmt::Display for WikiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WikiError::Io(e)      => write!(f, "wiki store io: {e}"),
            WikiError::Serde(e)   => write!(f, "wiki store serde: {e}"),
            WikiError::BadPath(p) => write!(f, "wiki store: unsafe page path {p:?}"),
            WikiError::Conflict   => write!(f, "wiki store: compare-and-swap version conflict (re-read and retry)"),
        }
    }
}
impl std::error::Error for WikiError {}
impl From<std::io::Error>    for WikiError { fn from(e: std::io::Error)    -> Self { WikiError::Io(e) } }
impl From<serde_json::Error> for WikiError { fn from(e: serde_json::Error) -> Self { WikiError::Serde(e) } }

/// Mint a **stable, content-independent** section id: `base32(fnv1a(group ‖ page ‖ mint-clock ‖
/// nonce))`, truncated. Identity-stable after minting — derived from birth coordinates, never from
/// the (freely-editable) heading or body.
pub fn mint_section_id(group: &str, page: &str, mint_clock: u64, nonce: u64) -> SectionId {
    let mut h: u64 = 0xcbf29ce4_84222325;
    let mut feed = |bytes: &[u8]| for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    };
    feed(group.as_bytes());
    feed(b"\0");
    feed(page.as_bytes());
    feed(&mint_clock.to_le_bytes());
    feed(&nonce.to_le_bytes());
    const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz"; // Crockford base32
    let mut s = String::with_capacity(13);
    s.push('s');
    let mut v = h;
    for _ in 0..12 {
        s.push(ALPHABET[(v & 0x1f) as usize] as char);
        v >>= 5;
    }
    Arc::from(s.as_str())
}
