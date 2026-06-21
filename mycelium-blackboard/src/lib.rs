//! # mycelium-blackboard — shared working memory on Mycelium's public API
//!
//! Blackboard-style **opportunistic multi-agent reasoning over typed facts**, rebuilt on Mycelium
//! the same way [`mycelium-tuple-space`](https://docs.rs/mycelium-tuple-space) rebuilt work
//! distribution. The design rationale + worked example (a community microgrid) live in
//! `docs/plans/mycelium-blackboard.md`; the phased build plan in
//! `docs/plans/v2-wsg-g3-blackboard.md`.
//!
//! ## The reading / consuming split (Linda's `rd` vs `in`)
//!
//! A blackboard surfaces one clean distinction:
//!
//! - **Reading facts is unconditional and concurrent** (`rd`). Many agents observe the same fact —
//!   a forecaster and a pricing agent both react to a surplus-energy fact. Mycelium's substrate
//!   already does this perfectly via gossiped KV + boundary predicates; nothing new is needed.
//! - **Consuming facts is competitive and exactly-once** (`in`). Acting on a *finite* fact (the
//!   surplus exists once) means two agents whose triggers both match must **race for an atomic
//!   claim** — exactly one consumes it, the loser's claim returns empty, and a winner that drops
//!   mid-work has the claim re-queued.
//!
//! This crate adds the **one** missing primitive: **competitive destructive claim-by-predicate**.
//! Everything else (fact propagation, trigger predicates, evaporation) is the substrate's `rd`.
//!
//! ## Why not the tuple space
//!
//! The tuple space routes by *position* (named FIFO lanes, topology known per stage). The blackboard
//! routes by *content*: a consumer's criterion is a **predicate over fact attributes**, and the
//! topology is *emergent per item* — a surplus fact routes through entirely different agents than a
//! deficit fact. A lane per (fact-type × interest) explodes against each agent's private, changing
//! declarations. The predicate language is the **capability attribute-filter grammar** (equality +
//! presence), *not* unification — already implemented, already understood, and enough for trigger
//! conditions.
//!
//! ## Status (WS-G / G3)
//!
//! **Phase 1 (this module): the in-memory core.** [`BoardStore`] is the pure claim-by-predicate
//! primitive — `post` / `read` / `claim` / `ack` / `release`, single-owner and exactly-once,
//! testable without a cluster (the [`mycelium-tuple-space`] `TupleStore::transient` analogue).
//! Later phases add WAL durability (against the shared exactly-once-effect contract), emergent
//! primary/secondary roles, the HTTP gateway + SDKs, and the worked example.

use std::collections::BTreeMap;

mod store;
pub use store::{BoardDepth, BoardStats, BoardStore};

/// A typed fact on the board: an attribute map (the matchable surface) plus an opaque payload.
/// Facts are non-destructively *read* by many and destructively *claimed* by at most one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fact {
    /// Board-assigned id; also the claim handle once the fact is claimed.
    pub id: u64,
    /// The matchable surface — string attributes a [`Predicate`] tests (the content-plane analogue
    /// of a capability's attributes). Routing is encoded here, never in the payload.
    pub attributes: BTreeMap<String, String>,
    /// Opaque payload (the substrate never matches on it).
    pub payload: bytes::Bytes,
}

/// One attribute constraint in a [`Predicate`]. Deliberately the capability attribute-filter
/// grammar — equality + presence — **not** unification/structural matching (scope creep until
/// demonstrated; see the crate docs' non-goals).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttrMatch {
    /// The attribute must be present and equal to this value.
    Equals(String),
    /// The attribute must be present (any value).
    Present,
}

/// A conjunctive predicate over fact attributes: **all** constraints must hold for a fact to match.
/// An empty predicate matches every fact.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Predicate {
    attrs: BTreeMap<String, AttrMatch>,
}

impl Predicate {
    /// A predicate matching every fact. Add constraints with [`eq`](Self::eq) / [`present`](Self::present).
    pub fn new() -> Self {
        Self { attrs: BTreeMap::new() }
    }

    /// Require attribute `key` to equal `value`.
    pub fn eq(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attrs.insert(key.into(), AttrMatch::Equals(value.into()));
        self
    }

    /// Require attribute `key` to be present (any value).
    pub fn present(mut self, key: impl Into<String>) -> Self {
        self.attrs.insert(key.into(), AttrMatch::Present);
        self
    }

    /// True iff every constraint holds against `attributes`.
    pub fn matches(&self, attributes: &BTreeMap<String, String>) -> bool {
        self.attrs.iter().all(|(k, m)| match m {
            AttrMatch::Present => attributes.contains_key(k),
            AttrMatch::Equals(v) => attributes.get(k).is_some_and(|got| got == v),
        })
    }

    /// Number of constraints (0 = match-all).
    pub fn len(&self) -> usize {
        self.attrs.len()
    }

    /// Whether this is the match-all predicate.
    pub fn is_empty(&self) -> bool {
        self.attrs.is_empty()
    }
}

/// Errors from the board API.
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum BlackboardError {
    /// Unknown claim id — already acked, already released, re-queued by the in-flight deadline, or
    /// never claimed.
    NotFound,
    /// No node currently serves this board (later phases — role resolution).
    NoProvider,
    /// Transport error talking to the board primary (later phases).
    Rpc(String),
}

impl std::fmt::Display for BlackboardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlackboardError::NotFound => write!(f, "unknown claim id"),
            BlackboardError::NoProvider => write!(f, "no blackboard primary resolvable"),
            BlackboardError::Rpc(s) => write!(f, "rpc error: {s}"),
        }
    }
}

impl std::error::Error for BlackboardError {}
