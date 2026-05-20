//! OSGi-style capability/requirement model with emergent group formation.
//!
//! Nodes advertise what they provide (`Capability`) under `cap/{node_id}/{ns}/{name}`
//! and declare what they need (`CapFilter`) under `req/{node_id}/{ns}/{name}`.
//! `CapFilter::matches(capability)` decides whether a provider satisfies a
//! requirement.
//!
//! `CapabilityGroupDef` defines an *emergent group*: any node whose own
//! capabilities match the def's `filter` self-joins, no operator coordination
//! required. The protocol's biological framing: each organism independently
//! determines whether it fits a niche, no coordinator assigns membership.

use crate::config::GroupTopologyPolicy;
use crate::framing::bincode_cfg;
use crate::node_id::NodeId;
use std::collections::BTreeMap;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, sync::Arc};
use tokio::sync::oneshot;

/// A typed capability/requirement attribute value.
///
/// `Text`/`Integer`/`Bool`/`Version` are totally ordered. `Float` uses
/// `f64::partial_cmp`, so NaN comparisons return `None` and fail any
/// gt/gte/lt/lte constraint.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CapValue {
    Text(Arc<str>),
    Integer(i64),
    Float(f64),
    Bool(bool),
    /// Semantic version triple (major, minor, patch). Lexicographic order.
    Version([u32; 3]),
}

/// Filter operator over a single attribute.
///
/// Type-cross constraints (e.g. comparing `Integer` against `Text`) always
/// fail to match — different `CapValue` variants are incomparable.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CapConstraint {
    Eq(CapValue),
    Ne(CapValue),
    Gt(CapValue),
    Gte(CapValue),
    Lt(CapValue),
    Lte(CapValue),
}

/// Discriminated filter over a `{namespace, name}` capability shape plus zero
/// or more attribute constraints. All constraints must match for the filter
/// to match a capability.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CapFilter {
    pub namespace:  Arc<str>,
    pub name:       Arc<str>,
    pub attributes: BTreeMap<Arc<str>, CapConstraint>,
}

/// What a node advertises it can provide.
///
/// Stored at `cap/{node_id}/{namespace}/{name}` as bincode-encoded bytes.
/// `attributes` carries the typed values that filters match against.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Capability {
    pub namespace:  Arc<str>,
    pub name:       Arc<str>,
    pub attributes: BTreeMap<Arc<str>, CapValue>,
}

/// Push notification delivered by `watch_capabilities`.
#[derive(Clone, Debug)]
pub enum CapabilityEvent {
    Added {
        node_id:    NodeId,
        capability: Capability,
    },
    Removed {
        node_id:   NodeId,
        namespace: Arc<str>,
        name:      Arc<str>,
    },
}

/// Outcome of a single requirement-resolution snapshot.
#[derive(Clone, Debug)]
pub enum RequirementStatus {
    Satisfied   { providers: Vec<(NodeId, Capability)> },
    Unsatisfied { filter:    CapFilter },
}

/// Drop to retract an advertised capability. The dropping side gossips a
/// tombstone for `cap/{node_id}/{namespace}/{name}`.
pub struct CapabilityHandle {
    pub(crate) _retract: oneshot::Sender<()>,
}

/// Drop to retract a declared requirement. Tombstones both `req/{…}` and any
/// `sys/load/{…}/req/{…}` opacity entry that this requirement may have
/// written when unsatisfied (see Phase 3d auto-opacity).
pub struct RequirementHandle {
    pub(crate) _retract: oneshot::Sender<()>,
}

/// Definition of an emergent capability group.
///
/// A node whose own capabilities match `filter` (evaluated against every
/// known `cap/{self}/*` entry) self-joins via `join_group(name)`. When the
/// definer drops its `CapabilityGroupHandle` the def is tombstoned and all
/// members self-leave.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CapabilityGroupDef {
    pub filter:          CapFilter,
    /// Optional per-group topology policy. Subordinate to
    /// `config.topology_policies[group]` — config always wins.
    pub topology_policy: Option<GroupTopologyPolicy>,
}

/// Drop to tombstone `cap-group/{group}`. All matching members will see the
/// tombstone via gossip and self-leave the group.
pub struct CapabilityGroupHandle {
    pub(crate) _retract: oneshot::Sender<()>,
    pub(crate) group:    Arc<str>,
}

impl CapabilityGroupHandle {
    /// Name of the group this handle defines.
    pub fn group(&self) -> &Arc<str> {
        &self.group
    }
}

// ── Comparison and matching ─────────────────────────────────────────────────

/// Total/partial ordering over two `CapValue`s.
///
/// Returns `None` when the variants disagree (e.g. comparing `Text` to
/// `Integer`) or when either value is `Float(NaN)`. All gt/gte/lt/lte
/// constraints in `CapConstraint` treat `None` as a non-match.
pub(crate) fn partial_cmp_cap(a: &CapValue, b: &CapValue) -> Option<Ordering> {
    match (a, b) {
        (CapValue::Text(x),    CapValue::Text(y))    => Some(x.as_ref().cmp(y.as_ref())),
        (CapValue::Integer(x), CapValue::Integer(y)) => Some(x.cmp(y)),
        (CapValue::Float(x),   CapValue::Float(y))   => x.partial_cmp(y),
        (CapValue::Bool(x),    CapValue::Bool(y))    => Some(x.cmp(y)),
        (CapValue::Version(x), CapValue::Version(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

impl CapConstraint {
    /// Tests whether `value` satisfies this constraint.
    pub fn matches(&self, value: &CapValue) -> bool {
        match self {
            CapConstraint::Eq(expected)  => value == expected,
            CapConstraint::Ne(expected)  => value != expected,
            CapConstraint::Gt(threshold) => matches!(partial_cmp_cap(value, threshold), Some(Ordering::Greater)),
            CapConstraint::Gte(threshold) => matches!(
                partial_cmp_cap(value, threshold),
                Some(Ordering::Greater | Ordering::Equal),
            ),
            CapConstraint::Lt(threshold) => matches!(partial_cmp_cap(value, threshold), Some(Ordering::Less)),
            CapConstraint::Lte(threshold) => matches!(
                partial_cmp_cap(value, threshold),
                Some(Ordering::Less | Ordering::Equal),
            ),
        }
    }
}

impl CapFilter {
    /// True iff `cap` shares the same `(namespace, name)` and every attribute
    /// constraint in this filter matches a present attribute value on `cap`.
    /// Missing attributes always fail.
    pub fn matches(&self, cap: &Capability) -> bool {
        if self.namespace != cap.namespace || self.name != cap.name {
            return false;
        }
        for (attr, constraint) in &self.attributes {
            let Some(value) = cap.attributes.get(attr) else { return false; };
            if !constraint.matches(value) { return false; }
        }
        true
    }

    /// Convenience constructor for a name-only filter.
    pub fn new<N: Into<Arc<str>>, S: Into<Arc<str>>>(namespace: N, name: S) -> Self {
        Self {
            namespace:  namespace.into(),
            name:       name.into(),
            attributes: BTreeMap::new(),
        }
    }

    /// Builder-style attribute addition.
    pub fn with<A: Into<Arc<str>>>(mut self, attribute: A, constraint: CapConstraint) -> Self {
        self.attributes.insert(attribute.into(), constraint);
        self
    }
}

impl Capability {
    /// Convenience constructor without attributes.
    pub fn new<N: Into<Arc<str>>, S: Into<Arc<str>>>(namespace: N, name: S) -> Self {
        Self {
            namespace:  namespace.into(),
            name:       name.into(),
            attributes: BTreeMap::new(),
        }
    }

    /// Builder-style attribute addition.
    pub fn with<A: Into<Arc<str>>>(mut self, attribute: A, value: CapValue) -> Self {
        self.attributes.insert(attribute.into(), value);
        self
    }
}

// ── Encode/decode (bincode) ─────────────────────────────────────────────────

macro_rules! impl_bincode_codec {
    ($t:ty) => {
        impl $t {
            #[allow(dead_code)] // some types are encoded only from external callers
            pub(crate) fn encode(&self) -> Bytes {
                let mut buf = BytesMut::new();
                let _ = bincode::serde::encode_into_std_write(self, &mut (&mut buf).writer(), bincode_cfg());
                buf.freeze()
            }
            #[allow(dead_code)]
            pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
                bincode::serde::decode_from_slice(bytes, bincode_cfg()).ok().map(|(v, _)| v)
            }
        }
    };
}

impl_bincode_codec!(Capability);
impl_bincode_codec!(CapFilter);
impl_bincode_codec!(CapabilityGroupDef);

#[cfg(test)]
mod tests {
    use super::*;

    fn iv(n: i64) -> CapValue { CapValue::Integer(n) }
    fn fv(n: f64) -> CapValue { CapValue::Float(n) }
    fn tv(s: &str) -> CapValue { CapValue::Text(Arc::from(s)) }

    #[test]
    fn partial_cmp_typed_pairs() {
        assert_eq!(partial_cmp_cap(&iv(1), &iv(2)), Some(Ordering::Less));
        assert_eq!(partial_cmp_cap(&iv(2), &iv(2)), Some(Ordering::Equal));
        assert_eq!(partial_cmp_cap(&fv(1.0), &fv(2.0)), Some(Ordering::Less));
        assert_eq!(partial_cmp_cap(&tv("a"), &tv("b")), Some(Ordering::Less));
        assert_eq!(partial_cmp_cap(&CapValue::Bool(false), &CapValue::Bool(true)), Some(Ordering::Less));
        assert_eq!(partial_cmp_cap(&CapValue::Version([1,2,3]), &CapValue::Version([1,2,4])), Some(Ordering::Less));
    }

    #[test]
    fn partial_cmp_mismatched_types_is_none() {
        assert!(partial_cmp_cap(&iv(1), &tv("1")).is_none());
        assert!(partial_cmp_cap(&fv(f64::NAN), &fv(0.0)).is_none());
    }

    #[test]
    fn constraint_matches_eq_ne() {
        assert!(CapConstraint::Eq(iv(5)).matches(&iv(5)));
        assert!(!CapConstraint::Eq(iv(5)).matches(&iv(6)));
        assert!(CapConstraint::Ne(iv(5)).matches(&iv(6)));
        assert!(!CapConstraint::Ne(iv(5)).matches(&iv(5)));
    }

    #[test]
    fn constraint_matches_ordering() {
        assert!(CapConstraint::Gt(iv(5)).matches(&iv(6)));
        assert!(!CapConstraint::Gt(iv(5)).matches(&iv(5)));
        assert!(CapConstraint::Gte(iv(5)).matches(&iv(5)));
        assert!(CapConstraint::Lt(iv(5)).matches(&iv(4)));
        assert!(CapConstraint::Lte(iv(5)).matches(&iv(5)));
    }

    #[test]
    fn constraint_type_mismatch_fails_ordering() {
        assert!(!CapConstraint::Gt(iv(5)).matches(&tv("6")));
    }

    #[test]
    fn filter_matches_capability() {
        let cap = Capability::new("compute", "gpu")
            .with("vram_gb", iv(24))
            .with("model",   tv("L40S"));
        let filter = CapFilter::new("compute", "gpu")
            .with("vram_gb", CapConstraint::Gte(iv(16)));
        assert!(filter.matches(&cap));
        let nope = CapFilter::new("compute", "gpu")
            .with("vram_gb", CapConstraint::Gte(iv(32)));
        assert!(!nope.matches(&cap));
        let wrong_name = CapFilter::new("compute", "tpu");
        assert!(!wrong_name.matches(&cap));
    }

    #[test]
    fn filter_missing_attribute_fails() {
        let cap = Capability::new("compute", "gpu");
        let filter = CapFilter::new("compute", "gpu")
            .with("vram_gb", CapConstraint::Gte(iv(16)));
        assert!(!filter.matches(&cap));
    }

    #[test]
    fn encode_decode_roundtrip() {
        let cap = Capability::new("ai", "agent")
            .with("model", tv("llama-3-70b"))
            .with("vram_gb", iv(80))
            .with("alive", CapValue::Bool(true));
        let bytes = cap.encode();
        let decoded = Capability::decode(&bytes).expect("decode");
        assert_eq!(cap, decoded);

        let filter = CapFilter::new("ai", "agent")
            .with("vram_gb", CapConstraint::Gte(iv(40)));
        let bytes = filter.encode();
        let decoded = CapFilter::decode(&bytes).expect("decode");
        assert_eq!(filter, decoded);

        let def = CapabilityGroupDef {
            filter:          filter.clone(),
            topology_policy: None,
        };
        let bytes = def.encode();
        let decoded = CapabilityGroupDef::decode(&bytes).expect("decode");
        assert_eq!(def, decoded);
    }
}
