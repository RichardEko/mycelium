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
use std::{cmp::Ordering, sync::{Arc, atomic::{AtomicBool, Ordering as AOrdering}}};
use tokio::sync::{oneshot, Notify};

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

/// Sort direction for [`CapRanking`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RankingOrder {
    Ascending,
    Descending,
}

/// Optional ranking applied to filter matches after the attribute constraints
/// have all been satisfied. Sorts providers by the named attribute using
/// [`partial_cmp_cap`]; providers missing the attribute (or whose value is
/// incomparable, e.g. `Float(NaN)`) sort to the end deterministically.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CapRanking {
    pub attribute: Arc<str>,
    pub order:     RankingOrder,
}

/// Discriminated filter over a `{namespace, name}` capability shape plus zero
/// or more attribute constraints. All constraints must match for the filter
/// to match a capability. An optional [`CapRanking`] post-orders the matches
/// without affecting which matches are selected.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CapFilter {
    pub namespace:  Arc<str>,
    pub name:       Arc<str>,
    pub attributes: BTreeMap<Arc<str>, CapConstraint>,
    /// `#[serde(default)]` so encoded filters that pre-date Phase 6 still
    /// decode (treating absence as "no ranking").
    #[serde(default)]
    pub ranking:    Option<CapRanking>,
    /// If set, `resolve` skips capabilities whose KV entry has not been
    /// refreshed within this window. Useful for crash-detection: a node
    /// that dies without sending tombstones will have its capabilities
    /// age out of `resolve` results after `max_age`. Must be larger than
    /// the `interval` passed to `advertise_capability` to avoid false
    /// positives — a multiple of 4–6× is typical.
    ///
    /// Not serialised (runtime-only filter; stored group requirements
    /// do not carry liveness constraints).
    #[serde(skip)]
    pub max_age:    Option<std::time::Duration>,
    /// When set, `matches` only accepts capabilities whose `schema_id` equals
    /// this value exactly. Gossip-propagated in `req/` KV entries so that
    /// requirements can declare a schema preference cluster-wide. `None` means
    /// accept any schema, including capabilities with no `schema_id` set.
    #[serde(default)]
    pub schema_id:  Option<Arc<str>>,
}

/// What a node advertises it can provide.
///
/// Stored at `cap/{node_id}/{namespace}/{name}` as bincode-encoded bytes.
/// `attributes` carries the typed values that filters match against.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Capability {
    pub namespace:          Arc<str>,
    pub name:               Arc<str>,
    pub attributes:         BTreeMap<Arc<str>, CapValue>,
    /// Allowlist of caller identities that may resolve this capability.
    /// Empty = unrestricted. Non-empty = `resolve_for_caller` returns this
    /// entry only when `CallerContext::caller_id` is in the list.
    #[serde(default)]
    pub authorized_callers: Vec<Arc<str>>,
    /// Optional schema identifier for this capability's invocation contract.
    /// Gossip-propagated. Callers can filter on this via `CapFilter::with_schema`
    /// to ensure they only wire to providers whose contract matches their
    /// expectations. Format is free-form — e.g. `"acme-ml/v1.2"` or a URN.
    /// `None` means unversioned / no schema constraint.
    #[serde(default)]
    pub schema_id:          Option<Arc<str>>,
    /// JSON Schema (as a JSON string) describing the expected request payload.
    /// Gossip-propagated alongside the capability so callers can inspect the
    /// input contract without a separate KV lookup.
    #[serde(default)]
    pub input_schema:       Option<Arc<str>>,
    /// JSON Schema describing the response payload.
    #[serde(default)]
    pub output_schema:      Option<Arc<str>>,
}

/// Caller identity passed to [`GossipAgent::resolve_for_caller`].
///
/// For unauthenticated internal callers use `CallerContext::unrestricted()`.
/// For language-bridge or SkillRunner callers set `caller_id` to the
/// identity string declared in `[capability.policy].authorized_callers`.
#[derive(Clone, Debug, Default)]
pub struct CallerContext {
    /// Opaque identity string for the caller. Compared against
    /// `Capability::authorized_callers` at resolve time.
    pub caller_id: Option<Arc<str>>,
}

impl CallerContext {
    /// No restriction — equivalent to the bare `resolve()` call.
    pub fn unrestricted() -> Self { Self { caller_id: None } }

    /// Named identity; only capabilities that list this ID (or are unrestricted) are returned.
    pub fn for_caller(id: impl Into<Arc<str>>) -> Self {
        Self { caller_id: Some(id.into()) }
    }

    /// Returns `true` if `cap` is visible to this caller context.
    pub(crate) fn can_see(&self, cap: &Capability) -> bool {
        if cap.authorized_callers.is_empty() { return true; }
        match &self.caller_id {
            None     => false,
            Some(id) => cap.authorized_callers.iter().any(|a| a.as_ref() == id.as_ref()),
        }
    }
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

/// RAII guard that signals the consolidated opacity watcher when the
/// `RequirementHandle` that owns it is dropped.
pub(crate) struct OpacityDropGuard {
    pub(crate) cancelled: Arc<AtomicBool>,
    pub(crate) notify:    Arc<Notify>,
}

impl Drop for OpacityDropGuard {
    fn drop(&mut self) {
        self.cancelled.store(true, AOrdering::Release);
        self.notify.notify_one();
    }
}

/// Drop to retract a declared requirement. Tombstones both `req/{…}` and any
/// `sys/load/{…}/req/{…}` opacity entry that this requirement may have
/// written when unsatisfied (see Phase 3d auto-opacity).
pub struct RequirementHandle {
    pub(crate) _retract:      oneshot::Sender<()>,
    pub(crate) _opacity_drop: OpacityDropGuard,
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
    /// Group-level capabilities the group asserts when it has any cap-joined
    /// members. Each member writes its own per-member projection under
    /// `gcap/{group}/{ns}/{name}/{member_node_id}`. Used by
    /// `signal_wired_via` and `watch_wiring` to discover provider groups.
    #[serde(default)]
    pub provides:        Vec<Capability>,
    /// Filters that the group's signal-flow depends on. When a filter is
    /// unsatisfied the group membership task writes a
    /// `sys/load/{node}/group-req/{group}/{idx}` opacity entry, making the
    /// node opaque until the requirement is met. `signal_wired_via(filter)`
    /// is the actual send primitive.
    #[serde(default)]
    pub requires:        Vec<CapFilter>,
}

/// Drop to tombstone `cap-group/{group}`. All matching members will see the
/// tombstone via gossip and self-leave the group.
pub struct CapabilityGroupHandle {
    pub(crate) _retract: oneshot::Sender<()>,
    pub(crate) group:    Arc<str>,
}

/// Output of `resolve_wiring` / `watch_wiring`. The `Wired` variant lists every
/// provider that currently satisfies the filter — both group-level projections
/// (`gcap/`) and standalone node capabilities (`cap/`). `shared_locality_depth`
/// is computed in Phase 5 and is `0` for Phase 4-only matches.
#[derive(Clone, Debug, PartialEq)]
pub enum WiringStatus {
    Wired   { providers: Vec<WiringProvider> },
    Unwired { filter:    CapFilter },
}

/// Outcome of a `signal_wired_via` / `signal_wired_via_locality` call.
/// Distinguishes the two empty-set cases at the type level so the caller
/// doesn't have to query wiring separately to tell them apart:
///
/// - `Emitted { providers }` — wiring resolved; one signal was dispatched
///   per listed provider (the `providers` Vec can be empty only when a
///   locality preference filtered every candidate out — i.e., the filter
///   matched at least one provider in the raw scan but `LocalityPreference::Strict`
///   rejected all of them).
/// - `Unwired { filter }` — no `cap/` or `gcap/` entries matched the filter
///   at the moment of the call. The signal was not dispatched. Caller can
///   subscribe to `watch_wiring(filter)` to react when wiring restores.
#[derive(Clone, Debug, PartialEq)]
pub enum WiredEmitOutcome {
    Emitted { providers: Vec<WiringProvider> },
    Unwired { filter:    CapFilter },
}

/// Aggregate "how many want this vs. how many offer it" view, computed across
/// the local KV state. Surfaced by `watch_demand` / `demand` so application
/// code can decide whether to spin up a new provider, scale down, or rebalance.
/// The library itself never auto-advertises in response to high demand —
/// turning pressure into action is an application-layer decision.
///
/// `demand_pressure` is `demanding_nodes.len() / max(providers.len(), 1)` as
/// an `f32`. A pressure of `1.0` means "one declared requirement per
/// available provider"; values much above `1.0` indicate undersupply.
#[derive(Clone, Debug, PartialEq)]
pub struct DemandStatus {
    pub filter:          CapFilter,
    pub demanding_nodes: Vec<NodeId>,
    pub providers:       Vec<NodeId>,
    pub demand_pressure: f32,
}

/// One discovered provider for a wiring filter.
#[derive(Clone, Debug, PartialEq)]
pub enum WiringProvider {
    /// An emergent group whose collective `provides` projection matches the
    /// filter. `contributors` lists every member whose `gcap/{group}/...`
    /// entry contributed to the match — useful for retraction tracking but
    /// not required by `signal_wired_via` (which routes via group scope).
    Group {
        name:                  Arc<str>,
        contributors:          Vec<NodeId>,
        shared_locality_depth: usize,
    },
    /// A standalone node whose direct `cap/{node_id}/{ns}/{name}` entry
    /// satisfies the filter without going through a group projection.
    Node {
        node_id:               NodeId,
        capability:            Capability,
        shared_locality_depth: usize,
    },
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
    /// True iff `cap` shares the same `(namespace, name)`, every attribute
    /// constraint matches a present attribute value on `cap`, and — when
    /// `self.schema_id` is set — the capability's `schema_id` matches exactly.
    /// Missing attributes or mismatched schema always fail.
    pub fn matches(&self, cap: &Capability) -> bool {
        if self.namespace != cap.namespace || self.name != cap.name {
            return false;
        }
        for (attr, constraint) in &self.attributes {
            let Some(value) = cap.attributes.get(attr) else { return false; };
            if !constraint.matches(value) { return false; }
        }
        if let Some(ref sid) = self.schema_id {
            if cap.schema_id.as_deref() != Some(sid.as_ref()) {
                return false;
            }
        }
        true
    }

    /// Convenience constructor for a name-only filter.
    pub fn new<N: Into<Arc<str>>, S: Into<Arc<str>>>(namespace: N, name: S) -> Self {
        Self {
            namespace:  namespace.into(),
            name:       name.into(),
            attributes: BTreeMap::new(),
            ranking:    None,
            max_age:    None,
            schema_id:  None,
        }
    }

    /// Require that each matched capability was refreshed within `window`.
    /// Set to 4–6× the `interval` you pass to `advertise_capability`.
    pub fn with_max_age(mut self, window: std::time::Duration) -> Self {
        self.max_age = Some(window);
        self
    }

    /// Builder-style attribute addition.
    pub fn with<A: Into<Arc<str>>>(mut self, attribute: A, constraint: CapConstraint) -> Self {
        self.attributes.insert(attribute.into(), constraint);
        self
    }

    /// Builder-style ranking attachment. Replaces any previously-set ranking.
    pub fn with_ranking<A: Into<Arc<str>>>(mut self, attribute: A, order: RankingOrder) -> Self {
        self.ranking = Some(CapRanking { attribute: attribute.into(), order });
        self
    }

    /// Constrains resolution to capabilities advertising the given `schema_id`.
    ///
    /// Capabilities with no `schema_id` set will not match — the constraint is
    /// strict by design. Use this when you require a specific contract version
    /// and want to avoid silently wiring to an older or incompatible provider.
    pub fn with_schema(mut self, id: impl Into<Arc<str>>) -> Self {
        self.schema_id = Some(id.into());
        self
    }
}

impl Capability {
    /// Convenience constructor without attributes.
    pub fn new<N: Into<Arc<str>>, S: Into<Arc<str>>>(namespace: N, name: S) -> Self {
        Self {
            namespace:          namespace.into(),
            name:               name.into(),
            attributes:         BTreeMap::new(),
            authorized_callers: Vec::new(),
            schema_id:          None,
            input_schema:       None,
            output_schema:      None,
        }
    }

    /// Builder-style attribute addition.
    pub fn with<A: Into<Arc<str>>>(mut self, attribute: A, value: CapValue) -> Self {
        self.attributes.insert(attribute.into(), value);
        self
    }

    /// Restrict resolution to the named callers. Pass an empty slice to clear.
    pub fn with_authorized_callers<S: Into<Arc<str>>>(
        mut self,
        callers: impl IntoIterator<Item = S>,
    ) -> Self {
        self.authorized_callers = callers.into_iter().map(Into::into).collect();
        self
    }

    /// Sets the schema identifier for this capability's invocation contract.
    ///
    /// Peers that declare a requirement with `CapFilter::with_schema(id)` will
    /// only resolve to providers whose `schema_id` matches exactly.
    pub fn with_schema_id(mut self, id: impl Into<Arc<str>>) -> Self {
        self.schema_id = Some(id.into());
        self
    }

    /// Embeds the JSON Schema for this capability's input payload.
    ///
    /// The schema is gossip-propagated inside the `Capability` struct so callers
    /// can inspect input shapes from `resolve()` results without a separate KV lookup.
    /// Typically a compact JSON Schema object serialized as a string.
    pub fn with_input_schema(mut self, schema: impl Into<Arc<str>>) -> Self {
        self.input_schema = Some(schema.into());
        self
    }

    /// Embeds the JSON Schema for this capability's output payload.
    pub fn with_output_schema(mut self, schema: impl Into<Arc<str>>) -> Self {
        self.output_schema = Some(schema.into());
        self
    }
}

/// KV-wire wrapper for `cap/` and `gcap/` entries. Carries the advertiser's
/// intended refresh interval so any reader can apply the 3× evaporation
/// threshold without any global configuration.
///
/// Old-format entries (raw `Capability` bytes from pre-pheromone nodes) decode
/// via the `Capability::decode` fallback with `refresh_interval_ms = 60_000`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CapEntry {
    pub capability:          Capability,
    pub refresh_interval_ms: u64,
}

impl CapEntry {
    /// True when the entry was refreshed (per its HLC timestamp) within the
    /// 3× evaporation window: `now_ms − written_ms ≤ 3 × refresh_interval_ms`.
    pub fn is_fresh(&self, hlc_ts: u64, now_ms: u64) -> bool {
        let written_ms = crate::hlc::physical_ms(hlc_ts);
        now_ms.saturating_sub(written_ms) <= 3 * self.refresh_interval_ms
    }
}

/// KV-wire wrapper for `req/` entries. Mirrors `CapEntry`'s pheromone model
/// so stale requirement declarations from crashed nodes age out automatically.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReqEntry {
    pub filter:              CapFilter,
    pub refresh_interval_ms: u64,
}

impl ReqEntry {
    pub fn is_fresh(&self, hlc_ts: u64, now_ms: u64) -> bool {
        let written_ms = crate::hlc::physical_ms(hlc_ts);
        now_ms.saturating_sub(written_ms) <= 3 * self.refresh_interval_ms
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
            pub fn decode(bytes: &[u8]) -> Option<Self> {
                bincode::serde::decode_from_slice(bytes, bincode_cfg()).ok().map(|(v, _)| v)
            }
        }
    };
}

impl_bincode_codec!(Capability);
impl_bincode_codec!(CapFilter);
impl_bincode_codec!(CapabilityGroupDef);
impl_bincode_codec!(CapEntry);
impl_bincode_codec!(ReqEntry);

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
            provides:        Vec::new(),
            requires:        Vec::new(),
        };
        let bytes = def.encode();
        let decoded = CapabilityGroupDef::decode(&bytes).expect("decode");
        assert_eq!(def, decoded);
    }

    #[test]
    fn cap_entry_encode_decode() {
        let cap   = Capability::new("compute", "gpu").with("vram_gb", iv(80));
        let entry = CapEntry { capability: cap.clone(), refresh_interval_ms: 5_000 };
        let bytes   = entry.encode();
        let decoded = CapEntry::decode(&bytes).expect("decode");
        assert_eq!(decoded.capability, cap);
        assert_eq!(decoded.refresh_interval_ms, 5_000);
    }

    #[test]
    fn cap_entry_freshness() {
        use crate::hlc::Hlc;
        let entry = CapEntry { capability: Capability::new("ai", "llm"), refresh_interval_ms: 1_000 };
        let hlc        = Hlc::new();
        let ts         = hlc.tick();
        let written_ms = crate::hlc::physical_ms(ts);
        assert!( entry.is_fresh(ts, written_ms + 2_500)); // 2.5 s < 3×1 s = 3 s
        assert!(!entry.is_fresh(ts, written_ms + 4_000)); // 4.0 s > 3 s
    }

    #[test]
    fn req_entry_freshness() {
        use crate::hlc::Hlc;
        let entry = ReqEntry { filter: CapFilter::new("ai", "llm"), refresh_interval_ms: 500 };
        let hlc        = Hlc::new();
        let ts         = hlc.tick();
        let written_ms = crate::hlc::physical_ms(ts);
        assert!( entry.is_fresh(ts, written_ms + 1_400)); // 1.4 s < 3×0.5 s = 1.5 s
        assert!(!entry.is_fresh(ts, written_ms + 1_600)); // 1.6 s > 1.5 s
    }

    #[test]
    fn capability_group_def_with_provides_roundtrip() {
        let filter = CapFilter::new("compute", "gpu");
        let def = CapabilityGroupDef {
            filter,
            topology_policy: None,
            provides: vec![
                Capability::new("storage", "durable")
                    .with("replication", CapValue::Integer(3)),
            ],
            requires: vec![
                CapFilter::new("logging", "sink"),
            ],
        };
        let bytes = def.encode();
        let decoded = CapabilityGroupDef::decode(&bytes).expect("decode");
        assert_eq!(def, decoded);
    }

    #[test]
    fn schema_id_filter_matching() {
        let cap_v2 = Capability::new("compute", "gpu")
            .with_schema_id("acme-ml/v2")
            .with_input_schema(r#"{"type":"object"}"#)
            .with_output_schema(r#"{"type":"string"}"#);
        let cap_v1 = Capability::new("compute", "gpu")
            .with_schema_id("acme-ml/v1");
        let cap_none = Capability::new("compute", "gpu");

        let filter_v2   = CapFilter::new("compute", "gpu").with_schema("acme-ml/v2");
        let filter_any  = CapFilter::new("compute", "gpu");

        // Strict match: filter_v2 accepts only v2.
        assert!( filter_v2.matches(&cap_v2));
        assert!(!filter_v2.matches(&cap_v1));
        assert!(!filter_v2.matches(&cap_none)); // no schema_id = does not satisfy versioned filter

        // Unversioned filter accepts all.
        assert!(filter_any.matches(&cap_v2));
        assert!(filter_any.matches(&cap_v1));
        assert!(filter_any.matches(&cap_none));

        // Schema fields survive encode/decode.
        let bytes   = cap_v2.encode();
        let decoded = Capability::decode(&bytes).expect("decode");
        assert_eq!(decoded.schema_id.as_deref(), Some("acme-ml/v2"));
        assert_eq!(decoded.input_schema.as_deref(), Some(r#"{"type":"object"}"#));
        assert_eq!(decoded.output_schema.as_deref(), Some(r#"{"type":"string"}"#));

        // CapFilter schema_id survives encode/decode.
        let bytes   = filter_v2.encode();
        let decoded = CapFilter::decode(&bytes).expect("decode");
        assert_eq!(decoded.schema_id.as_deref(), Some("acme-ml/v2"));
    }
}
