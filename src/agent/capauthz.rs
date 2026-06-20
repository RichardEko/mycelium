//! Gossip-level capability authorization (WS-D / M6 · D4–D5) — **enforce at resolve, detect at
//! write**.
//!
//! The v1.x RBAC subset (WS1) enforces at the gateway + the `sys/` tripwire. M6 extends enforcement
//! to **resolve time** on gossiped capabilities: a cluster policy says "only advertisers holding
//! role R may provide `ns/name`", and every resolver **routes around** an advertiser whose
//! *signed* role does not satisfy it — node-locally, emergently, with no admission coordinator.
//!
//! The advertisement still propagates per LWW (Core Principle 4 — never a Layer I write guard); the
//! resolver simply declines to *act* on it and bumps a tripwire counter
//! ([`SystemStats::cap_authz_violations`](super::SystemStats::cap_authz_violations)), the same
//! detection-not-prevention posture as `commit_conflicts` / `sys_namespace_violations`.
//!
//! The policy is a plain KV entry at `sys/capauthz/{ns}/{name}` (operator-set in D4; D6 distributes
//! it via consensus so every resolver enforces the *same* policy — consensus carries the policy, it
//! is not an admission coordinator). No policy entry ⇒ open (backward-compatible).

use serde::{Deserialize, Serialize};

use crate::node_id::NodeId;
use super::TaskCtx;

/// KV namespace for capability-authorization policy: `sys/capauthz/{ns}/{name}`.
pub const CAPAUTHZ_PREFIX: &str = "sys/capauthz/";

/// KV key for the policy governing who may provide `ns/name`.
pub fn capauthz_key(ns: &str, name: &str) -> String {
    format!("{CAPAUTHZ_PREFIX}{ns}/{name}")
}

/// The policy for one `ns/name`: an **any-of** list of roles an advertiser must hold to be admitted
/// by resolvers. Empty ⇒ open (treated as no policy).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CapAuthzPolicy {
    pub required_roles: Vec<String>,
}

impl CapAuthzPolicy {
    pub(crate) fn encode(&self) -> bytes::Bytes {
        bytes::Bytes::from(mycelium_core::serde_fixint::to_vec(self).unwrap_or_default())
    }
    pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
        mycelium_core::serde_fixint::from_slice(bytes).ok()
    }
}

/// Set (or, with an empty `roles`, effectively open) the authorization policy for `ns/name`. Writes
/// `sys/capauthz/{ns}/{name}`; gossips to every node, which then enforces it at resolve.
pub(crate) fn set_policy(ctx: &TaskCtx, ns: &str, name: &str, roles: Vec<String>) -> bool {
    let policy = CapAuthzPolicy { required_roles: roles };
    mycelium_core::kv_handle::KvHandle::from_core(std::sync::Arc::clone(&ctx.core))
        .set(capauthz_key(ns, name), policy.encode())
}

/// The required roles for `ns/name`, if a non-empty policy is published. `None` ⇒ open.
pub(crate) fn required_roles(ctx: &TaskCtx, ns: &str, name: &str) -> Option<Vec<String>> {
    let bytes = mycelium_core::kv_handle::KvHandle::from_core(std::sync::Arc::clone(&ctx.core))
        .get(&capauthz_key(ns, name))?;
    let policy = CapAuthzPolicy::decode(&bytes)?;
    (!policy.required_roles.is_empty()).then_some(policy.required_roles)
}

/// Is `advertiser` authorized to provide a capability requiring one of `required` roles? `true` iff
/// it holds (via a **signature-verified** role claim — `sys/role/{advertiser}` checked against the
/// retained-and-non-revoked key set) at least one required role. A forged or revoked-key-signed role
/// claim reads back as no roles, so it does not satisfy the policy.
pub(crate) fn advertiser_authorized(ctx: &TaskCtx, advertiser: &NodeId, required: &[String]) -> bool {
    let Some(bytes) = mycelium_core::kv_handle::KvHandle::from_core(std::sync::Arc::clone(&ctx.core))
        .get(&super::rbac::role_key(advertiser))
    else {
        return false;
    };
    // Verify the role claim against the advertiser's known (retained, non-revoked) keys — the same
    // path `roles_of` uses, so revocation (D1) and forgery rejection apply here too.
    let claim = super::helpers::known_verifying_keys(ctx, advertiser)
        .iter()
        .find_map(|vk| super::rbac::verified_roles(&bytes, advertiser, vk));
    match claim {
        Some(c) => required.iter().any(|r| c.has_role(r)),
        None => false,
    }
}
