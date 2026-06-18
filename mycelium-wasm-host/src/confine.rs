//! Namespace confinement — the **enforcement point** of the host ⇄ component boundary.
//!
//! A WASM guest is untrusted foreign code running in the node's own process. Unlike the
//! substrate (which is detection-not-prevention, because Layer I must not police higher
//! *layers*), the host *mediates every import a guest makes* and can legitimately **prevent**:
//! a component addresses KV keys **relative to its own capability namespace**, and the host
//! prefixes them into a private, per-component subtree it can never escape.
//!
//! Component KV lives under [`COMPONENT_KV_PREFIX`]`{node}/{namespace}/…` — a dedicated prefix,
//! deliberately **not** `cap/` (which the capability resolver/`demand` scan): component working
//! state must not pollute the capability registry. (This refines the §E.2 sketch, which named
//! `cap/{me}/{ns}/*`, for that reason.)

use mycelium::NodeId;

/// KV prefix owned by component working-state. One private subtree per `(node, namespace)`.
pub const COMPONENT_KV_PREFIX: &str = "comp/";

/// Why a component-relative key was refused. The guest cannot turn any of these into a
/// host-side write — confinement is enforced, not merely logged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfinementError {
    /// Empty key.
    Empty,
    /// Leading `/` — an attempt to address an absolute key outside the subtree.
    Absolute,
    /// A `..` path segment — a traversal attempt out of the subtree.
    Traversal,
}

impl std::fmt::Display for ConfinementError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "empty component key"),
            Self::Absolute => write!(f, "absolute key escapes the component subtree"),
            Self::Traversal => write!(f, "`..` traversal escapes the component subtree"),
        }
    }
}

impl std::error::Error for ConfinementError {}

/// Map a component-relative `rel_key` to its confined absolute KV key
/// `comp/{node}/{namespace}/{rel_key}`, or refuse it.
///
/// `namespace` is host-set (from the component's manifest, trusted); only `rel_key` is
/// guest-controlled, so only it is validated. The guest can never read or write outside its
/// own `(node, namespace)` subtree — every escape shape (`/…`, `../…`, `a/../../b`) is rejected.
pub fn confine_key(node: &NodeId, namespace: &str, rel_key: &str) -> Result<String, ConfinementError> {
    if rel_key.is_empty() {
        return Err(ConfinementError::Empty);
    }
    if rel_key.starts_with('/') {
        return Err(ConfinementError::Absolute);
    }
    if rel_key.split('/').any(|seg| seg == "..") {
        return Err(ConfinementError::Traversal);
    }
    Ok(format!("{COMPONENT_KV_PREFIX}{node}/{namespace}/{rel_key}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node() -> NodeId {
        NodeId::new("127.0.0.1", 9000).unwrap()
    }

    #[test]
    fn confines_a_normal_key_under_the_component_subtree() {
        let k = confine_key(&node(), "nlp", "state/cursor").unwrap();
        assert_eq!(k, format!("comp/{}/nlp/state/cursor", node()));
        assert!(k.starts_with(COMPONENT_KV_PREFIX));
    }

    #[test]
    fn rejects_empty_key() {
        assert_eq!(confine_key(&node(), "nlp", ""), Err(ConfinementError::Empty));
    }

    #[test]
    fn rejects_absolute_key() {
        assert_eq!(confine_key(&node(), "nlp", "/etc/secret"), Err(ConfinementError::Absolute));
    }

    #[test]
    fn rejects_traversal_in_any_segment() {
        assert_eq!(confine_key(&node(), "nlp", ".."), Err(ConfinementError::Traversal));
        assert_eq!(confine_key(&node(), "nlp", "../other"), Err(ConfinementError::Traversal));
        assert_eq!(confine_key(&node(), "nlp", "a/../../cap/evil"), Err(ConfinementError::Traversal));
    }

    #[test]
    fn a_component_can_never_reach_another_namespace_or_the_cap_registry() {
        // Even a key that *names* cap/ stays inside the component's own subtree.
        let k = confine_key(&node(), "nlp", "cap/whatever").unwrap();
        assert!(k.starts_with(&format!("comp/{}/nlp/", node())));
        assert!(!k.starts_with("cap/"), "must not land in the capability registry");
    }
}
