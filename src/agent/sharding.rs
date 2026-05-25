use crate::capability::Capability;
use crate::node_id::NodeId;

/// Returned by [`GossipAgent::emit_sharded`] when the capability filter matches
/// no live providers in the local KV view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardError {
    /// No providers matched the capability filter at call time.
    NoProviders,
}

impl std::fmt::Display for ShardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("no providers for shard key")
    }
}

impl std::error::Error for ShardError {}

/// FNV-1a 64-bit hash — stable, zero-dep ring key for shard placement.
pub(crate) fn fnv64(data: &[u8]) -> u64 {
    const OFFSET: u64 = 14_695_981_039_346_656_037;
    const PRIME:  u64 = 1_099_511_628_211;
    let mut h = OFFSET;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(PRIME);
    }
    h
}

/// Deterministic, pure consistent-hash ring placement.
///
/// Providers are ordered by `NodeId::id_hash()`. The shard key is hashed to a
/// u64 and the first provider whose ring position is ≥ that value is returned
/// (wrapping to index 0 when no provider is ≥ the key hash).
///
/// Returns `None` when `providers` is empty.
pub(crate) fn shard_owner(shard_key: &str, providers: &[(NodeId, Capability)]) -> Option<NodeId> {
    if providers.is_empty() {
        return None;
    }

    let key_hash = fnv64(shard_key.as_bytes());

    // Build a sorted list of (ring_position, index) without allocating a new Vec<NodeId>.
    let mut ring: Vec<(u64, usize)> = providers.iter()
        .enumerate()
        .map(|(i, (node, _))| (node.id_hash(), i))
        .collect();
    ring.sort_unstable_by_key(|(h, _)| *h);

    // Successor: first position >= key_hash; wraps to index 0.
    let idx = ring.iter()
        .find(|(h, _)| *h >= key_hash)
        .map(|(_, i)| *i)
        .unwrap_or_else(|| ring[0].1);   // wrap-around

    Some(providers[idx].0.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_id::NodeId;

    fn make_cap() -> Capability {
        Capability::new("compute", "gpu")
    }

    fn node(addr: &str) -> NodeId {
        addr.parse().unwrap()
    }

    #[test]
    fn empty_returns_none() {
        assert_eq!(shard_owner("any-key", &[]), None);
    }

    #[test]
    fn single_provider_always_wins() {
        let n = node("127.0.0.1:9001");
        let providers = vec![(n.clone(), make_cap())];
        for key in ["", "x", "user-12345", "aaaaaaa", "zzzzz"] {
            assert_eq!(shard_owner(key, &providers), Some(n.clone()));
        }
    }

    #[test]
    fn deterministic() {
        let providers = vec![
            (node("127.0.0.1:9001"), make_cap()),
            (node("127.0.0.1:9002"), make_cap()),
            (node("127.0.0.1:9003"), make_cap()),
        ];
        for key in ["user-1", "user-2", "tenant-abc", "session-xyz", ""] {
            let a = shard_owner(key, &providers);
            let b = shard_owner(key, &providers);
            assert_eq!(a, b, "key={key}");
        }
    }

    #[test]
    fn result_is_always_a_known_provider() {
        let providers = vec![
            (node("127.0.0.1:9001"), make_cap()),
            (node("127.0.0.1:9002"), make_cap()),
            (node("127.0.0.1:9003"), make_cap()),
            (node("127.0.0.1:9004"), make_cap()),
        ];
        let known: std::collections::HashSet<String> =
            providers.iter().map(|(n, _)| n.to_string()).collect();
        for i in 0u32..1000 {
            let key = format!("key-{i}");
            let owner = shard_owner(&key, &providers).unwrap();
            assert!(
                known.contains(&owner.to_string()),
                "key {key} returned unknown owner {owner}"
            );
        }
    }

    #[test]
    fn all_providers_reachable() {
        // With enough diverse keys every provider must be chosen at least once.
        let providers = vec![
            (node("127.0.0.1:9001"), make_cap()),
            (node("127.0.0.1:9002"), make_cap()),
            (node("127.0.0.1:9003"), make_cap()),
            (node("127.0.0.1:9004"), make_cap()),
        ];
        let mut seen = std::collections::HashSet::new();
        for i in 0u64..10_000 {
            let key = format!("shard-{i}");
            seen.insert(shard_owner(&key, &providers).unwrap().to_string());
            if seen.len() == providers.len() { break; }
        }
        assert_eq!(seen.len(), providers.len(), "not all providers were selected: {:?}", seen);
    }
}
