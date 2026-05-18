use crate::error::GossipError;
use std::{
    fmt,
    hash::{Hash, Hasher},
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::{Arc, OnceLock},
};

/// A node address in `"IP:port"` form (e.g. `"127.0.0.1:8080"`).
///
/// Construct with [`NodeId::new`] or parse from a string with `"IP:port".parse()`.
/// Only numeric IP addresses are accepted — hostnames are not resolved.
///
/// Clone is O(1) via `Arc<str>` reference counting.
/// `to_socket_addr()` returns the pre-parsed address — no allocation, no parsing.
/// Hash and Eq compare the parsed `SocketAddr`, not the raw string.
/// `id_hash()` returns a stable u64 used as the compact sender identity on the wire.
#[derive(Debug, Clone)]
pub struct NodeId {
    s: Arc<str>,
    addr: SocketAddr,
    id_hash: u64,
}

impl NodeId {
    pub fn new(address: &str, port: u16) -> Result<Self, GossipError> {
        let ip: IpAddr = address.parse().map_err(|e| {
            GossipError::Config(format!("Invalid IP address '{}': {}", address, e))
        })?;
        let addr = SocketAddr::new(ip, port);
        // Use SocketAddr's Display for the canonical string: "[::1]:8080" for IPv6,
        // "127.0.0.1:8080" for IPv4 — consistent with from_str and From<SocketAddr>.
        let s: Arc<str> = addr.to_string().into();
        Ok(Self { s, addr, id_hash: Self::hash_addr(addr) })
    }

    pub fn as_str(&self) -> &str {
        &self.s
    }

    pub fn to_socket_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Stable u64 identity used in place of the full address string on the wire.
    /// Computed once at construction with fixed ahash seeds — same binary = same hash.
    ///
    /// Collisions are rare (2⁶⁴ address space) but not truly benign. `id_hash` is used
    /// in two places:
    /// 1. **Echo-suppression** in the connection handler — drops frames back to the originator.
    /// 2. **Sender filter** in gossip shards — prevents re-forwarding to the frame's source.
    ///
    /// A collision between two distinct nodes causes writes from one to be silently filtered
    /// when forwarding to the other — a propagation failure, not just a missed optimisation.
    /// At typical cluster sizes (< 10,000 nodes) the birthday-problem probability is < 1 in 10⁸.
    #[inline]
    pub fn id_hash(&self) -> u64 {
        self.id_hash
    }

    fn hash_addr(addr: SocketAddr) -> u64 {
        static HASHER: OnceLock<ahash::RandomState> = OnceLock::new();
        HASHER
            .get_or_init(|| {
                ahash::RandomState::with_seeds(0xDEAD_BEEF, 0xCAFE_BABE, 0x1234_5678, 0xABCD_EF01)
            })
            .hash_one(addr)
    }
}

impl PartialEq for NodeId {
    fn eq(&self, other: &Self) -> bool {
        self.addr == other.addr
    }
}

impl Eq for NodeId {}

impl Hash for NodeId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.addr.hash(state);
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.s)
    }
}

impl FromStr for NodeId {
    type Err = GossipError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let addr = s.parse::<SocketAddr>().map_err(|e| {
            GossipError::Config(format!("Invalid node address '{}': {}", s, e))
        })?;
        Ok(Self { s: addr.to_string().into(), addr, id_hash: Self::hash_addr(addr) })
    }
}

impl From<SocketAddr> for NodeId {
    fn from(addr: SocketAddr) -> Self {
        let s: Arc<str> = addr.to_string().into();
        Self { s, addr, id_hash: Self::hash_addr(addr) }
    }
}

impl serde::Serialize for NodeId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.s.serialize(serializer)
    }
}

/// Custom `Deserialize` for `NodeId` that validates the address via `FromStr`.
/// This ensures invalid addresses are rejected at deserialise time (e.g., from
/// TOML config files and incoming gossip frames).
impl<'de> serde::Deserialize<'de> for NodeId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse::<NodeId>().map_err(serde::de::Error::custom)
    }
}
