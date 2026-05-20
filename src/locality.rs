//! Hierarchical locality paths for topology-aware gossip and consensus.
//!
//! A [`LocalityPath`] is an ordered sequence of opaque segment names from coarse
//! to fine — typical examples: `["eu-west-1", "az-1a", "rack-14"]`. The protocol
//! never interprets segment values; equality at a given index defines "same
//! domain at that level," and the length of the shared prefix between two paths
//! defines their topological distance.
//!
//! Encoded into a compact byte format for storage under
//! `cap/{node_id}/locality/self` — see [`encode`](LocalityPath::encode) and
//! [`decode`](LocalityPath::decode).

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Hierarchical topology address. Segments run coarse → fine (region → AZ → rack
/// → host, or whatever the deployment chooses to model). An empty path means
/// "unspecified" and shares zero prefix with any other path.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct LocalityPath {
    pub(crate) segments: Vec<Arc<str>>,
}

impl LocalityPath {
    /// Builds a path from any iterator of segment names.
    pub(crate) fn new<I, S>(segments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Arc<str>>,
    {
        Self { segments: segments.into_iter().map(Into::into).collect() }
    }

    /// Number of segments — zero means unspecified locality.
    #[allow(dead_code)] // used by Phase 5 locality-aware resolve and Phase 2 gate
    pub(crate) fn depth(&self) -> usize {
        self.segments.len()
    }

    /// Number of leading segments that match `other`. Symmetric.
    /// `["eu","az1","r14"]` vs `["eu","az1","r3"]` → `2`.
    pub(crate) fn shared_prefix_len(&self, other: &Self) -> usize {
        self.segments
            .iter()
            .zip(other.segments.iter())
            .take_while(|(a, b)| a.as_ref() == b.as_ref())
            .count()
    }

    /// First index where two paths differ — equivalent to `shared_prefix_len`
    /// but reads more naturally in topology-gate code that asks "at what level
    /// did these voters diverge."
    #[allow(dead_code)] // used by Phase 2 evaluate_topology_gate
    pub(crate) fn divergence_level(&self, other: &Self) -> usize {
        self.shared_prefix_len(other)
    }

    /// Segment at `idx`, or `None` if out of bounds.
    #[allow(dead_code)] // used by Phase 2 evaluate_topology_gate
    pub(crate) fn value_at(&self, idx: usize) -> Option<&Arc<str>> {
        self.segments.get(idx)
    }

    /// Encodes as: little-endian `u16` segment count, then for each segment a
    /// little-endian `u16` UTF-8 byte length followed by the segment bytes.
    /// Compact, version-stable, and avoids pulling bincode into hot KV paths.
    pub(crate) fn encode(&self) -> Bytes {
        let total = 2 + self.segments.iter().map(|s| 2 + s.len()).sum::<usize>();
        let mut out = Vec::with_capacity(total);
        out.extend_from_slice(&(self.segments.len() as u16).to_le_bytes());
        for seg in &self.segments {
            out.extend_from_slice(&(seg.len() as u16).to_le_bytes());
            out.extend_from_slice(seg.as_bytes());
        }
        Bytes::from(out)
    }

    /// Inverse of [`encode`]. Returns `None` on malformed or truncated input.
    pub(crate) fn decode(mut bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 2 { return None; }
        let n = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
        bytes = &bytes[2..];
        let mut segments = Vec::with_capacity(n);
        for _ in 0..n {
            if bytes.len() < 2 { return None; }
            let len = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
            bytes = &bytes[2..];
            if bytes.len() < len { return None; }
            let s = std::str::from_utf8(&bytes[..len]).ok()?;
            segments.push(Arc::from(s));
            bytes = &bytes[len..];
        }
        Some(Self { segments })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_prefix_len_basics() {
        let a = LocalityPath::new(["eu-west-1", "az-1a", "rack-14"]);
        let b = LocalityPath::new(["eu-west-1", "az-1a", "rack-3"]);
        let c = LocalityPath::new(["us-east-1", "az-2b", "rack-1"]);
        let empty = LocalityPath::default();

        assert_eq!(a.shared_prefix_len(&b), 2);
        assert_eq!(a.shared_prefix_len(&c), 0);
        assert_eq!(a.shared_prefix_len(&empty), 0);
        assert_eq!(empty.shared_prefix_len(&empty), 0);
        assert_eq!(a.shared_prefix_len(&a), 3);
    }

    #[test]
    fn divergence_level_matches_shared_prefix() {
        let a = LocalityPath::new(["a", "b", "c"]);
        let b = LocalityPath::new(["a", "x", "c"]);
        assert_eq!(a.divergence_level(&b), a.shared_prefix_len(&b));
    }

    #[test]
    fn value_at_returns_segments() {
        let p = LocalityPath::new(["eu", "az1"]);
        assert_eq!(p.value_at(0).map(|s| s.as_ref()), Some("eu"));
        assert_eq!(p.value_at(1).map(|s| s.as_ref()), Some("az1"));
        assert!(p.value_at(2).is_none());
    }

    #[test]
    fn encode_decode_round_trips() {
        let cases = [
            LocalityPath::default(),
            LocalityPath::new(["eu"]),
            LocalityPath::new(["eu-west-1", "az-1a", "rack-14"]),
            LocalityPath::new(["α", "β", "γ"]), // multi-byte UTF-8
        ];
        for original in &cases {
            let bytes = original.encode();
            let decoded = LocalityPath::decode(&bytes).expect("decode");
            assert_eq!(&decoded, original);
        }
    }

    #[test]
    fn decode_rejects_truncated_input() {
        assert!(LocalityPath::decode(&[]).is_none());
        assert!(LocalityPath::decode(&[1, 0]).is_none()); // claims 1 segment, no data follows
        assert!(LocalityPath::decode(&[1, 0, 5, 0, b'a', b'b']).is_none()); // claims len 5, only 2 bytes
    }

    #[test]
    fn empty_path_has_zero_depth() {
        assert_eq!(LocalityPath::default().depth(), 0);
        assert_eq!(LocalityPath::new::<_, &str>([]).depth(), 0);
    }
}
