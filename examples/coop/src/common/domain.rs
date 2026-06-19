//! The constructive domain vocabulary shared by every example: surplus-food donations moving
//! through a co-op of depots. Payloads cross the wire as opaque `Bytes`; these are the typed
//! views the demos (de)serialise.

use serde::{Deserialize, Serialize};

/// One surplus-food donation entering the co-op from a donor (market, farm, bakery), to be routed
/// to a community kitchen before `perishable_by_ms`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Donation {
    pub id:          u64,
    pub donor:       String,
    pub items:       String,
    pub origin_zone: String,
}

impl Donation {
    pub fn new(id: u64, donor: &str, items: &str, origin_zone: &str) -> Self {
        Self { id, donor: donor.into(), items: items.into(), origin_zone: origin_zone.into() }
    }

    /// JSON bytes for the wire (mailbox payload / tuple-space lane item).
    pub fn to_bytes(&self) -> bytes::Bytes {
        bytes::Bytes::from(serde_json::to_vec(self).unwrap_or_default())
    }

    /// Decode a wire payload back into a donation (`None` on garbage).
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        serde_json::from_slice(bytes).ok()
    }

    /// A short human-readable line — the natural-language input handed to the triage skill.
    pub fn summary(&self) -> String {
        format!("donation #{} — {} from {} (origin: {})", self.id, self.items, self.donor, self.origin_zone)
    }
}
