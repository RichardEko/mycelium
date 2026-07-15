//! SWIM membership table with incarnation-based conflict resolution (WS-B M5 Stage 3).
//!
//! This is the peer-sampling layer that **decouples discovery from the bounded
//! forwarding set**: membership updates gossip over the UDP probe traffic
//! ([`MemberUpdate`] piggybacked on `Ping`/`Ack`), so every node learns the full
//! cluster independent of which `k` peers it forwards to — which is what lets a
//! well-known seed de-pin and flattens the connection fan-out (G1).
//!
//! Conflict resolution follows SWIM: every member carries an **incarnation** number;
//! a higher incarnation always wins, and at equal incarnation the status precedence is
//! `Dead > Suspect > Alive`. A node refutes a `Suspect`/`Dead` rumour about *itself* by
//! bumping its own incarnation past the rumour and re-gossiping `Alive` — so only a
//! genuinely absent node stays dead. The table is pure and deterministic; the live
//! socket/timer wiring lives in [`crate::swim`].

use crate::node_id::NodeId;
use ahash::AHashMap;
use serde::{Deserialize, Serialize};
use std::time::Instant;

/// Liveness status of a member in the SWIM table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemberStatus {
    Alive,
    Suspect,
    Dead,
}

impl MemberStatus {
    /// Status precedence at equal incarnation: `Dead` > `Suspect` > `Alive`.
    fn rank(self) -> u8 {
        match self {
            MemberStatus::Alive => 0,
            MemberStatus::Suspect => 1,
            MemberStatus::Dead => 2,
        }
    }
}

/// A single membership fact, gossiped on a SWIM datagram.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberUpdate {
    pub node: NodeId,
    pub incarnation: u64,
    pub status: MemberStatus,
}

/// What applying an update changed — drives the side effects in [`crate::swim`]
/// (the `peers` map for discovery/eviction, and re-gossip of a self-refutation).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyEffect {
    /// No observable change (stale or duplicate rumour).
    None,
    /// A peer became alive (new, or recovered from suspect/dead) — add to `peers`.
    BecameAlive(NodeId),
    /// A peer was confirmed dead — remove it from `peers` and evict its writer.
    BecameDead(NodeId),
    /// A `Suspect`/`Dead` rumour about *us* arrived; we bumped our own incarnation to
    /// the returned value and should re-gossip our `Alive` state to refute it.
    RefutedSelf(u64),
}

struct Entry {
    incarnation: u64,
    status: MemberStatus,
    changed: Instant,
}

/// The local view of cluster membership.
pub struct SwimMembership {
    self_id: NodeId,
    self_incarnation: u64,
    members: AHashMap<NodeId, Entry>,
}

impl SwimMembership {
    pub fn new(self_id: NodeId) -> Self {
        Self { self_id, self_incarnation: 0, members: AHashMap::new() }
    }

    pub fn self_incarnation(&self) -> u64 {
        self.self_incarnation
    }

    /// Merge one gossiped update, returning the side effect to apply. `now` is injected
    /// so the table stays pure/testable.
    pub fn apply(&mut self, u: &MemberUpdate, now: Instant) -> ApplyEffect {
        // A rumour about ourselves: refute Suspect/Dead by out-incarnating it.
        if u.node == self.self_id {
            return match u.status {
                MemberStatus::Suspect | MemberStatus::Dead if u.incarnation >= self.self_incarnation => {
                    // Refute by out-incarnating (`>=` also lets a restarted node reclaim its identity
                    // when the cluster remembers it at a higher incarnation). `saturating_add`, NOT
                    // `+ 1`: SWIM datagrams are unauthenticated UDP, so a *corrupted* frame (UDP's
                    // checksum is weak) carrying `incarnation == u64::MAX` must never (a) panic under
                    // overflow-checks — the fire-and-forget listener task would die and the failure
                    // detector silently stop — nor (b) wrap to 0 under the release profile (which sets
                    // `panic = "abort"`, overflow-checks off), which would RESET our refutation power
                    // and get this live node evicted cluster-wide. Saturating pins us high instead —
                    // degraded but alive. (A *malicious* peer forging a high incarnation can still pin
                    // us at the ceiling; that is a Byzantine action, outside Mycelium's CFT-not-BFT
                    // threat model — the guard here is against corruption-induced crash/eviction.)
                    self.self_incarnation = u.incarnation.saturating_add(1);
                    ApplyEffect::RefutedSelf(self.self_incarnation)
                }
                _ => ApplyEffect::None,
            };
        }

        let overrides = match self.members.get(&u.node) {
            None => true,
            Some(e) => {
                u.incarnation > e.incarnation
                    || (u.incarnation == e.incarnation && u.status.rank() > e.status.rank())
            }
        };
        if !overrides {
            return ApplyEffect::None;
        }

        let prev_alive = matches!(self.members.get(&u.node), Some(e) if e.status == MemberStatus::Alive);
        self.members.insert(
            u.node.clone(),
            Entry { incarnation: u.incarnation, status: u.status, changed: now },
        );

        match u.status {
            MemberStatus::Alive if !prev_alive => ApplyEffect::BecameAlive(u.node.clone()),
            MemberStatus::Dead => ApplyEffect::BecameDead(u.node.clone()),
            _ => ApplyEffect::None,
        }
    }

    /// Locally observe that `node` is alive (e.g. we just got an `Ack`). Records it at
    /// its current incarnation, defaulting to incarnation 0 for a first sighting.
    pub fn observe_alive(&mut self, node: &NodeId, now: Instant) -> ApplyEffect {
        let inc = self.members.get(node).map(|e| e.incarnation).unwrap_or(0);
        self.apply(&MemberUpdate { node: node.clone(), incarnation: inc, status: MemberStatus::Alive }, now)
    }

    /// Locally suspect `node` (both direct and indirect probes failed). Only takes effect
    /// if it is currently `Alive`; returns the update to gossip, or `None`.
    pub fn suspect(&mut self, node: &NodeId, now: Instant) -> Option<MemberUpdate> {
        let e = self.members.get_mut(node)?;
        if e.status == MemberStatus::Alive {
            e.status = MemberStatus::Suspect;
            e.changed = now;
            Some(MemberUpdate { node: node.clone(), incarnation: e.incarnation, status: MemberStatus::Suspect })
        } else {
            None
        }
    }

    /// Promote to `Dead` every member that has been `Suspect` for at least `timeout`.
    /// Returns the confirmed-dead nodes (the caller removes them from `peers` + gossips
    /// `Dead`). The entries are retained as `Dead` tombstones so a late `Alive` rumour at
    /// the same incarnation cannot resurrect them (only a higher incarnation can).
    pub fn promote_expired_suspects(&mut self, now: Instant, timeout: std::time::Duration) -> Vec<MemberUpdate> {
        let mut dead = Vec::new();
        for (node, e) in self.members.iter_mut() {
            if e.status == MemberStatus::Suspect && now.duration_since(e.changed) >= timeout {
                e.status = MemberStatus::Dead;
                e.changed = now;
                dead.push(MemberUpdate { node: node.clone(), incarnation: e.incarnation, status: MemberStatus::Dead });
            }
        }
        dead
    }

    /// Members currently believed `Alive` (probe/forwarding candidates).
    pub fn alive_members(&self) -> Vec<NodeId> {
        self.members
            .iter()
            .filter(|(_, e)| e.status == MemberStatus::Alive)
            .map(|(n, _)| n.clone())
            .collect()
    }

    /// A gossip sample of up to `n` updates, with our own `Alive` state always included
    /// first so peers learn (and can refute against) our incarnation.
    ///
    /// The remaining slots are **half most-recently-changed, half uniform-random** over the
    /// rest. The recency half propagates joins / `Suspect` / `Dead` fast; the random half is
    /// what makes the *full* roster keep disseminating and heal after a dropped datagram. A
    /// pure newest-first sample re-gossips only the same recent few once a node's view
    /// stabilises, so the long tail of the roster never re-propagates over lossy transport —
    /// which stalled membership below the de-pin threshold at scale over the Docker bridge
    /// (Stage 4 Docker re-validation). Datagram size is unchanged (still ≤ `n+1` updates).
    pub fn gossip_sample(&self, n: usize) -> Vec<MemberUpdate> {
        let mut out = Vec::with_capacity(n + 1);
        out.push(MemberUpdate {
            node: self.self_id.clone(),
            incarnation: self.self_incarnation,
            status: MemberStatus::Alive,
        });
        if n == 0 || self.members.is_empty() {
            return out;
        }
        let mut entries: Vec<(&NodeId, &Entry)> = self.members.iter().collect();
        let recent = n.div_ceil(2).min(entries.len());
        // Recency half (most-recently-changed first).
        entries.sort_unstable_by_key(|(_, e)| std::cmp::Reverse(e.changed));
        for (node, e) in entries.iter().take(recent) {
            out.push(MemberUpdate { node: (*node).clone(), incarnation: e.incarnation, status: e.status });
        }
        // Random half: partial Fisher–Yates over the remaining tail (no overlap with the
        // recency half, which is already included), so every member is gossiped over time.
        let tail = &mut entries[recent..];
        let want = (n - recent).min(tail.len());
        for i in 0..want {
            let j = i + fastrand::usize(..tail.len() - i);
            tail.swap(i, j);
            let (node, e) = tail[i];
            out.push(MemberUpdate { node: node.clone(), incarnation: e.incarnation, status: e.status });
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn id(p: u16) -> NodeId { NodeId::new("127.0.0.1", p).unwrap() }
    fn alive(p: u16, inc: u64) -> MemberUpdate { MemberUpdate { node: id(p), incarnation: inc, status: MemberStatus::Alive } }
    fn suspect(p: u16, inc: u64) -> MemberUpdate { MemberUpdate { node: id(p), incarnation: inc, status: MemberStatus::Suspect } }
    fn dead(p: u16, inc: u64) -> MemberUpdate { MemberUpdate { node: id(p), incarnation: inc, status: MemberStatus::Dead } }

    #[test]
    fn first_alive_sighting_becomes_alive() {
        let mut m = SwimMembership::new(id(1));
        let now = Instant::now();
        assert_eq!(m.apply(&alive(2, 0), now), ApplyEffect::BecameAlive(id(2)));
        // Re-applying the same alive is a no-op (already alive).
        assert_eq!(m.apply(&alive(2, 0), now), ApplyEffect::None);
        assert_eq!(m.alive_members(), vec![id(2)]);
    }

    #[test]
    fn higher_incarnation_always_wins() {
        let mut m = SwimMembership::new(id(1));
        let now = Instant::now();
        m.apply(&suspect(2, 5), now); // suspect at inc 5
        // Alive at higher inc overrides suspect → becomes alive again.
        assert_eq!(m.apply(&alive(2, 6), now), ApplyEffect::BecameAlive(id(2)));
        // Stale alive at lower inc is ignored.
        assert_eq!(m.apply(&alive(2, 3), now), ApplyEffect::None);
    }

    #[test]
    fn equal_incarnation_precedence_dead_over_suspect_over_alive() {
        let mut m = SwimMembership::new(id(1));
        let now = Instant::now();
        m.apply(&alive(2, 4), now);
        // Suspect at same inc overrides alive (no peers effect yet).
        assert_eq!(m.apply(&suspect(2, 4), now), ApplyEffect::None);
        // Alive at same inc does NOT override suspect.
        assert_eq!(m.apply(&alive(2, 4), now), ApplyEffect::None);
        // Dead at same inc overrides suspect.
        assert_eq!(m.apply(&dead(2, 4), now), ApplyEffect::BecameDead(id(2)));
        assert!(m.alive_members().is_empty());
    }

    #[test]
    fn dead_tombstone_not_resurrected_at_same_incarnation() {
        let mut m = SwimMembership::new(id(1));
        let now = Instant::now();
        m.apply(&dead(2, 7), now);
        assert_eq!(m.apply(&alive(2, 7), now), ApplyEffect::None, "same-inc alive can't revive a tombstone");
        // Only a strictly higher incarnation revives it.
        assert_eq!(m.apply(&alive(2, 8), now), ApplyEffect::BecameAlive(id(2)));
    }

    #[test]
    fn self_refutes_suspect_and_dead_by_outincarnating() {
        let mut m = SwimMembership::new(id(1));
        let now = Instant::now();
        assert_eq!(m.self_incarnation(), 0);
        // Someone suspects us at inc 0 → we bump to 1 and refute.
        assert_eq!(m.apply(&suspect(1, 0), now), ApplyEffect::RefutedSelf(1));
        assert_eq!(m.self_incarnation(), 1);
        // A Dead rumour at inc 1 → bump to 2.
        assert_eq!(m.apply(&dead(1, 1), now), ApplyEffect::RefutedSelf(2));
        // A stale rumour below our incarnation is ignored.
        assert_eq!(m.apply(&suspect(1, 0), now), ApplyEffect::None);
        // An Alive rumour about ourselves never refutes.
        assert_eq!(m.apply(&alive(1, 5), now), ApplyEffect::None);
    }

    #[test]
    fn regression_self_refute_saturates_at_max_incarnation_no_overflow() {
        // Audit 2026-07-15 (pass 2): a corrupted/forged unauthenticated SWIM datagram carrying a
        // self-rumour at incarnation u64::MAX must NOT `u.incarnation + 1` → panic (overflow-checks
        // build: kills the fire-and-forget listener task) or wrap to 0 (release: resets our
        // refutation power and gets us evicted cluster-wide). Saturating pins us high — degraded but
        // alive and non-panicking.
        let mut m = SwimMembership::new(id(1));
        let now = Instant::now();
        let effect = m.apply(&suspect(1, u64::MAX), now); // must not panic
        assert_eq!(effect, ApplyEffect::RefutedSelf(u64::MAX));
        assert_eq!(m.self_incarnation(), u64::MAX, "must saturate, not wrap to 0");
    }

    #[test]
    fn suspect_then_promote_after_timeout() {
        let mut m = SwimMembership::new(id(1));
        let t0 = Instant::now();
        m.apply(&alive(2, 0), t0);
        // suspect() only fires from Alive.
        assert!(m.suspect(&id(2), t0).is_some());
        assert!(m.suspect(&id(2), t0).is_none(), "already suspect");
        // Not yet expired.
        assert!(m.promote_expired_suspects(t0 + Duration::from_millis(100), Duration::from_secs(1)).is_empty());
        // Expired → promoted to Dead.
        let dead = m.promote_expired_suspects(t0 + Duration::from_secs(2), Duration::from_secs(1));
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].status, MemberStatus::Dead);
        assert!(m.alive_members().is_empty());
    }

    #[test]
    fn gossip_sample_includes_self_and_is_bounded() {
        let mut m = SwimMembership::new(id(1));
        let now = Instant::now();
        for p in 2..20 { m.apply(&alive(p, 0), now); }
        let sample = m.gossip_sample(5);
        assert_eq!(sample.len(), 6, "self + up to n others");
        assert_eq!(sample[0].node, id(1), "self first");
        assert_eq!(sample[0].status, MemberStatus::Alive);
    }

    #[test]
    fn gossip_sample_random_tail_covers_the_whole_roster_over_time() {
        // A stabilised view: all members share the same `changed`, so the recency half is
        // arbitrary and the random half must carry full coverage. A pure newest-first sample
        // would emit the same fixed subset forever; the random tail must reach every member.
        let mut m = SwimMembership::new(id(1));
        let now = Instant::now();
        for p in 2..=60 { m.apply(&alive(p, 0), now); } // 59 stable members, n=6 per sample
        let mut seen = std::collections::HashSet::new();
        for _ in 0..400 {
            for u in m.gossip_sample(6) {
                if u.node != id(1) { seen.insert(u.node.clone()); }
            }
        }
        assert_eq!(seen.len(), 59, "every member must be gossiped over repeated samples");
    }
}
