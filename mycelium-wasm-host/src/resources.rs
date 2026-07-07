//! Node-local resource assessment — the probe behind **resource-aware install eligibility**
//! (`docs/design/artifact-library.md` §4.4).
//!
//! A node must not elect to install an artifact it cannot fit: before self-electing, the
//! `Provisioner` checks the entry's publisher-declared requirements against a *fraction* of what
//! this node actually has free (memory globally, disk at the runtime's placement root),
//! accounting for installs already in flight. The check is **node-local, binary, and silent** —
//! an ineligible node simply doesn't elect, some node that fits does, and if no live node can,
//! the tripwire counter says so. Resource-aware self-election *is* the fleet's placement
//! algorithm; there is no scheduler, no resource gossip, and no best-fit ranking anywhere.

use std::path::Path;

/// A view of this node's free resources. Implementations must be cheap enough to call once per
/// candidate entry per provision round. `None` means "cannot measure here" — the caller treats
/// that as *permissive* (detection, not prevention: blocking every install on an unmeasurable
/// platform would freeze a fleet silently; letting the runtime's real failure be the signal
/// does not).
pub trait ResourceProbe: Send + Sync {
    /// Bytes of memory currently available to new work, or `None` if unmeasurable.
    fn available_memory_bytes(&self) -> Option<u64>;

    /// Bytes of disk available on the filesystem containing `at`, or `None` if unmeasurable.
    /// `at` may not exist yet (a placement dir created on first install) — implementations
    /// should measure the nearest existing ancestor.
    fn available_disk_bytes(&self, at: &Path) -> Option<u64>;
}

/// The default probe: `sysinfo`-backed available memory + per-mount available disk (the disk
/// for a path is the mount with the longest mount-point prefix of its nearest existing
/// ancestor).
#[derive(Default)]
pub struct SystemResourceProbe;

impl SystemResourceProbe {
    pub fn new() -> Self {
        Self
    }
}

impl ResourceProbe for SystemResourceProbe {
    fn available_memory_bytes(&self) -> Option<u64> {
        let mut sys = sysinfo::System::new();
        sys.refresh_memory();
        let avail = sys.available_memory();
        (avail > 0).then_some(avail)
    }

    fn available_disk_bytes(&self, at: &Path) -> Option<u64> {
        // Walk up to the nearest existing ancestor (the placement dir may not exist yet),
        // canonicalize it, then pick the mount with the longest matching prefix.
        let mut existing = at;
        while !existing.exists() {
            existing = existing.parent()?;
        }
        let canon = existing.canonicalize().ok()?;
        let disks = sysinfo::Disks::new_with_refreshed_list();
        disks
            .list()
            .iter()
            .filter(|d| canon.starts_with(d.mount_point()))
            .max_by_key(|d| d.mount_point().as_os_str().len())
            .map(|d| d.available_space())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_probe_measures_this_host() {
        let probe = SystemResourceProbe::new();
        // Both metrics should be measurable on the dev/CI platforms; values are live so we
        // only assert sanity, and tolerate `None` on exotic hosts (the permissive contract).
        if let Some(mem) = probe.available_memory_bytes() {
            assert!(mem > 0, "available memory reads as a positive byte count");
        }
        if let Some(disk) = probe.available_disk_bytes(&std::env::temp_dir()) {
            assert!(disk > 0, "temp dir's mount reports positive available space");
        }
        // A not-yet-existing placement dir measures via its nearest existing ancestor.
        let ghost = std::env::temp_dir().join("mycelium-does-not-exist").join("models");
        assert_eq!(
            probe.available_disk_bytes(&ghost).is_some(),
            probe.available_disk_bytes(&std::env::temp_dir()).is_some(),
            "unborn placement dirs measure like their existing ancestor"
        );
    }
}
