//! Cluster-level manifest describing system anatomy.
//!
//! A [`MeshManifest`] declares the *expected* topology of a Mycelium deployment:
//! how many logical groups the system has, what capability each group provides,
//! and how many agents must be present per group for the mesh to be considered
//! healthy.
//!
//! This is a **read-only deployment descriptor** — it drives no runtime
//! behaviour by itself.  Call [`MeshManifest::check_status`] at any time to
//! compare the declared anatomy against the live mesh KV view held by any
//! [`GossipAgent`].
//!
//! ## TOML format
//!
//! ```toml
//! [mesh]
//! name    = "my-agent-system"
//! version = "0.1"
//!
//! [[group]]
//! name        = "llm-inference"
//! description = "LLM inference nodes"
//! min_agents  = 2
//! max_agents  = 8   # optional
//!
//!   [[group.capability]]
//!   ns   = "llm"
//!   name = "inference"
//!   # First capability is the membership indicator used by check_status().
//!   # Additional capabilities describe co-located services on the same node.
//! ```

use crate::capability_config::CapabilityProbeEntry;
use crate::error::GossipError;
use crate::{CapFilter, GossipAgent};
use serde::{Deserialize, Serialize};
use std::path::Path;

// ── Config types ──────────────────────────────────────────────────────────────

/// Cluster-level deployment descriptor.
///
/// Load with [`MeshManifest::load_from_file`], then call
/// [`MeshManifest::check_status`] to evaluate live mesh health.
/// Use [`MeshManifest::to_toml`] + [`GossipAgent::set`] to push live updates
/// to the mesh via the `manifest/current` KV key.
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct MeshManifest {
    /// Top-level metadata (`[mesh]` section).
    #[serde(default)]
    pub mesh: MeshMeta,
    /// Group declarations (`[[group]]` array).
    #[serde(default, rename = "group")]
    pub groups: Vec<GroupManifest>,
}

/// Top-level metadata for a mesh deployment.
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct MeshMeta {
    /// Human-readable name for this deployment.
    #[serde(default)]
    pub name: String,
    /// Deployment version string (semver, e.g. `"0.1.0"`).
    #[serde(default)]
    pub version: String,
}

/// Declaration for one logical group of agents.
///
/// A group is a set of agents that collectively provide a named capability.
/// `min_agents` is the minimum number that must be live for the group to be
/// considered satisfied.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GroupManifest {
    /// Identifier for this group (e.g. `"llm-inference"`).
    pub name: String,
    /// Human-readable description, shown in status output.
    pub description: Option<String>,
    /// Minimum live agent count for the group to be `satisfied`.
    pub min_agents: usize,
    /// Optional upper bound on expected agent count.
    pub max_agents: Option<usize>,
    /// Capabilities that characterise this group.  The **first** entry is the
    /// *membership indicator*: [`check_status`](MeshManifest::check_status)
    /// counts how many nodes currently advertise that `(ns, name)` pair.
    #[serde(default, rename = "capability")]
    pub capabilities: Vec<CapabilityProbeEntry>,
}

// ── Status types ──────────────────────────────────────────────────────────────

/// Live status snapshot for one group, computed by
/// [`MeshManifest::check_status`].
#[derive(Debug, Clone)]
pub struct GroupStatus {
    /// Group name from the manifest.
    pub name: String,
    /// Group description from the manifest, if any.
    pub description: Option<String>,
    /// Declared minimum agent count.
    pub min_agents: usize,
    /// Declared maximum agent count, if any.
    pub max_agents: Option<usize>,
    /// Number of agents currently advertising the group's primary capability.
    pub actual: usize,
    /// Whether `actual >= min_agents`.
    pub satisfied: bool,
    /// `max(0, min_agents − actual)` — additional agents needed.
    pub deficit: usize,
}

/// Aggregated health snapshot for the entire mesh, computed by
/// [`MeshManifest::check_status`].
#[derive(Debug, Clone)]
pub struct MeshStatus {
    /// Per-group status entries, in manifest declaration order.
    pub groups: Vec<GroupStatus>,
}

impl MeshStatus {
    /// Returns `true` only when every group is satisfied (`actual >= min_agents`).
    pub fn is_healthy(&self) -> bool {
        self.groups.iter().all(|g| g.satisfied)
    }

    /// Sum of deficits across all unsatisfied groups.
    pub fn total_deficit(&self) -> usize {
        self.groups.iter().map(|g| g.deficit).sum()
    }
}

// ── MeshManifest impl ─────────────────────────────────────────────────────────

/// KV keys used by the live manifest control system.
///
/// All keys live under the `manifest/` prefix, which is application-owned
/// (distinct from the library-reserved `sys/` prefix).
pub mod manifest_keys {
    /// Current active manifest — TOML bytes.
    pub const CURRENT: &str = "manifest/current";
    /// Semver string of the current manifest — fast compare without full decode.
    pub const VERSION: &str = "manifest/version";
    /// System-wide control state: `"running"` or `"stopped"`.
    pub const CONTROL_SYSTEM: &str = "manifest/control/system";
    /// Prefix for per-group control entries.
    pub const CONTROL_GROUP_PREFIX: &str = "manifest/control/group/";

    /// Returns the KV key for a group's control state.
    pub fn control_group(name: &str) -> String {
        format!("manifest/control/group/{name}")
    }
    /// Returns the KV key for an archived manifest version.
    pub fn history(ver: &str) -> String {
        format!("manifest/history/{ver}")
    }
}

/// Returns `true` if `new_ver` is strictly greater than `old_ver` under
/// semantic versioning (`major.minor.patch`).  Leading `v` is stripped.
/// Unparseable components are treated as `0`.
pub fn semver_gt(new_ver: &str, old_ver: &str) -> bool {
    let parse = |s: &str| -> (u64, u64, u64) {
        let mut p = s.trim_start_matches('v').split('.');
        (
            p.next().and_then(|x| x.parse().ok()).unwrap_or(0),
            p.next().and_then(|x| x.parse().ok()).unwrap_or(0),
            p.next().and_then(|x| x.parse().ok()).unwrap_or(0),
        )
    };
    parse(new_ver) > parse(old_ver)
}

impl MeshManifest {
    /// Loads and parses a cluster manifest from a TOML file.
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, GossipError> {
        let s = std::fs::read_to_string(path).map_err(GossipError::Io)?;
        toml::from_str(&s).map_err(GossipError::Toml)
    }

    /// Serialises the manifest back to a TOML string for KV storage.
    pub fn to_toml(&self) -> Result<String, toml::ser::Error> {
        toml::to_string(self)
    }

    /// Deserialises a manifest from raw TOML bytes (e.g. read from the `manifest/current` KV key).
    pub fn from_toml_bytes(b: &[u8]) -> Option<Self> {
        let s = std::str::from_utf8(b).ok()?;
        toml::from_str(s).ok()
    }

    /// Computes live health status by querying `agent`'s current mesh KV view.
    ///
    /// For each group the first declared `[[group.capability]]` entry is used
    /// as the membership indicator: the number of nodes that currently
    /// advertise that `(ns, name)` pair is compared against `min_agents`.
    ///
    /// Any node in the mesh can serve as the reporter — each agent holds a
    /// gossiped view of the full cluster's capability advertisements.
    pub fn check_status(&self, agent: &GossipAgent) -> MeshStatus {
        let groups = self.groups.iter().map(|g| {
            let actual = g.capabilities.first().map_or(0, |cap| {
                agent.capabilities().resolve(&CapFilter::new(cap.ns.as_str(), cap.name.as_str())).len()
            });
            let satisfied = actual >= g.min_agents;
            let deficit   = g.min_agents.saturating_sub(actual);
            GroupStatus {
                name:        g.name.clone(),
                description: g.description.clone(),
                min_agents:  g.min_agents,
                max_agents:  g.max_agents,
                actual,
                satisfied,
                deficit,
            }
        }).collect();
        MeshStatus { groups }
    }

    /// Prints the declared anatomy to stdout.
    pub fn print_anatomy(&self) {
        println!("Mesh          : {} v{}", self.mesh.name, self.mesh.version);
        println!("Groups        : {}", self.groups.len());
        for g in &self.groups {
            let cap_str = g.capabilities.first()
                .map(|c| format!("{}/{}", c.ns, c.name))
                .unwrap_or_else(|| "(none)".into());
            let max_str = g.max_agents
                .map(|m| format!("/{m}"))
                .unwrap_or_default();
            let desc = g.description.as_deref()
                .map(|d| format!("  — {d}"))
                .unwrap_or_default();
            println!(
                "  {:22} min={}{:<5}  indicator: {cap_str}{desc}",
                g.name, g.min_agents, max_str,
            );
        }
    }
}
