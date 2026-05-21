//! Declarative local capability configuration.
//!
//! A node declares what external services it hosts and what mesh capabilities
//! to advertise when those services respond to health probes. The probe-and-
//! advertise lifecycle is started by calling [`run_capability_probes`].
//!
//! ## Separation of concerns
//!
//! [`GossipConfig`](crate::GossipConfig) owns *how* this node connects to the
//! mesh (ports, peers, TTL).  [`NodeCapabilityConfig`] owns *what* this node
//! offers — which external services are co-located and what capability shape
//! to advertise when they are healthy.
//!
//! ## TOML format
//!
//! ```toml
//! # One [[capability]] block per advertised capability.
//! # Multiple blocks with the same ns/name are valid (e.g. several installable models).
//!
//! [[capability]]
//! ns                 = "llm"
//! name               = "inference"
//! probe_url          = "http://localhost:11434/api/tags"
//! probe_timeout_secs = 3
//! ttl_secs           = 30
//!
//!   [capability.attrs]
//!   model    = "llama3.2"
//!   context  = 8192
//!   backend  = "ollama"
//!   endpoint = "http://localhost:11434/v1"
//!
//! [[capability]]
//! ns       = "data"
//! name     = "realtime"
//! ttl_secs = 60
//!   # no probe_url → always-alive
//! ```
//!
//! `probe_url` is optional. When absent the capability is treated as
//! always-alive — useful for in-process capabilities (MCP tool handlers,
//! compute functions) that don't have a separate health endpoint.

use crate::{CapValue, Capability, CapabilityHandle, GossipAgent};
use crate::error::GossipError;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{atomic::{AtomicBool, Ordering}, Arc},
    time::Duration,
};

// ── Config types ──────────────────────────────────────────────────────────────

/// Top-level node capability configuration.
///
/// Load with [`NodeCapabilityConfig::load_from_file`], then drive the probe
/// loop with [`run_capability_probes`].
#[derive(Debug, Default, Deserialize)]
pub struct NodeCapabilityConfig {
    /// All capability probe entries declared for this node.
    #[serde(default, rename = "capability")]
    pub capabilities: Vec<CapabilityProbeEntry>,
}

/// A single probe-and-advertise declaration.
///
/// On startup and every 10 s thereafter, [`run_capability_probes`] GETs
/// `probe_url` (if set).  A 2xx response causes the capability to be
/// advertised (or kept alive); any other outcome tombstones it until the
/// probe recovers.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CapabilityProbeEntry {
    /// Capability namespace, e.g. `"llm"`.
    pub ns: String,
    /// Capability name, e.g. `"inference"`.
    pub name: String,
    /// HTTP GET URL to probe for liveness. 2xx = service alive.
    /// Absent = always-alive (no external dependency to check).
    pub probe_url: Option<String>,
    /// Probe request timeout. Default: 3 s.
    #[serde(default = "default_probe_timeout_secs")]
    pub probe_timeout_secs: u64,
    /// Capability re-assertion TTL passed to [`GossipAgent::advertise_capability`].
    /// Default: 30 s.
    #[serde(default = "default_ttl_secs")]
    pub ttl_secs: u64,
    /// Static capability attributes announced to the mesh on probe success.
    #[serde(default)]
    pub attrs: BTreeMap<String, TomlCapValue>,
}

fn default_probe_timeout_secs() -> u64 { 3 }
fn default_ttl_secs()            -> u64 { 30 }

/// TOML-deserializable capability attribute value.
///
/// Converts directly into [`CapValue`] via [`From`].
///
/// Variant order is load-bearing: serde's untagged deserialization tries
/// each variant in declaration order, so `Bool` must precede `Integer`
/// (otherwise `true`/`false` would be attempted as integers first).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum TomlCapValue {
    Bool(bool),
    Integer(i64),
    Float(f64),
    Text(String),
}

impl From<TomlCapValue> for CapValue {
    fn from(v: TomlCapValue) -> Self {
        match v {
            TomlCapValue::Bool(b)    => CapValue::Bool(b),
            TomlCapValue::Integer(n) => CapValue::Integer(n),
            TomlCapValue::Float(f)   => CapValue::Float(f),
            TomlCapValue::Text(s)    => CapValue::Text(s.into()),
        }
    }
}

impl NodeCapabilityConfig {
    /// Loads and parses a TOML capability config file.
    ///
    /// The file uses the `[[capability]]` array-of-tables format described in
    /// the module documentation.
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, GossipError> {
        let s = std::fs::read_to_string(path).map_err(GossipError::Io)?;
        toml::from_str(&s).map_err(GossipError::Toml)
    }
}

impl CapabilityProbeEntry {
    pub(crate) fn build_capability(&self) -> Capability {
        let mut cap = Capability::new(self.ns.as_str(), self.name.as_str());
        for (k, v) in &self.attrs {
            cap = cap.with(k.as_str(), v.clone().into());
        }
        cap
    }

    pub(crate) async fn passes_probe(&self, client: &reqwest::Client) -> bool {
        let Some(url) = &self.probe_url else { return true };
        client
            .get(url)
            .timeout(Duration::from_secs(self.probe_timeout_secs))
            .send().await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
    }
}

// ── Probe events ──────────────────────────────────────────────────────────────

/// Emitted by [`run_capability_probes`] whenever a capability transitions
/// between live and tombstoned.
pub struct ProbeEvent {
    /// Namespace of the capability that changed state.
    pub ns:    String,
    /// Name of the capability that changed state.
    pub name:  String,
    /// New state.
    pub state: ProbeState,
}

/// Whether the probe passed or failed.
pub enum ProbeState {
    /// Probe passed — capability is advertised on the mesh.
    Up,
    /// Probe failed — capability handle dropped (KV tombstoned).
    Down,
}

// ── Probe loop ────────────────────────────────────────────────────────────────

const HEALTH_INTERVAL_SECS: u64 = 10;

/// Runs the probe-and-advertise loop for all entries in `config`.
///
/// For each entry the loop:
/// - Probes the declared URL immediately on startup (always-alive when no URL).
/// - Calls [`GossipAgent::advertise_capability`] and fires
///   `on_event(ProbeEvent { state: Up })` when the probe passes.
/// - Re-probes every 10 s; drops the [`CapabilityHandle`] (tombstoning the
///   KV entry) and fires `on_event(ProbeEvent { state: Down })` on failure.
/// - Re-advertises and fires `Up` on the next successful probe after a failure.
///
/// **`pause_flag`** — when set to `true` (e.g. by a manifest control watcher),
/// the loop drops all active handles (tombstoning their KV entries) and skips
/// re-advertisement until the flag is cleared.  On clearance the next iteration
/// runs a full probe pass and re-advertises any capabilities whose probes pass.
/// Pass `Arc::new(AtomicBool::new(false))` for normal always-on operation.
///
/// `on_event` is called synchronously within the probe loop — keep it cheap
/// (e.g., push to a channel or append to a `Mutex<Vec>`). It must be `Send`
/// since the loop runs inside a `tokio::spawn`.
///
/// This function never returns. Call it inside `tokio::spawn`:
///
/// ```ignore
/// tokio::spawn(run_capability_probes(agent, config, pause_flag, |e| {
///     println!("{}/{} is {}", e.ns, e.name,
///              if matches!(e.state, ProbeState::Up) { "up" } else { "down" });
/// }));
/// ```
pub async fn run_capability_probes<F>(
    agent:      Arc<GossipAgent>,
    config:     NodeCapabilityConfig,
    pause_flag: Arc<AtomicBool>,
    on_event:   F,
) where
    F: Fn(ProbeEvent) + Send + 'static,
{
    let client = reqwest::Client::new();
    let n = config.capabilities.len();
    let mut handles: Vec<Option<CapabilityHandle>> = (0..n).map(|_| None).collect();

    // ── Initial probe pass ────────────────────────────────────────────────────
    if !pause_flag.load(Ordering::Relaxed) {
        for (i, entry) in config.capabilities.iter().enumerate() {
            if entry.passes_probe(&client).await {
                handles[i] = Some(agent.advertise_capability(
                    entry.build_capability(),
                    Duration::from_secs(entry.ttl_secs),
                ));
                tracing::info!(ns = %entry.ns, name = %entry.name, "capability up");
                on_event(ProbeEvent {
                    ns: entry.ns.clone(), name: entry.name.clone(), state: ProbeState::Up,
                });
            } else {
                tracing::warn!(
                    ns = %entry.ns, name = %entry.name,
                    probe_url = ?entry.probe_url,
                    "capability probe failed — will retry every {HEALTH_INTERVAL_SECS}s",
                );
                on_event(ProbeEvent {
                    ns: entry.ns.clone(), name: entry.name.clone(), state: ProbeState::Down,
                });
            }
        }
    }

    // ── Health loop ───────────────────────────────────────────────────────────
    loop {
        tokio::time::sleep(Duration::from_secs(HEALTH_INTERVAL_SECS)).await;

        if pause_flag.load(Ordering::Relaxed) {
            // Paused: tombstone all live handles.
            for (i, entry) in config.capabilities.iter().enumerate() {
                if handles[i].is_some() {
                    handles[i] = None;
                    tracing::info!(ns = %entry.ns, name = %entry.name,
                                   "capability paused — tombstoned");
                    on_event(ProbeEvent {
                        ns: entry.ns.clone(), name: entry.name.clone(), state: ProbeState::Down,
                    });
                }
            }
            continue;
        }

        for (i, entry) in config.capabilities.iter().enumerate() {
            let was_up = handles[i].is_some();
            let is_up  = entry.passes_probe(&client).await;

            match (was_up, is_up) {
                (true, false) => {
                    handles[i] = None;
                    tracing::warn!(ns = %entry.ns, name = %entry.name,
                                   "capability probe failed — tombstoned");
                    on_event(ProbeEvent {
                        ns: entry.ns.clone(), name: entry.name.clone(), state: ProbeState::Down,
                    });
                }
                (false, true) => {
                    handles[i] = Some(agent.advertise_capability(
                        entry.build_capability(),
                        Duration::from_secs(entry.ttl_secs),
                    ));
                    tracing::info!(ns = %entry.ns, name = %entry.name,
                                   "capability recovered — re-advertised");
                    on_event(ProbeEvent {
                        ns: entry.ns.clone(), name: entry.name.clone(), state: ProbeState::Up,
                    });
                }
                _ => {}
            }
        }
    }
}
