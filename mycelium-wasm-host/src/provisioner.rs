//! The **provisioner** — the app-layer loop that *closes* the autonomic loop (M15 item 4):
//! watch demand → resolve unmet requirements against the installable catalog → dispatch to the
//! matching [`ArtifactRuntime`] (pull + verify + install) → advertise, so demand is relieved.
//!
//! **Core Principle 1 (no coordinator).** This is a regular agent built on Mycelium's public API,
//! **not** a substrate mechanism. The library never auto-provisions; the agency to pull-and-run is
//! the node's own local choice. No coordinator assigns provisioning duty — every node runs its own
//! provisioner, each independently observes demand and **self-elects** (probabilistically, to damp
//! the thundering herd; any over-provisioning self-corrects when a future governor sheds providers
//! over `max`). This generalises `demand.rs`'s "the library never auto-advertises" stance.
//!
//! **Kind dispatch (`docs/design/artifact-library.md` §4).** The convergence loops here are
//! kind-agnostic — they reason about capabilities, demand, and provider counts. *How* an artifact
//! becomes live is the registered [`ArtifactRuntime`]'s business: `WasmComponent` instantiates in
//! the sandboxed host, `Blob` places bytes for a node-local runtime. A node without a runtime for
//! an entry's kind (or whose install budget the entry exceeds) silently never self-elects for it —
//! **eligibility is node-local truth** — and a tripwire counter records the skip (detection, not
//! prevention). Installs run as **background tasks against an `Installing` reservation**, so a
//! multi-GB pull never blocks the provision tick and a round never double-starts an artifact.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mycelium::{CapFilter, CapValue, Capability, CapabilityReg, GossipAgent};

use crate::artifact::{ArtifactId, ArtifactKind, ArtifactSource};
use crate::catalog::{InstallableCatalog, InstallableEntry, ResourceRequirements};
use crate::host::WasmHost;
use crate::resources::{ResourceProbe, SystemResourceProbe};
use crate::runtime::{ArtifactRuntime, Installed, ProgressFn, RuntimeCtx, WasmComponentRuntime};

/// How often a provisioned capability re-asserts its `cap/` advertisement.
const ADVERTISE_INTERVAL: Duration = Duration::from_secs(5);

/// One hosted artifact's node-level lifecycle state.
enum HostedState {
    /// A background install task is in flight — the reservation that prevents a later round
    /// double-starting the same artifact. The token identifies *which* task, so a
    /// withdraw-then-reinstall never lets a stale task's completion clobber the newer install.
    /// `reserved` carries the entry's declared requirements so concurrent eligibility checks
    /// see resources already spoken for (two 3 GB models must not both pass a 5 GB-free check).
    Installing { token: u64, reserved: ResourceRequirements },
    /// Installed, advertised, serving. (Its real consumption — placed bytes on disk, activated
    /// memory — is visible to the probe directly, so no virtual accounting is kept.)
    Live(LiveHosted),
}

/// A capability this node has provisioned and is now hosting: the advertisement registration
/// (dropping it tombstones the `cap/` entry) plus the runtime's lifecycle handle.
struct LiveHosted {
    _cap:      CapabilityReg,
    installed: Box<dyn Installed>,
}

/// A **capability-presence invariant** (M14 supervision): keep at least `min_providers` live
/// providers of `filter` across the fleet. A standing desired-state, independent of organic demand
/// — it is what makes provisioning *self-healing*: a provider that dies has its `cap/` entry
/// evaporate, the live count drops below `min_providers`, and a supervising node re-provisions.
/// **Restart ≡ first-time provisioning** — the same resolve-and-pull path serves both.
#[derive(Clone, Debug)]
pub struct SupervisionPolicy {
    pub filter:        CapFilter,
    pub min_providers: usize,
    /// Upper bound (Track 2b elastic sizing). `Some(max)` ⇒ when the live provider count exceeds
    /// `max`, a hosting node self-elects to **withdraw** (cooperative self-removal — tombstone its
    /// `cap/` + stop serving), the symmetric shed path to bring-up. `None` ⇒ unbounded.
    pub max_providers: Option<usize>,
}

/// An app-layer provisioner / supervisor. Construct it with the node, a [`WasmHost`], an
/// [`InstallableCatalog`], and an [`ArtifactSource`]; call [`provision_round`](Self::provision_round)
/// on a tick to keep the node provisioning what (a) unmet **demand** and (b) **presence
/// invariants** ([`supervise`](Self::supervise)) call for — both are just "desired state" the node
/// reconciles locally, the same resolve-and-pull path. The
/// [`WasmComponent`](ArtifactKind::WasmComponent) runtime is registered automatically; add more
/// kinds via [`register_runtime`](Self::register_runtime).
pub struct Provisioner {
    agent:        Arc<GossipAgent>,
    catalog:      InstallableCatalog,
    source:       Arc<dyn ArtifactSource + Send + Sync>,
    /// Install dispatch: one runtime per [`ArtifactKind`] this node can host.
    runtimes:     HashMap<ArtifactKind, Arc<dyn ArtifactRuntime>>,
    /// Probability of self-electing to satisfy an unmet requirement on a given round (herd
    /// damping). `1.0` = always (fine for a single provisioner); lower it when many nodes run one.
    self_elect_p: f64,
    /// Capability-presence invariants this node supervises (M14).
    policies:     Vec<SupervisionPolicy>,
    /// If non-empty, only catalog entries with valid provenance from one of these publisher keys
    /// are installed (Ed25519 over the entry — kind, content address, declared-provide). Empty =
    /// accept any (integrity-only).
    trusted_publishers: Vec<[u8; 32]>,
    /// Skip entries whose `size_bytes` hint exceeds this (node-local install budget). `None` =
    /// unbounded.
    install_budget_bytes: Option<u64>,
    /// Resource-aware eligibility (§4.4): a probe of this node's free memory/disk plus a
    /// headroom fraction (< 1.0) — an entry's declared requirements must fit within
    /// `headroom × available − reserved-by-in-flight-installs`. Default: the system probe at
    /// 0.8. `None` = disabled. Unmeasurable resources are permissive (detection, not
    /// prevention).
    resource_policy: Option<(Arc<dyn ResourceProbe>, f64)>,
    /// Artifacts this node has reserved (install in flight) or brought live — a round never
    /// double-starts. Lock discipline: acquired once per function, flat, never across `await`
    /// (wiki lock-order table row 21).
    hosted:       Arc<Mutex<HashMap<ArtifactId, HostedState>>>,
    /// Tripwire (detection, not prevention): resolvable entries skipped because no runtime is
    /// registered for their kind or they exceed the install budget. Counts skip *events* (one
    /// per entry per round), not distinct entries.
    ineligible:   Arc<AtomicU64>,
}

impl Provisioner {
    /// Build a provisioner that self-elects with probability `self_elect_p` (use `1.0` for a
    /// single provisioner; lower for a fleet of them). Registers the WASM-component runtime over
    /// `host`; add other kinds via [`register_runtime`](Self::register_runtime).
    pub fn new(
        agent: Arc<GossipAgent>,
        host: Arc<WasmHost>,
        catalog: InstallableCatalog,
        source: Arc<dyn ArtifactSource + Send + Sync>,
        self_elect_p: f64,
    ) -> Self {
        let mut runtimes: HashMap<ArtifactKind, Arc<dyn ArtifactRuntime>> = HashMap::new();
        runtimes.insert(ArtifactKind::WasmComponent, Arc::new(WasmComponentRuntime::new(host)));
        Self {
            agent,
            catalog,
            source,
            runtimes,
            self_elect_p,
            policies: Vec::new(),
            trusted_publishers: Vec::new(),
            install_budget_bytes: None,
            resource_policy: Some((Arc::new(SystemResourceProbe::new()), 0.8)),
            hosted: Arc::new(Mutex::new(HashMap::new())),
            ineligible: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Register a runtime for an additional [`ArtifactKind`] (e.g. a blob/model runtime). A node
    /// only ever self-elects for kinds it has a runtime for — eligibility is node-local truth.
    pub fn register_runtime(&mut self, runtime: Arc<dyn ArtifactRuntime>) {
        self.runtimes.insert(runtime.kind(), runtime);
    }

    /// Cap the artifacts this node will elect to install by their `size_bytes` hint.
    pub fn set_install_budget(&mut self, max_bytes: u64) {
        self.install_budget_bytes = Some(max_bytes);
    }

    /// Override the resource-eligibility policy (§4.4): an entry's declared requirements must
    /// fit within `headroom × available − reserved`, where `available` comes from `probe`
    /// (memory globally; disk at the kind's runtime `resource_root`). `headroom` is clamped to
    /// `(0, 1]` — a node never commits 100 %+ of what it has free. The default is the system
    /// probe at `0.8`.
    pub fn set_resource_policy(&mut self, probe: Arc<dyn ResourceProbe>, headroom: f64) {
        self.resource_policy = Some((probe, headroom.clamp(f64::MIN_POSITIVE, 1.0)));
    }

    /// Disable resource-aware eligibility entirely (kind + budget checks remain).
    pub fn disable_resource_policy(&mut self) {
        self.resource_policy = None;
    }

    /// Requirements reserved by in-flight installs (the `Installing` states).
    fn reserved_requirements(&self) -> ResourceRequirements {
        let map = self.hosted.lock().unwrap();
        let mut sum = ResourceRequirements::default();
        for state in map.values() {
            if let HostedState::Installing { reserved, .. } = state {
                sum.disk_bytes = sum.disk_bytes.saturating_add(reserved.disk_bytes);
                sum.mem_bytes = sum.mem_bytes.saturating_add(reserved.mem_bytes);
            }
        }
        sum
    }

    /// Require **signed provenance**: only install catalog entries carrying a valid Ed25519
    /// signature over the entry (kind + content address + declared-provide) from one of `trusted`
    /// publisher keys. Without this, the provisioner trusts the catalog for integrity (hash match)
    /// but not origin; with it, an unsigned or untrusted-signer artifact is refused even if its
    /// bytes hash correctly.
    pub fn require_provenance(&mut self, trusted: Vec<[u8; 32]>) {
        self.trusted_publishers = trusted;
    }

    /// True if `entry` may be installed under the current provenance policy.
    fn provenance_ok(&self, entry: &InstallableEntry) -> bool {
        self.trusted_publishers.is_empty() || entry.verify_provenance(&self.trusted_publishers)
    }

    /// True if this node *can* host `entry`: a runtime is registered for its kind, it fits the
    /// install budget, and its declared requirements fit this node's **resource headroom**
    /// (§4.4: `headroom × available − reserved-by-in-flight`; memory globally, disk at the
    /// runtime's `resource_root`; undeclared requirements and unmeasurable resources are
    /// permissive). A miss is silent non-participation — some node that fits elects; this is
    /// the fleet's placement algorithm, with no scheduler and no resource gossip — plus a
    /// tripwire tick, see [`ineligible_skips`](Self::ineligible_skips).
    fn eligible(&self, entry: &InstallableEntry) -> bool {
        let Some(runtime) = self.runtimes.get(&entry.kind) else {
            self.ineligible.fetch_add(1, Ordering::Relaxed);
            metrics::counter!("mycelium_artifact_ineligible_skips_total", "reason" => "no_runtime")
                .increment(1);
            tracing::debug!(ns = %entry.provides.namespace, name = %entry.provides.name,
                kind = ?entry.kind, "no runtime registered for kind — not electing");
            return false;
        };
        if let Some(max) = self.install_budget_bytes
            && entry.size_bytes > max
        {
            self.ineligible.fetch_add(1, Ordering::Relaxed);
            metrics::counter!("mycelium_artifact_ineligible_skips_total", "reason" => "budget")
                .increment(1);
            tracing::debug!(ns = %entry.provides.namespace, name = %entry.provides.name,
                size = entry.size_bytes, budget = max, "entry exceeds install budget — not electing");
            return false;
        }
        if let Some((probe, headroom)) = &self.resource_policy
            && (entry.requires.mem_bytes > 0 || entry.requires.disk_bytes > 0)
        {
            let reserved = self.reserved_requirements();
            let usable = |avail: u64| (avail as f64 * headroom) as u64;
            if entry.requires.mem_bytes > 0
                && let Some(avail) = probe.available_memory_bytes()
                && entry.requires.mem_bytes.saturating_add(reserved.mem_bytes) > usable(avail)
            {
                self.ineligible.fetch_add(1, Ordering::Relaxed);
                metrics::counter!("mycelium_artifact_ineligible_skips_total", "reason" => "memory")
                    .increment(1);
                tracing::debug!(ns = %entry.provides.namespace, name = %entry.provides.name,
                    require = entry.requires.mem_bytes, reserved = reserved.mem_bytes,
                    available = avail, headroom, "memory requirement exceeds headroom — not electing");
                return false;
            }
            if entry.requires.disk_bytes > 0
                && let Some(root) = runtime.resource_root()
                && let Some(avail) = probe.available_disk_bytes(root)
                && entry.requires.disk_bytes.saturating_add(reserved.disk_bytes) > usable(avail)
            {
                self.ineligible.fetch_add(1, Ordering::Relaxed);
                metrics::counter!("mycelium_artifact_ineligible_skips_total", "reason" => "disk")
                    .increment(1);
                tracing::debug!(ns = %entry.provides.namespace, name = %entry.provides.name,
                    require = entry.requires.disk_bytes, reserved = reserved.disk_bytes,
                    available = avail, headroom, "disk requirement exceeds headroom — not electing");
                return false;
            }
        }
        true
    }

    /// Tripwire counter: skip events for resolvable-but-ineligible entries (no runtime for the
    /// kind, or over the install budget). One tick per skipped entry per round.
    pub fn ineligible_skips(&self) -> u64 {
        self.ineligible.load(Ordering::Relaxed)
    }

    /// Add a capability-presence invariant (M14 supervision): keep ≥ `min_providers` live providers
    /// of `filter` alive, re-provisioning from the catalog when the count drops (a dead provider's
    /// `cap/` evaporates → count falls → re-satisfied). Drives bring-up *without* organic demand.
    pub fn supervise(&mut self, filter: CapFilter, min_providers: usize) {
        self.policies.push(SupervisionPolicy { filter, min_providers, max_providers: None });
    }

    /// Like [`supervise`](Self::supervise) but with an upper bound (Track 2b elastic sizing): keep
    /// the live provider count within `[min, max]`. Below `min` → bring up; above `max` → a hosting
    /// node self-elects to **withdraw** (cooperative self-removal). Bounds are convergence targets,
    /// not guarantees (sovereign veto / soft self-election), consistent with the membership governor.
    pub fn supervise_band(&mut self, filter: CapFilter, min_providers: usize, max_providers: usize) {
        self.policies.push(SupervisionPolicy {
            filter,
            min_providers,
            max_providers: Some(max_providers),
        });
    }

    /// Stop hosting `artifact`: tombstone its `cap/` advertisement and let the runtime tear down
    /// ([`Installed::uninstall`] — abort the serve task, delete placed bytes). An **in-flight**
    /// install is cancelled by removing its reservation; the install task's token check tears the
    /// finished result down on completion. Cooperative self-removal — the symmetric counterpart
    /// to [`start_install`](Self::start_install).
    fn withdraw(&mut self, artifact: &ArtifactId) -> bool {
        let removed = self.hosted.lock().unwrap().remove(artifact);
        match removed {
            Some(HostedState::Live(live)) => {
                live.installed.uninstall(); // dropping live._cap tombstones the cap/ entry
                true
            }
            Some(HostedState::Installing { .. }) => true,
            None => false,
        }
    }

    /// Number of capabilities this node is hosting **live** via provisioning. In-flight installs
    /// are not counted — see [`installing_count`](Self::installing_count).
    pub fn hosted_count(&self) -> usize {
        self.hosted
            .lock()
            .unwrap()
            .values()
            .filter(|s| matches!(s, HostedState::Live(_)))
            .count()
    }

    /// Number of installs currently in flight (reserved, background task running).
    pub fn installing_count(&self) -> usize {
        self.hosted
            .lock()
            .unwrap()
            .values()
            .filter(|s| matches!(s, HostedState::Installing { .. }))
            .count()
    }

    /// True if `artifact` is reserved or live on this node.
    fn is_hosted(&self, artifact: &ArtifactId) -> bool {
        self.hosted.lock().unwrap().contains_key(artifact)
    }

    /// Start bringing one capability live on this node: **reserve** the artifact, then run the
    /// kind's runtime install as a background task (pull + verify + install), advertising the
    /// declared-provide and flipping the reservation to `Live` on success (on failure the
    /// reservation is dropped, so a later round retries — restart ≡ provisioning). Returns `true`
    /// if an install was newly started, `false` if already reserved/hosted or no runtime matches.
    /// Shared by the demand and presence paths — the one resolve-and-pull path the architecture
    /// promises.
    fn start_install(&self, entry: InstallableEntry) -> bool {
        let Some(runtime) = self.runtimes.get(&entry.kind).map(Arc::clone) else {
            return false; // eligible() screens this; belt-and-braces for direct callers
        };

        static INSTALL_SEQ: AtomicU64 = AtomicU64::new(0);
        let token = INSTALL_SEQ.fetch_add(1, Ordering::Relaxed);
        {
            let mut map = self.hosted.lock().unwrap();
            if map.contains_key(&entry.artifact) {
                return false;
            }
            map.insert(
                entry.artifact,
                HostedState::Installing { token, reserved: entry.requires },
            );
        }
        metrics::counter!("mycelium_artifact_installs_started_total").increment(1);

        let agent = Arc::clone(&self.agent);
        let source = Arc::clone(&self.source);
        let hosted = Arc::clone(&self.hosted);
        tokio::spawn(async move {
            let artifact = entry.artifact;
            let provides = entry.provides.clone();

            // Loading tier: while the install runs, `{ns}/loading` is advertised with a `pct`
            // attribute stepped in tens — the capability-tier convention the llm_agent example
            // established, here driven by real bytes from the runtime's pull. Each step
            // tombstones-then-re-advertises (that order makes the advertise the LWW winner);
            // the whole tier drops when the install resolves. Lock-order table row 22.
            let loading: Arc<Mutex<(u64, Option<CapabilityReg>)>> =
                Arc::new(Mutex::new((u64::MAX, None)));
            let progress: ProgressFn = {
                let loading = Arc::clone(&loading);
                let agent = Arc::clone(&agent);
                let ns = provides.namespace.clone();
                Arc::new(move |fetched, total| {
                    if total == 0 {
                        return;
                    }
                    let step = (fetched.saturating_mul(100) / total).min(100) / 10 * 10;
                    let mut tier = loading.lock().unwrap();
                    if tier.0 == step {
                        return;
                    }
                    tier.0 = step;
                    let mut cap = Capability::new(ns.clone(), "loading");
                    cap.attributes.insert("pct".into(), CapValue::Integer(step as i64));
                    tier.1.take(); // tombstone the previous step first
                    tier.1 =
                        Some(agent.capabilities().advertise_capability(cap, ADVERTISE_INTERVAL));
                })
            };
            let ctx = RuntimeCtx { agent: Arc::clone(&agent) };

            let result = runtime.install(entry, source, ctx, progress).await;
            loading.lock().unwrap().1.take(); // install resolved — the loading tier ends

            match result {
                Ok(installed) => {
                    // Advertise only after a successful install: a resolvable `cap/` entry always
                    // has a live receiver behind it (the runtime registered its serve path before
                    // returning).
                    let cap = agent
                        .capabilities()
                        .advertise_capability(provides.clone(), ADVERTISE_INTERVAL);
                    // Take the lock once; hand ownership back out on the not-ours path so the
                    // teardown below runs *outside* the lock. Dropping `installed` would NOT stop
                    // the serve path (JoinHandle drop detaches) — uninstall must be explicit.
                    let leftover = {
                        let mut map = hosted.lock().unwrap();
                        match map.get(&artifact) {
                            Some(HostedState::Installing { token: t, .. }) if *t == token => {
                                map.insert(
                                    artifact,
                                    HostedState::Live(LiveHosted { _cap: cap, installed }),
                                );
                                None
                            }
                            _ => Some((cap, installed)),
                        }
                    };
                    match leftover {
                        None => {
                            metrics::counter!("mycelium_artifact_installs_completed_total")
                                .increment(1);
                            tracing::info!(ns = %provides.namespace, name = %provides.name,
                                "provisioned + serving capability");
                        }
                        Some((cap, installed)) => {
                            // Withdrawn (or superseded) while installing: nothing references
                            // this install — tombstone the just-made ad and tear it down.
                            drop(cap);
                            installed.uninstall();
                            tracing::info!(ns = %provides.namespace, name = %provides.name,
                                "install finished after withdraw — torn down");
                        }
                    }
                }
                Err(e) => {
                    metrics::counter!("mycelium_artifact_installs_failed_total",
                        "stage" => e.stage()).increment(1);
                    tracing::warn!(ns = %provides.namespace, name = %provides.name, %e,
                        "provisioning failed");
                    let mut map = hosted.lock().unwrap();
                    if matches!(map.get(&artifact),
                        Some(HostedState::Installing { token: t, .. }) if *t == token)
                    {
                        map.remove(&artifact);
                    }
                }
            }
        });
        true
    }

    /// True if this node should self-elect to act this round (herd damping).
    fn self_elects(&self) -> bool {
        fastrand::f64() < self.self_elect_p
    }

    /// One convergence pass over **both** desired-state sources, returning how many installs were
    /// newly **started** (installs run as background tasks — poll
    /// [`hosted_count`](Self::hosted_count) / the capability ring for completion). Idempotent —
    /// already-reserved/hosted/satisfied entries are skipped.
    ///
    /// 1. **Demand-driven** (M15): a catalog entry whose declared-provide has demand but no live
    ///    provider → bring it live (relieves demand).
    /// 2. **Presence-driven** (M14 supervision): a policy whose live provider count is below
    ///    `min_providers` → resolve the catalog and bring a provider live. Self-healing falls out:
    ///    a dead provider's `cap/` evaporates, the count drops, this fires again — restart and
    ///    first-time provisioning are the same path.
    pub fn provision_round(&mut self) -> usize {
        let mut started = 0;

        // ── Probe-gated health (§4.2) ────────────────────────────────────────
        // A Live install whose probe fails is withdrawn — advertisement retracted, runtime
        // torn down — and the demand/presence passes reinstall it as soon as the retracted
        // ad clears the local capability view (typically the next round; the tombstone write
        // is not synchronous with this pass): restart ≡ provisioning. Health is the hosting
        // node's own observation; there is no fleet health protocol. Probes run under the
        // hosted lock — they must be cheap and non-blocking (see `Installed::probe`).
        let unhealthy: Vec<ArtifactId> = {
            let map = self.hosted.lock().unwrap();
            map.iter()
                .filter_map(|(artifact, state)| match state {
                    HostedState::Live(live) if !live.installed.probe() => Some(*artifact),
                    _ => None,
                })
                .collect()
        };
        for artifact in unhealthy {
            metrics::counter!("mycelium_artifact_probe_withdrawals_total").increment(1);
            tracing::warn!(artifact = %artifact,
                "hosted install failed its probe — withdrawing (this round reinstalls if still wanted)");
            self.withdraw(&artifact);
        }

        // ── Demand-driven (M15) ──────────────────────────────────────────────
        let entries: Vec<InstallableEntry> = self.catalog.entries().to_vec();
        for entry in entries {
            if self.is_hosted(&entry.artifact) || !self.provenance_ok(&entry) {
                continue;
            }
            let filter =
                CapFilter::new(entry.provides.namespace.clone(), entry.provides.name.clone());
            let demand = self.agent.capabilities().demand(&filter);
            let unmet = demand.providers.is_empty() && !demand.demanding_nodes.is_empty();
            if !unmet {
                continue;
            }
            // Eligibility is checked only for entries this node would otherwise act on — an
            // idle catalog must not tick the tripwire every round.
            if self.eligible(&entry) && self.self_elects() && self.start_install(entry) {
                started += 1;
            }
        }

        // ── Presence-driven (M14 supervision) ────────────────────────────────
        let policies = self.policies.clone();
        for policy in &policies {
            // Live provider count is freshness-aware: a crashed provider's cap/ entry ages out,
            // so `providers` reflects only currently-live providers (this is the self-heal trigger).
            let live = self.agent.capabilities().demand(&policy.filter).providers.len();
            if live >= policy.min_providers {
                continue; // invariant already satisfied across the fleet
            }
            // Resolve the catalog for an artifact that would satisfy the invariant.
            let Some(entry) = self.catalog.resolve_best(&policy.filter).cloned() else {
                continue; // nothing in the catalog provides it
            };
            if self.is_hosted(&entry.artifact)
                || !self.provenance_ok(&entry)
                || !self.eligible(&entry)
            {
                continue; // already reserved/hosted here, or fails policy
            }
            if self.self_elects() && self.start_install(entry) {
                started += 1;
            }
        }

        // ── Shed-driven (Track 2b elastic sizing) ────────────────────────────
        // Symmetric to bring-up: when a band's live provider count exceeds `max`, a hosting node
        // self-elects to withdraw. Over-provisioning (e.g. a transient duplicate after a herd of
        // self-elections) self-corrects here.
        for policy in &policies {
            let Some(max) = policy.max_providers else { continue };
            let live = self.agent.capabilities().demand(&policy.filter).providers.len();
            if live <= max {
                continue; // within the band
            }
            let Some(artifact) = self.catalog.resolve_best(&policy.filter).map(|e| e.artifact)
            else {
                continue;
            };
            if self.is_hosted(&artifact) && self.self_elects() {
                self.withdraw(&artifact); // cooperative self-removal
            }
        }

        started
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::InstallableEntry;
    use crate::InMemorySource;
    use mycelium::Capability;

    const ECHO_COMPONENT: &[u8] = include_bytes!("../tests/fixtures/echo_component.wasm");

    fn alloc_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
    }

    async fn live_agent() -> Arc<GossipAgent> {
        for _ in 0..16 {
            let port = alloc_port();
            let id = mycelium::NodeId::new("127.0.0.1", port).unwrap();
            let cfg = mycelium::GossipConfig { bind_port: port, ..Default::default() };
            let agent = Arc::new(GossipAgent::new(id, cfg));
            if agent.start().await.is_ok() {
                return agent;
            }
        }
        panic!("could not bind a gossip port after 16 attempts");
    }

    /// Structural poll: installs run as background tasks, so completion is awaited, not assumed.
    async fn wait_live(prov: &Provisioner, n: usize) {
        for _ in 0..200 {
            if prov.hosted_count() == n && prov.installing_count() == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!(
            "hosted_count did not settle at {n} (live={}, installing={})",
            prov.hosted_count(),
            prov.installing_count()
        );
    }

    #[tokio::test]
    async fn provisions_an_unmet_requirement_then_stops_once_satisfied() {
        let agent = live_agent().await;
        let host = Arc::new(WasmHost::new().expect("engine"));

        // Catalog: installing this artifact would provide text/echo.
        let mut source = InMemorySource::new();
        let id = source.insert(ECHO_COMPONENT.to_vec());
        let mut catalog = InstallableCatalog::new();
        catalog.add(InstallableEntry::new(Capability::new("text", "echo"), id));

        let mut prov = Provisioner::new(
            Arc::clone(&agent),
            host,
            catalog,
            Arc::new(source),
            1.0, // single provisioner: always self-elect
        );

        // No requirement yet → nothing to provision.
        assert_eq!(prov.provision_round(), 0, "no demand, no provisioning");

        // Declare a requirement for text/echo → demand with no provider.
        let _req = agent
            .capabilities()
            .declare_requirement(CapFilter::new("text", "echo"), Duration::from_secs(30));
        // Let the req/ write land.
        let mut saw_demand = false;
        for _ in 0..40 {
            let d = agent.capabilities().demand(&CapFilter::new("text", "echo"));
            if !d.demanding_nodes.is_empty() {
                saw_demand = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(saw_demand, "requirement should register as demand");

        // The provisioner satisfies it: starts the install (a background task), which pulls +
        // instantiates + advertises on completion.
        assert_eq!(prov.provision_round(), 1, "unmet requirement should start an install");
        wait_live(&prov, 1).await;

        // The advertisement relieves demand — a provider now exists.
        let mut saw_provider = false;
        for _ in 0..40 {
            let d = agent.capabilities().demand(&CapFilter::new("text", "echo"));
            if !d.providers.is_empty() {
                saw_provider = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(saw_provider, "provisioned capability should advertise itself");

        // Idempotent: a second round provisions nothing (already hosted + now has a provider).
        assert_eq!(prov.provision_round(), 0, "satisfied requirement is not re-provisioned");
        assert_eq!(prov.hosted_count(), 1);

        // The provisioned capability is callable: an inbound RPC reaches the component's `handle`.
        let reply = agent
            .service()
            .rpc_call(
                agent.node_id().clone(),
                crate::runtime::cap_invoke_kind("text", "echo"),
                b"call me".to_vec(),
                Duration::from_secs(5),
            )
            .await
            .expect("served capability replies");
        assert_eq!(reply.as_ref(), b"call me", "the hosted component echoed the invocation");

        agent.shutdown().await;
    }

    #[tokio::test]
    async fn supervision_brings_a_capability_live_with_no_demander() {
        // M14: a presence invariant ("keep >=1 provider of text/echo alive") provisions the
        // capability with NO organic demand — and because a restart re-runs this same path when a
        // dead provider's cap/ evaporates, restart and first-time provisioning are identical.
        let agent = live_agent().await;
        let host = Arc::new(WasmHost::new().expect("engine"));

        let mut source = InMemorySource::new();
        let id = source.insert(ECHO_COMPONENT.to_vec());
        let mut catalog = InstallableCatalog::new();
        catalog.add(InstallableEntry::new(Capability::new("text", "echo"), id));

        let mut prov =
            Provisioner::new(Arc::clone(&agent), host, catalog, Arc::new(source), 1.0);

        // No demand and no policy yet → nothing happens.
        assert_eq!(prov.provision_round(), 0, "no demand, no policy");

        // Supervise: keep >=1 provider of text/echo alive.
        prov.supervise(CapFilter::new("text", "echo"), 1);

        // The presence invariant alone (no demander) brings the capability live.
        assert_eq!(prov.provision_round(), 1, "presence invariant provisions with no demander");
        wait_live(&prov, 1).await;

        // It actually serves invocations.
        let reply = agent
            .service()
            .rpc_call(
                agent.node_id().clone(),
                crate::runtime::cap_invoke_kind("text", "echo"),
                b"supervised".to_vec(),
                Duration::from_secs(5),
            )
            .await
            .expect("served");
        assert_eq!(reply.as_ref(), b"supervised");

        // Invariant satisfied (a live provider exists) → subsequent rounds are no-ops.
        let mut saw_provider = false;
        for _ in 0..40 {
            if !agent.capabilities().demand(&CapFilter::new("text", "echo")).providers.is_empty() {
                saw_provider = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(saw_provider, "supervised capability advertises a provider");
        assert_eq!(prov.provision_round(), 0, "invariant satisfied → no re-provisioning");

        agent.shutdown().await;
    }

    #[tokio::test]
    async fn track2b_sheds_a_provider_when_over_max() {
        // Track 2b: the symmetric shed path. Bring a capability live, then a band whose `max` is
        // below the live count makes a hosting node self-elect to withdraw (tombstone + stop
        // serving). (Single-node: max=0 forces shed of the one local provider — the cross-node
        // case is the identical code path against real provider counts.)
        let agent = live_agent().await;
        let host = Arc::new(WasmHost::new().expect("engine"));

        let mut source = InMemorySource::new();
        let id = source.insert(ECHO_COMPONENT.to_vec());
        let mut catalog = InstallableCatalog::new();
        catalog.add(InstallableEntry::new(Capability::new("text", "echo"), id));

        let mut prov =
            Provisioner::new(Arc::clone(&agent), host, catalog, Arc::new(source), 1.0);

        // Bring it live via a min>=1 invariant.
        prov.supervise(CapFilter::new("text", "echo"), 1);
        assert_eq!(prov.provision_round(), 1);
        wait_live(&prov, 1).await;

        // Wait until the provider is observable (the shed decision is based on the live count).
        let mut saw_provider = false;
        for _ in 0..40 {
            if !agent.capabilities().demand(&CapFilter::new("text", "echo")).providers.is_empty() {
                saw_provider = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(saw_provider, "provider must be observable before it can be shed");

        // Replace the policy with a band that wants *fewer* than are live (max=0) → shed.
        prov.policies.clear();
        prov.supervise_band(CapFilter::new("text", "echo"), 0, 0);
        prov.provision_round(); // shed pass withdraws the local provider
        assert_eq!(prov.hosted_count(), 0, "over-max provider is withdrawn");

        agent.shutdown().await;
    }

    #[tokio::test]
    async fn require_provenance_refuses_unsigned_but_installs_trusted_signed() {
        use crate::catalog::InstallableEntry;
        use ed25519_dalek::SigningKey;

        let agent = live_agent().await;
        let host = Arc::new(WasmHost::new().expect("engine"));
        let sk = SigningKey::from_bytes(&[42u8; 32]);
        let trusted = sk.verifying_key().to_bytes();

        let mut source = InMemorySource::new();
        let id = source.insert(ECHO_COMPONENT.to_vec());
        let source: Arc<dyn crate::ArtifactSource + Send + Sync> = Arc::new(source);

        // Unsigned catalog entry + provenance required → refused.
        let mut unsigned_cat = InstallableCatalog::new();
        unsigned_cat.add(InstallableEntry::new(Capability::new("text", "echo"), id));
        let mut p1 = Provisioner::new(
            Arc::clone(&agent), Arc::clone(&host), unsigned_cat, Arc::clone(&source), 1.0);
        p1.require_provenance(vec![trusted]);
        p1.supervise(CapFilter::new("text", "echo"), 1);
        assert_eq!(p1.provision_round(), 0, "unsigned artifact refused under provenance policy");
        assert_eq!(p1.hosted_count(), 0);

        // Signed by the trusted publisher → installed.
        let mut signed_cat = InstallableCatalog::new();
        signed_cat.add(InstallableEntry::new(Capability::new("text", "echo"), id).signed_by(&sk));
        let mut p2 = Provisioner::new(Arc::clone(&agent), host, signed_cat, source, 1.0);
        p2.require_provenance(vec![trusted]);
        p2.supervise(CapFilter::new("text", "echo"), 1);
        assert_eq!(p2.provision_round(), 1, "trusted-signed artifact starts installing");
        wait_live(&p2, 1).await;

        agent.shutdown().await;
    }

    /// A probe reporting fixed numbers — resource-eligibility tests must not depend on the
    /// machine they run on.
    struct FixedProbe {
        mem:  u64,
        disk: u64,
    }
    impl ResourceProbe for FixedProbe {
        fn available_memory_bytes(&self) -> Option<u64> {
            Some(self.mem)
        }
        fn available_disk_bytes(&self, _at: &std::path::Path) -> Option<u64> {
            Some(self.disk)
        }
    }

    /// A Blob runtime whose installs block on a semaphore — lets a test hold an install
    /// in-flight while asserting what a concurrent eligibility check sees.
    struct GatedRuntime {
        gate: Arc<tokio::sync::Semaphore>,
    }
    struct NoopInstalled;
    impl Installed for NoopInstalled {
        fn probe(&self) -> bool {
            true
        }
        fn uninstall(self: Box<Self>) {}
    }
    #[async_trait::async_trait]
    impl ArtifactRuntime for GatedRuntime {
        fn kind(&self) -> ArtifactKind {
            ArtifactKind::Blob
        }
        async fn install(
            &self,
            _entry: InstallableEntry,
            _source: Arc<dyn ArtifactSource + Send + Sync>,
            _ctx: RuntimeCtx,
            _progress: ProgressFn,
        ) -> Result<Box<dyn Installed>, crate::runtime::InstallError> {
            let permit = self
                .gate
                .acquire()
                .await
                .map_err(|e| crate::runtime::InstallError::Host(e.to_string()))?;
            permit.forget();
            Ok(Box::new(NoopInstalled))
        }
    }

    #[tokio::test]
    async fn resource_requirements_gate_election_and_count_inflight_reservations() {
        // §4.4: a node must not elect for an artifact it cannot fit — and "fit" must account
        // for installs already in flight (two 6 GB models must not both pass a 10 GB check).
        let agent = live_agent().await;
        let host = Arc::new(WasmHost::new().expect("engine"));

        let mut source = InMemorySource::new();
        let a = source.insert(b"model-a".to_vec());
        let b = source.insert(b"model-b".to_vec());
        let c = source.insert(b"model-c".to_vec());

        let mut catalog = InstallableCatalog::new();
        catalog.add(InstallableEntry::new(Capability::new("llm", "model-a"), a)
            .with_kind(ArtifactKind::Blob).with_requirements(0, 6_000));
        catalog.add(InstallableEntry::new(Capability::new("llm", "model-b"), b)
            .with_kind(ArtifactKind::Blob).with_requirements(0, 6_000));
        catalog.add(InstallableEntry::new(Capability::new("llm", "model-c"), c)
            .with_kind(ArtifactKind::Blob).with_requirements(0, 12_000)); // never fits alone

        let mut prov =
            Provisioner::new(Arc::clone(&agent), host, catalog, Arc::new(source), 1.0);
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        prov.register_runtime(Arc::new(GatedRuntime { gate: Arc::clone(&gate) }));
        prov.set_resource_policy(Arc::new(FixedProbe { mem: 10_000, disk: 10_000 }), 1.0);
        prov.supervise(CapFilter::new("llm", "model-a"), 1);
        prov.supervise(CapFilter::new("llm", "model-b"), 1);
        prov.supervise(CapFilter::new("llm", "model-c"), 1);

        // Round 1: model-a starts (reserves 6_000); model-b would joint-exceed (6k + 6k > 10k)
        // and is skipped; model-c exceeds alone (12k > 10k) and is skipped.
        assert_eq!(prov.provision_round(), 1, "only the first model fits");
        assert_eq!(prov.installing_count(), 1);
        assert_eq!(prov.ineligible_skips(), 2, "joint-exceed + exceeds-alone each ticked");

        // While model-a is still installing, nothing changes on a re-round.
        assert_eq!(prov.provision_round(), 0, "reservation still holds the headroom");
        assert!(prov.ineligible_skips() >= 3, "model-b (and c) keep ticking while blocked");

        // Let model-a finish: its reservation clears (real consumption is the probe's business,
        // and this probe is fixed) → model-b now fits; model-c still never does.
        gate.add_permits(1);
        wait_live(&prov, 1).await;
        assert_eq!(prov.provision_round(), 1, "freed headroom admits the second model");
        gate.add_permits(1);
        wait_live(&prov, 2).await;
        assert_eq!(prov.provision_round(), 0, "model-c can never fit on this node");

        agent.shutdown().await;
    }

    #[tokio::test]
    async fn blob_kind_installs_via_a_registered_runtime() {
        // The kind-dispatch acceptance path: a Blob entry, a registered BlobRuntime, and the
        // same demand/presence machinery — the artifact ends up *placed* (not instantiated)
        // and its capability advertised.
        use crate::artifact::FsLibrarySource;
        use crate::runtime::BlobRuntime;

        let agent = live_agent().await;
        let host = Arc::new(WasmHost::new().expect("engine"));

        static SEQ: AtomicU64 = AtomicU64::new(0);
        let base = std::env::temp_dir().join(format!(
            "mycelium-prov-blob-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
        ));
        let lib_dir = base.join("library");
        let place_dir = base.join("models");

        let lib = Arc::new(FsLibrarySource::open(&lib_dir).unwrap());
        let payload: Vec<u8> = (0..100u8).collect();
        let id = lib.store(&payload).unwrap();

        let mut catalog = InstallableCatalog::new();
        catalog.add(
            InstallableEntry::new(Capability::new("llm", "weights"), id)
                .with_kind(ArtifactKind::Blob)
                .with_cost(payload.len() as u64, 1),
        );

        let mut prov =
            Provisioner::new(Arc::clone(&agent), host, catalog, lib as Arc<_>, 1.0);
        prov.register_runtime(Arc::new(BlobRuntime::new(&place_dir).with_chunk_bytes(16)));
        prov.supervise(CapFilter::new("llm", "weights"), 1);

        assert_eq!(prov.provision_round(), 1, "blob install starts");
        wait_live(&prov, 1).await;

        // Placed (streamed from the ranged library source), content-correct.
        assert_eq!(std::fs::read(place_dir.join(id.to_hex())).unwrap(), payload);

        // And advertised like any provisioned capability.
        let mut saw_provider = false;
        for _ in 0..40 {
            if !agent.capabilities().demand(&CapFilter::new("llm", "weights")).providers.is_empty() {
                saw_provider = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(saw_provider, "placed blob's capability is advertised");

        agent.shutdown().await;
        std::fs::remove_dir_all(&base).ok();
    }

    /// An `Installed` that records its own teardown.
    struct TrackingInstalled {
        uninstalled: Arc<std::sync::atomic::AtomicBool>,
    }
    impl Installed for TrackingInstalled {
        fn probe(&self) -> bool {
            true
        }
        fn uninstall(self: Box<Self>) {
            self.uninstalled.store(true, Ordering::SeqCst);
        }
    }

    /// A Blob-kind runtime for exercising the provisioner's concurrency paths: optionally
    /// blocks on a semaphore mid-install, optionally fails the first N installs, and returns
    /// a teardown-tracking handle.
    struct TrackingRuntime {
        gate:           Option<Arc<tokio::sync::Semaphore>>,
        fail_remaining: Arc<AtomicU64>,
        uninstalled:    Arc<std::sync::atomic::AtomicBool>,
    }
    #[async_trait::async_trait]
    impl ArtifactRuntime for TrackingRuntime {
        fn kind(&self) -> ArtifactKind {
            ArtifactKind::Blob
        }
        async fn install(
            &self,
            _entry: InstallableEntry,
            _source: Arc<dyn ArtifactSource + Send + Sync>,
            _ctx: RuntimeCtx,
            _progress: ProgressFn,
        ) -> Result<Box<dyn Installed>, crate::runtime::InstallError> {
            if let Some(gate) = &self.gate {
                let permit = gate
                    .acquire()
                    .await
                    .map_err(|e| crate::runtime::InstallError::Host(e.to_string()))?;
                permit.forget();
            }
            if self
                .fail_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(crate::runtime::InstallError::Fetch("transient install failure".into()));
            }
            Ok(Box::new(TrackingInstalled { uninstalled: Arc::clone(&self.uninstalled) }))
        }
    }

    #[tokio::test]
    async fn wasm_component_full_lifecycle_install_invoke_shed_reinstall() {
        // The WasmComponent kind through its whole life via the provisioner: supervision
        // installs → the capability serves real invocations → the shed band withdraws it →
        // re-supervising reinstalls the SAME artifact and it serves again (restart ≡
        // provisioning as a test, not just a demo).
        let agent = live_agent().await;
        let host = Arc::new(WasmHost::new().expect("engine"));

        let mut source = InMemorySource::new();
        let id = source.insert(ECHO_COMPONENT.to_vec());
        let mut catalog = InstallableCatalog::new();
        catalog.add(InstallableEntry::new(Capability::new("text", "echo"), id));

        let mut prov =
            Provisioner::new(Arc::clone(&agent), host, catalog, Arc::new(source), 1.0);

        // Act 1 — install + serve.
        prov.supervise(CapFilter::new("text", "echo"), 1);
        assert_eq!(prov.provision_round(), 1);
        wait_live(&prov, 1).await;
        let reply = agent.service()
            .rpc_call(agent.node_id().clone(), crate::runtime::cap_invoke_kind("text", "echo"),
                b"first life".to_vec(), Duration::from_secs(5))
            .await.expect("serves");
        assert_eq!(reply.as_ref(), b"first life");

        // Act 2 — shed (cooperative self-removal).
        let mut observable = false;
        for _ in 0..40 {
            if !agent.capabilities().demand(&CapFilter::new("text", "echo")).providers.is_empty() {
                observable = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(observable);
        prov.policies.clear();
        prov.supervise_band(CapFilter::new("text", "echo"), 0, 0);
        prov.provision_round();
        assert_eq!(prov.hosted_count(), 0, "shed withdrew the component");

        // Act 3 — reinstall: the same demand machinery brings the same artifact back.
        prov.policies.clear();
        prov.supervise(CapFilter::new("text", "echo"), 1);
        let mut restarted = 0;
        for _ in 0..40 {
            restarted += prov.provision_round();
            if prov.hosted_count() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(restarted >= 1, "re-supervision reinstalls");
        wait_live(&prov, 1).await;
        let reply = agent.service()
            .rpc_call(agent.node_id().clone(), crate::runtime::cap_invoke_kind("text", "echo"),
                b"second life".to_vec(), Duration::from_secs(5))
            .await.expect("serves after reinstall");
        assert_eq!(reply.as_ref(), b"second life");

        agent.shutdown().await;
    }

    #[tokio::test]
    async fn blob_full_lifecycle_install_probe_selfheal_and_shed() {
        // The Blob kind through its whole life: supervision installs (streamed from the
        // library, placed on disk, advertised) → the placed file is destroyed out-of-band →
        // the probe pass catches it and the SAME round reinstalls (probe-gated self-heal) →
        // the shed band withdraws it and the placed file is actually deleted.
        use crate::artifact::FsLibrarySource;
        use crate::runtime::BlobRuntime;

        let agent = live_agent().await;
        let host = Arc::new(WasmHost::new().expect("engine"));

        static SEQ: AtomicU64 = AtomicU64::new(0);
        let base = std::env::temp_dir().join(format!(
            "mycelium-blob-lifecycle-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
        ));
        let (lib_dir, place_dir) = (base.join("library"), base.join("models"));

        let lib = Arc::new(FsLibrarySource::open(&lib_dir).unwrap());
        let payload: Vec<u8> = (0..200u8).cycle().take(500).collect();
        let id = lib.store(&payload).unwrap();
        let placed = place_dir.join(id.to_hex());

        let mut catalog = InstallableCatalog::new();
        catalog.add(
            InstallableEntry::new(Capability::new("llm", "weights"), id)
                .with_kind(ArtifactKind::Blob)
                .with_cost(payload.len() as u64, 1),
        );

        let mut prov =
            Provisioner::new(Arc::clone(&agent), host, catalog, lib as Arc<_>, 1.0);
        prov.register_runtime(Arc::new(BlobRuntime::new(&place_dir).with_chunk_bytes(64)));

        // Act 1 — install: streamed, placed, advertised.
        prov.supervise(CapFilter::new("llm", "weights"), 1);
        assert_eq!(prov.provision_round(), 1);
        wait_live(&prov, 1).await;
        assert_eq!(std::fs::read(&placed).unwrap(), payload);
        let mut observable = false;
        for _ in 0..40 {
            if !agent.capabilities().demand(&CapFilter::new("llm", "weights")).providers.is_empty() {
                observable = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(observable, "placed blob advertises");

        // Act 2 — sabotage: the placed file vanishes. The default probe (file exists) fails,
        // the probe pass withdraws, and supervision reinstalls from the library once the
        // retracted ad clears the local view (content-addressed placement makes the re-pull
        // honest: dest is gone). Structural poll over rounds — the tombstone lands async.
        std::fs::remove_file(&placed).unwrap();
        let mut restarted = 0;
        for _ in 0..80 {
            restarted += prov.provision_round();
            if restarted >= 1 && prov.hosted_count() == 1 && prov.installing_count() == 0
                && placed.exists()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(restarted >= 1, "probe failure leads to a reinstall");
        wait_live(&prov, 1).await;
        assert_eq!(std::fs::read(&placed).unwrap(), payload, "self-healed: file re-placed");

        // Act 3 — shed: cooperative self-removal deletes the placed bytes.
        prov.policies.clear();
        prov.supervise_band(CapFilter::new("llm", "weights"), 0, 0);
        prov.provision_round();
        assert_eq!(prov.hosted_count(), 0, "over-max blob is withdrawn");
        assert!(!placed.exists(), "uninstall deleted the placed blob");

        agent.shutdown().await;
        std::fs::remove_dir_all(&base).ok();
    }

    #[tokio::test]
    async fn failed_install_drops_the_reservation_and_a_later_round_retries() {
        // The Err path of start_install: a transient install failure must remove the
        // Installing reservation (not leave a zombie that blocks forever), and the next
        // round retries the same artifact to success — restart ≡ provisioning.
        let agent = live_agent().await;
        let host = Arc::new(WasmHost::new().expect("engine"));

        let mut source = InMemorySource::new();
        let id = source.insert(b"flaky model".to_vec());
        let mut catalog = InstallableCatalog::new();
        catalog.add(InstallableEntry::new(Capability::new("llm", "flaky"), id)
            .with_kind(ArtifactKind::Blob));

        let mut prov =
            Provisioner::new(Arc::clone(&agent), host, catalog, Arc::new(source), 1.0);
        let uninstalled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        prov.register_runtime(Arc::new(TrackingRuntime {
            gate:           None,
            fail_remaining: Arc::new(AtomicU64::new(1)), // first install fails
            uninstalled:    Arc::clone(&uninstalled),
        }));
        prov.supervise(CapFilter::new("llm", "flaky"), 1);

        assert_eq!(prov.provision_round(), 1, "first attempt starts");
        // The failure clears the reservation…
        let mut cleared = false;
        for _ in 0..200 {
            if prov.installing_count() == 0 && prov.hosted_count() == 0 {
                cleared = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(cleared, "failed install drops the Installing reservation");

        // …so the next round retries, and this one sticks.
        assert_eq!(prov.provision_round(), 1, "retry starts");
        wait_live(&prov, 1).await;

        agent.shutdown().await;
    }

    #[tokio::test]
    async fn withdraw_during_install_tears_down_the_stale_result() {
        // The token-check path: an install withdrawn while in flight must, on completion,
        // find its reservation gone and tear its fresh result down (advertisement dropped,
        // Installed::uninstall called) — never resurrect itself into the map.
        let agent = live_agent().await;
        let host = Arc::new(WasmHost::new().expect("engine"));

        let mut source = InMemorySource::new();
        let id = source.insert(b"withdrawn mid-flight".to_vec());
        let artifact = id;
        let mut catalog = InstallableCatalog::new();
        catalog.add(InstallableEntry::new(Capability::new("llm", "midflight"), id)
            .with_kind(ArtifactKind::Blob));

        let mut prov =
            Provisioner::new(Arc::clone(&agent), host, catalog, Arc::new(source), 1.0);
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let uninstalled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        prov.register_runtime(Arc::new(TrackingRuntime {
            gate:           Some(Arc::clone(&gate)),
            fail_remaining: Arc::new(AtomicU64::new(0)),
            uninstalled:    Arc::clone(&uninstalled),
        }));
        prov.supervise(CapFilter::new("llm", "midflight"), 1);

        assert_eq!(prov.provision_round(), 1);
        assert_eq!(prov.installing_count(), 1, "install is in flight, blocked on the gate");

        // Withdraw while installing (in the fleet this happens when another provider makes
        // the band's live count exceed max while ours is still pulling).
        assert!(prov.withdraw(&artifact), "reservation withdrawn mid-install");
        assert_eq!(prov.installing_count(), 0);

        // Let the install finish: the completion's token check must tear it down.
        gate.add_permits(1);
        let mut torn_down = false;
        for _ in 0..200 {
            if uninstalled.load(std::sync::atomic::Ordering::SeqCst) {
                torn_down = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(torn_down, "the stale install was explicitly uninstalled");
        assert_eq!(prov.hosted_count(), 0, "it never resurrected into the hosted map");
        assert_eq!(prov.installing_count(), 0);

        agent.shutdown().await;
    }

    /// M2 Run-38 falsification probe (Resource Management), kept: an agent shut down while an
    /// install is still in flight must stay harmless — the detached install task completes
    /// against the dead agent, its advertisement is a no-op write, nothing panics, and the
    /// provisioner's state stays coherent (the install may land Live in the map — the map is
    /// node-local bookkeeping; the node itself is gone).
    #[tokio::test]
    async fn agent_shutdown_mid_install_is_harmless() {
        let agent = live_agent().await;
        let host = Arc::new(WasmHost::new().expect("engine"));

        let mut source = InMemorySource::new();
        let id = source.insert(b"shutdown mid-flight".to_vec());
        let mut catalog = InstallableCatalog::new();
        catalog.add(InstallableEntry::new(Capability::new("llm", "shutdown"), id)
            .with_kind(ArtifactKind::Blob));

        let mut prov =
            Provisioner::new(Arc::clone(&agent), host, catalog, Arc::new(source), 1.0);
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let uninstalled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        prov.register_runtime(Arc::new(TrackingRuntime {
            gate:           Some(Arc::clone(&gate)),
            fail_remaining: Arc::new(AtomicU64::new(0)),
            uninstalled:    Arc::clone(&uninstalled),
        }));
        prov.supervise(CapFilter::new("llm", "shutdown"), 1);
        assert_eq!(prov.provision_round(), 1);
        assert_eq!(prov.installing_count(), 1);

        // Shut the node down under the in-flight install, then let the install finish.
        agent.shutdown().await;
        gate.add_permits(1);

        // The task completes without panicking the runtime; the reservation resolves.
        let mut settled = false;
        for _ in 0..200 {
            if prov.installing_count() == 0 {
                settled = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(settled, "the in-flight install resolves after shutdown (no zombie reservation)");
        // And the provisioner is still coherent to use (no poisoned lock, no panic).
        assert_eq!(prov.provision_round(), 0, "rounds on a shut agent are safe no-ops");
    }

    /// M2 Run-38 falsification probe (Philosophy / no-coordinator), kept: resource-aware
    /// self-election must leave NO fleet-visible scheduler state. After a full install cycle,
    /// the only KV the provisioner produced is the capability advertisement family for what it
    /// hosts (`cap/...`) — no assignment keys, no resource gossip, no queue: eligibility was
    /// decided and forgotten locally.
    #[tokio::test]
    async fn self_election_writes_no_scheduler_state_to_the_fleet() {
        let agent = live_agent().await;
        let host = Arc::new(WasmHost::new().expect("engine"));

        let mut source = InMemorySource::new();
        let id = source.insert(ECHO_COMPONENT.to_vec());
        let mut catalog = InstallableCatalog::new();
        catalog.add(InstallableEntry::new(Capability::new("text", "echo"), id)
            .with_requirements(0, 1_000)); // resource-checked election (real probe, default policy)

        // Snapshot every key before the provisioner exists.
        let before: std::collections::HashSet<String> =
            agent.kv().keys().into_iter().map(|k| k.to_string()).collect();

        let mut prov =
            Provisioner::new(Arc::clone(&agent), host, catalog, Arc::new(source), 1.0);
        prov.supervise(CapFilter::new("text", "echo"), 1);
        assert_eq!(prov.provision_round(), 1);
        wait_live(&prov, 1).await;
        for _ in 0..3 {
            prov.provision_round(); // extra rounds: probe pass + idempotence also write nothing
        }

        // The ad's KV write lands asynchronously — poll structurally for it before diffing.
        let mut after: Vec<String> = Vec::new();
        for _ in 0..80 {
            after = agent.kv().keys().into_iter()
                .map(|k| k.to_string())
                .filter(|k| !before.contains(k))
                .collect();
            if !after.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(!after.is_empty(), "the capability advertisement itself must appear");
        for key in &after {
            assert!(
                key.starts_with("cap/"),
                "self-election left non-advertisement fleet state: {key} — that would be a \
                 scheduler wearing a costume (design §4.4)"
            );
        }

        agent.shutdown().await;
    }

    /// Run-38 floor fix (Observability): the tripwires are no longer programmatic-only — they
    /// reach the `metrics` facade, so any embedder with a recorder (the node's `/metrics`
    /// exporter) sees resource-skip storms without extra plumbing.
    #[test]
    fn tripwire_counters_reach_the_metrics_facade() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            // An unstarted agent suffices: the presence pass reads only local (empty) state,
            // resolves the catalog, and skips on "no runtime for kind" — the tripwire path.
            let port =
                std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
            let id = mycelium::NodeId::new("127.0.0.1", port).unwrap();
            let agent = Arc::new(GossipAgent::new(
                id,
                mycelium::GossipConfig { bind_port: port, ..Default::default() },
            ));
            let host = Arc::new(WasmHost::new().expect("engine"));
            let mut source = InMemorySource::new();
            let blob = source.insert(b"unhostable model".to_vec());
            let mut catalog = InstallableCatalog::new();
            catalog.add(InstallableEntry::new(Capability::new("llm", "unhostable"), blob)
                .with_kind(ArtifactKind::Blob));
            let mut prov = Provisioner::new(agent, host, catalog, Arc::new(source), 1.0);
            prov.supervise(CapFilter::new("llm", "unhostable"), 1);
            assert_eq!(prov.provision_round(), 0);
            assert!(prov.ineligible_skips() >= 1);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let skip = snapshot.iter().find(|(key, _, _, _)| {
            key.key().name() == "mycelium_artifact_ineligible_skips_total"
                && key.key().labels().any(|l| l.key() == "reason" && l.value() == "no_runtime")
        });
        match skip {
            Some((_, _, _, DebugValue::Counter(n))) => {
                assert!(*n >= 1, "skip counter incremented through the facade")
            }
            other => panic!("expected the no_runtime skip counter in the snapshot, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unregistered_kind_and_over_budget_entries_are_skipped_and_counted() {
        // Eligibility is node-local truth: no runtime for a kind → never self-elect for it; over
        // the install budget → same. Both are silent non-participation plus a tripwire tick
        // (detection, not prevention) — never an error.
        let agent = live_agent().await;
        let host = Arc::new(WasmHost::new().expect("engine"));

        let mut source = InMemorySource::new();
        let blob_id = source.insert(b"model weights stand-in".to_vec());
        let wasm_id = source.insert(ECHO_COMPONENT.to_vec());

        // A blob-kind entry (no blob runtime registered here) + an over-budget wasm entry.
        let mut catalog = InstallableCatalog::new();
        catalog.add(
            InstallableEntry::new(Capability::new("llm", "weights"), blob_id)
                .with_kind(ArtifactKind::Blob),
        );
        catalog.add(
            InstallableEntry::new(Capability::new("text", "echo"), wasm_id).with_cost(10_000, 1),
        );

        let mut prov =
            Provisioner::new(Arc::clone(&agent), host, catalog, Arc::new(source), 1.0);
        prov.set_install_budget(1_000); // below the wasm entry's 10_000 hint
        prov.supervise(CapFilter::new("llm", "weights"), 1);
        prov.supervise(CapFilter::new("text", "echo"), 1);

        assert_eq!(prov.provision_round(), 0, "neither entry is eligible on this node");
        assert_eq!(prov.hosted_count(), 0);
        assert_eq!(prov.installing_count(), 0);
        assert_eq!(prov.ineligible_skips(), 2, "one tripwire tick per skipped entry");

        // Raising the budget makes the wasm entry eligible; the blob kind stays unhostable.
        prov.set_install_budget(1_000_000);
        assert_eq!(prov.provision_round(), 1, "wasm entry becomes eligible");
        wait_live(&prov, 1).await;
        assert_eq!(prov.ineligible_skips(), 3, "blob entry ticked again this round");

        agent.shutdown().await;
    }
}
