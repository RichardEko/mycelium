//! The **provisioner** — the app-layer loop that *closes* the autonomic loop (M15 item 4):
//! watch demand → resolve unmet requirements against the installable catalog → pull + verify +
//! instantiate → advertise, so demand is relieved.
//!
//! **Core Principle 1 (no coordinator).** This is a regular agent built on Mycelium's public API,
//! **not** a substrate mechanism. The library never auto-provisions; the agency to pull-and-run is
//! the node's own local choice. No coordinator assigns provisioning duty — every node runs its own
//! provisioner, each independently observes demand and **self-elects** (probabilistically, to damp
//! the thundering herd; any over-provisioning self-corrects when a future governor sheds providers
//! over `max`). This generalises `demand.rs`'s "the library never auto-advertises" stance.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use mycelium::{CapFilter, Capability, CapabilityReg, GossipAgent, RpcRequestRx};

use crate::artifact::{ArtifactId, ArtifactSource};
use crate::catalog::InstallableCatalog;
use crate::host::{HostState, Instance, WasmHost};

/// How often a provisioned capability re-asserts its `cap/` advertisement.
const ADVERTISE_INTERVAL: Duration = Duration::from_secs(5);

/// The RPC `kind` an inbound caller uses to invoke the hosted capability `ns/name`. A caller
/// resolves the capability to a provider node, then `rpc_call(provider, cap_invoke_kind(ns, name),
/// payload, timeout)`; the provisioner's serve loop routes it to the component's `handle` export.
pub fn cap_invoke_kind(namespace: &str, name: &str) -> String {
    format!("cap.invoke/{namespace}/{name}")
}

/// A capability this node has provisioned and is now hosting: the advertisement registration
/// (dropping it tombstones the `cap/` entry) plus the serve task that owns the live component
/// instance and answers inbound invocations.
struct Hosted {
    _cap:   CapabilityReg,
    _serve: tokio::task::JoinHandle<()>,
}

/// Serve loop for one hosted capability: owns the component [`Instance`] (wasmtime stores are
/// single-threaded, so one task per instance serialises calls) and answers each inbound RPC by
/// invoking the component's `handle` export and replying with its output.
async fn serve_loop(agent: Arc<GossipAgent>, mut instance: Instance, mut rx: RpcRequestRx) {
    while let Some(req) = rx.recv().await {
        let payload = req.payload().to_vec();
        // NB: wasm execution is synchronous and blocks this task for its duration — fine for
        // short handlers; long-running components want fuel/epoch limits + spawn_blocking (follow-up).
        let result: Vec<u8> = match instance.invoke("invoke", payload) {
            Ok(Ok(out)) => out,
            Ok(Err(e)) => format!("component-error: {e}").into_bytes(),
            Err(e) => format!("host-error: {e}").into_bytes(),
        };
        agent.service().rpc_respond(&req, result);
    }
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
}

/// An app-layer provisioner / supervisor. Construct it with the node, a [`WasmHost`], an
/// [`InstallableCatalog`], and an [`ArtifactSource`]; call [`provision_round`](Self::provision_round)
/// on a tick to keep the node provisioning what (a) unmet **demand** and (b) **presence
/// invariants** ([`supervise`](Self::supervise)) call for — both are just "desired state" the node
/// reconciles locally, the same resolve-and-pull path.
pub struct Provisioner {
    agent:        Arc<GossipAgent>,
    host:         Arc<WasmHost>,
    catalog:      InstallableCatalog,
    source:       Arc<dyn ArtifactSource + Send + Sync>,
    /// Probability of self-electing to satisfy an unmet requirement on a given round (herd
    /// damping). `1.0` = always (fine for a single provisioner); lower it when many nodes run one.
    self_elect_p: f64,
    /// Capability-presence invariants this node supervises (M14).
    policies:     Vec<SupervisionPolicy>,
    /// Artifacts this node has already provisioned (kept alive), so a round never double-pulls.
    hosted:       HashMap<ArtifactId, Hosted>,
}

impl Provisioner {
    /// Build a provisioner that self-elects with probability `self_elect_p` (use `1.0` for a
    /// single provisioner; lower for a fleet of them).
    pub fn new(
        agent: Arc<GossipAgent>,
        host: Arc<WasmHost>,
        catalog: InstallableCatalog,
        source: Arc<dyn ArtifactSource + Send + Sync>,
        self_elect_p: f64,
    ) -> Self {
        Self {
            agent,
            host,
            catalog,
            source,
            self_elect_p,
            policies: Vec::new(),
            hosted: HashMap::new(),
        }
    }

    /// Add a capability-presence invariant (M14 supervision): keep ≥ `min_providers` live providers
    /// of `filter` alive, re-provisioning from the catalog when the count drops (a dead provider's
    /// `cap/` evaporates → count falls → re-satisfied). Drives bring-up *without* organic demand.
    pub fn supervise(&mut self, filter: CapFilter, min_providers: usize) {
        self.policies.push(SupervisionPolicy { filter, min_providers });
    }

    /// Number of capabilities this node is currently hosting via provisioning.
    pub fn hosted_count(&self) -> usize {
        self.hosted.len()
    }

    /// Bring one capability live on this node: pull + verify + instantiate `artifact`, register the
    /// serve handler, advertise `provides`, and spawn the serve task. Returns `true` if newly
    /// hosted, `false` if already hosted or provisioning failed. Shared by the demand and presence
    /// paths — the one resolve-and-pull path the architecture promises.
    fn bring_live(&mut self, provides: Capability, artifact: ArtifactId) -> bool {
        if self.hosted.contains_key(&artifact) {
            return false;
        }
        let state = HostState::new(
            self.agent.node_id().clone(),
            provides.namespace.clone(),
            self.agent.kv(),
            self.agent.mesh(),
        );
        match self.host.provision(&*self.source, &artifact, state) {
            Ok(instance) => {
                // Register the inbound serve handler *before* advertising, so a caller that
                // resolves the fresh `cap/` entry always finds a live RPC receiver.
                let kind = cap_invoke_kind(&provides.namespace, &provides.name);
                let rx = self.agent.service().rpc_rx(kind);
                let cap = self
                    .agent
                    .capabilities()
                    .advertise_capability(provides.clone(), ADVERTISE_INTERVAL);
                let serve = tokio::spawn(serve_loop(Arc::clone(&self.agent), instance, rx));
                self.hosted.insert(artifact, Hosted { _cap: cap, _serve: serve });
                tracing::info!(ns = %provides.namespace, name = %provides.name, "provisioned + serving capability");
                true
            }
            Err(e) => {
                tracing::warn!(ns = %provides.namespace, name = %provides.name, %e, "provisioning failed");
                false
            }
        }
    }

    /// True if this node should self-elect to act this round (herd damping).
    fn self_elects(&self) -> bool {
        fastrand::f64() < self.self_elect_p
    }

    /// One convergence pass over **both** desired-state sources, returning how many capabilities
    /// were newly provisioned. Idempotent — already-hosted/already-satisfied are skipped.
    ///
    /// 1. **Demand-driven** (M15): a catalog entry whose declared-provide has demand but no live
    ///    provider → bring it live (relieves demand).
    /// 2. **Presence-driven** (M14 supervision): a policy whose live provider count is below
    ///    `min_providers` → resolve the catalog and bring a provider live. Self-healing falls out:
    ///    a dead provider's `cap/` evaporates, the count drops, this fires again — restart and
    ///    first-time provisioning are the same path.
    pub fn provision_round(&mut self) -> usize {
        let mut provisioned = 0;

        // ── Demand-driven (M15) ──────────────────────────────────────────────
        let entries: Vec<(Capability, ArtifactId)> = self
            .catalog
            .entries()
            .iter()
            .map(|e| (e.provides.clone(), e.artifact))
            .collect();
        for (provides, artifact) in entries {
            if self.hosted.contains_key(&artifact) {
                continue;
            }
            let filter = CapFilter::new(provides.namespace.clone(), provides.name.clone());
            let demand = self.agent.capabilities().demand(&filter);
            let unmet = demand.providers.is_empty() && !demand.demanding_nodes.is_empty();
            if unmet && self.self_elects() && self.bring_live(provides, artifact) {
                provisioned += 1;
            }
        }

        // ── Presence-driven (M14 supervision) ────────────────────────────────
        let policies = self.policies.clone();
        for policy in policies {
            // Live provider count is freshness-aware: a crashed provider's cap/ entry ages out,
            // so `providers` reflects only currently-live providers (this is the self-heal trigger).
            let live = self.agent.capabilities().demand(&policy.filter).providers.len();
            if live >= policy.min_providers {
                continue; // invariant already satisfied across the fleet
            }
            // Resolve the catalog for an artifact that would satisfy the invariant.
            let Some((provides, artifact)) = self
                .catalog
                .resolve_best(&policy.filter)
                .map(|e| (e.provides.clone(), e.artifact))
            else {
                continue; // nothing in the catalog provides it
            };
            if self.hosted.contains_key(&artifact) {
                continue; // this node already contributes a provider
            }
            if self.self_elects() && self.bring_live(provides, artifact) {
                provisioned += 1;
            }
        }

        provisioned
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::InstallableEntry;
    use crate::InMemorySource;

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

        // The provisioner satisfies it: pulls + instantiates + advertises.
        assert_eq!(prov.provision_round(), 1, "unmet requirement should be provisioned");
        assert_eq!(prov.hosted_count(), 1);

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
                cap_invoke_kind("text", "echo"),
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
        assert_eq!(prov.hosted_count(), 1);

        // It actually serves invocations.
        let reply = agent
            .service()
            .rpc_call(
                agent.node_id().clone(),
                cap_invoke_kind("text", "echo"),
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
}
