//! Pulling artifacts **over the cluster mesh** — closes §E.4.4 (content-addressed distribution)
//! on Mycelium's public API.
//!
//! A node that holds artifacts runs [`serve_artifacts`], which answers `artifact.fetch` RPCs with
//! the bytes for a requested [`ArtifactId`]. Any node can then [`pull_artifact`] from a peer; the
//! bytes are verified against the content address on arrival, so the *source is untrusted* (a peer
//! returning the wrong bytes is rejected, exactly like any other [`ArtifactSource`]).
//!
//! Transport is RPC (rides the gossip frame, ≤ `MAX_FRAME_BYTES` = 10 MiB) — fine for typical
//! components; artifacts beyond that want the bulk transport (`ServiceHandle::bulk_serve`).
//!
//! [`ArtifactSource`]: crate::ArtifactSource

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use mycelium::{GossipAgent, NodeId};

use crate::artifact::{verify_artifact, ArtifactId, ArtifactSource};

/// RPC kind used to request artifact bytes by content address.
pub const ARTIFACT_FETCH_KIND: &str = "artifact.fetch";

/// Serve artifacts held by `source` to the cluster: spawns an RPC handler that, for each inbound
/// `artifact.fetch` (payload = 32-byte [`ArtifactId`]), replies with the bytes (or empty if not
/// held). Returns the serve task handle (drop/abort to stop serving; it also ends on shutdown).
pub fn serve_artifacts(
    agent: Arc<GossipAgent>,
    source: Arc<dyn ArtifactSource + Send + Sync>,
) -> tokio::task::JoinHandle<()> {
    let mut rx = agent.service().rpc_rx(ARTIFACT_FETCH_KIND);
    tokio::spawn(async move {
        while let Some(req) = rx.recv().await {
            let payload = req.payload();
            let bytes = <[u8; 32]>::try_from(payload.as_ref())
                .ok()
                .map(ArtifactId::from_bytes)
                .and_then(|id| source.fetch(&id))
                .unwrap_or_default();
            agent.service().rpc_respond(&req, bytes);
        }
    })
}

/// Pull the bytes for `id` from `provider` over the mesh, verifying the content address on arrival.
/// `None` if the peer doesn't hold it, the call times out, or the returned bytes don't match `id`
/// (an untrusted peer cannot substitute bytes).
pub async fn pull_artifact(
    agent: &GossipAgent,
    provider: NodeId,
    id: &ArtifactId,
    timeout: Duration,
) -> Option<Bytes> {
    let reply = agent
        .service()
        .rpc_call(provider, ARTIFACT_FETCH_KIND, id.as_bytes().to_vec(), timeout)
        .await
        .ok()?;
    if reply.is_empty() || verify_artifact(&reply, id).is_err() {
        return None;
    }
    Some(reply)
}

/// An [`ArtifactSource`] backed by mesh peers. Because the trait's `fetch` is synchronous, bytes
/// must be [`prefetch`](Self::prefetch)ed (async, verified) into the cache before
/// `WasmHost::provision` reads them; `fetch` then serves from that cache. (Transparent on-demand
/// mesh pull would require an async `ArtifactSource` — a deliberate future refinement.)
pub struct MeshArtifactSource {
    agent:     Arc<GossipAgent>,
    providers: Vec<NodeId>,
    timeout:   Duration,
    cache:     Mutex<HashMap<ArtifactId, Bytes>>,
}

impl MeshArtifactSource {
    pub fn new(agent: Arc<GossipAgent>, providers: Vec<NodeId>, timeout: Duration) -> Self {
        Self { agent, providers, timeout, cache: Mutex::new(HashMap::new()) }
    }

    /// Pull `id` from the first provider that has it into the local cache (verified on arrival).
    /// Returns whether it is now cached. Idempotent — a cached id short-circuits.
    pub async fn prefetch(&self, id: &ArtifactId) -> bool {
        if self.cache.lock().unwrap().contains_key(id) {
            return true;
        }
        for provider in &self.providers {
            if let Some(bytes) = pull_artifact(&self.agent, provider.clone(), id, self.timeout).await {
                self.cache.lock().unwrap().insert(*id, bytes);
                return true;
            }
        }
        false
    }
}

impl ArtifactSource for MeshArtifactSource {
    fn fetch(&self, id: &ArtifactId) -> Option<Bytes> {
        self.cache.lock().unwrap().get(id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemorySource;

    fn alloc_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
    }

    async fn agent(port: u16, bootstrap: Option<u16>) -> Arc<GossipAgent> {
        let id = NodeId::new("127.0.0.1", port).unwrap();
        let cfg = mycelium::GossipConfig {
            bind_port: port,
            bootstrap_peers: bootstrap
                .map(|b| vec![NodeId::new("127.0.0.1", b).unwrap()])
                .unwrap_or_default(),
            ..Default::default()
        };
        let a = Arc::new(GossipAgent::new(id, cfg));
        a.start().await.expect("agent start");
        a
    }

    #[tokio::test]
    async fn pulls_and_verifies_an_artifact_from_a_peer() {
        // Server A holds the artifact and serves it; client B pulls it over the mesh.
        let a_port = alloc_port();
        let a = agent(a_port, None).await;
        let b = agent(alloc_port(), Some(a_port)).await;

        // wait until peered both ways
        for _ in 0..80 {
            if !a.peers().is_empty() && !b.peers().is_empty() { break; }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let mut src = InMemorySource::new();
        let id = src.insert(Bytes::from_static(b"artifact-over-the-mesh"));
        let _serve = serve_artifacts(Arc::clone(&a), Arc::new(src));

        // B pulls by content address, with retry for the RPC path to warm up.
        let mesh = MeshArtifactSource::new(Arc::clone(&b), vec![a.node_id().clone()], Duration::from_secs(2));
        let mut ok = false;
        for _ in 0..20 {
            if mesh.prefetch(&id).await { ok = true; break; }
        }
        assert!(ok, "B should pull the artifact from A over the mesh");
        assert_eq!(mesh.fetch(&id).as_deref(), Some(&b"artifact-over-the-mesh"[..]));

        // An id no peer holds → not cached.
        let unknown = ArtifactId::of(b"nobody has this");
        assert!(!mesh.prefetch(&unknown).await);
        assert!(mesh.fetch(&unknown).is_none());

        a.shutdown().await;
        b.shutdown().await;
    }
}
