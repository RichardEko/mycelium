//! The **object-store source** — pulling artifacts from an HTTP(S) blob store
//! (`docs/design/artifact-library.md` §2.2, sequencing step 6).
//!
//! Two pieces, deliberately separated:
//!
//! - [`BlobFetcher`] — the *async* remote-fetch face and the vendor extension point: implement
//!   it against any backend (an AWS SDK client with SigV4, an OCI registry, a Warg client) and
//!   hand it to [`PrefetchingSource`]. The shipped implementation is [`HttpLibrarySource`]:
//!   plain `GET {base_url}/{artifact-hex}`, which covers S3-compatible stores (public,
//!   bucket-policy, or static-header auth), nginx/CDN blob directories, and artifact servers.
//!   Native SigV4 request signing is out of scope by choice — that's a vendor SDK's job, and
//!   the trait is where such an SDK plugs in.
//! - [`PrefetchingSource`] — bridges any `BlobFetcher` into the sync [`ArtifactSource`] face
//!   the host consumes: `prefetch` (async) pulls and **verifies against the content address**
//!   before caching; `fetch` serves the verified cache. The same two-step
//!   `MeshArtifactSource` proved; the remote stays untrusted either way.
//!
//! **Egress:** an object-store pull is an outbound reach the node chooses, so
//! [`HttpLibrarySource`] is gated by an [`EgressPolicy`] exactly like the LLM backends — a
//! denied host fails *before* any connection is attempted. Every pulling node carries its own
//! read credentials and its own policy: direct per-node pulls, no relay, no leader (L3).

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::Arc;

use bytes::Bytes;
use mycelium::EgressPolicy;

use crate::artifact::{verify_artifact, ArtifactId, ArtifactSource};

/// Async remote fetch by content address — the extension point for vendor object-store SDKs.
/// `Ok(None)` = the remote doesn't hold it (a miss, not a failure); `Err` = the attempt failed
/// (unreachable, denied, 5xx) and is worth logging.
#[async_trait::async_trait]
pub trait BlobFetcher: Send + Sync {
    async fn fetch_remote(&self, id: &ArtifactId) -> Result<Option<Bytes>, String>;
}

/// Bridges an async [`BlobFetcher`] into the sync [`ArtifactSource`] face: bytes must be
/// [`prefetch`](Self::prefetch)ed — pulled and verified against the content address — into the
/// cache before `WasmHost::provision` (or a serving librarian) reads them via `fetch`.
pub struct PrefetchingSource {
    fetcher: Arc<dyn BlobFetcher>,
    cache:   Mutex<HashMap<ArtifactId, Bytes>>,
}

impl PrefetchingSource {
    pub fn new(fetcher: Arc<dyn BlobFetcher>) -> Self {
        Self { fetcher, cache: Mutex::new(HashMap::new()) }
    }

    /// Pull `id` from the remote into the local cache, verified on arrival — a remote returning
    /// the wrong bytes is rejected like any other untrusted source. Returns whether the id is
    /// now cached. Idempotent — a cached id short-circuits.
    pub async fn prefetch(&self, id: &ArtifactId) -> bool {
        if self.cache.lock().unwrap().contains_key(id) {
            return true;
        }
        match self.fetcher.fetch_remote(id).await {
            Ok(Some(bytes)) if verify_artifact(&bytes, id).is_ok() => {
                self.cache.lock().unwrap().insert(*id, bytes);
                true
            }
            Ok(Some(_)) => {
                tracing::warn!(artifact = %id, "remote returned bytes that fail the content address — rejected");
                false
            }
            Ok(None) => false,
            Err(e) => {
                tracing::warn!(artifact = %id, %e, "remote fetch failed");
                false
            }
        }
    }

    /// Prefetch a set of ids (e.g. everything in a library manifest — the mirror step for a
    /// librarian fronting a remote store). Returns how many are now cached.
    pub async fn prefetch_all(&self, ids: &[ArtifactId]) -> usize {
        let mut ok = 0;
        for id in ids {
            if self.prefetch(id).await {
                ok += 1;
            }
        }
        ok
    }
}

impl ArtifactSource for PrefetchingSource {
    fn fetch(&self, id: &ArtifactId) -> Option<Bytes> {
        self.cache.lock().unwrap().get(id).cloned()
    }
}

/// [`BlobFetcher`] over a plain HTTP(S) blob store: `GET {base_url}/{artifact-hex}`. Optional
/// static headers carry credentials (`Authorization: Bearer …`, S3-compatible static auth);
/// an [`EgressPolicy`] gates every request **before** it is dispatched.
pub struct HttpLibrarySource {
    base_url: String,
    headers:  Vec<(String, String)>,
    egress:   EgressPolicy,
    client:   reqwest::Client,
}

impl HttpLibrarySource {
    /// A source reading `{base_url}/{artifact-hex}` with an allow-all egress policy (empty
    /// `allow_hosts` — [`EgressPolicy`]'s default). Production nodes should
    /// [`with_egress`](Self::with_egress) their configured policy.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            headers:  Vec::new(),
            egress:   EgressPolicy::default(),
            client:   reqwest::Client::new(),
        }
    }

    /// Attach a static request header (credentials: `("Authorization", "Bearer …")`).
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Gate pulls with the node's egress policy — the same WS3 posture as the LLM backends:
    /// an outbound pull is a reach this node chooses, and a denied host fails before any
    /// connection is attempted.
    pub fn with_egress(mut self, egress: EgressPolicy) -> Self {
        self.egress = egress;
        self
    }
}

#[async_trait::async_trait]
impl BlobFetcher for HttpLibrarySource {
    async fn fetch_remote(&self, id: &ArtifactId) -> Result<Option<Bytes>, String> {
        let url = format!("{}/{}", self.base_url.trim_end_matches('/'), id.to_hex());
        if !self.egress.permits_url(&url) {
            return Err(format!("egress policy denies {url}"));
        }
        let mut req = self.client.get(&url);
        for (name, value) in &self.headers {
            req = req.header(name, value);
        }
        let resp = req.send().await.map_err(|e| format!("GET {url}: {e}"))?;
        match resp.status() {
            s if s.is_success() => {
                let bytes = resp.bytes().await.map_err(|e| format!("read {url}: {e}"))?;
                Ok(Some(bytes))
            }
            reqwest::StatusCode::NOT_FOUND => Ok(None),
            s => Err(format!("GET {url}: {s}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A minimal blocking HTTP file server: serves `GET /{hex}` from a map, counts hits,
    /// records the Authorization header it saw. Enough protocol for reqwest; no deps.
    fn spawn_test_server(
        blobs: HashMap<String, Vec<u8>>,
    ) -> (String, Arc<AtomicUsize>, Arc<Mutex<Option<String>>>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let auth_seen = Arc::new(Mutex::new(None));
        let (h, a) = (Arc::clone(&hits), Arc::clone(&auth_seen));
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                h.fetch_add(1, Ordering::SeqCst);
                let mut buf = [0u8; 4096];
                let n = stream.read(&mut buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                let path = req.lines().next().and_then(|l| l.split(' ').nth(1)).unwrap_or("/");
                if let Some(line) = req.lines().find(|l| l.to_ascii_lowercase().starts_with("authorization:")) {
                    *a.lock().unwrap() =
                        Some(line.split_once(' ').map(|x| x.1).unwrap_or("").trim().to_string());
                }
                let key = path.trim_start_matches('/');
                let response = match blobs.get(key) {
                    Some(body) => {
                        let mut r = format!(
                            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                            body.len()
                        )
                        .into_bytes();
                        r.extend_from_slice(body);
                        r
                    }
                    None => b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_vec(),
                };
                let _ = stream.write_all(&response);
            }
        });
        (format!("http://{addr}"), hits, auth_seen)
    }

    #[tokio::test]
    async fn http_source_pulls_verifies_and_serves_via_the_prefetch_cache() {
        let good = b"the artifact the catalogue promised".to_vec();
        let good_id = ArtifactId::of(&good);
        let lying = b"advertised under someone else's hash".to_vec();
        let lied_about = ArtifactId::of(b"what the catalogue actually named");

        let mut blobs = HashMap::new();
        blobs.insert(good_id.to_hex(), good.clone());
        blobs.insert(lied_about.to_hex(), lying); // wrong bytes at that key
        let (base, _hits, auth) = spawn_test_server(blobs);

        let source = PrefetchingSource::new(Arc::new(
            HttpLibrarySource::new(&base).with_header("Authorization", "Bearer test-token"),
        ));

        // Happy path: pulled over HTTP, verified, then served through the sync face.
        assert!(source.prefetch(&good_id).await, "pull + verify from the HTTP store");
        assert_eq!(source.fetch(&good_id).as_deref(), Some(&good[..]));
        assert_eq!(auth.lock().unwrap().as_deref(), Some("Bearer test-token"),
            "credentials header reaches the store");

        // A lying store is rejected (content address is the trust anchor)…
        assert!(!source.prefetch(&lied_about).await, "wrong bytes are rejected, not cached");
        assert!(source.fetch(&lied_about).is_none());

        // …and a miss is a miss.
        assert!(!source.prefetch(&ArtifactId::of(b"nobody has this")).await);

        // prefetch_all mirrors a manifest's worth in one call (cached ids short-circuit).
        assert_eq!(source.prefetch_all(&[good_id, lied_about]).await, 1);
    }

    #[tokio::test]
    async fn egress_policy_denies_before_any_connection() {
        let (base, hits, _auth) = spawn_test_server(HashMap::new());

        // 127.0.0.1 is not in the allowlist → denied *before* dispatch.
        let gated = HttpLibrarySource::new(&base)
            .with_egress(EgressPolicy { allow_hosts: vec!["library.allowed.example".into()] });
        let id = ArtifactId::of(b"x");
        let err = gated.fetch_remote(&id).await.expect_err("denied host errors");
        assert!(err.contains("egress policy denies"), "got: {err}");
        assert_eq!(hits.load(Ordering::SeqCst), 0, "no connection was attempted");

        // The prefetching wrapper surfaces it as a non-cache (logged), not a panic.
        let source = PrefetchingSource::new(Arc::new(gated));
        assert!(!source.prefetch(&id).await);
        assert_eq!(hits.load(Ordering::SeqCst), 0);

        // An allow-all policy (the default) reaches the store.
        let open = HttpLibrarySource::new(&base);
        assert_eq!(open.fetch_remote(&id).await.unwrap(), None, "404 is a miss");
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }
}
