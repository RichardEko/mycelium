/// Node TLS context — always compiles, only has content with the `tls` feature.
///
/// When `tls` is disabled this is a zero-size struct and every method is
/// unreachable; the struct is used only as a type in `Option<Arc<NodeTls>>`
/// so function signatures stay uniform regardless of the feature flag.
///
/// The handle (`Arc<NodeTls>` in `TaskCtx::tls`) is still set once, but its
/// inner state is **swappable at runtime** (WS5 hot cert rotation): the active
/// signing key and rustls configs live behind lock-free [`arc_swap::ArcSwap`]
/// cells, so [`rotate`](NodeTls::rotate) can replace them atomically while
/// signing/handshake paths keep reading the current value via the accessor
/// methods. Read the key/configs through the methods, never a cached clone, so
/// a rotation is observed.
pub struct NodeTls {
    #[cfg(feature = "tls")]
    server_config: arc_swap::ArcSwap<rustls::ServerConfig>,
    #[cfg(feature = "tls")]
    client_config: arc_swap::ArcSwap<rustls::ClientConfig>,
    #[cfg(feature = "tls")]
    signing_key: arc_swap::ArcSwap<ed25519_dalek::SigningKey>,
}

#[cfg(feature = "tls")]
impl NodeTls {
    /// The current rustls server config (accept side). Cloned cheaply (Arc).
    pub fn server_config(&self) -> std::sync::Arc<rustls::ServerConfig> {
        self.server_config.load_full()
    }
    /// The current rustls client config (connect side).
    pub fn client_config(&self) -> std::sync::Arc<rustls::ClientConfig> {
        self.client_config.load_full()
    }
    /// The current Ed25519 signing/identity key.
    pub fn signing_key(&self) -> std::sync::Arc<ed25519_dalek::SigningKey> {
        self.signing_key.load_full()
    }
    /// The current 32-byte verifying key.
    pub fn verifying_key_bytes(&self) -> [u8; 32] {
        self.signing_key().verifying_key().to_bytes()
    }

    /// Atomically swap in previously-generated rotation material — the cutover
    /// step of a hot rotation (WS5). New gossip signatures and new TLS handshakes
    /// (`server_config()` / `client_config()` are read per connection) pick up the
    /// new key/cert immediately; existing connections keep their old (CA-trusted)
    /// session. Call only *after* the new verifying key has been published to
    /// peers, so they already accept it.
    pub fn activate(&self, m: RotationMaterial) {
        self.signing_key.store(m.signing_key);
        self.server_config.store(m.server_config);
        self.client_config.store(m.client_config);
    }
}

/// A freshly-generated identity key + CA-signed cert + rustls configs, not yet
/// activated. Produced by `generate_rotation`; consumed by [`NodeTls::activate`].
#[cfg(feature = "tls")]
pub struct RotationMaterial {
    pub verifying_key: [u8; 32],
    server_config: std::sync::Arc<rustls::ServerConfig>,
    client_config: std::sync::Arc<rustls::ClientConfig>,
    signing_key: std::sync::Arc<ed25519_dalek::SigningKey>,
}

#[cfg(feature = "tls")]
mod imp {
    use super::NodeTls;
    use crate::{config::TlsConfig, error::GossipError, node_id::NodeId};

    use ed25519_dalek::{SigningKey, VerifyingKey};
    use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, SanType, PKCS_ED25519};
    use rustls::{
        pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
        server::WebPkiClientVerifier,
        ClientConfig, RootCertStore, ServerConfig,
    };
    use std::{fs, path::Path, sync::Arc};

    pub fn load_or_generate(
        cfg: &TlsConfig,
        node_id: &NodeId,
    ) -> Result<NodeTls, GossipError> {
        fs::create_dir_all(&cfg.auto_cert_dir).map_err(|e| {
            GossipError::InvalidField { field: "tls", reason: format!("TLS: cannot create cert dir {:?}: {e}", cfg.auto_cert_dir) }
        })?;

        // ── 1. Node signing / identity key ────────────────────────────────
        let sanitized = node_id.as_str().replace([':', '.'], "_");
        let auto_key_path = cfg.auto_cert_dir.join(format!("{sanitized}.key"));
        let signing_key: SigningKey = match &cfg.key_pem {
            Some(p) => load_key_from_pkcs8_pem(p)?,
            None => {
                if auto_key_path.exists() {
                    load_key_raw(&auto_key_path)?
                } else {
                    let key = generate_key()?;
                    save_key_raw(&key, &auto_key_path)?;
                    key
                }
            }
        };

        // ── 2. CA cert + key ──────────────────────────────────────────────
        let auto_ca_cert_path = cfg.auto_cert_dir.join("ca-cert.pem");
        let auto_ca_key_path  = cfg.auto_cert_dir.join("ca-key.pem");

        let ca_cert_path = cfg.ca_cert_pem.clone().unwrap_or(auto_ca_cert_path.clone());
        let ca_cert_der: CertificateDer<'static>;
        let ca_key_pair: KeyPair;

        if ca_cert_path.exists() && auto_ca_key_path.exists() {
            // Load existing CA
            let pem = fs::read_to_string(&ca_cert_path)
                .map_err(|e| GossipError::InvalidField { field: "tls", reason: format!("TLS: read CA cert: {e}") })?;
            ca_cert_der = pem_cert_to_der(&pem)?;
            let key_pem = fs::read_to_string(&auto_ca_key_path)
                .map_err(|e| GossipError::InvalidField { field: "tls", reason: format!("TLS: read CA key: {e}") })?;
            ca_key_pair = KeyPair::from_pem(&key_pem)
                .map_err(|e| GossipError::InvalidField { field: "tls", reason: format!("TLS: parse CA key: {e}") })?;
        } else {
            // Generate new CA
            ca_key_pair = KeyPair::generate_for(&PKCS_ED25519)
                .map_err(|e| GossipError::InvalidField { field: "tls", reason: format!("TLS: generate CA key: {e}") })?;
            let mut ca_params = CertificateParams::new(vec![])
                .map_err(|e| GossipError::InvalidField { field: "tls", reason: format!("TLS: CA params: {e}") })?;
            ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            ca_params.not_before = rcgen::date_time_ymd(2024, 1, 1);
            ca_params.not_after  = rcgen::date_time_ymd(2099, 1, 1);
            let ca_cert = ca_params
                .self_signed(&ca_key_pair)
                .map_err(|e| GossipError::InvalidField { field: "tls", reason: format!("TLS: self-sign CA: {e}") })?;

            ca_cert_der = CertificateDer::from(ca_cert.der().to_vec());

            // Save CA cert as PEM for peer distribution
            let pem_str = cert_der_to_pem(ca_cert_der.as_ref());
            fs::write(&auto_ca_cert_path, pem_str)
                .map_err(|e| GossipError::InvalidField { field: "tls", reason: format!("TLS: write CA cert: {e}") })?;
            let key_pem = ca_key_pair.serialize_pem();
            fs::write(&auto_ca_key_path, key_pem)
                .map_err(|e| GossipError::InvalidField { field: "tls", reason: format!("TLS: write CA key: {e}") })?;

            tracing::info!(
                "TLS: generated new cluster CA in {:?} — distribute ca-cert.pem to all nodes",
                cfg.auto_cert_dir
            );
        }

        // ── 3. Node cert (regenerated every startup, signed by CA) ───────
        let node_cert_der = generate_node_cert(node_id, &signing_key, &ca_key_pair)?;

        // ── 4. Build rustls configs ───────────────────────────────────────
        let (server_config, client_config) =
            build_rustls_configs(node_cert_der, &signing_key, ca_cert_der)?;

        Ok(NodeTls {
            server_config: arc_swap::ArcSwap::from_pointee(server_config),
            client_config: arc_swap::ArcSwap::from_pointee(client_config),
            signing_key: arc_swap::ArcSwap::from_pointee(signing_key),
        })
    }

    /// Generate a fresh identity key + CA-signed node cert + rustls configs
    /// WITHOUT activating them, persisting the new key to disk so a restart uses
    /// it. Returns the material (and the new verifying key) so the caller can
    /// publish the new key to peers before the cutover ([`NodeTls::activate`]).
    /// Reuses the **existing** cluster CA — never regenerates it — and errors if
    /// no CA is present (rotation only makes sense post-bootstrap).
    pub fn generate_rotation(
        cfg: &TlsConfig,
        node_id: &NodeId,
    ) -> Result<super::RotationMaterial, GossipError> {
        let signing_key = generate_key()?;
        let verifying_key = signing_key.verifying_key().to_bytes();

        // Persist the new key (raw 32 bytes), same layout as load_or_generate.
        let sanitized = node_id.as_str().replace([':', '.'], "_");
        let auto_key_path = cfg.auto_cert_dir.join(format!("{sanitized}.key"));
        save_key_raw(&signing_key, &auto_key_path)?;

        let (ca_cert_der, ca_key_pair) = load_existing_ca(cfg)?;
        let node_cert_der = generate_node_cert(node_id, &signing_key, &ca_key_pair)?;
        let (server_config, client_config) =
            build_rustls_configs(node_cert_der, &signing_key, ca_cert_der)?;

        Ok(super::RotationMaterial {
            verifying_key,
            server_config: Arc::new(server_config),
            client_config: Arc::new(client_config),
            signing_key: Arc::new(signing_key),
        })
    }

    /// Load the existing cluster CA cert + key (load-only; errors if absent —
    /// unlike `load_or_generate`, rotation must never mint a new CA).
    fn load_existing_ca(cfg: &TlsConfig) -> Result<(CertificateDer<'static>, KeyPair), GossipError> {
        let auto_ca_cert_path = cfg.auto_cert_dir.join("ca-cert.pem");
        let auto_ca_key_path  = cfg.auto_cert_dir.join("ca-key.pem");
        let ca_cert_path = cfg.ca_cert_pem.clone().unwrap_or(auto_ca_cert_path);
        let pem = fs::read_to_string(&ca_cert_path).map_err(|e| GossipError::InvalidField {
            field: "tls", reason: format!("TLS: rotation needs an existing CA cert ({ca_cert_path:?}): {e}"),
        })?;
        let ca_cert_der = pem_cert_to_der(&pem)?;
        let key_pem = fs::read_to_string(&auto_ca_key_path).map_err(|e| GossipError::InvalidField {
            field: "tls", reason: format!("TLS: rotation needs the CA key ({auto_ca_key_path:?}): {e}"),
        })?;
        let ca_key_pair = KeyPair::from_pem(&key_pem).map_err(|e| GossipError::InvalidField {
            field: "tls", reason: format!("TLS: parse CA key: {e}"),
        })?;
        Ok((ca_cert_der, ca_key_pair))
    }

    fn generate_key() -> Result<SigningKey, GossipError> {
        use rand_core::OsRng;
        Ok(SigningKey::generate(&mut OsRng))
    }

    fn save_key_raw(key: &SigningKey, path: &Path) -> Result<(), GossipError> {
        fs::write(path, key.as_bytes())
            .map_err(|e| GossipError::InvalidField { field: "tls", reason: format!("TLS: write key {:?}: {e}", path) })
    }

    fn load_key_raw(path: &Path) -> Result<SigningKey, GossipError> {
        let bytes = fs::read(path)
            .map_err(|e| GossipError::InvalidField { field: "tls", reason: format!("TLS: read key {:?}: {e}", path) })?;
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| GossipError::InvalidField { field: "tls", reason: "TLS: key file must be exactly 32 bytes".into() })?;
        Ok(SigningKey::from_bytes(&arr))
    }

    fn load_key_from_pkcs8_pem(path: &Path) -> Result<SigningKey, GossipError> {
        use base64::{engine::general_purpose::STANDARD, Engine};
        use ed25519_dalek::pkcs8::DecodePrivateKey;
        let pem = fs::read_to_string(path)
            .map_err(|e| GossipError::InvalidField { field: "tls", reason: format!("TLS: read key PEM {:?}: {e}", path) })?;
        // Strip PEM armor and base64-decode to DER, then parse.
        // Avoids requiring the `pem` feature flag on the pkcs8 crate.
        let b64: String = pem.lines()
            .filter(|l| !l.starts_with("-----"))
            .collect();
        let der = STANDARD.decode(b64.trim())
            .map_err(|e| GossipError::InvalidField { field: "tls", reason: format!("TLS: decode key PEM: {e}") })?;
        SigningKey::from_pkcs8_der(&der)
            .map_err(|e| GossipError::InvalidField { field: "tls", reason: format!("TLS: parse PKCS8 key: {e}") })
    }

    fn generate_node_cert(
        node_id: &NodeId,
        signing_key: &SigningKey,
        ca_key_pair: &KeyPair,
    ) -> Result<CertificateDer<'static>, GossipError> {
        use ed25519_dalek::pkcs8::EncodePrivateKey;

        // Convert ed25519-dalek key → rcgen KeyPair via PKCS8 DER
        let pkcs8 = signing_key
            .to_pkcs8_der()
            .map_err(|e| GossipError::InvalidField { field: "tls", reason: format!("TLS: encode node key: {e}") })?;
        let node_key_pair = KeyPair::try_from(pkcs8.as_bytes())
            .map_err(|e| GossipError::InvalidField { field: "tls", reason: format!("TLS: rcgen node key: {e}") })?;

        // Add the node's IP address as a Subject Alternative Name
        let ip: std::net::IpAddr = node_id.to_socket_addr().ip();
        let mut params = CertificateParams::new(vec![])
            .map_err(|e| GossipError::InvalidField { field: "tls", reason: format!("TLS: node cert params: {e}") })?;
        params.subject_alt_names = vec![SanType::IpAddress(ip)];
        params.not_before = rcgen::date_time_ymd(2024, 1, 1);
        params.not_after  = rcgen::date_time_ymd(2099, 1, 1);

        // Reconstruct the CA Certificate for signing from the key pair + known fixed params.
        // rcgen 0.13 removed CertificateParams::from_ca_cert_der; since Mycelium always
        // generates its own CA with these exact params, reconstruction is deterministic.
        // Rustls verifies the chain via the SubjectKeyIdentifier (public-key hash), not the
        // serial number, so the reconstructed cert's AKI matches the saved ca-cert.pem.
        let mut ca_params = CertificateParams::new(vec![])
            .map_err(|e| GossipError::InvalidField { field: "tls", reason: format!("TLS: CA params for signing: {e}") })?;
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.not_before = rcgen::date_time_ymd(2024, 1, 1);
        ca_params.not_after  = rcgen::date_time_ymd(2099, 1, 1);
        let signing_ca = ca_params
            .self_signed(ca_key_pair)
            .map_err(|e| GossipError::InvalidField { field: "tls", reason: format!("TLS: reconstruct CA for signing: {e}") })?;

        let node_cert = params
            .signed_by(&node_key_pair, &signing_ca, ca_key_pair)
            .map_err(|e| GossipError::InvalidField { field: "tls", reason: format!("TLS: sign node cert: {e}") })?;

        Ok(CertificateDer::from(node_cert.der().to_vec()))
    }

    fn build_rustls_configs(
        node_cert_der: CertificateDer<'static>,
        signing_key: &SigningKey,
        ca_cert_der: CertificateDer<'static>,
    ) -> Result<(ServerConfig, ClientConfig), GossipError> {
        use ed25519_dalek::pkcs8::EncodePrivateKey;

        // rustls 0.23 resolves a *process-level* CryptoProvider when a
        // ServerConfig/ClientConfig builder runs, and panics if none is set.
        // We build with `default-features = false, features = ["ring"]`, so the
        // aws-lc-rs auto-install never happens — we must pin the ring provider
        // ourselves, exactly once per process. `install_default` returns Err if
        // a provider is already set (a second agent in the same process, or a
        // host that installed one first); that is the desired idempotent no-op.
        static INSTALL_CRYPTO_PROVIDER: std::sync::Once = std::sync::Once::new();
        INSTALL_CRYPTO_PROVIDER.call_once(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });

        // Convert signing key to rustls PrivateKeyDer
        let pkcs8 = signing_key
            .to_pkcs8_der()
            .map_err(|e| GossipError::InvalidField { field: "tls", reason: format!("TLS: encode key for rustls: {e}") })?;
        let key_der: PrivateKeyDer<'static> =
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(pkcs8.as_bytes().to_vec()));

        // Build root store from CA cert
        let mut root_store = RootCertStore::empty();
        root_store
            .add(ca_cert_der.clone())
            .map_err(|e| GossipError::InvalidField { field: "tls", reason: format!("TLS: add CA to root store: {e}") })?;
        let root_store = Arc::new(root_store);

        // Server config: require client cert verified against CA
        let verifier = WebPkiClientVerifier::builder(Arc::clone(&root_store))
            .build()
            .map_err(|e| GossipError::InvalidField { field: "tls", reason: format!("TLS: build client verifier: {e}") })?;

        let server_config = ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(vec![node_cert_der.clone()], key_der.clone_key())
            .map_err(|e| GossipError::InvalidField { field: "tls", reason: format!("TLS: build server config: {e}") })?;

        // Client config: present node cert, verify server against CA
        let client_config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_client_auth_cert(vec![node_cert_der], key_der)
            .map_err(|e| GossipError::InvalidField { field: "tls", reason: format!("TLS: build client config: {e}") })?;

        Ok((server_config, client_config))
    }

    fn pem_cert_to_der(pem: &str) -> Result<CertificateDer<'static>, GossipError> {
        let mut cursor = std::io::Cursor::new(pem.as_bytes());
        let certs: Vec<CertificateDer<'static>> =
            rustls_pemfile::certs(&mut cursor)
                .collect::<Result<_, _>>()
                .map_err(|e| GossipError::InvalidField { field: "tls", reason: format!("TLS: parse CA cert PEM: {e}") })?;
        certs
            .into_iter()
            .next()
            .ok_or_else(|| GossipError::InvalidField { field: "tls", reason: "TLS: CA cert PEM contains no certificate".into() })
    }

    fn cert_der_to_pem(der: &[u8]) -> String {
        use base64::{engine::general_purpose::STANDARD, Engine};
        let b64 = STANDARD.encode(der);
        // wrap at 64 chars
        let wrapped: String = b64
            .as_bytes()
            .chunks(64)
            .map(|c| std::str::from_utf8(c).expect("infallible: STANDARD base64 encoding produces ASCII-only bytes"))
            .collect::<Vec<_>>()
            .join("\n");
        format!("-----BEGIN CERTIFICATE-----\n{wrapped}\n-----END CERTIFICATE-----\n")
    }

    // ── Public helpers ────────────────────────────────────────────────────

    pub fn sign_bytes(key: &SigningKey, msg: &[u8]) -> [u8; 64] {
        use ed25519_dalek::Signer;
        key.sign(msg).to_bytes()
    }

    pub fn verify_bytes(pub_key_bytes: &[u8; 32], msg: &[u8], sig: &[u8]) -> bool {
        let Ok(vk) = VerifyingKey::from_bytes(pub_key_bytes) else { return false };
        let Ok(arr): Result<[u8; 64], _> = sig.try_into() else { return false };
        let sig = ed25519_dalek::Signature::from_bytes(&arr);
        vk.verify_strict(msg, &sig).is_ok()
    }
}

#[cfg(feature = "tls")]
pub use imp::{generate_rotation, load_or_generate, sign_bytes, verify_bytes};
