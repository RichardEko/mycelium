/// Node TLS context — always compiles, only has content with the `tls` feature.
///
/// When `tls` is disabled this is a zero-size struct and every method is
/// unreachable; the struct is used only as a type in `Option<Arc<NodeTls>>`
/// so function signatures stay uniform regardless of the feature flag.
pub(crate) struct NodeTls {
    #[cfg(feature = "tls")]
    pub server_config: std::sync::Arc<rustls::ServerConfig>,
    #[cfg(feature = "tls")]
    pub client_config: std::sync::Arc<rustls::ClientConfig>,
    #[cfg(feature = "tls")]
    pub signing_key: std::sync::Arc<ed25519_dalek::SigningKey>,
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

    pub(crate) fn load_or_generate(
        cfg: &TlsConfig,
        node_id: &NodeId,
    ) -> Result<NodeTls, GossipError> {
        fs::create_dir_all(&cfg.auto_cert_dir).map_err(|e| {
            GossipError::Config(format!("TLS: cannot create cert dir {:?}: {e}", cfg.auto_cert_dir))
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
                .map_err(|e| GossipError::Config(format!("TLS: read CA cert: {e}")))?;
            ca_cert_der = pem_cert_to_der(&pem)?;
            let key_pem = fs::read_to_string(&auto_ca_key_path)
                .map_err(|e| GossipError::Config(format!("TLS: read CA key: {e}")))?;
            ca_key_pair = KeyPair::from_pem(&key_pem)
                .map_err(|e| GossipError::Config(format!("TLS: parse CA key: {e}")))?;
        } else {
            // Generate new CA
            ca_key_pair = KeyPair::generate_for(&PKCS_ED25519)
                .map_err(|e| GossipError::Config(format!("TLS: generate CA key: {e}")))?;
            let mut ca_params = CertificateParams::new(vec![])
                .map_err(|e| GossipError::Config(format!("TLS: CA params: {e}")))?;
            ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            ca_params.not_before = rcgen::date_time_ymd(2024, 1, 1);
            ca_params.not_after  = rcgen::date_time_ymd(2099, 1, 1);
            let ca_cert = ca_params
                .self_signed(&ca_key_pair)
                .map_err(|e| GossipError::Config(format!("TLS: self-sign CA: {e}")))?;

            ca_cert_der = CertificateDer::from(ca_cert.der().to_vec());

            // Save CA cert as PEM for peer distribution
            let pem_str = cert_der_to_pem(ca_cert_der.as_ref());
            fs::write(&auto_ca_cert_path, pem_str)
                .map_err(|e| GossipError::Config(format!("TLS: write CA cert: {e}")))?;
            let key_pem = ca_key_pair.serialize_pem();
            fs::write(&auto_ca_key_path, key_pem)
                .map_err(|e| GossipError::Config(format!("TLS: write CA key: {e}")))?;

            tracing::info!(
                "TLS: generated new cluster CA in {:?} — distribute ca-cert.pem to all nodes",
                cfg.auto_cert_dir
            );
        }

        // ── 3. Node cert (regenerated every startup, signed by CA) ───────
        let node_cert_der = generate_node_cert(node_id, &signing_key, &ca_cert_der, &ca_key_pair)?;

        // ── 4. Build rustls configs ───────────────────────────────────────
        let (server_config, client_config) =
            build_rustls_configs(node_cert_der, &signing_key, ca_cert_der)?;

        Ok(NodeTls {
            server_config: Arc::new(server_config),
            client_config: Arc::new(client_config),
            signing_key: Arc::new(signing_key),
        })
    }

    fn generate_key() -> Result<SigningKey, GossipError> {
        use rand_core::OsRng;
        Ok(SigningKey::generate(&mut OsRng))
    }

    fn save_key_raw(key: &SigningKey, path: &Path) -> Result<(), GossipError> {
        fs::write(path, key.as_bytes())
            .map_err(|e| GossipError::Config(format!("TLS: write key {:?}: {e}", path)))
    }

    fn load_key_raw(path: &Path) -> Result<SigningKey, GossipError> {
        let bytes = fs::read(path)
            .map_err(|e| GossipError::Config(format!("TLS: read key {:?}: {e}", path)))?;
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| GossipError::Config("TLS: key file must be exactly 32 bytes".into()))?;
        Ok(SigningKey::from_bytes(&arr))
    }

    fn load_key_from_pkcs8_pem(path: &Path) -> Result<SigningKey, GossipError> {
        use ed25519_dalek::pkcs8::DecodePrivateKey;
        let pem = fs::read_to_string(path)
            .map_err(|e| GossipError::Config(format!("TLS: read key PEM {:?}: {e}", path)))?;
        SigningKey::from_pkcs8_pem(&pem)
            .map_err(|e| GossipError::Config(format!("TLS: parse PKCS8 PEM: {e}")))
    }

    fn generate_node_cert(
        node_id: &NodeId,
        signing_key: &SigningKey,
        ca_cert_der: &CertificateDer<'static>,
        ca_key_pair: &KeyPair,
    ) -> Result<CertificateDer<'static>, GossipError> {
        use ed25519_dalek::pkcs8::EncodePrivateKey;

        // Convert ed25519-dalek key → rcgen KeyPair via PKCS8 DER
        let pkcs8 = signing_key
            .to_pkcs8_der()
            .map_err(|e| GossipError::Config(format!("TLS: encode node key: {e}")))?;
        let node_key_pair = KeyPair::try_from(pkcs8.as_bytes())
            .map_err(|e| GossipError::Config(format!("TLS: rcgen node key: {e}")))?;

        // Add the node's IP address as a Subject Alternative Name
        let ip: std::net::IpAddr = node_id.to_socket_addr().ip();
        let mut params = CertificateParams::new(vec![])
            .map_err(|e| GossipError::Config(format!("TLS: node cert params: {e}")))?;
        params.subject_alt_names = vec![SanType::IpAddress(ip)];
        params.not_before = rcgen::date_time_ymd(2024, 1, 1);
        params.not_after  = rcgen::date_time_ymd(2099, 1, 1);

        // Reconstruct the CA Certificate object from DER + key for signing
        let ca_params = CertificateParams::from_ca_cert_der(ca_cert_der)
            .map_err(|e| GossipError::Config(format!("TLS: parse CA cert DER: {e}")))?;
        let signing_ca = ca_params
            .self_signed(ca_key_pair)
            .map_err(|e| GossipError::Config(format!("TLS: reconstruct CA: {e}")))?;

        let node_cert = params
            .signed_by(&node_key_pair, &signing_ca, ca_key_pair)
            .map_err(|e| GossipError::Config(format!("TLS: sign node cert: {e}")))?;

        Ok(CertificateDer::from(node_cert.der().to_vec()))
    }

    fn build_rustls_configs(
        node_cert_der: CertificateDer<'static>,
        signing_key: &SigningKey,
        ca_cert_der: CertificateDer<'static>,
    ) -> Result<(ServerConfig, ClientConfig), GossipError> {
        use ed25519_dalek::pkcs8::EncodePrivateKey;

        // Convert signing key to rustls PrivateKeyDer
        let pkcs8 = signing_key
            .to_pkcs8_der()
            .map_err(|e| GossipError::Config(format!("TLS: encode key for rustls: {e}")))?;
        let key_der: PrivateKeyDer<'static> =
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(pkcs8.as_bytes().to_vec()));

        // Build root store from CA cert
        let mut root_store = RootCertStore::empty();
        root_store
            .add(ca_cert_der.clone())
            .map_err(|e| GossipError::Config(format!("TLS: add CA to root store: {e}")))?;
        let root_store = Arc::new(root_store);

        // Server config: require client cert verified against CA
        let verifier = WebPkiClientVerifier::builder(Arc::clone(&root_store))
            .build()
            .map_err(|e| GossipError::Config(format!("TLS: build client verifier: {e}")))?;

        let server_config = ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(vec![node_cert_der.clone()], key_der.clone_key())
            .map_err(|e| GossipError::Config(format!("TLS: build server config: {e}")))?;

        // Client config: present node cert, verify server against CA
        let client_config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_client_auth_cert(vec![node_cert_der], key_der)
            .map_err(|e| GossipError::Config(format!("TLS: build client config: {e}")))?;

        Ok((server_config, client_config))
    }

    fn pem_cert_to_der(pem: &str) -> Result<CertificateDer<'static>, GossipError> {
        let mut cursor = std::io::Cursor::new(pem.as_bytes());
        let certs: Vec<CertificateDer<'static>> =
            rustls_pemfile::certs(&mut cursor)
                .collect::<Result<_, _>>()
                .map_err(|e| GossipError::Config(format!("TLS: parse CA cert PEM: {e}")))?;
        certs
            .into_iter()
            .next()
            .ok_or_else(|| GossipError::Config("TLS: CA cert PEM contains no certificate".into()))
    }

    fn cert_der_to_pem(der: &[u8]) -> String {
        use base64::{engine::general_purpose::STANDARD, Engine};
        let b64 = STANDARD.encode(der);
        // wrap at 64 chars
        let wrapped: String = b64
            .as_bytes()
            .chunks(64)
            .map(|c| std::str::from_utf8(c).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        format!("-----BEGIN CERTIFICATE-----\n{wrapped}\n-----END CERTIFICATE-----\n")
    }

    // ── Public helpers ────────────────────────────────────────────────────

    pub(crate) fn sign_bytes(key: &SigningKey, msg: &[u8]) -> [u8; 64] {
        use ed25519_dalek::Signer;
        key.sign(msg).to_bytes()
    }

    pub(crate) fn verify_bytes(pub_key_bytes: &[u8; 32], msg: &[u8], sig: &[u8; 64]) -> bool {
        use ed25519_dalek::Verifier;
        let Ok(vk) = VerifyingKey::from_bytes(pub_key_bytes) else { return false };
        let sig = ed25519_dalek::Signature::from_bytes(sig);
        vk.verify_strict(msg, &sig).is_ok()
    }
}

#[cfg(feature = "tls")]
pub(crate) use imp::{load_or_generate, sign_bytes, verify_bytes};
