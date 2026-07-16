//! WS4 — generic OIDC bearer-token validation for the gateway.
//!
//! **Human-operator authentication, not agent identity** (orthogonal to the
//! NANDA/M16 agent-identity track — no forward-design is owed here). An operator
//! presents an OIDC JWT (from Entra / Okta / Auth0 / Keycloak — all
//! OIDC-conformant) as the gateway bearer; this module validates it and maps the
//! token's IdP groups to gateway scopes, so an OIDC principal is authorized
//! exactly like a [`GatewayToken`](crate::GatewayToken) — just authenticated by
//! signature instead of a shared secret.
//!
//! **Security posture.** We validate against an **explicit allowlist of
//! asymmetric algorithms** and never trust the token header's `alg` to select
//! the verification family — this closes the classic JWT alg-confusion bypass
//! (an attacker re-signing with `HS256` using the public key as the MAC secret).
//! Issuer, audience, and expiry are all checked. A token whose `kid` is not in
//! the configured key set is rejected.
//!
//! Gated behind the `compliance` feature. Vendor differences are configuration,
//! not code: discovery is the standard `.well-known/openid-configuration` + JWKS
//! (runtime fetch/cache lives in the gateway wiring).

use serde::Deserialize;

use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};

/// `OidcConfig` (the plain config struct) now lives in core `config` so
/// `GossipConfig` does not name an upper type; this module owns the verifier logic.
pub use crate::config::OidcConfig;

/// The asymmetric signature algorithms we accept. Symmetric (`HS*`) and `none`
/// are deliberately excluded — accepting them is the JWT alg-confusion bypass.
const ALLOWED_ALGS: &[Algorithm] = &[
    Algorithm::RS256, Algorithm::RS384, Algorithm::RS512,
    Algorithm::ES256, Algorithm::ES384,
    Algorithm::PS256, Algorithm::PS384, Algorithm::PS512,
];

/// Why an OIDC token was rejected. Coarse on purpose — the gateway answers a flat
/// 401, and finer detail goes only to logs (never leak validation specifics to
/// an unauthenticated caller).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OidcError {
    /// Header unparseable, missing `kid`, or a disallowed algorithm.
    Malformed,
    /// No configured key matches the token's `kid`.
    UnknownKid,
    /// Signature, issuer, audience, or expiry check failed.
    Invalid,
}

/// A validated OIDC principal: its subject and the gateway scopes its groups grant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedOidcPrincipal {
    pub subject: String,
    pub scopes:  Vec<String>,
}

#[derive(Deserialize)]
struct RawClaims {
    #[serde(default)]
    sub: String,
    #[serde(flatten)]
    extra: serde_json::Map<String, serde_json::Value>,
}

/// Validate `token` against the supplied `(kid, key)` set and `cfg`.
///
/// Steps, in order: parse the header; reject any algorithm outside
/// [`ALLOWED_ALGS`]; select the key by `kid`; verify signature + `iss` + `aud` +
/// `exp` with a `Validation` pinned to the allowed algorithms (never the header's
/// claimed alg); then extract `sub` and the configured group claim and map groups
/// to scopes. Returns [`VerifiedOidcPrincipal`] only if every check passes.
pub(crate) fn validate_token(
    cfg: &OidcConfig,
    keys: &[(String, DecodingKey)],
    token: &str,
) -> Result<VerifiedOidcPrincipal, OidcError> {
    let header = decode_header(token).map_err(|_| OidcError::Malformed)?;
    if !ALLOWED_ALGS.contains(&header.alg) {
        return Err(OidcError::Malformed); // HS*/none → alg-confusion attempt
    }
    let kid = header.kid.ok_or(OidcError::Malformed)?;
    let key = keys
        .iter()
        .find(|(k, _)| *k == kid)
        .map(|(_, k)| k)
        .ok_or(OidcError::UnknownKid)?;

    // The allowlist is enforced above (HS*/none → Malformed before we get here),
    // so `header.alg` is now a vetted asymmetric algorithm. Pin verification to
    // exactly that algorithm — a single family that matches the key — rather than
    // a mixed-family list (which jsonwebtoken rejects as InvalidAlgorithm).
    let mut validation = Validation::new(header.alg);
    validation.set_issuer(&[cfg.issuer.as_str()]);
    validation.set_audience(&[cfg.audience.as_str()]);
    validation.validate_exp = true;
    // `set_issuer`/`set_audience` only validate the claim WHEN PRESENT; jsonwebtoken requires only
    // `exp` by default, so a token that simply OMITS `aud`/`iss` sails past both checks — an
    // audience/issuer-confusion bypass in shared-IdP deployments (audit 2026-07-15 pass 3). Require
    // all three to be present so a missing claim is rejected, not silently skipped.
    validation.set_required_spec_claims(&["exp", "iss", "aud"]);

    let data = decode::<RawClaims>(token, key, &validation).map_err(|_| OidcError::Invalid)?;
    let claims = data.claims;

    let groups: Vec<String> = claims
        .extra
        .get(&cfg.group_claim)
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|g| g.as_str().map(str::to_string)).collect())
        .unwrap_or_default();

    Ok(VerifiedOidcPrincipal {
        subject: claims.sub,
        scopes:  cfg.scopes_for_groups(&groups),
    })
}

// ── Runtime verifier: JWKS fetch + cache (gateway wiring) ─────────────────────

use std::time::{Duration, Instant};

/// Re-fetch the IdP's JWKS at most this often (also re-fetched on an unknown
/// `kid`, so key rotation is picked up without waiting out the TTL).
const JWKS_TTL: Duration = Duration::from_secs(3600);

struct CachedKeys {
    at:   Instant,
    keys: Vec<(String, DecodingKey)>,
}

/// Holds the OIDC config + a cached JWKS, and validates tokens against it. One
/// per gateway; `verify` is cheap on the hot path (a read-lock + cached keys),
/// fetching only on cold cache, TTL expiry, or an unknown `kid`.
pub(crate) struct OidcVerifier {
    cfg:   OidcConfig,
    http:  reqwest::Client,
    cache: tokio::sync::RwLock<Option<CachedKeys>>,
}

impl OidcVerifier {
    pub(crate) fn new(cfg: OidcConfig) -> Self {
        Self { cfg, http: reqwest::Client::new(), cache: tokio::sync::RwLock::new(None) }
    }

    /// Validate `token`; `Some` only if signature, issuer, audience, and expiry
    /// all check out against the (possibly just-refreshed) JWKS.
    pub(crate) async fn verify(&self, token: &str) -> Option<VerifiedOidcPrincipal> {
        let header = decode_header(token).ok()?;
        if !ALLOWED_ALGS.contains(&header.alg) {
            return None;
        }
        let kid = header.kid.clone()?;

        let mut keys = self.cached_keys(false).await;
        if !keys.iter().any(|(k, _)| *k == kid) {
            // Unknown kid — the IdP may have rotated; force one refresh.
            keys = self.cached_keys(true).await;
        }
        validate_token(&self.cfg, &keys, token).ok()
    }

    /// Return cached keys, fetching when cold, stale (TTL), or `force`.
    async fn cached_keys(&self, force: bool) -> Vec<(String, DecodingKey)> {
        if !force {
            let guard = self.cache.read().await;
            if let Some(c) = guard.as_ref()
                && c.at.elapsed() < JWKS_TTL
            {
                return c.keys.clone();
            }
        }
        let fetched = self.fetch_keys().await;
        let mut guard = self.cache.write().await;
        // Keep a previous good set if the refresh failed (avoid flapping to empty).
        if fetched.is_empty()
            && let Some(c) = guard.as_ref()
        {
            return c.keys.clone();
        }
        *guard = Some(CachedKeys { at: Instant::now(), keys: fetched.clone() });
        fetched
    }

    /// Resolve the JWKS URI (explicit, or via `.well-known/openid-configuration`),
    /// fetch it, and build `(kid, DecodingKey)` pairs. Returns empty on any failure
    /// (logged) — `verify` then simply finds no matching key and rejects.
    async fn fetch_keys(&self) -> Vec<(String, DecodingKey)> {
        let jwks_uri = match self.resolve_jwks_uri().await {
            Some(u) => u,
            None => return Vec::new(),
        };
        let jwks: jsonwebtoken::jwk::JwkSet = match self.http.get(&jwks_uri).send().await {
            Ok(r) => match r.json().await {
                Ok(j) => j,
                Err(e) => { tracing::warn!("oidc: JWKS parse failed: {e}"); return Vec::new(); }
            },
            Err(e) => { tracing::warn!("oidc: JWKS fetch failed: {e}"); return Vec::new(); }
        };
        jwks.keys
            .iter()
            .filter_map(|jwk| {
                let kid = jwk.common.key_id.clone()?;
                DecodingKey::from_jwk(jwk).ok().map(|k| (kid, k))
            })
            .collect()
    }

    async fn resolve_jwks_uri(&self) -> Option<String> {
        if let Some(u) = &self.cfg.jwks_uri {
            return Some(u.clone());
        }
        // OIDC discovery.
        let disco = format!("{}/.well-known/openid-configuration", self.cfg.issuer.trim_end_matches('/'));
        let doc: serde_json::Value = self.http.get(&disco).send().await.ok()?.json().await.ok()?;
        doc.get("jwks_uri")?.as_str().map(str::to_string)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    // 2048-bit RSA test keypair (test-only; never used in production).
    const TEST_PRIV: &str = include_str!("../../tests/fixtures/oidc_test.key");
    const TEST_PUB:  &str = include_str!("../../tests/fixtures/oidc_test.pub");
    const OTHER_PUB: &str = include_str!("../../tests/fixtures/oidc_other.pub");

    fn now() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
    }

    fn cfg() -> OidcConfig {
        let mut group_scopes = HashMap::new();
        group_scopes.insert("admins".to_string(), vec!["*".to_string()]);
        group_scopes.insert("readers".to_string(), vec!["kv:read".to_string()]);
        OidcConfig {
            issuer: "https://idp.example".into(),
            audience: "mycelium-cluster".into(),
            group_claim: "groups".into(),
            group_scopes,
            jwks_uri: None,
        }
    }

    fn keys() -> Vec<(String, DecodingKey)> {
        vec![("test-kid".to_string(), DecodingKey::from_rsa_pem(TEST_PUB.as_bytes()).unwrap())]
    }

    /// Mint a signed JWT with the test key under `kid` "test-kid" / RS256.
    fn mint(claims: serde_json::Value, alg: Algorithm, kid: &str, priv_pem: &str) -> String {
        let mut header = Header::new(alg);
        header.kid = Some(kid.to_string());
        let key = EncodingKey::from_rsa_pem(priv_pem.as_bytes()).unwrap();
        encode(&header, &claims, &key).unwrap()
    }

    fn valid_claims() -> serde_json::Value {
        json!({
            "sub": "alice@example",
            "iss": "https://idp.example",
            "aud": "mycelium-cluster",
            "exp": now() + 3600,
            "groups": ["admins", "readers"],
        })
    }

    #[test]
    fn valid_token_maps_groups_to_scopes() {
        let p = validate_token(&cfg(), &keys(), &mint(valid_claims(), Algorithm::RS256, "test-kid", TEST_PRIV)).unwrap();
        assert_eq!(p.subject, "alice@example");
        assert!(p.scopes.contains(&"*".to_string()));
        assert!(p.scopes.contains(&"kv:read".to_string()));
    }

    #[test]
    fn expired_token_is_rejected() {
        let mut c = valid_claims();
        c["exp"] = json!(now() - 7200); // well beyond jsonwebtoken's default 60s leeway
        assert_eq!(validate_token(&cfg(), &keys(), &mint(c, Algorithm::RS256, "test-kid", TEST_PRIV)), Err(OidcError::Invalid));
    }

    #[test]
    fn wrong_issuer_is_rejected() {
        let mut c = valid_claims();
        c["iss"] = json!("https://evil.example");
        assert_eq!(validate_token(&cfg(), &keys(), &mint(c, Algorithm::RS256, "test-kid", TEST_PRIV)), Err(OidcError::Invalid));
    }

    #[test]
    fn wrong_audience_is_rejected() {
        let mut c = valid_claims();
        c["aud"] = json!("some-other-app");
        assert_eq!(validate_token(&cfg(), &keys(), &mint(c, Algorithm::RS256, "test-kid", TEST_PRIV)), Err(OidcError::Invalid));
    }

    #[test]
    fn regression_missing_audience_is_rejected() {
        // audit 2026-07-15 pass 3: `set_audience` only validates `aud` WHEN PRESENT, and jsonwebtoken
        // requires only `exp` by default — so a token that OMITS `aud` bypassed the audience check
        // (audience-confusion in shared-IdP deployments). `set_required_spec_claims` now rejects it.
        let mut c = valid_claims();
        c.as_object_mut().unwrap().remove("aud");
        assert_eq!(validate_token(&cfg(), &keys(), &mint(c, Algorithm::RS256, "test-kid", TEST_PRIV)), Err(OidcError::Invalid));
    }

    #[test]
    fn regression_missing_issuer_is_rejected() {
        // Same bypass on the issuer claim — a token omitting `iss` must be rejected.
        let mut c = valid_claims();
        c.as_object_mut().unwrap().remove("iss");
        assert_eq!(validate_token(&cfg(), &keys(), &mint(c, Algorithm::RS256, "test-kid", TEST_PRIV)), Err(OidcError::Invalid));
    }

    #[test]
    fn wrong_signing_key_is_rejected() {
        // Signed with TEST_PRIV but the verifier only knows a different public key.
        let keys = vec![("test-kid".to_string(), DecodingKey::from_rsa_pem(OTHER_PUB.as_bytes()).unwrap())];
        assert_eq!(validate_token(&cfg(), &keys, &mint(valid_claims(), Algorithm::RS256, "test-kid", TEST_PRIV)), Err(OidcError::Invalid));
    }

    #[test]
    fn unknown_kid_is_rejected() {
        assert_eq!(validate_token(&cfg(), &keys(), &mint(valid_claims(), Algorithm::RS256, "other-kid", TEST_PRIV)), Err(OidcError::UnknownKid));
    }

    #[test]
    fn hs256_alg_confusion_is_rejected() {
        // Forge an HS256 token using the RSA *public* key bytes as the HMAC secret —
        // the classic alg-confusion attack. Must be rejected as Malformed (alg not
        // in the asymmetric allowlist), never validated.
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some("test-kid".to_string());
        let secret = EncodingKey::from_secret(TEST_PUB.as_bytes());
        let forged = encode(&header, &valid_claims(), &secret).unwrap();
        assert_eq!(validate_token(&cfg(), &keys(), &forged), Err(OidcError::Malformed));
    }

    #[test]
    fn garbage_token_is_malformed() {
        assert_eq!(validate_token(&cfg(), &keys(), "not.a.jwt"), Err(OidcError::Malformed));
    }

    #[test]
    fn jwks_fixture_builds_a_working_key() {
        // The JWKS fixture must carry the test public key correctly (modulus/exp),
        // so a token signed by TEST_PRIV verifies against a key built from the JWK.
        let jwks: jsonwebtoken::jwk::JwkSet =
            serde_json::from_str(include_str!("../../tests/fixtures/oidc_jwks.json")).unwrap();
        let keys: Vec<(String, DecodingKey)> = jwks
            .keys
            .iter()
            .filter_map(|j| {
                let kid = j.common.key_id.clone()?;
                DecodingKey::from_jwk(j).ok().map(|k| (kid, k))
            })
            .collect();
        assert_eq!(keys.len(), 1, "fixture should yield one key");
        let token = mint(valid_claims(), Algorithm::RS256, "test-kid", TEST_PRIV);
        let p = validate_token(&cfg(), &keys, &token).expect("JWK-built key must verify the token");
        assert_eq!(p.subject, "alice@example");
    }

    #[test]
    fn scopes_for_groups_unions_and_dedups() {
        let c = cfg();
        assert_eq!(c.scopes_for_groups(&["readers".into()]), vec!["kv:read".to_string()]);
        assert!(c.scopes_for_groups(&["unknown".into()]).is_empty());
        // admins → "*"; duplicate groups don't duplicate scopes.
        let s = c.scopes_for_groups(&["admins".into(), "admins".into()]);
        assert_eq!(s, vec!["*".to_string()]);
    }
}
