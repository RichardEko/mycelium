# Gateway TLS

↑ [operations](README.md)

The embedded HTTP gateway (`http_port`) serves the REST/SSE/WS surface — capability advertise,
KV, signals, RPC, governance, audit query. **By default it is plaintext HTTP.** Bearer tokens and
OIDC JWTs sent to `/gateway/*` therefore traverse the wire in the clear unless you terminate TLS.
Two supported ways to fix that (SOC 2 CC6 "encryption in transit"):

## Option 1 — front it with a TLS-terminating proxy (unchanged, always available)

Run the gateway on loopback (`http_addr = "127.0.0.1"`, the default) behind nginx / Envoy / a
service-mesh sidecar that terminates TLS with your own cert. Nothing to configure in Mycelium.
This is the right choice when you already run a proxy/mesh or want a hostname cert managed centrally.

## Option 2 — native gateway TLS (`GossipConfig::gateway_tls`, 2026-07-22)

Set `gateway_tls` and the gateway serves HTTPS directly — no proxy required. It is **server-side
TLS only** (no client-cert demand), unlike the mutually-authenticated gossip transport (`tls`), so
ordinary HTTP clients (SDKs, curl, browsers) connect normally.

```rust
use mycelium::{GossipConfig, GatewayTlsConfig, TlsConfig};

let mut cfg = GossipConfig::default();
cfg.http_port = Some(9443);

// (a) Reuse the node identity cert — both fields None. Requires `tls`.
cfg.tls = Some(TlsConfig::default());
cfg.gateway_tls = Some(GatewayTlsConfig::default());

// (b) Operator-supplied cert with a real hostname SAN (browser trust):
cfg.gateway_tls = Some(GatewayTlsConfig {
    cert_pem_path: Some("/etc/mycelium/gateway-cert.pem".into()),
    key_pem_path:  Some("/etc/mycelium/gateway-key.pem".into()),
});
```

**Which cert?**

| Mode | Cert used | Trust story |
|---|---|---|
| Both fields `None` (default) | The node identity cert (from `tls`), served with no client-cert demand | Carries an **IP SAN** only — fits **CA-pinning SDK clients** and proxied setups, **not** hostname/browser trust. Requires `GossipConfig::tls` (else startup errors). Rotates automatically with the node identity (hot cert rotation). |
| `cert_pem_path` + `key_pem_path` | Your PEM cert chain + PKCS8 key | Use a cert with a real **DNS SAN** for browser/hostname clients. Both fields must be set together. You own its rotation. |

**Feature gate.** Native gateway TLS needs the `tls` feature (which `compliance` implies). Without
`tls`, `gateway_tls` is inert and the gateway stays plaintext.

**Behaviour.** The listener terminates rustls (ring provider) per connection and serves the axum app
over the TLS stream. On shutdown it stops accepting; in-flight connections drain. A handshake from a
client that does not trust the cert simply fails — the gateway never falls back to plaintext.

## Verifying

```bash
# node-cert mode — pin the generated cluster CA (IP SAN matches 127.0.0.1):
curl --cacert ./mycelium-tls/ca-cert.pem https://127.0.0.1:9443/health
# operator-cert mode with a hostname SAN:
curl https://gateway.example.com:9443/health
```

Regression gate: `agent::http::tests::test_gateway_serves_native_tls` (a rustls client validating
against the generated CA completes a real handshake and gets `/health` 200; a plaintext client fails).

## Shared-responsibility note

Encryption-in-transit for the gateway is **Shared**: Mycelium provides native TLS *or* you terminate
with a proxy — pick one; do not leave the gateway plaintext on a routable interface. See the
[shared-responsibility matrix](shared-responsibility-matrix.md).
