# Security Policy

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.** Public disclosure before
a fix is available puts all Mycelium users at risk.

Instead, send a private report to:

**security@mycelium.dev** (or richard.nicholson64@protonmail.com until a dedicated address
is configured)

Include:
- A description of the vulnerability and its potential impact
- Steps to reproduce or a proof-of-concept (even partial)
- The version or commit where you observed the issue

## Response commitments

| Milestone | Target |
|-----------|--------|
| Acknowledge receipt | 48 hours |
| Confirm scope and severity | 5 business days |
| Release patch or mitigation | 30 days (critical), 90 days (medium/low) |
| Public CVE / advisory | Coordinated with reporter |

If a timeline cannot be met we will notify you promptly and explain why.

## Scope

**In scope:**
- The `mycelium` Rust library (`src/`)
- Wire protocol security (framing, TLS handshake, Ed25519 signing)
- Authentication and authorisation in the HTTP gateway
- Cryptographic primitives (key generation, signing, verification)

**Out of scope:**
- Demo credentials in `examples/` (e.g., `postgresql://pipeline:pipeline@...` in the
  fluid_pipeline Docker Compose — these are intentional local-only demo credentials)
- Vulnerabilities in third-party dependencies (report those upstream; we will pull fixes
  into Mycelium promptly when they are available)
- Denial-of-service via resource exhaustion in a test/dev cluster

## Credit

We are happy to acknowledge security reporters in the release notes and CHANGELOG unless
you prefer to remain anonymous. Please let us know your preference when you report.
