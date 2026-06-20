# Operations

DevOps-facing runbooks for deploying and operating a Mycelium cluster. The
developer-side counterpart is the [guide cookbook](../guide/cookbook.md); the
vocabulary is in [00 · Concepts](../guide/00-concepts.md).

| Doc | What it covers |
|---|---|
| [deployment.md](deployment.md) | the library-embed model, ports, seeds, TLS/auto-CA, containers, restart behaviour |
| [observability.md](observability.md) | the public endpoints (`/health` `/ready` `/stats` `/metrics`), reading the tripwire counters, **viewing AgentFacts**, Prometheus, dashboards |
| [dynamic-scaling.md](dynamic-scaling.md) | elastic membership + capacity via `/gateway/govern`; **seeing scaling** live; live tuning |
| [artifacts.md](artifacts.md) | the cluster-wide **artifact catalogue** — where it lives, registering, authoring + publishing a deployable |
| [tuning.md](tuning.md) | the config knobs (shards, channel depth, intervals, connection cap) |
| [rbac.md](rbac.md) | signed role claims, capability authz, OAuth2 gateway ACLs |
| [sso.md](sso.md) | generic-OIDC SSO at the gateway |
| [audit.md](audit.md) | the tamper-evident, hash-chained audit trail |
| [cert-rotation.md](cert-rotation.md) | hot Ed25519 identity/cert rotation with no disruption |
| [crown-jewel.md](crown-jewel.md) | data-at-rest cipher hook + egress allowlist + threat model |

Start with [deployment.md](deployment.md), then [observability.md](observability.md).
