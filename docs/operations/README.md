# Operations

DevOps-facing runbooks for deploying and operating a Mycelium cluster. The
developer-side counterpart is the [guide cookbook](../guide/cookbook.md); the
vocabulary is in [00 · Concepts](../guide/00-concepts.md).

## Start here

- **Deploying?** → [deployment.md](deployment.md), then run the
  [production-readiness.md](production-readiness.md) go-live checklist (the north star).
- **Watching a live cluster?** → [observability.md](observability.md) (endpoints, `/stats`,
  scraping) + [metrics.md](metrics.md) (every emitted metric, what to alert on).
- **Something's wrong?** → [diagnostics.md](diagnostics.md) — localize / explain / diagnose a
  coordinator-free fleet.
- **Need to prove what happened?** → [audit.md](audit.md) — the tamper-evident trail,
  revocation transparency proofs, and proving a guardrail stopped an agent.

| Doc | What it covers |
|---|---|
| [production-readiness.md](production-readiness.md) | **the go-live checklist** — one pre-flight tying the topic docs below into a single sweep (security · persistence · sizing · observability · supply chain · companions) |
| [customer-pilot.md](customer-pilot.md) | **first customer-led project** — scoping, de-risking, and treating the pilot as the external validation the internal audit loop can't self-supply |
| [deployment.md](deployment.md) | the library-embed model, ports, seeds, TLS/auto-CA, containers, restart behaviour |
| [observability.md](observability.md) | the public endpoints (`/health` `/ready` `/stats` `/metrics`), reading the tripwire counters, **viewing AgentFacts**, Prometheus, dashboards |
| [metrics.md](metrics.md) | **the metrics reference** — every emitted Prometheus series (gossip · emergent · governor · artifact · guardrails · reason), by family, with what to watch for |
| [diagnostics.md](diagnostics.md) | **diagnosing a coordinator-free fleet** — localize/explain/diagnose, one runbook entry per emergent pathology, Prometheus alert recipes |
| [dynamic-scaling.md](dynamic-scaling.md) | elastic membership + capacity via `/gateway/govern`; **seeing scaling** live; live tuning |
| [artifacts.md](artifacts.md) | the cluster-wide **artifact catalogue** — where it lives, registering, authoring + publishing a deployable |
| [companions.md](companions.md) | **operating the companions** (tuple-space · blackboard · wiki) — durability/WAL, capability-ring failover, the wiki's node-independent store, teardown invariants |
| [tuning.md](tuning.md) | the full config reference + env-var precedence, auto-derivation, hard invariants, per-size scaling profiles, RPC-heavy-pair pinning, performance baselines |
| [rbac.md](rbac.md) | signed role claims, capability authz, OAuth2 gateway ACLs |
| [sso.md](sso.md) | generic-OIDC SSO at the gateway |
| [gateway-tls.md](gateway-tls.md) | native HTTPS for the gateway (so tokens aren't cleartext) |
| [audit.md](audit.md) | the tamper-evident, hash-chained audit trail |
| [cert-rotation.md](cert-rotation.md) | hot Ed25519 identity/cert rotation with no disruption |
| [crown-jewel.md](crown-jewel.md) | data-at-rest cipher hook + egress allowlist + threat model |

Start with [deployment.md](deployment.md), then [observability.md](observability.md). Before a
production go-live, run the [production-readiness.md](production-readiness.md) checklist; for a first
customer engagement, add [customer-pilot.md](customer-pilot.md).
