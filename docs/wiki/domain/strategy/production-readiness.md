# The production-readiness gate — four sub-gates

↑ [strategy](strategy.md) · engineering status: **shipped** (see [dev/security](../../dev/security.md), [dev/history](../../dev/history.md))

The shape of "production-hardening for regulated buyers" — kept because the *framing* is
how those conversations go, even now the engineering is done:

1. **AuthN/Z + RBAC** — data-classification-aware, not generic RBAC (L1 board read ≠ L3
   SPOF topology). Shipped as WS1 (signed role claims + clearance, provider-side authz,
   scoped gateway ACLs) + WS4 OIDC SSO.
2. **Audit — complete and tamper-evident** — every action traceable to a principal;
   cryptographically chained so an inspector can verify no post-hoc edits. Shipped as WS2
   (per-node hash-chained signed streams; `/gateway/audit`).
3. **Crown-jewel posture** — the sharpest gate: the fleet-state/twin is the concentrated
   SPOF map; the buyer question is *blast radius of compromising it*, not "is it secure".
   Data-at-rest, egress boundary, blast-radius model — shipped as WS3 +
   `docs/threat-model.md`. Brokered competitors answer this badly: their broker IS the
   crown jewel, structurally exposed.
4. **Support/SLA** — "who owns this at 3 AM Saturday?" Commercial-track, not engineering:
   SLA tiers, named relationships, escalation paths, reference customers. Open until paid
   production deployments exist.

Lead with this structure (substrate-provides vs still-to-build) rather than generic
"hardening" — it moves the conversation to specific work.
