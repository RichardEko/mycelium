# Licensing tiers and the compliance moat

↑ [strategy](strategy.md)

**Three tiers:** **AGPL** (free; seeds adoption) · **Commercial Embed** (removes copyleft
for proprietary embedding) · **Compliance** (BAA + SOC 2 Type II report + controls evidence
+ incident-response SLA — same code, different accountability; a services contract, not
gated features). RBAC/SSO/audit are in ALL tiers (they're in the AGPL library); the
compliance tier sells the *programme around them* — competitors can copy code, not a live
BAA or a SOC 2 report.

**The moat is process time, not code secrecy:** SOC 2 Type II requires a mandatory
~6-month observation window (AICPA; incompressible). Engineering prerequisites (v1.x WS1/
WS2/WS4) are shipped — the engagement can open. Costs: ~$30–50k readiness + $60–120k Type
II + ~$20–40k/yr renewal (Vanta/Drata reduce ongoing 40–60%).

**Beachhead sequence:** healthcare AI (HIPAA BAA is statutory; no workaround) → financial
services (de-facto mandatory via OCC/FDIC third-party risk) → federal (FedRAMP Tailored;
SOC 2 prerequisite; longer cycle).

**AGPL taint boundary:** HTTP-gateway callers = not derivative = clean; `use mycelium::*` /
`import mycelium` = tainted. The network-use clause closes the hosted-service loophole. SI
dynamics: a Big-4 build path = 18–24 months + the same SOC 2 queue + prior-art problem →
build firms end up licensing; the partner path converts in months. Priority asset:
`partners.mycelium.dev` tier/registration page before a competitor approaches the same SIs.
