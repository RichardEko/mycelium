# Licensing tiers and the compliance moat

↑ [strategy](strategy.md)

**Confirmed strategic path (2026-07-22, user): pure library — no managed service.** "SOC 2"
here always means a SOC 2 on our **own corporate/SDLC systems** (how we build), never a runtime
certification of the library, and never a hosted cluster we operate. The managed-service path
(which *would* yield a runtime SOC 2) is considered and **rejected** — it would break
[deployment-framing](deployment-framing.md). Revisit only if that framing ever changes.

**Three tiers:** **AGPL** (free; seeds adoption) · **Commercial Embed** (removes copyleft
for proprietary embedding) · **Compliance** (a vendor-assurance *programme* — same code,
different accountability; a services contract, not gated features). RBAC/SSO/audit are in ALL
tiers (they're in the AGPL library); the compliance tier sells the assurance apparatus around
them, which a fork cannot copy off the source.

**What the tier delivers — and the SOC 2 scope trap.** SOC 2 attests to an *operated system*;
Mycelium is an embedded library the vendor runs nothing of, so **there is no SOC 2 on
Mycelium's runtime** — the report a buyer pictures (the library's controls, certified) cannot
exist. What the vendor *can* hold, and share with paying customers under NDA, is a SOC 2 Type II
on its **own corporate/SDLC systems** (source control, CI/CD, endpoint + access management,
release process): it attests we *build and ship* under control, and lands in the customer's
audit as **vendor-risk evidence** (their CC9 vendor-management control), never as their
operational controls. Bundle it with a third-party **penetration-test report** on the library
(answers "is the software secure" directly — faster and cheaper than SOC 2, the better *first*
buy), the **shared-responsibility + SOC 2/HIPAA control-mapping** docs, an SBOM +
vuln-disclosure / patch **SLA**, and contractual instruments (DPA; **BAA only if we are actually
a business associate** — pure embedded software the vendor never receives PHI through generally
is *not* one). Describe the SOC 2 as "on our development/corporate systems"; implying a runtime
certification of the library misrepresents scope, and the buyer's auditor reads the scope section.

**The moat is the accumulated assurance apparatus, not code secrecy:** a fork gets the code
free but starts from zero on evidence that only accrues over calendar time — a corporate SOC 2
Type II (mandatory ~6-month observation window, AICPA; incompressible — and now on a system we
genuinely operate), a pentest history, signed customer references, executed BAAs/DPAs, a
patch-SLA track record. Engineering prerequisites (v1.x WS1/WS2/WS4) are shipped — the SDLC
engagement can open. Costs: ~$30–50k readiness + $60–120k Type II + ~$20–40k/yr renewal
(Vanta/Drata reduce ongoing 40–60%); the pentest is a separate, smaller, faster line item.

**Beachhead sequence:** healthcare AI (compliance pressure highest and non-negotiable — though
the BAA question is *our* business-associate status, which for never-touch-PHI embedded software
may resolve to a DPA, not a BAA) → financial services (de-facto mandatory via OCC/FDIC
third-party risk) → federal (FedRAMP Tailored; SOC 2 prerequisite; longer cycle).

**AGPL taint boundary:** HTTP-gateway callers = not derivative = clean; `use mycelium::*` /
`import mycelium` = tainted. The network-use clause closes the hosted-service loophole. SI
dynamics: a Big-4 build path = 18–24 months + the same SOC 2 queue + prior-art problem →
build firms end up licensing; the partner path converts in months. Priority asset:
`partners.mycelium.dev` tier/registration page before a competitor approaches the same SIs.
