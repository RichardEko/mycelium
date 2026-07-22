# 2026-07-22 — Compliance-tier mechanism corrected (SOC 2 scope error)

**Trigger:** adopters starting to ask about SOC 2; a review of what the compliance tier can
actually sell surfaced a category error in `licensing-and-compliance.md`.

**The error:** the tier was framed as delivering "a SOC 2 Type II report" and the moat as "SOC 2
process time," implying a report on *Mycelium's runtime*. SOC 2 attests to an **operated
system**; a library vendor operates nothing in the customer's data path, so no such report can
exist. A buyer who hears "Mycelium has a SOC 2" assumes runtime certification — a scope
misrepresentation the buyer's auditor would catch (they read the scope section).

**The correction (now in the page):**
- The tier sells a **vendor-assurance programme**, not a runtime certification. Deliverables:
  a SOC 2 Type II on the vendor's **own corp/SDLC systems** (build pipeline, repo, endpoints —
  achievable; attests *how we build*, lands as the customer's **CC9 vendor-risk** evidence, not
  their operational controls), a **third-party pentest** on the library (answers "is it secure"
  directly; cheaper/faster; the better *first* buy), the shared-responsibility + control-mapping
  docs, SBOM + patch/vuln-disclosure SLA, and contractual instruments.
- **BAA only if we are actually a business associate** — pure embedded software the vendor never
  receives PHI through generally is *not* one; may resolve to a DPA. (Prior text asserted "HIPAA
  BAA is statutory; no workaround" — an overclaim about *our* status.)
- **Moat reframed:** the accumulated assurance apparatus (corp SOC 2, pentest history, executed
  BAAs/DPAs, references) that accrues over calendar time — not an impossible report on the library,
  and not code secrecy. The ~6-month observation window survives, re-attributed to the corp SOC 2
  (a system we genuinely operate).

**Cost numbers unchanged** (~$30–50k readiness + $60–120k Type II + renewal) — only the *subject*
of the report was wrong.

**Open follow-ups:**
- `docs/internal/commercial.html` (the full deck, gitignored/local-only) almost certainly repeats
  the "SOC 2 report on Mycelium" framing — needs the same correction; flagged to the user.
- Business-associate determination + the actual contract instruments (BAA/DPA/indemnity) want a
  compliance consultant / counsel — outside what the wiki should assert.
- The consolidated adopter-facing shared-responsibility matrix is still unbuilt (offered).

## Path confirmed (2026-07-22, same day)

User confirmed the **pure-library path**: corp/SDLC SOC 2 + pentest + evidence, **no managed
service** ("unlikely to change in the near future"). This resolves the fork the mechanics doc
(`TathataSystems/docs/compliance_audit_mechanics.html`) had left open — that doc previously
*recommended* the managed-service path as "primary scope." Reconciled: its two-path list now
marks development-practices as "our path — confirmed" and managed-service as "NOT our path
(considered and rejected — would break library-not-platform)." The `commercial.html` deck was
already corrected to the pure-library framing; `licensing-and-compliance.md` now carries the
confirmed-path note at the top. All three surfaces (deck · mechanics doc · wiki tier page) are
now consistent. (TathataSystems `docs/` is not a git repo — those two edits are loose-file saves.)
