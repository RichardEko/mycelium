# First Customer Pilot — readiness & de-risking checklist

↑ [Operations](README.md) · pairs with [production-readiness.md](production-readiness.md)

Mycelium is technically ready to carry a real customer engagement — v2.0 complete, the companion
ecosystem shipped, security/observability/compliance in place, CI-gated. But every strong signal today
is **self-assessed**: the project's own audit methodology
([`docs/analysis/ratings.md`](../analysis/ratings.md), M2) caps any dimension at 9 without **external
validation**, and calls a 10 "unreachable from inside this loop." A first customer-led project is
exactly what closes that gap — *the pilot is the validation*. This page is how to run that first one so
it de-risks the engagement instead of stress-testing the newest code in production.

## The framing (say this out loud with the customer)

- The **substrate** (Layers I–III, security, SDKs) is mature and heavily tested.
- Some **companions are new** — the `mycelium-wiki` crate and its access broker are days old and have
  been audited once, by the team that built them. A pilot is their first real-world exercise.
- Therefore: **bounded scope + engineering in the loop + explicit success criteria.** Treat findings as
  the point, not a surprise.

## 1 · Scope it to proven ground

- ☐ **Pick a use case a shipped companion already models**, not a greenfield stress of the newest
  surface. The worked examples are the template:
  - work distribution / pipelines → `mycelium-tuple-space` ([`fluid_pipeline`](../../examples/fluid_pipeline)),
  - opportunistic content-routed claiming → `mycelium-blackboard` ([`microgrid`](../../mycelium-blackboard/examples/microgrid.rs)),
  - durable curated knowledge (org-twin / council) → `mycelium-wiki` ([`wiki_chat`](../../mycelium-wiki/examples/wiki_chat.rs)),
  - autonomic code mobility → `mycelium-wasm-host`, federation edge → `mycelium-agentfacts`.
- ☐ **Bound the scale** to a node count you have *rehearsed formation at on the real target network*
  (not Docker-bridge — see the known 100-node CI ceiling in [tuning.md](tuning.md)). Start well inside
  the envelope; grow after the first success.
- ☐ **One coordination primitive first.** Don't debut the wiki + consensus + cross-group + WASM in the
  same pilot. Add surfaces once the first is boringly stable.

## 2 · Production gates (inherited)

- ☐ **Complete [production-readiness.md](production-readiness.md) for the deployment** — TLS, gateway
  auth, persistence + restart rehearsal, sizing profile, metrics/alerts, `cargo audit`. The pilot does
  not get to skip these because it is "just a pilot."
- ☐ **If the pilot uses the wiki:** provision the node-independent store (shared FS / S3 / doc store),
  set the curator's `Membership` access policy, and confirm `Wiki::shutdown` is called on teardown
  (the curator's background tasks are otherwise long-lived). → [companions/wiki.md](../wiki/dev/companions/wiki.md)

## 3 · Keep engineering in the loop

- ☐ **Named escalation path** — a substrate engineer reachable during the pilot window, not just docs.
- ☐ **Diagnosis rehearsed together** — the customer's operator has run `/gateway/diagnose` +
  `/gateway/explain` against a *deliberately perturbed* staging fleet, so the first real incident isn't
  the first time they read a fleet narrative. → [diagnostics.md](diagnostics.md)
- ☐ **Feedback loop** — a lightweight channel to file what the pilot surfaces; expect it to feed a
  `/mycelium-analysis` run and possibly a fix, same as Run 32 caught the wiki lifecycle leak.

## 4 · Define success before you start

- ☐ **Functional criteria** — the specific coordination outcome the use case needs (e.g. "N work items
  complete exactly once with no coordinator", "the council corpus answers M queries grounded in curated
  facts").
- ☐ **Operational criteria** — mesh forms and stays converged at the target scale; a killed node fails
  over within the expected window; the fleet stays diagnosable; no unbounded resource growth over the
  pilot duration (watch `gossip_store_entries`, task counts, memory).
- ☐ **Non-goals stated** — no SLA/uptime guarantee (Support/SLA is commercial-track, out of engineering
  scope); this is a validation engagement, and both sides agree on that.

## 5 · What to watch (the pilot's own tripwires)

- ☐ **Convergence & liveness** — `gossip_peers_connected`, anti-entropy activity, no reconnect storms.
- ☐ **Back-pressure** — `gossip_frames_dropped_total`, the `sys/load/` opacity signals; is anything
  shedding admission?
- ☐ **Growth** — `gossip_store_entries` and process memory over days, not minutes (the class of leak
  Run 32 found is invisible in a short test).
- ☐ **The emergent-fleet questions** — periodically run `/gateway/diagnose`; a healthy pilot should read
  as boring.

## Exit — turning the pilot into evidence

- ☐ **Write it up** — what held, what surfaced, what was fixed. A successful pilot is the first external
  data point the M2 audit series has ever had; record it in the calibration ledger's spirit (scores
  that predicted reality) and in the customer-facing record.
- ☐ **Decide the next envelope** — more nodes, a second primitive, or a second use case — from evidence,
  not optimism.

---

**Bottom line:** the honest readiness statement is *"ready for a bounded first customer-led project,
with engineering engaged, treated as the external validation the internal loop cannot self-supply."*
Run it that way and the second engagement starts from evidence instead of self-assessment.
