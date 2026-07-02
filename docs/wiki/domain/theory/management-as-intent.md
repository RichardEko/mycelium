# Management as intent — the governance pattern

↑ [theory](theory.md) · converged with Richard 2026-06-18; first instance = the WS-C M9 tuning governor (shipped)

**There is no "management" as a privileged thing.** There are only **intents** — published
by any entity with a concern (human via gateway, or an agent; the substrate cannot tell them
apart) — and **nodes that locally reconcile** them. A node is sovereign over its own config:
it *reads* intents and *decides*.

Rules: intent never command (desired-state into a gossiped `sys/…` prefix — evidence, never
a verdict) · decision stays at the node (local reconciler applies/clamps/vetoes; own intent
trumps remote; newest-wins among remotes) · nothing is permanently locked (a lock is just
the currently-winning intent; the human is just another publisher) · intents are refreshed
soft-state that **evaporate** — publisher vanishes ⇒ node reverts to its own derivation.
This evaporation is the load-bearing self-healing property.

**Litmus test for any management feature:** *if the management actor vanishes, does the
cluster keep running and self-heal?* Keeps running → participant ✅. Freezes waiting →
coordinator-with-extra-steps ❌.

**Coordination gradient — default to the weakest tier:** (1) advice (gossip + per-node
policy — ClusterTuner recommendations); (2) desired-state + veto (evaporating soft-state +
local reconciler — the tuning/membership governors; **most "management" lands here**);
(3) hard cluster-wide invariant → Layer III consensus, only for genuinely inviolable bounds.

**Abstraction seam (agreed 2026-06-18):** share the reconcile/transport plumbing (generic
`IntentReconciler<T>` — `src/agent/intent.rs`), keep policy/decision bespoke per behaviour;
no heavy `IntentGovernor<T>` trait unless the Rule of Three demands it (membership's
collective self-election shares nothing with the tuner's scalar gate).

**Operator surface model:** HTTP is opt-in on a *subset* of nodes; the governance engine is
gateway-free so headless nodes stay first-class. No consensus-elected "single active
endpoint" (rejected: coordinator/SPOF serialising idempotent LWW writes). Per-node control
without per-node HTTP = node-targeted fleet intent (`target: Option<NodeId>`). Routes behind
deny-by-default `govern:*` scopes; changes audited (WS2); effective state observed via
per-node Prometheus gauges (operator aggregates —
[deployment-framing](../strategy/deployment-framing.md)). Shipped surface:
[dev/operations](../../dev/operations.md).

Connects the Coordinator-Trap thesis to practice; the BRAIN-IoT lineage note: its
requirement→deploy loop was already decentralized (event bus + per-node resolver) —
Mycelium's edge is **edge-triggered → level-triggered evaporating soft-state**, not
coordinator removal per se.
