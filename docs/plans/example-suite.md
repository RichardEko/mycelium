# Delivery plan — the Food-Rescue Co-op example suite

**Status:** ✅ **SHIPPED & CI-GATED** (2026-06-20). **All eleven examples** (the 11th, `catalog`,
added via the [docs-and-examples-alignment plan](docs-and-examples-alignment.md)) landed under
`examples/coop/` and run Docker-free via `examples/coop/ci_smoke.sh`, which is wired into the
`coop-smoke` CI job (retry-hardened for constrained runners — PR #65). The original six-example plan
below is preserved as the design record; the suite was then extended past it (see *Shipped status*).
A 12th worked example, the blackboard [`microgrid`](../../mycelium-blackboard/examples/microgrid.rs),
ships in the `mycelium-blackboard` crate with its own smoke (WS-G / G3 Phase 5).

**Shipped status — 11 examples (each its own PR, each CI-gated):**

| # | Bin | PR | Demonstrates |
|---|-----|----|--------------|
| 01 | `mailbox_llm` | #52 | actor ↔ LLM via the durable, HLC-ordered mailbox |
| 02 | `stigmergy` | #54 | coordinator-free load shedding via the `sys/load` pheromone |
| 03 | `elastic_intent` | #58 | elastic sizing as evaporating intent (operator-optional) |
| 04 | `provisioning` ⭐ | #59 | the full autonomic loop — tuple-space buffer + WASM self-provision + failover |
| 05 | `federation_facts` | #60 | cross-domain edge discovery via self-certified AgentFacts |
| 06 | `rotation` | #61 | zero-disruption identity rotation; pre-rotation facts still verify |
| 07 | `consensus` | #62 | Layer III — cross-group multi-bloc agreement + leased (decaying) decisions |
| 08 | `llm_pipeline` | #63 | homogeneous LLM workers, competitive pull over a linear tuple-space pipeline |
| 09 | `mcp_toolgrowth` | #64 | an LLM agent grows the fabric's toolset at runtime (MCP tool loaded on demand) |
| 10 | `llm_council` | #66 | a council of **differentiated** LLM agents — fan-out → synthesis → iterative refinement |
| 11 | `catalog` | #73 | the cluster-wide artifact catalogue — gossiped `installable/`, register / discover / pull-over-mesh / provision (no registry server) |

CI gating: PR #65 (`coop-smoke` job, retry harness).

**Steps 07–10 were added past the original six-example plan**, to close coverage gaps surfaced during
a differentiator audit: Layer III consensus (07) was entirely absent; the LLM-over-tuple
compositions were thin (08 covered only a homogeneous worker pool; 10 added differentiated agents +
fan-out/fan-in + iterative refinement); and MCP runtime tool-growth (09) was undemonstrated. The one
LLM-coordination pattern still *not expressible* — keyed-correlation fan-in joins — is named
explicitly in `llm_council` (ROADMAP M13, Paper 1 §9.4) rather than left unsaid.

**Substrate side-effects surfaced and resolved while building** (the examples doubled as a stress
test): the emergent-watcher-vs-governor fix (#56 → PR #57) that unblocked Step 03; the `crdt.rs`
retained-key verification fix (PR #51) that Step 06 demonstrates; a flaky opacity-gate test (PR #53);
and the `coop-smoke` constrained-runner hardening (capture stderr + per-demo retries + widened
budgets, PR #65). Issue #55 (cross-node Individual-scoped signals not reaching a remote `signal_rx`)
was filed for a maintainer decision.

**Goal.** A cohesive set of runnable examples that demonstrate the capabilities
shipped since the last example pass — the **mailbox** (actor/event delivery),
**WS-C** governance (management-as-intent / elastic sizing), **WS-E** autonomic
provisioning (`mycelium-wasm-host`), **WS-F** federation (`mycelium-agentfacts`),
and the **tuple-space** pull pipeline — *composed in one constructive world*
rather than five isolated API toys.

Per the project's example-domain convention, the world is **constructive and
civic**, never crisis/war-room framed.

---

## The world: a regional food-rescue logistics co-op

A network of **depot** nodes coordinates rescuing surplus food (from donors:
markets, farms, bakeries) and routing it to community kitchens before it spoils.
Each depot is a Mycelium node. The co-op has **no central dispatcher** — depots
advertise capabilities, claim work when ready, and self-organise. A neighbouring
co-op is a *separate domain* the federation facet talks to.

This single narrative is the suite's **conceptual-integrity anchor**: every
example is a facet of the same world, so a reader sees the layers *compose*, not
just individual calls.

Shared domain vocabulary (in `common/`):
- **Donation** — `{ id, donor, items, perishable_by, origin_zone }` (opaque
  `Bytes` payload on the wire; a typed struct in the demo).
- **Depot** — a node; advertises capabilities like `intake`, `cold-storage`,
  `route-optimize`.
- **Kitchen** — a sink zone a route terminates at.

---

## Shared harness — `examples/coop/common/`

Built **first**, because every example mounts it:

1. **`bootstrap`** — spin up N depot agents with consistent config (gateway on,
   `tls` for identity, bootstrap-peer wiring), constructive node names
   (`depot-camden`, `depot-hackney`, …).
2. **`facts_lens`** — mounts `mycelium_agentfacts::agent_facts_router(...)` on
   every depot via `GossipAgent::with_http_routes`, so each node serves a live
   `/.well-known/agent-facts.json` (self-certified edge view) **and** the CRDT
   `domain_facts` board. *This is selection #3 ("AgentFacts lens") — infrastructure,
   not a standalone example.* Each depot also `publish_field`s a couple of live
   facts (`status`, `zone`) so the board is populated.
3. **`domain` types** — `Donation`, zone enums, (de)serialisation helpers.

**Crate shape.** `examples/coop/` is a **standalone workspace crate** depending on
`mycelium` + the three companion crates (`mycelium-tuple-space`,
`mycelium-wasm-host`, `mycelium-agentfacts`) — it cannot be a `[[example]]` of the
main crate because those companions depend *on* `mycelium`, not the reverse. Each
demo is a `[[bin]]` (or a `docker-compose.yml` for the multi-node ones). Decision:
add it as a workspace **member** (so `cargo build` covers it) unless build time
forces `exclude` like `conway-gpu`.

---

## The six examples (build order = small → flagship)

> **Design record.** This section is the *original* six-example plan. The shipped suite has **eleven**
> examples (07–11 were added past this plan — see the *Shipped status* table at the top). The six
> below are preserved as the design rationale; the canonical per-example descriptions now live in
> [`examples/coop/README.md`](../../examples/coop/README.md).

### 01 — `mailbox-llm` (selection: LLM agent mailbox)
**Story.** A depot receives a donation and asks the co-op's **triage** skill —
an LLM-backed node — "which kitchen, which route?" The answer comes back to the
depot's own mailbox.

**APIs.** `agent.service().deliver_event(target, "triage.ask", donation)` →
target's `open_mailbox("triage.ask")` drains in HLC-causal order → `agent.llm()`
(Prompt Skills, `llm` feature, mock backend in CI) → `deliver_event` the reply to
the sender's `"triage.reply"` mailbox.

**What it proves.** Actor↔LLM interaction on the substrate; **durable
redelivery** — kill the triage node mid-request, restart within the TTL window,
it picks the pending event back up (at-least-once via anti-entropy + tombstone).

**Features.** `llm`, `gateway`, `tls`. Smallest — validates the harness + lens.

---

### 02 — `stigmergy` (additional idea: backpressure pheromone)
**Story.** An overloaded depot signals "I'm at capacity" and incoming donations
reroute to peers; when it recovers it silently rejoins.

**APIs.** `advertise_capability("intake")` on all depots; the busy depot writes
`sys/load/{self}` opacity (becomes `is_self_opaque`), so capability `resolve`
skips it; clearing the load makes it resolvable again. No messages, no manager —
pure stigmergy.

**What it proves.** Coordinator-free load shedding; the pheromone *is* the failure
detector. Tiny code, very visual on the `/stats` + facts board.

**Features.** `gateway`, `tls`.

---

### 03 — `elastic-intent` (selection: elastic sizing by intent)
**Story.** The co-op operator declares "keep between 3 and 6 depots online for the
morning rush." Depots **self-elect** to join/drain. Then the **operator goes
offline** — and the cluster keeps running on the last intent and self-heals.

**APIs.** `MembershipIntent::new("rush-pool", 3, Some(6))` published via the
`/gateway/govern/membership` operator surface (or `publish_intent`);
`start_membership_governor()` on each depot (probabilistic self-election,
`join_probability`); the intent is **evaporating soft-state** (`MEMBERSHIP_INTENT_TTL_MS`)
so killing the operator does not freeze the cluster.

**What it proves.** Management-as-intent (`intent.rs`): no privileged controller —
just an evaporating desired-state + local reconcile. Litmus: *"if management
vanishes, does the cluster self-heal?"* → demonstrated by killing the operator.

**Features.** `gateway`, `tls`, `compliance` (for the `/gateway/govern` audit).

---

### 04 — `provisioning` ⭐ FLAGSHIP (selection: self-provisioning under backpressure)
**Story.** A surge of donations needs a `route-optimize` capability **no depot
currently has**. Donations **buffer in a tuple-space lane** while idle depots
**self-provision** the optimizer (a WASM component) and then drain the backlog.
Kill an optimizer mid-drain → another depot self-provisions and the survivors
complete under their original ids.

**APIs.**
- Buffer: `TupleSpace` lane `route-optimize.pending` — seeder `put`s donations;
  lane depth = the backpressure pheromone (`sys/tuple/.../backpressure`).
- Demand: unmet requirement raises pressure (`src/agent/demand.rs`,
  `sys/load/{self}/group-req/...`).
- Provision: `mycelium_wasm_host::Provisioner::new(...).supervise(CapFilter, min)`;
  `provision_round()` resolves → pulls the content-addressed artifact
  (`ArtifactId`, `verify_artifact`, `require_provenance` Ed25519) → instantiates →
  `advertise_capability("route-optimize")`.
- Drain: workers `take()` from the lane the moment the capability appears;
  `complete` → `ack`, exactly-once.
- Failover: WS-E supervision (`min_providers`) re-provisions on a killed provider —
  *restart ≡ provisioning*.

**What it proves.** The whole autonomic loop end-to-end: **nothing predicted who
would run the optimizer** — demand was a pheromone, provisioning self-elected, the
buffer lost no item, the rendezvous failed over on its own. The thesis in one
`docker compose up`.

**Features.** `gateway`, `tls` + `mycelium-tuple-space`, `mycelium-wasm-host`.
Multi-node (Docker compose). The largest build; depends on `common/` + lessons
from 01–03.

---

### 05 — `federation-facts` (selection: AgentFacts + federation)
**Story.** A **neighbouring co-op** (a second domain) has overflow it can't route.
It discovers *our* co-op's `route-optimize` / `cold-storage` capabilities by
**pulling our AgentFacts at the edge** — self-certified, signature-verified, no
shared trust authority — and routes the overflow to us.

**APIs.** `signed_agent_facts(agent, opts)` + `agent_facts_router` serve the edge
doc; the neighbour fetches `/.well-known/agent-facts.json`, calls `SignedFacts::
verify()` (trust is the fetcher's decision — Core Principle 1), reads the
capability list, and routes. The intra-domain CRDT board (`domain_facts`,
`publish_field`/`read_verified_fields`) shows the multi-depot assembled view.

**What it proves.** Mycelium domain as a sovereign NANDA-quilt patch: discoverable
across domains with cryptographic verification and **no coordinator-shaped trust
authority**.

**Features.** `tls`, `gateway` + `mycelium-agentfacts`. Two-domain Docker compose.

---

### 06 — `rotation` (additional idea: zero-disruption identity rotation)
**Story.** A depot rotates its Ed25519 identity mid-operation (routine hygiene).
Peers keep verifying its audit chain **and** its pre-rotation AgentFacts fields
across the rotation — zero disruption, no dropout from the federation view.

**APIs.** `agent.rotate_identity(propagation)` (writes `new‖old`, swaps the key);
peers' retained-key-set verification (`crdt.rs` `verify_any`, connection/consensus/
rbac/audit paths) accept both. Directly showcases **PR #51** (the retained-key fix
we just merged).

**What it proves.** WS5 hot rotation + retained-key verification: a field signed
before the rotation still reads as verified after it. Small, pointed, closes the
loop on freshly shipped work.

**Features.** `tls`, `gateway`, `compliance` (audit chain) + `mycelium-agentfacts`.

---

## Cross-cutting requirements

- **CI-runnable without secrets.** LLM uses a mock backend (as the now-retired
  `prompt_skill_demo` and the community smoke did). Each example ships a `ci_smoke.sh`
  that runs Docker-free where possible, wired into the existing `afn-smoke`-style
  job pattern.
- **Every node serves the facts lens** — so any example can be inspected live via
  `/.well-known/agent-facts.json` and the `domain_facts` board.
- **A README per example** following the existing `concept → run → dev notes`
  guide convention (`docs/guide/README.md`), plus one suite-level
  `examples/coop/README.md` tying the world together.
- **Philosophy litmus in each README** — name the facet's "would Holland approve?"
  beat (no manager, no coordinator, emergence from local rules).

## Build order & milestones

| Step | Deliverable | Gate |
|---|---|---|
| 0 | `examples/coop/` crate + `common/` (bootstrap + facts_lens + domain types) | builds; one depot serves a verified facts doc |
| 1 | `01-mailbox-llm` | round-trip event + durable redelivery smoke |
| 2 | `02-stigmergy` | busy depot drops from `resolve`, recovers |
| 3 | `03-elastic-intent` | self-election to band; operator-kill self-heal |
| 4 | `04-provisioning` ⭐ | buffered backlog drains after self-provision; provider-kill failover |
| 5 | `05-federation-facts` | cross-domain edge discovery + verify |
| 6 | `06-rotation` | pre-rotation field verifies post-rotation |

Each step is independently shippable (its own PR), so the suite lands
incrementally rather than as one mega-PR.

## Decisions (resolved at ship, 2026-06-20)
All three pre-approval decisions were taken as their recommended defaults:
1. **Crate membership** → **workspace member** — `examples/coop` is in `Cargo.toml`
   `members`, built by `cargo build` and CI-gated via `coop-smoke`. Build time never
   regressed enough to force `exclude` (unlike `conway-gpu`).
2. **Scope of first PR** → **incremental** — the harness + first demo, then one PR per
   subsequent example (see the *Shipped status* table above: PRs #52–#65).
3. **Naming** → **`coop`** (`examples/coop/`; the README sets the scene).
