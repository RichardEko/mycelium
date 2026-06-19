# Food-Rescue Co-op — example suite

A cohesive set of runnable demos for Mycelium's newer capabilities — the **mailbox**, **governance
(management-as-intent)**, **autonomic provisioning**, **federation (AgentFacts)**, and the
**tuple-space** — composed in *one constructive world* rather than as isolated API toys.

> **The world.** A regional **food-rescue logistics co-op**: a network of **depot** nodes rescues
> surplus food (from markets, farms, bakeries) and routes it to community kitchens before it
> spoils. There is **no central dispatcher** — depots advertise capabilities, claim work when
> ready, and self-organise. A neighbouring co-op is a separate *domain* the federation demo talks to.

Full design + the six-example roadmap: [`docs/plans/example-suite.md`](../../docs/plans/example-suite.md).

## Shared harness (`src/common/`)

Every demo builds on this:

| Module | What it gives you |
|---|---|
| `domain` | the constructive vocabulary (`Donation`, zones) |
| `bootstrap` | `spawn_depot(...)` — a depot with gateway + tls identity, consistent across the suite |
| `facts_lens` | mounts the WS-F **AgentFacts edge endpoint on every depot**, so any running example is inspectable live at `/.well-known/agent-facts.json` |

The AgentFacts lens is *infrastructure, not a separate example*: while any demo runs, you can
inspect the federation view of a depot at the gateway port it prints on startup —

```bash
curl http://127.0.0.1:<printed-port>/.well-known/agent-facts.json
```

## Examples

| # | Bin | Status | Demonstrates |
|---|-----|--------|--------------|
| 01 | `mailbox_llm` | ✅ shipped | actor ↔ LLM via the durable, HLC-ordered **mailbox** |
| 02 | `stigmergy` | ✅ shipped | coordinator-free load shedding via `sys/load` pheromone |
| 03 | `elastic_intent` | ✅ shipped | elastic sizing as evaporating **intent** (operator-optional) |
| 04 | `provisioning` ⭐ | planned | the full **autonomic loop**: buffer in a tuple-space lane while peers self-provision the missing capability |
| 05 | `federation_facts` | planned | cross-domain edge discovery via self-certified AgentFacts |
| 06 | `rotation` | planned | zero-disruption identity rotation; pre-rotation facts still verify |

### 01 — `mailbox_llm`

```bash
cargo run -p mycelium-coop-examples --bin mailbox_llm
```

Three depots: `kitchen-router` hosts a `routing/suggest` Prompt Skill (`EchoBackend`, so no model
or API key is needed); `depot-intake` receives donations and **delivers an event to triage's
mailbox** asking where to route each; `depot-triage` is the **actor** — it drains its mailbox in
**HLC-causal order**, consults the router (a genuine cross-node RPC), and delivers the answer back
to intake's reply mailbox.

**Philosophy beat** ("would Holland approve?"): actor-style messaging — addressed, ordered, durable
within the gossip TTL window — emerges from Layer I (KV) + HLC ordering. No broker, no actor
registry, no explicit lifecycle. Addressing is just the target `NodeId` + a `kind` string.

### 02 — `stigmergy`

```bash
cargo run -p mycelium-coop-examples --bin stigmergy
```

Three worker depots advertise `depot/intake` and run an opacity governor over their `work.intake`
queue; a `depot-dispatch` node decides where intake goes by **reading the pheromone trail the medium
carries**. When one depot hits a local backlog, its governor writes an `is_opaque` pheromone to
`sys/load/{depot}/work.intake`; dispatch reads it (`is_node_opaque`) and routes around the busy
depot. Drain the queue and the pheromone evaporates — the depot rejoins the eligible set on its own.

**Philosophy beat:** load shedding with **no coordinator, no message, no failure detector**. The
busy node reports only its own saturation; every other node reads the trail and decides locally.

### 03 — `elastic_intent`

```bash
cargo run -p mycelium-coop-examples --bin elastic_intent
```

An operator declares "keep `rush-pool` in `[2, 3]` depots" by publishing an evaporating
`MembershipIntent` (soft-state, **not** a command). Five candidate depots run a `MembershipGovernor`
and **self-elect** so the pool holds a *subset* in the band — no controller picks who. Then: (2) the
operator goes offline and the band still holds (the intent persists in gossip within its TTL); (3) a
pool member is killed and the governors self-heal the count back to `MIN`.

**Philosophy beat:** *management = intent + local reconcile*. There is no privileged controller —
just an evaporating desired-state and nodes that reconcile locally. The litmus *"if management
vanishes, does the cluster keep working?"* is shown by killing the operator. (Requires the governor
to own a group under intent — see #56 / the emergent-defers-to-governor fix.)

## CI

`./ci_smoke.sh` runs the shipped demos Docker-free and asserts on their output (wired the same way
as the AFN fluid-pipeline smoke).
