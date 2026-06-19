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
| 02 | `stigmergy` | planned | coordinator-free load shedding via `sys/load` pheromone |
| 03 | `elastic_intent` | planned | elastic sizing as evaporating **intent** (operator-optional) |
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

## CI

`./ci_smoke.sh` runs the shipped demos Docker-free and asserts on their output (wired the same way
as the AFN fluid-pipeline smoke).
