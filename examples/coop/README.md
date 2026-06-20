# Food-Rescue Co-op — example suite

A cohesive set of runnable demos for Mycelium's newer capabilities — the **mailbox**, **governance
(management-as-intent)**, **autonomic provisioning**, **federation (AgentFacts)**, and the
**tuple-space** — composed in *one constructive world* rather than as isolated API toys.

> **The world.** A regional **food-rescue logistics co-op**: a network of **depot** nodes rescues
> surplus food (from markets, farms, bakeries) and routes it to community kitchens before it
> spoils. There is **no central dispatcher** — depots advertise capabilities, claim work when
> ready, and self-organise. A neighbouring co-op is a separate *domain* the federation demo talks to.

Full design + the six-example roadmap: [`docs/plans/example-suite.md`](../../docs/plans/example-suite.md).
**All six examples are shipped** — run them all Docker-free with [`ci_smoke.sh`](ci_smoke.sh).

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
| 04 | `provisioning` ⭐ | ✅ shipped | the full **autonomic loop**: buffer in a tuple-space lane while peers self-provision the missing capability |
| 05 | `federation_facts` | ✅ shipped | cross-domain edge discovery via self-certified AgentFacts |
| 06 | `rotation` | ✅ shipped | zero-disruption identity rotation; pre-rotation facts still verify |
| 07 | `consensus` | ✅ shipped | multi-bloc agreement via cross-group consensus + leased (decaying) decisions |

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

### 04 — `provisioning` ⭐ (the flagship)

```bash
cargo run -p mycelium-coop-examples --bin provisioning
```

The whole thesis in one process. A surge of donations needs a `route/optimize` capability **no depot
has yet**, so they **buffer in a tuple-space lane**. The worker declares the requirement (the demand);
a provider depot **self-provisions** the optimizer — a real WASM component pulled, content-verified,
and instantiated — advertises it, and serves it over RPC. The worker then drains the backlog: `take`
→ invoke the optimizer → `complete` to `done`. Finally the **active optimizer is killed**: its
capability evaporates, a second wave buffers, and a **standby self-provisions** to restore it —
*restart ≡ provisioning*. Both waves drain.

The WASM artifact is the committed `echo_component.wasm` fixture (it echoes its input — a
deterministic "optimized route"), so CI needs no wasm toolchain.

**Philosophy beat:** nothing predicted who would run the optimizer. It was **unmet demand** (a
pheromone), satisfied by a node **electing to provision**; the buffer lost no item; and the
rendezvous **self-healed** across a provider death — no coordinator anywhere in the loop.

### 05 — `federation_facts`

```bash
cargo run -p mycelium-coop-examples --bin federation_facts
```

**Two separate domains** (separate clusters, separate auto-CAs — they do *not* peer): our co-op
(`coop-a`, advertising `route/optimize`) and a neighbouring co-op (`coop-b`) with overflow. The
neighbour discovers our capability the way a NANDA-style quilt does — it **pulls our AgentFacts at
the edge** (`/.well-known/agent-facts.json`, served by the facts lens), a self-signed JSON-LD
document, and **verifies the signature itself**. It reads the capability list, decides to route
overflow to us, and — a tampered copy of the document fails verification.

**Philosophy beat:** discovery across a trust boundary with **no shared CA and no issuer authority**.
The facts are self-certified by the node identity; trust is the *fetcher's* decision (Core Principle
1). A Mycelium domain is a sovereign quilt-patch.

### 06 — `rotation`

```bash
cargo run -p mycelium-coop-examples --bin rotation
```

`depot-a` publishes a signed AgentFacts field, then **rotates its Ed25519 identity** mid-operation
(routine hygiene). Its peer `depot-b` keeps verifying across the rotation: (1) the field before the
rotation; (2) the **same old-key-signed field after the rotation** — it still verifies, because A's
`sys/identity/{a}` retains `new ‖ old` and every verify path tries the whole **retained key set**;
(3) a fresh field A signs with the new key.

**Philosophy beat:** key hygiene with **no disruption and no re-signing of history** — a retired key
stays verifiable for what it signed. This is the runnable form of the retained-key-set fix
(PR #51 / `crdt.rs::verify_any`).

### 07 — `consensus`

```bash
cargo run -p mycelium-coop-examples --bin consensus
```

A large donation spans two depot blocs (`north`, `south`) and accepting it commits *both* to
cold-chain capacity — so acceptance requires **each bloc to independently reach quorum**
(`cross_group_propose` over two `GroupQuorum`s). Phase 1 commits (both blocs agree); Phase 2 — adding
a third bloc with no voters — **times out** (no bloc can be coerced); Phase 3 commits a
**short-leased** decision that **decays**, so the slot reads back as reopened.

**Philosophy beat:** Layer III — an emergent coordinator (proposer + quorum) that exists only for the
decision and **dissolves once it commits**, riding ordinary signals on the same substrate. Commitments
are *promise-strength* (a bloc with no voters can't be bound), and decisions evaporate like any other
mandate (epoch-leased commit). "Complex societies do need coordinators; they emerge — they aren't the
starting point."

## CI

`./ci_smoke.sh` runs the shipped demos Docker-free and asserts on their output (wired the same way
as the AFN fluid-pipeline smoke).
