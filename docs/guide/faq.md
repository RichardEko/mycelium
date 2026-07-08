# Mycelium FAQ — start here

The rest of this guide is deep reference. This page is the **first read**: short
answers that point you at the one doc or example you actually need. If a question
here doesn't route you well, that's a docs bug — open an issue.

---

## Is Mycelium for me?

Mycelium earns its keep when you have **many agents (or nodes) that must
coordinate, and you don't want a coordinator** — no broker, no scheduler, no
control plane to run or scale or fail. State converges by gossip; work is claimed,
not dispatched; roles are discovered, not assigned. It embeds as a Rust library
inside your process — there is no daemon.

It is **probably overkill** if you just want to chain a few LLM calls, run one
orchestrator that fans out to workers, or coordinate through a database or queue
you already operate. Reach for a workflow engine (LangGraph, Temporal) or a broker
(NATS, Kafka) there — see *[Why not X?](#why-not-langgraph--temporal--nats--)* below.

The sweet spot: coordination **at scale, without a single point of control** —
fleets that partition and heal, edge/on-prem meshes, systems where "who is in
charge" must be emergent and recallable. The rationale is in
[`philosophy.html`](../philosophy.html) and [`ROADMAP.md`](../../ROADMAP.md)
("The Structural Inversion").

---

## Which primitive or companion do I want?

Everything is built on the `mycelium-core` substrate (KV + signals + consensus).
The companions are opinionated coordination patterns on top of the **public API**
— pick by the shape of your problem:

| Your problem | Use | Start with |
|---|---|---|
| Shared eventually-consistent state, events, opt-in strong consistency | **core substrate** | [`three_node_demo`](../../README.md#run) · guide [01](01-gossip-kv.md)/[03](03-signals.md)/[04](04-consensus.md) |
| Stage-by-stage work handoff, pull-based pipelines | **tuple-space** (position/coordination) | [`redistribution.rs`](../../mycelium-tuple-space/examples/redistribution.rs) · guide [07](07-pipelines.md) |
| One shared pool of facts many agents read & refine | **blackboard** (content) | [`microgrid.rs`](../../mycelium-blackboard/examples/microgrid.rs) |
| Durable, curated, queryable memory the fleet grounds on | **wiki** | [`wiki_chat.rs`](../../mycelium-wiki/examples/wiki_chat.rs) |
| Ship code/policy to where the data is | **wasm-host** (code mobility) | [`mycelium-wasm-host`](../../mycelium-wasm-host/README.md) |
| Cross-fleet identity & capability federation | **agentfacts** | [`mycelium-agentfacts`](../../mycelium-agentfacts/README.md) |
| LLM agents that self-organise by meaning, not addresses | **semantic coordination** | [`semantic_coordination.rs`](../../examples/semantic_coordination.rs) · guide [11](11-semantic-coordination.md) |

Two `mycelium` vs `mycelium-core` crate-choice questions are answered in the
[README](../../README.md#which-crate--mycelium-vs-mycelium-core).

---

## Where do I start — hello world?

1. **`cargo run --example hello_mesh`** — two agents share a value by gossip, no setup, no LLM.
   Read the ~25 lines ([`examples/hello_mesh.rs`](../../examples/hello_mesh.rs)): that's the whole
   Layer-I substrate everything else builds on.
2. Read guide [00-concepts.md](00-concepts.md) for the three-layer model and the
   sub-handle API (`kv()`, `mesh()`, `capabilities()`, `consensus()`).
3. Skim a runnable example close to your problem from the table above.

For an interactive two-node REPL (`set`/`get` keys by hand) see the README
[Run](../../README.md#run) section.

**Building a use case *on top* of Mycelium** (a coordinator, an agent fleet, your
own companion crate)? Go to **[Building on Mycelium](building-on-mycelium.md)** — the
integrator contract: the dependency, the public-API-only rule, which KV prefixes to
avoid, the invariants you must respect, and a copyable `CLAUDE.md` snippet for your
own agent-driven project.

---

## Which example maps to my problem?

| I want to see… | Example |
|---|---|
| A real gossip mesh doing visible work | [`conway.rs`](../../examples/conway.rs) (Game of Life on a 16×16 mesh) |
| An LLM agent using tools + skills over the mesh | [`llm_agent.rs`](../../examples/llm_agent.rs), [`three_node_demo.rs`](../../examples/three_node_demo.rs) |
| Invoking a named skill hosted on another node | [`invoke_skill.rs`](../../examples/invoke_skill.rs) |
| Agents self-organising by meaning | [`semantic_coordination.rs`](../../examples/semantic_coordination.rs) |
| **Proof** the coordination is really coordinator-free | [`coordinator_comparison.rs`](../../examples/coordinator_comparison.rs), [`three_arm_workdist.rs`](../../examples/three_arm_workdist.rs) |
| A full companion worked example | [`redistribution.rs`](../../mycelium-tuple-space/examples/redistribution.rs) (tuple-space), [`microgrid.rs`](../../mycelium-blackboard/examples/microgrid.rs) (blackboard), [`wiki_chat.rs`](../../mycelium-wiki/examples/wiki_chat.rs) (wiki) |

---

## Why not LangGraph / Temporal / NATS / …?

Short version — Mycelium is a **substrate**, not a framework, and the distinction
is the coordinator:

- **LangGraph / AutoGen** — great for a *defined* graph of steps driven by one
  process. Mycelium has no central graph: topology is emergent and survives the
  loss of any node, including "the orchestrator."
- **Temporal / durable workflow engines** — give you a durable *scheduler you
  operate*. Mycelium removes the scheduler; there is nothing central to run.
- **NATS / Kafka / a broker** — a broker *is* the coordinator (and the thing you
  scale and page on). Mycelium's mesh is the bus, registry, and scheduler at once,
  with no broker process.
- **Erlang/OTP, Akka** — closest in spirit (supervision, location transparency),
  but still a managed cluster with explicit process addressing. Mycelium adds
  receiver-side signal boundaries, gossip-convergent state, and roles discovered
  by capability rather than addressed by PID.

If your problem *has* a natural coordinator and you're happy running it, one of
the above is likely the simpler choice. Mycelium is for when you specifically
don't want that dependency.

---

## Common gotchas

- **Call `shutdown()`.** Companions with background curators/loops (e.g. the wiki)
  hold task cycles; drop alone won't stop them. See
  [14-patterns-and-pitfalls.md](14-patterns-and-pitfalls.md).
- **The gateway has no auth by default.** The HTTP gateway is for trusted networks
  unless you put mTLS / a proxy in front — see guide [09-security.md](09-security.md)
  and [operations](../operations/README.md).
- **Rolling upgrades are wire-version gated.** One version step per rollout
  (`WIRE_VERSION`/`PREV_WIRE_VERSION`); mixed clusters spanning two steps won't
  talk. See guide [13-cluster-topology.md](13-cluster-topology.md).
- **Opacity/load is emergent, not commanded.** You don't "mark a node down"; nodes
  advertise load and peers route around it. See [00-concepts.md](00-concepts.md).
- **Consistency is opt-in.** `kv()` is eventually consistent; reach for
  `consensus()` only where you actually need linearisability (guide
  [04-consensus.md](04-consensus.md)).

More failure modes: [14-patterns-and-pitfalls.md](14-patterns-and-pitfalls.md) and
[error-handling.md](error-handling.md).

---

## How do I build, test, and run?

- **Build:** `cargo build --release` (a `--no-default-features` build drops the
  gateway; see [Cargo features](../../README.md)).
- **Pre-push gate:** `make check` (clippy across the CI feature matrix, ~3 min);
  `make check-full` adds the test suites.
- **Run a mesh:** README [Run](../../README.md#run) section.
- **Task recipes** ("how do I do X?"): [cookbook.md](cookbook.md).

---

*Deeper than this page:* the numbered guide chapters ([README](README.md)) for
reference, [`docs/operations/`](../operations/README.md) for running it in
production, and [`docs/wiki/`](../wiki/wiki.md) for the maintainer-facing synthesis.
