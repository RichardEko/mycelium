# 15 · Reasoning & LangGraph

The other chapters are about **coordination** — who talks to whom, shared state,
discovery, consensus. This one is about the axis on top of it: **authoring the LLM
reasoning that rides on the mesh**, and making a reasoning framework run *on* Mycelium
rather than beside it. It is grounded in a runnable ladder — [`examples/langgraph/`](../../examples/langgraph/) —
that climbs from a one-line LangChain call to a graph whose model dependency survives a
node failure. The companion crate is [`mycelium-reason`](../../mycelium-reason/); the
strategy behind it is [`docs/plans/mycelium-reason.md`](../plans/mycelium-reason.md).

Two questions this chapter answers:

- **"Why not just run LangGraph on Postgres/Redis?"** Because on Mycelium the same
  coordinator-free properties that make coordination resilient make *reasoning* resilient:
  inference routed with no central proxy, tamper-evident causal traces of the whole fleet's
  thinking, and graph state whose **model dependencies follow it across nodes**. A
  single-process framework on a commodity store cannot express the last one — and it is the
  strongest reason to adopt.
- **"How do I author reasoning on the mesh pleasantly?"** Through three thin surfaces —
  routed inference, typed output, and a LangGraph checkpointer — that reuse the substrate
  you already have. The Rust core takes **zero** changes; it is all companion-crate and
  `mycelium-py`.

> **Different layer from chapter 08.** [08 · A2A interop](08-a2a-interop.md) is LangChain
> *calling* Mycelium skills as tools (Mycelium is the tool provider). This chapter is the
> reverse and deeper: **LangGraph runs *on* Mycelium**, its graph state backed by the mesh
> (Mycelium is the resilient state backend). They teach different things — don't conflate
> them. The full four-touchpoint map is at the end of this chapter.

---

## The ladder

Each rung is a runnable script under [`examples/langgraph/`](../../examples/langgraph/) that
teaches one idea and builds on the rung below. Rungs 0–5 are deterministic (they run against
an `EchoBackend` model, so the "model" just echoes — the point is the wiring, not the prose)
and run in CI; rung 6 is the flagship.

| Rung | Teaches | Run |
|---|---|---|
| [0](../../examples/langgraph/00_hello_skill.py) | a Mycelium skill *is* a LangChain `Runnable` | `python examples/langgraph/00_hello_skill.py` |
| [1](../../examples/langgraph/01_typed.py) | typed (schema-validated) output through the mesh | `python examples/langgraph/01_typed.py` |
| [2](../../examples/langgraph/02_durable_state.py) | LangGraph state is durable in the mesh | `python examples/langgraph/02_durable_state.py` |
| [3](../../examples/langgraph/03_cross_node.py) | any node resumes any thread | `python examples/langgraph/03_cross_node.py` |
| [4](../../examples/langgraph/04_routed.py) | load-aware routed inference + failover | `python examples/langgraph/04_routed.py` |
| [5](../../examples/langgraph/05_traces.py) | replayable fleet-reasoning traces | `python examples/langgraph/05_traces.py` |
| [6](../../examples/langgraph/06_deploy_reheal.py) | **the model follows the thread across node death** | `python examples/langgraph/06_deploy_reheal.py` |

Rungs 0–5 need a running node — the [`reason_node`](../../mycelium-reason/examples/reason_node.rs)
example is the fixture the CI drives:

```sh
# terminal 1 — a node serving llm/fable-mini (EchoBackend) with the gateway
BIND_PORT=7101 HTTP_PORT=8101 BLOB_DIR=/tmp/reason-a \
  cargo run -p mycelium-reason --features llm,gateway --example reason_node

# terminal 2 — the rung, pointed at it
MYCELIUM_TEST_PORT=8101 python examples/langgraph/04_routed.py
```

Rung 3 needs a second, mesh-joined node (`MYCELIUM_TEST_PORT_B`); rung 6 manages its own
nodes. Each rung skips cleanly (prints a note, exits 0) when its env vars are unset, so the
suite is safe to run anywhere.

---

## Walking the rungs

**Rung 0 — a skill is a `Runnable`.** The whole ladder rests on one fact: a routed skill
call wraps into a LangChain `RunnableLambda`, so it composes into any LangChain pipeline.
No agent, no tool-selection, no LLM reasoning loop — that is the a2a example's job. This is
the primitive underneath it.

**Rung 1 — typed output through the mesh.** [`mycelium.call_typed`](../../mycelium-py/src/mycelium/typed.py)
wraps a *through-the-mesh* prompt-skill call with a pydantic contract and a
validation-feedback retry loop: it extracts the first balanced JSON object from the output
(LLMs wrap JSON in prose), validates it, and on failure retries with the error handed back
in `context["validation_feedback"]`. When you talk to a provider *directly*, use the adopted
libraries instead — [Instructor](https://python.useinstructor.com/) / Pydantic AI; this
helper is specifically for skills resolved and called *over the mesh*.

**Rung 2 — durable state.** Compile a LangGraph `StateGraph` with
[`MyceliumCheckpointSaver`](../../langgraph-checkpoint-mycelium/) and the graph's checkpoints
live in the mesh. The rung proves it by re-instantiating a *fresh* client against the same
node and re-reading the checkpoint — the state outlived the client. Storage follows the
substrate's grain: **index rows in gossiped KV** (`ckpt/`/`ckptw/`, metadata inline so
`list()` filters without fetching payloads), **payloads in a content-addressed blob tier**
(one blob per channel value, so an unchanged value dedups across super-steps for free).
Never checkpoint blobs into KV — KV floods every node and is size-gated.

**Rung 3 — any node resumes any thread.** Checkpoint a run partway on node A, then resume it
on node B. The index rows gossip A→B; the payload blobs are fetched from whichever peer holds
them. The rung waits for convergence with a bounded structural poll (read-your-writes holds
only against the *same* node's gateway; a cross-node reader polls until the head has gossiped
in — an honest consequence of eventual consistency, not a bug).

**Rung 4 — routed inference.** `POST /gateway/reason/route` (and `ReasonClient.route`) route
each call to a healthy `llm/{model}` provider and fail over down a ranked candidate list.
This is a *real routing layer*, not a byproduct of resolution: capability resolution is
deliberately **load-blind** (it ranks by freshness/attributes/locality), so the router adds
what the substrate leaves out — *resolve → drop dead/opaque nodes → rank by pheromone fill →
fail over.* Contrast `/gateway/llm/call`, which resolves one provider and does a single RPC
with no failover.

**Rung 5 — fleet-reasoning traces.** Pass a `run_id` to a routed call and the route decision
and each attempt are recorded to the event-log overlay (`reason/{run_id}/{node}`, one
substream per writer so same-millisecond writes from two nodes don't collide). `trace(run_id)`
replays and narrates it — a causal, replayable account of *why the fleet reasoned as it did*,
readable from any node. Under the `compliance` feature the running trace hash can be anchored
into the tamper-evident WS2 audit chain.

---

## The three differentiators (`mycelium-reason` Tier 3)

Rungs 4–6 surface the three things a single-process framework structurally cannot offer:

1. **Capability-routed inference** — inference across the fleet with no central proxy (vs a
   LiteLLM proxy you operate and that can fail). See rung 4.
2. **Fleet-reasoning traces** — tamper-evidenceable causal traces of the *whole fleet's* run,
   not one process's spans. See rung 5.
3. **Artifact-aware resume** — the flagship. A resumed graph's *model dependencies follow
   it*: the node picking up a suspended thread declares `require_model`, and the model streams
   in where the thread landed. See rung 6.

The convention that ties them together: **a served model is a prompt skill** — capability
`llm/{model}` via `serve_model`, plus a parallel attributed `llm-meta/{model}` ad (context
window, family) for constraint-based routing.

---

## The flagship — deploy/reheal (rung 6)

This is the story that beats a commodity checkpoint store on non-commodity terms. It is also
the concrete answer to the [FAQ's positioning](faq.md#why-not-langgraph--temporal--nats--)
— *"Mycelium survives the loss of any node, including the orchestrator"* — shown rather than
asserted: run a LangGraph graph **on** Mycelium and its reasoning outlives the death of the
node it ran against. The choreography, all of it real substrate machinery:

1. A LangGraph graph whose LLM node calls the mesh via the routed endpoint runs on **node A**
   (which serves the model), reaches an interrupt, and **checkpoints** — state gossips, the
   payload lands in the blob tier.
2. The checkpoint **replicates A→B** (the driver waits for convergence — once it's replicated,
   the failure is survivable).
3. **Node A is killed.**
4. **Node B** — which was *not* serving the model — reheals: `require_model` declares the
   demand, the model artifact **streams in over the mesh** (content-addressed, SHA-256
   verified), and on arrival `serve_model` **bridges** it into a live, mesh-invocable skill.
5. The driver resumes the thread via **B**; the graph's routed LLM call now lands on B —
   *the model followed the thread.* Postgres and Redis cannot express step 4.

```sh
# self-contained — it starts and kills its own nodes
python examples/langgraph/06_deploy_reheal.py
# → ✓ checkpointed on A … ✓ node A down … ✓ model rehealed on B … ✓ resumed on B … FLAGSHIP OK
```

### The lesson the flagship taught the router

The first cut of this demo was slow and rigged, and finding out *why* produced a real
substrate improvement — the kind of thing a flagship is *for*. A killed node lingers in the
**capability-freshness** view for ~90 s (it stops re-advertising, but there is no instant
tombstone), and a mesh RPC to a dead peer has no fast connection-refused — so routing to it
burned the full per-attempt timeout, and a dead *lower-id* node poisoned every post-kill
route. The fix, in [`InferenceRouter`](../../mycelium-reason/src/route.rs):

- **Liveness filter** — route only to nodes SWIM currently believes are alive
  (`GossipAgent::peers()`, plus self). A departed node drops an order of magnitude faster
  than the freshness window. (Regression: `liveness_filter_drops_a_non_peer_cap`.)
- **Fast failover** — `RouterConfig::failover_timeout` (default 8 s) caps non-final attempts;
  only the last/lone candidate gets the full `call_timeout`, so a candidate that died inside
  the detection window costs ~8 s to skip, not 30 s, and a genuinely-slow *sole* provider is
  never cut off.

The general lesson for any reheal deployment: **route to live members, and fail over fast** —
don't spend the inference budget on a node that might be dead.

### Echo vs. a real model

The CI flagship uses `EchoBackend`, so it is deterministic and needs no GPU — it proves the
*seam* (require_model → mesh fetch + verify → `serve_model` bridge → routed resume). The
**Ollama variant** ([`examples/coop/src/bin/reheal_deploy.rs`](../../examples/coop/src/bin/reheal_deploy.rs),
manual) swaps the echo fixture for a **real GGUF** streamed through the artifact library's
`BlobRuntime` and served via a local-Ollama backend — the same seam, real weights, real
tokens generated on the survivor. It is manual (needs Ollama + a model file) and excluded
from CI, exactly like [`model_deploy`](../../examples/coop/src/bin/model_deploy.rs); see that
binary's docs for the artifact-library half. Honest single-machine caveat: A and B share one
local Ollama daemon, so on one host it is the *streamed bytes + the Mycelium capability* that
follow the thread; give each node its own Ollama for the true multi-machine story.

---

## The one integration map (anti-scatter)

Mycelium × LangChain/LangGraph is **one** coherent story with four touchpoints on different
layers — not several look-alike "LangChain examples." Pick by what you're doing:

| Touchpoint | Direction | Use when |
|---|---|---|
| [`examples/a2a_langchain/`](../../examples/a2a_langchain/) (chapter 08) | LangChain → Mycelium | a LangChain/AutoGen agent should *call Mycelium skills* as tools |
| [`langgraph-checkpoint-mycelium`](../../langgraph-checkpoint-mycelium/) (rungs 2/3/6) | LangGraph **on** Mycelium | a graph should *survive node loss and hand off across the fleet* |
| [`mycelium-reason`](../../mycelium-reason/) (rungs 4/5/6) | substrate-native | you want routed inference, fleet traces, artifact-aware resume |
| [`mycelium.call_typed`](../../mycelium-py/src/mycelium/typed.py) (rung 1) | through-the-mesh | schema-validated output from a *mesh-routed* skill (Instructor/Pydantic AI for direct calls) |

---

## Honest limits

- **Deterministic rungs use echo.** Rungs 0–5 prove wiring, not model quality; a real model
  is the Ollama variant / the a2a example.
- **Blobs ≤ 8 MiB (v1).** The mesh blob fetch rides a single RPC frame; chunked transfer via
  `bulk_call` is the named follow-up (the artifact library's `BlobRuntime` already streams
  large models by ranged reads — that is the Ollama variant's path).
- **Gossip-eventual metadata.** Cross-node reads see a checkpoint once its index row has
  gossiped in; read-your-writes holds only against the same node's gateway.
- **Reserved prefixes.** The checkpointer owns `ckpt/`/`ckptw/`; `mycelium-reason` owns
  `log/reason/` and the `reason/blob-cache` capability — treat them as reserved
  ([Building on Mycelium](building-on-mycelium.md)).

---

**Next:** back to the [guide index](README.md), or [08 · A2A interop](08-a2a-interop.md) for
the other direction (LangChain calling Mycelium). The full strategy and its code-verified
design decisions are in [`docs/plans/mycelium-reason.md`](../plans/mycelium-reason.md) and
[`docs/plans/mycelium-reason-examples.md`](../plans/mycelium-reason-examples.md).
