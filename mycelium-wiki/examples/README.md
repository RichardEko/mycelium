# Wiki companion — examples

## Objective

Two runnable demos for the **wiki companion**: a shared, gossip-replicated decision store that
agents *ground their answers in*. The wiki is built atop Mycelium **layer I** (gossip-KV) — a
curator drains proposals into the store (the single writer of record); every reader opens the store
and reads directly, **with no node and no curator**. Both demos show the same shape — *retrieve from
the shared wiki, then phrase a grounded, cited answer* — one as a CLI, one as a browser showcase over
a fleet of specialists. This is a **library, not a platform**: there is no service to stand up, only a
store the examples read.

## How to run

Both need only the **Rust toolchain**; `wiki_council_viz` optionally uses a **local Ollama** for
phrased answers. Install both via the [shared setup](../../examples/README.md#shared-setup) — this page
does not re-explain it. Each demo imports its constructive corpus (neighbourhood-council decisions:
transport, energy, planning, budget) into a fresh store on startup, then serves reads from it.

## `wiki_chat`

**Objective.** A CLI that imports a corpus of documents into the shared wiki, then answers questions
grounded in it. One binary serves both driving use cases unchanged — an organisation "twin" (a domain
canon a chat agent answers from) and a community council (decisions a resident navigates by chat) —
you point it at a different corpus directory, nothing else changes.

**How to run.**
```bash
cargo run -p mycelium-wiki --example wiki_chat --features llm
# import a corpus, then ask / chat against the shared store:
#   wiki_chat import --store DIR --group G --corpus mycelium-wiki/examples/corpus/council
#   wiki_chat ask    --store DIR --group G [--mock] "what was decided about the Elm Street bike lane?"
#   wiki_chat chat   --store DIR --group G [--mock]        # interactive REPL
```
`--mock` uses a deterministic **in-process echo backend** that returns the retrieved wiki context
verbatim — no network, no API key (this is what CI's `ci_smoke.sh` asserts against). For a real backend,
set `WIKI_CHAT_LLM_KEY` (plus optional `WIKI_CHAT_LLM_URL`, `WIKI_CHAT_LLM_MODEL`) to any
OpenAI-compatible endpoint — a local Ollama (see [shared setup](../../examples/README.md#shared-setup))
works as well as a hosted API. With no key and no `--mock`, it falls back to a trivial echo so the
pipeline still runs.

**What it demonstrates.** The two planes of the wiki, each used as intended: `import` is a **writer** on
the control plane — a curator drains proposals and applies them to the store — while `ask`/`chat` are
**readers** on the data plane, opening the store and reading in parallel with no node and no curator. The
reader's only extra dependency is an LLM to *phrase* the answer; retrieval is keyword-overlap over the
curated text (structured recall of the exact curated wiki, not embedding similarity), and the prompt
instructs the model to answer **only** from the wiki context and cite the section headings.

## `wiki_council_viz` ★

**Objective.** A **browser showcase** — a *live chat* over a fleet of four wiki-grounded specialists
(**Transport · Energy · Planning · Budget**) sharing one council wiki. A question fans out to the
relevant specialists, each answers grounded in the wiki with citations, and a synthesizer merges them
into one cited reply. It follows the [UI-example contract](../../docs/wiki/dev/ui-example-contract.md):
gateway + metrics on, an Ops Console back-link (advertised via the `ui/viz` KV key), and a "what you're
seeing" concepts box.

**How to run.**
```bash
cargo run -p mycelium-wiki --example wiki_council_viz --features gateway,llm,metrics
# → open http://127.0.0.1:8095/  (runs continuously; Ctrl-C to stop)
```
Runs offline with no cloud and no API key. For **phrased** answers, start a local model first —
`ollama serve` then `ollama pull llama3.2:1b` (override with `WIKI_COUNCIL_MODEL`); see
[shared setup](../../examples/README.md#shared-setup).

**What it demonstrates.** The LLM runs **locally, on the mesh**. At startup, if a local Ollama is serving
the model on `:11434`, the node registers it as an `llm/{model}` capability (`register_prompt_skill`), and
each specialist *phrases* its grounded answer over the mesh (`call_prompt_skill` → resolve the provider on
the capability ring → RPC). If Ollama is absent — or any call fails — the specialist falls back to
**deterministic grounded extraction** (the top wiki sentence, or the £ figure for the Budget analyst), so
the demo always runs and the answer is grounded in wiki records either way. Each specialist is a
**data-plane reader** (opens the shared store, no node/curator); in a distributed deployment each would be
a separate mesh agent routed by its `domain` capability — here they share one process so the whole fleet
is watchable in one dashboard. Point the [Ops Console](../../examples/README.md#ops-console) at
`127.0.0.1:9095` to see this node's gateway, KV, and live metrics.
