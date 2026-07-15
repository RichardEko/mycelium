# Mycelium

A **broker-less mesh runtime for AI agent fleets**, embedded as a Rust library. Agents discover
each other's capabilities, route tool calls, exchange events, and reach consensus — with **no
coordinator, central registry, or single point of failure**. State converges by gossip; work is
claimed, not dispatched; roles are discovered, not assigned.

## Hello, mesh — 30 seconds, no setup

```sh
cargo run --example hello_mesh
```

Two embedded agents on loopback: one writes a value, the other learns it **by gossip** — no
broker, no config, no LLM, no features to enable. That's Layer I, the shared KV store everything
else builds on. It's ~25 readable lines — start there: [`examples/hello_mesh.rs`](examples/hello_mesh.rs).

Then `cargo run --example hello_capability` ([source](examples/hello_capability.rs)) shows the value
proposition itself — one node advertises what it *does*, another finds it *by name* and calls it over
RPC, **no registry and no configured addresses**.

## Where next?

| You are… | Go to |
|---|---|
| **New here** — is this for me? which primitive? which demo? why-not-X? | the **[FAQ](docs/guide/faq.md)** — your map, and the intended first read |
| **Building a use case *on* Mycelium** | [Building on Mycelium](docs/guide/building-on-mycelium.md) — the integrator contract (dependency, public-API rule, reserved KV prefixes, a copyable `CLAUDE.md`) |
| **Wanting the guided depth** | the **[developer guide](docs/guide/README.md)** — 17 chapters, each with a runnable example |

> The rest of this page is a **short orientation** — what the system is, the demos, and a
> layers-at-a-glance table. Every deep dive lives one link away in the
> [guide](docs/guide/README.md) and [operations](docs/operations/README.md) docs.

## What it is

Mycelium is three layers: a broker-less gossip KV store (Layer I), an ephemeral
scoped event mesh (Layer II), and an opt-in consensus overlay (Layer III). The
capability system sits across all three layers and provides broker-less service
discovery. Four application patterns build on this substrate: Skills (LLM agents
as mesh nodes), MCP tool discovery (LLM finds tools dynamically from the KV
store), fluid pipelines (Agentic Flow Networks), and A2A interop (LangChain /
AutoGen). Built on TCP epidemic propagation with last-write-wins conflict
resolution; each agent chooses its own payload serialisation.

### Which crate? — `mycelium` vs `mycelium-core`

The project is a Cargo workspace of two published crates. **Most users want `mycelium`** —
the full runtime. Reach for `mycelium-core` only for a minimal embed.

| | `mycelium` | `mycelium-core` |
|---|---|---|
| **Layers** | I + II + III (gossip KV, signal mesh, consensus, capabilities, services, gateway, TLS) | I + II only (gossip KV + signal/boundary mesh) |
| **Dependency tree** | ~140 crates (pulls in Axum/hyper for the HTTP gateway) | ~50 crates — no axum/hyper/gateway |
| **Use it when** | You need RPC, consensus, the capability system, the HTTP/MCP/A2A gateway, or RBAC/audit | You only need last-write-wins KV propagation + the scoped event mesh, on bare-metal / size-constrained / no-gateway targets |

```toml
# Full runtime (default):
mycelium = { version = "…", features = ["tls"] }

# Minimal substrate embed (Layers I + II, no gateway):
mycelium-core = "…"
```

`mycelium` re-exports everything in `mycelium-core`, and a `mycelium-core` node still *forwards*
all traffic (including consensus frames) — it just never acts on the higher layers. You can also
trim `mycelium` itself toward the core with `default-features = false` (drops the gateway) and
`--no-default-features --features gateway` (drops consensus). The split landed in v2.0 M1 — see
[ROADMAP §v2.0 Milestones](ROADMAP.md) for the rationale. Two coordination companion crates are
built entirely on the public API: [`mycelium-tuple-space`](mycelium-tuple-space/) (a pull-based
pipeline buffer — work routed by lane *position*) and
[`mycelium-blackboard`](mycelium-blackboard/) (shared working memory — facts claimed by *content*
predicate, competitive and exactly-once). Known stages → tuple space; emergent topology over shared
facts → blackboard.

---

## Skills vs MCP tools — in one line

> **MCP tool** = a *function* in the mesh (any language; the LLM calls it).
> **Skill** = an *LLM agent* in the mesh (TOML manifest, no code; callable by any node — including other skills).

They compose naturally; the full comparison and when-to-use guide is in
[guide ch. 00 — Concepts](docs/guide/00-concepts.md#reference--skills-vs-mcp-tools-choosing-the-right-primitive).

## Demos

The full, runnable set — each with an objective, setup, and a walkthrough — lives in
**[`examples/`](examples/README.md)** (start there; it has the shared setup and the doc index).
The highlights:

| Demo | What it shows | Start |
|---|---|---|
| **Mesh Control UI** — `llm_agent` | Capability emergence across 3 nodes + a live topology UI (11 presets, emergent manager election, simulated failover) | `MOCK_LLM=1 cargo run --example llm_agent --features metrics` → http://127.0.0.1:8100 |
| **Interactive Chat** — `three_node_demo` | Live MCP tool discovery — tools join a running mesh and the LLM finds them without a restart | [`examples/chat/`](examples/chat/README.md) |
| **Skills cluster** — SkillRunner | LLM agents as mesh citizens; skills as capabilities, live load-balancing | [`examples/community/`](examples/community/README.md) |
| **Food-Rescue Co-op** — 12 demos | The whole pattern catalogue in one constructive world | [`examples/coop/`](examples/coop/README.md) |
| **Agentic Flow Networks** | Tuple-space pull pipeline (stigmergic backpressure) vs a push baseline | [`examples/fluid_pipeline/`](examples/fluid_pipeline/README.md) |
| **Reasoning ladder** — LangGraph | Local checkpointer → cross-node deploy/reheal, 7 rungs | [`examples/langgraph/`](examples/langgraph/README.md) |
| **A2A interop** — LangChain / AutoGen | External agents auto-discover skills via `/.well-known/agent.json` | [`examples/a2a_langchain/`](examples/a2a_langchain/README.md) |
| **Consistency Overlay** — `three_node_demo` (overlay) | Consensus REST cluster + copy-paste Python scenarios | [`tests/overlay/`](tests/overlay/README.md) |
| **Conway's Game of Life** | The substrate itself — 256 gossip agents coordinate a 16×16 grid | `cargo run --example conway` |

Docker one-liners (no local setup): `make test-llm-agent` (11 scenarios, no Ollama) ·
`make test-llm-demo` (interactive chat, needs Ollama) · `make test-overlay` (3-node consensus).

---

## Build

```
cargo build --release
```

The fuzz harness (`fuzz/` — wire + capability decoders) needs nightly:
`cargo +nightly fuzz run wire_decode -- -max_total_time=60`.

## Run

Two nodes on one machine, no config:

```
cargo run -- --port 7946                          # bootstrap node
cargo run -- --port 7947 --peers 127.0.0.1:7946   # joins via the first
```

Type `set k v` / `get k` in either terminal and watch the other converge. The richer
interactive cluster (HTTP dashboards, MCP tools, chat) is
[`three_node_demo`](examples/chat/README.md).

## The system, layer by layer

Each row is a one-line orientation; the linked chapter carries the concept *and* the full
reference (API surface, observability, design notes) — one home per fact.

| Layer / subsystem | What it gives you | Depth |
|---|---|---|
| **Layer I — gossip KV** | Broker-less shared state: LWW + HLC, Merkle anti-entropy, WAL persistence | [ch. 01](docs/guide/01-gossip-kv.md) |
| **Layer II — signal mesh** | Ephemeral scoped events: unconditional forwarding, boundary-gated *action*, opacity & inhibition | [ch. 03](docs/guide/03-signals.md) |
| **Layer III — consensus** | Opt-in agreement: ballots/quorum, `consistent_set`/`consistent_get`, locks, leader election, durable log + consumer groups | [ch. 04](docs/guide/04-consensus.md) |
| **Capabilities** | Discovery by *what a node does*: advertise/resolve, schema registry, requirements & demand pressure, emergent groups, locality | [ch. 02](docs/guide/02-capabilities.md) |
| **Service layer** | RPC, bulk transfer, scatter-gather, actor mailboxes — on the mesh, no broker | [cookbook](docs/guide/cookbook.md#reference--the-service-layer-rpc-bulk-scatter-gather-mailbox) |
| **Skills & prompt skills** | LLM agents as mesh nodes (TOML manifests, composition) + LLM-backed capabilities in KV | [ch. 05](docs/guide/05-skills.md) |
| **Companions** | `mycelium-tuple-space` (pull pipeline by lane *position*) · `mycelium-blackboard` (claims by *content* predicate) | crate docs ([tuple-space](mycelium-tuple-space/) · [blackboard](mycelium-blackboard/)) |

## Security

mTLS peer admission (the real data-isolation boundary), Ed25519-signed consensus, RBAC +
tamper-evident audit (`compliance` feature), hot identity rotation. Posture + threat framing:
[guide ch. 09](docs/guide/09-security.md) · [threat model](docs/threat-model.md) · operator
runbooks under [`docs/operations/`](docs/operations/README.md) (rbac, sso, audit,
cert-rotation, crown-jewel).

## Operating it

Prometheus `/metrics`, dashboards, readiness, diagnostics, tuning (including the performance
baselines and the `GossipConfig` reference): start at the
[operations index](docs/operations/README.md) — notably
[observability](docs/operations/observability.md), the
[metrics reference](docs/operations/metrics.md), and [tuning](docs/operations/tuning.md).

## Language bridges

Python (`mycelium-py`) and TypeScript (`mycelium-ts`) clients drive a node through the embedded
gateway — capabilities, signals, RPC, KV, mailboxes, tuple space. [Guide ch. 10](docs/guide/10-language-bridges.md)
has the API tour; each package README has the quick start.

## How the project keeps itself honest

Mycelium runs an adversarial **self-audit system**: four LLM-assisted audits (code · internal docs ·
documentation coverage · external claims) plus deterministic CI gates — and each audit keeps a dated
**ledger of its own misses**, a verdict it once declared clean that later proved wrong. The
mechanisms cross-correct. For reviewers doing technical diligence,
[`docs/analysis/README.md`](docs/analysis/README.md) explains the system and links the real, in-repo
ledgers (candid by design — a self-audit is only worth the failures it records).

## Research & citation

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.20665238.svg)](https://doi.org/10.5281/zenodo.20665238)

Mycelium is the working implementation behind a published architectural argument. The lead paper:
R. Nicholson, *"The Coordinator Trap: Structural Scaling Liabilities in Mediated Multi-Agent
Architectures and a Substrate-Based Alternative,"* Tathata Systems Ltd, 2026 —
[doi:10.5281/zenodo.20665238](https://doi.org/10.5281/zenodo.20665238) (CC BY 4.0; source in
[`docs/publications/`](docs/publications/), reproducible at tag
[`paper-submission-v2`](https://github.com/RichardEko/mycelium/tree/paper-submission-v2)).

It is the lead of a **four-part corpus** (all CC BY 4.0; full read-order, dependency graph, and
DOIs in [`docs/publications/README.md`](docs/publications/README.md)):

- **Heterogeneous Local Knowledge Systems (HLKS)** — the cross-domain convergence argument
  (`mycelium-tuple-space` is its constructive evidence) — [doi:10.5281/zenodo.20813058](https://doi.org/10.5281/zenodo.20813058)
- **The Capture Problem** — power vs knowledge; closes the sequence — [doi:10.5281/zenodo.20813463](https://doi.org/10.5281/zenodo.20813463)
- **Monetary Ecology** — the MCB/P/S/Î evaluation framework the distributive argument draws on — [doi:10.5281/zenodo.20811062](https://doi.org/10.5281/zenodo.20811062)

## License

Mycelium is released under the [GNU Affero General Public License v3.0](LICENSE) (AGPL-3.0-only).

**Open use:** Any project distributed under a compatible open-source license may use Mycelium freely under the AGPL terms. Network-deployed applications using Mycelium must make their source available to users of that service.

**Commercial embedding:** Organisations that need to embed Mycelium in a proprietary product without the AGPL copyleft obligation can obtain a commercial license. Contact [tathatasystems@proton.me](mailto:tathatasystems@proton.me) to discuss terms.
