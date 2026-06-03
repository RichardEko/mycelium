# Changelog

All notable changes to this project will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

---

## [1.0.0] - 2026-06-03

### Added

**Layer I — Gossip KV store**
- Last-write-wins key-value store propagated over TCP gossip
- Hybrid Logical Clock (HLC) causal ordering for all writes
- Anti-entropy sync: nodes reconcile state on reconnect
- Per-key TTL with lazy expiry
- Write-ahead log (WAL) + snapshot persistence; configurable sync modes (none / sync / flush)
- Prefix-based subscriptions with optional predicate filtering

**Layer II — Signal mesh**
- Ephemeral scoped signals with epidemic flood delivery
- Pheromone-style opacity composition: any `sys/load/{node}/...` key with `is_opaque=true` gates signal reception
- Signal scopes: `Node`, `Group`, `Global`, `Groups`
- Dedup via nonce; TTL-bounded forwarding

**Layer III — Epidemic consensus**
- Group-scoped, system-scoped, and cross-group proposals
- `GroupQuorum` for multi-voting-bloc decisions with independent per-group quorum fractions
- Epidemic proposal flood; no coordinator; no external Raft dependency

**Capability and discovery subsystem**
- Node-level `provides` / `requires` capability advertisement via `cap/` KV prefix
- Emergent group membership: nodes self-join groups based on local capability evaluation
- Locality-aware capability resolution with ranking and topology policies
- Group-level opacity and demand pressure tracking
- Inter-group wiring resolved per-emission (`signal_wired_via`)
- Filter opacity watcher with debounce

**Agent state machine**
- `GossipAgent` public API: KV, signals, consensus, capabilities, consistency overlay, sharding
- HTTP management gateway with SSE streaming
- RPC (`rpc_call` / `rpc_respond`), scatter-gather, Actor/Event mailboxes
- Cluster sharding (`shard_for` / `emit_sharded`)

**Consistency overlay (opt-in)**
- `consistent_set` / `consistent_get` — linearisable read-modify-write over gossip KV
- `distributed_lock` — named mutex with TTL-based lease
- `elect_leader` — leader election per named group
- `append` / `scan_log` / `compact_log` / `subscribe_log` / `subscribe_log_group` — ordered durable log with consumer-group cursors

**`--features tls`**
- mTLS peer connections using `tokio-rustls`
- Ed25519 node identity; keypair stored in `sys/identity/{node}`
- Consensus payload signing (`SignedConsensusMsg`)
- `WireMessage::SignedData` for Ed25519-signed KV writes (wire v10)

**`--features metrics`**
- Prometheus scrape endpoint at `/metrics`
- 10 counters, gauges, and histograms covering KV operations, gossip fan-out, signal delivery, consensus rounds
- Grafana dashboard at `dashboards/mycelium-grafana.json`

**`--features a2a`**
- A2A protocol adapter: `/.well-known/agent.json`, `/a2a` JSON-RPC endpoint
- Python and TypeScript `A2aClient`

**`--features llm`**
- Prompt Skills: `PromptTemplate` stored in KV, cross-node invocation via `call_prompt_skill`
- SkillRunner: `.skill.toml` capability-as-skill, OpenAI-compatible LLM driver
- HLC audit trail and OpenTelemetry tracing in SkillRunner
- MCP bridge: server-side tool discovery and routing; client-side tool consumption
- `OpenAiBackend` / `EchoBackend`

**Language bridges**
- Python sidecar bridge (local HTTP, ~1 ms overhead) — see `examples/fluid_pipeline/` and `examples/a2a_langchain/`
- TypeScript sidecar bridge — 28 methods, SSE streaming, full overlay and A2A coverage

**Examples**
- `examples/fluid_pipeline/` — Agentic Flow Networks demo: 10-worker fluid pool, KV ring as distributed buffer, 4-stage article pipeline, PostgreSQL sink. Run with `docker compose up --build --scale worker=10`.
- `examples/a2a_langchain/` — LangChain ReAct agent and AutoGen v0.4 agent auto-discovering Mycelium skills via `/.well-known/agent.json`
- `examples/community/` — 3-node demo cluster with orchestrator, researcher, verifier, and writer skills

**Wire protocol**
- Wire v10 with rolling-upgrade compatibility window (PREV = v9)
- Bincode-encoded framing; version negotiation on every peer connection
