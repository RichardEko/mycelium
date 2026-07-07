# Changelog

All notable changes to this project will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added
- **The artifact library** (`mycelium-wasm-host`; design record
  [`docs/design/artifact-library.md`](docs/design/artifact-library.md)): a durable origin tier for
  content-addressed artifacts — `FsLibrarySource` (blob dir + signed `Manifest`;
  `Manifest::append_entry` is the one-call CI publish step), the **librarian** role
  (`spawn_librarian`: serve + `artifact/librarian` discovery + signature-scoped
  manifest→catalogue reconcile), `MeshArtifactSource::resolving` (holders discovered via the
  capability ring — no hardcoded node-ids), and the HTTP object-store source
  (`BlobFetcher` / `PrefetchingSource` / `HttpLibrarySource`, egress-gated before dispatch).
- **Artifact kinds + node runtimes**: `ArtifactKind` (WasmComponent | Blob) in a clean-slate
  versioned entry encoding; `ArtifactRuntime`/`Installed` traits — `WasmHost` becomes the engine
  inside one runtime; `BlobRuntime` places models/data (ranged streaming via
  `RangedArtifactSource`, complete-or-absent placement, activation hook, pluggable probe). The
  `Provisioner` gains a kind registry, async install reservations, **resource-aware
  eligibility** (signed per-entry `requires{disk,mem}`, `ResourceProbe` + headroom fraction,
  in-flight reservations counted), real `{ns}/loading` percent tiers, and a per-round **probe
  health pass** (fail → withdraw → reinstall).
- **Provenance binds the whole entry** (version‖kind‖artifact‖requirements‖capability): a signed
  artifact cannot be re-labeled under a different capability or kind, and resource requirements
  are tamper-evident (cost hints remain unsigned ranking inputs).
- **Examples**: `catalog` reworked honest (runtime-read library origin, librarian, origin death →
  peer-cache install); `mcp_toolgrowth` now installs real arriving code (new committed
  `unit_convert_component` fixture; activation-vs-installation taught explicitly); new **manual**
  `model_deploy` demo — real GGUF weights **and** their deployment profile as two signed
  artifacts (profile → weights by content address, design §4.3.1), activated into Ollama,
  generating real tokens under the governed profile.
- **Typed install errors**: `InstallError` is now an enum
  (`Fetch | Verify | Place | Activation | Resources | Host`) with a stable `stage()` label —
  callers match on cause instead of parsing strings.
- **Operator-visible artifact tripwires**: the provisioner/librarian emit
  `mycelium_artifact_*` counters through the `metrics` facade (ineligible-skip reasons,
  install started/completed/failed-by-stage, probe withdrawals, librarian publish/tombstone).
- **The CI flake tier** (`scripts/ci-retest.sh`): socket-binding suites re-run only their
  failed tests once — a deterministic failure still reds the build (fails twice); a flake
  keeps it green but emits a loud annotation (a bug report, never silence).

### Fixed
- `mycelium-wiki` integration tests: the `free_port()` bind-race flake class retired
  (pair-granularity bind retries; `AddrInUse` CI failure 2026-07-07).
- `crossbeam-epoch` 0.9.18 → 0.9.20 (RUSTSEC-2026-0204; bench-only dependency path).

## [2.0.0] — 2026-07-04

The **v2.0 epoch** — all 16 milestones (M1–M16) shipped and signed off
([`docs/plans/v2.0.md`](docs/plans/v2.0.md)), plus the companion-crate ecosystem. First release since
`v1.2.0`. All workspace crates version in lockstep at `2.0.0` (a unified release train); the companion
crates (`mycelium-wiki` / `-blackboard` / `-tuple-space` / `-agentfacts` / `-wasm-host`) are newer than
the substrate and their APIs may still evolve within the 2.x line — pin exact versions if that matters.

### ⚠ Breaking changes (from `v1.2.0`)

- **Wire protocol `v10` → `v12`.** `v11` added `hlc_seq` to `Signal` (ordered delivery); `v12` replaced
  the full key→timestamp anti-entropy index with a fixed-width Merkle **`bucket_hashes`** digest. A
  `v1.2.0` node **cannot** interoperate with a `2.0.0` node except across the rolling-upgrade window —
  `read_frame` accepts `WIRE_VERSION = 12` **and** `PREV_WIRE_VERSION = 11` only.
- **Workspace restructure** — the substrate (Layers I+II) was extracted into a separate `mycelium-core`
  crate. `mycelium` re-exports an unchanged public Rust API, but consumers reaching internal module
  paths must update; and `consensus` (Layer III) is now behind a feature gate.
- **Serialization dependency** — the wire/WAL codec is the in-tree hand-rolled fixed-int codec
  (`mycelium-core::codec` / `serde_fixint`), **byte-identical** to the former `bincode`; `bincode` is
  fully removed from the dependency tree (RUSTSEC-2025-0141 no longer applies). This is *not* a wire or
  on-disk format change — persisted WAL/snapshots decode unchanged.

### Migration

- **Rolling upgrade is one step only.** A mixed `v11 ⇄ v12` cluster converges (a `v11` anti-entropy
  request downgrades to a full snapshot). Upgrading from `≤ v9` (i.e. `v1.2.0` and earlier) is a
  **stop-the-world** upgrade: drain, stop the cluster, deploy `2.0.0`, restart. WAL/snapshots persisted
  by the former codec load without migration.
- **No public API removals** in `mycelium` itself; new capability arrives behind additive feature flags.
  Before a production go-live, walk [`docs/operations/production-readiness.md`](docs/operations/production-readiness.md);
  for a first customer engagement, [`docs/operations/customer-pilot.md`](docs/operations/customer-pilot.md).

### Added

- **`mycelium-wiki` companion crate** — a group-scoped, LLM-curated wiki: the durable, curated third coordination primitive (long-term-memory sibling of the blackboard's working memory), built on the public `mycelium` API only. **Control-plane / data-plane**: the corpus lives in a node-independent pluggable store (the `WikiStore` trait + an `FsStore` reference impl — manifest-last, torn-read-safe); a single elected **curator** serialises writes while group agents **read the store directly, in parallel** (no curator on the read path). Curator **election + ring-failover** on the capability ring; an evaporating KV **proposal queue** (`wiki/{group}/proposal/`); a **single-writer reconcile** that groups proposals by section (`DirectReconciler` lossless append-merge, or `LlmReconciler` 3-way merge behind `llm`); a **change-driven lint** loop (structural dead-cross-link/empty-section checks always on, LLM self-consistency behind `llm`; runs only after a write); **MCP tools** (`wiki.read`/`query`/`propose`) and an **HTTP gateway** (`/gateway/wiki/*`, feature `gateway`) with Python/TS `Wiki` SDKs; and a membership-gated **access broker** (`Wiki::request_store_access` → `StoreGrant`, RPC point-to-point). `Wiki::shutdown` reclaims the curator's background tasks. Features `control-plane` / `llm` / `gateway`; cross-node `tests/{failover,gateway,access}.rs` + the `wiki_chat` worked example (`ci_smoke.sh`, both use-case corpora). Audited in analysis Run 32 (one Major finding found + fixed same-session). Design/plan: `docs/plans/mycelium-wiki.md`; companion page `docs/wiki/dev/companions/wiki.md`.
- **Legible Emergence — coordinator-free fleet diagnosability** — make an emergent, coordinator-free fleet debuggable by a *non-designer* (Detect → Localize → Explain → Intervene) without a central collector: diagnostics are computed from each node's locally-held KV + HLC causal order + a **bounded** scatter-gather fan-out (`EXPLAIN_MAX_FANOUT`, with the skipped set *named* as `not_queried`, never silently dropped). Emergent tripwires + counters, a fleet snapshot, causal-order reconstruction, a fleet-state narrative, and the operator surface: `GET /gateway/explain` + `/gateway/diagnose` and the `make check` / `make check-full` pre-push gates. Phases 0–5. Plan: `docs/plans/legible-emergence.md`.
- **`mycelium-blackboard` companion crate** — content-routed shared working memory (the peer of the tuple space, routing by a **predicate over fact attributes** rather than lane position): `claim(predicate)` is a competitive, non-blocking destructive claim (Linda's `in`), `read`/`rd` is shared, `ack` is the idempotent terminal, `release`/deadline re-queues (at-least-once). `BoardStore` (WAL, magic `MBBWAL`) + `Blackboard` (roles + RPC + failover, `Post`/`Ack`-only replication). HTTP gateway `/gateway/bb/*`, Python/TS SDKs, the community-microgrid worked example. WS-G / G3. Plan: `docs/plans/v2-wsg-g3-blackboard.md`.
- **`mycelium-tuple-space` companion crate** — Linda-style pull-based pipeline buffer as a workspace member, built entirely on the public `mycelium` API (the composability proof: zero core changes were needed). Workers `take()` when ready, so readiness is self-announcing and the push-predict staleness/misroute failure mode does not exist. Single-lock store hot path; WAL durability with 4 record types (`Complete` is one indivisible record so a stage transition can never half-replay) and epoch'd compaction; `TupleRole::{Primary, Secondary, Auto, Client}` with secondary mirroring via replicate RPCs, heartbeat Signal, and promotion when the primary's capability evaporates; `Auto` elects with a lowest-candidate-id tie-break. Owns the `tuple/inflight/{ns}/{id}` and `sys/tuple/{node}/{ns}/…` KV prefixes. HTTP gateway (`/api/tuple`), Python and TypeScript SDKs, integration scenario 13. Design doc: `docs/plans/mycelium-tuple-space.md`.
- **TupleSpace WAL format header** — every tuple-space WAL now opens with `MTSWAL` magic + u16 LE version (v1). A file with a newer format version, or without the magic, is refused at open **byte-untouched** with an error naming both versions; previously an unrecognised record kind read as a torn tail and was silently truncated — an upgrade data-loss hazard. The header survives compaction, and the secondary replay-chunk cursor clamps past it. Format break is free: no earlier WAL shape was ever in a release.
- **HLC remote clock-drift bound** — `GossipConfig::max_clock_drift_ms` (also `GOSSIP_MAX_CLOCK_DRIFT_MS`; default 300 000 ms = 5 minutes, `0` disables). `Hlc::observe` now clamps remote physical time to `wall_now + bound`, with a rate-limited `warn!` naming the offending drift when the clamp engages. Previously one peer with a far-future clock (NTP failure, or hostile in a non-TLS cluster) dragged every node's HLC forward irrecoverably — the `max` never decays — and read-side evaporation, the substrate's failure detector (including tuple-space secondary promotion), was *silently suspended for the full drift duration*. The cited Kulkarni et al. 2014 HLC algorithm mandates exactly this bound. Documented trade-off in the `hlc` module: stamps beyond the bound waive the "local write after observe dominates remote" guarantee; store-level rejection of out-of-bound updates is deferred to the next wire-policy pass.
- **Symmetric capability-freshness window** — `CapEntry::is_fresh` / `ReqEntry::is_fresh` now also treat entries stamped further in the **future** than the 3× evaporation window as stale. A writer whose clock is persistently ahead by more than 3× its refresh interval quarantines itself instead of becoming un-evaporable; failure detection no longer depends on the sender's clock sanity. Regression gates: `observe_bounds_remote_clock_drift` (hlc), `future_stamped_entry_is_quarantined_not_fresh` (capability).
- **Commit-conflict tripwire** — the consensus listener now refuses to endorse a `COMMIT` carrying a *different* value for a slot whose existing commitment is still live (slots are commit-once; leased slots reopen only after expiry). Conflicts are logged at `warn!` and counted in `SystemStats::commit_conflicts` (also on `GET /stats`). Namespace ownership of `consensus/` remains promise-strength by design — the tripwire makes violations legible without teaching Layer I a Layer III law.
- **Epoch-leased commitments** — `ConsensusConfig::committed_lease_secs: Option<u64>`. When set, the commit also writes `consensus/lease/{slot}` (u64 LE ms) and lease expiry is evaluated **read-side** against the committed entry's HLC timestamp — the same evaporation convention as `CapEntry::is_fresh`, no background task, no renewal RPC. An expired lease reads as not-committed and the slot reopens for re-proposal. Renewal = re-proposing the same value while the lease is live (a fresh quorum round that refreshes the commit timestamp); a different value while live returns `Superseded`. Default `None` = permanent commitment (existing behaviour preserved). Lease-aware readers: `consensus_get`, `consistent_get`, `elect_leader` winner lookup, `GET /consensus/{slot}` (now also returns `lease_ms` + `lease_expired`); `consensus_rx` is deliberately the raw KV view.
- **Proposer-side clobber guard** — `try_commit_if_ready` and `cross_propose` now return `Superseded` instead of overwriting when a *different* live commitment landed between the supersession check and quorum (a lost race with another proposer).

### Security

- **Decode allocation bound (remote DoS fix)** — `bincode_cfg()` now sets `.with_limit::<MAX_FRAME_BYTES>()`. Without it, a frame whose internal length prefix claimed a huge element count drove an unbounded `Vec::with_capacity` and the process OOM-aborted (SIGABRT) — one malformed frame from any connected peer, or a bit-flip on a non-TLS link, killed the node. `read_frame` capped the frame size but not the element counts decoded from inside it. All decoders share the config, so the whole wire surface (gossip, capability, signal, locality, WAL sync) was exposed. Found by a decoder mini-fuzz now kept in-suite (`mini_fuzz_decoders_survive_adversarial_bytes`, `fuzz-internals` feature) and wired into CI — the `fuzz/` targets existed but had never run in CI.
- **Dependency advisories cleared** (lockfile bumps, no manifest changes): `bytes` 1.10.1 → 1.11.1 (RUSTSEC-2026-0007, integer overflow in `BytesMut::reserve` — `read_frame` calls `reserve` on the wire path, though the 10 MiB frame cap already bounded the input), `tracing-subscriber` 0.3.19 → 0.3.20 (RUSTSEC-2025-0055, ANSI-escape log poisoning), `tokio` 1.44.1 → 1.46.1 (RUSTSEC-2025-0023, broadcast-channel unsoundness). `cargo audit` now reports zero vulnerabilities; remaining unmaintained-crate warnings (notably `bincode`, the wire codec) are tracked as a roadmap concern.

### Added

- **A2A agent card: schema-aware skills** — `GET /.well-known/agent.json` now populates each skill's `description` from its gossiped input schema (`skills/{ns}/{name}/{node}/input`, published by SkillRunner) and exposes the raw JSON Schema as an additive `inputSchema` field. Tool-calling frameworks build properly-typed tools from it instead of guessing payload shapes from prose — previously the empty description left LangChain/AutoGen agents passing plain text to JSON-expecting skills, which failed with a parse error and let the agent silently fall back to answering from its own weights. The bundled `examples/a2a_langchain/` agents (ported to LangChain ≥ 1.0 `create_agent` and current AutoGen) now derive their tool signatures from `inputSchema`.

- **Demo smoke in CI** — `examples/community/ci_smoke.sh` runs the community cluster against a deterministic mock LLM (`mock_llm.py`, stdlib-only, OpenAI-compatible) on every push: 4-skill convergence, schema-aware agent card, A2A + dashboard router coexistence, the full orchestrator → researcher → writer tool-call pipeline, and SIGTERM cleanup. Each assertion is a regression gate for one of the four bugs the 2026-06-11 live run-through found; the mock additionally rejects non-string chat content exactly as Ollama does, so the tool-result coercion fix cannot silently regress. SkillRunner now **re-asserts its gossiped input/output schemas periodically** (ttl/4, ≥ 5 s) like capability advertisements — a one-shot startup write could race peer-connection establishment and leave tool discovery incomplete for tens of seconds (observed 1-in-3 under load; 8/8 clean after).

### Added (observability)

- **`individual_flood_fallbacks` counter** — on `SystemStats` and `GET /stats`. Counts Individual-scoped frames (RPC requests/responses, consensus votes) that had no direct sender→target route and fell back to flooding — plus, with a rate-limited `warn!`, the residual case of an Individual frame dropped with zero peers. Non-zero under steady state is correct behaviour but signals topology pressure: RPC-heavy pairs without direct peering pay relay latency. Companion to the flood-fallback fix below; the resilience scale test gains a Phase 1b cross-worker RPC gate that exercises exactly this path under `GOSSIP_MAX_ACTIVE_CONNECTIONS`-capped partial meshes.

### Changed

- **AFN fluid-pipeline demo migrated to the pull pattern** — `examples/fluid_pipeline/` now runs the canonical tuple-space architecture by default: workers `take()` from the deepest stage and `complete()` into the next (fluidity = self-selection against per-stage depth), and the former coordinator collapses into a seeder/sink edge client. The original coordinator-dispatch architecture — the project's own named anti-pattern — is retained behind `PIPELINE_MODE=push` as the comparison baseline for the push→pull refinement (`flow_networks.html`, Paper 2a). New `ci_smoke.sh` runs both modes end-to-end as local processes (3 nodes, 24 items, fresh cluster per mode) and is wired into CI as the `afn-smoke` job, so both distribution models are regression-gated.

- **`/gateway/llm/call` reports failures via HTTP status codes** — 404 (`no_provider`), 502 (provider-side error, incl. `parse_error`), 504 (RPC `timeout`); the `{"error":...,"detail":...}` JSON body is unchanged. The endpoint was the gateway's one 200-on-error outlier (every other handler already used `BAD_REQUEST`/`NOT_FOUND`/`GATEWAY_TIMEOUT`), which made failures invisible to `curl -f` and `raise_for_status()` callers — integration scenario 12's flake diagnostic was an empty `{}` for exactly this reason. The SSE `/gateway/llm/stream` endpoint deliberately keeps in-stream `{"type":"error"}` events: the status line is committed before the stream body. SDKs already throw/raise on non-2xx; the Python docstring now documents the raising behaviour. Regression test: `test_llm_call_no_provider_returns_404`.
- **`writer_channel_depth` default raised 256 → 1024** — both scale tests recorded dropped frames at burst (56 at 100 nodes / depth 2048 override; 92 at 5 000-key bulk / depth 4096 override), and the doc comment's own budget math (`N × F` at fan-out 4, N = 256) says 1024. Channel memory is per in-flight frame, not preallocated, so idle cost is nil. Bulk-write workloads should still override to 4096+ via `GOSSIP_WRITER_CHANNEL_DEPTH`.

### Fixed

- **Fan-out activation was polled, not event-driven — inbound-only nodes were mute for live sends for up to two health-check intervals** — the gossip loop's send-target list comes from a watch channel that only the health monitor published (10 s default cadence, plus a change-check that can skip the first tick), while peers are learned on Ping receipt. A node with no outbound dials — a seed, a tuple-space primary, any pure listener — could not send *anything* live (signals, RPC responses, consensus votes) to freshly-connected peers until the cadences aligned: worst case ≈ 2× `health_check_interval`. Anti-entropy silently healed KV, which is why this never surfaced; one-shot Individual frames (RPC replies!) just timed out. Found by the new random-topology property test (`test_individual_consumers_over_random_partial_meshes`: relay delivery worked at "attempt 7" on one graph — exactly a health tick — and never within 8 s on another). Fix: the connection handler publishes the updated peer list at insertion time; the health monitor remains the steady-state reconciler/evictor. All three graphs now deliver on attempt 0.
- **Individual-scoped signals silently dropped when the target was not in the sender's outbound peer list** — with `group_aware_forwarding` (default on), `ForwardHint::Individual` sent the frame *only* to a directly-peered target and otherwise to nobody: the signal never entered the medium. Individual scope carries RPC requests, RPC responses, and consensus votes, so in any partial mesh — exactly what `GOSSIP_MAX_ACTIVE_CONNECTIONS` (the documented iptables mitigation) and `max_forwarding_peers` produce — RPCs timed out and ballots starved between non-peered pairs, with nothing logged. This also contradicted the architecture's stated model (forwarding is unconditional; only *admission* is scoped — `Boundary::admits`). The targeted send is kept as an optimization when a direct route exists; otherwise the frame now falls back to unconditional flooding (each hop applies the same rule; the seen-set dedups, hop-TTL bounds it). Found during the three-arm experiment bring-up: a synchronized first-`take` volley wedged all workers for the full RPC deadline whenever responses raced route establishment. Regression test: `test_individual_signal_reaches_unpeered_target_via_relay` (line topology A→B→C; fails pre-fix).
- **Tombstone GC never fired since the v9 HLC migration (2026-05-20)** — the GC predicate compared the store entry's *packed HLC* timestamp (`(physical_ms << 16) | logical`) against a wall-clock-*millisecond* cutoff; a packed stamp is ~65 536× any ms cutoff, so the condition was unsatisfiable and every tombstone accumulated forever (unbounded store growth on delete-heavy workloads). Every other timestamp consumer (`CapEntry::is_fresh`, seen-set eviction) unpacks via `hlc::physical_ms`; the GC was the one that didn't. The sweep is extracted to `store::sweep_stale_tombstones` (unpacks correctly, preserves the conditional-remove discipline from the race-family fix below). Found by an M2 Run-21 falsification probe; regression test `tombstone_gc_sweep_unpacks_hlc_timestamps`.
- **TypeScript SDK: `shardFor()` crashed on every call** — it referenced `this._base` (the property is `base`) and omitted the path's leading slash; plus 7 further `tsc` errors from assigning fetch's `unknown` JSON to typed values. CI now runs `tsc --noEmit` over `mycelium-ts` (new `sdk-ts` job) so the SDK can't ship type-broken again, and a dedicated time-boxed `cargo fuzz` job covers the wire/capability decoders that the in-suite mini-fuzz samples more shallowly.
- **`with_http_routes` replaced earlier routers instead of merging** — the extra-routes slot was last-caller-wins, so composing registrations silently dropped all but the final one. Concretely: SkillRunner registers `with_a2a()` and then its management dashboard, and the dashboard erased the A2A endpoints — `/.well-known/agent.json` 404'd in exactly the documented A2A setup. Routers are now merged (`Router::merge`); regression test `with_http_routes_merges_across_calls`.
- **A2A `tasks/send` server-side timeout raised 30 s → 120 s** — A2A skills are frequently multi-step LLM pipelines (orchestrator → researcher → writer takes ~90 s on local Ollama); the 30 s cap made every such composition return `-32603 rpc call failed` while the pipeline was still working.
- **SkillRunner: tool results sent to chat APIs as raw JSON** — tool-role messages carried `content` as a JSON *object* when a tool returned structured output; Ollama rejects non-string content (`invalid message content type: map[string]interface {}`), breaking every skill→skill composition whose callee returns JSON. Tool results are now coerced to strings, same as user input already was.
- **SkillRunner survived SIGTERM indefinitely** — the shutdown task drained the agent but the skill loop never returns, and the consumed signal suppressed the default terminate action. Generations of "stopped" skillrunners accumulated invisibly across demo runs, with `SO_REUSEPORT` letting every generation keep sharing the same ports (old binaries answered a fraction of requests). The shutdown task now exits the process after the agent drains; demo `stop.sh` gained an orphan sweep.
- **Example/demo repairs from a live run-through** — `mesh_demo` referenced manifests at a path renamed long ago (hidden by cargo's incremental cache; examples now built in CI); the community demo's convergence check counted a KV prefix that cannot match the real `cap/{node}/{ns}/{name}` key shape; `invoke.sh`'s fallback caller hardcoded the `llm/hello` smoke-test capability (now driven by `SKILL_CAP`/`SKILL_PAYLOAD`); cold-start bind races in `start.sh`/`demo.sh` (spokes now wait for the seed's port); `a2a_langchain/requirements.txt` used a non-portable `file:` relative reference.
- **Prefix-index divergence under concurrent tombstone/insert** — `apply_and_notify` maintained the secondary structures (`prefix_index`, `cap_ns_index`, `peer_localities`) *after* the lock-free store CAS, derived from the update being applied. Two winning writers to the same key could interleave their index ops in the opposite order of their CASes — e.g. a delete racing a higher-timestamp rewrite arriving on another shard — leaving a live store key permanently invisible to `scan_prefix` and capability resolution. Anti-entropy could not repair it (re-applying the same `(key, ts)` loses LWW and never touches the index); only a later rewrite of the key did. Index maintenance is now a *reconcile*: under a per-key-hash stripe lock (`KvStore::index_stripes`, 64 stripes), the writer re-reads the stored entry and sets membership in every secondary structure to match it, so the final index state always matches the final store state. Found by an M2 falsification probe (86 of 100 000 racing rounds reproduced the loss); the probe and an 8-thread mixed-churn consistency test are kept as regression gates.
- **Signal handler registration could panic under contention** — the `HandlerTable` registration closure moved its sender into the map via a single-use `slot.take().expect(...)` inside a papaya `compute`. papaya re-invokes the closure when the entry changes concurrently, so two tasks registering the same signal kind simultaneously (or one racing the closed-sender eviction in delivery) panicked on the retry. The closure now clones the sender per invocation. Regression test: `concurrent_same_kind_signal_registration_does_not_panic` (reproduced the panic instantly pre-fix).
- **Concurrent `set_with_min_acks` on the same key starved each other** — the per-key tracker slot was single-occupancy: a second concurrent caller overwrote the first caller's tracker, and the first caller's unconditional cleanup then deleted the second's — both could report spurious timeouts while the acks arrived. Each key now holds a copy-on-write *list* of trackers (`kv_quorum::{install_tracker, remove_tracker}`): every inbound update is observed by all in-flight callers and each caller removes exactly its own tracker by `Arc` identity. Applies to both the Rust API and the HTTP gateway endpoint.
- **Prompt-skill registration races** (`llm` feature) — (1) two first registrations racing could both observe an empty registry and spawn two `llm.invoke` dispatch loops, each receiving every invoke signal (duplicate RPC responses); the spawn is now gated by an atomic swap. (2) Dropping a stale `PromptSkillHandle` after the same skill id had been re-registered deleted the *new* backend from the registry; the cancellation path now removes only if the registry still holds the backend it registered.
- **A2A task cleanup could evict a live task** — the 5-minute sweep collected stale task ids and then removed them unconditionally; a status update re-inserting the task with a fresh `created_at` between collect and removal was evicted, and clients polling the task got NotFound. The sweep now uses a conditional `compute` (remove only if still stale at removal time).
- **Tombstone GC could delete a concurrent live write** — the GC task collected stale-tombstone keys and then removed them *unconditionally*; a live write winning the store CAS on the same key between collect and removal was deleted outright (recoverable only via anti-entropy from a peer). Same race family as the prefix-index fix above. The removal is now a conditional `compute`: the entry is removed only if it is still a stale tombstone at removal time.
- **LWW equal-timestamp divergence** — concurrent data writes to the same key carrying *identical* HLC timestamps (two writers in the same wall-clock millisecond whose clocks had not yet observed each other) previously resolved by arrival order: each node kept whichever value it applied first, diverging permanently — and undetectably, because the anti-entropy digest hashes `(key, timestamp)` only and was identical on both sides. `lww_wins` now breaks data-vs-data timestamp ties deterministically (lexicographically greater value wins), so apply order no longer matters. Tombstone tie rules are unchanged (tombstone still wins ties; data never resurrects a tombstone on a tie). Rolling-upgrade note: nodes on older versions lack the tiebreak, so a mixed cluster retains the old exposure on exact ties until fully upgraded — no worse than before.
- **Consensus listener registration race** — `start_consensus_listener` now registers the PROPOSE/COMMIT signal receivers synchronously before spawning the voter task. Previously registration happened inside the task's first poll, so a proposal arriving in the startup window was silently dropped and the node failed to vote on it.

---

## [1.1.0] — 2026-06-07

### Added

- **Per-peer gossip rate-limiting** — `GossipConfig::max_inbound_frames_per_sec` (also `GOSSIP_MAX_INBOUND_FRAMES_PER_SEC` env var). When set to a non-zero value, frames received faster than this rate from a single peer are dropped with a warning log. Prevents a malicious or misbehaving peer from flooding the inbound processing pipeline. Default `0` = unlimited (existing behaviour preserved).
- **`bulk_serve` handler concurrency cap** — `GossipConfig::max_concurrent_bulk_handlers` (also `GOSSIP_MAX_CONCURRENT_BULK_HANDLERS` env var). Limits the number of concurrent per-request background tasks spawned by `bulk_serve` via a `tokio::sync::Semaphore`. When the cap is reached, new bulk signals are dropped with a warning. Default `64`; set to `0` for unlimited.

### Changed

- **`GossipError::Config(String)` replaced by three structured variants** — `InvalidField { field: &'static str, reason: String }`, `FieldConflict { field_a, field_b, reason }`, `NodeIdMismatch { node_id, bind_addr }`. Callers can now match specific configuration failures without parsing error strings. All `validate()` and `apply_env_overrides()` error paths updated.
- **`GossipError::Network(String)` replaced by two structured variants** — `FrameTooLarge { size: usize, limit: usize }` and `UnsupportedWireVersion { received: u8, current: u8, prev: u8, hint: &'static str }`. Framing errors are now fully typed; callers can distinguish oversized frames from version mismatches.

### Added

- **HTTP gateway bearer-token authentication** — `GossipConfig::gateway_auth_token: Option<String>` (also `GOSSIP_GATEWAY_AUTH_TOKEN` env var). When set, every `/gateway/**` request must carry `Authorization: Bearer <token>`; unauthenticated requests receive `401 Unauthorized`. Health, ready, stats, and metrics endpoints are always public. Suitable for deployments where `http_addr = "0.0.0.0"`.
- **Error handling guide** — `docs/guide/error-handling.md` documents all eight public error types (`GossipError`, `ConsistencyError`, `RpcError`, `QuorumError`, `ScatterError`, `SchemaError`, `BulkError`, `ShardError`), their recoverability classification, propagation strategy, and a relationship diagram per handle.
- **100-node scale test** — `make test-scale` starts a 100-node Docker cluster (1 seed + 99 workers + mgmt + runner), validates full gossip convergence, KV propagation (seed write → mgmt read), and zero dropped frames. Override size with `make test-scale SCALE_WORKERS=49`. Compose file at `tests/integration/docker-compose.scale.yml`; runner script at `tests/integration/run_scale.sh`.
- **`LlmHandle`** (via `agent.llm()`) — typed handle for LLM prompt-skill operations: `register_prompt_skill`, `call_prompt_skill`, `update_prompt`, `get_prompt`, `list_prompts`, `delete_prompt`. Available under `--features llm`.
- **`McpHandle`** (via `agent.mcp()`) — typed handle for MCP tool bridge operations: `register_mcp_tool` (server-role tool registration), `connect_mcp_server` (client-role tool discovery and proxying). `connect_mcp_server` requires `--features gateway`.
- **`CapEntry` re-exported** from crate root — allows external tooling and benches to encode/decode capability entries from the gossip KV namespace.
- **`#[non_exhaustive]` on all public error and result enums** — `GossipError`, `ConsistencyError`, `RpcError`, `QuorumError`, `ScatterError`, `SchemaError`, `BulkError`, `ShardError`, `McpError` are now `#[non_exhaustive]`. Adding new variants in future releases will not break exhaustive `match` arms in downstream code.
- **Wire rolling-upgrade test** — `read_frame_accepts_prev_wire_version` in `src/framing.rs` verifies that v10 Signal frames (no `hlc_seq`) are accepted by `read_frame`, decoded via `WireMessageV10`, and converted to `WireMessage::Signal { hlc_seq: None }`.
- **`Capability::encode()` / `CapEntry::encode()` made public** — these were `pub(crate)`; now `pub` so external tooling can serialise capability entries for seeding or testing.
- **Capability-resolve benchmark** (`benches/throughput.rs`) — measures `capabilities().resolve()` against 1/10/50/100 pre-seeded providers; shows O(providers) scan cost.
- **KV payload-size benchmark** (`benches/throughput.rs`) — measures `kv().set()` at 64 / 1 024 / 65 536 byte payloads; exercises the framing encode path at representative sizes.
- **Typed sub-handle facade** — `GossipAgent` exposes eight domain-scoped handles, each a zero-cost `Arc<TaskCtx>` clone: `KvHandle` (via `agent.kv()`), `MeshHandle` (via `agent.mesh()`), `CapabilitiesHandle` (via `agent.capabilities()`), `ConsensusHandle` (via `agent.consensus()`), `ServiceHandle` (via `agent.service()`), `SchemaHandle` (via `agent.schemas()`), `LlmHandle` (via `agent.llm()`), `McpHandle` (via `agent.mcp()`). All domain methods live exclusively on their typed handle; `GossipAgent` retains only lifecycle and utility methods.
- `gateway` Cargo feature (on by default) — gates the Axum HTTP server and its transitive deps (`axum`, `tower-http`, `tokio-stream`, `futures-util`). Disable with `default-features = false` for bare-metal / WASM embeds. All gossip, KV, signal, consensus, capability, and service APIs compile without `gateway`.
- `rust-toolchain.toml` — pins the toolchain to `stable`.

### Changed

- `GossipError::State(String)` replaced by two structured variants: `GossipError::AlreadyRunning` (called `start()` on a running agent) and `GossipError::Shutdown` (called `start()` after shutdown). Callers can now match lifecycle errors without parsing strings.
- `set_quorum` renamed to `set_with_min_acks` — name now reflects the actual semantics (wait for N gossip echo receipts, not consensus quorum).
- Cargo.toml `description` improved: now accurately describes the three-layer substrate.
- `a2a` and `llm` features now imply `gateway` (they expose HTTP endpoints).

### Added

- `Capability::with_schema_id` / `CapFilter::with_schema` — optional contract version gossip-propagated with every capability entry. Resolvers that call `with_schema` only match providers advertising the same `schema_id`; capabilities without a `schema_id` do not match (strict by default).
- `Capability::with_input_schema` / `with_output_schema` — embed JSON Schema strings directly in the gossip-propagated capability entry so callers can inspect the invocation contract from `resolve()` results without a separate KV lookup. SkillRunner now embeds `.skill.toml` input/output schemas in the capability in addition to the existing `skills/.../input` KV keys.
- `GossipAgent::signal_rx_from(kind, trusted)` — delivers only signals whose `sender` is in the trusted list. Addresses the semantic-injection attack vector (arXiv 2511.19699 §5.1) for LLM-driven agents processing signal payloads as prompts. Empty `trusted` list delegates to the unfiltered path with no overhead.
- Speech act taxonomy in the crate-level doc comment: maps FIPA-ACL performatives to Mycelium primitives.
- `examples/semantic_coordination.rs` — in-process example demonstrating all three features.
- `GossipAgent::publish_schema(schema_id, json_bytes)` — validates JSON, conflict-detects against the existing `schemas/{id}` KV entry, and writes only on `Published`. Returns `SchemaPublishResult::{Published, Unchanged, Conflict}`.
- `GossipAgent::force_publish_schema` — overwrites without conflict detection; intended for dev / migration tooling.
- `GossipAgent::get_schema(schema_id)` — retrieves authoritative schema bytes from the KV ring.
- `GossipAgent::list_schemas()` — enumerates the full schema catalogue sorted by ID.
- `GossipAgent::seed_schemas_from_dir(path)` — seeds all `*.json` files from a directory tree; file path relative to `dir` (without extension) becomes the `schema_id`.
- `SchemaPublishResult` / `SchemaError` public types.
- `schemas/{schema_id}` added to the KV namespace ownership table.
- Wire v11: `hlc_seq: Option<u64>` added to `WireMessage::Signal` for causal ordering via `emit_ordered()`. v10 rolling-upgrade shim decodes v10 frames with `hlc_seq = None`.
- `emit_ordered()` — stamps an HLC sequence number on the signal frame; receivers with `signal_ordered_delivery = true` buffer per `(sender, kind)` and deliver in ascending HLC order.
- Watcher C2: consolidated requirement opacity watcher — one task and one `cap/` subscription for all declared requirements on a node (previously one task per `declare_requirement` call).

### Fixed

- `publish_schema` / `force_publish_schema` now validate `schema_id` and reject empty IDs, leading/trailing `/`, `//`, `.`/`..` path segments, and non-ASCII characters. `SchemaError::InvalidSchemaId` variant added.
- GC task now proactively evicts closed `prefix_watchers` and `prefix_predicate_watchers` entries on every GC cycle, preventing accumulation of dead senders when the prefix never receives a write after the subscriber drops.
- GC task now evicts orphaned `quorum_trackers` entries (those whose caller future was dropped mid-wait, leaving a dangling tracker with no live waiter).
- Signal reorder buffer now logs a `warn!` when a depth-based flush degrades causal ordering (`max_depth` exceeded). Previously this was silent.
- `rpc_pending` mutex `.lock()` calls now recover from a poisoned mutex rather than panicking, preventing a cascade failure when a panic occurs in a concurrent task.

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
