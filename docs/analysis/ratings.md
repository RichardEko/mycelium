# Mycelium — Analysis Ratings

Project evaluation across 25 orthogonal dimensions, tracked over time.
Each run is appended by `/mycelium-analysis`. Higher is better; 10 = no
meaningful improvement possible at the current stage of the project.

**Dimensions:** Philosophy/Coherence · Conceptual Integrity · Architecture ·
Modularity · API Design · Error Handling · Configurability · Language Best
Practices · Concurrency Correctness · Resource Management · Semantic Correctness ·
Robustness · Security · Failure Mode Legibility · Performance · Scalability ·
Testability · Test Architecture · Observability · Debuggability · Operational
Readiness · Evolvability · Documentation · Developer Experience · Dependency Hygiene

---

## 2026-06-04 — Run 1

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Feature set maps tightly to Holland/Jini/OSGi/Paremus synthesis; library-not-platform honored; consensus overlay correctly positioned as higher-order |
| 2 | Conceptual Integrity | 7 | Core idioms consistent but README mixes old flat-API (23 calls) with new sub-handle API (38); `signal_wired_via().await` in README is a bug in a code example |
| 3 | Architecture | 8 | Three-layer model respected; namespace ownership table explicit; gateway feature gate clean; Layer I/II entanglement documented as known constraint |
| 4 | Modularity | 7 | Sub-handle facade is real API-level separation but all six handles share one `Arc<TaskCtx>` — isolation is external, not internal; 30+ impl files still tightly coupled |
| 5 | API Design | 8 | `#![forbid(unsafe_code)]`; sub-handle facade reduces surface; `CapabilityHandle` vs `CapabilitiesHandle` naming collision is a residual footgun |
| 6 | Error Handling Model | 6 | Eight distinct public error types with no documented relationship; 244 `.unwrap()` calls in production code (most are slice conversions, some are real); error propagation strategy undocumented |
| 7 | Configurability | 8 | 22+ documented `GossipConfig` fields; TOML + `GOSSIP_*` env vars; clean feature flag taxonomy; `writer_channel_depth` correctness threshold documented but not enforced at runtime |
| 8 | Language Best Practices | 8 | `#![forbid(unsafe_code)]`; idiomatic use of `thiserror`, `papaya`, `parking_lot`; one `std::sync::Mutex::lock().unwrap()` in capability_handle.rs is a mild inconsistency |
| 9 | Concurrency Correctness | 7 | Lock-free `papaya` hot paths; `AtomicU64/Bool` for counters; `grp_generation` cache invalidation is clean; two timing-sensitive tests flaky under load suggest real scheduling sensitivity |
| 10 | Resource Management | 7 | RAII for capabilities/locks/handles; GC task evicts orphaned watchers and quorum trackers (recent fix); TCP writer idle timeout; no documented bound on concurrently spawned tasks |
| 11 | Semantic Correctness | 7 | LWW merge correct; HLC Kulkarni 2014 with documented limits; `consistent_set` documented as "linearizable" but epidemic two-phase voting is closer to CASPaxos — formal gap between claim and protocol |
| 12 | Robustness | 7 | `MAX_FRAME_BYTES` bound; TTL decrement; reconnect backoff; `max_store_entries` OOM guard; fail-open signing-key verification creates a TOCTOU window during peer key exchange |
| 13 | Security | 6 | mTLS opt-in (off by default); HTTP gateway has no authentication; no gossip rate-limiting; fail-open on unverified signing keys; `signal_rx_from` sender auth is good but optional |
| 14 | Failure Mode Legibility | 7 | `dropped_frames` with actionable diagnostic guide; `gc_alive`/`health_monitor_alive` flags; `rpc_pending` mutex-poison recovery; consensus timeout returns vote counts; Nack reasons not surfaced |
| 15 | Performance | 8 | Benchmarks published; 16 ns get, 151 ns set; lock-free hot path; gossip sharding; zero-copy forward; `reqwest` non-optional adds binary weight for embedded targets |
| 16 | Scalability | 6 | System-scope gossip is O(n) fan-out; anti-entropy is O(n) rounds; `scan_prefix` O(|store|) fallback for unknown prefixes; no documented node-count ceiling tested beyond demo scale |
| 17 | Testability | 8 | `make_agent()` zero-port helper; `loopback_pair()` in-process TCP; `EchoBackend` for LLM; `alloc_port()` for live-node tests; `TaskCtx` wires through rather than injected |
| 18 | Test Architecture | 7 | 263 unit tests; 12 Docker integration scenarios; 2 fuzz targets; 3 overlay Python scenarios; no property-based tests for convergence invariants; test pyramid is reasonable |
| 19 | Observability | 7 | Prometheus endpoint (`--features metrics`, off by default); `system_stats()`; pre-built Grafana dashboard; no distributed tracing in core (OTEL only in skillrunner) |
| 20 | Debuggability | 7 | KV dump endpoint; `/stats`; `/ready`; management dashboard; `peer_drop_counts()`; consensus ballot state not directly inspectable via API |
| 21 | Operational Readiness | 8 | `is_ready()`/`/ready`; `shutdown_with_timeout()`; `sys/load/` back-pressure; Docker Compose; `GOSSIP_*` env vars; rolling upgrade window; no documented stop-the-world upgrade procedure |
| 22 | Evolvability | 8 | Wire version policy v2–v11 with one-version rolling window; CHANGELOG follows Keep a Changelog; forwarding stubs preserve backward compat; v2.0 milestones documented in ROADMAP |
| 23 | Documentation | 7 | Philosophy doc is excellent; guide chapters 01–12 use current API; README capability section uses old flat API; `signal_wired_via().await` bug in README code example; FIPA-ACL mapping is sophisticated |
| 24 | Developer Experience | 8 | `rust-toolchain.toml`; `CLAUDE.md` on-ramp; guide runnable-example column; diagnostic flow in README; no migration guide for flat-API → sub-handle transition |
| 25 | Dependency Hygiene | 7 | Well-chosen core deps; optional deps correctly feature-gated; `reqwest` required (not optional) pulls TLS into embedded builds; `tokio` `test-util` in `[dependencies]` not `[dev-dependencies]` |
| — | **Mean** | **7.3** | |

---

## 2026-06-04 — Run 2

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Holland/Jini/OSGi/Paremus synthesis fully honored; "consistency as a service" framing intact; bearer-token auth is library-level, not platform-level — consistent with philosophy |
| 2 | Conceptual Integrity | 8 | `set_with_min_acks` rename removes naming–semantics gap; signal group vs capability group disambiguated in doc comments and guide; "no coordinator" claim now properly scoped in README and ROADMAP; sub-handle facade makes domain boundaries lexically explicit |
| 3 | Architecture | 8 | Layer I/II entanglement documented with v2 roadmap item; gateway feature gate clean; three-layer model and namespace ownership unchanged — no regressions |
| 4 | Modularity | 7 | Sub-handle facade provides API-level domain separation; `TaskCtx` is still a 30+ field God object shared by all six handles; true internal isolation requires v2 workspace split |
| 5 | API Design | 8 | `set_with_min_acks` improves semantics; bearer-token config as `Option<String>` is clean; `CapabilityHandle` (ad handle) vs `CapabilitiesHandle` (domain handle) naming ambiguity persists |
| 6 | Error Handling Model | 7 | `docs/guide/error-handling.md` documents all 8 public error types with recoverability classification and propagation strategy; `SchemaPublishResult::Conflict` advisory semantics documented; 181 production `.unwrap()` calls remain |
| 7 | Configurability | 8 | `gateway_auth_token: Option<String>` + `GOSSIP_GATEWAY_AUTH_TOKEN` env var added cleanly; scale test validated `GOSSIP_WRITER_CHANNEL_DEPTH` and `GOSSIP_PING_PEER_SAMPLE_SIZE` at 100 nodes |
| 8 | Language Best Practices | 8 | `rpc_pending` mutex poison recovery fix (no longer panics on poisoned lock); `#![forbid(unsafe_code)]` maintained; 181 production unwraps unchanged — most are slice/OnceLock conversions, a handful are real |
| 9 | Concurrency Correctness | 7 | Scenarios 04 and 07 flaky-test fixes (gossip timing robustness) are a positive signal; no formal deadlock proof; `AtomicBool` usage in opacity governor has no documented memory-ordering rationale |
| 10 | Resource Management | 7 | GC task evicts orphaned `quorum_trackers` and closed prefix watchers (prior fix persists); no new issues; spawned task bound still undocumented |
| 11 | Semantic Correctness | 7 | "No coordinator" overclaim now scoped correctly; `set_with_min_acks` name eliminates quorum–ACK ambiguity; `consistent_set` still described as "linearizable" while the epidemic two-phase protocol is closer to CASPaxos — formal gap persists |
| 12 | Robustness | 7 | Signal reorder buffer `warn!` on degraded flush (prior fix); no new robustness changes; fail-open on unverified Ed25519 keys during key exchange remains |
| 13 | Security | 7 | HTTP gateway bearer-token auth (`gateway_auth_token`) closes the main unauthenticated API surface; health/ready/stats/metrics intentionally public; mTLS still opt-in (trusted-domain default); no gossip rate-limiting |
| 14 | Failure Mode Legibility | 7 | No changes; `dropped_frames` diagnostic guide, `gc_alive`/`health_monitor_alive` flags, consensus vote counts on timeout remain; Nack reasons still not surfaced to callers |
| 15 | Performance | 8 | No changes; benchmarks published; lock-free hot path intact; `reqwest` still non-optional overhead for embedded targets |
| 16 | Scalability | 7 | 100-node Docker scale test passes reliably; practical ceiling (~200–400 nodes) documented; ROADMAP v2 milestone #4 specifies partial-mesh gossip fix with O(N·log N) target; O(N²) topology still present in current release |
| 17 | Testability | 8 | 265 tests (up from 263); `EchoBackend`, `loopback_pair`, `alloc_port` helpers unchanged; no structural changes |
| 18 | Test Architecture | 8 | 100-node Docker scale test (`make test-scale`) validates gossip convergence, KV propagation, and dropped-frame rate at production-adjacent scale; 265 unit + 12 integration + 2 fuzz + 3 overlay + 1 scale; still no property-based convergence tests |
| 19 | Observability | 7 | No changes; Prometheus endpoint (opt-in `metrics` feature); `system_stats()`; Grafana dashboard; OTEL only in skillrunner, not core |
| 20 | Debuggability | 7 | No changes; KV dump, `/stats`, `/ready`, management dashboard, `peer_drop_counts()` intact; consensus ballot state not inspectable via API |
| 21 | Operational Readiness | 8 | Gateway auth makes production HTTP deployment safe; `is_ready()`/`/ready`; `shutdown_with_timeout()`; Docker Compose; rolling upgrade window; no stop-the-world upgrade procedure documented |
| 22 | Evolvability | 8 | CHANGELOG updated with three new features; ROADMAP expanded with detailed partial-mesh gossip v2 milestone; wire version policy unchanged and correct |
| 23 | Documentation | 8 | `docs/guide/error-handling.md` closes the biggest documentation gap; "no coordinator" claim scoped correctly in two places; signal/capability group distinction documented; ROADMAP O(N²) engineering note is detailed and actionable; README capabilities section still uses old flat API (valid via forwarding stubs) |
| 24 | Developer Experience | 8 | No changes; `rust-toolchain.toml`, `CLAUDE.md`, scale test `make test-scale` target with `SCALE_WORKERS` override; CLAUDE.md scale test constraint documented for contributors |
| 25 | Dependency Hygiene | 7 | Gateway feature gate complete (Axum, tower-http, tokio-stream, futures-util all optional); `reqwest` still required (not optional) — adds TLS to all embedded builds; `tokio::test-util` still in `[dependencies]` not `[dev-dependencies]` |
| — | **Mean** | **7.6** | |

---

## 2026-06-04 — Run 3

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Holland/Jini/OSGi/Paremus synthesis intact; library-not-platform honored; consensus overlay correctly positioned as higher-order concern; ROADMAP still uses "Linearizable" in the comparison table but API docs now correct |
| 2 | Conceptual Integrity | 8 | Sub-handle facade lexically enforces domain separation; `set_with_min_acks` rename correct; `CapabilityHandle` (ad handle) vs `CapabilitiesHandle` (domain handle) naming ambiguity persists; ROADMAP "linearizable" residue (4 occurrences) not yet fixed |
| 3 | Architecture | 8 | Three-layer model and namespace ownership unchanged; gateway feature gate clean; Layer I/II entanglement documented with v2 roadmap direction; no regressions |
| 4 | Modularity | 8 | `TaskCtx` now carries a comprehensive doc comment with field-group table, rationale (reference-cycle prevention), and v2 roadmap direction; CLAUDE.md "God Object" section added for contributors; API-level isolation via sub-handles intact; true runtime isolation still v2 |
| 5 | API Design | 8 | `CapabilityHandle` vs `CapabilitiesHandle` naming ambiguity persists; forwarding stubs listed in `GossipAgent` doc comment index create a dual-path surface; core ergonomics and surface discipline otherwise solid |
| 6 | Error Handling Model | 7 | `docs/guide/error-handling.md` from Run 2 covers all 8 public types with recoverability classification; ~181 production `.unwrap()` calls remain (most are slice/OnceLock conversions, some are real recovery gaps); no structured `Result` propagation across async boundaries documented |
| 7 | Configurability | 8 | 22+ documented `GossipConfig` fields; TOML + `GOSSIP_*` env vars; clean feature flag taxonomy; gateway auth token from Run 2 makes production HTTP deployment safe |
| 8 | Language Best Practices | 8 | `#![forbid(unsafe_code)]`; idiomatic `thiserror`, `papaya`, `parking_lot`; `grp_generation` ordering fix (Relaxed→Release/Acquire) removes a real correctness gap; memory ordering policy now codified for future contributors |
| 9 | Concurrency Correctness | 8 | `grp_generation` Release/Acquire pair correct and documented (comment in store.rs + tasks.rs); `AliveGuard`/`ListenerGuard` Relaxed usage explicitly justified; memory ordering policy documented in CLAUDE.md; `caps_advertised` Release/Acquire already correct; no formal deadlock proof |
| 10 | Resource Management | 7 | RAII for capabilities/locks/handles; GC task evicts orphaned watchers and quorum trackers; TCP writer idle timeout; spawned task bound still undocumented; `JoinSet` growth could be unbounded in long-running nodes |
| 11 | Semantic Correctness | 8 | `consistent_set` / `consistent_get` doc comments corrected throughout — API docs, forwarding stubs, HTTP endpoint descriptions, and README all updated from "linearizable" to accurate "ballot-serialized" description; ROADMAP still has 4 "linearizable" occurrences not yet fixed; LWW, HLC, anti-entropy remain correct |
| 12 | Robustness | 7 | `MAX_FRAME_BYTES` bound; TTL decrement; reconnect backoff; `max_store_entries` OOM guard; fail-open on unverified Ed25519 keys during peer key exchange remains |
| 13 | Security | 7 | Gateway bearer-token auth from Run 2 intact; mTLS still opt-in; no gossip rate-limiting; Ed25519 fail-open during key exchange; `signal_rx_from` sender auth is optional; no session-scoped capability views for LLM agents |
| 14 | Failure Mode Legibility | 7 | `dropped_frames` with actionable diagnostic guide; `gc_alive`/`health_monitor_alive` flags; consensus timeout returns vote counts; Nack reasons still not surfaced to callers; no structured panic context beyond `expect()` messages |
| 15 | Performance | 8 | Benchmarks published; 16 ns get, 151 ns set; lock-free hot path; gossip sharding; zero-copy forward; `reqwest` non-optional adds binary weight for embedded targets |
| 16 | Scalability | 7 | 100-node test passes; practical ceiling (~200–400 nodes) documented; O(N·log N) partial-mesh gossip is a v2 roadmap item; `scan_prefix` O(store) fallback for unknown prefixes; O(N²) topology in current release |
| 17 | Testability | 8 | `make_agent()`, `loopback_pair()`, `EchoBackend`, `alloc_port()` helpers intact; 263 unit tests confirmed; `TaskCtx` still wired-through rather than injected |
| 18 | Test Architecture | 8 | 263 unit + 12 integration + 2 fuzz + 3 overlay Python + 1 scale (100-node Docker); all 12 integration scenarios pass; no property-based convergence tests |
| 19 | Observability | 7 | Prometheus endpoint (opt-in `metrics` feature); `system_stats()`; Grafana dashboard; OTEL only in skillrunner, not in core gossip or signal hot paths; trace-level diagnostics for partition events absent |
| 20 | Debuggability | 7 | KV dump endpoint; `/stats`; `/ready`; management dashboard; `peer_drop_counts()`; consensus ballot state not directly inspectable via public API |
| 21 | Operational Readiness | 8 | `is_ready()`/`/ready`; `shutdown_with_timeout()`; `sys/load/` back-pressure; Docker Compose; `GOSSIP_*` env vars; rolling upgrade window (v10→v11 open); no stop-the-world upgrade procedure documented |
| 22 | Evolvability | 8 | Wire version policy v2–v11 with one-version rolling window; CHANGELOG updated; memory ordering policy documented prevents future ordering regressions; v2.0 workspace-split milestones documented in ROADMAP |
| 23 | Documentation | 8 | `TaskCtx` struct now carries a comprehensive contributor doc comment with field-group table, rationale, and v2 direction; memory ordering policy in CLAUDE.md is actionable; ROADMAP still uses "Linearizable" in 4 places; guide chapter 11 (AFN pipeline) absent from `docs/guide/` |
| 24 | Developer Experience | 8 | `rust-toolchain.toml`; `CLAUDE.md` on-ramp; memory ordering policy guides future atomic additions; TaskCtx section comments aid navigation for new contributors; `make test-scale` for 100-node validation |
| 25 | Dependency Hygiene | 7 | `gateway` feature gate complete (Axum, tower-http, tokio-stream, futures-util all optional); `reqwest` still required (not optional) — adds TLS transitive deps to all embedded builds; `tokio::test-util` still in `[dependencies]` not `[dev-dependencies]` |
| — | **Mean** | **7.7** | |

---

## 2026-06-06 — Run 4

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Holland/Jini/OSGi/Paremus synthesis intact; library-not-platform honored; `max_active_connections` is an operational knob, not a philosophy violation; 50-node resilience test further validates the "survive churn" goal |
| 2 | Conceptual Integrity | 8 | ROADMAP "linearizable" residue fully cleared (0 occurrences); sub-handle facade enforces domain separation lexically; `CapabilityHandle` (ad handle) vs `CapabilitiesHandle` (domain handle) naming ambiguity persists |
| 3 | Architecture | 8 | Three-layer model and namespace ownership unchanged; gateway feature gate clean; iptables O(N²) constraint documented with v1 runtime mitigation (`max_active_connections`) and v2 structural fix (SWIM hybrid transport) in CLAUDE.md |
| 4 | Modularity | 8 | No structural change; sub-handle isolation at API level intact; `TaskCtx` God Object documented with v2 roadmap direction; true runtime isolation still v2 |
| 5 | API Design | 8 | `max_active_connections` field and `GOSSIP_MAX_ACTIVE_CONNECTIONS` env var added cleanly; `CapabilityHandle` vs `CapabilitiesHandle` naming ambiguity persists; no new footguns |
| 6 | Error Handling Model | 7 | No changes; `docs/guide/error-handling.md` covers all 8 public types; ~181 production `.unwrap()` calls remain; no structured Result propagation policy across async boundaries |
| 7 | Configurability | 8 | `max_active_connections` env var parsed via `map_err(GossipError::Parse)?` (not unwrap) — consistent with existing pattern; 23+ documented `GossipConfig` fields; Fisher-Yates capping documented in CLAUDE.md |
| 8 | Language Best Practices | 8 | Fisher-Yates partial shuffle implemented correctly using `fastrand`; loop bound `slots.min(non_bootstrap.len())` prevents OOB; no new unwraps; `#![forbid(unsafe_code)]` maintained |
| 9 | Concurrency Correctness | 8 | `max_active_connections` peer-selection runs inside existing `run_health_monitor` arc-and-loop without new shared state; no new lock ordering or atomics; memory ordering policy documented in CLAUDE.md remains accurate |
| 10 | Resource Management | 7 | No changes; RAII for capabilities/locks/handles intact; GC task evicts orphaned watchers; spawned task bound still undocumented; `JoinSet` growth could be unbounded in long-running nodes |
| 11 | Semantic Correctness | 8 | ROADMAP "linearizable" residue cleared (was 4 occurrences in Run 3); `consistent_set`/`consistent_get` API and HTTP docs accurate; LWW, HLC, anti-entropy correct; one correct negative use ("not a substitute for linearizable reads") in README |
| 12 | Robustness | 7 | 50-node resilience test validates crash recovery (5 workers), anti-entropy inbound/outbound (late joiner), and 3-cycle churn (10 workers); fail-open on Ed25519 key exchange remains; `MAX_FRAME_BYTES` and reconnect backoff unchanged |
| 13 | Security | 7 | No changes; bearer-token gateway auth from Run 2 intact; mTLS opt-in; no gossip rate-limiting; Ed25519 fail-open during key exchange; `signal_rx_from` sender auth optional |
| 14 | Failure Mode Legibility | 7 | No changes; `dropped_frames` diagnostic guide, `gc_alive`/`health_monitor_alive` flags, consensus vote counts on timeout remain; Nack reasons still not surfaced to callers |
| 15 | Performance | 8 | No changes; `max_active_connections` Fisher-Yates is O(K) not O(N) on every health-check tick — no regression; benchmarks published; lock-free hot path intact |
| 16 | Scalability | 7 | `max_active_connections` reduces O(N²)→O(N×K) per-node TCP connections at runtime — meaningful mitigation for 100–500 node deployments; default 0 (unlimited) preserves existing behaviour; O(N·log N) partial-mesh and SWIM UDP transport remain v2 roadmap items |
| 17 | Testability | 8 | No structural changes; `make_agent()`, `loopback_pair()`, `EchoBackend`, `alloc_port()` helpers intact; 263 unit tests confirmed; `TaskCtx` still wired-through |
| 18 | Test Architecture | 8 | 50-node resilience test (`make test-scale-resilience`) adds 4-phase crash/anti-entropy/late-joiner/churn coverage; `docker run` late-joiner validates anti-entropy inbound and gossip outbound independently; all 12 integration scenarios pass; no property-based convergence tests |
| 19 | Observability | 7 | No changes; Prometheus endpoint (opt-in `metrics` feature); `system_stats()`; Grafana dashboard; OTEL only in skillrunner, not in core gossip or signal hot paths |
| 20 | Debuggability | 7 | No changes; KV dump endpoint; `/stats`; `/ready`; management dashboard; `peer_drop_counts()`; consensus ballot state not directly inspectable via public API |
| 21 | Operational Readiness | 8 | `make test-scale-resilience` target adds operator-facing 50-node validation; `is_ready()`/`/ready`; `shutdown_with_timeout()`; `sys/load/` back-pressure; rolling upgrade window open (v10→v11) |
| 22 | Evolvability | 8 | v2.0 Milestone #5 (hybrid TCP/UDP transport, SWIM-style) documented in ROADMAP with trigger conditions and expected outcome; CLAUDE.md extended with v2 transport fix note; CHANGELOG updated; wire version policy unchanged |
| 23 | Documentation | 8 | ROADMAP "linearizable" residue cleared; SWIM-style v2 transport section detailed and accurate (correct SWIM protocol description — UDP pings, TCP state transfer); iptables constraint section in CLAUDE.md now cross-references v1 mitigation and v2 structural fix; guide chapter 11 (AFN pipeline) still absent from `docs/guide/` |
| 24 | Developer Experience | 8 | `make test-scale-resilience` and `make test-scale-resilience-clean` targets added; `RESILIENCE_WORKERS` override; `make test-scale-resilience RESILIENCE_WORKERS=10` for quick local validation; `CLAUDE.md` on-ramp unchanged |
| 25 | Dependency Hygiene | 7 | No changes; `gateway` feature gate complete; `reqwest` still required (not optional); `tokio::test-util` still in `[dependencies]` not `[dev-dependencies]` |
| — | **Mean** | **7.7** | |

---

## 2026-06-06 — Run 5

Changes since Run 4: anti-entropy on reconnect (d4520be); `is_ready()` / `/ready` endpoint (d4520be); durability contract documentation (d4520be); 6 integration scenarios fixed (4 outright failing + 2 flaky → all 12 passing — 2ef4b4a, ad4122f, eb74cf0); `ConsensusPair` helper + consensus test bug (tests passing by ballot-retry luck, not correct setup — 5bf958b); CLAUDE.md testing conventions (3261b11).

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Holland/Jini/OSGi/Paremus synthesis intact; library-not-platform honored; anti-entropy-on-reconnect aligns directly with "always partition-tolerant substrate" goal |
| 2 | Conceptual Integrity | 8 | `CapabilityHandle` (ad handle) vs `CapabilitiesHandle` (domain handle) naming ambiguity persists; otherwise consistent throughout |
| 3 | Architecture | 8 | Three-layer model and namespace ownership unchanged; anti-entropy-on-reconnect is correctly placed in `run_health_monitor` (tasks.rs) — no layer violations |
| 4 | Modularity | 8 | Sub-handle facade intact; `TaskCtx` God Object documented with v2 roadmap direction |
| 5 | API Design | 8 | `is_ready()` public method added cleanly; `CapabilityHandle` vs `CapabilitiesHandle` naming residue persists |
| 6 | Error Handling Model | 7 | No changes; `docs/guide/error-handling.md` covers all 8 types; ~181 production `.unwrap()` calls remain |
| 7 | Configurability | 8 | No new config fields; `health_check_max_jitter_ms` now exercised correctly in test helpers |
| 8 | Language Best Practices | 8 | `#![forbid(unsafe_code)]` maintained; anti-entropy fix is a clean loop addition; `caps_advertised` Release/Acquire ordering consistent with documented policy |
| 9 | Concurrency Correctness | 8 | Anti-entropy on reconnect adds `request_state` inside an existing lock-free peer-set diff — no new shared state; memory ordering policy intact |
| 10 | Resource Management | 7 | No changes; spawned task bound still undocumented |
| 11 | Semantic Correctness | 8 | Anti-entropy-on-reconnect closes a correctness gap: soft-state keys (caps, locality) now propagate within one gossip round-trip of reconnection rather than waiting up to 30 s; LWW and HLC correctness unchanged |
| 12 | Robustness | 8 | ↑ Anti-entropy on reconnect is a real production fix — previously a restarted node's capabilities/locality wouldn't propagate to the cluster until the next advertisement tick (5–30 s); now immediate; 6 integration scenarios stabilised (4 outright failing, 2 flaky — all 12 now pass); fail-open on Ed25519 key exchange remains |
| 13 | Security | 7 | No changes; bearer-token gateway auth intact; mTLS opt-in; no gossip rate-limiting |
| 14 | Failure Mode Legibility | 7 | No changes; Nack reasons still not surfaced to callers |
| 15 | Performance | 8 | `request_state` on reconnect is one extra StateRequest per new peer-set member — negligible; lock-free hot path intact |
| 16 | Scalability | 7 | No changes; O(N²) topology in current release; O(N·log N) partial-mesh is v2 roadmap |
| 17 | Testability | 8 | `ConsensusPair` encapsulates correct 4-step multi-node setup; `TaskCtx` still wired-through rather than injected — incremental improvement |
| 18 | Test Architecture | 9 | ↑ Two independent quality improvements: (a) 6 integration scenarios fixed (4 failing + 2 flaky → all 12 passing reliably); (b) consensus unit tests refactored from "passing by ballot-retry luck" to "passing by correct structural polling + proper listener setup" — tests now assert the intended invariant, not an accidental side-effect of the retry window |
| 19 | Observability | 7 | No changes; Prometheus endpoint (opt-in `metrics` feature); Grafana dashboard; OTEL only in skillrunner |
| 20 | Debuggability | 7 | No changes; consensus ballot state still not directly inspectable |
| 21 | Operational Readiness | 9 | ↑ `is_ready()` + `/ready` (503 while starting, 200 when soft state hydrated) implements the standard Kubernetes two-probe liveness/readiness distinction; previously only `/health` (liveness) existed — no way to know if capabilities had been advertised post-restart |
| 22 | Evolvability | 8 | No changes; wire version policy intact |
| 23 | Documentation | 8 | Durability contract section added to `src/lib.rs` crate doc (what needs at least one persistent node, what regenerates on reconnect); CLAUDE.md testing conventions section documents `start_consensus_listener` requirement and structural polling principle |
| 24 | Developer Experience | 9 | ↑ `ConsensusPair` helper + CLAUDE.md testing conventions document a non-obvious pitfall (ballot retry window masks peer connectivity race); anti-entropy-on-reconnect + `/ready` make restart behaviour predictable; `make test-scale-resilience RESILIENCE_WORKERS=10` for quick local validation |
| 25 | Dependency Hygiene | 7 | No changes; `reqwest` still required; `tokio::test-util` still in `[dependencies]` not `[dev-dependencies]` |
| — | **Mean** | **7.8** | |

---

## 2026-06-07 — Run 6

Changes since Run 5: Ed25519 fail-open → fail-closed (`SignedData` from unknown signers now dropped with `warn!` — fail-closed is safe because anti-entropy-on-reconnect delivers the signer's `sys/identity/` key within one gossip round-trip); complete mutex poison recovery (10 remaining `.lock().unwrap()` in `helpers.rs`, `capability_ops.rs`, `capability_handle.rs`, `http.rs` → `.unwrap_or_else(|e| e.into_inner())`); 4 clippy errors fixed (`a2a.rs` dead_code, `lifecycle.rs` Arc::clone ×2, `prompt.rs` while-let) — clippy now passes at 0 warnings with full `--features tls,metrics,a2a,llm -D warnings`; `docs/guide/13-cluster-topology.md` — comprehensive cluster operations chapter (seed shapes, sizing worksheet, partition recovery, Docker Compose health-check pattern); all test levels pass: 277 unit, 12/12 integration, 5/5 scale (100 nodes), 10/10 resilience (21 nodes).

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Holland/Jini/OSGi/Paremus synthesis intact; fail-closed Ed25519 tightens substrate invariants without architectural drift |
| 2 | Conceptual Integrity | 8 | `CapabilityHandle` (advertisement handle) vs `CapabilitiesHandle` (domain handle) naming residue persists; topology guide uses consistent glossary |
| 3 | Architecture | 8 | Three-layer model and namespace table unchanged; `cap_ns_index` secondary index provides O(1) cap lookups; `reqwest` bleeds into non-gateway builds via `capability_config.rs` and `bulk.rs` |
| 4 | Modularity | 8 | Sub-handle facade intact; `TaskCtx` God Object documented; v2 crate-split roadmap item unchanged |
| 5 | API Design | 8 | No changes; `is_ready()` + sub-handle surface clean; `CapabilityHandle` naming residue noted |
| 6 | Error Handling Model | 7 | Complete mutex poison recovery (all `.lock().unwrap()` now `.unwrap_or_else`); ~170+ production unwraps remain (most infallible slice conversions); error taxonomy and guide unchanged |
| 7 | Configurability | 8 | No config API changes; topology guide documents which knobs to tune and when, making the 30-field `GossipConfig` surface approachable for operators |
| 8 | Language Best Practices | 8 | 0 clippy warnings at full feature matrix (`--features tls,metrics,a2a,llm -D warnings`); mutex poison recovery complete; `#![deny(unsafe_code)]` enforced; only `unsafe` in codebase is `std::env::set_var` inside a test |
| 9 | Concurrency Correctness | 8 | No new concurrency code; papaya pin() guard invariant documented and respected; atomic ordering policy unchanged |
| 10 | Resource Management | 7 | No changes; spawned task count still unbounded — no documented ceiling on concurrent capability advertisement tasks |
| 11 | Semantic Correctness | 8 | LWW merge, HLC causality, quorum accounting unchanged; anti-entropy-on-reconnect correctness confirmed by 21-node resilience test (all 3 Phase 3 late-joiner probes pass) |
| 12 | Robustness | 9 | ↑ Ed25519 fail-closed plugs the explicitly called-out gap from Run 5; `SignedData` from unknown signers dropped rather than accepted; recovery within one round-trip via anti-entropy confirmed; complete mutex poison recovery eliminates the cascade-panic risk from Mutex poisoning |
| 13 | Security | 8 | ↑ Ed25519 now fail-closed (was fail-open); bearer-token gateway auth; mTLS opt-in; gossip rate-limiting still absent |
| 14 | Failure Mode Legibility | 7 | `warn!` for Ed25519 drops is specific and actionable; Nack reasons still not surfaced to callers; no changes to ballot state visibility |
| 15 | Performance | 8 | No changes; `cap_ns_index` O(1) cap lookups; lock-free papaya hot paths intact |
| 16 | Scalability | 7 | Topology guide documents sizing worksheet and `max_active_connections` partial-mesh mitigation; O(N²) cliff documented but not architecturally addressed until v2 |
| 17 | Testability | 8 | No changes; 277 tests; `ConsensusPair` helper; `TaskCtx` still wired-through rather than injected |
| 18 | Test Architecture | 9 | 277/277 unit, 12/12 integration, 5/5 scale, 10/10 resilience all pass; full feature matrix (`--features tls,metrics,a2a,llm`) now verified at 0 warnings |
| 19 | Observability | 7 | No changes; `/metrics` Prometheus endpoint; `dropped_frames` tracked; no OTEL span propagation in gossip hot path |
| 20 | Debuggability | 7 | No changes; topology guide documents `/ready` + `/health` usage; consensus ballot state still not directly inspectable |
| 21 | Operational Readiness | 9 | Topology guide (chapter 13) covers seed configuration, Docker Compose health-check pattern, partition recovery, sizing worksheet — closes the primary operational documentation gap |
| 22 | Evolvability | 8 | No changes; wire v11 / v10 rolling window intact; CHANGELOG maintained |
| 23 | Documentation | 8 | Chapter 13 topology guide added; all core chapters (01–04) verified to use current sub-handle API syntax; chapter 11 (`11-semantic-coordination.md`) still referenced in README but file does not exist |
| 24 | Developer Experience | 9 | 0 clippy warnings at full feature matrix now enforced; topology guide reduces operational guesswork; test suite covers all five levels |
| 25 | Dependency Hygiene | 7 | `reqwest` still in `[dependencies]` (used by `capability_config.rs` + `bulk.rs` — bleeds Hyper into bare-metal builds); `tokio-test-util` still in `[dependencies]` not `[dev-dependencies]` |
| — | **Mean** | **7.9** | |

---

## 2026-06-07 — Run 7

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Holland/Jini/OSGi/Paremus synthesis fully honored; `reqwest` now optional restores the "library, not platform" constraint for bare-metal targets; no feature drift |
| 2 | Conceptual Integrity | 8 | `CapabilityHandle` (ad handle) vs `CapabilitiesHandle` (domain handle) naming ambiguity persists as the one remaining idiom inconsistency; all other naming consistent |
| 3 | Architecture | 8 | Three-layer model and namespace ownership unchanged; `reqwest` properly gated — `--no-default-features` build passes cleanly; no regressions |
| 4 | Modularity | 8 | Sub-handle facade intact; `TaskCtx` God Object documented with v2 roadmap direction; no structural change |
| 5 | API Design | 8 | `#[tracing::instrument]` on all 11 critical public methods (KvHandle×4, MeshHandle×3, ConsensusHandle×4) adds observability without API surface change; `CapabilityHandle` naming residue persists |
| 6 | Error Handling Model | 7 | Infallible `.unwrap()` converted to `.expect("message")` in connection.rs, signal.rs, store.rs, bulk.rs — intent now documented; ~200 production unwraps remain, majority in test helper functions and framing tests; no structural error-type changes |
| 7 | Configurability | 8 | No config API changes; tracing instrument on hot-path methods provides dynamic observability gain at no config overhead |
| 8 | Language Best Practices | 9 | ↑ Infallible unwrap → `.expect()` conversion documents intent at call sites; `cargo build --lib --no-default-features` passes (reqwest now optional); 277/277 tests at 0 clippy warnings; `#![forbid(unsafe_code)]` maintained; `field_reassign_with_default` allow is correctly scoped to test code |
| 9 | Concurrency Correctness | 8 | No new concurrency code; atomic ordering policy unchanged and correct; memory ordering policy documentation intact |
| 10 | Resource Management | 7 | No changes; spawned task count still undocumented; RAII handle semantics intact |
| 11 | Semantic Correctness | 8 | LWW, HLC causality, consensus quorum accounting all correct; no semantic changes in this run |
| 12 | Robustness | 9 | No regression; Ed25519 fail-closed from Run 6 persists; mutex poison recovery complete; anti-entropy-on-reconnect persists |
| 13 | Security | 8 | No changes; Ed25519 fail-closed; bearer-token gateway auth; mTLS opt-in; gossip rate-limiting still absent |
| 14 | Failure Mode Legibility | 7 | `.expect("message")` on formerly silent unwraps makes panics actionable; tracing spans on critical paths improve distributed debugging context; Nack reasons still not surfaced to callers |
| 15 | Performance | 8 | `#[tracing::instrument]` at trace/debug level is zero-cost when no subscriber is installed (tracing is no-op by default); no hot-path regression; benchmarks unchanged |
| 16 | Scalability | 7 | No changes; O(N²) topology cliff documented but architecturally deferred to v2 |
| 17 | Testability | 8 | 277 unit tests pass; no structural testability change; `TaskCtx` still wired-through rather than injected |
| 18 | Test Architecture | 9 | 277/277 unit, 12/12 integration, 100-node scale, 21-node resilience all pass; full feature matrix (`--features tls,metrics,a2a,llm`) verified at 0 warnings; no property-based convergence tests |
| 19 | Observability | 8 | ↑ `#[tracing::instrument]` on `KvHandle::{set,get,set_async,scan_prefix}`, `MeshHandle::{emit,emit_ordered,emit_async}`, `ConsensusHandle::{group_propose,system_propose,consistent_set,distributed_lock}` — 11 spans on the operations that matter most; combined with existing `/metrics` Prometheus endpoint and Grafana dashboard, operators can now correlate latency to specific operations; OTEL still only in skillrunner |
| 20 | Debuggability | 7 | Tracing spans help; consensus ballot state still not directly inspectable via public API |
| 21 | Operational Readiness | 9 | No changes; `is_ready()`/`/ready`; `shutdown_with_timeout()`; chapter 13 topology guide; rolling upgrade window |
| 22 | Evolvability | 8 | No changes; wire v11 / v10 rolling window intact; CHANGELOG maintained |
| 23 | Documentation | 8 | No new chapters; chapter 11 (`11-semantic-coordination.md`) still absent but referenced in docs/guide/README.md; existing chapters 01–13 use current sub-handle API |
| 24 | Developer Experience | 9 | `cargo build --lib --no-default-features` now confirmed clean — contributors can develop against the bare substrate without pulling Hyper; tracing spans on critical paths aid local debugging; all existing DX improvements persist |
| 25 | Dependency Hygiene | 8 | ↑ `reqwest` is now optional (`gateway` feature only) — `cargo build --lib --no-default-features` passes and does not pull Hyper; `tokio-test-util` still in `[dependencies]` not `[dev-dependencies]` (minor residue); supply chain risk otherwise low |
| — | **Mean** | **8.0** | |

## 2026-06-07 — Run 8

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Exceptional Holland/Jini/Paremus alignment; "no coordinator" claim now properly qualified with seed-as-soft-coordinator and consensus proposer caveats |
| 2 | Conceptual Integrity | 8 | Consistent idioms throughout; LLM/MCP methods remain on `GossipAgent` rather than a typed handle — minor inconsistency with the sub-handle pattern |
| 3 | Architecture | 8 | Clean layer separation enforced by namespace table; Layer I/II entanglement documented as known v2 item; `TaskCtx` God Object section added to CLAUDE.md |
| 4 | Modularity | 8 | Six handles independently understandable and storeable; `TaskCtx` shared state is the only remaining coupling, deferred to workspace split in v2 |
| 5 | API Design | 8 | Minimal public surface; sub-handle pattern is clean and hard to misuse; `set_with_min_acks` naming now correct; no footguns in core paths |
| 6 | Error Handling Model | 7 | All production `unwrap()` converted to `expect("infallible: reason")`; `GossipError::State(String)` still opaque; no structured error code taxonomy |
| 7 | Configurability | 9 | Comprehensive `GossipConfig` with env overrides (`GOSSIP_*`), well-organized feature gates, TOML load, CLI flags — covers all operational concerns |
| 8 | Language Best Practices | 8 | `#![deny(unsafe_code)]`, idiomatic async Rust; dual concurrent map (`dashmap` + `papaya`) is minor redundancy; no `unwrap()` in production paths |
| 9 | Concurrency Correctness | 8 | Memory ordering policy documented for every atomic; lock-free hot paths; `JoinSet` reaping prevents task accumulation; no identified races |
| 10 | Resource Management | 8 | `task_count` in `SystemStats` now surfaced; per-peer drop counts via `peer_drop_counts()`; explicit drop semantics on capability handles |
| 11 | Semantic Correctness | 8 | LWW merge correct (tombstone wins on tie); HLC tick/observe contract correct; quorum arithmetic correct; epidemic Paxos re-proposal sound |
| 12 | Robustness | 8 | Graceful degradation on shard death; listener auto-restart; anti-entropy on reconnect; missing frame-size cap is known DoS surface |
| 13 | Security | 7 | mTLS opt-in with Ed25519 identity; signed consensus payloads; frame-size DoS still open; no RBAC; gateway auth is `compliance` feature (unimplemented) |
| 14 | Failure Mode Legibility | 8 | `ConsensusResult` variants carry detail; `task_count` exposes leaks; `/consensus/{slot}` HTTP inspector; `peer_drop_counts` identifies slow peers |
| 15 | Performance | 8 | 151 ns `set`, 16 ns `get` benchmarked; tracing at `trace!` level; gossip fan-out is O(K) per node not O(N); no hot-path allocations identified |
| 16 | Scalability | 8 | O(N²) TCP cliff now documented with explicit connection-count table and iptables saturation thresholds; `max_active_connections` mitigation documented |
| 17 | Testability | 8 | Deterministic single-process unit tests; injectable config; no global state; fuzz coverage on framing and HLC |
| 18 | Test Architecture | 8 | 277 unit, 12 integration, 3 overlay, 2 fuzz, 2 scale tests; good pyramid shape; property-based / proptest coverage absent |
| 19 | Observability | 8 | Prometheus + Grafana, structured tracing, OTEL traces, `task_count` in stats, `/consensus/{slot}` added this run |
| 20 | Debuggability | 8 | `/consensus/{slot}` endpoint for live inspection; `task_count` catches leaks; `peer_drop_counts()` identifies slow peers; KV dump via scan |
| 21 | Operational Readiness | 8 | `/ready` probe, `shutdown_with_timeout`, persistence (`SyncMode::Flush`), Docker Compose health check wiring documented |
| 22 | Evolvability | 8 | Wire version policy (`WIRE_VERSION` + `PREV_WIRE_VERSION`), CHANGELOG under `[Unreleased]`, ROADMAP v2 milestones; debt documented not hidden |
| 23 | Documentation | 8 | 13 guide chapters (ch.01–13) + philosophy.html; ch.11 (consensus guide) missing; no CONTRIBUTING.md; API examples use current sub-handle syntax |
| 24 | Developer Experience | 8 | CLAUDE.md onramp with operational diagnostics reference; `rust-toolchain.toml`; Makefile; no visible CI config in repo |
| 25 | Dependency Hygiene | 8 | Optional deps properly feature-gated; `dashmap`/`papaya` redundancy; one transitive deprecation warning (`block v0.1.6`); `Cargo.lock` present |
| — | **Mean** | **8.0** | |

## 2026-06-07 — Run 9

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Holland/Jini/Paremus synthesis fully realised; "no coordinator" properly scoped; every feature traces back to the stated substrate purpose |
| 2 | Conceptual Integrity | 8 | Sub-handle pattern consistent across six domains; `register_prompt_skill` / `register_mcp_tool` still live on `GossipAgent` directly rather than typed handles — deliberate but still reads as inconsistency |
| 3 | Architecture | 8 | Layer separation enforced by namespace table; papaya pin-guard `await` invariant now documented in `KvState`; Layer I/II entanglement acknowledged as v2 roadmap item |
| 4 | Modularity | 8 | Six handles independently usable; single concurrent map (`papaya`) throughout after dashmap removal; TaskCtx shared state remains the only cross-handle coupling |
| 5 | API Design | 8 | Minimal, hard-to-misuse surface; `AlreadyRunning`/`Shutdown` typed errors; `signal_rx_from` trust filter elegant; no new footguns |
| 6 | Error Handling Model | 8 | `GossipError::AlreadyRunning` and `Shutdown` are matchable structural variants; all production paths use `expect("infallible: reason")`; `Network(String)` / `Config(String)` are still catch-alls for their domains |
| 7 | Configurability | 9 | Comprehensive `GossipConfig` with env overrides, feature gates, TOML, CLI — all operational knobs exposed without requiring code changes |
| 8 | Language Best Practices | 9 | `#![deny(unsafe_code)]`, no production `unwrap()`, single concurrent map (papaya) throughout after dashmap removal, `is_some_and` used idiomatically; `async-trait` is the only lingering rough edge |
| 9 | Concurrency Correctness | 8 | Memory ordering policy documented for every atomic; `last_state_sent` is task-local (no sharing); no identified races |
| 10 | Resource Management | 8 | `task_count` in SystemStats; anti-entropy cooldown adds per-connection `Instant` (zero heap); explicit handle drop semantics throughout |
| 11 | Semantic Correctness | 8 | LWW convergence and HLC monotonicity now property-tested with proptest; formal guarantees match code; chunked anti-entropy gap documented |
| 12 | Robustness | 9 | StateRequest cooldown prevents scan-flood DoS; frame rejection tests verify zero-length and oversized paths work; existing infrastructure (frame cap, connection limit, TTL clamp, malformed-frame skip) comprehensive |
| 13 | Security | 7 | mTLS + Ed25519 + gateway bearer token; StateRequest cooldown adds per-connection DoS protection; no global rate limit, no RBAC, `compliance` feature unimplemented |
| 14 | Failure Mode Legibility | 8 | `AlreadyRunning`/`Shutdown` give clear callsite diagnostics; `expect("infallible: ...")` messages explain invariants; cooldown logs at `debug!`; no regression |
| 15 | Performance | 8 | 151 ns set, 16 ns get; O(K) gossip fan-out; anti-entropy cooldown adds only an `Instant::elapsed()` check on the hot path |
| 16 | Scalability | 8 | O(N²) cliff documented with explicit table; `max_active_connections` mitigation documented; v2 SWIM transport on roadmap |
| 17 | Testability | 8 | Deterministic, injectable, no hidden global state; proptest now part of the test tool-chain |
| 18 | Test Architecture | 9 | 287 unit + 12 integration + 2 fuzz + 10 proptest (LWW convergence, HLC monotonicity, framing round-trip) — four-tier pyramid; property coverage of core formal invariants |
| 19 | Observability | 8 | Prometheus + OTEL + `task_count` + `/consensus/{slot}`; no regression |
| 20 | Debuggability | 8 | `/consensus/{slot}`, `task_count`, `peer_drop_counts()`, KV dump; no new tools this run |
| 21 | Operational Readiness | 8 | `/ready`, `shutdown_with_timeout`, persistence, Docker health checks; no regression |
| 22 | Evolvability | 8 | CHANGELOG consistently updated; dashmap removal is clean dep-tree hygiene; wire-version policy unchanged |
| 23 | Documentation | 9 | All 13 guide chapters now exist (ch.11 written this run); CONTRIBUTING.md comprehensive with build matrix, layer rules, wire-version policy; no guide chapter gaps remaining |
| 24 | Developer Experience | 8 | CONTRIBUTING.md now covers full contribution path; `CLAUDE.md` onramp detailed; no CI config in repo remains the main gap |
| 25 | Dependency Hygiene | 9 | dashmap removed — single concurrent map (`papaya`) throughout; all optional deps properly feature-gated; `Cargo.lock` present; `--no-default-features` compiles cleanly |
| — | **Mean** | **8.2** | |

---

## 2026-06-07 — Run 10

Changes since Run 9: `LlmHandle` and `McpHandle` sub-handles added (`17aef72`), completing the 8-handle typed facade; all six LLM prompt-skill methods (`register_prompt_skill`, `call_prompt_skill`, `update_prompt`, `get_prompt`, `list_prompts`, `delete_prompt`) moved off `GossipAgent` onto `LlmHandle`; MCP bridge methods (`register_mcp_tool`, `connect_mcp_server`) moved to `McpHandle`; `advertise_capability` return type renamed from `CapabilityHandle` to `CapabilityReg` — resolves the long-standing handle-naming ambiguity; `tokio test-util` confirmed in `[dev-dependencies]` (not `[dependencies]`); all test levels pass; 0 clippy warnings.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Holland/Jini/OSGi/Paremus synthesis fully honored; handle completion solidifies the "library, not platform" contract; no feature drift |
| 2 | Conceptual Integrity | 9 | `CapabilityReg` (advertisement lifetime) vs `CapabilitiesHandle` (domain handle) naming ambiguity resolved — the last named idiom inconsistency is gone; all eight domains now follow the same pattern; `LlmHandle`/`McpHandle` are idiomatic with the rest |
| 3 | Architecture | 8 | Three-layer model and namespace table unchanged; gateway feature gate clean; Layer I/II entanglement documented with v2 roadmap direction; `reqwest` now optional — no regressions |
| 4 | Modularity | 9 | Eight independently understandable, storable, moveable handles: `KvHandle`, `MeshHandle`, `CapabilitiesHandle`, `ConsensusHandle`, `ServiceHandle`, `SchemaHandle`, `LlmHandle`, `McpHandle`; `TaskCtx` shared state remains the only coupling, correctly deferred to v2 workspace split; LLM skills registry moved into `TaskCtx` so `LlmHandle` needs no borrow of the agent |
| 5 | API Design | 9 | `CapabilityReg` return type makes drop semantics explicit at the type level; `#[must_use]` on `advertise_capability`; eight-handle facade is minimal, orthogonal, hard to misuse; no remaining public footguns in core paths; `signal_rx_from` trust filter is elegant |
| 6 | Error Handling Model | 8 | `GossipError::AlreadyRunning`/`Shutdown` matchable; all production `.unwrap()` are in test helpers or genuinely infallible; `Network(String)` / `Config(String)` remain catch-alls for their domains — no structured error code taxonomy; error-handling guide covers all 8 public types |
| 7 | Configurability | 9 | Comprehensive `GossipConfig` with env overrides (`GOSSIP_*`), TOML load, CLI flags, feature gates — all operational knobs exposed without requiring code changes; cluster topology guide makes the 30-field surface approachable |
| 8 | Language Best Practices | 9 | `#![deny(unsafe_code)]`, 0 clippy warnings at full feature matrix, `tokio test-util` in `[dev-dependencies]`, `CapabilityReg` `#[must_use]`, proptest on core invariants; `async-trait` is the only lingering rough edge |
| 9 | Concurrency Correctness | 8 | Memory ordering policy documented for every atomic; `LlmHandle` accesses `llm_skills` via `TaskCtx` field — same concurrency model as other handles; no new lock ordering issues; no formal deadlock proof |
| 10 | Resource Management | 8 | `CapabilityReg` makes advertisement lifetime explicit and drop-based; `task_count` in `SystemStats`; RAII throughout; spawned task ceiling still not documented |
| 11 | Semantic Correctness | 8 | LWW convergence and HLC monotonicity property-tested; quorum arithmetic correct; `consistent_set` correctly described as "ballot-serialized" throughout; no regressions |
| 12 | Robustness | 9 | Ed25519 fail-closed; anti-entropy-on-reconnect; mutex poison recovery complete; `MAX_FRAME_BYTES` bound; listener auto-restart; 21-node resilience test validates late-joiner and churn recovery |
| 13 | Security | 8 | mTLS + Ed25519 + gateway bearer token; signed consensus payloads; no gossip rate-limiting; no RBAC; `compliance` feature unimplemented; `signal_rx_from` sender auth covers semantic injection |
| 14 | Failure Mode Legibility | 8 | `ConsensusResult` variants carry detail; `expect("infallible: …")` messages document invariants; `task_count` exposes leaks; `peer_drop_counts` identifies slow peers; Nack reasons still not surfaced to callers |
| 15 | Performance | 8 | 151 ns set, 16 ns get benchmarked; O(K) gossip fan-out; lock-free hot paths; `LlmHandle` adds no overhead — same `Arc<TaskCtx>` clone pattern; no hot-path regressions |
| 16 | Scalability | 8 | O(N²) TCP cliff documented with explicit table; `max_active_connections` mitigation operational; v2 SWIM hybrid transport on roadmap; `scan_prefix` O(store) fallback for unknown prefixes |
| 17 | Testability | 8 | Deterministic, injectable, no hidden global state; proptest on LWW/HLC/framing; `ConsensusPair` helper; `EchoBackend` for LLM; `TaskCtx` still wired-through rather than injected |
| 18 | Test Architecture | 9 | 287 unit + 12 integration + 2 fuzz + 3 overlay + 2 scale (100-node + 21-node resilience) + proptest — five-tier pyramid covering formal invariants; all pass; full feature matrix (`--features tls,metrics,a2a,llm`) at 0 warnings |
| 19 | Observability | 8 | Prometheus + Grafana, `#[tracing::instrument]` on 11 critical methods, `task_count`, `/consensus/{slot}`; OTEL still only in skillrunner |
| 20 | Debuggability | 8 | `/consensus/{slot}`, `task_count`, `peer_drop_counts()`, KV dump; handle typesystem makes ownership traces easier to follow; consensus ballot state not directly inspectable via API |
| 21 | Operational Readiness | 9 | `/ready`, `shutdown_with_timeout`, persistence (`SyncMode::Flush`), Docker Compose health-check wiring, chapter 13 topology guide, `is_ready()` Kubernetes readiness probe |
| 22 | Evolvability | 8 | Wire version policy (`WIRE_VERSION` + `PREV_WIRE_VERSION`); CHANGELOG under `[Unreleased]`; ROADMAP v2 milestones; 8-handle facade documented as the stable public surface going forward |
| 23 | Documentation | 9 | All 13 guide chapters present; CONTRIBUTING.md with CLA, build matrix, layer rules, wire-version policy; philosophy.html; API examples use `agent.llm()` / `agent.mcp()` syntax throughout; no chapter gaps |
| 24 | Developer Experience | 9 | `cargo build --lib --no-default-features` clean; CONTRIBUTING.md; CLAUDE.md onramp; Makefile; tracing spans on critical paths; handle pattern is learnable from one example; no CI config in repo remains the main gap |
| 25 | Dependency Hygiene | 9 | `tokio test-util` in `[dev-dependencies]`; `reqwest` optional (`gateway` feature only); `papaya` single concurrent map throughout; all optional deps correctly feature-gated; `Cargo.lock` present; `--no-default-features` compiles cleanly |
| — | **Mean** | **8.6** | |

---

## 2026-06-07 — Run 11

Changes since Run 10: `KvStore`/`KvState` architectural split (`src/store.rs`) with `Deref<Target=KvStore>` keeping all call sites unchanged; Layer I/II Bridge Invariant table added to `CLAUDE.md` naming `apply_and_notify` and `subscribe/subscribe_prefix` as the sole crossing points; Lock-Order Table added to `CLAUDE.md` documenting 6 lock sites with the "no simultaneous acquisition" invariant; `test_lww_convergence_two_concurrent_writers` and `test_cross_group_propose_requires_all_group_quorums` unit tests added (290 tests total with full feature matrix); `kv_payload_size` and `capability_resolve` Criterion benchmarks added to `benches/throughput.rs`; `#[non_exhaustive]` added to all 9 public error/result enums (`GossipError`, `ConsistencyError`, `RpcError`, `QuorumError`, `ScatterError`, `ShardError`, `BulkError`, `SchemaError`, `McpError`); `read_frame_accepts_prev_wire_version` wire rolling-upgrade test added; `consistent_get` doc updated with staleness bound (`anti_entropy_interval_secs`, default 30 s).

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | KvStore/KvState split is a Layer I/II structural improvement — entirely consistent with the substrate-not-platform philosophy; no feature drift |
| 2 | Conceptual Integrity | 9 | Naming and idiom consistent throughout; `KvStore` vs `KvState` distinction is clear and documented; no new inconsistencies introduced |
| 3 | Architecture | 9 | `KvStore` (Layer I) / `KvState` (Layer II bridge) split materialises the documented conceptual separation in code; CLAUDE.md bridge invariant names the two crossing points; `Deref` ensures zero call-site churn; `TaskCtx` God Object deferred to v2 with explicit roadmap |
| 4 | Modularity | 9 | Unchanged from Run 10 — eight independently storable handles; KvStore/KvState split adds internal clarity without changing external handle boundaries |
| 5 | API Design | 9 | Unchanged from Run 10; no new surface changes |
| 6 | Error Handling Model | 8 | `#[non_exhaustive]` now on all 9 public error enums — semver-safe variant additions guaranteed; `Network(String)` / `Config(String)` catch-all variants remain, no structured error code taxonomy |
| 7 | Configurability | 9 | Unchanged from Run 10 |
| 8 | Language Best Practices | 9 | Unchanged; `#[non_exhaustive]` adds idiomatic forward-compatibility signal |
| 9 | Concurrency Correctness | 9 | Lock-Order Table documents 6 lock sites with the "no simultaneous acquisition" invariant; `!Send` Mutex guard compiler enforcement noted; papaya pin() guard invariant documented; no formal deadlock proof but the table precludes the classic acquire-in-different-order pattern |
| 10 | Resource Management | 8 | Unchanged from Run 10; spawned task ceiling still undocumented per operation type |
| 11 | Semantic Correctness | 9 | `test_lww_convergence_two_concurrent_writers` verifies HLC-ordered convergence (sequenced writes prevent concurrent-equal-timestamp ambiguity); `test_cross_group_propose_requires_all_group_quorums` proves split-brain property for multi-group proposals; `consistent_get` staleness bound (30 s) documented; epidemic Paxos vs true linearizability gap acknowledged but not formally analysed |
| 12 | Robustness | 9 | Unchanged from Run 10 |
| 13 | Security | 8 | Unchanged from Run 10; no gossip rate-limiting; `compliance` feature unimplemented |
| 14 | Failure Mode Legibility | 8 | Unchanged from Run 10 |
| 15 | Performance | 9 | `kv_payload_size` benchmarks (64/1 024/65 536 bytes) show framing cost scaling; `capability_resolve` benchmarks (1/10/50/100 providers) characterise O(providers) scan; bench file fully updated to sub-handle API; no hot-path regressions; consensus round-trip not yet benchmarked |
| 16 | Scalability | 8 | Unchanged from Run 10 |
| 17 | Testability | 8 | Unchanged from Run 10; `KvStore` slightly more isolatable but `TaskCtx` wiring unchanged |
| 18 | Test Architecture | 9 | 290 unit (full feature matrix) + 12 integration + 2 fuzz + 3 overlay + 2 scale + proptest; two new targeted correctness tests (LWW convergence, cross-group split-brain); wire rolling-upgrade test in framing.rs; five-tier pyramid intact |
| 19 | Observability | 8 | Unchanged from Run 10 |
| 20 | Debuggability | 8 | Unchanged from Run 10 |
| 21 | Operational Readiness | 9 | Unchanged from Run 10 |
| 22 | Evolvability | 9 | `#[non_exhaustive]` on all 9 public error enums — wire-safe downstream match arms; `read_frame_accepts_prev_wire_version` test verifies rolling-upgrade window; CHANGELOG [Unreleased] comprehensive; ROADMAP v2 milestones current; wire version policy documented and tested end-to-end |
| 23 | Documentation | 9 | Unchanged from Run 10 |
| 24 | Developer Experience | 9 | Lock-Order Table and Layer I/II Bridge Invariant in CLAUDE.md improve contributor onramp for concurrency-sensitive work |
| 25 | Dependency Hygiene | 9 | Unchanged from Run 10 |
| — | **Mean** | **8.7** | |
