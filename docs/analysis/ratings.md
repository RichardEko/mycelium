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
