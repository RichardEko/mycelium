# Mycelium — Analysis Ratings

Project evaluation across 25 orthogonal dimensions, tracked over time.
Each run is appended by `/mycelium-analysis`. Higher is better; 10 = no
meaningful improvement possible at the current stage of the project.

**Methodology note:** Runs 1–15 used methodology v1 (read-and-rate). From
Run 16, methodology v2 (M2) applies: execution-evidence gate (9 requires
named execution evidence from the run; 10 requires external validation),
mandatory falsification probes against the top-3 dimensions, rotating
five-dimension deep-dives, blind scoring, a cadence gate, and this
calibration ledger. Do not compare absolute scores across the v1/v2 boundary.

## Calibration Ledger

Records bugs later found in dimensions that scored ≥ 8 while the bug already
existed. This is the framework's own report card.

- 2026-06-10: **Concurrency Correctness** scored 8–9 in Runs 9–15 while the
  consensus listener registration race existed (handlers registered on the
  task's first poll, so proposals racing listener startup were silently
  dropped — node fails to vote). Found by an adversarial test during the
  lease/tripwire work; fixed same day (synchronous registration in
  `start_consensus_listener`).
- 2026-06-10: **Documentation** scored 8–9 in Runs 10–15 while CLAUDE.md
  conflated the wire hop-count TTL with key evaporation ("every key has a
  TTL"); evaporation is a read-side freshness convention
  (`CapEntry::is_fresh`), and the store never time-evicts live keys. Found by
  doc-vs-code cross-check during a philosophy audit; docs corrected same day.
- 2026-06-10: **Semantic Correctness** scored 8–9 in Runs 11–15 while LWW
  diverged permanently on equal-timestamp concurrent data writes (first
  arrival won; anti-entropy could not detect it because the digest hashes
  key+timestamp only). Found by M2 falsification probe in Run 16; fixed same
  day (`lww_wins` deterministic data-vs-data tiebreak).

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

---

## 2026-06-07 — Run 12

Changes since Run 11: Anti-entropy timing resonance bug fixed — cooldown reduced from `interval_secs` to `interval_secs - 1` in `src/connection.rs`, eliminating the race where the health monitor's retry could arrive at the exact cooldown boundary; bootstrap peer re-trigger loop added to health monitor in `src/agent/tasks.rs` so nodes in `cached_ping_targets` (bootstrap peers) correctly re-trigger anti-entropy on reconnect after cluster restart; scenario 04 (full-cluster restart with WAL recovery) now passes consistently; `docs/operations/tuning.md` added (318 lines) with quick-reference table of 16 parameters, 5 hard invariants with mathematical bounds, scaling guidelines for 4 cluster size ranges, reconnect storm mitigations, tombstone safety window formula, and monitoring checklist; structured error variants (`InvalidField`, `FieldConflict`, `NodeIdMismatch`, `FrameTooLarge`, `UnsupportedWireVersion`) replace stringly-typed `Config(String)` / `Network(String)` catch-alls; `max_inbound_frames_per_sec` and `max_concurrent_bulk_handlers` added to `GossipConfig` with env vars; 100-node scale test and 21-node resilience test (3 churn cycles) confirm no regressions.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Holland/Jini/OSGi/Paremus synthesis fully honored; library-not-platform positioning intact; KvStore/KvState split maintains substrate purity; no feature drift |
| 2 | Conceptual Integrity | 9 | Eight-handle facade consistent across all domains; naming unambiguous; `docs/operations/tuning.md` invariants match code behaviour; no new idiom inconsistencies |
| 3 | Architecture | 9 | Three-layer model enforced by namespace table; KvStore/KvState split materialises Layer I/II boundary; gateway feature gate clean; entanglement documented with v2 roadmap |
| 4 | Modularity | 9 | Eight independently understandable, moveable, storable handles; TaskCtx shared state correctly deferred to v2 workspace split; internal coupling explicit and documented |
| 5 | API Design | 9 | `CapabilityReg` return type makes drop semantics explicit; `#[must_use]` on `advertise_capability`; eight-handle pattern minimal and orthogonal; no footguns in core paths |
| 6 | Error Handling Model | 8 | `InvalidField`/`FieldConflict`/`NodeIdMismatch` structured variants replace stringly-typed catch-alls; `#[non_exhaustive]` on all 9 public error enums; `Network(String)` still present for unclassified I/O errors; no structured error code taxonomy |
| 7 | Configurability | 9 | `max_inbound_frames_per_sec` and `max_concurrent_bulk_handlers` added; all 16 tuning parameters now documented with hard invariants in `docs/operations/tuning.md`; operational knobs exposed without code changes |
| 8 | Language Best Practices | 9 | 0 clippy warnings at full feature matrix; `#![deny(unsafe_code)]`; no production `unwrap()`; `#[non_exhaustive]` on all public error enums; proptest on core invariants |
| 9 | Concurrency Correctness | 9 | Lock-Order Table documents 6 lock sites with no-simultaneous-acquisition invariant; Release/Acquire ordering documented per atomic; `!Send` Mutex enforced by compiler; no new lock sites introduced |
| 10 | Resource Management | 8 | `CapabilityReg` RAII for advertisement lifetime; `task_count` in SystemStats; RAII throughout; per-operation spawned task ceiling (`max_concurrent_bulk_handlers`) now documented and enforced |
| 11 | Semantic Correctness | 9 | Anti-entropy timing resonance eliminated: `interval - 1` cooldown gives deterministic 1 s margin; bootstrap re-trigger loop handles `cached_ping_targets` gap; scenario 04 confirms end-to-end; LWW convergence and cross-group split-brain property-tested |
| 12 | Robustness | 9 | All 12 integration scenarios pass; 100-node scale (0 dropped frames); 21-node resilience (3 churn cycles); Ed25519 fail-closed; `MAX_FRAME_BYTES` bound; `max_inbound_frames_per_sec` guards against misbehaving peers |
| 13 | Security | 8 | mTLS + Ed25519 + gateway bearer token + signed consensus payloads + `signal_rx_from` sender auth; `max_inbound_frames_per_sec` adds basic gossip rate-limiting; no RBAC; `compliance` feature unimplemented |
| 14 | Failure Mode Legibility | 8 | Structured error variants carry field context; `ConsensusResult` variants carry detail; `task_count` exposes leaks; `peer_drop_counts` identifies slow peers; Nack reasons still not surfaced to callers |
| 15 | Performance | 9 | 151 ns set, 16 ns get benchmarked; O(K) gossip fan-out; lock-free hot paths; `kv_payload_size` and `capability_resolve` benchmarks; no hot-path regressions from new config fields |
| 16 | Scalability | 8 | O(N²) TCP cliff documented with cluster-size table in `tuning.md`; `max_active_connections` mitigation operational; `scan_prefix` O(store) fallback documented; v2 SWIM hybrid transport on roadmap |
| 17 | Testability | 8 | Deterministic, injectable, no hidden global state; proptest on LWW/HLC/framing; `ConsensusPair` helper; `EchoBackend` for LLM; `TaskCtx` still wired-through rather than injected |
| 18 | Test Architecture | 9 | 290 unit + 12 integration + 2 fuzz + 3 overlay + 2 scale (100-node + 21-node resilience) + proptest; all pass including previously flaky scenario 04; five-tier pyramid |
| 19 | Observability | 8 | Prometheus + Grafana; `#[tracing::instrument]` on critical methods; `task_count`; `/consensus/{slot}`; monitoring checklist in `tuning.md`; OTEL still only in skillrunner |
| 20 | Debuggability | 8 | `/consensus/{slot}`, `task_count`, `peer_drop_counts()`, KV dump; monitoring checklist in `tuning.md`; consensus ballot state not directly inspectable via API |
| 21 | Operational Readiness | 9 | `/ready`, `shutdown_with_timeout`, persistence (`SyncMode::Flush`), Docker Compose health-check wiring, `is_ready()` Kubernetes readiness semantics; `docs/operations/tuning.md` fills the operational runbook gap |
| 22 | Evolvability | 9 | `#[non_exhaustive]` on all 9 public error enums; `WIRE_VERSION`/`PREV_WIRE_VERSION` tested end-to-end; CHANGELOG [Unreleased] comprehensive; ROADMAP v2 milestones current |
| 23 | Documentation | 9 | All 13 guide chapters; CONTRIBUTING.md; philosophy.html; API examples use sub-handle syntax; `docs/operations/tuning.md` adds operational runbook with hard invariants; 5 invariants have mathematical proofs |
| 24 | Developer Experience | 9 | `cargo build --lib --no-default-features` clean; CONTRIBUTING.md + CLAUDE.md onramp; tuning invariants help operators configure correctly without reading source; handle pattern learnable from one example |
| 25 | Dependency Hygiene | 9 | `tokio test-util` in `[dev-dependencies]`; `reqwest` optional; `papaya` single concurrent map; all optional deps feature-gated; `Cargo.lock` present; `--no-default-features` compiles cleanly |
| — | **Mean** | **8.7** | |

---

## 2026-06-07 — Run 13

Changes since Run 12: `GossipError::Io` variant doc clarified — explicitly states it fires only during startup-time I/O (TCP listener bind, WAL read, TLS setup); runtime peer errors absorbed internally and surfaced via `dropped_frames`/`peer_drop_counts()`, callers directed to `err.kind()` for sub-classification. `BulkTransport::active_handlers: Arc<AtomicU64>` added — incremented before `tokio::spawn`, decremented via `ActiveHandlerGuard` RAII on task exit (including panic/cancel); surfaced as `SystemStats::active_bulk_handlers`; CLAUDE.md and `docs/operations/tuning.md` monitoring checklist updated. ROADMAP.md v2.0 Milestones extended with items 6 (RBAC gossip-level authorization, `compliance` feature, trigger = regulated-industry deployment) and 7 (cluster-wide distributed rate-limiting, trigger = confirmed intra-cluster abuse). All 276 unit tests, 12 integration scenarios, 100-node scale, and 21-node resilience pass.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Holland/Jini/Paremus synthesis fully honored; library-not-platform intact; no feature drift |
| 2 | Conceptual Integrity | 9 | Eight-handle facade consistent; `CapabilityReg` naming resolved last ambiguity; all idioms aligned |
| 3 | Architecture | 9 | KvStore/KvState split materialises Layer I/II boundary; namespace table enforced; gateway feature gate clean; entanglement documented with v2 roadmap |
| 4 | Modularity | 9 | Eight independently understandable, moveable, storable handles; TaskCtx coupling explicit; v2 workspace split roadmapped |
| 5 | API Design | 9 | `CapabilityReg` drop semantics explicit at type level; `#[must_use]` on advertisement; eight-handle facade minimal and orthogonal; no footguns |
| 6 | Error Handling Model | 8 | `Io` doc now distinguishes startup-vs-runtime boundary and directs callers to `err.kind()`; `#[non_exhaustive]` on all 9 public enums; numerous infallible `.expect()` are correct but no structured taxonomy between them |
| 7 | Configurability | 9 | 16+ tunable parameters with hard invariants and scaling guidelines; tuning.md makes the surface approachable; all knobs exposed without code changes |
| 8 | Language Best Practices | 9 | 0 clippy warnings at full feature matrix; `#![deny(unsafe_code)]`; `ActiveHandlerGuard` RAII idiomatic; infallible `.expect()` documented with invariant strings |
| 9 | Concurrency Correctness | 9 | Lock-Order Table documents 6 sites; `active_handlers` uses Relaxed (correct for a diagnostic counter); `ActiveHandlerGuard` covers drop/panic; no new contention |
| 10 | Resource Management | 9 | `active_bulk_handlers` via `ActiveHandlerGuard` closes the last untracked resource gap; all lifecycle patterns RAII-governed and observable |
| 11 | Semantic Correctness | 9 | Anti-entropy timing resonance eliminated; bootstrap re-trigger loop correct; LWW convergence and split-brain property-tested; scenario 04 reliable |
| 12 | Robustness | 9 | All 12 integration + 100-node scale + 21-node resilience (3 churn cycles) pass; Ed25519 fail-closed; `MAX_FRAME_BYTES`; per-peer rate-limiting |
| 13 | Security | 8 | mTLS + Ed25519 + bearer token + signed consensus + `signal_rx_from` + per-peer rate-limiting; RBAC and cluster-wide rate-limiting now formally documented v2 milestones with design sketches |
| 14 | Failure Mode Legibility | 8 | Structured error variants carry field context; `active_bulk_handlers` surfaces handler saturation; `Io` doc points to `err.kind()`; Nack reasons still not surfaced to callers |
| 15 | Performance | 9 | 151 ns set, 16 ns get; O(K) fan-out; lock-free hot paths; `active_handlers` counter is Relaxed atomic — zero hot-path cost |
| 16 | Scalability | 8 | O(N²) TCP cliff documented; `max_active_connections` + `max_inbound_frames_per_sec` mitigations operational; v2 SWIM transport and distributed rate-limiting roadmapped |
| 17 | Testability | 8 | Deterministic, injectable, no hidden global state; proptest on LWW/HLC/framing; `ConsensusPair` helper; `TaskCtx` still wired-through not injected |
| 18 | Test Architecture | 9 | 276 unit + 12 integration + 2 fuzz + scale + resilience + proptest; five-tier pyramid; all pass; full feature matrix at 0 warnings |
| 19 | Observability | 8 | Prometheus + Grafana + tracing spans + `task_count` + `active_bulk_handlers` + `/consensus/{slot}`; tuning.md checklist updated; OTEL still only in skillrunner |
| 20 | Debuggability | 8 | `/consensus/{slot}` + `task_count` + `active_bulk_handlers` + `peer_drop_counts()` + KV dump + tuning.md checklist; consensus ballot state not directly inspectable |
| 21 | Operational Readiness | 9 | `/ready` + `shutdown_with_timeout` + `SyncMode::Flush` + Docker health-checks + tuning.md with 5 hard invariants + monitoring checklist includes `active_bulk_handlers` |
| 22 | Evolvability | 9 | `#[non_exhaustive]` on all 9 enums; wire version policy tested; ROADMAP v2 milestones now 7 items (RBAC + distributed rate-limiting explicitly designed); CHANGELOG comprehensive |
| 23 | Documentation | 9 | All 13 guide chapters; CONTRIBUTING.md; `error.rs` doc explains startup-vs-runtime I/O boundary; tuning.md monitoring checklist complete; sub-handle API examples consistent |
| 24 | Developer Experience | 9 | No-default-features clean; CLAUDE.md + CONTRIBUTING.md onramp; tuning invariants let operators configure without reading source; `active_bulk_handlers` observable without code changes |
| 25 | Dependency Hygiene | 9 | `tokio test-util` in `[dev-dependencies]`; `reqwest` optional; all optional deps feature-gated; `Cargo.lock` present; `--no-default-features` compiles cleanly |
| — | **Mean** | **8.8** | |

---

## 2026-06-08 — Run 14

Changes since Run 13: no code changes (`Cargo.lock`, `docs/analysis/ratings.md`, `docs/philosophy.html` are the only working-tree modifications). Today's substantive work is a documentation/theoretical expansion: `docs/philosophy.html` now carries §9 (Promise Theory Convergence — Holland scope distinction vs Burgess), §10 (Subsidiarity Principle — Ostrom polycentric governance + Olson capture dynamics + Bookchin/Öcalan mandate TTL + Dewey epistemic symmetry); seven properties (1–4 epistemic correctness, 5 Capture Resistance, 6 Mandate TTL, 7 Epistemic Symmetry) and four failure modes (I acute collapse, II chronic erosion, III internal capture, IV class entrenchment) are now articulated. `docs/internal/paper2_substrate_convergence.html` working draft exists for Paper 2a (HLKS convergence, SFI target) and 2b (capture dynamics). `docs/internal/compliance_audit_mechanics.html` added for SOC 2 / HIPAA operational mechanics. Paper 1 ("The Coordinator Trap", Nicholson 2026) is now confirmed on SSRN; source at `docs/arxiv/main.tex`. 290 unit tests pass at full feature matrix `--features tls,metrics,a2a,llm`.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Philosophy document now articulates four-tradition convergence (Holland/Hayek/Burgess/Ostrom) plus mandate TTL (Bookchin/Öcalan) and epistemic symmetry (Dewey); seven substrate properties and four failure modes formalised; implementation continues to satisfy Properties 1–4 by design and Property 6 structurally (TTL'd KV entries dissolving Layer III roles); no feature drift; theoretical grounding now deeper than implementation needs strictly require |
| 2 | Conceptual Integrity | 9 | Eight-handle facade consistent; `CapabilityReg` naming resolved; all idioms aligned; philosophy framework consistent across philosophy.html, paper2 draft, and CLAUDE.md |
| 3 | Architecture | 9 | KvStore/KvState split materialises Layer I/II boundary; namespace table enforced; gateway feature gate clean; entanglement documented with v2 roadmap; no regressions |
| 4 | Modularity | 9 | Eight independently understandable, moveable, storable handles; TaskCtx coupling explicit; v2 workspace split roadmapped |
| 5 | API Design | 9 | `CapabilityReg` drop semantics explicit at type level; `#[must_use]` on advertisement; eight-handle facade minimal and orthogonal; no footguns |
| 6 | Error Handling Model | 8 | `Io` doc distinguishes startup-vs-runtime boundary; `#[non_exhaustive]` on all 9 public enums; structured `InvalidField`/`FieldConflict`/`NodeIdMismatch` variants; `Network(String)` catch-all remains for unclassified I/O; no structured error code taxonomy |
| 7 | Configurability | 9 | 16+ tunable parameters with hard invariants and scaling guidelines in `docs/operations/tuning.md`; all knobs exposed without code changes |
| 8 | Language Best Practices | 9 | 0 clippy warnings at full feature matrix; `#![deny(unsafe_code)]`; `ActiveHandlerGuard` RAII idiomatic; infallible `.expect()` documented with invariant strings |
| 9 | Concurrency Correctness | 9 | Lock-Order Table documents 6 sites; `active_handlers` Relaxed correct for diagnostic counter; `ActiveHandlerGuard` covers drop/panic; memory ordering policy codified in CLAUDE.md |
| 10 | Resource Management | 9 | `active_bulk_handlers` via `ActiveHandlerGuard` closes the last untracked resource gap; all lifecycle patterns RAII-governed and observable |
| 11 | Semantic Correctness | 9 | Anti-entropy timing resonance eliminated (`interval-1` cooldown); bootstrap re-trigger loop correct; LWW convergence and split-brain property-tested; epidemic Paxos vs true linearizability gap acknowledged in docs |
| 12 | Robustness | 9 | All 12 integration + 100-node scale + 21-node resilience pass; 290 unit tests at full feature matrix; Ed25519 fail-closed; `MAX_FRAME_BYTES`; per-peer rate-limiting |
| 13 | Security | 8 | mTLS + Ed25519 + bearer token + signed consensus + `signal_rx_from` + per-peer rate-limiting; RBAC and cluster-wide rate-limiting are documented v2 milestones with design sketches; `compliance` feature unimplemented |
| 14 | Failure Mode Legibility | 8 | Structured error variants carry field context; `active_bulk_handlers` surfaces handler saturation; `Io` doc points to `err.kind()`; Nack reasons still not surfaced to callers |
| 15 | Performance | 9 | 151 ns set, 16 ns get; `kv_payload_size` and `capability_resolve` benchmarks; O(K) fan-out; lock-free hot paths; `active_handlers` counter is Relaxed atomic — zero hot-path cost |
| 16 | Scalability | 8 | O(N²) TCP cliff documented with cluster-size table; `max_active_connections` + `max_inbound_frames_per_sec` mitigations operational; v2 SWIM hybrid transport and distributed rate-limiting on roadmap |
| 17 | Testability | 8 | Deterministic, injectable, no hidden global state; proptest on LWW/HLC/framing; `ConsensusPair` helper; `EchoBackend` for LLM; `TaskCtx` still wired-through rather than injected |
| 18 | Test Architecture | 9 | 290 unit (full feature matrix `tls,metrics,a2a,llm`) + 12 integration + 2 fuzz + 3 overlay + 2 scale (100-node + 21-node resilience) + proptest; five-tier pyramid; all pass |
| 19 | Observability | 8 | Prometheus + Grafana + tracing spans + `task_count` + `active_bulk_handlers` + `/consensus/{slot}`; tuning.md checklist; OTEL still only in skillrunner, not core |
| 20 | Debuggability | 8 | `/consensus/{slot}` + `task_count` + `active_bulk_handlers` + `peer_drop_counts()` + KV dump + tuning.md checklist; consensus ballot state not directly inspectable from API |
| 21 | Operational Readiness | 9 | `/ready` + `shutdown_with_timeout` + `SyncMode::Flush` + Docker health-checks + `docs/operations/tuning.md` with 5 hard invariants + monitoring checklist |
| 22 | Evolvability | 9 | `#[non_exhaustive]` on all 9 public enums; wire version policy tested; ROADMAP v2 milestones now include RBAC, distributed rate-limiting, self-tuning metabolism (8–10); CHANGELOG comprehensive |
| 23 | Documentation | 9 | All 13 guide chapters; CONTRIBUTING.md; `docs/philosophy.html` now articulates four-tradition cross-domain framework with seven properties and four failure modes; Paper 1 on SSRN; Paper 2a/2b draft at `docs/internal/paper2_substrate_convergence.html`; `docs/internal/compliance_audit_mechanics.html` added; README capabilities section migration guide for flat-API → sub-handle still absent |
| 24 | Developer Experience | 9 | `cargo build --lib --no-default-features` clean; CLAUDE.md + CONTRIBUTING.md onramp; tuning invariants let operators configure without reading source; eight-handle pattern learnable from one example |
| 25 | Dependency Hygiene | 9 | `tokio test-util` in `[dev-dependencies]`; `reqwest` optional; all optional deps feature-gated; `Cargo.lock` present; `--no-default-features` compiles cleanly |
| — | **Mean** | **8.8** | |

---

## 2026-06-10 — Run 15

Changes since Run 14: v1.1.0 released (commercial-license note in `Cargo.toml`, Tathata Systems); ROADMAP v1.x gaps #7 (durable audit trail), #8 (RBAC subset), #9 (SSO/Entra) committed, plus an uncommitted 87-line **production-hardening gate** section naming four sub-gates (AuthN/Z+RBAC, tamper-evident audit, crown-jewel posture, support/SLA) as the regulated-buyer evaluation lens; **entry-volume scale test** added (`make test-scale-entries` — 30 nodes, 5 000×512 B entries, 7 phases measuring live-gossip fraction, anti-entropy sweep tail, stability, random-sample integrity, backpressure; burst and paced write modes; documented in CLAUDE.md with rationale for staying below the iptables ceiling); **`examples/coordinator_comparison.rs`** (+ plot/runner scripts) — empirical companion to Paper 2a measuring misroute rate, staleness, and decision latency for broker-mediated vs locally-resolved routing on the identical substrate; compiles clean. Verified this run: 290/290 unit tests at full feature matrix (`tls,metrics,a2a,llm`), clippy 0 warnings.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Holland/Hayek/Burgess/Ostrom synthesis intact; `coordinator_comparison.rs` turns the core thesis into a measurable experiment on the same substrate — the strongest philosophy-to-code alignment artifact yet; no feature drift |
| 2 | Conceptual Integrity | 9 | Eight-handle facade consistent; new example uses current sub-handle API (`kv()`, `service()`) idiomatically; entry-volume test follows established runner/helper conventions |
| 3 | Architecture | 9 | Three-layer model and namespace table unchanged; KvStore/KvState split intact; new work is test/example/roadmap only — no structural change, no regressions |
| 4 | Modularity | 9 | Eight independently storable handles; `TaskCtx` (22 fields) remains the documented God Object with v2 workspace-split direction; no new coupling |
| 5 | API Design | 9 | No surface changes; `CapabilityReg` drop semantics, `#[must_use]`, eight-handle orthogonality all persist; example code demonstrates the API reads naturally at ~350 lines |
| 6 | Error Handling Model | 8 | No changes; structured variants + `#[non_exhaustive]` on all 9 public enums; `Network(String)` catch-all remains; no structured error code taxonomy |
| 7 | Configurability | 9 | Entry-volume test exercises `GOSSIP_WRITER_CHANNEL_DEPTH=4096` and `GOSSIP_MAX_STORE_ENTRIES=200000` overrides with documented rationale — config surface proven tunable for volume workloads without code changes |
| 8 | Language Best Practices | 9 | 0 clippy warnings at full feature matrix verified this run; `#![deny(unsafe_code)]`; example code clean (env-var `unwrap()`s acceptable in example context) |
| 9 | Concurrency Correctness | 9 | Lock-Order Table (6 sites, no nested acquisition) unchanged; memory ordering policy intact; no new concurrency code in library |
| 10 | Resource Management | 9 | No changes; `ActiveHandlerGuard` RAII, `task_count`/`active_bulk_handlers` observability persist |
| 11 | Semantic Correctness | 9 | 290/290 tests pass; LWW convergence and cross-group split-brain property-tested; anti-entropy timing fixes persist; entry-volume test adds end-to-end convergence-completeness verification (5 000 entries, byte-count integrity sample) |
| 12 | Robustness | 9 | No changes; Ed25519 fail-closed, mutex poison recovery, frame caps, per-peer rate-limiting all persist; entry-volume test confirms no eviction/flapping at 5 000-entry volume |
| 13 | Security | 8 | Production-hardening gate section converts vague "harden it" into four concrete sub-gates with existing/in-flight/to-design inventory per gate — excellent planning legibility, but gaps #7–#9 remain unimplemented; crown-jewel posture (egress policy, at-rest encryption, threat model) identified as new work |
| 14 | Failure Mode Legibility | 8 | No changes; Nack reasons still not surfaced to callers; entry-volume test's backpressure phase gives operators an actionable `dropped_frames` → channel-depth diagnostic |
| 15 | Performance | 9 | No hot-path changes; benchmarks persist; entry-volume test adds write-throughput and convergence-latency measurement at the system level |
| 16 | Scalability | 8 | Entry-volume axis now empirically characterised (live-gossip fraction vs anti-entropy sweep tail at 5 000 entries × 30 nodes) — the second scale axis is no longer untested; O(N²) TCP topology cliff still present in v1.x with SWIM transport deferred to v2 |
| 17 | Testability | 8 | No structural changes; `TaskCtx` still wired-through rather than injected; deterministic helpers persist |
| 18 | Test Architecture | 9 | Suite now spans six tiers: 290 unit + proptest + 2 fuzz + 12 integration + 3 overlay + 3 scale variants (100-node node-count, 21-node resilience, 30-node entry-volume) — the new test covers the previously unvalidated volume axis with explicit phase structure; consensus-protocol property tests still absent |
| 19 | Observability | 8 | No changes; Prometheus + Grafana + tracing spans + `task_count` + `active_bulk_handlers` + `/consensus/{slot}`; OTEL still only in skillrunner |
| 20 | Debuggability | 8 | No changes; consensus ballot state still not directly inspectable beyond `/consensus/{slot}` |
| 21 | Operational Readiness | 9 | Entry-volume test doubles as a capacity-planning tool (paced mode simulates sustained-rate load; overrides documented in Makefile help); `/ready`, `shutdown_with_timeout`, tuning.md invariants persist |
| 22 | Evolvability | 9 | Production-hardening gate maps every security gap to a procurement-facing sub-gate with explicit existing/in-flight/to-design status — debt is not just documented but sequenced; v1.x gaps #7–#9 and v2 milestones 8–10 committed; wire policy unchanged |
| 23 | Documentation | 9 | CLAUDE.md entry-volume section explains both the what and the why-30-nodes; coordinator_comparison doc-comment ties code to Paper 2a §9; philosophy.html §9–§10 (Promise Theory, Subsidiarity) carried from Run 14; all 13 guide chapters persist |
| 24 | Developer Experience | 9 | `make test-scale-entries` with `ENTRY_COUNT`/`ENTRY_BYTES`/`WRITE_DELAY_MS`/`SCALE_ENTRIES_WORKERS` overrides and worked examples in Makefile comments; no CI config in repo remains the main gap |
| 25 | Dependency Hygiene | 9 | No dependency changes; v1.1.0 published with all optional deps feature-gated; transitive `block v0.1.6` future-incompat warning (dev-deps only) persists |
| — | **Mean** | **8.8** | |

---

## 2026-06-10 — Run 16 (M2)

**Methodology v2 rebaseline** — first run under the execution-evidence gate,
falsification quota, and calibration ledger. Scores are not comparable to the
v1 series; the step change from 8.8 to 8.1 is methodological, not regression.
Blind-scoring exemption: prior runs were unavoidably in context this session;
the blind rule applies from Run 17.

Deep-dive dimensions this run: 9 (Concurrency), 10 (Resource Mgmt),
11 (Semantic Correctness), 12 (Robustness), 18 (Test Architecture).
Next run by rotation: 1–5.

Execution evidence this run: 302/302 unit tests at full feature matrix
(`tls,metrics,a2a,llm`); clippy 0 warnings (`-D warnings`, full matrix);
`--no-default-features` build; all examples build; 12/12 integration
scenarios; 100-node scale 5/5 with 0 dropped frames (fresh Docker VM);
21-node resilience 10/10; 30-node entry-volume 6/6 (100% live-gossip
fraction); 3 falsification probes executed.

Changes since Run 15: epoch-leased commitments (`committed_lease_secs` +
`consensus/lease/{slot}` + lease-aware readers); commit-conflict tripwire
(`SystemStats::commit_conflicts`, `/stats`); proposer-side clobber guard;
consensus listener registration race fixed (synchronous handler
registration); LWW equal-timestamp tiebreak (`lww_wins`); philosophy §5a
(Anderson / symmetry breaking / corrected litmus); CLAUDE.md hop-TTL vs
evaporation correction + Layer III invariant-posture section; namespace
table promise-strength note.

### Findings

- **Major (fixed in-run)** — *Semantic Correctness*: LWW diverged permanently
  on equal-timestamp concurrent data writes; first arrival won on each node,
  and the value-blind anti-entropy digest (key ⊕ timestamp) made the
  divergence undetectable. Probe: `lww_equal_timestamp_concurrent_data_converges`
  (initially failed: node1=`from-a`, node2=`from-b`). Fixed with deterministic
  data-vs-data tiebreak in `lww_wins`; probe kept as regression test;
  calibration ledger entry recorded. Dimension capped at 6 this run per M2.
- **Probe passed** — *Robustness*: live agent survived garbage bytes, a 4 GiB
  length-prefix announcement, and zero-length frames on the gossip port; no
  dead shards, fully serviceable after (`probe_garbage_on_gossip_port_survives`,
  kept).
- **Probe passed** — *Resource Management*: `shutdown_with_timeout` drains all
  tracked tasks to 0 and releases the gossip port for rebinding
  (`probe_shutdown_drains_tasks_and_releases_port`, kept).
- **Test-infra (not a dimension cap)** — repeated same-day 100-node rounds
  degrade Docker VM formation (PASS → 80/100 → 97/100); fresh engine restart
  → 5/5 with 0 drops. Documented in CLAUDE.md.

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 8 | Reading-capped (M2). §5a closes the Layer III derivation gap (Anderson level-laws, corrected litmus #3, mandate-TTL-for-decisions); lease feature is the philosophy applied to code; `coordinator_comparison` exists but was not executed this run |
| 2 | Conceptual Integrity | 8 | Reading-capped. Lease deliberately reuses the read-side evaporation convention (`CapEntry::is_fresh` pattern) — idiom-consistent; tripwire follows the diagnostics-counter idiom |
| 3 | Architecture | 8 | Reading-capped. Tripwire/lease kept wholly in Layer III (no substrate write-guard — dependency direction preserved and now documented in CLAUDE.md §Layer III invariant posture); `--no-default-features` build executed |
| 4 | Modularity | 8 | Reading-capped. Eight handles unchanged; `commit_conflicts` added to TaskCtx (the God Object grows by one diagnostic field; v2 split still roadmapped) |
| 5 | API Design | 8 | Reading-capped. `committed_lease_secs: Option<u64>` opt-in with `None` default preserves behaviour; `consensus_rx` raw-view vs `consensus_get` lease-aware split documented |
| 6 | Error Handling Model | 7 | `Network(String)` catch-all and no structured error-code taxonomy persist (v1 finding, unchanged) |
| 7 | Configurability | 8 | Suites exercised env-override surface (`GOSSIP_WRITER_CHANNEL_DEPTH` etc.) but the config surface was not deep-dived this run |
| 8 | Language Best Practices | 9 | **Evidence:** clippy 0 warnings at `-D warnings`, full feature matrix, executed this run; `#![deny(unsafe_code)]`; `lww_wins` extracted as shared comparator rather than duplicated |
| 9 | Concurrency Correctness | 8 | Deep-dive. Listener registration race found and fixed this session (calibration ledger); fix verified by 302 unit + 12/12 integration re-runs. Capped at 8 as the ledger now shows this dimension's artifacts (lock tables, ordering policy) did not predict the race |
| 10 | Resource Management | 9 | Deep-dive + probe. **Evidence:** `probe_shutdown_drains_tasks_and_releases_port` passed (task_count → 0, port rebindable); `ActiveHandlerGuard`/RAII patterns re-verified |
| 11 | Semantic Correctness | 6 | Deep-dive + probe. **Capped: Major finding this run** (LWW equal-timestamp divergence, see Findings). Fixed in-run with deterministic tiebreak + regression test; 302 unit incl. convergence tests pass — expect recovery next run with sustained evidence |
| 12 | Robustness | 9 | Deep-dive + probe. **Evidence:** `probe_garbage_on_gossip_port_survives` passed; 21-node resilience 10/10 re-executed post-LWW-change; Ed25519 fail-closed and frame caps re-verified by suite |
| 13 | Security | 8 | tls-feature unit tests executed (signing/verification paths); tripwire adds violation legibility; gaps #7–#9 (audit, RBAC, SSO) remain unimplemented v1.x line items |
| 14 | Failure Mode Legibility | 8 | Tripwire `warn!` + `commit_conflicts` counter + `lease_expired` on `/consensus/{slot}` all tested this run; consensus Nack reasons still not surfaced to callers |
| 15 | Performance | 8 | Capped: no benchmarks executed this run. `lww_wins` adds a byte-compare only on the equal-timestamp path (nonce-deduped duplicates never reach it) — reading-verified only |
| 16 | Scalability | 8 | **Evidence:** 100-node 5/5 with 0 dropped frames (fresh VM) + 30-node entry-volume 6/6 executed this run; held at 8 because the O(N²) TCP ceiling remains the structural v1.x constraint (SWIM transport is v2) |
| 17 | Testability | 8 | Probes were straightforward to write against public API + test helpers (good sign); `TaskCtx` still wired-through rather than injected |
| 18 | Test Architecture | 9 | Deep-dive. **Evidence:** all tiers executed this run — 302 unit (3 new probes now permanent) + proptest + 2 fuzz targets (built) + 12/12 integration + 3 scale suites; falsification quota now institutionalised in the methodology |
| 19 | Observability | 8 | `commit_conflicts` on `/stats`; suites exercised health/ready/stats endpoints; OTEL still skillrunner-only |
| 20 | Debuggability | 8 | `/consensus/{slot}` now distinguishes never-committed from lease-expired; ballot internals still not inspectable |
| 21 | Operational Readiness | 9 | **Evidence:** all four Docker suites executed this run end-to-end against `/health`, `/ready`, `/stats`; VM-fatigue diagnostic procedure added to CLAUDE.md |
| 22 | Evolvability | 8 | Reading-capped. CHANGELOG discipline maintained (3 Added + 2 Fixed this run, incl. rolling-upgrade note for the LWW tiebreak); wire version unchanged |
| 23 | Documentation | 8 | Reading-capped. Hop-TTL vs evaporation conflation fixed (calibration ledger); §5a + namespace lease row + Layer III posture section added; guide chapters not re-verified this run |
| 24 | Developer Experience | 8 | Reading-capped. Builds + suites green from clean state; no CI config in repo remains the standing gap |
| 25 | Dependency Hygiene | 8 | `--no-default-features` build executed; no dep changes; transitive `block v0.1.6` future-incompat (dev-deps only) persists |
| — | **Floor (lowest 3)** | **6, 7, 8** | Semantic Correctness (6, capped by finding), Error Handling (7), ten dimensions tied at 8 — Security is the most actionable of them |
| — | Mean (continuity footnote) | 8.1 | not a target; M2 step change — see methodology note |

## 2026-06-11 — Run 17 (M2)

Deep-dive dimensions this run (by rotation from Run 16): 1 (Philosophy), 2
(Conceptual Integrity), 3 (Architecture), 4 (Modularity), 5 (API Design).
Next run by rotation: 6–10.

**Process disclosure.** The scoring agent had read Runs 15–16 earlier in the
same working session (for the coordinator_comparison analysis), so the blind
rule is compromised this run; scores were written from this run's own
evidence before re-opening this file, but prior exposure existed.

Diff since Run 16: `mycelium-tuple-space` workspace companion crate (Linda-
style pull buffer: single-lock store, 4-record WAL with atomic
CompleteRecord + epoch'd compaction, primary/secondary replication +
emergent promotion, Auto election with lowest-candidate tie-break,
sys/tuple metrics + pressure pheromone, HTTP gateway, py/ts SDKs);
integration scenario 13; `coordinator_comparison` demoted (doc-comment
scope warning); Paper 1 §3.3/§8/§9.5 and Paper 2a §Two Grades/§Promise
Reading/§Homogenisation Corollary/§Empirical rewrite; two pre-existing
no-default-features warnings fixed in `bulk.rs`. Working tree only — not
yet committed.

Execution evidence this run: companion suite 27 tests across 6 binaries
incl. 3 fresh falsification probes (re-run post-fix); core 304/304 at full
feature matrix (`tls,metrics,a2a,llm`); clippy `-D warnings` clean on both
crates, all targets, both feature configs; `--no-default-features` build 0
warnings; `make test` 13/13 Docker scenarios incl. new scenario 13 (ran
earlier this run, pre-fix; the fix is companion-internal and the companion
suite was re-run post-fix); release perf smoke `wal_throughput_smoke`:
3.79M transient put/take pairs/s, 215k WAL-backed put/take/ack cycles/s.

### Findings

- **Minor — Observability (#19, capped at 6).** `StageState::waiters_count`
  underflowed to ~4.29e9 (u32 wrap) whenever a parked take timed out and a
  later put skipped its dead sender: the timeout path and the dispatch pop
  path both decremented. The garbage value fed `sys/tuple/...waiters`, the
  depth RPC, and `/api/tuple`. Found by new probe
  `metrics_accounting_identity`; fixed in-run (counter is now a strict
  mirror of deque membership — decrement only at pop, under the stage
  lock; underflow structurally impossible) and the probe stays as a
  regression test. No calibration-ledger entry: the bug was introduced and
  caught within the same run's diff — it never existed under a prior ≥ 8
  score. First run in which the falsification quota caught a defect before
  it shipped.

Falsification probes (3, against the three highest provisional scores):
`wal_torn_tail_recovery` (#11 — WAL truncated at EVERY byte offset inside
the final record; recovery exact at all offsets; log re-appendable — PASS);
`metrics_accounting_identity` (#19 — chaotic lifecycle then exact-zero
quiescence accounting — FAIL → finding above → fixed → PASS);
`shutdown_flushes_wal_and_restart_recovers` (#21 — full lifecycle drill:
traffic in four item states, shutdown, restart over same WAL; exactly the
unacked set recovers, acked items do not resurrect — PASS). All three are
permanent regression tests (`store.rs` tests, `tests/probes.rs`).

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | 9 | Deep-dive. The run's central act was adversarial: `coordinator_comparison` (Run 15's "strongest alignment artifact") was invalidated as a tautology measuring its own staleness arithmetic — logged here as the falsification the quota intends. Its replacement is execution-backed: the pull thesis is now a running artifact (scenario 13 in 13/13 Docker suite; failover drill; both papers reframed around two-grades/promise/homogenisation). Evidence: scenario 13 PASS, `failover.rs` suite, `probes.rs` drill |
| 2 | Conceptual Integrity | 8 | Deep-dive. Companion reuses the read-side evaporation idiom (pressure pheromone, capability election), namespace-table convention extended (`tuple/inflight`, `sys/tuple`), error/`#[non_exhaustive]`/handle idioms match core; one deliberate divergence documented in-code (flat capability names — parser rejects `/` in segments). Reading-capped |
| 3 | Architecture | 9 | Deep-dive. Companion-crate boundary is compiler-enforced: public API only, zero core changes required (`with_http_routes` was already public); two architecture conflicts found and resolved by design, not patching (sys/load opacity would false-trigger promotion → own-prefix pheromone; cap-key `/` limit → flat names). Evidence: full-matrix builds + `--no-default-features` 0 warnings executed |
| 4 | Modularity | 8 | Deep-dive. TaskCtx God Object untouched by the entire companion (the strongest modularity evidence yet is what did NOT need changing); mirror state confined to `TupleSpace`; no new cross-handle coupling. Reading-capped |
| 5 | API Design | 8 | Deep-dive. Four-role `TupleRole`, `BackpressureMode`, `#[non_exhaustive]` `TupleError`, Arc-handle pattern consistent with core; `Client` role added when Auto became electing (plan gap); `local_depth` vs `depth` split documented. Reading-capped |
| 6 | Error Handling Model | 7 | `Network(String)` catch-all persists in core; `TupleError::Rpc(String)` reproduces the same untyped-catch-all gap in new code |
| 7 | Configurability | 8 | `TupleConfig` 13 knobs incl. `cap_refresh` as single-knob test cadence (proven across 4 test files); env overrides for scenario 13 (`MYCELIUM_TUPLE_ROLE/NS`); reading-capped |
| 8 | Language Best Practices | 9 | **Evidence:** clippy `-D warnings` clean this run — both crates, all targets, both feature configs; `#![deny(unsafe_code)]` in companion; the two remaining `unwrap()`s are length-guarded slice converts |
| 9 | Concurrency Correctness | 7 | Two consecutive runs with concurrency-accounting defects (Run 16 listener race; Run 17 waiters double-decrement — found by probe, fixed). Lock-order documented in `store.rs` header (WAL→stage→inflight) and `concurrent_100`/storm paths pass, but the ledger pattern warrants structural skepticism, not an 8 |
| 10 | Resource Management | 8 | Companion tasks are abort-on-shutdown and not in agent `task_handles` (documented); shutdown drill verifies WAL flush + clean agent stop but does not assert task/fd counts for companion tasks — that residual gap keeps this at 8 |
| 11 | Semantic Correctness | 9 | **Evidence:** `wal_torn_tail_recovery` passed at every truncation offset; WAL replay/inflight/compaction/epoch-chunk suite; atomic CompleteRecord crash-window test pair; `concurrent_100` exactly-once; core 304/304 incl. Run-16 LWW regression. Recovered from Run 16 cap with sustained + new evidence |
| 12 | Robustness | 8 | Torn-tail recovery proven; record decoder fully bounds-checked (`.get()` throughout) but UNFUZZED while parsing peer-supplied bytes (replicate handler) — gap named, see #18 |
| 13 | Security | 7 | New intra-cluster mutation surface: any peer can `ack`/`replicate` against a tuple namespace (consistent with the documented operator-owned-cluster trust model, but unauthenticated and unreviewed); core tls/signing untouched |
| 14 | Failure Mode Legibility | 8 | 9 tracing sites in companion (promotion warn names ns; requeue warns per id; replication-unconfirmed warns per node); `TupleError` Display strings actionable; pressure value carries depth+timestamp. No log-assertion tests → 8 |
| 15 | Performance | 9 | **Evidence:** release smoke this run — 3.79M transient pairs/s, 215k WAL cycles/s vs plan target 50k (4.3×, with WAL). Hot path measured, not asserted: WAL page-cache append under mutex costs ~4.6 µs/cycle amortised |
| 16 | Scalability | 7 | Single-primary ceiling (documented, sharding designed not built); per-claim inflight KV key gossips cluster-wide (metadata-only but O(cluster) chatter per item); per-put secondary resolve+spawn; no tuple-space entry-volume test yet |
| 17 | Testability | 9 | **Evidence:** 19 store tests run in 0.05 s with zero cluster (transient mode + temp WAL); all three probes were writable in minutes against public/`pub(crate)` seams — the quota itself is the testability measurement |
| 18 | Test Architecture | 8 | Pyramid: 19 unit + 8 e2e (5 files) + scenario 13 + 3 probes-as-regressions + ignored perf smoke. Gaps: no fuzz target for the WAL/replicate record decoder (core has fuzz for its own decoders; the new adversarial surface is uncovered), no property tests on replay |
| 19 | Observability | 6 | **Capped: finding this run** (waiters_count underflow shipped garbage to sys/tuple metrics, depth RPC, /api/tuple). Fixed in-run + regression kept; surface itself is rich (role/depth/inflight/totals/p99/pressure + /api/tuple aggregation verified in scenario 13 phase IV) — expect recovery next run with sustained evidence |
| 20 | Debuggability | 8 | Inflight keys + pressure pheromone are operator-readable JSON in plain KV; `/api/tuple` one-call cluster view; WAL has test-only reader but no operator dump tool |
| 21 | Operational Readiness | 9 | **Evidence:** lifecycle drill probe passed (shutdown fsync → restart → exact unacked-set recovery, four item states); failover promotion drill; 13/13 scenarios; promotion latency documented (≈3× cap_refresh) |
| 22 | Evolvability | 7 | Companion WAL has no format-version/magic header: an unknown record kind reads as a torn tail → silent truncation of old logs on any future format change (upgrade data-loss hazard, named improvement target); core wire policy (v11/v10 window) unchanged |
| 23 | Documentation | 8 | CLAUDE.md §TupleSpace (key facts for future sessions), 952-line plan doc now implemented, both papers updated, crate-level rustdoc with passing doctest; gaps: no guide chapter; `paper2a/main.html` stale vs `main.tex` |
| 24 | Developer Experience | 8 | `cargo -p mycelium-tuple-space` workflows fast (unit suite 0.05 s); single-knob test cadence; still no CI config in repo |
| 25 | Dependency Hygiene | 9 | **Evidence:** `--no-default-features` 0-warning build executed this run (was 3 warnings — fixed in `bulk.rs`); companion adds zero new transitive deps (papaya/parking_lot/bytes/base64 all already in core's tree; gateway deps optional); `block v0.1.6` (dev-deps) persists |
| — | **Floor (lowest 3)** | **6, 7, 7** | Observability (6, capped by finding); five-way tie at 7 — Error Handling, Concurrency Correctness, Security, Scalability, Evolvability; Concurrency and Evolvability are the most actionable |
| — | Mean (continuity footnote) | 8.0 | not a target; see M2 preamble |
