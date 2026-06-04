Evaluate the Mycelium project across 25 orthogonal dimensions, rate each 1–10,
then append a timestamped entry to `docs/analysis/ratings.md` so the series
can be tracked over time.

## Step 1 — Load context

Read the following files before rating anything. They are the canonical sources:

- `docs/philosophy.html` — the authoritative definition of purpose; anchor for
  dimensions 1 and 2. Read this first.
- `README.md`, `ROADMAP.md`, `CLAUDE.md` — architecture, layer model, active plans.
- `Cargo.toml` — features, dependencies, description.
- `src/lib.rs` — public API surface, KV-namespace ownership table, crate doc.
- `src/agent/mod.rs` — sub-handle accessors, lifecycle methods.

Then explore source files as needed to evaluate individual dimensions. Read deeply
where a rating is uncertain — shallow reads produce inflated scores.

---

## Step 2 — Rate all 25 dimensions

Rate each 1–10. Use the full range.

### Identity

**1. Philosophy / Coherence with Goal**
Anchor: `docs/philosophy.html`. Does the implemented feature set serve the stated
purpose without drift? Is anything present that contradicts or dilutes what the
philosophy document says the system is? Check README and ROADMAP for any claims
that conflict with the philosophy.

**2. Conceptual Integrity**
Does the implementation feel like one mind designed it — consistent idiom, naming,
abstraction level, and decision-making throughout? Check naming conventions across
`src/`, the sub-handle API surface, example code, and guide chapters for divergence
from the patterns the philosophy establishes.

### Structure

**3. Architecture**
Are the three layers (KV / Signal / Consensus) correctly separated and respected?
Is the dependency graph acyclic? Read the namespace table in `src/lib.rs`, the
layer description in `CLAUDE.md`, and check whether higher-layer code writes
through the documented key prefixes rather than bypassing them.

**4. Modularity**
Can the six sub-handles (`KvHandle`, `MeshHandle`, `CapabilitiesHandle`,
`ConsensusHandle`, `ServiceHandle`, `SchemaHandle`) be understood and reasoned
about independently? Check `src/agent/` for hidden coupling across handle
boundaries, shared mutable state, and dependency direction.

### Interface

**5. API Design**
Read the public re-exports in `src/lib.rs` and each `*_handle.rs` file. Is the
surface minimal and hard to misuse? Are names consistent? Are there footguns,
over-exposed internals, or methods where the correct call sequence is non-obvious?

**6. Error Handling Model**
Check error types in `src/lib.rs`, propagation across all six handles, and the
example code error paths. Are errors typed consistently? Can callers distinguish
recoverable from unrecoverable? Is propagation via `?` coherent or mixed with
`unwrap()`?

**7. Configurability**
Check `src/config.rs`, feature flags in `Cargo.toml`, and environment variable
usage in `examples/` and `Makefile`. Is the configuration surface well-designed —
neither over-constrained nor arbitrarily large? Are operational knobs distinct from
code-change concerns?

### Implementation

**8. Language Best Practices**
Is Rust used idiomatically? Check for `unwrap()` / `expect()` outside test code,
`unsafe` blocks, unnecessary clones, lifetime anti-patterns, and missed
opportunities to use the type system for correctness. Check a cross-section of
`src/agent/` files.

**9. Concurrency Correctness**
Check `Arc`/`Mutex`/`RwLock` usage in `src/store.rs` and `src/connection.rs`,
`AtomicBool` patterns in `src/agent/capability_ops.rs` and `src/agent/demand.rs`,
channel usage, task spawning in `src/agent/tasks.rs`. Are shared-state boundaries
explicit? Are there potential deadlocks or race conditions?

**10. Resource Management**
Check handle drop semantics (capability advertisement TTL, lock guards in
`src/agent/consensus_handle.rs`), connection cleanup in `src/connection.rs`,
spawned task cancellation in `src/agent/tasks.rs`. Are lifecycles explicit and
correct?

### Correctness

**11. Semantic Correctness**
Does the implementation correctly solve the formal problems it claims?
- LWW convergence: `src/store.rs` merge logic
- HLC causality: `src/hlc.rs` tick/observe contract
- Consensus linearisability: `src/consensus.rs` quorum accounting
- Anti-entropy progress: reconnect logic in `src/connection.rs`
Look for off-by-one errors in quorum calculations, edge cases that break
convergence, and places where the formal guarantee and the code diverge.

### Resilience

**12. Robustness**
Check connection error paths, malformed wire frame handling in `src/framing.rs`,
TTL edge cases, and behaviour when a peer disappears mid-operation. Does the
system degrade gracefully or hard-fail on unexpected input?

**13. Security**
Check `src/tls.rs` (mTLS), Ed25519 signing in `src/consensus.rs`, input
validation in `src/framing.rs` and `src/agent/http.rs`. Assess authentication,
authorisation, secrets management, and whether the `tls` feature is usable
without expert knowledge.

**14. Failure Mode Legibility**
When things go wrong, does the system fail obviously and point to the cause?
Check error message quality throughout, log output in key failure paths, and
whether panics include actionable context. Compare against the opacity/load
mechanism — does it communicate *why* a node is unavailable?

### Performance

**15. Performance**
Check `src/store.rs` (LWW merge on hot path), `src/framing.rs` (serialisation),
`src/signal.rs` (fan-out loop), gossip broadcast. Are there unnecessary
allocations, copies, or blocking calls on async paths?

**16. Scalability**
How does behaviour change as node count or data volume grows? Check
`scan_prefix` complexity, capability resolution ranking algorithm,
anti-entropy round complexity, gossip fan-out strategy. Does the system have
a known cliff edge?

### Verification

**17. Testability**
Is the design deterministic, injectable, and free of hidden global state?
Can individual components be exercised without starting a full cluster?
Check test utilities in `src/lib_tests.rs` and how unit tests construct
agents.

**18. Test Architecture**
Check `src/lib_tests.rs` (unit), `tests/` (integration scenarios), `fuzz/`
(fuzz targets). Is there an appropriate pyramid? Are property-based or fuzz
tests used where inputs are adversarial? Is the integration suite fast enough
for CI? Are the 12 scenarios covering the right invariants?

### Operations

**19. Observability**
Check `src/agent/http.rs` for the `/metrics` endpoint and management dashboard.
Are metrics, logs, and traces built into hot paths or absent? Can an operator
understand what the cluster is doing in production without modifying it?

**20. Debuggability**
Check the KV dump endpoint, mesh dashboard, `/ready` endpoint, and any internal
state inspection tools. Can a developer reproduce and understand a specific
failure using available tooling alone?

**21. Operational Readiness**
Check `is_ready()` / `/ready`, `shutdown_with_timeout()`, the `sys/load/`
back-pressure mechanism, Docker Compose setup in `examples/`, environment
variable configuration, and persistence/restart behaviour documented in
`README.md`.

### Sustainability

**22. Evolvability**
Check `CHANGELOG.md`, the wire version policy at the top of `src/framing.rs`
(`WIRE_VERSION` / `PREV_WIRE_VERSION`), the `[Unreleased]` section, and
`ROADMAP.md` v2 milestones. Is there a coherent backwards-compatibility policy?
Is technical debt being paid down or accruing?

**23. Documentation**
Read `docs/guide/` chapters 01–12, `docs/philosophy.html`, `README.md`. Are
the guide's code examples consistent with the current API (sub-handle syntax)?
Are they runnable? Is the philosophy document current? Is there a clear path
from zero to productive for a newcomer?

**24. Developer Experience**
Check `rust-toolchain.toml`, `Makefile`, build output quality, and the
`CLAUDE.md` on-ramp. How long does a clean `cargo build --lib` take? Are error
messages and diagnostics helpful? Is the contribution path clear?

**25. Dependency Hygiene**
Check `Cargo.toml` for dependency count, optional vs required classification,
and whether each dep is well-chosen and actively maintained. Verify
`--no-default-features` compiles (`gateway`-free build). Check `Cargo.lock`
is present. Assess supply chain risk from the dep graph.

---

## Step 3 — Persist results

Append to `docs/analysis/ratings.md` (create the file and `docs/analysis/`
directory if they do not exist). Determine the run number by counting existing
`## ` headings in the file and adding 1.

Use this exact format:

```
## {YYYY-MM-DD} — Run {N}

| # | Dimension | Score | Notes |
|---|-----------|:-----:|-------|
| 1 | Philosophy / Coherence with Goal | ? | one-line justification |
| 2 | Conceptual Integrity | ? | |
| 3 | Architecture | ? | |
| 4 | Modularity | ? | |
| 5 | API Design | ? | |
| 6 | Error Handling Model | ? | |
| 7 | Configurability | ? | |
| 8 | Language Best Practices | ? | |
| 9 | Concurrency Correctness | ? | |
| 10 | Resource Management | ? | |
| 11 | Semantic Correctness | ? | |
| 12 | Robustness | ? | |
| 13 | Security | ? | |
| 14 | Failure Mode Legibility | ? | |
| 15 | Performance | ? | |
| 16 | Scalability | ? | |
| 17 | Testability | ? | |
| 18 | Test Architecture | ? | |
| 19 | Observability | ? | |
| 20 | Debuggability | ? | |
| 21 | Operational Readiness | ? | |
| 22 | Evolvability | ? | |
| 23 | Documentation | ? | |
| 24 | Developer Experience | ? | |
| 25 | Dependency Hygiene | ? | |
| — | **Mean** | **?.?** | |
```

---

## Step 4 — Display to user

Print the following table with actual scores filled in:

```
Mycelium Analysis — {YYYY-MM-DD} (Run {N})
══════════════════════════════════════════════════════════════════════════
 #   Dimension                        Score   Notes
──────────────────────────────────────────────────────────────────────────
 1   Philosophy / Coherence            ?/10   ...
 2   Conceptual Integrity              ?/10   ...
 3   Architecture                      ?/10   ...
 4   Modularity                        ?/10   ...
 5   API Design                        ?/10   ...
 6   Error Handling Model              ?/10   ...
 7   Configurability                   ?/10   ...
 8   Language Best Practices           ?/10   ...
 9   Concurrency Correctness           ?/10   ...
10   Resource Management               ?/10   ...
11   Semantic Correctness              ?/10   ...
12   Robustness                        ?/10   ...
13   Security                          ?/10   ...
14   Failure Mode Legibility           ?/10   ...
15   Performance                       ?/10   ...
16   Scalability                       ?/10   ...
17   Testability                       ?/10   ...
18   Test Architecture                 ?/10   ...
19   Observability                     ?/10   ...
20   Debuggability                     ?/10   ...
21   Operational Readiness             ?/10   ...
22   Evolvability                      ?/10   ...
23   Documentation                     ?/10   ...
24   Developer Experience              ?/10   ...
25   Dependency Hygiene                ?/10   ...
──────────────────────────────────────────────────────────────────────────
     Mean                              ?.?/10
```

Then list the three lowest-scoring dimensions as the top improvement targets.

---

## Scoring guidance

- **1–3**: Significant problems that would block a serious user or contributor.
- **4–6**: Functional but with notable gaps or rough edges.
- **7–8**: Solid; minor issues only.
- **9**: Excellent; the gap to 10 is real but small.
- **10**: No meaningful improvement possible at this stage of the project.

Be honest. Inflate nothing. The value of the score series is its accuracy
over time, not its flattery at any single point. If you cannot evaluate a
dimension from available files, say so and score conservatively.

$ARGUMENTS
