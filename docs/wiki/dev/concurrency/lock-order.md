# Lock-order table

↑ [concurrency](concurrency.md) · sibling: [lock-free-and-atomics](lock-free-and-atomics.md)

All `Mutex` and `RwLock` sites in the codebase. **Invariant: no function acquires more than
one lock from this table** — flat acquisitions only, so no ordering discipline is needed
beyond this list.

**Keep this table honest:** it claims completeness — when adding any `Mutex`/`RwLock` field,
add a row. Analysis Run 28 found rows 9–14 missing after three feature waves; the flat
invariant held, but only by luck of review. The doc-vs-code lint (schema §Lint) greps for
`Mutex<`/`RwLock<` declarations and diffs against this table.

| # | Field | Type | Acquired in | Notes |
|---|-------|------|-------------|-------|
| 1 | `CoreCtx::task_handles` | `Mutex<JoinSet>` | `spawn_task()`, `wait_for_tasks()`, shutdown drain | Short-lived; the shutdown drain swaps the set out before awaiting |
| 2 | `TaskCtx::rpc_pending` | `Mutex<HashMap>` | `rpc_call_ctx`, `rpc.result` handler | Poison-recovering; never across `await` |
| 3 | `CoreCtx::reorder_buf` | `Mutex<ReorderBuffer>` | `emit_ordered()`, flush task | Lock+flush synchronous |
| 4 | `CoreCtx::signal_boundary` | `parking_lot::RwLock<Boundary>` | read: every `emit()`; write: join/leave/suppress | `read()` is the hot path |
| 5 | `GossipAgent::gossip_rxs` | `Mutex<Option<Vec<Receiver>>>` | `start()` (`take()`d once) | Single-use |
| 6 | `GossipAgent::extra_routes` | `Mutex<Option<Router>>` | `with_http_routes()`, `start()` | Gateway feature; single-use |
| 7 | `KvStore::index_stripes` | `[Mutex<()>; 64]` | `apply_and_notify` index reconcile | Leaf lock; exists because the store CAS is lock-free (M2 Run-18 finding: index ops could interleave opposite to CAS order and strand a key outside `scan_prefix`) |
| 8 | `TaskCtx::audit_chain` | `Mutex<AuditChainState>` (`compliance`) | `audit()` sealing | Guard released **before** the KV write; signing happens after the lock |
| 9 | `AgentStateMachine::current` | `parking_lot::Mutex<ExecutionState>` | `state()`, `try_commit()`, `force_failed_transition()` | The commit lock: validate-and-swap **plus** budget-counter check + reserve as one atomic step (Run 28 fix). Policy is snapshotted before acquiring — never take #10 while holding this |
| 10 | `AgentStateMachine::policy` | `parking_lot::RwLock<AgentPolicy>` | guards, snapshots, `set_policy()` | Read-mostly; never held while acquiring #9 |
| 11 | `AgentStateMachine::task_id` / `::timeout_handle` | `parking_lot::Mutex<…>` | task-id set/read; timeout arm/cancel | Leaf locks, single-statement |
| 12 | `SwimState::pending` | `Mutex<AHashMap<u64, oneshot::Sender>>` | probe register/resolve/forget | Leaf; poison-recovering |
| 13 | `SwimState::membership` | `Mutex<SwimMembership>` | `lock_membership()` callers | Leaf; never held while acquiring #12 |
| 14 | `FilterOpacityRegistry::entries` | `Mutex<Vec<RegEntry>>` | `declare_requirement`, opacity watcher | Leaf; poison-recovering |
| 15 | `HttpCtx::gateway_caps` | `Mutex<HashMap<String, oneshot::Sender>>` | gateway capability register/retract handlers | Leaf; poison-recovering; single-statement insert/remove |
| 16 | `HttpCtx::lock_guards` | `Mutex<HashMap<String, LockGuard>>` | gateway distributed-lock acquire/release handlers | Leaf; poison-recovering; single-statement insert/remove |
| 17 | `OidcVerifier::cache` | **`tokio::sync::RwLock<Option<CachedKeys>>`** | JWT verify (read), JWKS refresh (write) | **The one sanctioned async lock**: the write guard is *deliberately held across the JWKS HTTP fetch* so refresh is single-flight (readers on the hot path take a cheap read-lock on cached keys). Do not copy this pattern without the same single-flight justification |
| 18 | `SignalLog::sender_log` values | `PapayaMap<Arc<str>, Arc<parking_lot::Mutex<VecDeque<(NodeId, Instant)>>>>` | `record()`, `quorum()`, sender-history reads (`mycelium-core/src/signal.rs`) | Per-kind leaf locks *inside* a papaya map (hidden behind the `SenderLog` type alias): arc retrieved via retry-safe `compute`, then locked for a short synchronous prune/push or scan. Never across `await` |
| 19 | `EventRing::events` | `std::sync::Mutex<VecDeque<Event>>` | `record()`, `since()` (Legible-Emergence Phase 3, `src/agent/emergent.rs`) | Leaf lock: bounded event ring; short synchronous push-and-drop-oldest or filter-and-clone scan, never across `await` |
| 20 | `MeshArtifactSource::cache` | `std::sync::Mutex<HashMap<ArtifactId, Bytes>>` | `prefetch()` (contains/insert), `fetch()` (get) (`mycelium-wasm-host/src/mesh_source.rs`) | Leaf; single-statement ops; released before the `pull_artifact` await inside `prefetch` (added retroactively — pre-dated this row, found in the artifact-library session 2026-07-07) |
| 21 | `Provisioner::hosted` | `Arc<Mutex<HashMap<ArtifactId, HostedState>>>` | `provision_round` passes (`is_hosted`), `start_install` reservation, install-task completion swap, `withdraw`, counts, `reserved_requirements` (resource eligibility, §4.4) (`mycelium-wasm-host/src/provisioner.rs`) | Leaf; acquired once per function, never across `await`; install tasks lock once at completion (token-checked swap), teardown of a superseded install runs *outside* the lock |
| 22 | install-task loading tier (local) | `Arc<Mutex<(u64, Option<CapabilityReg>)>>` | the `ProgressFn` closure + post-install drop in `start_install`'s spawned task (`mycelium-wasm-host/src/provisioner.rs`) | Leaf, task-local (not a struct field); callback runs on the runtime's pull thread (`spawn_blocking`) — sync only, never across `await`; guards the last-pct step + the `{ns}/loading` advertisement handle |
| 23 | `PrefetchingSource::cache` | `std::sync::Mutex<HashMap<ArtifactId, Bytes>>` | `prefetch()`/`prefetch_all()` (contains/insert), `fetch()` (get) (`mycelium-wasm-host/src/http_source.rs`) | Leaf; single-statement ops; released before the `fetch_remote` await inside `prefetch` — same shape as row 20 |

**Scope (made explicit 2026-07-07):** the table covers `mycelium-core`, `mycelium`, and
`mycelium-wasm-host`. The data-plane companion crates (`mycelium-tuple-space`,
`mycelium-blackboard`, `mycelium-wiki`) hold their own lock sites (~20 — store/WAL inners,
role registrations, task lists) that are **not** inventoried here; they follow the same
one-lock-per-function flat discipline, documented at their declaration sites. Whether to
extend this table to a full workspace inventory is an open decision — flagged by the
2026-07-07 lint, not taken unilaterally.

**Async contexts:** guards from every *sync* lock above are `!Send` across `await`
(`std::sync` and default `parking_lot` alike) — the compiler enforces it for spawned
futures; all those sites release before any `await`. `std::sync::Mutex` is the default
flavour; `signal_boundary` (row 4) and `AgentStateMachine` (rows 9–11) use `parking_lot`
with the same discipline. `tokio::sync` locks are banned **except** row 17's documented
single-flight exception.
