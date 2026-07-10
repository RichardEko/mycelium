# Runtime invariants — do not "fix" these

↑ [architecture](architecture.md) · siblings: [layers-and-crates](layers-and-crates.md)

Invariants that look like bugs or optimization targets and are neither. Each has burned a
session or shipped a regression before being written down.

## Layer III posture: detection, not prevention

The substrate never enforces the `consensus/` prefix — a rogue writer can clobber
`consensus/committed/{slot}` and LWW accepts it. The deliberate response is the
**commit-conflict tripwire** (listener refuses to re-endorse a conflicting COMMIT, `warn!`s,
increments `SystemStats::commit_conflicts`). Do **not** add a `consensus/`-prefix write guard
to `apply_and_notify` — that teaches Layer I a Layer III law and inverts the dependency.
The same posture governs the `sys/` namespace tripwire (`sys_namespace_violations`, see
[security](../security.md)) and the RBAC design generally.

**Epoch-leased commitments** (`ConsensusConfig::committed_lease_secs`, default `None` =
permanent): when set, readers apply the capability-style read-side freshness convention —
expired lease reads as not-committed and the slot reopens. Renewal = re-proposing the *same*
value; a different value while live returns `Superseded`. `consensus_rx` stays the raw KV view.

**Listener handlers register synchronously.** `start_consensus_listener` registers
PROPOSE/COMMIT receivers *before* spawning the voter task. Registration inside the task's
first poll silently dropped proposals racing startup (single-node tests self-quorum and never
notice). Keep it synchronous through any refactor.

## Individual-scope routing: forwarding stays unconditional

`SignalScope::Individual` carries RPC and consensus votes. The gossip loop sends directly to
a directly-peered target (latency optimization) and otherwise **falls back to flooding** —
the seen-set dedups, hop-TTL bounds it. Pre-2026-06-12 the fallback was "optimized" to a
silent drop, which broke RPC and ballot voting in partial meshes. Only *admission* is scoped
(`Boundary::admits`); forwarding never is. Regression gates:
`test_individual_signal_reaches_unpeered_target_via_relay`,
`test_individual_consumers_over_random_partial_meshes` (both in `src/lib_tests.rs`).

**The one legitimate termination: the frame's own target (2026-07-10, #162).** An Individual
frame addressed to *this* node has nowhere further to route — admission already delivered it
locally. Pre-fix it still entered the forward path (both self-emits like mailbox
deliver-to-self and relayed frames arriving at their destination traverse the gossip queue),
found no route to itself, and **flooded the cluster** until seen-set/TTL death — plus a
topology-pressure warn naming the node itself and spurious `individual_flood_fallbacks`
counts (this noise complicated the #161 diagnosis). The gossip shard now terminates
self-addressed Individual frames before any forwarding logic (`src/agent/tasks.rs`). This is
**routing at the terminal, not scope admission** — do not cite it as precedent for
conditional forwarding: any frame addressed *elsewhere* still forwards unconditionally.
Gate: `self_targeted_signal_does_not_flood` (`src/lib_tests.rs`, verified failing pre-fix).

**Flood-relay is correct but slow for RPC — pin RPC-heavy pairs.** The direct-send path only
fires for a peer in the *forwarding-target set*, and that set **deliberately de-pins non-active
peers** (seed-scalability, WS-B M4: avoid O(N) pinning on shared seeds — `src/agent/tasks.rs`,
the target rebuild on peer-list change). So an Individual-scoped frame to a peer that isn't a
current forwarding target degrades to flood-relay: fine for a one-shot signal, but a
request-response **RPC** pays a multi-hop round-trip and can miss its deadline (this was the S13
tuple-space flake — a secondary→primary `take` timing out at HTTP 408, grounded in node logs;
#150). The fix is **`GossipAgent::connect_peer(peer)` / `disconnect_peer(peer)`** (`src/agent/topology.rs`):
a `pinned_peers` set (papaya, lock-free — no lock-order row) that the flood-fallback honours
(`targets.contains(t) || pinned_peers.contains(t)`) on *every* rebuild, so a specifically-pinned
peer keeps a direct route without undoing the seed de-pinning. `connect_peer` also **actively
warms** the link — writers connect lazily on first frame (`mycelium-core/src/writer.rs`
`run_peer_writer`), so it spawns the writer and sends a Ping on call, establishing the connection
*ahead* of the first RPC rather than on its deadline. Call it a little ahead of the RPC (a
background keeper), not inline — the tuple-space secondary runs exactly such a warm-keeper in
`become_secondary` (`mycelium-tuple-space/src/lib.rs`). Any RPC-heavy relationship toward a
specific peer should pin it; general signalling should not (that is what the seed de-pinning
protects). Reusable lesson: **an Individual-scoped RPC's latency depends on whether its target is
a direct forwarding target — for a hot pair, pin the route and warm it, don't rely on flood-relay.**
Regression gate: integration scenario 13 (`make test`, `13_tuple_space.sh`) plus the hosted
cluster-suites CI workflow. *Correction (2026-07-10):* an earlier version of this note cited
`make test-overlay` S13 — a different scenario (consensus shared-log) that never exercised this
path; the flaky test was always **integration** S13. The pin+warm was also only half the fix:
the other half was the tuple-space spurious-promotion split-brain
([companions/tuple-space](../companions/tuple-space.md), #158) — flood-relay latency masked it
locally, the hosted 2-core runner exposed it.

## KV floods the cluster — a group is not a data-isolation boundary

`WireMessage::Data` (the KV path) always forwards `ForwardHint::All`
(`mycelium-core/src/connection.rs` — the seen-set dedups, hop-TTL bounds it): **Layer-I KV
replicates to every node in the cluster, unconditionally.** `Boundary::admits` gates only
whether a node *acts on a Signal*; it never scopes KV propagation. Consequences a contributor
*will* get wrong (it was mis-stated once in the wiki design record and caught by this lint):

- A **capability group** (`join_group`, `gcap/`) organises *who participates* — it never
  scopes *what replicates*. Group-scoped state (e.g. a `wiki/{group}/…` namespace) is
  **namespaced and access-gated, not replication-isolated**: every cluster node holds the
  bytes regardless of membership. *(This invariant is why the `mycelium-wiki` plan moved its
  durable corpus **out** of KV to an external store — a group's wiki is not KV-isolated, so a
  large per-group corpus would flood every node; only its small evaporating proposal queue
  stays in KV. See `docs/plans/mycelium-wiki.md` → Architecture.)*
- Therefore an in-cluster access label (RBAC clearance on a key, an `authorized_callers`
  ACL) is a **served-path gate** — governance + audit (detection-not-prevention), never
  confidentiality. It withholds the *convenient* read path; it cannot un-replicate bytes a
  node already holds.
- The genuine **data-isolation boundary is the cluster/mesh** — peer admission (TLS
  mutual-auth: who you connect to and authenticate). Bytes that must never *reach* a node
  belong in a **separate cluster** (its KV never peers in). This is the domain-level
  self-election of
  [coordinator-free-recursion](../../domain/theory/coordinator-free-recursion.md) — the
  boundary that isolates *data* is one level up from the group.
- For "the bytes may *reach* a node but it must not *read* them" **within** one cluster, the
  answer is **application/envelope payload encryption** — the value is ciphertext before the
  KV write, so the substrate only ever holds opaque bytes and only key-holders decrypt. This
  is **not** WS3 `DataAtRestCipher`: WS3 is **on-disk only** (`mycelium-core/src/persistence.rs`
  — "data in memory is not [protected]"), one cluster-uniform cipher per node, so every node
  still holds the *plaintext in memory* — WS3 is disk-theft defence-in-depth, not a per-page
  or cross-node read boundary.

Worked examples (governance · cluster boundary · payload encryption vs WS3):
`docs/design/wiki-concurrent-edit.md`
§4.3.1–4.3.3.

## Fan-out activation is event-driven

The connection handler publishes the peer list the moment a new peer is inserted (Ping
receipt); waiting for the health monitor's next tick left inbound-only nodes (seeds, tuple
primaries) mute for live sends for up to 2× `health_check_interval`. The health monitor
remains the reconciler/evictor, not the activator.

## Anti-entropy is chunked; KV writes are size-gated (2026-07-02)

`StateResponse` is sent as multiple frames under a per-chunk byte budget
(`mycelium-core/src/connection.rs`), so a store larger than one frame can still bootstrap a
late joiner; an individually un-frameable legacy entry is skipped by name. Upstream,
`kv_set`/`kv_set_async` reject writes over `framing::MAX_KV_WRITE_BYTES` outright, and the
per-peer writer drops a `FrameTooLarge` frame *without* tearing down the connection. Gates:
`test_late_joiner_converges_past_frame_sized_store_via_chunked_anti_entropy`,
`test_oversized_value_is_rejected_outright_and_cluster_stays_healthy`. History: analysis
Run 28 Finding 1 (`docs/analysis/ratings.md`).

## `subscribe_log_group` is single-active — do NOT turn it into a load-balanced work queue (#149)

Two different patterns keep getting conflated because the word "consumer group" (and the S11
"task auction" framing) implies work-sharing. They are distinct, and the substrate has a
**separate correct primitive for each**:

- **Exact-once *ordered log consumption*** → `subscribe_log_group` / the gateway
  `/gateway/overlay/log/group/subscribe`. Contract: **at most one active consumer at a time**,
  failover on death. Achieved (`src/agent/http.rs`; gate: overlay S11) by a **leased consensus
  claim with converged-holder confirmation**: `system_propose` commits **optimistically against a
  node's *local* committed view** (`src/consensus.rs`), so two near-simultaneous proposers can
  both commit — the return is **not** trustworthy for mutual exclusion. But commit-keys are
  LWW-by-HLC, so the *converged* holder is deterministic: after committing, read the converged
  committed holder (`live_committed_value`) and only that node consumes; losers stand by **without
  releasing** (a tombstone would clear the winner's claim). The winner drains a **private** offset
  (exact-once by construction) and renews the lease → a standby takes over on death.
  **Reusable lesson:** a consensus-backed lock is not mutually exclusive on the *propose return* —
  confirm via the converged committed value. The `mycelium-core` library
  `KvHandle::subscribe_log_group` is the **best-effort** version (LWW claim, no consensus — can
  double-deliver under contention; documented as such).
- **Load-balanced exactly-once *work distribution*** (each item claimed by exactly one of many
  competing workers) → the **`mycelium-tuple-space`** companion (O(1) FIFO `take`, single-owner
  claim). `examples/fluid_pipeline` is the worked demo.

**The trap (do not re-attempt):** making the log-group *load-balance* by handing entries off
between consumers per-item. It cannot work with a single advancing offset. A **bare LWW offset**
lets a peer read stale and re-drain (double-delivery); a **consensus offset** can't be
re-advanced (a second `system_propose` to the same slot returns `Superseded`, not a new value)
and per-item consensus floods the engine (starves other consensus users — it broke overlay S12).
Competitive per-item consumption is the *tuple-space's* job, by design; don't rebuild it on a
log. History: #149 (the full got-10 → got-1 → got-10 dead-end is on the issue).
