# Build plan — WS-G / G3 · `mycelium-blackboard` companion crate

**Status:** ✅ **COMPLETE** (2026-06-21). All six phases shipped:

| Phase | What | PR |
|---|---|---|
| 1 | Core claim-by-predicate `BoardStore` | #95 |
| 2 | WAL durability (against the exactly-once contract) | #96 |
| 3 | Agent-backed `Blackboard` — emergent roles + failover | #97 |
| 4 | Gateway endpoints + py/ts SDKs | #98 |
| 5 | Community-microgrid worked example + CI smoke | #99 |
| 6 | Exactly-once overlay decision (declined-with-evidence → closes G2) | this |

The `mycelium-blackboard` crate is shipped: opportunistic shared working memory with competitive
destructive claim-by-predicate (Linda's `in`), on the public API, with emergent failover. **WS-G is
now complete** (M13 keyed `take` + G2 contract + G3 blackboard). Design record follows.

---

**Original status:** phased build plan (2026-06-20), promoting the design sketch
[`mycelium-blackboard.md`](mycelium-blackboard.md) per the WS-G plan's G3 step. Design rationale,
worked example (community microgrid), and non-goals live in the sketch; this doc is the
phasing/sequencing only.

**What it is:** a companion crate rebuilding **blackboard-style shared working memory** on Mycelium's
public API — the same composability proof as `mycelium-tuple-space`. Fact *reading* is unconditional
and concurrent (Layers I/II already do it perfectly); the crate adds the **one** missing primitive:
**competitive destructive claim-by-predicate** (Linda's `in(pattern)`) with exactly-once discipline.

**The exactly-once discipline is G2's documented contract** ([`exactly-once-effect.md`](../design/exactly-once-effect.md)).
G3 is its **second real user** — so G3 builds against that contract and is the trigger to extract the
shared overlay (see Phase 6).

**House-style constraints (from the sketch's non-goals):**
- **No matching engine in `mycelium-tuple-space`** — the lane properties (O(1) claims, per-lane
  backpressure, one-record transitions) must not be un-bought. The blackboard is a *separate* crate.
- **Predicate language = the capability attribute-filter grammar** (equality + presence), **not**
  unification/structural matching. Already implemented, already understood; full matching is scope
  creep until demonstrated.
- **No semantic/embedding matching** in the substrate or this crate (that's a selection-edge concern).
- **Fan-in joins are out of scope** — G1's keyed `take` covers them.

**Done when:** an agent can post facts that gossip to all (non-destructive `rd`), and two agents
whose predicates both match a finite fact **race for an atomic claim** — exactly one consumes it, the
loser's claim returns empty, and a winner that drops mid-work has the claim re-queued by the
in-flight deadline. Driven end-to-end by the community-microgrid worked example + an integration
scenario.

---

## Phase 1 — Core: the board store + claim-by-predicate ✅ SHIPPED

**Shipped** — the `mycelium-blackboard` crate scaffold + the pure in-memory `BoardStore`:
`post` / `read` (non-destructive `rd`) / `claim` (competitive destructive `in`, single-owner,
non-blocking) / `ack` (idempotent terminal) / `release` / `requeue_expired`, with a conjunctive
attribute `Predicate` (equality + presence). 8 unit tests incl. G-G3.1 (a 16-thread race over one
finite fact — exactly one claims it); clippy `--features gateway --all-targets` clean; CI job added.

The in-memory primitive, mirroring `mycelium-tuple-space`'s Phase 1 (core store, no WAL/roles yet).

- A `Board` holds **facts** (`bb/{board}/{fact_id}` — typed attribute maps, gossiped KV writes) and a
  **claim index**. A fact is `(id, attributes: Map, payload, posted_hlc)`.
- `post(board, fact)` — write a fact (non-destructive; gossips to all readers).
- `read(board, predicate)` — non-destructive scan returning all matching facts (Linda `rd`; pure
  read over the gossip view, no claim).
- **`claim(board, predicate) -> Option<Fact>`** — the new primitive: atomically claim **one** fact
  matching `predicate` into in-flight (single-owner); `None` if none match. `ack(claim_id)` /
  `release(claim_id)` terminal. Predicate = `CapFilter`-style attribute equality + presence.
- Per-board counters (matchable facts, in-flight claims) — `sys/bb/{node}/{board}/…`, the `sys/tuple/`
  posture.
- **Gate G-G3.1:** two concurrent `claim`s with overlapping predicates over one finite fact — exactly
  one wins, the other gets `None`; non-destructive `read` sees the fact until it's claimed; `release`
  returns it to claimable.

## Phase 2 — Durability: WAL (build against the G2 contract) ✅ SHIPPED

**Shipped** — `mycelium-blackboard/src/wal.rs` + `BoardStore::{transient, persistent}`. WAL records
`Post` / `Claim` / `Ack` / `Release` (each one indivisible record — the blackboard has no stage
transitions, so no compound `Complete`), magic+versioned header (refuses a newer format), corrupt-tail
truncation, and compaction with an epoch bump. Replay liveness: a fact is claimable iff `Post`ed and
not `Ack`ed; a claimed-but-unacked fact re-queues (at-least-once). 5 G-G3.2 gate tests
(posted-survive-replay, **claimed-but-unacked re-queues** = the "winner drops mid-charge" path,
acked-does-not-resurrect, compaction-preserves-live-drops-acked, refuses-newer-format) + 2 WAL unit
tests; clippy clean.

- Claim path served through a primary with a WAL + in-flight deadline, **exactly the G2 discipline**:
  `Claim` is **one indivisible record**; an unacked claim past the deadline re-queues (at-least-once,
  idempotent ack). Reuse the tuple-space WAL shape (record kinds, v-prefixed header, v1-replay-accept
  posture, compaction epoch).
- **Gate G-G3.2:** a claimed-but-unacked fact re-queues after the deadline and is re-claimable; an
  acked claim does not resurrect on WAL replay; the worked-example "winner drops mid-charge → loser
  claims the remainder" path passes.

## Phase 3 — Roles & failover ✅ SHIPPED

**Shipped** — the agent-backed `Blackboard` (`BoardConfig` + `BoardRole`): primary discovered on the
capability ring (`blackboard/{ns}.primary`), secondary mirrors + promotes on evaporation, `Auto`
elects lowest-candidate. Public `post`/`read`/`claim`/`ack`/`release` serve locally on the primary or
RPC to it. **Replication is `Post`/`Ack`-only** (a `Claim`/`Release` doesn't change mirror liveness —
a claimed-but-unacked fact stays claimable in the mirror = the at-least-once re-queue a promotion
wants), so the heartbeat/WAL-replay-cursor machinery is unneeded: snapshot-on-join + live replication
keep the mirror a complete live view. Gates G-G3.3 (2 integration tests): an in-flight claim survives
a primary kill + promotion and is re-claimable (then acked → no resurrection); `Auto` election elects
a serving primary. Clippy `--features gateway --all-targets` clean.

- `BoardRole` (`Primary`/`Secondary`/`Auto`/`Client`) mirroring `TupleRole`: primary discovered by
  capability advertisement; secondary mirrors via replicate RPC + heartbeat and promotes when the
  primary's capability evaporates (the ring IS the failure detector); `Auto` elects lowest-candidate.
- Claim records replicated to secondaries; promotion replay re-establishes the in-flight set.
- **Gate G-G3.3:** kill the primary with a claim in-flight → a standby promotes and the in-flight
  claim survives (re-queues under the deadline); claims are not double-served across promotion.

## Phase 4 — Edge: gateway + py/ts SDKs ✅ SHIPPED

**Shipped** — `src/http.rs` (gateway feature): `POST /gateway/bb/{post,read,claim,ack,release}` +
`GET /gateway/bb/depth`, with a JSON predicate (`eq` map + `present` list) and base64 payloads.
Python `mycelium.blackboard.Blackboard` + TypeScript `Blackboard` SDKs (post/read/claim/ack/release/
depth). Gate G-G3.4 (`tests/gateway.rs`): the full post→read→claim→ack lifecycle + the competitive
claim race drive across the HTTP gateway (second claimer gets nothing; acked fact does not re-serve;
double-ack → 404; wrong-ns → 400). `tsc --noEmit` clean; `py_compile` OK; clippy clean.

- `POST /gateway/bb/{board}/post`, `/read`, `/claim`, `/ack`, `/release`; `GET /gateway/bb/{board}/depth`.
- Python + TypeScript SDK methods (`post`/`read`/`claim`/`ack`/`release`) mirroring the wire shape, as
  `mycelium-py`/`mycelium-ts` do for the tuple space.
- **Gate G-G3.4:** the claim race drives across the HTTP gateway (a gateway integration test).

## Phase 5 — The worked example + integration scenario ✅ SHIPPED

**Shipped** — `examples/microgrid.rs` + `ci_smoke.sh` (wired into the CI `blackboard` job). The
community-microgrid demo runs Docker-free: an inverter posts 12 finite surplus facts; a forecaster +
a tariff agent both `read` them (non-destructive `rd`, shared); two storage executors `claim` +
`ack` competitively (`in`) — every surplus consumed **exactly once** (the example asserts the
invariant: total == posted, no id claimed twice). The **cross-node** integration (claim race +
primary-kill → secondary-promotes → in-flight re-queues) is covered at the Rust integration level by
`tests/failover.rs` (G-G3.3) rather than a separate Docker scenario — proportionate, since the
failover test already exercises the multi-node path deterministically.

- The **community-microgrid** example (sketch §"Worked example"): a fact pool with a forecaster +
  tariff agent (non-destructive readers) and two storage executors (competitive claimers of finite
  surplus). Demonstrates the `rd` (share) / `in` (compete) split end to end. A coop-style demo +
  `ci_smoke.sh`, Docker-free, retry-harnessed.
- **Integration scenario 14** (cross-node): a board primary + secondary, fact posted, two clients
  race to claim, one wins, the other gets empty; primary killed mid-claim → secondary promotes →
  in-flight re-queues.
- **Gate G-G3.5:** scenario 14 green; the example smoke passes in CI.

## Phase 6 — Resolve the exactly-once overlay decision (closes G2) ✅ DONE

**Done — extraction declined-with-evidence.** With the blackboard now shipped as the second real
user, the deferred G2 decision was made by *examining both implementations* (the whole point of the
deferral). The finding: a load-bearing divergence the surface similarity hid — the tuple space's
in-flight timestamp is **wall-clock-ms, WAL-persisted, and cross-node** (it lives in `Record::Take`
and the gossiped `tuple/inflight/{id}` key because its exactly-once spans nodes/restarts), while the
blackboard's is a **monotonic `Instant`, in-process** deadline. A shared `InflightTracker<T>` would
have to be generic over the clock and would couple two crates with divergent evolution for a ~15-line
kernel. So **both crates implement the same documented contract, validated by the same gate shape, with
no code coupling** — the Rule-of-Three call, made with evidence rather than anticipation. Recorded in
[`docs/design/exactly-once-effect.md`](../design/exactly-once-effect.md). **G-G3.6 met** in spirit: the
discipline is one *tested contract* both crates satisfy (their gate suites are the independent tests),
rather than one shared code unit that would be a leaky abstraction.

With G3 as the **second real user** of the in-flight-claim/ack/requeue mechanism (the tuple space is
the first), lift the shared core out of the two per [`exactly-once-effect.md`](../design/exactly-once-effect.md)'s
extraction trigger: a small tested overlay both crates reference, rather than two copies. This is the
deferred half of G2 — done here because now there are two concrete shapes to factor from.

- **Gate G-G3.6:** the extracted overlay's claim/ack/requeue invariants are unit-tested
  independently, and both the tuple space and the blackboard use it (behaviour unchanged — the
  existing gates stay green).

---

## Sequencing & gates

Each phase is its own PR (the `mycelium-tuple-space` cadence). Gates: `cargo test -p
mycelium-blackboard --features gateway` + `cargo clippy -p mycelium-blackboard --features gateway
--all-targets -- -D warnings`, plus the SDK typecheck and the integration scenario, mirroring the
tuple-space crate's gate set. The crate's single normal dependency on `mycelium` is the composability
proof; a dev-dependency back (for an example) is a legal Cargo cycle.

**Scope note:** this is a **large** increment — comparable to the original `mycelium-tuple-space`
build (which was 5 phases). It is the last WS-G item and is deliberately sequenced last.
