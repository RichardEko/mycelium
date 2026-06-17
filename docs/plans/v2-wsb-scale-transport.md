# v2.0 WS-B — Scale & Transport — Delivery Plan

**Status:** Not started (trigger-gated, like all of v2.0 beyond WS-A). This document is
the *strategy/sequencing* plan; the canonical per-milestone design home stays
[ROADMAP §v2.0 Milestones](../../ROADMAP.md) (M4, M5, M11) and the NANDA Quilt deep-dive
(Merkle anti-entropy). The workstream summary lives in
[`docs/plans/v2.0.md`](v2.0.md) §WS-B.

WS-B theme: **break the O(N²) connection ceiling; bound bytes-on-wire.**

## Items in scope

| Item | ROADMAP | Phase here |
|---|---|---|
| Partial-mesh gossip (bounded fan-out) | M4 | Phase 1 |
| Hybrid TCP/UDP transport (SWIM-style) | M5 | Phase 2 |
| Wire-codec succession (bincode → hand-rolled fixed-layout) | M11 | Phase 3 |
| Merkle-tree anti-entropy | [Quilt-DD] #1 | Phase 3 |

## 1. Definition of Done — revised to 100 nodes

The ROADMAP DoD names a >500-node cluster. We deliberately retarget to **100 nodes**:
not a weakening, but a recognition that the Docker-bridge iptables FORWARD-chain ceiling
is *already observable well below 100*, so 100 nodes demonstrates both the disease and
the cure. Accessing 500-node infrastructure is impractical for our CI/dev loop; it is also
unnecessary, because the failure symptoms are documented at far smaller scale:

- `make test-scale-resilience` is **capped at `RESILIENCE_WORKERS=20`** today — at 50 workers
  the FORWARD chain saturates and the Phase-3 late-joiner probe's TCP SYN to seed times out
  at the OS level (errno 110, ~2 min). *(CLAUDE.md §Scale and resilience tests)*
- Consecutive 100-node rounds degrade monotonically (PASS → 80/100 → 97/100) from kernel
  conntrack/iptables accumulation across same-session rounds.
- Seed accumulates ~200 ESTABLISHED connections at 100 nodes (ROADMAP M4).

**DoD = four falsifiable gates, all on existing harnesses:**

| Gate | Harness | Pass criterion |
|---|---|---|
| **G1 — connection memory O(1)** | `make test-scale SCALE_WORKERS={30,50,70,100}` | Seed ESTABLISHED count **flattens** as N grows (today ~linear). Captured as a 4-point curve. |
| **G2 — no FORWARD saturation at 100** | `make test-scale` (100) | Forms with **zero dropped frames**, repeatable across ≥3 consecutive same-session rounds. |
| **G3 — resilience at 50 workers** | `make test-scale-resilience RESILIENCE_WORKERS=50` | **Phase-3 late-joiner probe passes** (join + anti-entropy inbound + gossip outbound). The sharpest gate — *cannot* pass today; only the M5 structural fix makes it pass. |
| **G4 — anti-entropy tail bounded by divergence** | `make test-scale-entries` | Bytes-on-wire per StateRequest round scales with **divergence**, not store size. |

G3 is the load-bearing proof of the structural fix; G1/G2/G4 are supporting measurements.

## 2. Sequencing

```
Phase 0  Baseline + instrumentation       (prove the "before")
Phase 1  M4  partial-mesh bounded fan-out  [TCP-level, incremental — clears G1/G2]
Phase 2  M5  hybrid UDP/TCP SWIM           [structural — clears G3]
Phase 3  v12 wire bump: M11 + Merkle AE    [bundled — clears G4 + retires bincode]
```

**M4-vs-M5 decision (resolved):** ROADMAP notes M4 is *"largely subsumed by M5."* We ship
**M4 standalone first** as a low-risk, wire-compatible intermediate that should already clear
G1/G2 on pure TCP — giving a shippable milestone and a fallback if M5 slips — then M5 removes
persistent connection state entirely (the only thing that clears G3).

**Bundling decision:** M11 (codec) and Merkle both reshape `StateRequest`, so both ride the
single v11→v12 bump in Phase 3 — one rolling-upgrade break, not two. M5's UDP ping format is a
*separate* lightweight datagram type, **not** a `WireMessage` variant, so M5 does not force the
v12 bump and Phase 2 stays wire-compatible with v11.

## 3. Phase detail

### Phase 0 — Baseline & instrumentation
- Extend `tests/integration/run_scale.sh` to emit, at convergence, the **seed ESTABLISHED
  count** and the **FORWARD-chain rule count**.
- Run the G1 4-point curve and G3-at-50 on current `main` → record the "before" (G3 fails,
  G1 grows). This is the regression baseline every later phase is measured against.
- No library code change; harness/measurement only.

### Phase 1 — M4 partial-mesh (bounded fan-out)
- **What:** connection maintenance keeps TCP only to a bounded random subset (`k = O(log N)`),
  relying on multi-hop epidemic flooding for the rest. Today `GOSSIP_PING_PEER_SAMPLE_SIZE`
  limits *pinging* but not *connections*.
- **Touch points:**
  - `src/agent/tasks.rs::run_health_monitor` (~line 399) — the connection-establishment loop
    around `cached_ping_targets` / `max_active_connections` (~lines 441–552). Replace
    "connect to all known peers" with "maintain `k` random outbound writers."
  - `mycelium-core/src/writer.rs::get_or_spawn_writer` (~line 193) — idle eviction exists;
    ensure evicted-peer churn re-randomises rather than reconnecting the same set.
  - `mycelium-core/src/config.rs` — new `gossip_fanout: usize` (env `GOSSIP_FANOUT`, default
    ~`ceil(log2 N)`); `max_active_connections` (~line 392) is the hard ceiling.
- **Guards to keep green:** `test_individual_consumers_over_random_partial_meshes` and
  `test_individual_signal_reaches_unpeered_target_via_relay` (CLAUDE.md §Individual-scope
  routing) — the unconditional-forwarding fallback is what makes bounded fan-out safe for
  RPC/ballot delivery to non-adjacent pairs.
- **Exit:** G1 + G2 green at 100 nodes on TCP.

#### Phase 1 outcome (2026-06-16) — shipped as a *partial* reduction; full G1 deferred to M5

Implemented: `gossip_fanout` (auto `2·⌈log2 N⌉`, floor `AUTO_FANOUT_FLOOR=8`, capped at
known peers; `max_active_connections` as hard ceiling), a **sticky** `reconcile_active_targets`,
the bounded **active set as the single forwarding source of truth** (`peer_list_tx` publishes
it; the gossip shard no longer re-pins bootstrap; the connection-handler event-driven
activation is bounded/incremental), and `writer_idle_timeout_secs` default `0 → 30 s`.
Both partial-mesh gates pass; full lib suite green; the cluster still converges at 100 nodes.

Measured (`tests/integration/baseline/scale-m4.csv` vs the Phase-0 `scale-baseline.csv`):

| nodes | pre-M4 `seed_established` | M4 |
|---:|---:|---:|
| 31  | 62  | 42  |
| 51  | 102 | 64  |
| 71  | 142 | 76  |
| 101 | 202 | 128 |

≈37 % reduction and the curve bends sub-linear — but **not flat** (G1 wants ~`2k`≈28 at
100 nodes). **Root cause of the residual:** peer *discovery* is coupled to the active set —
nodes learn peers only via `Ping` piggyback from nodes that ping *them*, so under bounded
fan-out most workers never discover ≥`AUTO_FANOUT_FLOOR` peers, keep `retain_bootstrap`
true, and pin the seed; and a discovered node's sticky active set still retains the seed
(it is in `known` and was the initial member). Fully flattening requires **decoupling
discovery from the active set + a symmetric active view** (peer-sampling / shuffle,
HyParView-style, with reciprocal peer-exchange on inbound pings).

**Decision (2026-06-16):** that membership machinery is exactly what **M5 (SWIM) reworks**
(the plan already notes M4 is *"largely subsumed by M5"*). So M4 ships as the bounded-fan-out
knob + idle-timeout + bounded-forwarding **partial reduction**, and the **flat-`seed_established`
G1 gate moves to M5's definition of done** (Phase 2), where the SWIM membership/peer-sampling
layer lands once instead of building HyParView twice. G2 (no FORWARD saturation / zero dropped
frames) is already comfortably met at 100 nodes.

### Phase 2 — M5 hybrid UDP/TCP (SWIM)
- **What:** heartbeats/pings move to **stateless UDP**; TCP opened **on-demand** for
  anti-entropy (StateRequest/Response) and Data/Signal delivery, then closed. Loss triggers a
  SWIM **indirect probe** (ask `k` peers to ping on your behalf) before marking a node suspect.
- **Touch points:**
  - New UDP socket + datagram codec in `mycelium-core` (sibling to `framing.rs`; a separate
    small frame type — ping / ack / ping-req / ping-req-ack — **not** a `WireMessage` variant).
  - `src/agent/tasks.rs::run_health_monitor` — rewrite to send UDP datagrams to
    `cached_ping_targets`; add the suspect→indirect-probe→confirm state machine with SWIM
    incarnation numbers; failure detection drives the existing peer-eviction path.
  - `mycelium-core/src/connection.rs` — TCP listener stays for anti-entropy/data;
    `get_or_spawn_writer` retained for bulk transfer, but the liveness path no longer holds
    persistent connections.
  - Config: UDP port strategy (decision below), probe timeout, indirect-probe fan-out `k`,
    suspicion multiplier.
- **Risks:** keep ping datagrams < MTU (~512 B); validate UDP traversal of the Docker bridge
  early; the suspect-timing/incarnation state machine is the subtle part — port SWIM's
  `Alive`/`Suspect`/`Dead` logic faithfully.
- **Also owns G1 (moved from M4):** the SWIM membership/peer-sampling layer decouples
  discovery from the active set and makes the active view symmetric, which is what actually
  flattens `seed_established` to ~`2k`. Re-run `make test-scale-baseline` after M5 and show the
  column flat vs the committed `scale-baseline.csv` / `scale-m4.csv`.
- **Exit:** **G1 + G3 green** — `seed_established` flat across 30→100 nodes, and
  `test-scale-resilience RESILIENCE_WORKERS=50` Phase-3 probe passes; zero persistent
  inter-node connections for heartbeats.

#### M5 execution staging (multi-PR; started 2026-06-16)

M5 is too large for one PR — it is a SWIM failure detector *plus* the symmetric
membership/peer-sampling layer that M4 showed is required to flatten G1. It is delivered
in four independently-mergeable stages, each gated behind `GossipConfig::swim_failure_detector`
(default **false**) so every stage is inert for existing deployments until the final cutover
flips the default (the M4-default-flip lesson):

**Progress:** Stage 1 ✅ (PR #15) · Stage 2 ✅ (PR #16) · Stage 3 ✅ (PR #17) · Stage 4 🟢 **G1 + G3 both
green over Docker** (2026-06-17). The root cause of the long in-process/Docker divergence was a config
bug — the demo never called `apply_env_overrides()`, so SWIM was *off* in every Docker scale run; with
SWIM actually on plus the membership/de-pin hardening below, `seed_established` is flat (N=50=24,
N=100=22) and the 50-worker resilience late-joiner passes (11/11). Only the deliberate default flip
(`swim_failure_detector` → true) remains; default stays **off** pending that release decision. Details
below.

#### Stage 4 findings (2026-06-16) — cutover mechanics + the membership-collapse fix

The Stage 4 cutover (gated under `swim_failure_detector`): the TCP heartbeat ping is gone
under SWIM; the forwarding set starts empty, never pins the bootstrap, and is gradually
rotated so no member is permanently retained.

**Load-bearing bug found & fixed:** the health monitor's *staleness eviction* (drop a peer
not heard from within `interval × peer_eviction_intervals`) is the TCP-ping liveness model and
is **wrong under SWIM** — the prober refreshes only *one* peer per period, so each peer is
touched every ~N×period, far slower than the window. The health monitor was evicting live peers
faster than SWIM refreshed them and **collapsing the membership** (in a 13-node in-process repro,
the seed's view fell from 12 → 1). Fix: under SWIM, liveness + eviction are owned **entirely** by
the failure detector (a confirmed-`Dead` member is removed via `apply_effect`); the health
monitor no longer does staleness eviction. With the fix, membership converges and **holds**
(all nodes know all peers).

**G1 mechanism proven (in-process, deterministic):** 1 seed + 50 workers, SWIM on — seed
`peers=50` (converged), seed **outbound ≈ 21**, **inbound ≈ 23** → seed total **≈ 44 vs 2N=102**.
Connections are **bounded ~2k, not N** — G1's flattening works.

#### Stage 4 divergence — RESOLVED (2026-06-16, in-process, not Docker)

The N=50-converges / N=100-doesn't divergence was **reproduced in-process at N=100** with a fast
deterministic oracle (`src/swim_oracle_tests.rs`, `#[ignore]`; 1 seed + N-1 workers over loopback
TCP/UDP, SWIM on, Docker cadence; measures the membership-view-size distribution + the seed's
*live-writer* connection split). It was never Docker networking — at N=100 membership converges fine
by t≈30 (median node knows 99) yet the seed connection count failed to settle to ~2k. Two coupled,
N-scaling amplifiers:

1. **De-pin too slow (seed_in).** The old bulk `~k/3` random rotation per 10 s health tick drained
   the t=0 ~2N spike over ~15 ticks (~150 s) — exactly the Docker "doesn't converge in a window."
2. **Anti-entropy reply-writer storm (seed_out, dominant).** `request_state` fired on *every*
   forwarding-set add and the responder spawns a persistent `writer_idle_timeout` (30 s) reply
   writer. The rotation churn re-added the seed on many nodes inside every idle window, so the seed
   accreted O(N) warm reply writers that never idle-closed. (A measurement trap to note: an
   idle-closed writer leaves a *stale* map entry until lazily reaped, so count `is_live()` writers —
   the in-process analogue of `/proc/net/tcp` ESTABLISHED — not raw map length.)

**Fix** (all gated under `swim_failure_detector`, so the default-off path is untouched — see
`src/agent/tasks.rs::run_health_monitor`):
- **One-time bootstrap de-pin** once a node knows `> 2k` peers — collapses the O(N) early greedy-fill
  seed pinning in a single tick instead of bleeding off over ~150 s. The seed is re-added afterwards
  only at its fair uniform `~k/N` share, like any peer (never permanently excluded).
- **Slow uniform shuffle** — drop ONE random member per tick (not bulk `k/3`), so the set drifts into
  a moving uniform sample without the churn storm.
- **Decoupled anti-entropy** — sync each *current* forwarding member at most once per resync cooldown
  (`interval × peer_eviction_intervals`), instead of on every add. The forwarding set is bounded by
  k, so the seed is anti-entropy-targeted by only ~k nodes per window, not by everyone who briefly
  churned it in. (Bonus: stable members now re-sync periodically — the old on-add trigger never
  re-synced them — and the responder's store-hash fast-path makes a converged exchange empty.)

**G1 result — flat across 30→100 nodes IN-PROCESS** (oracle, live-writer count, SWIM on, Docker
cadence; seed total = outbound + inbound writers, KV canary = nodes that received a seed-written key).
NB: this is loopback (lossless UDP); the Docker re-validation below shows it does *not* yet hold over
the bridge:

| N | seed_out | seed_in | **seed_total** | canary |
|---|---|---|---|---|
| 30 | 13 | 14 | **27** | 30/30 |
| 50 | 20 | 20 | **40** | 50/50 |
| 70 | 20 | 22 | **42** | 70/70 |
| 100 | 19 | 21 | **40** | 100/100 |

`seed_total` is flat from N=50 (40→42→40 across a 2× node increase) and `seed_in` collapses to ~k by
t≈15 s — versus the pre-fix run where `seed_in` sat at N (99) for 100 s+. Reproduce with
`SWIM_ORACLE_N=100 cargo test --lib swim_scale_oracle -- --ignored --nocapture`.

#### ⚠️ CORRECTION (2026-06-17) — the 2026-06-16 Docker numbers below were SWIM-*off*

The real in-process/Docker divergence was a **config bug, not a transport one**: the demo binary
(`examples/three_node_demo.rs::make_agent`) built its config without calling `apply_env_overrides()`,
so `GOSSIP_SWIM_FAILURE_DETECTOR` (and every `GOSSIP_*` knob) was silently ignored — **SWIM was OFF in
every `SWIM=1` scale run**. The whole "in-process converges, Docker doesn't" mystery was the Docker
side running the non-SWIM M4 path (which pins the seed) while the in-process oracle set the bool
directly. The 2026-06-16 table below and its "newest-first gossip / membership" root cause are
therefore measuring the **non-SWIM path** — kept for the record but superseded. Fixed by calling
`apply_env_overrides()` in the demo. With SWIM actually on:

| N | seed_established (SWIM **off**, the bug) | seed_established (SWIM **on**) | worker membership (SWIM on) |
|---|---|---|---|
| 50 | 64–69 | **33** ✓ flat (~2k) | ~21 |
| 100 | 105–121 | **83** (membership-limited) | ~12 |

N=50 is now flat. N=100 is membership-limited: SWIM's UDP gossip only reaches ~12-14 known peers at
N=100 (< the `2k`=24 de-pin threshold), so the de-pin doesn't engage — the gossip *rate* is the
bottleneck at scale. Because the demo fix makes the `GOSSIP_SWIM_*` knobs effective, this is now
*tunable*, and raising the rate confirms the mechanism (N=100, 120 s settle):

| GOSSIP_SWIM_GOSSIP_UPDATES / PROBE_INTERVAL_MS | seed_established | worker membership |
|---|---|---|
| 6 / 1000 (default) | 89 | ~14 |
| 14 / 400 (bumped) | **54** | **~23** |

Higher gossip rate → membership 14→23 (reaching the de-pin threshold) → seed_established 89→54. So
the N=100 path is clear: faster SWIM membership convergence (raise the default gossip rate and/or
lower the de-pin threshold from `2k` toward `~1.5k` so it engages at sparser membership). Full N=100
trajectory: **121** (SWIM off, the bug) → **89** (SWIM on, default) → **54** (SWIM on, tuned).

**Update (2026-06-17) — raised the SWIM gossip-rate defaults** (`swim_probe_interval_ms` 1000→500,
`swim_gossip_updates` 6→12; `swim_probe_timeout_ms` 500→300). Docker re-run (SWIM on, defaults only):

| N | seed_established | worker membership |
|---|---|---|
| 50 | 35 (flat) | **32** (was 21) |
| 100 | 72 (was 89) | **24** (was 14) |

So the raised rate clearly improves membership (N=50: 21→32; N=100: 14→24) and N=100 seed_established
89→72 — but not yet flat, because the membership landed right *at* the old `2k`=24 threshold.

**Update (2026-06-17) — lowered the de-pin threshold + excluded the bootstrap from the reconcile pool
→ N=100 FLAT.** Two coupled changes in `run_health_monitor` (gated under SWIM):
- de-pin engages at `known > k + k/3` (~1.33k, floor `> k`) instead of `> 2k`, so it fires at the
  sparse membership SWIM reaches at scale;
- once de-pinning, the bootstrap is removed from the reconcile *candidate pool*, not just the active
  set — otherwise reconcile re-adds it at `k/known`, which at sparse membership (k=12 of known≈24 ⇒
  ~50%) keeps it pinned on half the cluster. Excluding it holds the seed near-zero inbound; it stays
  current via its own outbound anti-entropy pulls (in-process oracle: seed_total 29, **canary 100%** —
  KV propagation intact with the seed out of every forwarding set).

Docker result (SWIM on, raised gossip defaults + this change):

| N | seed_established | note |
|---|---|---|
| 50 | **24** | = 2k |
| 100 | **22** | = 2k — **flat across N=50→100** |

**Full N=100 trajectory: 121** (SWIM off, the bug) **→ 89** (SWIM on, default gossip) **→ 72** (raised
gossip) **→ 22** (+ lowered threshold + pool exclusion). G1 — flat `seed_established` as N grows — is
now demonstrated **over the real Docker bridge**, not just in-process.

**G3 GREEN (2026-06-17).** `SWIM=1 make test-scale-resilience RESILIENCE_WORKERS=50` — **11/11 PASS,
0 FAIL**, runner exit 0. The load-bearing Phase 3 late-joiner probe passes at 50 workers (joins +
anti-entropy inbound + gossip outbound) — which *cannot* pass on the non-SWIM path (iptables FORWARD
saturation makes the fresh probe's SYN to seed time out at errno 110). Phases 1–4 (formation, crash
+ recovery + anti-entropy, late-joiner, 3× churn) all pass. Two test-infra gaps were fixed to run G3
under SWIM: the resilience compose now sets `GOSSIP_SWIM_FAILURE_DETECTOR` (gated `${SWIM:-0}`), and
the dynamically-started late-joiner probe inherits it (it had run SWIM-off in a SWIM-on cluster).

**Both G1 and G3 are now green over Docker — the M5 Stage-4 cutover criteria are met.** The remaining
step is the deliberate default flip (`swim_failure_detector: false → true`), a release decision.

The `gossip_sample` randomized-tail + continuous de-pin + decoupled anti-entropy changes are correct
and shipped (in-process oracle flat at seed_total=11, canary 100% across N=30..100); they were just
never reached over Docker before the demo fix.

---

#### Stage 4 Docker re-validation (2026-06-16) — [SUPERSEDED: ran SWIM-off, see correction above]

`SWIM=1 SETTLE_SECS=45 make test-scale-baseline` (the fix branch, real Docker bridge, measures
`/proc/net/tcp` ESTABLISHED on the seed):

| N | seed_established (fix) | note |
|---|---|---|
| 30 | 45 (settles ~40) | drains 62→40 by ~t=90 s then holds |
| 50 | 69 | dropped=0 |
| 70 | 91 | dropped=89 |
| 100 | **121** | dropped=59 |

This is **linear (~N+10), not flat** — the in-process oracle gave a false positive because loopback
UDP is lossless. A live drain probe (N=30) decomposed it: the startup ~2N is every worker holding
*two* sockets to the seed (forwarding writer + the seed's reply writer); the drain to ~N+10 is the
anti-entropy fix correctly shedding the **duplicate reply writers**, but **every worker keeps its
*primary* connection** — the seed stays pinned by ~all workers, and the worker peer fan-out plateaus
at ~18/30 distinct peers.

**Localized root cause:** the de-pin only fires once a node knows `> 2k` peers. Over the lossy
Docker-bridge UDP, **SWIM membership gossip does not converge past that threshold** — `gossip_sample`
is *newest-changed-first*, so once a node's view stabilizes it re-gossips the same recent `n` and the
long tail of the roster never re-propagates / heals after a dropped datagram. The reliably-bootstrapped
seed is the one peer every node knows, so it is over-weighted in each node's forwarding sample. The
in-process fix is correct and a real improvement (kills the reply-writer storm + the ~150 s settle
tail → Docker now settles by ~90 s instead of never), but Docker flatness needs the membership layer
to converge under loss.

**Next (closing it out):** (B) test whether SWIM gossip knobs alone close it — bump
`GOSSIP_SWIM_GOSSIP_UPDATES` (6 → fill the 512 B MTU) and lower `GOSSIP_SWIM_PROBE_INTERVAL_MS`; if
env tuning is insufficient, make `gossip_sample` mix newest-changed (fast suspect/dead/join
propagation) with a **uniform-random tail** so the full roster disseminates and heals under loss
(standard SWIM/`memberlist` retransmit behaviour). Then re-run the Docker baseline + **G3**
(`test-scale-resilience RESILIENCE_WORKERS=50`). **Default stays off until G1 (Docker) + G3 are green.**

- **Stage 1 — UDP datagram transport foundation.** New `mycelium-core/src/swim.rs`: the
  `SwimDatagram` enum (`Ping`/`Ack`/`PingReq`/`PingReqAck`) + a compact codec with a version
  byte; a UDP socket bound at the **same port number as the gossip TCP port** (decision below)
  when the flag is on; a recv loop that reflects `Ping → Ack`. Additive + gated → no behaviour
  change. Codec round-trips unit-tested.
- **Stage 2 — SWIM failure detector.** Direct ping→ack with timeout; indirect `PingReq` to `k`
  random peers; `Alive`/`Suspect`/`Dead` state machine with incarnation numbers; drive peer
  eviction from it. TCP retained for anti-entropy + Data/Signal. Replaces the TCP-ping liveness
  path (under the flag).
- **Stage 3 — symmetric membership / peer sampling (the G1 flattener).** Piggyback membership
  deltas on ping/ack; a symmetric active view + periodic shuffle so discovery is decoupled from
  the active set and the seed is not pinned. This is what actually flattens `seed_established`.
- **Stage 4 — cutover + validation.** On-demand TCP for anti-entropy/data; no persistent
  heartbeat connections; flip `swim_failure_detector` default to true; run
  `make test-scale-baseline` (G1 flat) + `test-scale-resilience RESILIENCE_WORKERS=50` (G3).

**UDP-port decision (resolves the open question):** the UDP socket binds the **same port number
as the gossip TCP port** (`bind_port`), on a separate UDP socket — the SWIM/`memberlist`
convention (one port to open in firewalls for both protocols). An optional `swim_udp_port`
override is available for environments that must separate them. Ops/firewall docs updated at
cutover (Stage 4).

### Phase 3 — v12 wire bump: M11 codec + Merkle anti-entropy
- **M11 (hand-rolled codec):** replace `bincode` (RUSTSEC-2025-0141, unmaintained) with a
  ~300-line explicit fixed-layout encoder/decoder for the closed `WireMessage` enum.
  `framing.rs` already hand-builds the header and micro-manages layout (v6 field reorder for
  in-place TTL decrement), so the scope is bounded. Re-point the existing fuzz targets
  (`fuzz/fuzz_targets/`) at the new codec.
- **Merkle anti-entropy:** reshape `StateRequest` from the full
  `key_timestamps: Vec<(Arc<str>, u64)>` index (O(store size) every probe) to per-shard
  **Merkle roots**; descend only divergent subtrees; fetch only missing leaves → bytes-on-wire
  O(divergence). Keep the v7 whole-store `store_hash` XOR as the level-0 skip.
  - Touch: `mycelium-core/src/connection.rs` StateRequest/Response handlers (~lines 254–331);
    `store.rs` gains a shard-Merkle digest alongside `store_hash_acc`.
- **Wire-version mechanics** (per `framing.rs` policy, ~lines 56–67): `WIRE_VERSION=12`,
  `PREV_WIRE_VERSION=11`, add a `WireMessageV11` shim + `From` conversion (empty Merkle root ⇒
  full-index fallback, exactly like the v8 `key_timestamps=vec![]` sentinel); keep the window
  open until the cluster converges, then close `PREV_WIRE_VERSION`.
- **Exit:** **G4 green** — anti-entropy tail bounded by divergence; `cargo audit` bincode
  warning retired.

## 4. Cross-cutting

- **Test matrix:** every phase keeps `cargo test --lib --features tls,metrics,a2a,llm` +
  `cargo clippy --lib --tests … -D warnings` green on **both crates** (transport lives in
  `mycelium-core`). Phase 3 re-points the fuzz job at the new codec.
- **Rolling-upgrade safety:** Phases 1–2 are wire-compatible (no `WireMessage` shape change).
  Only Phase 3 opens a v12 window, carrying *both* M11 and Merkle — a single break.
- **Docs to update on completion:** ROADMAP M4/M5/M11 + the Quilt-DD Merkle note (mark
  shipped); CLAUDE.md §Scale and resilience tests (replace the "v2 structural fix" forward
  reference with the delivered mechanism, raise `RESILIENCE_WORKERS` guidance);
  `docs/operations/tuning.md` (UDP port, fan-out, probe knobs); this plan doc gets a
  per-phase execution record appended as work lands (mirrors `v2-m1/m2/m3` plan docs).

## 5. Open decisions

1. **UDP port strategy** — reuse the gossip port number on a separate UDP socket, or a
   dedicated configurable UDP port? (Affects ops/firewall docs.)
2. **Phase 3 granularity** — land M11 + Merkle truly together in one v12 PR (one break, larger
   PR), or M11 first as a pure codec swap with no semantic change, then Merkle as a second
   v12-window change (smaller blast radius per PR)?

*(Resolved 2026-06-16: DoD retargeted to 100 nodes; M4 ships standalone-first.)*
