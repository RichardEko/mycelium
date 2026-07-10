# Operating the companions

> Audience: **DevOps**. The developer "which companion do I want / how do I build on one" story is
> [building-on-mycelium §5](../guide/building-on-mycelium.md#5-start-from-a-template-not-a-blank-file)
> and the [FAQ](../guide/faq.md). This runbook is about *running one in production*.

The three companions — `mycelium-tuple-space`, `mycelium-blackboard`, `mycelium-wiki` — build only on
Mycelium's public API, but each has an operational surface the core-cluster runbooks don't cover.
[Production-readiness §7](production-readiness.md#7--the-companions-you-actually-use) is the go-live
gate; this is the detail behind it.

**The shared shape** (true of all three):

- **Failover is the capability ring, not consensus.** Each serving role advertises a capability
  (`{ns}.primary` / `.curator`); a watcher promotes when that capability *evaporates*. Lowest-node-id
  wins, reconciled continuously — there is no leader-election consensus and no distributed lock.
  Promotion latency ≈ the capability refresh/evaporation window (`cap_refresh`): failover is
  **seconds**, not instantaneous.
- **Durability is opt-in.** Tuple-space and blackboard ship with their WAL **off** (`persist: false`);
  the wiki needs a node-independent store. A `persist: false` primary that crashes with no secondary
  **loses un-mirrored state**. Provision durability before you depend on it.
- **`shutdown()` is required.** The background loops hold `Arc<Self>`; dropping a companion without
  `shutdown()` leaks it (the wiki makes this a hard invariant, with a canary test).
- **No Prometheus metrics.** None of the three emit `metrics::` series. Observe via their API
  (`depth()` / `is_primary()` / `is_curator()` / `last_lint()`), tuple-space's `sys/tuple/…` KV
  soft-state (`/api/tuple`), and `tracing` logs. A `/metrics`-based companion dashboard will have no
  data — don't build one.

## mycelium-tuple-space — staged pull pipelines

- **Durability.** WAL off by default — set `persist: true` + `wal_path`. fsync every `checkpoint_every`
  appends (default **500**) plus a ~1 s safety sync and on shutdown. Format v2 (reads v1 for rolling
  upgrade; refuses a *newer* format rather than truncating). No WAL **and** no secondary ⇒ a primary
  crash is total loss.
- **Un-acked work re-queues.** `worker_timeout_secs` (default **300**): an item taken but not
  `complete`d within the window is re-queued (at-least-once); the scan runs every 30 s. Set it above
  your longest task — too low duplicates work, too high slows recovery of a dead worker's item.
- **Backpressure.** `high_watermark` (default **500**) — `put` signals backpressure above it
  (hysteresis clears at 0.7×). `mirror_payload_limit` (**1 MiB**) — items above this are **not**
  live-replicated and rely on WAL replay only.
- **Failover.** `cap_refresh` default **10 s**; promotion after a primary crash ≈ 3× that.
  **Cold-start subtlety:** the first-ever promotion on a fresh cluster waits a 10× orphan-grace (the
  #150 startup-lag split-brain guard) — a cold secondary that never saw a primary can take ~100 s to
  self-promote.
- **Observe.** `is_primary()` / `is_secondary()`, `depth()` (non-blocking — safe as a probe); cluster
  metrics as `sys/tuple/{node}/{ns}/…` KV keys (role, wal_bytes, per-stage depth/waiters/inflight/…),
  exposed over HTTP at `/api/tuple`.
- **Teardown.** `shutdown()` retracts the caps, aborts tasks, and **fsyncs the WAL** before returning.
- **Lock note (advanced).** The store has a documented 3-tier lock order (WAL → stage → inflight,
  released in reverse) — relevant only if you profile the hot/compaction path.

## mycelium-blackboard — shared fact pool

- **Durability.** WAL off by default (`persist: true` + `wal_path`); fsync every `checkpoint_every`
  (**500**) plus a 1 s periodic task. Same rule: no WAL + no secondary = total loss on crash.
- **No TTL, no eviction.** A posted fact lives until it is **claimed _and_ acked** — ack is the only
  terminal. An un-drained board grows **unbounded**; size it against consumer throughput and alert on
  `depth().available`. `claim_timeout_secs` (default **300**) re-queues an *in-flight* claim whose
  claimer missed the deadline — that is **not** eviction.
- **Failover.** `cap_refresh` **10 s**; the promotion watch is **simpler** than tuple-space's (no
  orphan-grace guard), so a cold start where two nodes come up together is more exposed to a brief
  split-brain — assign explicit `Primary` / `Secondary` roles on a cold cluster if that matters.
- **Observe.** `is_primary()` / `is_secondary()`, `depth()` → `{available, inflight}`, also
  `GET /gateway/bb/depth`. (`BoardStats` counters exist in-process but are **not** wired to a route or
  KV key — don't rely on them operationally.)
- **Teardown.** `shutdown()` aborts tasks and retracts caps but **does not fsync the WAL** — the
  periodic 1 s sync is the last durability point, so a clean shutdown can lose up to ~1 s (plus up to
  `checkpoint_every` un-synced appends) of tail. Reopen truncates a torn tail cleanly — bounded
  freshness loss, not corruption. For hard-durability teardown, quiesce writers ~2 s before shutdown.

## mycelium-wiki — durable curated canon

The wiki has a genuinely different operational model: a **node-independent store** and an elected
**curator**. (Feature `control-plane`; the data plane is Mycelium-agnostic.)

- **The store is the record of record — and it is NOT gossiped KV.** The corpus lives in a pluggable
  external store (`WikiStore`; `FsStore` = a shared filesystem mount, `S3Store` = a bucket). Only the
  *evaporating proposal queue* is in KV. **This is the load-bearing operational fact:** failover
  transfers *nothing* — a promoted curator resumes against the **same** store. If the store is on
  node-local disk and that node dies, the curated corpus is **gone**. Provision a genuinely
  node-independent store before production. (FsStore writes are atomic per object, manifest-committed
  last for torn-read safety; dropped sections become orphans and there is **no GC yet** — orphan
  growth is unbounded, so schedule a periodic prune.)
- **Curator election / failover.** Each `Auto` node advertises `{group}.candidate`; the **lowest
  node-id** self-elects and advertises `{group}.curator`. A reader promotes after two consecutive
  empty `curator` resolves one `cap_refresh` apart (split-brain guard). A `sentinel` task applies
  lowest-id-wins *continuously*, so a superseded curator `resign`s. Promotion latency ≈ the capability
  evaporation window.
- **Who is the curator?** `is_curator()` locally; cluster-wide, resolve the `{group}.curator`
  capability on any node. `request_store_access` returns `NoCurator` when none is elected — a usable
  liveness probe. No metric or endpoint names the curator.
- **Config.** `role` (`Auto` / `Curator` / `Reader`), `cap_refresh` (**2 s** — advertisement + failover
  granularity), `drain_interval` (**200 ms** — proposal drain), `lint_interval` (**30 s** — group-health
  lint, runs only when the corpus changed). Access-broker membership: `Open` (default) or
  `Allowlist(node-ids)`.
- **Observe.** `is_curator()`, `last_lint()` / `lint_pass_count()` (the group-health report), lint
  `warn!` logs. No metrics series.
- **Teardown — required.** `shutdown()` is **mandatory** to reclaim a `Wiki`: the background loops hold
  strong `Arc<Self>` references, so without it the wiki is a reference cycle that outlives its caller
  (a real leak for any process that creates and discards wikis). Idempotent; it aborts the loops,
  awaits cancellation so the `Arc`s drop, then retracts the advertisements.

## See also

- [production-readiness §7](production-readiness.md#7--the-companions-you-actually-use) — the go-live checklist.
- Design & rationale (maintainer-facing): [`dev/companions/`](../wiki/dev/companions/companions.md) —
  [wiki curator/failover](../wiki/dev/companions/wiki.md#curator-election--failover-the-recallable-role-not-the-coordinator-trap),
  [tuple-space](../wiki/dev/companions/tuple-space.md), [blackboard](../wiki/dev/companions/blackboard.md).
- Developer "which one / how to build on it": [building-on-mycelium](../guide/building-on-mycelium.md), [FAQ](../guide/faq.md).
