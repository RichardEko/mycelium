# Mycelium — Operational Tuning Reference

This document covers all timing and sizing parameters, the invariants that must
hold between them, and how to scale them safely as cluster size grows.

---

## Quick-reference table

| Parameter | Default | Env var | Unit |
|---|---:|---|---|
| `health_check_interval_secs` | `10` | `GOSSIP_HEALTH_CHECK_INTERVAL_SECS` | s |
| `reconnect_backoff_secs` | `5` | `GOSSIP_RECONNECT_BACKOFF_SECS` | s |
| `peer_eviction_intervals` | `3` | `GOSSIP_PEER_EVICTION_INTERVALS` | × interval |
| `propagation_window_secs` | `60` | `GOSSIP_PROPAGATION_WINDOW_SECS` | s |
| `default_ttl` | `5` | `GOSSIP_DEFAULT_TTL` | hops |
| `max_active_connections` | `0` (unlimited) | `GOSSIP_MAX_ACTIVE_CONNECTIONS` | connections |
| `max_forwarding_peers` | unlimited | `GOSSIP_MAX_FORWARDING_PEERS` | peers |
| `ping_peer_sample_size` | `20` | `GOSSIP_PING_PEER_SAMPLE_SIZE` | peers |
| `max_peers` | unlimited | `GOSSIP_MAX_PEERS` | peers |
| `writer_channel_depth` | `256` | `GOSSIP_WRITER_CHANNEL_DEPTH` | frames |
| `gossip_channel_capacity` | `1024` | `GOSSIP_GOSSIP_CHANNEL_CAPACITY` | frames |
| `max_seen_entries` | `100_000` | `GOSSIP_MAX_SEEN_ENTRIES` | nonces |
| `max_inbound_frames_per_sec` | `0` (off) | `GOSSIP_MAX_INBOUND_FRAMES_PER_SEC` | fps |
| `max_concurrent_bulk_handlers` | `64` | `GOSSIP_MAX_CONCURRENT_BULK_HANDLERS` | tasks |
| `gossip_shards` | CPU count (≤16) | — | shards |
| `writer_idle_timeout_secs` | `0` (off) | `GOSSIP_WRITER_IDLE_TIMEOUT_SECS` | s |

---

## Hard invariants

These relationships must hold or the cluster will oscillate, stall, or produce
spurious anti-entropy storms. They are not enforced by `validate()` — violating
them is legal but pathological.

### 1 — Backoff must be shorter than the health-check interval (minus safety margin)

```
reconnect_backoff_secs  <  health_check_interval_secs − 2
```

**Why it matters.**  When a node restarts it sends a startup `StateRequest` to
each bootstrap peer (at t ≈ startup + jitter). The receiver sets an
anti-entropy cooldown equal to `health_check_interval_secs − 1`.  The health
monitor's first tick fires at t ≈ startup + interval, which is the first moment
a *retry* `StateRequest` can be sent.  For that retry to arrive after the
cooldown has expired, the startup `StateRequest` must have landed more than
`interval − 1` seconds earlier — which is only true if the startup request went
out at time  `t < 1 s` into the interval.

Separately, the startup `StateRequest`'s *response* travels from the peer back
via a TCP writer.  If that writer is in reconnect backoff (because the node was
recently absent), the response is silently dropped.  The backoff must clear
before the health monitor's first tick fires, so the retry arrives when the
writer is actually connected.

**Combined constraint:**
```
reconnect_backoff_secs  <  health_check_interval_secs − 2
```

At the defaults (backoff = 5 s, interval = 10 s) the margin is 3 s.
The integration-test demo binary uses backoff = 2 s to maximise convergence
speed; at that value the margin is 7 s and `GOSSIP_RECONNECT_BACKOFF_SECS=2`
is safe.

**If you increase `health_check_interval_secs`** (e.g. to 30 s for a quieter
cluster), you can also raise `reconnect_backoff_secs` up to `interval − 3` s
without violating the invariant.  Raising backoff reduces reconnect-storm
pressure on large clusters (see §Scaling).

---

### 2 — Eviction window must exceed the maximum expected restart gap

```
health_check_interval_secs × peer_eviction_intervals  >  longest_restart_gap_secs
```

`longest_restart_gap_secs` is the wall-clock time from when a node stops to
when it is fully accepting TCP connections again.  For Docker containers this is
typically 3–10 s.  For VMs or bare-metal nodes that must reboot it can be
several minutes.

**Why it matters.**  A peer that is evicted before it comes back loses its
"trusted" status.  The restarted node's startup `StateRequest` will be rejected
by `StateRequest → unknown peer` guards, and it must wait for a full Ping
exchange (one tick = `health_check_interval_secs`) before anti-entropy can
start.  That delay compounds with the backoff invariant above and can push
convergence beyond the operator's expectation.

**Default:** 10 s × 3 = 30 s — sufficient for Docker and most cloud VM restarts.

For bare-metal reboots (60–120 s): set `peer_eviction_intervals = 15` or raise
`health_check_interval_secs` to 20 s and keep intervals at 3 (= 60 s window).

---

### 3 — Propagation window must cover the eviction window

```
propagation_window_secs  ≥  health_check_interval_secs × peer_eviction_intervals
```

`propagation_window_secs` controls two expiry cutoffs:
- Seen-set nonce retention (duplicate suppression).
- Tombstone GC retention.

If the propagation window is shorter than the eviction window, a tombstone can
be GC'd before a restarted node has had a chance to receive it.  On rejoin the
restarted node's store will not have the tombstone, so the value will resurface —
effectively an "undelete" race.

**Default:** 60 s is 2× the default eviction window (30 s), which provides a
2× safety margin.  If you extend the eviction window, raise
`propagation_window_secs` proportionally.

---

### 4 — TTL (hop count) must be at least the gossip diameter

```
default_ttl  ≥  ceil(log₂(N))    where N = cluster size
```

`default_ttl` is a *hop counter*, not a wall-clock expiry.  Each forwarding
hop decrements it by one; a frame with TTL = 1 is applied locally and not
forwarded.  A value that is too low partitions the gossip graph — nodes beyond
`default_ttl` hops never see the update.

| Cluster size | Minimum TTL |
|---:|---:|
| ≤ 4 nodes | 2 |
| ≤ 8 nodes | 3 |
| ≤ 32 nodes | 5 |
| ≤ 256 nodes | 8 |
| ≤ 1 024 nodes | 10 |

The default of **5** is safe up to ~32 nodes.  Large TTL values do not cause
oscillation (the seen-set deduplicates copies), but they do increase the total
message volume.  A broadcast storm cannot happen: each nonce is forwarded at
most once per node regardless of TTL.

**Ringing / amplification.**  The total message copies for one write is bounded
by `N × fanout` (not `fanout^TTL`) because the seen-set stops re-forwarding.
The amplification factor is therefore `fanout` (typically 4–8 from
`max_forwarding_peers`), not exponential.

---

### 5 — Writer channel depth must cover burst fan-in

```
writer_channel_depth  ≥  N × default_ttl    (approximate; exact = worst-case burst)
```

Each peer-writer has an MPSC channel of depth `writer_channel_depth`.  In a
write burst (e.g. node startup anti-entropy, or a bulk `set` loop), the
intermediate node that receives the most simultaneous forwards can saturate its
inbound processing, causing downstream writers to queue up.  When a writer's
channel fills, frames are **silently dropped** (only visible via
`system_stats().dropped_frames`).

At the defaults (depth = 256, fanout ≤ 4) the queue absorbs bursts of up to
64 simultaneous in-flight writes.  For clusters of >64 nodes with frequent
bulk writes, raise to 1024 or more.

---

## Scaling guidelines by cluster size

### Small cluster (≤ 20 nodes)

Defaults are correct.  Full mesh (`max_active_connections = 0`) is fine.

```toml
health_check_interval_secs = 10
reconnect_backoff_secs      = 5
peer_eviction_intervals     = 3
default_ttl                 = 5
max_active_connections      = 0   # full mesh
```

### Medium cluster (20–100 nodes)

The O(N²) TCP connection count becomes the binding constraint around 50 nodes
on Linux bridge networks (iptables FORWARD chain saturation — see CLAUDE.md §iptables).
Cap outbound connections per node to avoid the cliff.

```toml
health_check_interval_secs = 10
reconnect_backoff_secs      = 5
peer_eviction_intervals     = 3
default_ttl                 = 7         # log₂(100) ≈ 7
max_active_connections      = 16        # √N ≈ 10; use 16 for margin
max_forwarding_peers        = 8
ping_peer_sample_size       = 20        # keep; gossip-discovery still works
writer_channel_depth        = 512
max_seen_entries            = 250_000   # 100k × 2.5
```

Gossip diameter at K=16: log(100)/log(16) ≈ 1.7 hops. All nodes are reachable
within 2 hops; TTL = 7 is more than sufficient.

### Large cluster (100–1 000 nodes)

Raise the interval to reduce ping traffic, raise the backoff proportionally to
preserve the invariant, and cap connections aggressively.

```toml
health_check_interval_secs = 30
reconnect_backoff_secs      = 10        # < 30 − 2 = 28 ✓
peer_eviction_intervals     = 3         # eviction window = 90 s
propagation_window_secs     = 180       # ≥ 90 ✓
default_ttl                 = 10        # log₂(1000) ≈ 10
max_active_connections      = 20        # log(N); gossip diameter ≈ 3 hops
max_forwarding_peers        = 6
ping_peer_sample_size       = 20
writer_channel_depth        = 1024
gossip_channel_capacity     = 4096
max_seen_entries            = 1_000_000 # ~24 MB; scale with write rate
max_inbound_frames_per_sec  = 500       # guard against misbehaving peers
```

### Very large cluster (> 1 000 nodes)

At this scale Mycelium's full-mesh assumption breaks down; hybrid TCP/UDP
transport (v2 roadmap item) is the structural fix.  In the meantime:

- Use a dedicated seed tier (2–3 long-lived nodes, all others connect only to seeds).
- Set `max_active_connections = bootstrap_peers.len() + 5` to keep each node's
  connection count bounded regardless of topology discovery.
- Accept higher gossip diameter (3–4 hops) and raise TTL accordingly.
- Enable `writer_idle_timeout_secs = 120` so idle connections to transient peers
  are reclaimed, preventing fd exhaustion.

---

## Reconnect storm mitigation

After a network partition heals, all partitioned nodes simultaneously try to
reconnect, send Pings, and trigger anti-entropy.  The risk is:

1. **Connect storm** — O(N²) concurrent TCP SYN packets.
2. **Anti-entropy storm** — every node sends a `StateRequest` to every other;
   each receiver schedules a full KV scan.

**Mitigations already in place:**
- `health_check_max_jitter_ms` (default 0) — add 200–2000 ms of random jitter
  to each node's first Ping tick after startup or reconnect.  This staggers the
  reconnect wave across the cluster.
- Anti-entropy cooldown (`health_check_interval_secs − 1` per connection) — each
  connection processes at most one full scan per health-check interval.  The
  rate of full scans cluster-wide is bounded by `N/interval`.
- `max_active_connections` — caps simultaneous outbound TCP connections per node,
  reducing the burst connection count from O(N²) to O(N × K).
- Delta sync — `StateRequest` includes the sender's key→timestamp index; the
  receiver only sends keys the requester is missing.  A fully-synced node
  receives an empty `StateResponse` (hash fast-path), costing only a few bytes
  of network I/O.

**Recommended jitter setting for large clusters:**
```toml
health_check_max_jitter_ms = 2000   # stagger up to 2 s across all nodes
```

This spreads the first Ping tick uniformly over a 2-second window, reducing
the peak connection rate by the cluster size factor.

---

## Seen-set and duplicate suppression

Every Data (KV) and Signal frame carries a random nonce.  The seen-set records
each nonce for `propagation_window_secs` and discards frames whose nonce was
already seen.  This is the primary mechanism preventing message ringing.

**Capacity pressure.**  In a cluster of N nodes each emitting F signals/second,
the seen-set fills at rate `N × F × propagation_window_secs` entries.  At
100 nodes × 1 signal/s × 60 s = 6 000 entries — well within the 100 000 default.
At 1 000 nodes × 10 signals/s × 180 s = 1 800 000 entries — raise `max_seen_entries`.

When the seen-set is full, graduated eviction removes the oldest entries.  This
does not cause ringing but can allow duplicate processing of very old re-delivered
frames.

---

## Tombstone safety window

Tombstones are retained for:
```
default_ttl × propagation_window_secs × 10
```

At defaults: 5 × 60 × 10 = 3 000 seconds (~50 min).  This is intentionally
conservative: a node partitioned for up to 50 minutes will still receive the
tombstone on rejoin and not resurface the deleted value.

The `×10` factor is a hardcoded safety multiplier in the GC task.  If your
partition tolerance requirement is longer than `default_ttl × propagation_window_secs`
seconds, raise `propagation_window_secs` rather than relying on this multiplier.

---

## Monitoring checklist

| Metric / stat | Healthy | Investigate when |
|---|---|---|
| `system_stats().dropped_frames` | stable or 0 | growing — writer channels filling |
| `system_stats().peers` | ≈ cluster size (or K for capped mesh) | 0 — node is isolated |
| `system_stats().store_entries` | matches expected key set | diverging from other nodes |
| `system_stats().gc_alive` | `true` | `false` — GC task crashed |
| `GET /ready` | 200 | non-200 — caps advertised or dead shards |
| `GET /stats` `dropped_frames` | same as above | — |
| `system_stats().active_bulk_handlers` | 0 or low | at `max_concurrent_bulk_handlers` ceiling — raise limit or reduce bulk call rate |
| Warn log `Write to X failed` | occasional on restart | persistent — peer unreachable |
| Warn log `Gossip shard N full` | none | writer_channel_depth or gossip_channel_capacity too small |
| Warn log `bulk_serve: handler concurrency limit reached` | none | active_bulk_handlers at ceiling — incoming bulk signals being dropped |
