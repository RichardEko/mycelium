# Mycelium — Operational Tuning Reference

This document covers all timing and sizing parameters, the invariants that must
hold between them, and how to scale them safely as cluster size grows.

---

## Quick-reference table

Precedence (lowest → highest): TOML config file (`-c <path>`) → CLI flags → environment
variables (`GOSSIP_<FIELD_NAME>` exists for every field). The authoritative field list with
full doc comments is the rustdoc on `mycelium-core/src/config.rs`; this table is the
operator's working set.

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
| `writer_channel_depth` | `1024` | `GOSSIP_WRITER_CHANNEL_DEPTH` | frames |
| `gossip_channel_capacity` | `1024` | `GOSSIP_GOSSIP_CHANNEL_CAPACITY` | frames |
| `max_seen_entries` | `100_000` | `GOSSIP_MAX_SEEN_ENTRIES` | nonces |
| `max_inbound_frames_per_sec` | `0` (off) | `GOSSIP_MAX_INBOUND_FRAMES_PER_SEC` | fps |
| `max_concurrent_bulk_handlers` | `64` | `GOSSIP_MAX_CONCURRENT_BULK_HANDLERS` | tasks |
| `gossip_shards` | CPU count (≤16) | — | shards |
| `writer_idle_timeout_secs` | `30` | `GOSSIP_WRITER_IDLE_TIMEOUT_SECS` | s |
| `swim_failure_detector` | `true` | `GOSSIP_SWIM_FAILURE_DETECTOR` | bool |
| `swim_udp_port` | same as `bind_port` | `GOSSIP_SWIM_UDP_PORT` | port |
| `swim_probe_interval_ms` | `500` | `GOSSIP_SWIM_PROBE_INTERVAL_MS` | ms |
| `swim_probe_timeout_ms` | `300` | `GOSSIP_SWIM_PROBE_TIMEOUT_MS` | ms |
| `swim_indirect_probes` | `3` | `GOSSIP_SWIM_INDIRECT_PROBES` | peers |
| `swim_gossip_updates` | `12` | `GOSSIP_SWIM_GOSSIP_UPDATES` | updates/datagram |
| `swim_suspicion_timeout_ms` | `4000` | `GOSSIP_SWIM_SUSPICION_TIMEOUT_MS` | ms |
| `bind_address` | `127.0.0.1` | `GOSSIP_BIND_ADDRESS` | TCP listen address |
| `bind_port` | `8080` | `GOSSIP_BIND_PORT` | TCP listen port |
| `bootstrap_peers` | `[]` | `GOSSIP_BOOTSTRAP_PEERS` | Peers to contact on startup |
| `epidemic_extra_peers` | `3` | `GOSSIP_EPIDEMIC_EXTRA_PEERS` | Extra random non-member peers added to Group-scoped signal fan-out when `group_aware_forwarding = true`. Ensures epidemic coverage beyond the group. Raise to 5–7 for clusters > 1 000 nodes. |
| `group_aware_forwarding` | `true` | `GOSSIP_GROUP_AWARE_FORWARDING` | When true, Group signals are forwarded only to known group members plus `epidemic_extra_peers` random non-members. Set to `false` to revert to pre-v0.2 broadcast forwarding. |
| `signal_window_secs` | `600` | `GOSSIP_SIGNAL_WINDOW_SECS` | Retention window for the in-memory sender log and `quorum_written` rate-limit tracker. |
| `max_store_entries` | `0` (unlimited) | `GOSSIP_MAX_STORE_ENTRIES` | Hard cap on live KV entries. New live writes are silently dropped once reached; tombstones always accepted. |
| `intern_keys` | `true` | `GOSSIP_INTERN_KEYS` | Intern received keys in a process-wide pool so all connection handlers share one `Arc<str>` per distinct key. Disable for workloads with unbounded key spaces (e.g. UUID keys). |
| `intern_max_keys` | `0` (unlimited) | `GOSSIP_INTERN_MAX_KEYS` | Maximum keys in the intern pool. New keys bypass interning once reached. Only meaningful when `intern_keys = true`. |
| `health_check_max_jitter_ms` | `0` | `GOSSIP_HEALTH_CHECK_MAX_JITTER_MS` | Startup jitter cap (ms) before the first health-check ping. `0` = up to `health_check_interval_secs × 500` ms. Set to a small value (e.g. `50`) in test configs. |

---

## Auto-derivation — let the cluster size itself (WS-C M8)

Most of the size-dependent knobs above don't need to be set by hand. Construct the
config with [`GossipConfig::auto()`](../../mycelium-core/src/config.rs) (or set any one
field to its `0` "auto" sentinel) and `GossipAgent::new` derives a correct value from a
cluster-size estimate **N** (`bootstrap_peers` + self, a lower bound) at construction
time — before any task spawns, no consensus, no wire change. **An explicit non-zero value
— in code or via `GOSSIP_*` — always wins**, so you can auto-size everything and override
just the one knob you care about.

| Field | Auto formula | Notes |
|---|---|---|
| `default_ttl` | `max(5, ⌈log₂(N+1)⌉)` | covers the gossip diameter (invariant 4) |
| `writer_channel_depth` | `max(1024, N × 4)` | per-peer **node-count** fan-in floor (invariant 5). The *entry-volume* axis is orthogonal — a single node bulk-writing thousands of keys still wants a deeper channel; size that from the write burst, not N. |
| `max_seen_entries` | `max(100_000, N × 1000)` | dedup horizon scales with origin count |
| `ping_peer_sample_size` | `min(N, max(20, ⌊√N⌋))` | bounds Ping fan-in at large N |
| `propagation_window_secs` | `max(60, health_check_interval × peer_eviction_intervals × 2)` | ≥ eviction window (invariant 3) |

Fan-out (`gossip_fanout` / `max_active_connections`) is **already auto by default** — it is
resolved *live* per known-peer count by `resolved_fanout` (`k ≈ 2·⌈log₂ N⌉`, floored at
`AUTO_FANOUT_FLOOR = 8`), so small clusters stay full-mesh and only larger ones are bounded.

`GossipAgent::new` also runs `audit_invariants()`, which logs a `warn!` (detection, not
prevention) if your *explicit* values violate a cross-field invariant below — the config is
still honoured. Inspect the resolved values with `GossipAgent::config()`.

`GOSSIP_<FIELD>=0` is the env-level "auto" for any of these fields; leaving the env unset
keeps the static default in the table above (auto is opt-in via `auto()` or a `0`).

### Live retuning — hot-reload (WS-C M9)

Three params are **hot-reloadable** on a running node, no restart: `max_inbound_frames_per_sec`
(sampled per inbound frame), `max_concurrent_bulk_handlers` (sampled per bulk admission), and
`writer_channel_depth` (sampled at each *new* writer spawn — existing peers keep their channel).
Set them live with `GossipAgent::{set_max_inbound_frames_per_sec, set_max_concurrent_bulk_handlers,
set_writer_channel_depth}`; read the current live values with `hot_tunables()`.

For self-tuning, call `GossipAgent::start_cluster_tuner(interval, policy)` — a decentralized,
coordinator-free advisor: each node observes the live peer count, recomputes the M8 formula, and
gossips a recommendation to `sys/config/{param}`; every node applies it **only if its local
`ConfigPolicy` accepts it** (`accept_all()` / `reject_all()` / `clamped(min, max)`, or your own
closure). `start_config_applier(policy)` opts a node into applying without advising. The advisor
*advises*; the node *decides*.

### Governing the auto-tuner (WS-C M9 — management as intent)

Management constrains the auto-tuner through **intents, never commands** (see the
*management-as-intent* design principle). Two control surfaces, both gating the auto-tuner only
(a deliberate manual `set_*` is the operator's own override):

- **Local (sovereign).** On a node: `set_dynamic_tuning(bool)` (master enable/disable),
  `lock_tuning_floor(param, v)` / `lock_tuning_ceiling(param, v)` (low/high watermark),
  `set_tuning_ratchet(param, Ratchet::Up|Down|Off)` (one-way: `Up` never auto-decreases,
  `Down` never auto-increases), `clear_tuning_locks(param)` / `clear_all_tuning_locks()`.
  Inspect with `tuning_governor()`. `param` is a `HotParam` (`InboundFps` / `WriterDepth` /
  `BulkHandlers`).
- **Fleet (advisory, evaporating).** Any entity with a concern (human via gateway, or an agent)
  calls `publish_tuning_intent(GovernIntent)` → gossiped to `sys/govern/fleet`. Nodes running
  `start_governor_reconciler()` apply it **only where they have not locally pinned the param**
  (local always wins) and **only while it is fresh** — re-publish within `GOVERN_INTENT_TTL_MS`
  (5 min) or it evaporates and the node self-heals to its own derivation. Nothing is ever
  permanently locked: a lock/ratchet is just the currently-winning intent, lifted by a newer one.

#### Operator surface — HTTP gateway (WS-C Track 3)

The fleet-intent surface is also exposed over the gateway, so a HITL operator (or an
external agent) governs without a Rust dependency. HTTP is **opt-in per node** (the
`gateway` feature); headless nodes still reconcile and self-heal — they just are not an
operator entry point. Publishing is an idempotent LWW KV write, so **any** gateway node
accepts the POST and it gossips to converge — no elected/active endpoint, no forwarding
(want one URL? put an operator-side load balancer in front of the gateway nodes).

| Route | Scope | Body / effect |
|---|---|---|
| `POST /gateway/govern/tuning` | `govern:write` | `{"enabled":bool?, "params":[{"param","floor"?,"ceiling"?,"ratchet":"up\|down\|off"}], "target":NodeId?}` → publishes a `GovernIntent` to `sys/govern/fleet` |
| `POST /gateway/govern/membership` | `govern:write` | `{"group", "min", "max"?, "drain":[NodeId]?, "target":NodeId?}` → publishes a `MembershipIntent` to `sys/govern/membership/{group}` |
| `GET /gateway/govern` | `govern:read` | this node's **effective** tuning-governor snapshot (reconciled local pins + fleet intent) |

- **Per-node control without per-node HTTP:** set `target: <NodeId>` on any intent and POST
  it to *any* gateway node — it gossips to everyone (including headless nodes) and only the
  named node applies it (with local veto). Never reach a node's own HTTP for this.
- **Scopes are deny-by-default** (`compliance`): `govern:read` / `govern:write` must be granted
  via `gateway_scoped_tokens` (or OIDC group mapping); an unmapped token is 401, an
  insufficient one 403.
- **Provenance:** every governance POST is sealed into the WS2 tamper-evident audit trail
  (`action = Admin`, principal `gateway/govern`), queryable + verifiable at `GET /gateway/audit`.
- **Observability:** per-node Prometheus gauges (`mycelium_governor_auto_enabled`,
  `mycelium_governor_{floor,ceiling,ratchet,locally_pinned}{param=…}`) report **effective**
  state on `/metrics` (`metrics` feature; ceiling `-1` = unbounded, floor `0` = no floor). The
  operator aggregates the fleet view in their own Prometheus/Grafana — *library, not platform*.
  Prometheus carries effective state only; intent history lives in KV + the audit trail.

---

## Gossip transport modes — SWIM (default) vs legacy TCP-ping

Mycelium has two liveness/membership modes. They share the same TCP data plane
(KV/Signal delivery + anti-entropy); they differ only in *how a node learns which
peers exist and which are alive*.

| | **SWIM** (default, `swim_failure_detector = true`) | **Legacy TCP-ping** (`=0`) |
|---|---|---|
| Liveness | UDP failure detector: direct probe → indirect probe (via `k` relays) → suspect → dead | TCP `Ping` heartbeats; a peer not heard from within `health_check_interval × peer_eviction_intervals` is evicted |
| Discovery | Membership gossip piggybacked on every probe (UDP) | Peer list piggybacked on TCP `Ping` (`ping_peer_sample_size`) |
| Connection cost | **~2k** persistent TCP connections per node (k = fan-out), independent of N — UDP probes leave no conntrack/iptables-FORWARD state | **O(N×K)** capped by `max_active_connections`, but every probe is a TCP connection |
| Eviction owner | the failure detector (health-monitor staleness eviction is **disabled**) | the health monitor (staleness eviction) |
| Scales to | ~100+ nodes on a Linux bridge without hitting the iptables FORWARD ceiling | bounded by `max_active_connections`; the connection churn still pressures conntrack |

**Why SWIM is the default.** On a Linux bridge the iptables FORWARD chain grows
O(N²) with persistent TCP connections and the late-joiner/new-connection path
stalls (errno 110) well below 100 nodes. SWIM moves liveness/heartbeats to
connection-free UDP, so the seed and every node hold a flat ~2k TCP connections
regardless of cluster size. Validated over Docker: `seed_established` flat at
N=50 (24) and N=100 (22); 50-worker resilience late-joiner passes (see
`docs/plans/v2-wsb-scale-transport.md`).

**Switching.** Set `GOSSIP_SWIM_FAILURE_DETECTOR=0` to fall back to the legacy
TCP-ping path (e.g. environments where intra-cluster UDP is blocked). The UDP
socket binds the **same port number as the gossip TCP port** by default (one
firewall rule for both); override with `GOSSIP_SWIM_UDP_PORT` only if TCP/UDP must
be separated.

> **⚠ Rolling-upgrade caveat — do not mix modes in one cluster.**
> A SWIM-on node owns liveness and evicts any peer that fails its UDP probes — so
> a SWIM-on node will mark a SWIM-*off* peer (no UDP listener) **Dead** and drop it.
> Flip a whole cluster together. For a staged upgrade, pin
> `GOSSIP_SWIM_FAILURE_DETECTOR=0` on the new binary until every node carries it,
> then restart the cluster into SWIM-on. (Not a concern for fresh clusters.)

### SWIM tuning by environment size

The SWIM defaults (`probe 500 ms`, `12 gossip updates/datagram`) are tuned for
membership to converge **past the de-pin threshold (`> k + k/3`) at ~100 nodes
over a lossy bridge**. The membership-gossip *rate* (`updates × 1000/probe_ms`) is
the knob that matters as N grows — if it is too low, membership stays sparse, the
well-known seed stays over-represented in forwarding sets, and `seed_established`
creeps toward N instead of staying flat.

| Cluster size | SWIM settings | Notes |
|---|---|---|
| ≤ ~30 nodes | defaults | membership converges trivially; nothing to tune |
| ~50–150 nodes | defaults (`probe 500`, `updates 12`) | the validated operating point (G1/G3 green) |
| ~150–500 nodes | `swim_gossip_updates = 12–15`, `swim_probe_interval_ms = 400` | raise the gossip *rate* so membership still crosses the de-pin threshold; keep one datagram under the 512 B MTU (≈ 13 × ~25 B) |
| flaky/lossy network | raise `swim_suspicion_timeout_ms` (e.g. 8000) and/or `swim_indirect_probes` (e.g. 4) | fewer false-positive evictions when probes are dropped |

Diagnostics: `GET /stats` exposes `peers` (SWIM membership view size — should
approach N) and `cached_connections` (live persistent writers — should stay ~k).
A `peers` value well below N at scale means the gossip rate needs raising.

---

## Hard invariants

These relationships must hold or the cluster will oscillate, stall, or produce
spurious anti-entropy storms. They are not enforced by `validate()` — violating
them is legal but pathological.

> **Mode note.** Invariants #1 and #2 (startup `StateRequest` timing and the
> staleness-eviction window) describe the **legacy TCP-ping liveness path**. Under
> the default **SWIM** mode, liveness and eviction are owned by the UDP failure
> detector, not the health monitor — see §"Gossip transport modes". Invariants #3–#5
> (propagation window, TTL, writer-channel depth) and everything below apply to
> both modes; the `max_active_connections` caps in §Scaling remain useful as a hard
> ceiling but are no longer the primary connection-bound under SWIM (which is
> inherently ~2k).

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
writer_channel_depth        = 1024      # default; listed for completeness
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
writer_channel_depth        = 4096      # N × fan-out = 4 000 at N = 1 000
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

## RPC-heavy pairs & the topology-pressure warn

The forwarding-target set deliberately de-pins non-active peers (seed scalability), so an
Individual-scoped **request-response RPC** between two specific nodes can degrade to
flood-relay latency and time out. The node tells you when this happens:

```
WARN Individual-scoped frame has no direct route; flooding via relay
     (topology pressure — consider peering RPC-heavy pairs directly) target_node=…
```

and `individual_flood_fallbacks` grows on `/stats`. **Remedy:** the side that initiates the
RPCs pins the route — `agent.connect_peer(peer)` (idempotent; also pre-warms the TCP
connection so the first RPC doesn't pay setup on its own deadline; `disconnect_peer`
reverts). One-shot signals never need this — flooding is correct for them; pin only *hot
request-response pairs* (the tuple-space does this automatically for secondary↔primary —
the pattern to copy is its warm-keeper, `mycelium-tuple-space/src/lib.rs`). Background:
[runtime-invariants](../wiki/dev/architecture/runtime-invariants.md).

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

---

## Performance baselines

*Moved from the repo README (2026-07-10).*

Measured on the development machine, release build (`cargo bench`). Local hot-path only — no network I/O. Run `cargo bench` to regenerate on target hardware.

| Benchmark | Median | Notes |
|---|---|---|
| `kv/set` | 151 ns | Local store write + gossip channel dispatch |
| `kv/get` hit | 16 ns | Lock-free papaya read |
| `kv/get` miss | 13 ns | Same path, no allocation |
| `scan_prefix` 100 entries | 332 ns | Typical pheromone-trail store size |
| `scan_prefix` 1,000 entries | 2.7 µs | |
| `scan_prefix` 10,000 entries | 41 µs | |
| `scan_prefix` 100,000 entries | 622 µs | **~1 ms — monitor if store grows here** |
| `signal_fanout` 1 handler | ~700 ns | emit + boundary check + deliver + drain |
| `signal_fanout` 4 handlers | ~1.0 µs | |
| `signal_fanout` 16 handlers | ~1.4 µs | Very flat — mpsc try_send is cheap |

`scan_prefix` uses a prefix index for a fast O(|segment_keys|) path when the prefix segment is known (e.g. `"load/"`, `"grp/"`, `"svc/"`). Unknown prefixes fall back to an O(store_size) full scan. At typical pheromone-trail sizes (100–1,000 entries per segment) the cost is negligible relative to network latency.
