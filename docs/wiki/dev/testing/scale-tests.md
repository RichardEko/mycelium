# Scale tests, the Docker-bridge ceiling, and the SWIM saga

↑ [testing](testing.md)

## The iptables FORWARD-chain ceiling (environmental, not a Mycelium bug)

At 100 nodes, peer exchange creates ~5 000 TCP connections in a Docker bridge network; the
Linux bridge iptables FORWARD chain grows O(N²) and new runner→node connections start timing
out (errno 110). Everything below follows from this.

- **`make test-scale` (100 nodes)** passes by verifying the KV write on seed *immediately*
  (before saturation) and reading mgmt via conntrack entries established during earlier
  polling. Formation-within-240s variance (8–94/100 on identical code) is the documented
  ceiling, not a regression — identical code converges at 20/30/50 nodes.
- **`make test-scale-resilience` defaults to 20 workers** because its Phase-3 late-joiner
  probe needs a *fresh* TCP connection mid-test; at 50 workers the chain is already
  saturated. (With SWIM on, 50 workers passed 11/11 — see below.) If Phase 3 fails, suspect
  the chain first; mitigations: macvlan, nftables, keep workers ≤ 20.
- **`make test-scale-entries` (30 nodes, 5 000 × `ENTRY_BYTES` keys)** covers the
  entry-volume axis: live-gossip fraction, anti-entropy sweep tail, stability, sampled
  payload integrity, backpressure (`dropped_frames`; raise `GOSSIP_WRITER_CHANNEL_DEPTH`).
  It stays at 30 nodes because its polling keeps opening connections all test long.
- **Consecutive-run VM fatigue:** repeated 100-node rounds degrade formation monotonically
  in one Docker Desktop session (conntrack/iptables state accumulates in the VM across
  recreated networks). Before calling a formation timeout a regression, `docker desktop
  restart` and re-run once.

The v1 mitigation is `GOSSIP_MAX_ACTIVE_CONNECTIONS` (O(N×K)); the v2 structural fix was
SWIM (below). Anti-entropy has been `O(divergence)` since wire v12 (Merkle buckets) and
frame-chunked since 2026-07-02 ([runtime-invariants](../architecture/runtime-invariants.md)).

**Beyond the ceiling: go multi-host.** Every mitigation above is a *single-host* tweak — the
ceiling itself is that all N containers share one host's bridge/conntrack/iptables. The
structural escape is to spread nodes across **multiple hosts**, where each host carries only
its share of the connections and its own iptables state, so the O(N²) chain never forms on
any one host. The [`deploy/kubernetes/`](../../../../deploy/kubernetes/) reference cluster is
the path: `kubectl scale statefulset mycelium-worker --replicas=N` across a multi-node cluster
(confirm the spread with `kubectl get pods -o wide`). The in-repo Docker harness stays the
single-host *baseline* (fast, CI-adjacent, no cluster needed); multi-host k8s is the
next-level axis for node counts past ~100. Note the k8s manifests are validated offline only
(rendered, not applied in CI) — see their README.

**CI status (2026-07-10):** the scale suites run **nightly on a self-hosted runner**
(`.github/workflows/scale-nightly.yml`, 06:00 UTC, runner label `mycelium-scale` — queued
until the runner is registered). They stay off hosted runners and off the PR path on purpose:
a 2-core hosted runner hits the iptables ceiling above ~50 nodes, and each suite is
dozens-to-100 containers. The small correctness suites are the PR-path gate —
[cluster-suites](cluster-suites.md).

## The WSB-M5 SWIM divergence saga — lessons that outlive the bug

Stage-4 SWIM cutover showed a long in-process/Docker divergence (in-process flat, Docker
linear seed-connection growth). Resolution, in order of lesson value:

1. **The true root cause was config, not networking:** the Docker demo built its config from
   `GossipConfig::default()` and never called `apply_env_overrides()` — so
   `GOSSIP_SWIM_FAILURE_DETECTOR` (and every `GOSSIP_*` knob) was silently ignored; SWIM was
   OFF in every Docker run. **When a Docker test and an in-process test diverge, first
   confirm the binary actually applies the config the test thinks it set.**
2. With SWIM actually on, three mechanism fixes flattened the curve over the real bridge:
   `gossip_sample` newest+random tail (membership heals under UDP loss), faster SWIM gossip
   defaults, de-pin threshold `k+k/3` with the bootstrap excluded from the reconcile *pool*.
   Docker `seed_established`: N=50→24, N=100→22 (from 121). G3: 50-worker resilience 11/11.
3. The in-process oracle (`src/swim_oracle_tests.rs`,
   `SWIM_ORACLE_N=100 cargo test --lib swim_scale_oracle -- --ignored --nocapture`) is the
   fast reproduction harness; the full history is in
   `docs/plans/v2-wsb-scale-transport.md`.

`swim_failure_detector` now defaults **true**. Rolling-upgrade caveat: don't mix SWIM-on/off
nodes — flip a cluster together.
