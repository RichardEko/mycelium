//! Performance baselines for Layer 1 (KV), Layer 2 (signal), and Layer 3 (capability) hot paths.
//!
//! Run with:
//!   cargo bench
//!   cargo bench -- kv                  # only KV benchmarks
//!   cargo bench -- scan_prefix         # only scan_prefix
//!   cargo bench -- signal_fanout       # only signal fan-out
//!   cargo bench -- capability_resolve  # only capability resolution
//!   cargo bench -- kv_payload_size     # KV framing at varying payload sizes
//!
//! All benchmarks use an agent that has NOT called start() — the gossip shard
//! channels buffer outbound frames but are never drained.  This isolates the
//! local hot path (store reads/writes, boundary checks, mpsc fan-out) from
//! network I/O.  The gossip_channel_capacity is set large enough that the
//! channels do not fill during a typical criterion run.

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mycelium::{CapEntry, CapFilter, Capability, GossipAgent, GossipConfig, NodeId, SignalScope};
use std::sync::Arc;

fn bench_agent() -> Arc<GossipAgent> {
    let node_id = NodeId::new("127.0.0.1", 0).unwrap();
    let mut cfg = GossipConfig::default();
    // Large enough that unbounded gossip forwarding does not interfere with
    // the measurement window (criterion typically runs < 10k warm-up iters).
    cfg.gossip_channel_capacity = 1_048_576;
    Arc::new(GossipAgent::new(node_id, cfg))
}

// ── Layer 1: KV store ─────────────────────────────────────────────────────────

fn kv(c: &mut Criterion) {
    let agent = bench_agent();
    let value = Bytes::from_static(b"thirty-two-bytes-of-bench-value!");

    let mut g = c.benchmark_group("kv");

    g.bench_function("set", |b| {
        b.iter(|| {
            let _ = agent.kv().set("bench:key", value.clone());
        });
    });

    let _ = agent.kv().set("read:key", value);
    g.bench_function("get/hit", |b| {
        b.iter(|| agent.kv().get("read:key"));
    });

    g.bench_function("get/miss", |b| {
        b.iter(|| agent.kv().get("no-such-key"));
    });

    g.finish();
}

// ── Layer 1: KV payload size — framing cost at different value sizes ───────────
//
// Measures how the write path scales with value size.  64 / 1 KiB / 64 KiB
// spans typical capability blobs, JSON schema payloads, and bulk-staged chunks.

fn kv_payload_size(c: &mut Criterion) {
    let mut g = c.benchmark_group("kv_payload_size");

    for size in [64usize, 1_024, 65_536] {
        let agent = bench_agent();
        let value = Bytes::from(vec![0xABu8; size]);

        g.bench_with_input(
            BenchmarkId::new("set_bytes", size),
            &size,
            |b, _| {
                b.iter(|| {
                    let _ = agent.kv().set("bench:payload", value.clone());
                });
            },
        );
    }

    g.finish();
}

// ── Layer 1: scan_prefix — O(n) characterisation ──────────────────────────────
//
// Purpose: establish the store-size threshold at which scan_prefix becomes
// a bottleneck for pheromone-trail reads at Layer 3.  Each (n, hits) pair
// measures a store of n total entries where `hits` match the "load/" prefix.

fn scan_prefix(c: &mut Criterion) {
    let value = Bytes::from_static(b"v");
    let mut g = c.benchmark_group("scan_prefix");

    for (total, hits) in [
        (100usize,  10usize),
        (1_000,     10),
        (10_000,    10),
        (10_000,    100),
        (100_000,   10),
    ] {
        let agent = bench_agent();
        for i in 0..hits {
            let _ = agent.kv().set(format!("load/{i:06}"), value.clone());
        }
        for i in 0..(total - hits) {
            let _ = agent.kv().set(format!("data/{i:06}"), value.clone());
        }

        g.bench_with_input(
            BenchmarkId::new(format!("n={total}"), format!("hits={hits}")),
            &(),
            |b, _| b.iter(|| agent.kv().scan_prefix("load/")),
        );
    }
    g.finish();
}

// ── Layer 2: signal fan-out — emit + local deliver + drain ────────────────────
//
// Measures the complete producer-side path:
//   emit() → boundary check → opacity check → deliver() (N mpsc try_send)
//
// Receivers are pre-created with a large capacity and drained synchronously
// via try_recv() so channels never fill and warn!() paths are never hit.
// The gossip forwarding path (try_send to shard channels) always succeeds
// within the measurement window due to the 1M capacity set in bench_agent().

fn signal_fanout(c: &mut Criterion) {
    let agent = bench_agent();
    let payload = Bytes::from_static(b"bench-signal-payload");

    let mut g = c.benchmark_group("signal_fanout");

    for n in [1usize, 4, 16] {
        let kind = format!("bench.signal.{n}");
        // Large capacity so channels never fill during the criterion run.
        let mut rxs: Vec<_> = (0..n)
            .map(|_| agent.mesh().signal_rx_with_capacity(kind.as_str(), 1_048_576))
            .collect();

        g.bench_with_input(BenchmarkId::new("handlers", n), &n, |b, _| {
            b.iter(|| {
                let _ = agent.mesh().emit(kind.as_str(), SignalScope::System, payload.clone());
                // Drain to keep channels empty — ensures opacity stays at 0.0
                // and subsequent iterations hit the same hot path.
                for rx in &mut rxs {
                    while rx.try_recv().is_ok() {}
                }
            });
        });

        drop(rxs);
    }
    g.finish();
}

// ── Layer 3: capability resolution — O(providers) characterisation ────────────
//
// Measures the full resolve() path:
//   scan_prefix("cap/") → decode CapEntry → filter match → collect
//
// Providers are seeded using kv().set() with properly-encoded CapEntry bytes,
// at keys `cap/{port}/compute/gpu`.  The NodeId is a loopback address with a
// synthetic port so parse_cap_key succeeds.  Entries are written just before
// the bench run so they pass the freshness check (HLC timestamp ≈ now).

fn capability_resolve(c: &mut Criterion) {
    let mut g = c.benchmark_group("capability_resolve");

    let encoded = CapEntry {
        capability:          Capability::new("compute", "gpu"),
        refresh_interval_ms: 60_000,
    }.encode();

    for n_providers in [1usize, 10, 50, 100] {
        let agent = bench_agent();

        // Seed providers.  Port 20001..=20100 gives unique valid NodeId strings.
        for i in 0..n_providers {
            let key = format!("cap/127.0.0.1:{}/compute/gpu", 20_001 + i);
            let _ = agent.kv().set(key, encoded.clone());
        }

        let filter = CapFilter::new("compute", "gpu");

        g.bench_with_input(
            BenchmarkId::new("providers", n_providers),
            &n_providers,
            |b, _| b.iter(|| agent.capabilities().resolve(&filter)),
        );
    }

    g.finish();
}

criterion_group!(benches, kv, kv_payload_size, scan_prefix, signal_fanout, capability_resolve);
criterion_main!(benches);
