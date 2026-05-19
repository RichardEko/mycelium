//! Performance baselines for Layer 1 (KV) and Layer 2 (signal) hot paths.
//!
//! Run with:
//!   cargo bench
//!   cargo bench -- kv              # only KV benchmarks
//!   cargo bench -- scan_prefix     # only scan_prefix
//!   cargo bench -- signal_fanout   # only signal fan-out
//!
//! All benchmarks use an agent that has NOT called start() — the gossip shard
//! channels buffer outbound frames but are never drained.  This isolates the
//! local hot path (store reads/writes, boundary checks, mpsc fan-out) from
//! network I/O.  The gossip_channel_capacity is set large enough that the
//! channels do not fill during a typical criterion run.

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mycelium::{GossipAgent, GossipConfig, NodeId, SignalScope};
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
            let _ = agent.set("bench:key", value.clone());
        });
    });

    let _ = agent.set("read:key", value);
    g.bench_function("get/hit", |b| {
        b.iter(|| agent.get("read:key"));
    });

    g.bench_function("get/miss", |b| {
        b.iter(|| agent.get("no-such-key"));
    });

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
            let _ = agent.set(format!("load/{i:06}"), value.clone());
        }
        for i in 0..(total - hits) {
            let _ = agent.set(format!("data/{i:06}"), value.clone());
        }

        g.bench_with_input(
            BenchmarkId::new(format!("n={total}"), format!("hits={hits}")),
            &(),
            |b, _| b.iter(|| agent.scan_prefix("load/")),
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
            .map(|_| agent.signal_rx_with_capacity(kind.as_str(), 1_048_576))
            .collect();

        g.bench_with_input(BenchmarkId::new("handlers", n), &n, |b, _| {
            b.iter(|| {
                let _ = agent.emit(kind.as_str(), SignalScope::System, payload.clone());
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

criterion_group!(benches, kv, scan_prefix, signal_fanout);
criterion_main!(benches);
