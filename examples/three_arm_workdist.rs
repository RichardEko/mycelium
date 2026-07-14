//! # Three-arm work-distribution experiment
//!
//! The outcome-level comparison Paper 1 §9.5 names "Completing the three-arm
//! work-distribution experiment" and Paper 2a § "What Remains Open
//! Empirically" specifies. Design doc: `docs/plans/three_arm_workdist.md`.
//!
//! Three arms, one substrate, identical workload:
//!
//! - **MODE=broker** — prediction-central: a designated node answers
//!   `pick_worker` RPCs from its own gossip view of `wkr/*/load`.
//! - **MODE=gossip** — prediction-local: the submitting client picks from
//!   *its* gossip view. Same policy (lowest perceived queue), different
//!   location of the stale view.
//! - **MODE=pull**   — no prediction: jobs go into a tuple-space lane;
//!   workers `take()` when actually free. Readiness is the claim itself.
//!
//! Because pull abolishes the staleness/misroute vocabulary, the arms are
//! compared **only on outcomes**: end-to-end job latency, throughput,
//! idle-while-work-exists, and fairness (Jain index over worker utilisation).
//!
//! Complement, not superset in every axis: [`coordinator_comparison.rs`](coordinator_comparison.rs)
//! is the two-arm *decision-level* probe (broker vs gossip prediction, measured
//! on staleness/misroute — the vocabulary pull removes). It is the harness Paper 1
//! cites for the two prediction arms already run; this binary adds the pull arm and
//! the outcome-level instruments. Keep both.
//!
//! ## Workload knobs (env)
//!
//! | Var | Default | Meaning |
//! |---|---|---|
//! | `MODE`            | `pull`  | broker \| gossip \| pull |
//! | `N`               | 20      | workers |
//! | `HET`             | 0.0     | heterogeneity H = CV of worker speeds (lognormal, mean-normalised so aggregate capacity is constant across H) |
//! | `DRIFT`           | 0.0     | δ̄ — relative speed random-walk step per second (log-space, renormalised each tick to preserve aggregate capacity) |
//! | `LAMBDA_FRAC`     | 0.65    | offered load as a fraction of aggregate capacity |
//! | `MEAN_SERVICE_MS` | 80      | mean job service time at speed 1.0 |
//! | `DURATION_SECS`   | 40      | arrival window (first `RAMP_SECS` excluded from metrics) |
//! | `RAMP_SECS`       | 5       | settling window excluded from all metrics |
//! | `WARMUP_SECS`     | 8       | cluster formation wait before arrivals |
//! | `SEED`            | 1       | drives worker speeds, drift trajectory, arrivals — same seed ⇒ same fleet in every arm |
//! | `DISPATCH_CONCURRENCY` | 64 | dispatcher tasks (push arms); each holds its job RPC until completion |
//! | `PORT_BASE`       | 29400   | loopback port range start |
//!
//! Output: one CSV summary line on stdout (header with `CSV_HEADER=1`).

use bytes::Bytes;
use mycelium::{GossipAgent, GossipConfig, NodeId};
use std::sync::{
    atomic::{AtomicU32, AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};
use tokio::time::sleep;

const LOAD_PREFIX: &str = "wkr/";
const RPC_PICK: &str = "arm3.pick";
const RPC_JOB: &str = "arm3.job";
const RPC_DONE: &str = "arm3.done";
const TUPLE_NS: &str = "work3arm";
const TUPLE_STAGE: &str = "jobs";
const IWWE_SAMPLE_MS: u64 = 25;
const DRIFT_TICK_MS: u64 = 100;
static ERRS: AtomicU64 = AtomicU64::new(0);
static PUT_ERRS: AtomicU64 = AtomicU64::new(0);

// ─────────────────────────────────────────────────────────────────────────────

struct Shared {
    n: usize,
    mean_service_ms: f64,
    epoch: Instant,
    /// Worker speeds as f64 bits; mean-normalised to 1.0 at all times.
    speeds: Vec<AtomicU64>,
    busy: AtomicU32,
    submitted: AtomicU64,
    started: AtomicU64,
    done: AtomicU64,
    busy_ns: Vec<AtomicU64>,
    /// (submit_us, done_us) per completed job.
    completions: Mutex<Vec<(u64, u64)>>,
}

impl Shared {
    fn now_us(&self) -> u64 {
        self.epoch.elapsed().as_micros() as u64
    }
    fn speed(&self, i: usize) -> f64 {
        f64::from_bits(self.speeds[i].load(Ordering::Relaxed))
    }
    /// Execute one job on worker `i`: the only place "work" happens.
    async fn process(&self, i: usize, submit_us: u64) {
        self.started.fetch_add(1, Ordering::Relaxed);
        self.busy.fetch_add(1, Ordering::Relaxed);
        let dur = Duration::from_secs_f64(self.mean_service_ms / self.speed(i) / 1000.0);
        let t0 = Instant::now();
        sleep(dur).await;
        self.busy_ns[i].fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.busy.fetch_sub(1, Ordering::Relaxed);
        self.done.fetch_add(1, Ordering::Relaxed);
        let done_us = self.now_us();
        self.completions.lock().unwrap().push((submit_us, done_us));
    }
}

fn envf(name: &str, default: f64) -> f64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
fn envu(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Approximate standard normal from 12 uniforms (Irwin–Hall).
fn gauss(rng: &mut fastrand::Rng) -> f64 {
    (0..12).map(|_| rng.f64()).sum::<f64>() - 6.0
}

/// Draw mean-normalised lognormal speeds with CV = `het`.
fn draw_speeds(n: usize, het: f64, rng: &mut fastrand::Rng) -> Vec<f64> {
    if het <= 0.0 {
        return vec![1.0; n];
    }
    let sigma = (1.0 + het * het).ln().sqrt();
    let mut s: Vec<f64> = (0..n).map(|_| (sigma * gauss(rng)).exp()).collect();
    let mean = s.iter().sum::<f64>() / n as f64;
    for v in &mut s {
        *v /= mean;
    }
    s
}

// ─────────────────────────────────────────────────────────────────────────────

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let mode = std::env::var("MODE").unwrap_or_else(|_| "pull".to_string());
    let n = envu("N", 20) as usize;
    let het = envf("HET", 0.0);
    let drift = envf("DRIFT", 0.0);
    let lambda_frac = envf("LAMBDA_FRAC", 0.65);
    let mean_service_ms = envf("MEAN_SERVICE_MS", 80.0);
    let duration_secs = envu("DURATION_SECS", 40);
    let ramp_secs = envu("RAMP_SECS", 5);
    let warmup_secs = envu("WARMUP_SECS", 8);
    let seed = envu("SEED", 1);
    let dispatch_conc = envu("DISPATCH_CONCURRENCY", 64) as usize;
    let port_base: u16 = envu("PORT_BASE", 29400) as u16;

    // Aggregate capacity is exactly n / mean_service (speeds are mean-1), so
    // the offered rate is independent of H and δ̄ by construction.
    let lambda_hz = lambda_frac * n as f64 * 1000.0 / mean_service_ms;

    eprintln!(
        "# three_arm mode={mode} n={n} het={het} drift={drift} lambda={lambda_hz:.1}/s \
         mean_service={mean_service_ms}ms duration={duration_secs}s seed={seed}"
    );

    let mut rng = fastrand::Rng::with_seed(seed);
    let speeds0 = draw_speeds(n, het, &mut rng);

    let shared = Arc::new(Shared {
        n,
        mean_service_ms,
        epoch: Instant::now(),
        speeds: speeds0.iter().map(|s| AtomicU64::new(s.to_bits())).collect(),
        busy: AtomicU32::new(0),
        submitted: AtomicU64::new(0),
        started: AtomicU64::new(0),
        done: AtomicU64::new(0),
        busy_ns: (0..n).map(|_| AtomicU64::new(0)).collect(),
        completions: Mutex::new(Vec::with_capacity(16 * 1024)),
    });

    // ── Cluster ──────────────────────────────────────────────────────────────
    let cluster = build_cluster(&mode, n, port_base).await;

    // ── Drift task (deterministic trajectory from SEED, tick-indexed) ───────
    if drift > 0.0 {
        let shared = Arc::clone(&shared);
        let mut log_s: Vec<f64> = speeds0.iter().map(|s| s.ln()).collect();
        let mut drift_rng = fastrand::Rng::with_seed(seed ^ 0xD51F_7A11);
        let step = drift * (DRIFT_TICK_MS as f64 / 1000.0).sqrt();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(DRIFT_TICK_MS));
            loop {
                tick.tick().await;
                for ls in log_s.iter_mut() {
                    // Uniform on ±√3 has unit variance → step has std `step`.
                    let u = (drift_rng.f64() * 2.0 - 1.0) * 3f64.sqrt();
                    *ls = (*ls + u * step).clamp(-3.0, 3.0);
                }
                // Renormalise to mean speed 1.0: drift changes *who* is fast,
                // never how much total capacity exists.
                let mean = log_s.iter().map(|l| l.exp()).sum::<f64>() / shared.n as f64;
                for (i, ls) in log_s.iter().enumerate() {
                    shared.speeds[i].store((ls.exp() / mean).to_bits(), Ordering::Relaxed);
                }
            }
        });
    }

    // ── Per-arm worker plumbing ──────────────────────────────────────────────
    // Push arms: per-worker FIFO + queue-depth advert; RPC_JOB enqueues.
    // Pull arm: each worker loops take() → process → ack.
    let mut worker_nodes: Vec<NodeId> = Vec::with_capacity(n);
    for w in &cluster.workers {
        worker_nodes.push(w.node_id().clone());
    }

    let mut tuple_client = None;
    let mut _ts_primary_guard = None;
    if mode == "pull" {
        use mycelium_tuple_space::{TupleConfig, TupleRole, TupleSpace};
        let mk_cfg = |role: TupleRole| TupleConfig {
            namespace: Arc::from(TUPLE_NS),
            role,
            persist: false,
            high_watermark: 1_000_000, // open-loop arrivals must never be refused
            cap_refresh: Duration::from_secs(2),
            heartbeat_interval: Duration::from_secs(1),
            ..Default::default()
        };
        // The primary lives on a DEDICATED node (same topology slot as the
        // broker arm's broker), so client puts and worker takes both cross a
        // real RPC hop — no colocation shortcut for the pull arm.
        let space = Arc::clone(cluster.space.as_ref().expect("space node"));
        // Do NOT advertise the primary until the space node's outbound routes
        // to every worker exist: take responses are Individual-scoped signals
        // and are silently dropped without a route. Advertising first lets
        // all n workers fire a synchronized first-take volley into the gap,
        // wedging each for the full RPC deadline — observed as a ~7 s global
        // stall poisoning ~20% of runs.
        for _ in 0..100 {
            if space.peers().len() > n {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
        let ts_primary = TupleSpace::new(space, mk_cfg(TupleRole::Primary))
            .await
            .expect("tuple primary");
        for (i, w) in cluster.workers.iter().enumerate() {
            let ts = TupleSpace::new(Arc::clone(w), mk_cfg(TupleRole::Client))
                .await
                .expect("tuple client");
            let shared = Arc::clone(&shared);
            tokio::spawn(async move {
                loop {
                    match ts.take(TUPLE_STAGE, Duration::from_secs(2)).await {
                        Ok((id, payload)) => {
                            let submit_us =
                                u64::from_le_bytes(payload[..8].try_into().unwrap());
                            shared.process(i, submit_us).await;
                            let _ = ts.ack(id).await;
                        }
                        Err(e) => {
                            let c = ERRS.fetch_add(1, Ordering::Relaxed);
                            if c < 5 || c.is_multiple_of(500) {
                                eprintln!("# take err w{i} #{c}: {e:?}");
                            }
                            sleep(Duration::from_millis(20)).await;
                        }
                    }
                }
            });
        }
        _ts_primary_guard = Some(ts_primary);
        tuple_client = Some(
            TupleSpace::new(Arc::clone(&cluster.client), mk_cfg(TupleRole::Client))
                .await
                .expect("tuple client (submitter)"),
        );
    } else {
        // Queue + load advertisement per worker.
        for (i, w) in cluster.workers.iter().enumerate() {
            // The job RPC responds at COMPLETION (like an HTTP LB upstream),
            // so the dispatcher's outstanding count is exact — the strongest
            // practical push baseline (least-outstanding-requests).
            let (tx, mut rx) =
                tokio::sync::mpsc::unbounded_channel::<(u64, mycelium::RpcRequest)>();
            let qlen = Arc::new(AtomicU32::new(0));
            let inflight = Arc::new(AtomicU32::new(0));

            let mut job_rx = w.service().rpc_rx(RPC_JOB);
            let qlen_in = Arc::clone(&qlen);
            tokio::spawn(async move {
                while let Some(req) = job_rx.recv().await {
                    let p = req.payload();
                    let submit_us = u64::from_le_bytes(p[..8].try_into().unwrap());
                    qlen_in.fetch_add(1, Ordering::Relaxed);
                    let _ = tx.send((submit_us, req));
                }
            });

            // Serial executor: respond when the job is done.
            let shared_exec = Arc::clone(&shared);
            let w_exec = Arc::clone(w);
            let qlen_exec = Arc::clone(&qlen);
            let inflight_exec = Arc::clone(&inflight);
            tokio::spawn(async move {
                while let Some((submit_us, req)) = rx.recv().await {
                    qlen_exec.fetch_sub(1, Ordering::Relaxed);
                    inflight_exec.store(1, Ordering::Relaxed);
                    shared_exec.process(i, submit_us).await;
                    inflight_exec.store(0, Ordering::Relaxed);
                    w_exec.service().rpc_respond(&req, Bytes::from_static(b"done"));
                }
            });

            // Load advert at 10 Hz: queue depth + in-flight.
            let w_adv = Arc::clone(w);
            let key = format!("{}{}/load", LOAD_PREFIX, w.node_id());
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_millis(100));
                loop {
                    tick.tick().await;
                    let load = qlen.load(Ordering::Relaxed) + inflight.load(Ordering::Relaxed);
                    let _ = w_adv
                        .kv()
                        .set(key.clone(), Bytes::copy_from_slice(&load.to_le_bytes()));
                }
            });
        }

        // Broker pick handler (broker mode only): least-(advertised+outstanding).
        // The broker sees every dispatch it grants and every completion callback,
        // so its outstanding ledger is exact — the staleness in this arm lives
        // only in the advertised queue/progress component.
        if mode == "broker" {
            let broker = Arc::clone(cluster.broker.as_ref().expect("broker"));
            let outst: Arc<Mutex<std::collections::HashMap<String, i64>>> =
                Arc::new(Mutex::new(std::collections::HashMap::new()));
            let mut pick_rx = broker.service().rpc_rx(RPC_PICK);
            let b2 = Arc::clone(&broker);
            let o2 = Arc::clone(&outst);
            tokio::spawn(async move {
                while let Some(req) = pick_rx.recv().await {
                    let reply = {
                        let mut o = o2.lock().unwrap();
                        match scan_all_loads(&b2)
                            .into_iter()
                            .min_by_key(|(node, load)| {
                                *load as i64 + *o.get(&node.to_string()).unwrap_or(&0)
                            }) {
                            Some((node, _)) => {
                                *o.entry(node.to_string()).or_insert(0) += 1;
                                Bytes::from(node.to_string().into_bytes())
                            }
                            None => Bytes::from_static(b"NONE"),
                        }
                    };
                    b2.service().rpc_respond(&req, reply);
                }
            });
            let mut done_rx = broker.service().rpc_rx(RPC_DONE);
            let b3 = Arc::clone(&broker);
            tokio::spawn(async move {
                while let Some(req) = done_rx.recv().await {
                    if let Ok(node) = std::str::from_utf8(&req.payload()) {
                        let mut o = outst.lock().unwrap();
                        if let Some(v) = o.get_mut(node) {
                            *v = (*v - 1).max(0);
                        }
                    }
                    b3.service().rpc_respond(&req, Bytes::from_static(b"ok"));
                }
            });
        }
    }

    // ── Warmup: formation + (push) load keys visible / (pull) lane reachable ─
    let warm_deadline = Instant::now() + Duration::from_secs(warmup_secs);
    loop {
        sleep(Duration::from_millis(300)).await;
        let ready = if mode == "pull" {
            // End-to-end: a probe item must flow put → take → process → ack
            // before arrivals start. Counter-balanced so waiting math holds;
            // its completion predates ramp_end and is filtered from metrics.
            if shared.done.load(Ordering::Relaxed) == 0
                && shared.submitted.load(Ordering::Relaxed) == 0
            {
                shared.submitted.fetch_add(1, Ordering::Relaxed);
                let probe = Bytes::copy_from_slice(&shared.now_us().to_le_bytes());
                let _ = tuple_client.as_ref().unwrap().put(TUPLE_STAGE, probe).await;
            }
            shared.done.load(Ordering::Relaxed) >= 1
        } else {
            count_load_keys(&cluster.client) == n
                && (mode != "broker"
                    || count_load_keys(cluster.broker.as_ref().unwrap()) == n)
        };
        if ready {
            eprintln!("# warmup ready");
            break;
        }
        if Instant::now() >= warm_deadline {
            eprintln!(
                "# WARMUP TIMEOUT: load_keys client={} broker={} (n={n}) — run is suspect",
                count_load_keys(&cluster.client),
                cluster.broker.as_ref().map(count_load_keys).unwrap_or(0),
            );
            break;
        }
    }
    eprintln!("# warmup done; starting arrivals");
    {
        let shared = Arc::clone(&shared);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(2));
            loop {
                tick.tick().await;
                eprintln!(
                    "# t={}s sub={} start={} done={} take_errs={} put_errs={}",
                    shared.epoch.elapsed().as_secs(),
                    shared.submitted.load(Ordering::Relaxed),
                    shared.started.load(Ordering::Relaxed),
                    shared.done.load(Ordering::Relaxed),
                    ERRS.load(Ordering::Relaxed),
                    PUT_ERRS.load(Ordering::Relaxed),
                );
            }
        });
    }

    // ── IWWE sampler ─────────────────────────────────────────────────────────
    let iwwe_ns = Arc::new(AtomicU64::new(0));
    {
        let shared = Arc::clone(&shared);
        let iwwe = Arc::clone(&iwwe_ns);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(IWWE_SAMPLE_MS));
            loop {
                tick.tick().await;
                let waiting = shared
                    .submitted
                    .load(Ordering::Relaxed)
                    .saturating_sub(shared.started.load(Ordering::Relaxed));
                if waiting > 0 {
                    let idle = shared.n as u64 - shared.busy.load(Ordering::Relaxed) as u64;
                    iwwe.fetch_add(idle * IWWE_SAMPLE_MS * 1_000_000, Ordering::Relaxed);
                }
            }
        });
    }

    // ── Open-loop Poisson arrivals + dispatch ────────────────────────────────
    let arrivals_start = Instant::now();
    let ramp_end_us = shared.now_us() + ramp_secs * 1_000_000;
    // Zero per-worker busy time at the ramp boundary so utilisation/Jain and
    // IWWE cover the same window (small smear from in-flight jobs accepted).
    {
        let shared = Arc::clone(&shared);
        let iwwe = Arc::clone(&iwwe_ns);
        tokio::spawn(async move {
            sleep(Duration::from_secs(ramp_secs)).await;
            for b in &shared.busy_ns {
                b.store(0, Ordering::Relaxed);
            }
            iwwe.store(0, Ordering::Relaxed);
        });
    }

    let deadline = arrivals_start + Duration::from_secs(duration_secs);
    let mut arr_rng = fastrand::Rng::with_seed(seed ^ 0xA221_0BEE);

    // Push arms: K dispatcher tasks drain an unbounded arrival queue, so the
    // arrival process NEVER blocks on dispatch capacity (open-loop). Backlog
    // forms in this queue when dispatch (e.g. a serialised broker) can't keep
    // up — that queueing is the arm's cost, measured in job latency.
    let dispatch_tx = if mode != "pull" {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
        let rx = Arc::new(tokio::sync::Mutex::new(rx));
        let outst: Arc<Mutex<std::collections::HashMap<String, i64>>> =
            Arc::new(Mutex::new(std::collections::HashMap::new()));
        for _ in 0..dispatch_conc {
            let rx = Arc::clone(&rx);
            let client = Arc::clone(&cluster.client);
            let broker_node = cluster.broker_node.clone();
            let mode = mode.clone();
            let outst = Arc::clone(&outst);
            tokio::spawn(async move {
                loop {
                    let Some(payload) = rx.lock().await.recv().await else { break };
                    loop {
                        let pick: Option<NodeId> = match mode.as_str() {
                            // Least-(advertised + locally-outstanding): the client
                            // sees all its own dispatches, so only the advertised
                            // component (worker progress/speed) can be stale.
                            "gossip" => {
                                let mut o = outst.lock().unwrap();
                                let pick = scan_all_loads(&client)
                                    .into_iter()
                                    .min_by_key(|(node, load)| {
                                        *load as i64
                                            + *o.get(&node.to_string()).unwrap_or(&0)
                                    })
                                    .map(|(node, _)| node);
                                if let Some(n) = &pick {
                                    *o.entry(n.to_string()).or_insert(0) += 1;
                                }
                                pick
                            }
                            _ => {
                                let b = broker_node.clone().expect("broker node");
                                match client
                                    .service()
                                    .rpc_call(b, RPC_PICK, Bytes::new(), Duration::from_millis(500))
                                    .await
                                {
                                    Ok(r) if &r[..] != b"NONE" => parse_node(&r),
                                    _ => None,
                                }
                            }
                        };
                        let Some(node) = pick else {
                            sleep(Duration::from_millis(5)).await;
                            continue;
                        };
                        // Held until the job completes on the worker.
                        let res = client
                            .service()
                            .rpc_call(node.clone(), RPC_JOB, payload.clone(), Duration::from_secs(120))
                            .await;
                        match mode.as_str() {
                            "gossip" => {
                                let mut o = outst.lock().unwrap();
                                if let Some(v) = o.get_mut(&node.to_string()) {
                                    *v = (*v - 1).max(0);
                                }
                            }
                            _ => {
                                let b = broker_node.clone().expect("broker node");
                                let _ = client
                                    .service()
                                    .rpc_call(
                                        b,
                                        RPC_DONE,
                                        Bytes::from(node.to_string().into_bytes()),
                                        Duration::from_millis(500),
                                    )
                                    .await;
                            }
                        }
                        match res {
                            Ok(_) => break,
                            Err(_) => sleep(Duration::from_millis(5)).await,
                        }
                    }
                }
            });
        }
        Some(tx)
    } else {
        None
    };

    // Absolute-schedule pacing: the arrival process is a fixed timetable from
    // the seed, so offered load is IDENTICAL across arms regardless of how
    // much the arm's own machinery loads the runtime (relative sleeps let
    // scheduler contention slow the arrival clock — observed ~6% in pilot).
    let mut next_arrival = tokio::time::Instant::now();
    while Instant::now() < deadline {
        let u: f64 = arr_rng.f64().max(1e-12);
        let dt = -u.ln() / lambda_hz;
        next_arrival += Duration::from_secs_f64(dt);
        tokio::time::sleep_until(next_arrival).await;

        let submit_us = shared.now_us();
        shared.submitted.fetch_add(1, Ordering::Relaxed);
        let payload = Bytes::copy_from_slice(&submit_us.to_le_bytes());

        match mode.as_str() {
            "pull" => {
                let ts = tuple_client.as_ref().unwrap();
                // high_watermark is effectively unbounded; retry any transient.
                let mut tries = 0;
                loop {
                    match ts.put(TUPLE_STAGE, payload.clone()).await {
                        Ok(_) => break,
                        Err(e) => {
                            let c = PUT_ERRS.fetch_add(1, Ordering::Relaxed);
                            if c < 5 || c.is_multiple_of(200) {
                                eprintln!("# put err #{c}: {e:?}");
                            }
                            tries += 1;
                            if tries >= 50 { break; }
                            sleep(Duration::from_millis(2)).await;
                        }
                    }
                }
            }
            _ => {
                let _ = dispatch_tx.as_ref().unwrap().send(payload);
            }
        }
    }
    let meas_end_us = shared.now_us();
    let iwwe_at_end = iwwe_ns.load(Ordering::Relaxed);
    eprintln!("# arrivals done; draining");

    // Drain grace so late in-flight work finishes (excluded from metrics anyway).
    sleep(Duration::from_secs_f64((mean_service_ms / 1000.0 * 20.0).min(8.0))).await;

    // ── Metrics over [ramp_end, meas_end] ────────────────────────────────────
    let window_us = meas_end_us - ramp_end_us;
    let completions = shared.completions.lock().unwrap().clone();
    let mut lat_ms: Vec<f64> = completions
        .iter()
        .filter(|(s, d)| *s >= ramp_end_us && *d <= meas_end_us)
        .map(|(s, d)| (d - s) as f64 / 1000.0)
        .collect();
    lat_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let completed = lat_ms.len();
    let thr_hz = completed as f64 / (window_us as f64 / 1e6);
    let mean_ms = if completed > 0 {
        lat_ms.iter().sum::<f64>() / completed as f64
    } else {
        f64::NAN
    };
    let pct = |p: f64| -> f64 {
        if lat_ms.is_empty() {
            f64::NAN
        } else {
            lat_ms[((lat_ms.len() - 1) as f64 * p).round() as usize]
        }
    };

    let iwwe_frac = iwwe_at_end as f64 / (n as f64 * window_us as f64 * 1000.0);

    let utils: Vec<f64> = shared
        .busy_ns
        .iter()
        .map(|b| b.load(Ordering::Relaxed) as f64 / (window_us as f64 * 1000.0))
        .collect();
    let sum: f64 = utils.iter().sum();
    let sumsq: f64 = utils.iter().map(|u| u * u).sum();
    let jain = if sumsq > 0.0 {
        (sum * sum) / (n as f64 * sumsq)
    } else {
        f64::NAN
    };

    if envu("CSV_HEADER", 0) == 1 {
        println!(
            "arm,n,het,drift,seed,lambda_hz,submitted,completed,thr_hz,\
             mean_ms,p50_ms,p95_ms,p99_ms,iwwe,jain"
        );
    }
    println!(
        "{},{},{},{},{},{:.2},{},{},{:.2},{:.2},{:.2},{:.2},{:.2},{:.5},{:.4}",
        mode,
        n,
        het,
        drift,
        seed,
        lambda_hz,
        shared.submitted.load(Ordering::Relaxed),
        completed,
        thr_hz,
        mean_ms,
        pct(0.50),
        pct(0.95),
        pct(0.99),
        iwwe_frac,
        jain
    );

    // Shutdown.
    for w in cluster.workers {
        w.shutdown().await;
    }
    if let Some(b) = cluster.broker {
        b.shutdown().await;
    }
    if let Some(s) = cluster.space {
        s.shutdown().await;
    }
    cluster.client.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────────────

struct Cluster {
    workers: Vec<Arc<GossipAgent>>,
    client: Arc<GossipAgent>,
    broker: Option<Arc<GossipAgent>>,
    broker_node: Option<NodeId>,
    /// Pull arm: dedicated host for the tuple-space primary (the
    /// topology-symmetric counterpart of the broker node).
    space: Option<Arc<GossipAgent>>,
}

async fn build_cluster(mode: &str, n: usize, port_base: u16) -> Cluster {
    // RPC requests and responses are Individual-scoped signals, and those are
    // delivered only to nodes in the SENDER's outbound peer list (no flood
    // fallback — see ForwardHint::Individual in agent/tasks.rs). Every pair
    // that exchanges RPCs therefore gets BOTH directions bootstrapped
    // explicitly; first dials to not-yet-started nodes retry on a 1 s
    // backoff, which warmup absorbs.
    let worker_ids: Vec<NodeId> = (0..n)
        .map(|i| NodeId::new("127.0.0.1", port_base + i as u16).expect("worker port"))
        .collect();
    let client_id = NodeId::new("127.0.0.1", port_base + n as u16).expect("client port");
    // One aux slot: the broker (push-central) or the tuple-space host (pull).
    let aux_id = NodeId::new("127.0.0.1", port_base + n as u16 + 1).expect("aux port");
    let broker_id = aux_id.clone();

    let mk = |port: u16, boots: Vec<NodeId>| {
        let mut cfg = GossipConfig::default();
        cfg.cluster_name = Some(std::env::var("GOSSIP_CLUSTER_NAME").unwrap_or_else(|_| "workdist".to_string()));
        cfg.bind_port = port;
        cfg.bootstrap_peers = boots;
        cfg.health_check_max_jitter_ms = 50;
        cfg.reconnect_backoff_secs = 1;
        cfg
    };

    // Workers first (their dial to client/broker retries until those start).
    let needs_aux = mode == "broker" || mode == "pull";

    let mut workers = Vec::with_capacity(n);
    for (i, wid) in worker_ids.iter().enumerate() {
        let mut boots = vec![client_id.clone()];
        if needs_aux {
            boots.push(aux_id.clone());
        }
        let agent = Arc::new(GossipAgent::new(wid.clone(), mk(port_base + i as u16, boots)));
        agent.start().await.expect("worker start");
        workers.push(agent);
    }

    let mut client_boots = worker_ids.clone();
    if needs_aux {
        client_boots.push(aux_id.clone());
    }
    let client = Arc::new(GossipAgent::new(
        client_id.clone(),
        mk(port_base + n as u16, client_boots),
    ));
    client.start().await.expect("client start");

    let aux = if needs_aux {
        // Aux dials client AND every worker: its RPC responses (broker picks,
        // tuple take deliveries) are Individual-scoped and need outbound routes.
        let mut boots = vec![client_id.clone()];
        boots.extend(worker_ids.iter().cloned());
        let agent = Arc::new(GossipAgent::new(
            aux_id.clone(),
            mk(port_base + n as u16 + 1, boots),
        ));
        agent.start().await.expect("aux start");
        Some(agent)
    } else {
        None
    };

    let (broker, broker_node, space) = match mode {
        "broker" => (aux.clone(), Some(broker_id), None),
        "pull" => (None, None, aux),
        _ => (None, None, None),
    };

    Cluster { workers, client, broker, broker_node, space }
}

/// All advertised (node, load) pairs in this agent's local KV view.
fn scan_all_loads(agent: &Arc<GossipAgent>) -> Vec<(NodeId, u32)> {
    agent
        .kv()
        .scan_prefix(LOAD_PREFIX)
        .into_iter()
        .filter_map(|(key, bytes)| {
            let s = key.as_ref();
            if !s.ends_with("/load") || bytes.len() != 4 {
                return None;
            }
            let middle = &s[LOAD_PREFIX.len()..s.len() - "/load".len()];
            let (host, port_str) = middle.rsplit_once(':')?;
            let node = NodeId::new(host, port_str.parse().ok()?).ok()?;
            Some((node, u32::from_le_bytes(bytes[..].try_into().unwrap())))
        })
        .collect()
}

fn count_load_keys(agent: &Arc<GossipAgent>) -> usize {
    agent
        .kv()
        .scan_prefix(LOAD_PREFIX)
        .iter()
        .filter(|(k, _)| k.ends_with("/load"))
        .count()
}

fn parse_node(reply: &[u8]) -> Option<NodeId> {
    let s = std::str::from_utf8(reply).ok()?;
    let (host, port_str) = s.rsplit_once(':')?;
    NodeId::new(host, port_str.parse().ok()?).ok()
}
