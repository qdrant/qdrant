//! Single-threaded SQ8 plain scans through the production batch searcher.
//!
//! See docs/benchmarks/plain-scan.md for the workload and measurement contract.

use std::hint::black_box;
use std::path::PathBuf;
use std::sync::{Barrier, Mutex, mpsc};
use std::time::{Duration, Instant};

use clap::Parser;
use common::ambient;
use common::bitvec::BitVec;
use common::condition_checker::{CheckItem, ConditionChecker, Rest, Select, default_check_batched};
use common::types::{PointOffsetType, ScoredPointOffset};
use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};
use segment::common::operation_error::OperationError;
use segment::data_types::vectors::QueryVector;
use segment::index::ConditionCheckerEnum;
use segment::index::hnsw_index::point_scorer::BatchFilteredSearcher;
use segment::index::query_optimization::optimized_filter::OptimizedFilter;
use segment::types::{Distance, QuantizationConfig, ScalarQuantizationConfig, ScalarType};
use segment::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
use segment::vector_storage::quantized::quantized_vectors::{
    QuantizedVectors, QuantizedVectorsStorageType,
};
use segment::vector_storage::{DEFAULT_STOPPED, VectorStorage, VectorStorageEnum};
use serde::Serialize;
use tempfile::TempDir;

#[derive(Parser, Serialize)]
struct Args {
    #[arg(long, default_value_t = 1_000_000, value_parser = clap::value_parser!(u32).range(1..))]
    points: u32,
    #[arg(long, default_value_t = 512)]
    dim: usize,
    #[arg(long, value_delimiter = ',', default_value = "1,8,64")]
    batches: Vec<usize>,
    #[arg(long, default_value_t = 10)]
    top: usize,
    /// Percentage of points accepted by the synthetic filter.
    #[arg(long, default_value_t = 90, value_parser = clap::value_parser!(u32).range(1..=100))]
    filter_percent: u32,
    #[arg(long, default_value_t = 10)]
    samples: usize,
    #[arg(long, default_value_t = 2)]
    warmup: usize,
    #[arg(long, default_value_t = 42)]
    seed: u64,
    /// Write all samples, including unavailable cycle counts, as JSON.
    #[arg(long)]
    output: Option<PathBuf>,
    /// Fail if hardware CPU cycle counting is unavailable.
    #[arg(long)]
    require_cycles: bool,
    /// Run a closed-loop in-process request test at these concurrency levels.
    #[arg(long, value_delimiter = ',')]
    concurrency: Vec<usize>,
    /// Fixed number of search workers in concurrency mode; CPU affinity is external.
    #[arg(long, default_value_t = 3)]
    search_threads: usize,
    /// Keep replacing completed requests for this duration, then drain all in-flight work.
    #[arg(long, default_value_t = 10.0)]
    duration_secs: f64,
    /// Query pool in concurrency mode; increased if a requested batch is larger.
    #[arg(long, default_value_t = 128)]
    query_pool: usize,
    /// Memory budget for retained results and latency records; fail rather than truncate.
    #[arg(long, default_value_t = 64)]
    result_budget_mib: usize,
    // Passed automatically by cargo bench to harness=false executables.
    #[arg(long, hide = true)]
    bench: bool,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum Scan {
    Visible,
    Filtered,
}

struct SelectedPoints(u32);

impl ConditionChecker for SelectedPoints {
    type Error = OperationError;

    fn check(&self, id: PointOffsetType) -> Result<bool, Self::Error> {
        Ok(id % 100 < self.0)
    }

    fn check_batched<K: CheckItem>(
        &self,
        ids: &mut [K],
        select: Select,
        rest: Rest,
    ) -> Result<usize, Self::Error> {
        default_check_batched(ids, select, rest, |id| self.check(id))
    }
}

struct Dataset {
    dense: VectorStorageEnum,
    quantized: QuantizedVectors,
    queries: Vec<QueryVector>,
    deleted: BitVec,
    _dir: TempDir,
}

impl Dataset {
    fn new(args: &Args) -> Self {
        let mut rng = SmallRng::seed_from_u64(args.seed);
        let mut vector = || {
            let mut v = (0..args.dim)
                .map(|_| rng.random_range(-1.0f32..1.0))
                .collect::<Vec<_>>();
            let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            for x in &mut v {
                *x /= norm;
            }
            v
        };
        let mut dense = new_volatile_dense_vector_storage(args.dim, Distance::Dot);
        for id in 0..args.points {
            dense.insert_vector(id, vector().as_slice().into()).unwrap();
        }
        let max_batch = *args.batches.iter().max().unwrap();
        let query_count = if args.concurrency.is_empty() {
            max_batch
        } else {
            max_batch.max(args.query_pool)
        };
        let queries = (0..query_count)
            .map(|_| QueryVector::from(vector()))
            .collect();
        let dir = TempDir::new().unwrap();
        #[expect(
            deprecated,
            reason = "ScalarQuantizationConfig still carries always_ram"
        )]
        let config = QuantizationConfig::from(ScalarQuantizationConfig {
            r#type: ScalarType::Int8,
            quantile: None,
            always_ram: Some(true),
            memory: None,
        });
        let quantized = QuantizedVectors::create(
            &dense,
            &config,
            QuantizedVectorsStorageType::Immutable,
            dir.path(),
            1,
            &DEFAULT_STOPPED,
        )
        .unwrap();
        Self {
            dense,
            quantized,
            queries,
            deleted: BitVec::repeat(false, args.points as usize),
            _dir: dir,
        }
    }

    fn searcher<'a>(
        &'a self,
        args: &Args,
        scan: Scan,
        queries: &[&QueryVector],
    ) -> BatchFilteredSearcher<'a> {
        let filter = match scan {
            Scan::Visible => None,
            Scan::Filtered => Some(OptimizedFilter::from_checker(ConditionCheckerEnum::Dyn(
                Box::new(SelectedPoints(args.filter_percent)),
            ))),
        };
        BatchFilteredSearcher::new(
            queries,
            &self.dense,
            Some(&self.quantized),
            filter,
            args.top,
            &self.deleted,
        )
        .unwrap()
    }
}

fn run(
    searcher: BatchFilteredSearcher<'_>,
    scan: Scan,
    points: u32,
) -> Vec<Vec<ScoredPointOffset>> {
    match scan {
        Scan::Visible => {
            let empty = BitVec::new();
            searcher.peek_top_visible(None, &empty, &empty, &DEFAULT_STOPPED)
        }
        Scan::Filtered => searcher.peek_top_iter(0..points, &DEFAULT_STOPPED),
    }
    .unwrap()
}

#[derive(Serialize)]
struct Sample {
    scan: Scan,
    batch: usize,
    sample: usize,
    scored_points_per_query: u32,
    query_prepare_ns: u128,
    elapsed_ns: u128,
    queries_per_second: f64,
    ns_per_query_point: f64,
    cpu_cycles: Option<u64>,
    cycles_per_query_point: Option<f64>,
}

struct Request {
    id: usize,
    client: usize,
    submitted: Instant,
}

struct CompletedRequest {
    request: Request,
    started: Instant,
    finished: Instant,
    result: Vec<Vec<ScoredPointOffset>>,
}

#[derive(Serialize)]
struct RequestLatency {
    request_id: usize,
    client: usize,
    query_offset: usize,
    submitted_ns: u128,
    started_ns: u128,
    finished_ns: u128,
    received_ns: u128,
    queue_ns: u128,
    service_ns: u128,
    end_to_end_ns: u128,
}

#[derive(Serialize)]
struct ConcurrencySample {
    scan: Scan,
    batch: usize,
    concurrency: usize,
    search_threads: usize,
    sample: usize,
    submission_window_ns: u128,
    elapsed_including_drain_ns: u128,
    drain_ns: u128,
    successful_requests: usize,
    successful_logical_queries: usize,
    requests_completed_in_window: usize,
    requests_per_second_in_window: f64,
    requests_per_second: f64,
    logical_queries_per_second: f64,
    latency_p50_ns: u128,
    latency_p95_ns: u128,
    latency_p99_ns: u128,
    result_budget_bytes: usize,
    estimated_bytes_per_request: usize,
    /// Every measured result was checked bit-for-bit against independent B1 results.
    all_results_match_reference: bool,
    requests: Vec<RequestLatency>,
}

fn assert_results(
    actual: &[Vec<ScoredPointOffset>],
    reference: &[Vec<ScoredPointOffset>],
    offset: usize,
    batch: usize,
) {
    assert_eq!(actual.len(), batch);
    for (query, actual) in actual.iter().enumerate() {
        let expected = &reference[(offset + query) % reference.len()];
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(expected) {
            assert_eq!(
                (actual.idx, actual.score.to_bits()),
                (expected.idx, expected.score.to_bits()),
            );
        }
    }
}

fn concurrency_sample(
    args: &Args,
    dataset: &Dataset,
    reference: &[Vec<ScoredPointOffset>],
    scan: Scan,
    batch: usize,
    concurrency: usize,
    sample: usize,
) -> ConcurrencySample {
    let window = Duration::from_secs_f64(args.duration_secs);
    let result_capacity = reference.iter().map(Vec::capacity).max().unwrap();
    // Account for vector capacities, record growth and the post-run latency copy.
    // Reference searches use the same queues, limits and point order as each request.
    let bytes_per_request = 2 * size_of::<(CompletedRequest, Instant)>()
        + size_of::<RequestLatency>()
        + size_of::<u128>()
        + batch
            * (2 * size_of::<Vec<ScoredPointOffset>>()
                + result_capacity * size_of::<ScoredPointOffset>());
    let result_budget = args.result_budget_mib.checked_mul(1024 * 1024).unwrap();
    let max_requests = result_budget / bytes_per_request;
    assert!(
        max_requests >= concurrency,
        "--result-budget-mib must hold at least C complete request results"
    );
    let (requests_tx, requests_rx) = mpsc::sync_channel::<Request>(concurrency);
    let requests_rx = Mutex::new(requests_rx);
    let (completed_tx, completed_rx) =
        mpsc::sync_channel::<Result<CompletedRequest, Box<dyn std::any::Any + Send>>>(concurrency);
    let ready = Barrier::new(args.search_threads + 1);

    let (start, elapsed, completed, budget_exceeded) = std::thread::scope(|scope| {
        for worker in 0..args.search_threads {
            let completed_tx = completed_tx.clone();
            let requests_rx = &requests_rx;
            let ready = &ready;
            scope.spawn(move || {
                let _scope = ambient::test_guard();
                // Warm the actual worker threads before timing, including per-thread
                // feature setup. Workers never combine separate requests into a batch.
                let warm_offset = worker % dataset.queries.len();
                let warm_queries: Vec<_> = dataset
                    .queries
                    .iter()
                    .cycle()
                    .skip(warm_offset)
                    .take(batch)
                    .collect();
                let warmup = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    for _ in 0..args.warmup {
                        let result = run(
                            dataset.searcher(args, scan, &warm_queries),
                            scan,
                            args.points,
                        );
                        assert_results(&result, reference, warm_offset, batch);
                    }
                }));
                // A failed warmup must not leave the other threads stuck at the barrier.
                ready.wait();
                if let Err(error) = warmup {
                    let _ = completed_tx.send(Err(error));
                    return;
                }
                loop {
                    // Hold the receiver lock only while dequeuing, never while searching.
                    let request = requests_rx.lock().unwrap().recv();
                    let Ok(request) = request else { break };
                    let response = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let started = Instant::now();
                        let offset = request.id % dataset.queries.len();
                        let queries: Vec<_> = dataset
                            .queries
                            .iter()
                            .cycle()
                            .skip(offset)
                            .take(batch)
                            .collect();
                        // Request service includes query preparation and result construction.
                        let result = run(dataset.searcher(args, scan, &queries), scan, args.points);
                        let finished = Instant::now();
                        CompletedRequest {
                            request,
                            started,
                            finished,
                            result,
                        }
                    }));
                    let failed = response.is_err();
                    if completed_tx.send(response).is_err() || failed {
                        break;
                    }
                }
            });
        }
        drop(completed_tx);
        ready.wait();
        let start = Instant::now();
        let deadline = start + window;
        let mut next_id = 0;
        for client in 0..concurrency {
            requests_tx
                .send(Request {
                    id: next_id,
                    client,
                    // All initial clients arrive at the start of the test; dispatch
                    // time is included in their queue latency.
                    submitted: start,
                })
                .unwrap();
            next_id += 1;
        }
        let mut in_flight = concurrency;
        let mut completed = Vec::new();
        let mut budget_exceeded = false;
        while in_flight != 0 {
            let response = match completed_rx.recv().unwrap() {
                Ok(response) => response,
                Err(error) => std::panic::resume_unwind(error),
            };
            let received = Instant::now();
            in_flight -= 1;
            let submitted = Instant::now();
            if submitted < deadline && next_id < max_requests {
                requests_tx
                    .send(Request {
                        id: next_id,
                        client: response.request.client,
                        submitted,
                    })
                    .unwrap();
                next_id += 1;
                in_flight += 1;
            } else if submitted < deadline {
                budget_exceeded = true;
            }
            // Retain results for validation after the timed interval. Only capture
            // timestamps here; no sorting, percentiles or correctness checks in flight.
            completed.push((response, received));
        }
        let elapsed = start.elapsed();
        drop(requests_tx);
        // Own both channel ends in this scope, so any failure also disconnects
        // workers before scoped-thread joins wait for them.
        drop(completed_rx);
        (start, elapsed, completed, budget_exceeded)
    });
    assert!(
        !budget_exceeded,
        "result retention budget exceeded: shorten --duration-secs or increase --result-budget-mib"
    );

    let mut requests = Vec::with_capacity(completed.len());
    for (response, received) in completed {
        let CompletedRequest {
            request,
            started,
            finished,
            result,
        } = response;
        let offset = request.id % dataset.queries.len();
        assert_results(&result, reference, offset, batch);
        requests.push(RequestLatency {
            request_id: request.id,
            client: request.client,
            query_offset: offset,
            submitted_ns: request.submitted.duration_since(start).as_nanos(),
            started_ns: started.duration_since(start).as_nanos(),
            finished_ns: finished.duration_since(start).as_nanos(),
            received_ns: received.duration_since(start).as_nanos(),
            queue_ns: started.duration_since(request.submitted).as_nanos(),
            service_ns: finished.duration_since(started).as_nanos(),
            end_to_end_ns: received.duration_since(request.submitted).as_nanos(),
        });
    }
    let mut latencies: Vec<_> = requests
        .iter()
        .map(|request| request.end_to_end_ns)
        .collect();
    latencies.sort_unstable();
    // Nearest-rank percentiles: an observed latency, including the queue and delivery.
    let percentile = |percent: usize| latencies[(latencies.len() * percent).div_ceil(100) - 1];
    let requests_completed_in_window = requests
        .iter()
        .filter(|request| request.received_ns < window.as_nanos())
        .count();
    let successful_requests = requests.len();
    let successful_logical_queries = successful_requests * batch;
    ConcurrencySample {
        scan,
        batch,
        concurrency,
        search_threads: args.search_threads,
        sample,
        submission_window_ns: window.as_nanos(),
        elapsed_including_drain_ns: elapsed.as_nanos(),
        drain_ns: elapsed.saturating_sub(window).as_nanos(),
        successful_requests,
        successful_logical_queries,
        requests_completed_in_window,
        requests_per_second_in_window: requests_completed_in_window as f64 / window.as_secs_f64(),
        requests_per_second: successful_requests as f64 / elapsed.as_secs_f64(),
        logical_queries_per_second: successful_logical_queries as f64 / elapsed.as_secs_f64(),
        latency_p50_ns: percentile(50),
        latency_p95_ns: percentile(95),
        latency_p99_ns: percentile(99),
        result_budget_bytes: result_budget,
        estimated_bytes_per_request: bytes_per_request,
        all_results_match_reference: true,
        requests,
    }
}

#[cfg(target_os = "linux")]
fn cycle_counter(required: bool) -> Option<perf_event::Counter> {
    let mut builder = perf_event::Builder::new().kind(perf_event::events::Hardware::CPU_CYCLES);
    builder.exclude_kernel(true).exclude_hv(true).pinned(true);
    match builder.build() {
        Ok(counter) => Some(counter),
        Err(err) => {
            assert!(!required, "hardware CPU cycles unavailable: {err}");
            eprintln!(
                "Hardware CPU cycles unavailable ({err}); recording null, not estimated cycles."
            );
            None
        }
    }
}

fn main() {
    let args = Args::parse();
    assert!(args.dim > 0 && args.top > 0 && args.samples > 0);
    assert!(!args.batches.is_empty() && args.batches.iter().all(|&batch| batch > 0));
    assert!(args.concurrency.iter().all(|&concurrency| concurrency > 0));
    assert!(args.search_threads > 0 && args.query_pool > 0 && args.result_budget_mib > 0);
    assert!(args.duration_secs.is_finite() && args.duration_secs > 0.0);
    assert!(
        args.concurrency.is_empty() || !args.require_cycles,
        "--require-cycles is unavailable in concurrency mode: the main-thread counter does not count search workers"
    );
    #[cfg(target_os = "linux")]
    let mut cycles = if args.concurrency.is_empty() {
        cycle_counter(args.require_cycles)
    } else {
        None
    };
    #[cfg(not(target_os = "linux"))]
    assert!(!args.require_cycles, "hardware CPU cycles require Linux");

    let _scope = ambient::test_guard();
    eprintln!(
        "Preparing {} SQ8 vectors of dimension {}",
        args.points, args.dim
    );
    let dataset = Dataset::new(&args);
    let queries = dataset.queries.iter().collect::<Vec<_>>();
    let mut cases = Vec::new();
    let mut reference_results = Vec::new();
    let mut reference_scores = Vec::new();
    for scan in [Scan::Visible, Scan::Filtered] {
        // B1 remains the reference when later changes optimize multi-query scoring.
        // All correctness checks, allocation, and query encoding are outside timing.
        let reference = queries
            .iter()
            .map(|query| {
                run(dataset.searcher(&args, scan, &[*query]), scan, args.points)
                    .pop()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        // Preserve score bits so reports from different builds can be compared exactly.
        reference_results.push(serde_json::json!({
            "scan": scan,
            "queries": reference.iter().map(|points| points.iter().map(|point| {
                (point.idx, point.score.to_bits())
            }).collect::<Vec<_>>()).collect::<Vec<_>>()
        }));
        for &batch in &args.batches {
            let actual = run(
                dataset.searcher(&args, scan, &queries[..batch]),
                scan,
                args.points,
            );
            for (actual, reference) in actual.iter().zip(&reference) {
                assert_eq!(actual.len(), reference.len());
                for (a, b) in actual.iter().zip(reference) {
                    assert_eq!((a.idx, a.score.to_bits()), (b.idx, b.score.to_bits()));
                }
            }
            if args.concurrency.is_empty() {
                for _ in 0..args.warmup {
                    black_box(run(
                        dataset.searcher(&args, scan, &queries[..batch]),
                        scan,
                        args.points,
                    ));
                }
            }
            cases.push((scan, batch));
        }
        reference_scores.push(reference);
    }

    if !args.concurrency.is_empty() {
        let mut concurrent_cases: Vec<_> = cases
            .iter()
            .flat_map(|&(scan, batch)| {
                args.concurrency
                    .iter()
                    .map(move |&concurrency| (scan, batch, concurrency))
            })
            .collect();
        let mut samples = Vec::new();
        for sample in 0..args.samples {
            for &(scan, batch, concurrency) in &concurrent_cases {
                let reference = &reference_scores[match scan {
                    Scan::Visible => 0,
                    Scan::Filtered => 1,
                }];
                let result = concurrency_sample(
                    &args,
                    &dataset,
                    reference,
                    scan,
                    batch,
                    concurrency,
                    sample,
                );
                eprintln!(
                    "{scan:?} B{batch} C{concurrency} sample {sample}: {:.2} request/s, {:.2} logical query/s, p50/p95/p99 {:.2}/{:.2}/{:.2} ms, {:.2}s including drain",
                    result.requests_per_second,
                    result.logical_queries_per_second,
                    result.latency_p50_ns as f64 / 1e6,
                    result.latency_p95_ns as f64 / 1e6,
                    result.latency_p99_ns as f64 / 1e6,
                    result.elapsed_including_drain_ns as f64 / 1e9
                );
                samples.push(result);
            }
            concurrent_cases.reverse();
        }
        write_report(
            &args,
            serde_json::json!({
                "mode": "in_process_closed_loop",
                "config": args,
                "b1_reference_results": reference_results,
                "samples": samples,
            }),
        );
        return;
    }

    let mut samples = Vec::new();
    for sample in 0..args.samples {
        // Alternate case order to spread elapsed-time drift across batch sizes.
        for &(scan, batch) in &cases {
            let prepare_start = Instant::now();
            let searcher = dataset.searcher(&args, scan, &queries[..batch]);
            let query_prepare_ns = prepare_start.elapsed().as_nanos();
            #[cfg(target_os = "linux")]
            if let Some(counter) = &mut cycles {
                counter.reset().unwrap();
                counter.enable().unwrap();
            }
            let start = Instant::now();
            let result = run(searcher, scan, args.points);
            let elapsed = start.elapsed();
            #[cfg(target_os = "linux")]
            let cpu_cycles = cycles.as_mut().map(|counter| {
                counter.disable().unwrap();
                let count = counter.read_count_and_time().unwrap();
                assert!(count.time_running > 0, "CPU cycle counter did not run");
                assert_eq!(
                    count.time_running, count.time_enabled,
                    "CPU cycle counter was multiplexed"
                );
                count.count
            });
            #[cfg(not(target_os = "linux"))]
            let cpu_cycles = None;
            black_box(result);
            let scored = match scan {
                Scan::Visible => args.points,
                Scan::Filtered => {
                    args.points / 100 * args.filter_percent
                        + (args.points % 100).min(args.filter_percent)
                }
            };
            let pairs = f64::from(scored) * batch as f64;
            samples.push(Sample {
                scan,
                batch,
                sample,
                scored_points_per_query: scored,
                query_prepare_ns,
                elapsed_ns: elapsed.as_nanos(),
                queries_per_second: batch as f64 / elapsed.as_secs_f64(),
                ns_per_query_point: elapsed.as_secs_f64() * 1e9 / pairs,
                cpu_cycles,
                cycles_per_query_point: cpu_cycles.map(|cycles| cycles as f64 / pairs),
            });
            let row = samples.last().unwrap();
            eprintln!(
                "{scan:?} B{batch} sample {sample}: {:.2} query/s, {:.3} ns/query-point, cycles/query-point {:?}",
                row.queries_per_second, row.ns_per_query_point, row.cycles_per_query_point
            );
        }
        cases.reverse();
    }
    let report = serde_json::json!({
        "config": args,
        "b1_reference_results": reference_results,
        "samples": samples,
    });
    write_report(&args, report);
}

fn write_report(args: &Args, report: serde_json::Value) {
    let json = serde_json::to_string_pretty(&report).unwrap();
    if let Some(output) = &args.output {
        std::fs::write(output, &json).unwrap();
    } else {
        println!("{json}");
    }
}
