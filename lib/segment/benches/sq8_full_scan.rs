//! Single-threaded SQ8 plain scans through the production batch searcher.
//!
//! See docs/benchmarks/plain-scan.md for the workload and measurement contract.

use std::hint::black_box;
use std::path::PathBuf;
use std::time::Instant;

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
        let queries = (0..*args.batches.iter().max().unwrap())
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
    #[cfg(target_os = "linux")]
    let mut cycles = cycle_counter(args.require_cycles);
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
            for _ in 0..args.warmup {
                black_box(run(
                    dataset.searcher(&args, scan, &queries[..batch]),
                    scan,
                    args.points,
                ));
            }
            cases.push((scan, batch));
        }
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
    let json = serde_json::to_string_pretty(&report).unwrap();
    if let Some(output) = &args.output {
        std::fs::write(output, &json).unwrap();
    } else {
        println!("{json}");
    }
}
