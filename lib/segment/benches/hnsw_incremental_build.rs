use std::collections::BTreeSet;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic_refcell::AtomicRefCell;
use clap::Parser;
use common::budget::ResourcePermit;
use common::counter::hardware_counter::HardwareCounterCell;
use common::flags::{FeatureFlags, feature_flags, init_feature_flags};
use common::types::ScoredPointOffset;
use fs_err as fs;
use fs_err::File;
use io::file_operations::{atomic_save_json, read_json};
use itertools::Itertools as _;
use ndarray::{ArrayView2, Axis};
use ndarray_npy::ViewNpyExt;
use rand::rngs::StdRng;
use rand::seq::SliceRandom as _;
use rand::{Rng, SeedableRng as _};
use rayon::iter::{
    IndexedParallelIterator as _, IntoParallelIterator as _, IntoParallelRefIterator,
    ParallelIterator,
};
use segment::common::operation_error::OperationResult;
use segment::data_types::vectors::{
    DEFAULT_VECTOR_NAME, QueryVector, VectorElementType, VectorInternal, only_default_vector,
};
use segment::entry::SegmentEntry as _;
use segment::fixtures::index_fixtures::random_vector;
use segment::id_tracker::IdTrackerSS;
use segment::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
use segment::index::hnsw_index::num_rayon_threads;
use segment::index::{VectorIndex as _, VectorIndexEnum};
use segment::segment::Segment;
use segment::segment_constructor::VectorIndexBuildArgs;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{
    Distance, ExtendedPointId, HnswConfig, HnswGlobalConfig, SearchParams, SeqNumberType,
};
use sha2::Digest as _;
use tap::Pipe as _;
use tempfile::Builder;
use zerocopy::IntoBytes;

/// This benchmark measures time and accuracy of incremental HNSW building.
///
/// To speed up the benchmark across runs, some operations that not related
/// to incremental HNSW are cached.
///
/// # Plan
///
/// 1. Non-incrementally build initial segment of size `init-vectors`.
///    The sliding window is set to `0..init_vectors`.
///    If `--cache` is set, the index is cached across runs.
/// 2. For each iteration in a loop:
///    - Slide the window by adding `to-add` vectors and removing `to-remove`
///      vectors.
///    - Incrementally build an index using the previous index.
///    - Check the accuracy of the index.
///      - Exact search results are cached across runs.
#[derive(Parser, Debug)]
#[clap(verbatim_doc_comment)]
struct Args {
    /// Ignored. (`cargo bench` passes this argument)
    #[clap(long)]
    bench: bool,

    /// Path to a dataset in numpy format.
    /// Incompatible with `--dimensions`.
    #[clap(long)]
    dataset: Option<PathBuf>,

    /// Number of dimensions to generate random vectors.
    /// Incompatible with `--dataset`.
    #[clap(long, value_parser(parse_number::<usize>))]
    dimensions: Option<usize>,

    /// Number of iterations.
    #[clap(long, default_value = "300")]
    iterations: usize,

    /// Initial number of vectors.
    #[clap(long, default_value = "100_000", value_parser(parse_number::<usize>))]
    init_vectors: usize,

    /// Number of vectors to add in each iteration.
    #[clap(long, default_value = "1_000", value_parser(parse_number::<usize>))]
    to_add: usize,

    /// Number of vectors to remove in each iteration.
    #[clap(long, default_value = "1_000", value_parser(parse_number::<usize>))]
    to_remove: usize,

    /// Distance function to use.
    #[clap(long, default_value = "Cosine")]
    distance: Distance,

    /// Index build parameter `m`.
    #[clap(long, default_value = "16", value_parser(parse_number::<usize>))]
    m: usize,

    /// Index build parameter `ef_construct`.
    /// Also, used as a search parameter `ef`.
    #[clap(long, default_value = "100", value_parser(parse_number::<usize>))]
    ef_construct: usize,

    /// Number of queries for checking the accuracy.
    #[clap(long, default_value = "100", value_parser(parse_number::<usize>))]
    queries: usize,

    /// Index search parameter `ef`.
    #[clap(long, default_value = "64", value_parser(parse_number::<usize>))]
    ef: usize,

    /// Check the accuracy every Nth iteration.
    /// Set to 0 to disable accuracy checks.
    #[clap(long, default_value = "1", value_parser(parse_number::<usize>))]
    accuracy_check_period: usize,

    /// Random seed.
    #[clap(long, default_value = "42", value_parser(parse_number::<u64>))]
    random_seed: u64,

    /// If set, cache the initial HNSW index between runs.
    #[clap(long)]
    cache: bool,
}

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .init();

    init_feature_flags(FeatureFlags::default());

    let args = Args::parse();
    log::info!("args={args:?}");

    let mut main_rng = StdRng::seed_from_u64(args.random_seed);

    let tmp_dir = Builder::new()
        .prefix("hnsw_incremental_build")
        .tempdir()
        .unwrap();

    let cache_path = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(env!("CARGO_PKG_NAME"))
        .join(env!("CARGO_CRATE_NAME"));

    // Load the dataset or generate random vectors.
    let (dataset_mmap, dataset);
    let vectors_mem;
    let (query_vectors, vectors): (Vec<QueryVector>, Vec<&[VectorElementType]>) =
        match (args.dimensions, args.dataset) {
            // Load the dataset from a file.
            (None, Some(dataset_path)) => {
                let dataset_file = File::open(dataset_path).unwrap();
                dataset_mmap = unsafe { memmap2::Mmap::map(&dataset_file).unwrap() };
                dataset = ArrayView2::<f32>::view_npy(&dataset_mmap).unwrap();
                let dataset_len = dataset.len_of(Axis(0));

                let required_points = args.init_vectors + args.queries;
                let max_points = args.init_vectors + args.to_add * args.iterations + args.queries;
                if dataset_len < required_points {
                    panic!("Dataset has {dataset_len} points, need at least {max_points}");
                }
                log::info!(
                    "Dataset length: {dataset_len}, the dataset will be traversed {:.2} times",
                    (max_points as f64) / ((dataset_len - args.init_vectors) as f64)
                );

                let mut slices_vec = dataset
                    .axis_iter(Axis(0))
                    .map(|x| x.to_slice().unwrap())
                    .collect_vec();

                // Shuffle the dataset to avoid bias.
                slices_vec.shuffle(&mut StdRng::from_rng(&mut main_rng));

                // Last `arg_queries` vectors from the dataset are used as query vectors.
                let query_vectors = slices_vec
                    .split_off(slices_vec.len() - args.queries)
                    .iter()
                    .map(|&x| QueryVector::from(x.to_vec()))
                    .collect_vec();

                (query_vectors, slices_vec)
            }
            // Generate random vectors.
            (Some(dimensions), None) => {
                let mut rng = StdRng::from_rng(&mut main_rng);
                let query_vectors = std::iter::repeat_with(|| random_vector(&mut rng, dimensions))
                    .take(args.queries)
                    .map(QueryVector::from)
                    .collect_vec();

                let mut rng = StdRng::from_rng(&mut main_rng);
                vectors_mem = std::iter::repeat_with(|| random_vector(&mut rng, dimensions))
                    .take(args.init_vectors + args.to_add * args.iterations)
                    .collect_vec();
                (
                    query_vectors,
                    vectors_mem.iter().map(|x| x.as_slice()).collect_vec(),
                )
            }
            (_, _) => panic!("Either --dimensions or --dataset must be provided, but not both."),
        };

    // Hash query vectors to use as a cache key.
    let queries_hash = dataset_hash(query_vectors.iter().map(|q| match q {
        QueryVector::Nearest(VectorInternal::Dense(v)) => v.as_slice(),
        _ => unreachable!(),
    }));

    // Build initial segment and index it non-incrementally.
    let mut sliding_window = 0..args.init_vectors;
    let mut rng = StdRng::from_rng(&mut main_rng);
    let mut last_segment = make_segment(
        &mut rng,
        tmp_dir.path(),
        &vectors,
        sliding_window.clone(),
        args.distance,
    );
    let initial_index_path = if args.cache {
        cache_path.join(format!(
            "initial-{dataset_hash}-{m}-{ef_construct}-{distance:?}",
            dataset_hash = dataset_hash(vectors[sliding_window.clone()].iter().copied()),
            m = args.m,
            ef_construct = args.ef_construct,
            distance = args.distance,
        ))
    } else {
        last_segment.data_path().join("hnsw_bench")
    };
    let index = build_hnsw_index(
        &mut rng,
        &initial_index_path,
        &last_segment,
        &[],
        args.m,
        args.ef_construct,
    );
    let mut last_index = Arc::new(AtomicRefCell::new(VectorIndexEnum::Hnsw(index)));

    for iteration in 0..args.iterations {
        // Build a new segment and index it incrementally.
        let segment = make_segment(
            &mut rng,
            tmp_dir.path(),
            &vectors,
            sliding_window.clone(),
            args.distance,
        );
        let index = build_hnsw_index(
            &mut rng,
            &segment.data_path().join("hnsw_bench"),
            &segment,
            &[Arc::clone(&last_index)],
            args.m,
            args.ef_construct,
        );

        // Check the accuracy of the index.
        if args.accuracy_check_period > 0
            && (iteration % args.accuracy_check_period == 0 || iteration == args.iterations - 1)
        {
            let top = 10;
            let exact_cache_path = cache_path.join(format!(
                "exact-{queries_hash}-{}-{top}",
                dataset_hash(sliding_window.clone().map(|i| vectors[i % vectors.len()])),
            ));
            let accuracy = measure_accuracy(
                &exact_cache_path,
                &segment,
                &query_vectors,
                &index,
                args.ef,
                top,
            );
            println!("iteration={iteration}, accuracy={accuracy}");
            assert!(accuracy > 0.4);
        } else {
            println!("iteration={iteration}, accuracy=N/A");
        }

        let last_segment_path = last_segment.data_path();

        last_segment = segment;
        last_index = Arc::new(AtomicRefCell::new(VectorIndexEnum::Hnsw(index)));

        // Cleanup previous segment and index.
        fs::remove_dir_all(last_segment_path).unwrap();

        // Slide the window.
        sliding_window =
            (sliding_window.start + args.to_remove)..(sliding_window.end + args.to_add);
    }
}

fn make_segment(
    rng: &mut StdRng,
    path: &Path,
    all_vectors: &[&[VectorElementType]],
    sliding_window: std::ops::Range<usize>,
    distance: Distance,
) -> Segment {
    let mut sequence = sliding_window.map(|x| x % all_vectors.len()).collect_vec();
    sequence.shuffle(rng);

    let hw_counter = HardwareCounterCell::new();

    let mut segment = build_simple_segment(path, all_vectors[0].len(), distance).unwrap();
    for n in sequence {
        let vector = only_default_vector(all_vectors[n]);
        segment
            .upsert_point(
                n as SeqNumberType,
                ExtendedPointId::NumId(n as u64),
                vector,
                &hw_counter,
            )
            .unwrap();
    }

    segment
}

/// Hash the dataset to use as a cache key.
fn dataset_hash<'a>(
    vectors: impl DoubleEndedIterator<Item = &'a [VectorElementType]> + ExactSizeIterator,
) -> String {
    let mut hasher = sha2::Sha256::new();
    let mut vectors = vectors.peekable();

    hasher.update(vectors.len().to_le_bytes());
    hasher.update(vectors.peek().unwrap().len().to_le_bytes());

    // For performance reasons, hash only first and last 100 vectors.
    for vector in vectors.by_ref().take(100) {
        hasher.update(vector.as_bytes());
    }
    for vector in vectors.rev().take(100) {
        hasher.update(vector.as_bytes());
    }

    format!("{:x}", hasher.finalize())
}

fn build_hnsw_index<R: Rng + ?Sized>(
    rng: &mut R,
    path: &Path,
    segment: &Segment,
    old_indices: &[Arc<AtomicRefCell<VectorIndexEnum>>],
    m: usize,
    ef_construct: usize,
) -> HNSWIndex {
    let hnsw_config = HnswConfig {
        m,
        ef_construct,
        full_scan_threshold: 1,
        max_indexing_threads: 0,
        on_disk: Some(false),
        payload_m: None,
        inline_storage: None,
    };

    let open_args = HnswIndexOpenArgs {
        path,
        id_tracker: segment.id_tracker.clone(),
        vector_storage: segment.vector_data[DEFAULT_VECTOR_NAME]
            .vector_storage
            .clone(),
        quantized_vectors: segment.vector_data[DEFAULT_VECTOR_NAME]
            .quantized_vectors
            .clone(),
        payload_index: Arc::clone(&segment.payload_index),
        hnsw_config,
    };

    pub const HNSW_INDEX_CONFIG_FILE: &str = "hnsw_config.json";
    if path.join(HNSW_INDEX_CONFIG_FILE).exists() {
        log::info!("Loading cached HNSW index from {path:?}");
        return HNSWIndex::open(open_args).unwrap();
    }

    let permit_cpu_count = num_rayon_threads(open_args.hnsw_config.max_indexing_threads);
    let permit = Arc::new(ResourcePermit::dummy(permit_cpu_count as u32));

    HNSWIndex::build(
        open_args,
        VectorIndexBuildArgs {
            permit,
            old_indices,
            gpu_device: None,
            rng,
            stopped: &AtomicBool::new(false),
            hnsw_global_config: &HnswGlobalConfig::default(),
            feature_flags: feature_flags(),
        },
    )
    .unwrap()
}

fn measure_accuracy(
    exact_cache_path: &Path,
    segment: &Segment,
    query_vectors: &[QueryVector],
    hnsw_index: &HNSWIndex,
    ef: usize,
    top: usize,
) -> f64 {
    let id_tracker = segment.id_tracker.borrow();

    // Exact search (aka full scan) is slow, so we cache the results.
    let exact_search_results;
    if exact_cache_path.exists() {
        exact_search_results = read_json(exact_cache_path).unwrap()
    } else {
        let start = std::time::Instant::now();
        exact_search_results = query_vectors
            .par_iter()
            .map(|query| {
                segment.vector_data[DEFAULT_VECTOR_NAME]
                    .vector_index
                    .borrow()
                    .search(&[query], None, top, None, &Default::default())
                    .pipe(|results| process_search_results(&*id_tracker, results))
            })
            .collect::<Vec<_>>();
        log::debug!("Exact search time = {:?}", start.elapsed());
        atomic_save_json(exact_cache_path, &exact_search_results).unwrap();
    }

    let sames: usize = query_vectors
        .par_iter()
        .zip(exact_search_results.into_par_iter())
        .map(|(query, plain_result)| {
            let index_result = hnsw_index
                .search(
                    &[query],
                    None,
                    top,
                    Some(&SearchParams {
                        hnsw_ef: Some(ef),
                        ..Default::default()
                    }),
                    &Default::default(),
                )
                .pipe(|results| process_search_results(&*id_tracker, results));

            // Get number of same results.
            index_result
                .iter()
                .collect::<BTreeSet<_>>()
                .intersection(&plain_result.iter().collect())
                .count()
        })
        .sum();
    sames as f64 / (query_vectors.len() * top) as f64
}

fn process_search_results(
    id_tracker: &IdTrackerSS,
    results: OperationResult<Vec<Vec<ScoredPointOffset>>>,
) -> Vec<ExtendedPointId> {
    // Expect exactly one result
    let result = results.unwrap().into_iter().exactly_one().unwrap();
    // Convert ScoredPointOffset to ExtendedPointId
    result
        .into_iter()
        .map(|x| id_tracker.external_id(x.idx).unwrap())
        .collect_vec()
}

fn parse_number<T: TryFrom<u64>>(n: &str) -> Result<T, String> {
    parse_number_impl(n)
        .and_then(|v| v.try_into().ok())
        .ok_or_else(|| format!("Invalid number: {n}"))
}

fn parse_number_impl(n: &str) -> Option<u64> {
    let mut chars = n.chars();
    let mut result = u64::from(chars.next()?.to_digit(10)?);

    while let Some(c) = chars.next() {
        if let Some(v) = c.to_digit(10) {
            result = result.checked_mul(10)?.checked_add(u64::from(v))?;
        } else if c != '_' {
            let power = "kMGT".find(c)? as u32 + 1;
            let multiplier = match chars.next() {
                Some('i') => 1024u64.pow(power),
                Some(_) => return None,
                None => 1000u64.pow(power),
            };
            return result.checked_mul(multiplier);
        }
    }

    Some(result)
}
