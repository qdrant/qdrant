//! Tests of the PiPNN graph construction.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::time::Instant;

use common::ambient;
use common::bitvec::BitVec;
use common::mmap::AdviceSetting;
use common::types::PointOffsetType;
use ndarray::ArrayView2;
use ndarray_npy::ViewNpyExt as _;
use rand::SeedableRng as _;
use rand::rngs::StdRng;
use rayon::prelude::*;

use super::{PipnnParams, build_links};
use crate::data_types::vectors::{DenseVector, VectorElementType, VectorRef};
use crate::index::hnsw_index::HnswM;
use crate::index::hnsw_index::graph_layers::{GraphLayers, SearchAlgorithm};
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::graph_links::GraphLinksFormatParam;
use crate::index::hnsw_index::hnsw::SINGLE_THREADED_HNSW_BUILD_THRESHOLD;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::types::{
    Distance, QuantizationConfig, ScalarQuantizationConfig, TurboQuantBitSize,
    TurboQuantQuantizationConfig, TurboQuantization,
};
use crate::vector_storage::dense::appendable_dense_vector_storage::open_appendable_memmap_vector_storage_full;
use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
use crate::vector_storage::matrix_scorer::new_matrix_scorer;
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedVectors, QuantizedVectorsStorageType,
};
use crate::vector_storage::{VectorStorage as _, VectorStorageEnum};

const M: usize = 16;
const EF_CONSTRUCT: usize = 100;
const TOP: usize = 10;

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name).map_or(default, |value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{name} can't be parsed"))
    })
}

/// Fraction of the [`TOP`] true nearest neighbors that `graph` finds with `ef`.
fn recall(
    storage: &VectorStorageEnum,
    quantized: Option<&QuantizedVectors>,
    deleted: &BitVec,
    graph: &GraphLayers,
    queries: &[DenseVector],
    ground_truth: &[Vec<PointOffsetType>],
    ef: usize,
) -> f64 {
    let found: usize = std::iter::zip(queries, ground_truth)
        .par_bridge()
        .map(|(query, exact)| {
            let _scope = ambient::test_guard();
            let mut scorer =
                FilteredScorer::new(query.clone().into(), storage, quantized, None, deleted)
                    .unwrap();
            let result = graph
                .search(
                    TOP,
                    ef,
                    SearchAlgorithm::Hnsw,
                    &mut scorer,
                    &graph.unfiltered_entry_point(),
                    &AtomicBool::new(false),
                )
                .unwrap();
            let exact = &exact[..TOP];
            result.iter().filter(|s| exact.contains(&s.idx)).count()
        })
        .sum();
    found as f64 / (queries.len() * TOP) as f64
}

/// Throwaway benchmark: PiPNN against HNSW on a vector-db-benchmark dataset, a directory with
/// `vectors.npy` and `tests.jsonl`, whose `closest_ids` are the true nearest neighbors.
///
/// ```text
/// PIPNN_DATASET=<dir> cargo test --profile perf -p segment --lib pipnn::tests::bench_dataset \
///     -- --ignored --nocapture
/// ```
///
/// Optional: `PIPNN_DISTANCE` (cosine, euclid, dot; default cosine), `PIPNN_QUERIES` (1000),
/// `PIPNN_THREADS` (all), `PIPNN_QUANTIZATION` (none, scalar, tq1, tq2, tq4, tq8), `PIPNN_LEAF_K`,
/// `PIPNN_RESERVOIR_FACTOR`, `PIPNN_SKIP_HNSW`, `PIPNN_MMAP_ORIGINALS`.
#[test]
#[ignore]
#[expect(deprecated, reason = "always_ram")]
fn bench_dataset() {
    let _scope = ambient::test_guard();
    let _ = env_logger::builder()
        .filter_module(
            module_path!().trim_end_matches("::tests"),
            log::LevelFilter::Debug,
        )
        .try_init();

    let dir = std::env::var("PIPNN_DATASET").expect("PIPNN_DATASET is not set");
    let dir = Path::new(&dir);
    let distance = match std::env::var("PIPNN_DISTANCE").as_deref() {
        Err(_) | Ok("cosine") => Distance::Cosine,
        Ok("euclid") => Distance::Euclid,
        Ok("dot") => Distance::Dot,
        Ok(other) => panic!("PIPNN_DISTANCE must be cosine, euclid or dot, not {other}"),
    };
    let max_queries = env_or("PIPNN_QUERIES", 1000);
    let threads = env_or("PIPNN_THREADS", 0);
    let skip_hnsw = std::env::var_os("PIPNN_SKIP_HNSW").is_some();
    let defaults = PipnnParams::default();
    let params = PipnnParams {
        leaf_k: env_or("PIPNN_LEAF_K", defaults.leaf_k),
        reservoir_factor: env_or("PIPNN_RESERVOIR_FACTOR", defaults.reservoir_factor),
        seed: 42,
        ..defaults
    };

    // Vectors
    let timer = Instant::now();
    // Mapped, not read, so that 1M x 1536 fits in memory next to the storage's copy
    let file = fs_err::File::open(dir.join("vectors.npy")).unwrap();
    let mmap = unsafe { memmap2::Mmap::map(file.file()) }.unwrap();
    let matrix = ArrayView2::<f32>::view_npy(&mmap).unwrap();
    let (num_vectors, dim) = matrix.dim();
    // `PIPNN_MMAP_ORIGINALS`: originals in a file-backed storage in `TMPDIR`, whose pages the kernel
    // can evict, for runs that don't fit in memory otherwise (quantized ones at 1M x 1536)
    let storage_dir = tempfile::tempdir().unwrap();
    let mut storage = if std::env::var_os("PIPNN_MMAP_ORIGINALS").is_some() {
        open_appendable_memmap_vector_storage_full(
            storage_dir.path(),
            dim,
            distance,
            AdviceSetting::Global,
            false,
        )
        .unwrap()
    } else {
        new_volatile_dense_vector_storage(dim, distance)
    };
    for (id, row) in matrix.rows().into_iter().enumerate() {
        let vector = distance.preprocess_vector::<VectorElementType>(row.to_vec());
        storage
            .insert_vector(id as PointOffsetType, VectorRef::from(&vector))
            .unwrap();
    }
    drop(mmap);
    let deleted = BitVec::repeat(false, num_vectors);

    let tests = fs_err::read_to_string(dir.join("tests.jsonl")).unwrap();
    let (queries, ground_truth): (Vec<DenseVector>, Vec<Vec<PointOffsetType>>) = tests
        .lines()
        .take(max_queries)
        .map(|line| {
            let test: serde_json::Value = serde_json::from_str(line).unwrap();
            let query = serde_json::from_value(test["query"].clone()).unwrap();
            let closest = serde_json::from_value(test["closest_ids"].clone()).unwrap();
            (query, closest)
        })
        .unzip();
    eprintln!(
        "loaded {num_vectors} vectors of {dim} dims and {} queries in {:.2?}",
        queries.len(),
        timer.elapsed(),
    );

    let quantization_dir = tempfile::tempdir().unwrap();
    let quantization = std::env::var("PIPNN_QUANTIZATION").unwrap_or_default();
    let turbo = |bits| {
        Some(QuantizationConfig::Turbo(TurboQuantization {
            turbo: TurboQuantQuantizationConfig {
                always_ram: None,
                memory: None,
                bits: Some(bits),
            },
        }))
    };
    let config = match quantization.as_str() {
        "" | "none" => None,
        "scalar" => Some(
            ScalarQuantizationConfig {
                r#type: Default::default(),
                quantile: None,
                always_ram: None,
                memory: None,
            }
            .into(),
        ),
        "tq1" => turbo(TurboQuantBitSize::Bits1),
        "tq2" => turbo(TurboQuantBitSize::Bits2),
        "tq4" => turbo(TurboQuantBitSize::Bits4),
        "tq8" => turbo(TurboQuantBitSize::Bits8),
        other => panic!("PIPNN_QUANTIZATION must be none, scalar, or tq1/2/4/8, not {other}"),
    };
    let quantized = config.map(|config| {
        let timer = Instant::now();
        let quantized = QuantizedVectors::create(
            &storage,
            &config,
            QuantizedVectorsStorageType::Immutable,
            &quantization_dir.path().join("quantized"),
            std::thread::available_parallelism().map_or(1, usize::from),
            &AtomicBool::new(false),
        )
        .unwrap();
        eprintln!("{quantization} quantization in {:.2?}", timer.elapsed());
        quantized
    });
    let quantized = quantized.as_ref();

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .unwrap();
    let stopped = AtomicBool::new(false);

    // Same levels for both builders, entry points as `HNSWIndex::build` with the default
    // `full_scan_threshold` of 10000
    let hnsw_m = HnswM::new2(M);
    let entry_points_num = (num_vectors / 10_000 * 10).max(1);
    let mut rng = StdRng::seed_from_u64(42);
    let levels: Vec<usize> = {
        let builder = GraphLayersBuilder::new(0, hnsw_m, EF_CONSTRUCT, entry_points_num, true);
        (0..num_vectors)
            .map(|_| builder.get_random_layer(&mut rng))
            .collect()
    };
    let new_builder = || {
        let mut builder =
            GraphLayersBuilder::new(num_vectors, hnsw_m, EF_CONSTRUCT, entry_points_num, true);
        for (id, &level) in levels.iter().enumerate() {
            builder.set_levels(id as PointOffsetType, level);
        }
        builder
    };
    let points: Vec<PointOffsetType> = (0..num_vectors as PointOffsetType).collect();

    eprintln!(
        "{}, {num_vectors} vectors, m {M}, quantization {}, {} threads, {params:?}",
        dir.display(),
        if quantization.is_empty() {
            "none"
        } else {
            &quantization
        },
        pool.current_num_threads(),
    );

    // PiPNN
    let pipnn_builder = new_builder();
    let scorer = new_matrix_scorer(&storage, quantized).expect("no matrix scorer");
    let progress = AtomicU64::new(0);
    let timer = Instant::now();
    let links = pool
        .install(|| {
            build_links(
                &storage,
                quantized,
                &deleted,
                &scorer,
                &points,
                |point| levels[point as usize],
                hnsw_m,
                &params,
                &stopped,
                &progress,
            )
        })
        .unwrap()
        .expect("vectors can't be sketched");
    let links_time = timer.elapsed();
    // Sequentially, in point order, for deterministic entry points
    for (&point, links_by_level) in std::iter::zip(&points, links) {
        pipnn_builder.add_new_point(point, links_by_level);
    }
    let pipnn_time = timer.elapsed();
    let pipnn_degree = pipnn_builder.get_average_connectivity_on_level(0);
    let pipnn_graph = pipnn_builder.into_graph_layers_ram(GraphLinksFormatParam::Plain);
    eprintln!(
        "PiPNN build {pipnn_time:.2?} (links {links_time:.2?}), degree {pipnn_degree:.1}, \
         progress {}",
        progress.into_inner(),
    );

    // HNSW, as `HnswBuilder::insert`: the first points on one thread, to start connected
    let hnsw_graph = (!skip_hnsw).then(|| {
        let hnsw_builder = new_builder();
        let timer = Instant::now();
        let insert = |point: &PointOffsetType| {
            let _scope = ambient::test_guard();
            let scorer =
                FilteredScorer::new_internal(*point, &storage, quantized, None, &deleted).unwrap();
            hnsw_builder.link_new_point(*point, scorer);
        };
        let (first, rest) = points.split_at(num_vectors.min(SINGLE_THREADED_HNSW_BUILD_THRESHOLD));
        first.iter().for_each(insert);
        pool.install(|| rest.par_iter().with_max_len(64).for_each(insert));
        let hnsw_time = timer.elapsed();
        let hnsw_degree = hnsw_builder.get_average_connectivity_on_level(0);
        eprintln!(
            "HNSW build {hnsw_time:.2?}, ef_construct {EF_CONSTRUCT}, degree {hnsw_degree:.1}"
        );
        hnsw_builder.into_graph_layers_ram(GraphLinksFormatParam::Plain)
    });

    // With quantization, search once with the quantized scores (no rescoring) and once with the
    // originals, which shows the quality of the graph alone
    let searches: &[(&str, Option<&QuantizedVectors>)] = match quantized {
        Some(quantized) => &[("quantized", Some(quantized)), ("originals", None)],
        None => &[("originals", None)],
    };
    for &(search, search_quantized) in searches {
        for ef in [16, 32, 64, 128, 256, 512] {
            let measure = |graph: &GraphLayers| {
                let timer = Instant::now();
                let recall = pool.install(|| {
                    recall(
                        &storage,
                        search_quantized,
                        &deleted,
                        graph,
                        &queries,
                        &ground_truth,
                        ef,
                    )
                });
                (recall, timer.elapsed())
            };
            let (pipnn, pipnn_time) = measure(&pipnn_graph);
            let hnsw = hnsw_graph.as_ref().map_or("-".to_string(), |graph| {
                let (recall, time) = measure(graph);
                format!("{recall:.4} ({time:.2?})")
            });
            eprintln!(
                "search {search}, recall@{TOP} ef {ef:>3}: PiPNN {pipnn:.4} ({pipnn_time:.2?}), \
                 HNSW {hnsw}"
            );
        }
    }
}
