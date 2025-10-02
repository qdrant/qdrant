use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::budget::ResourcePermit;
use common::counter::hardware_counter::HardwareCounterCell;
use common::flags::FeatureFlags;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use segment::data_types::vectors::{
    DEFAULT_VECTOR_NAME, TypedMultiDenseVectorRef, only_default_multi_vector,
};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::payload_fixtures::random_multi_vector;
use segment::index::VectorIndex;
use segment::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
use segment::index::hnsw_index::num_rayon_threads;
use segment::segment_constructor::{VectorIndexBuildArgs, build_segment};
use segment::spaces::simple::DotProductMetric;
use segment::types::Distance::{Dot, Euclid};
use segment::types::{
    Distance, HnswConfig, HnswGlobalConfig, Indexes, MultiVectorConfig, SegmentConfig,
    SeqNumberType, VectorDataConfig, VectorStorageType,
};
use tempfile::Builder;

#[cfg(not(target_os = "windows"))]
mod prof;

const NUM_POINTS: usize = 10_000;
const VECTOR_DIM: usize = 128;
const TOP: usize = 10;

// intent: bench `search` without filter
fn multi_vector_search_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi-vector-search-group");
    let mut rnd = StdRng::seed_from_u64(42);

    for num_vectors_per_point in [16, 128, 256] {
        group.bench_with_input(
            BenchmarkId::new("hnsw-multivec-search-dot", num_vectors_per_point),
            &num_vectors_per_point,
            |b, &num_vectors_per_point| {
                let hnsw_index = make_segment_index(&mut rnd, Dot, num_vectors_per_point);

                b.iter_batched(
                    || random_multi_vector(&mut rnd, VECTOR_DIM, num_vectors_per_point).into(),
                    |query| {
                        let results = hnsw_index
                            .search(&[&query], None, TOP, None, &Default::default())
                            .unwrap();
                        assert_eq!(results[0].len(), TOP);
                    },
                    BatchSize::SmallInput,
                )
            },
        );

        group.bench_with_input(
            BenchmarkId::new("hnsw-multivec-search-euclidean", num_vectors_per_point),
            &num_vectors_per_point,
            |b, &num_vectors_per_point| {
                let hnsw_index = make_segment_index(&mut rnd, Euclid, num_vectors_per_point);

                b.iter_batched(
                    || random_multi_vector(&mut rnd, VECTOR_DIM, num_vectors_per_point).into(),
                    |query| {
                        let results = hnsw_index
                            .search(&[&query], None, TOP, None, &Default::default())
                            .unwrap();
                        assert_eq!(results[0].len(), TOP);
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }
    group.finish();
}

fn make_segment_index<R: Rng + ?Sized>(
    rng: &mut R,
    distance: Distance,
    num_vectors_per_point: usize,
) -> HNSWIndex {
    let stopped = AtomicBool::new(false);
    let segment_dir = Builder::new().prefix("data_dir").tempdir().unwrap();
    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();

    let segment_config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: VECTOR_DIM,
                distance,
                storage_type: VectorStorageType::default(),
                index: Indexes::Plain {},
                quantization_config: None,
                multivector_config: Some(MultiVectorConfig::default()), // uses multivec config
                datatype: None,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };

    let hw_counter = HardwareCounterCell::new();

    let mut segment = build_segment(segment_dir.path(), &segment_config, true).unwrap();
    for n in 0..NUM_POINTS {
        let idx = (n as u64).into();
        let multi_vec = random_multi_vector(rng, VECTOR_DIM, num_vectors_per_point);
        let named_vectors = only_default_multi_vector(&multi_vec);
        segment
            .upsert_point(n as SeqNumberType, idx, named_vectors, &hw_counter)
            .unwrap();
    }

    // build HNSW index
    let hnsw_config = HnswConfig {
        m: 8,
        ef_construct: 16,
        full_scan_threshold: 10, // low value to trigger index usage by default
        max_indexing_threads: 0,
        on_disk: None,
        payload_m: None,
        copy_vectors: None,
    };
    let permit_cpu_count = num_rayon_threads(hnsw_config.max_indexing_threads);
    let permit = Arc::new(ResourcePermit::dummy(permit_cpu_count as u32));
    let vector_storage = &segment.vector_data[DEFAULT_VECTOR_NAME].vector_storage;
    let quantized_vectors = &segment.vector_data[DEFAULT_VECTOR_NAME].quantized_vectors;
    let hnsw_index = HNSWIndex::build(
        HnswIndexOpenArgs {
            path: hnsw_dir.path(),
            id_tracker: segment.id_tracker.clone(),
            vector_storage: vector_storage.clone(),
            quantized_vectors: quantized_vectors.clone(),
            payload_index: segment.payload_index.clone(),
            hnsw_config,
        },
        VectorIndexBuildArgs {
            permit,
            old_indices: &[],
            gpu_device: None,
            stopped: &stopped,
            rng,
            hnsw_global_config: &HnswGlobalConfig::default(),
            feature_flags: FeatureFlags::default(),
        },
    )
    .unwrap();
    hnsw_index.populate().unwrap();
    hnsw_index
}

// An attempt to benchmark just the multivector dot product calculation itself, as fast as possible.
fn optimistic_multi_vector_dot_benchmark(c: &mut Criterion) {
    use segment::vector_storage::query_scorer::score_max_similarity;

    let mut group = c.benchmark_group("optimisitic-multi-vector-dot-product-group");
    let mut rnd = StdRng::seed_from_u64(42);

    for num_vectors_per_point in [16, 128, 256] {
        group.bench_with_input(
            BenchmarkId::new("optimistic-multivec-dot", num_vectors_per_point),
            &num_vectors_per_point,
            |b, &num_vectors_per_point| {
                let size = num_vectors_per_point * VECTOR_DIM;
                // both target and search vector are allocated side by side in memory to avoid cache aliasing effects.
                // cache line aligned allocation.
                let mut data = aligned_vec::AVec::<f32>::with_capacity(64, 2 * size);
                for _ in 0..2 * size {
                    data.push(rnd.random());
                }
                let (target, search) = data.split_at_mut(size);
                let target = TypedMultiDenseVectorRef {
                    flattened_vectors: target,
                    dim: VECTOR_DIM,
                };
                let search = TypedMultiDenseVectorRef {
                    flattened_vectors: search,
                    dim: VECTOR_DIM,
                };

                b.iter(|| {
                    score_max_similarity::<f32, DotProductMetric>(target, search);
                })
            },
        );
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = multi_vector_search_benchmark, optimistic_multi_vector_dot_benchmark
}

criterion_main!(benches);
