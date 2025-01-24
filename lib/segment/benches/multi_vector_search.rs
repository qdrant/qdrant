use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use common::counter::hardware_counter::HardwareCounterCell;
use common::cpu::CpuPermit;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::prelude::StdRng;
use rand::SeedableRng;
use segment::data_types::vectors::{only_default_multi_vector, DEFAULT_VECTOR_NAME};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::payload_fixtures::random_multi_vector;
use segment::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
use segment::index::hnsw_index::num_rayon_threads;
use segment::index::VectorIndex;
use segment::segment_constructor::{build_segment, VectorIndexBuildArgs};
use segment::types::Distance::Dot;
use segment::types::{
    HnswConfig, Indexes, MultiVectorConfig, SegmentConfig, SeqNumberType, VectorDataConfig,
    VectorStorageType,
};
use tempfile::Builder;

#[cfg(not(target_os = "windows"))]
mod prof;

const NUM_POINTS: usize = 10_000;
const NUM_VECTORS_PER_POINT: usize = 16;
const VECTOR_DIM: usize = 128;
const TOP: usize = 10;

fn multi_vector_search_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi-vector-search-group");
    let stopped = AtomicBool::new(false);
    let mut rnd = StdRng::seed_from_u64(42);

    let segment_dir = Builder::new().prefix("data_dir").tempdir().unwrap();
    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();

    let segment_config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: VECTOR_DIM,
                distance: Dot,
                storage_type: VectorStorageType::Memory,
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
        let multi_vec = random_multi_vector(&mut rnd, VECTOR_DIM, NUM_VECTORS_PER_POINT);
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
    };
    let permit_cpu_count = num_rayon_threads(hnsw_config.max_indexing_threads);
    let permit = Arc::new(CpuPermit::dummy(permit_cpu_count as u32));
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
        },
    )
    .unwrap();

    // intent: bench `search` without filter
    group.bench_function("hnsw-multivec-search", |b| {
        b.iter(|| {
            // new query to avoid caching effects
            let query = random_multi_vector(&mut rnd, VECTOR_DIM, NUM_VECTORS_PER_POINT).into();

            let results = hnsw_index
                .search(&[&query], None, TOP, None, &Default::default())
                .unwrap();
            assert_eq!(results[0].len(), TOP);
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = multi_vector_search_benchmark
}

criterion_main!(benches);
