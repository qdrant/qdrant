#[cfg(not(target_os = "windows"))]
mod prof;

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use common::cpu::CpuPermit;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::prelude::StdRng;
use rand::SeedableRng;
use segment::data_types::query_context::QueryContext;
use segment::data_types::vectors::{only_default_multi_vector, DEFAULT_VECTOR_NAME};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::payload_fixtures::random_multi_vector;
use segment::index::hnsw_index::graph_links::GraphLinksRam;
use segment::index::hnsw_index::hnsw::HNSWIndex;
use segment::index::hnsw_index::num_rayon_threads;
use segment::index::VectorIndex;
use segment::segment_constructor::build_segment;
use segment::types::Distance::Dot;
use segment::types::{
    HnswConfig, Indexes, MultiVectorConfig, SegmentConfig, SeqNumberType, VectorDataConfig,
    VectorStorageType,
};
use tempfile::Builder;

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
                multi_vec_config: Some(MultiVectorConfig::default()), // uses multivec config
                datatype: None,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };

    let mut segment = build_segment(segment_dir.path(), &segment_config, true).unwrap();
    for n in 0..NUM_POINTS {
        let idx = (n as u64).into();
        let multi_vec = random_multi_vector(&mut rnd, VECTOR_DIM, NUM_VECTORS_PER_POINT);
        let named_vectors = only_default_multi_vector(&multi_vec);
        segment
            .upsert_point(n as SeqNumberType, idx, named_vectors)
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
    let mut hnsw_index = HNSWIndex::<GraphLinksRam>::open(
        hnsw_dir.path(),
        segment.id_tracker.clone(),
        vector_storage.clone(),
        quantized_vectors.clone(),
        segment.payload_index.clone(),
        hnsw_config,
    )
    .unwrap();

    hnsw_index.build_index(permit.clone(), &stopped).unwrap();

    // intent: bench `search` without filter
    group.bench_function("hnsw-multivec-search", |b| {
        b.iter(|| {
            // new query to avoid caching effects
            let query = random_multi_vector(&mut rnd, VECTOR_DIM, NUM_VECTORS_PER_POINT).into();

            let results = hnsw_index
                .search(
                    &[&query],
                    None,
                    TOP,
                    None,
                    &stopped,
                    &QueryContext::new(usize::MAX),
                )
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
