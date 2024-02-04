#[cfg(not(target_os = "windows"))]
mod prof;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::cpu::CpuPermit;
use common::types::PointOffsetType;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::rngs::StdRng;
use rand::SeedableRng;
use segment::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
use segment::fixtures::payload_context_fixture::FixtureIdTracker;
use segment::index::hnsw_index::num_rayon_threads;
use segment::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use segment::index::sparse_index::sparse_vector_index::SparseVectorIndex;
use segment::index::struct_payload_index::StructPayloadIndex;
use segment::index::VectorIndex;
use segment::payload_storage::in_memory_payload_storage::InMemoryPayloadStorage;
use segment::vector_storage::simple_sparse_vector_storage::open_simple_sparse_vector_storage;
use segment::vector_storage::VectorStorage;
use sparse::common::sparse_vector_fixture::random_sparse_vector;
use sparse::index::inverted_index::inverted_index_mmap::InvertedIndexMmap;
use sparse::index::inverted_index::inverted_index_ram::InvertedIndexRam;
use sparse::index::inverted_index::InvertedIndex;
use tempfile::Builder;

const NUM_VECTORS: usize = 100_000;
const MAX_SPARSE_DIM: usize = 30_000;

fn sparse_vector_index_build_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("sparse-vector-build-group");

    let stopped = AtomicBool::new(false);
    let mut rnd = StdRng::seed_from_u64(42);

    let payload_dir = Builder::new().prefix("payload_dir").tempdir().unwrap();
    let storage_dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let index_dir = Builder::new().prefix("index_dir").tempdir().unwrap();

    // setup
    let id_tracker = Arc::new(AtomicRefCell::new(FixtureIdTracker::new(NUM_VECTORS)));
    let payload_storage = InMemoryPayloadStorage::default();
    let wrapped_payload_storage = Arc::new(AtomicRefCell::new(payload_storage.into()));
    let payload_index = StructPayloadIndex::open(
        wrapped_payload_storage,
        id_tracker.clone(),
        payload_dir.path(),
        true,
    )
    .unwrap();
    let wrapped_payload_index = Arc::new(AtomicRefCell::new(payload_index));

    let db = open_db(storage_dir.path(), &[DB_VECTOR_CF]).unwrap();
    let vector_storage = open_simple_sparse_vector_storage(db, DB_VECTOR_CF).unwrap();
    let mut borrowed_storage = vector_storage.borrow_mut();

    // add points to storage only once
    for idx in 0..NUM_VECTORS {
        let vec = &random_sparse_vector(&mut rnd, MAX_SPARSE_DIM);
        borrowed_storage
            .insert_vector(idx as PointOffsetType, vec.into())
            .unwrap();
    }
    drop(borrowed_storage);

    // save index config to disk
    let index_config = SparseIndexConfig::new(Some(10_000), SparseIndexType::ImmutableRam);

    let permit_cpu_count = num_rayon_threads(0);
    let permit = Arc::new(CpuPermit::dummy(permit_cpu_count as u32));

    // intent: measure in-memory build time from storage
    group.bench_function("build-ram-index", |b| {
        b.iter(|| {
            let mut sparse_vector_index: SparseVectorIndex<InvertedIndexRam> =
                SparseVectorIndex::open(
                    index_config,
                    id_tracker.clone(),
                    vector_storage.clone(),
                    wrapped_payload_index.clone(),
                    index_dir.path(),
                )
                .unwrap();
            sparse_vector_index
                .build_index(permit.clone(), &stopped)
                .unwrap();
            assert_eq!(sparse_vector_index.indexed_vector_count(), NUM_VECTORS);
        })
    });

    // build once to reuse in mmap conversion benchmark
    let mut sparse_vector_index: SparseVectorIndex<InvertedIndexRam> = SparseVectorIndex::open(
        index_config,
        id_tracker,
        vector_storage.clone(),
        wrapped_payload_index,
        index_dir.path(),
    )
    .unwrap();

    sparse_vector_index.build_index(permit, &stopped).unwrap();

    // intent: measure mmap conversion time
    group.bench_function("convert-mmap-index", |b| {
        b.iter(|| {
            let mmap_index_dir = Builder::new().prefix("mmap_index_dir").tempdir().unwrap();
            let mmap_inverted_index = InvertedIndexMmap::convert_and_save(
                &sparse_vector_index.inverted_index,
                &mmap_index_dir,
            )
            .unwrap();
            assert_eq!(mmap_inverted_index.vector_count(), NUM_VECTORS);
        })
    });

    group.finish();
}

#[cfg(not(target_os = "windows"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = sparse_vector_index_build_benchmark
}

#[cfg(target_os = "windows")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = sparse_vector_index_build_benchmark,
}

criterion_main!(benches);
