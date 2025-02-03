#[cfg(not(target_os = "windows"))]
mod prof;

use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::SeedableRng;
use rand::rngs::StdRng;
use segment::common::rocksdb_wrapper::{DB_VECTOR_CF, open_db};
use segment::vector_storage::VectorStorage;
use segment::vector_storage::sparse::mmap_sparse_vector_storage::MmapSparseVectorStorage;
use segment::vector_storage::sparse::simple_sparse_vector_storage::open_simple_sparse_vector_storage;
use sparse::common::sparse_vector_fixture::random_sparse_vector;
use tempfile::Builder;

const NUM_VECTORS: usize = 10_000;
const MAX_SPARSE_DIM: usize = 1_000;

fn sparse_vector_storage_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("sparse-vector-storage-group");

    let stopped = AtomicBool::new(false);
    let mut rnd = StdRng::seed_from_u64(42);
    let storage_dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let db = open_db(storage_dir.path(), &[DB_VECTOR_CF]).unwrap();

    let mut rocksdb_sparse_vector_storage =
        open_simple_sparse_vector_storage(db, DB_VECTOR_CF, &stopped).unwrap();

    let hw_counter = HardwareCounterCell::new();

    group.bench_function("insert-rocksdb", |b| {
        b.iter(|| {
            for idx in 0..NUM_VECTORS {
                let vec = &random_sparse_vector(&mut rnd, MAX_SPARSE_DIM);
                rocksdb_sparse_vector_storage
                    .insert_vector(idx as PointOffsetType, vec.into(), &hw_counter)
                    .unwrap();
            }
        })
    });

    group.bench_function("read-rocksdb", |b| {
        b.iter(|| {
            for idx in 0..NUM_VECTORS {
                let vec = rocksdb_sparse_vector_storage.get_vector_opt(idx as PointOffsetType);
                assert!(vec.is_some());
            }
        })
    });

    drop(rocksdb_sparse_vector_storage);

    let storage_dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let mut mmap_sparse_vector_storage =
        MmapSparseVectorStorage::open_or_create(storage_dir.path()).unwrap();

    group.bench_function("insert-mmap-compression", |b| {
        b.iter(|| {
            for idx in 0..NUM_VECTORS {
                let vec = &random_sparse_vector(&mut rnd, MAX_SPARSE_DIM);
                mmap_sparse_vector_storage
                    .insert_vector(idx as PointOffsetType, vec.into(), &hw_counter)
                    .unwrap();
            }
        })
    });

    group.bench_function("read-mmap-compression", |b| {
        b.iter(|| {
            for idx in 0..NUM_VECTORS {
                let vec = mmap_sparse_vector_storage.get_vector_opt(idx as PointOffsetType);
                assert!(vec.is_some());
            }
        })
    });

    drop(mmap_sparse_vector_storage);

    group.finish();
}

#[cfg(not(target_os = "windows"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = sparse_vector_storage_benchmark
}

#[cfg(target_os = "windows")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = sparse_vector_storage_benchmark,
}

criterion_main!(benches);
