#[cfg(not(target_os = "windows"))]
mod prof;

use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::types::PointOffsetType;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::SeedableRng;
use rand::rngs::SmallRng;
use segment::vector_storage::sparse::mmap_sparse_vector_storage::MmapSparseVectorStorage;
use segment::vector_storage::sparse::volatile_sparse_vector_storage::VolatileSparseVectorStorage;
use segment::vector_storage::{VectorStorage, VectorStorageRead};
use sparse::common::sparse_vector_fixture::random_sparse_vector;
use tempfile::Builder;

const NUM_VECTORS: usize = 10_000;
const MAX_SPARSE_DIM: usize = 1_000;

fn sparse_vector_storage_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("sparse-vector-storage-group");

    let mut rnd = SmallRng::seed_from_u64(42);

    let mut volatile_sparse_vector_storage = VolatileSparseVectorStorage::default();

    let hw_counter = HardwareCounterCell::new();

    group.bench_function("insert-volatile", |b| {
        b.iter(|| {
            for idx in 0..NUM_VECTORS {
                let vec = &random_sparse_vector(&mut rnd, MAX_SPARSE_DIM);
                volatile_sparse_vector_storage
                    .insert_vector(idx as PointOffsetType, vec.into(), &hw_counter)
                    .unwrap();
            }
        })
    });

    group.bench_function("read-volatile", |b| {
        b.iter(|| {
            for idx in 0..NUM_VECTORS {
                let vec =
                    volatile_sparse_vector_storage.get_vector_opt::<Random>(idx as PointOffsetType);
                assert!(vec.is_some());
            }
        })
    });

    drop(volatile_sparse_vector_storage);

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
                let vec =
                    mmap_sparse_vector_storage.get_vector_opt::<Random>(idx as PointOffsetType);
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
