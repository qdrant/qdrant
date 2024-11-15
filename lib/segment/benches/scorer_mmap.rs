use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use rand::distributions::Standard;
use rand::Rng;
use segment::data_types::named_vectors::CowVector;
use segment::data_types::vectors::{DenseVector, QueryVector};
use segment::fixtures::payload_context_fixture::FixtureIdTracker;
use segment::id_tracker::IdTrackerSS;
use segment::types::Distance;
use segment::vector_storage::dense::memmap_dense_vector_storage::open_memmap_vector_storage;
use segment::vector_storage::{new_raw_scorer, VectorStorage, VectorStorageEnum};
use tempfile::Builder;

#[cfg(not(target_os = "windows"))]
mod prof;

const NUM_VECTORS: usize = 10_000;
const DIM: usize = 1024;

fn random_vector(size: usize) -> DenseVector {
    let rng = rand::thread_rng();
    rng.sample_iter(Standard).take(size).collect()
}

fn init_mmap_vector_storage(
    path: &Path,
    dim: usize,
    num: usize,
    dist: Distance,
) -> (VectorStorageEnum, Arc<AtomicRefCell<IdTrackerSS>>) {
    let id_tracker = Arc::new(AtomicRefCell::new(FixtureIdTracker::new(num)));
    let mut storage = open_memmap_vector_storage(path, dim, dist).unwrap();
    let mut vectors = (0..num).map(|_id| {
        let vector = random_vector(dim);
        (CowVector::from(vector), false)
    });
    storage
        .update_from(&mut vectors, &AtomicBool::from(false))
        .unwrap();

    assert_eq!(storage.available_vector_count(), num);
    drop(storage);
    let storage = open_memmap_vector_storage(path, dim, dist).unwrap();
    assert_eq!(storage.available_vector_count(), num);
    (storage, id_tracker)
}

fn benchmark_scorer_mmap(c: &mut Criterion) {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

    let dist = Distance::Dot;
    let (storage, id_tracker) = init_mmap_vector_storage(dir.path(), DIM, NUM_VECTORS, dist);
    let borrowed_id_tracker = id_tracker.borrow();

    let mut group = c.benchmark_group("storage-score-all");

    group.bench_function("storage batched vector scoring", |b| {
        b.iter_batched(
            || QueryVector::from(random_vector(DIM)),
            |vector| {
                new_raw_scorer(
                    vector,
                    &storage,
                    borrowed_id_tracker.deleted_point_bitslice(),
                )
                .unwrap()
                .peek_top_all(10)
            },
            BatchSize::SmallInput,
        )
    });
}

#[cfg(not(target_os = "windows"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = benchmark_scorer_mmap
}

#[cfg(target_os = "windows")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = benchmark_scorer_mmap,
}

criterion_main!(benches);
