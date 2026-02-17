use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic_refcell::AtomicRefCell;
use common::mmap::AdviceSetting;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use rand::Rng;
use rand::distr::StandardUniform;
use segment::data_types::named_vectors::CowVector;
use segment::data_types::vectors::{DenseVector, QueryVector};
use segment::fixtures::payload_context_fixture::FixtureIdTracker;
use segment::id_tracker::IdTrackerSS;
use segment::index::hnsw_index::point_scorer::BatchFilteredSearcher;
use segment::types::Distance;
use segment::vector_storage::dense::memmap_dense_vector_storage::open_memmap_vector_storage;
use segment::vector_storage::{DEFAULT_STOPPED, VectorStorage, VectorStorageEnum};
use tempfile::Builder;

#[cfg(not(target_os = "windows"))]
mod prof;

const NUM_VECTORS: usize = 10_000;
const DIM: usize = 1024;

fn random_vector(size: usize) -> DenseVector {
    let rng = rand::rng();
    rng.sample_iter(StandardUniform).take(size).collect()
}

fn init_mmap_vector_storage(
    path: &Path,
    dim: usize,
    num: usize,
    dist: Distance,
    populate: bool,
) -> (VectorStorageEnum, Arc<AtomicRefCell<IdTrackerSS>>) {
    let id_tracker = Arc::new(AtomicRefCell::new(FixtureIdTracker::new(num)));
    let mut storage =
        open_memmap_vector_storage(path, dim, dist, AdviceSetting::Global, populate).unwrap();
    let mut vectors = (0..num).map(|_id| {
        let vector = random_vector(dim);
        (CowVector::from(vector), false)
    });
    storage
        .update_from(&mut vectors, &AtomicBool::from(false))
        .unwrap();

    assert_eq!(storage.available_vector_count(), num);
    drop(storage);
    let storage =
        open_memmap_vector_storage(path, dim, dist, AdviceSetting::Global, populate).unwrap();
    assert_eq!(storage.available_vector_count(), num);
    (storage, id_tracker)
}

fn benchmark_scorer_mmap(c: &mut Criterion) {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

    let dist = Distance::Dot;
    let (storage, id_tracker) = init_mmap_vector_storage(dir.path(), DIM, NUM_VECTORS, dist, false);
    let borrowed_id_tracker = id_tracker.borrow();

    let mut group = c.benchmark_group("storage-score-all");

    group.bench_function("storage batched vector scoring", |b| {
        b.iter_batched(
            || QueryVector::from(random_vector(DIM)),
            |vector| {
                BatchFilteredSearcher::new_for_test(
                    &[vector],
                    &storage,
                    borrowed_id_tracker.deleted_point_bitslice(),
                    10,
                )
                .peek_top_all(&DEFAULT_STOPPED)
                .unwrap()
            },
            BatchSize::SmallInput,
        )
    });
}

// Batched search gives performance benefit only when memory is contended.
// For a single-threaded criterion run, it only shows that batching penalty is relatively small.
// We might run a thread pool explicitly, though.
fn benchmark_scorer_mmap_4(c: &mut Criterion) {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

    let dist = Distance::Dot;
    let (storage, id_tracker) = init_mmap_vector_storage(dir.path(), DIM, NUM_VECTORS, dist, false);
    let borrowed_id_tracker = id_tracker.borrow();

    let mut group = c.benchmark_group("storage-score-all");

    group.bench_function("storage batched vector scoring, 4 vectors batch", |b| {
        b.iter_batched(
            || {
                [
                    QueryVector::from(random_vector(DIM)),
                    QueryVector::from(random_vector(DIM)),
                    QueryVector::from(random_vector(DIM)),
                    QueryVector::from(random_vector(DIM)),
                ]
            },
            |vecs| {
                BatchFilteredSearcher::new_for_test(
                    &vecs,
                    &storage,
                    borrowed_id_tracker.deleted_point_bitslice(),
                    10,
                )
                .peek_top_all(&DEFAULT_STOPPED)
                .unwrap()
            },
            BatchSize::SmallInput,
        )
    });
}
#[cfg(not(target_os = "windows"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = benchmark_scorer_mmap, benchmark_scorer_mmap_4,
}

#[cfg(target_os = "windows")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = benchmark_scorer_mmap, benchmark_scorer_mmap_4,
}

criterion_main!(benches);
