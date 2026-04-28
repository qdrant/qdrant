use std::array;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic_refcell::AtomicRefCell;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use rand::RngExt;
use rand::distr::StandardUniform;
use segment::data_types::named_vectors::CowVector;
use segment::data_types::vectors::{DenseVector, QueryVector};
use segment::fixtures::payload_context_fixture::create_id_tracker_fixture;
use segment::id_tracker::IdTrackerRead;
use segment::index::hnsw_index::point_scorer::BatchFilteredSearcher;
use segment::types::Distance;
use segment::vector_storage::dense::dense_vector_storage::open_dense_vector_storage;
use segment::vector_storage::{DEFAULT_STOPPED, VectorStorage};
use tempfile::Builder;

#[cfg(not(target_os = "windows"))]
mod prof;

const DIM: usize = 1024;

fn random_vector(size: usize) -> DenseVector {
    rand::rng()
        .sample_iter(StandardUniform)
        .take(size)
        .collect()
}

fn random_query_batch<const SIZE: usize>() -> [QueryVector; SIZE] {
    array::from_fn(|_| QueryVector::from(random_vector(DIM)))
}

fn benchmark<const IO_URING: bool, const VECTORS: usize, const BATCH: usize>(c: &mut Criterion) {
    let tmp = Builder::new()
        .prefix("vector-search-bench")
        .tempdir()
        .expect("tempdir created");

    #[cfg(target_os = "linux")]
    segment::vector_storage::common::set_async_scorer(IO_URING);

    #[cfg(not(target_os = "linux"))]
    assert!(!IO_URING, "async scorer is only supported on Linux");

    let mut storage = open_dense_vector_storage(tmp.path(), DIM, Distance::Dot, false)
        .expect("vector storage created");

    let mut vectors = (0..VECTORS).map(|_| {
        let vector = random_vector(DIM);
        (CowVector::from(vector), false)
    });

    storage
        .update_from(&mut vectors, &AtomicBool::from(false))
        .expect("vector storage populated");

    let id_tracker = Arc::new(AtomicRefCell::new(create_id_tracker_fixture(VECTORS)));
    let id_tracker = id_tracker.borrow();

    let mut group = c.benchmark_group("vector search");

    let benchmark_id = format!(
        "{} storage/{}k vectors/batch of {BATCH}",
        if IO_URING { "io_uring" } else { "mmap" },
        VECTORS / 1000,
    );

    group.bench_function(benchmark_id, |b| {
        b.iter_batched(
            random_query_batch::<BATCH>,
            |vectors| {
                BatchFilteredSearcher::new_for_test(
                    &vectors,
                    &storage,
                    id_tracker.deleted_point_bitslice(),
                    10,
                )
                .peek_top_all(&DEFAULT_STOPPED, None)
                .expect("points scored")
            },
            BatchSize::SmallInput,
        )
    });
}

#[cfg(target_os = "linux")]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(1000));
    targets =
        benchmark::<false, 10_000, 1>,
        benchmark::<false, 10_000, 4>,
        benchmark::<false, 100_000, 1>,
        benchmark::<false, 100_000, 4>,

        benchmark::<true, 10_000, 1>,
        benchmark::<true, 10_000, 4>,
        benchmark::<true, 100_000, 1>,
        benchmark::<true, 100_000, 4>,
}

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(1000));
    targets =
        benchmark::<false, 10_000, 1>,
        benchmark::<false, 10_000, 4>,
        benchmark::<false, 100_000, 1>,
        benchmark::<false, 100_000, 4>,
}

#[cfg(target_os = "windows")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets =
        benchmark::<false, 10_000, 1>,
        benchmark::<false, 10_000, 4>,
        benchmark::<false, 100_000, 1>,
        benchmark::<false, 100_000, 4>,
}

criterion_main!(benches);
