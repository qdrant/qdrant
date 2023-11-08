#[cfg(not(target_os = "windows"))]
mod prof;

use std::sync::atomic::AtomicBool;

use criterion::{criterion_group, criterion_main, Criterion};
use rand::rngs::StdRng;
use rand::SeedableRng;
use segment::fixtures::sparse_fixtures::fixture_sparse_index_ram;
use segment::index::VectorIndex;
use sparse::common::sparse_vector_fixture::random_sparse_vector;

const NUM_VECTORS: usize = 10000;
const MAX_SPARSE_DIM: usize = 512;

fn sparse_vector_index_search_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("sparse-vector-search-group");

    let stopped = AtomicBool::new(false);
    let mut rnd = StdRng::seed_from_u64(42);
    let sparse_vector_index =
        fixture_sparse_index_ram(&mut rnd, NUM_VECTORS, MAX_SPARSE_DIM, &stopped);

    let mut result_size = 0;
    let mut query_count = 0;

    group.bench_function("sparse-index-search", |b| {
        b.iter(|| {
            let sparse_vector = random_sparse_vector(&mut rnd, MAX_SPARSE_DIM);
            let query_vector = sparse_vector.into();
            result_size += sparse_vector_index
                .search(&[&query_vector], None, 1, None, &stopped)
                .unwrap()
                .len();

            query_count += 1;
        })
    });

    eprintln!(
        "result_size / query_count = {:#?}",
        result_size / query_count
    );

    group.finish();
}

#[cfg(not(target_os = "windows"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = sparse_vector_index_search_benchmark
}

#[cfg(target_os = "windows")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = sparse_vector_index_search_benchmark,
}

criterion_main!(benches);
