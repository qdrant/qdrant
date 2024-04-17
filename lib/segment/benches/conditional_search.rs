#[cfg(not(target_os = "windows"))]
mod prof;

use common::types::PointOffsetType;
use criterion::{criterion_group, criterion_main, Criterion};
use itertools::Itertools;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use segment::fixtures::payload_context_fixture::{
    create_plain_payload_index, create_struct_payload_index,
};
use segment::fixtures::payload_fixtures::{random_match_any_filter, random_must_filter};
use segment::index::PayloadIndex;
use tempfile::Builder;

const NUM_POINTS: usize = 100000;
const CHECK_SAMPLE_SIZE: usize = 1000;

fn conditional_plain_search_benchmark(c: &mut Criterion) {
    let seed = 42;

    let mut rng = StdRng::seed_from_u64(seed);
    let mut group = c.benchmark_group("conditional-search-group");

    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let plain_index = create_plain_payload_index(dir.path(), NUM_POINTS, seed);

    let mut result_size = 0;
    let mut query_count = 0;

    group.bench_function("conditional-search-query-points", |b| {
        b.iter(|| {
            let filter = random_must_filter(&mut rng, 2);
            result_size += plain_index.query_points(&filter).len();
            query_count += 1;
        })
    });
    if query_count != 0 {
        eprintln!(
            "result_size / query_count = {:#?}",
            result_size / query_count
        );
    }

    let mut result_size = 0;
    let mut query_count = 0;

    // Same benchmark, but with larger expected result
    group.bench_function("conditional-search-query-points-large", |b| {
        b.iter(|| {
            let filter = random_must_filter(&mut rng, 1);
            result_size += plain_index.query_points(&filter).len();
            query_count += 1;
        })
    });
    if query_count != 0 {
        eprintln!(
            "result_size / query_count = {:#?}",
            result_size / query_count
        );
    }

    let mut result_size = 0;
    let mut query_count = 0;

    group.bench_function("conditional-search-context-check", |b| {
        b.iter(|| {
            let filter = random_must_filter(&mut rng, 2);
            let sample = (0..CHECK_SAMPLE_SIZE)
                .map(|_| rng.gen_range(0..NUM_POINTS) as PointOffsetType)
                .collect_vec();
            let context = plain_index.filter_context(&filter);

            let filtered_sample = sample
                .into_iter()
                .filter(|id| context.check(*id))
                .collect_vec();
            result_size += filtered_sample.len();
            query_count += 1;
        })
    });

    if query_count != 0 {
        eprintln!(
            "result_size / query_count = {:#?}",
            result_size / query_count
        );
    }

    let mut result_size = 0;
    let mut query_count = 0;

    group.bench_function("conditional-search-match-any", |b| {
        let filter = random_match_any_filter(&mut rng, 2, 51.0);
        b.iter(|| {
            let sample = (0..CHECK_SAMPLE_SIZE)
                .map(|_| rng.gen_range(0..NUM_POINTS) as PointOffsetType)
                .collect_vec();
            let context = plain_index.filter_context(&filter);
            let filtered_sample = sample
                .into_iter()
                .filter(|id| context.check(*id))
                .collect_vec();
            result_size += filtered_sample.len();
            query_count += 1;
        });
    });

    group.bench_function("conditional-search-large-match-any", |b| {
        let filter = random_match_any_filter(&mut rng, 1000, 15.0);
        b.iter(|| {
            let sample = (0..CHECK_SAMPLE_SIZE)
                .map(|_| rng.gen_range(0..NUM_POINTS) as PointOffsetType)
                .collect_vec();
            let context = plain_index.filter_context(&filter);
            let filtered_sample = sample
                .into_iter()
                .filter(|id| context.check(*id))
                .collect_vec();
            result_size += filtered_sample.len();
            query_count += 1;
        });
    });

    group.finish();
}

fn conditional_struct_search_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let mut group = c.benchmark_group("conditional-search-group");

    let seed = 42;

    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let struct_index = create_struct_payload_index(dir.path(), NUM_POINTS, seed);

    let mut result_size = 0;
    let mut query_count = 0;

    let filter = random_must_filter(&mut rng, 2);
    let cardinality = struct_index.estimate_cardinality(&filter);

    let indexed_fields = struct_index.indexed_fields();

    eprintln!("cardinality = {cardinality:#?}");
    eprintln!("indexed_fields = {indexed_fields:#?}");

    group.bench_function("struct-conditional-search-query-points", |b| {
        b.iter(|| {
            let filter = random_must_filter(&mut rng, 2);
            result_size += struct_index.query_points(&filter).len();
            query_count += 1;
        })
    });
    if query_count != 0 {
        eprintln!(
            "result_size / query_count = {:#?}",
            result_size / query_count
        );
    }

    let mut result_size = 0;
    let mut query_count = 0;

    group.bench_function("struct-conditional-search-context-check", |b| {
        b.iter(|| {
            let filter = random_must_filter(&mut rng, 2);
            let sample = (0..CHECK_SAMPLE_SIZE)
                .map(|_| rng.gen_range(0..NUM_POINTS) as PointOffsetType)
                .collect_vec();
            let context = struct_index.filter_context(&filter);

            let filtered_sample = sample
                .into_iter()
                .filter(|id| context.check(*id))
                .collect_vec();
            result_size += filtered_sample.len();
            query_count += 1;
        })
    });

    if query_count != 0 {
        eprintln!(
            "result_size / query_count = {:#?}",
            result_size / query_count
        );
    }

    group.finish();
}

#[cfg(not(target_os = "windows"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = conditional_struct_search_benchmark, conditional_plain_search_benchmark
}

#[cfg(target_os = "windows")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = conditional_struct_search_benchmark, conditional_plain_search_benchmark
}

criterion_main!(benches);
