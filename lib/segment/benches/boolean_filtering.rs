use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use segment::fixtures::payload_context_fixture::{
    create_payload_storage_fixture, create_plain_payload_index, create_struct_payload_index,
    FixtureIdTracker,
};
use segment::fixtures::payload_fixtures::BOOL_KEY;
use segment::index::struct_payload_index::StructPayloadIndex;
use segment::index::PayloadIndex;
use segment::types::{Condition, FieldCondition, Filter, Match, PayloadSchemaType, ValueVariants};
use tempfile::Builder;

mod prof;

const NUM_POINTS: usize = 100000;

fn random_bool_filter<R: Rng + ?Sized>(rng: &mut R) -> Filter {
    Filter::new_must(Condition::Field(FieldCondition::new_match(
        BOOL_KEY.parse().unwrap(),
        Match::new_value(ValueVariants::Bool(rng.gen_bool(0.5))),
    )))
}

pub fn plain_boolean_query_points(c: &mut Criterion) {
    let seed = 42;

    let mut rng = StdRng::seed_from_u64(seed);
    let mut group = c.benchmark_group("boolean-query-points");

    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let plain_index = create_plain_payload_index(dir.path(), NUM_POINTS, seed);

    let mut result_size = 0;
    let mut query_count = 0;

    group.bench_function("plain", |b| {
        b.iter(|| {
            let filter = random_bool_filter(&mut rng);
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
}

pub fn struct_boolean_query_points(c: &mut Criterion) {
    let seed = 42;

    let mut rng = StdRng::seed_from_u64(seed);

    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let struct_index = create_struct_payload_index(dir.path(), NUM_POINTS, seed);

    let mut group = c.benchmark_group("boolean-query-points");

    let mut result_size = 0;
    let mut query_count = 0;
    group.bench_function("binary-index", |b| {
        b.iter(|| {
            let filter = random_bool_filter(&mut rng);
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

    group.finish();
}

pub fn keyword_index_boolean_query_points(c: &mut Criterion) {
    let seed = 42;

    let mut rng = StdRng::seed_from_u64(seed);

    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let payload_storage = Arc::new(AtomicRefCell::new(
        create_payload_storage_fixture(NUM_POINTS, seed).into(),
    ));
    let id_tracker = Arc::new(AtomicRefCell::new(FixtureIdTracker::new(NUM_POINTS)));

    let mut index = StructPayloadIndex::open(
        payload_storage,
        id_tracker,
        std::collections::HashMap::new(),
        dir.path(),
        true,
    )
    .unwrap();

    index
        .set_indexed(&BOOL_KEY.parse().unwrap(), PayloadSchemaType::Keyword)
        .unwrap();

    let mut group = c.benchmark_group("boolean-query-points");

    let mut result_size = 0;
    let mut query_count = 0;
    group.bench_function("keyword-index", |b| {
        b.iter(|| {
            let filter = random_bool_filter(&mut rng);
            result_size += index.query_points(&filter).len();
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
    targets = plain_boolean_query_points, struct_boolean_query_points, keyword_index_boolean_query_points
}

#[cfg(target_os = "windows")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = plain_boolean_query_points, struct_boolean_query_points, keyword_index_boolean_query_points
}

criterion_main!(benches);
