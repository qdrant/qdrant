#[cfg(not(target_os = "windows"))]
mod prof;

use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use ordered_float::OrderedFloat;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use segment::fixtures::payload_context_fixture::FixtureIdTracker;
use segment::fixtures::payload_fixtures::{FLT_KEY, INT_KEY};
use segment::index::PayloadIndex;
use segment::index::struct_payload_index::StructPayloadIndex;
use segment::payload_json;
use segment::payload_storage::PayloadStorage;
use segment::payload_storage::in_memory_payload_storage::InMemoryPayloadStorage;
use segment::types::{
    Condition, FieldCondition, Filter, PayloadSchemaType, Range as RangeCondition,
};
use tempfile::Builder;

const NUM_POINTS: usize = 100_000;
const MAX_RANGE: f64 = 100_000.0;

fn random_range_filter<R: Rng + ?Sized>(rng: &mut R, key: &str) -> Filter {
    Filter::new_must(Condition::Field(FieldCondition::new_range(
        key.parse().unwrap(),
        RangeCondition {
            lt: None,
            gt: None,
            gte: Some(OrderedFloat(rng.random_range(0.0..MAX_RANGE / 2.0))),
            lte: Some(OrderedFloat(rng.random_range(MAX_RANGE / 2.0..MAX_RANGE))),
        },
    )))
}

fn range_filtering(c: &mut Criterion) {
    let mut group = c.benchmark_group("range-filtering-group");

    let seed = 42;

    let mut rng = StdRng::seed_from_u64(seed);

    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

    let hw_counter = HardwareCounterCell::new();

    // generate points with payload
    let mut payload_storage = InMemoryPayloadStorage::default();
    for id in 0..NUM_POINTS {
        let payload = payload_json! {
            INT_KEY: rng.random_range(0..MAX_RANGE.round() as usize),
            FLT_KEY: rng.random_range(0.0..MAX_RANGE),
        };
        payload_storage
            .set(id as PointOffsetType, &payload, &hw_counter)
            .unwrap();
    }

    let payload_storage = Arc::new(AtomicRefCell::new(payload_storage.into()));
    let id_tracker = Arc::new(AtomicRefCell::new(FixtureIdTracker::new(NUM_POINTS)));

    let mut index = StructPayloadIndex::open(
        payload_storage.clone(),
        id_tracker.clone(),
        std::collections::HashMap::new(),
        dir.path(),
        true,
        true,
    )
    .unwrap();

    // add numeric float index
    index
        .set_indexed(
            &FLT_KEY.parse().unwrap(),
            PayloadSchemaType::Float,
            &hw_counter,
        )
        .unwrap();

    // add numeric integer index
    index
        .set_indexed(
            &INT_KEY.parse().unwrap(),
            PayloadSchemaType::Integer,
            &hw_counter,
        )
        .unwrap();

    // make sure all points are indexed
    assert_eq!(index.indexed_points(&FLT_KEY.parse().unwrap()), NUM_POINTS);
    assert_eq!(index.indexed_points(&INT_KEY.parse().unwrap()), NUM_POINTS);

    let mut result_size = 0;
    let mut query_count = 0;

    group.bench_function("float-mutable-index", |b| {
        b.iter_batched(
            || random_range_filter(&mut rng, FLT_KEY),
            |filter| {
                result_size += index.query_points(&filter, &hw_counter).len();
                query_count += 1;
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("integer-mutable-index", |b| {
        b.iter_batched(
            || random_range_filter(&mut rng, INT_KEY),
            |filter| {
                result_size += index.query_points(&filter, &hw_counter).len();
                query_count += 1;
            },
            BatchSize::SmallInput,
        )
    });

    // flush data
    index.flusher()().unwrap();
    drop(index);

    // reload as IMMUTABLE index
    let index = StructPayloadIndex::open(
        payload_storage,
        id_tracker,
        std::collections::HashMap::new(),
        dir.path(),
        false,
        true,
    )
    .unwrap();

    group.bench_function("float-immutable-index", |b| {
        b.iter_batched(
            || random_range_filter(&mut rng, FLT_KEY),
            |filter| {
                result_size += index.query_points(&filter, &hw_counter).len();
                query_count += 1;
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("integer-immutable-index", |b| {
        b.iter_batched(
            || random_range_filter(&mut rng, INT_KEY),
            |filter| {
                result_size += index.query_points(&filter, &hw_counter).len();
                query_count += 1;
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

#[cfg(not(target_os = "windows"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = range_filtering
}

criterion_main!(benches);
