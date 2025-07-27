#[cfg(not(target_os = "windows"))]
mod prof;

use std::hint::black_box;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use segment::index::field_index::null_index::mmap_null_index::MmapNullIndex;
use segment::index::field_index::{FieldIndexBuilderTrait, PayloadFieldIndex};
use segment::json_path::JsonPath;
use segment::types::FieldCondition;
use serde_json::Value;
use tempfile::Builder;

const NUM_POINTS: usize = 10_000; // Smaller for initial testing
const ADDITIONAL_POINTS: usize = 1_000;
// Removed unused FILTER_CHECKS constant

/// Benchmark for testing MmapNullIndex performance with buffered updates.
///
/// This benchmark measures:
/// 1. Basic filtering performance after flushing data to disk
/// 2. Performance impact of buffered updates on filtering operations
/// 3. Cost of flushing buffered updates to disk
/// 4. Direct method call performance (values_is_empty, values_is_null)
///
/// Two main scenarios:
/// - mmap_null_index_benchmark: Tests baseline performance with flushed data
/// - buffered_updates_benchmark: Tests performance impact of having buffered updates
fn create_is_empty_filter(is_empty: bool) -> FieldCondition {
    FieldCondition::new_is_empty(JsonPath::new("test_field"), is_empty)
}

fn create_is_null_filter(is_null: bool) -> FieldCondition {
    FieldCondition::new_is_null(JsonPath::new("test_field"), is_null)
}

fn mmap_null_index_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let mut group = c.benchmark_group("mmap-null-index");

    let hw_counter = HardwareCounterCell::new();

    // Scenario 1: Basic functionality test
    let dir = Builder::new().prefix("null_index_test").tempdir().unwrap();
    let mut builder = MmapNullIndex::builder(dir.path()).unwrap();
    builder.init().unwrap();

    // Insert some test data
    for point_id in 0..NUM_POINTS {
        let has_value = rng.random_bool(0.7); // 70% chance of having a value
        let value = if has_value {
            Value::Number(42.into())
        } else {
            Value::Null
        };
        let payload = [&value];
        builder
            .add_point(point_id as PointOffsetType, &payload, &hw_counter)
            .unwrap();
    }

    let index = builder.finalize().unwrap();
    index.flusher()().unwrap();

    // Simple benchmark: filter for non-empty values
    let has_values_filter = create_is_empty_filter(false);
    group.bench_function("filter-has-values", |b| {
        b.iter(|| {
            let results: Vec<_> = index
                .filter(&has_values_filter, &hw_counter)
                .unwrap()
                .take(10)
                .collect();
            black_box(results);
        });
    });

    // Simple benchmark: filter for null values
    let is_null_filter = create_is_null_filter(true);
    group.bench_function("filter-is-null", |b| {
        b.iter(|| {
            let results: Vec<_> = index
                .filter(&is_null_filter, &hw_counter)
                .unwrap()
                .take(10)
                .collect();
            black_box(results);
        });
    });

    // Simple benchmark: direct method calls
    group.bench_function("values-is-empty-check", |b| {
        b.iter(|| {
            for i in 0..10 {
                let point_id = (i * NUM_POINTS / 10) as PointOffsetType;
                black_box(index.values_is_empty(point_id));
            }
        });
    });

    group.bench_function("values-is-null-check", |b| {
        b.iter(|| {
            for i in 0..10 {
                let point_id = (i * NUM_POINTS / 10) as PointOffsetType;
                black_box(index.values_is_null(point_id));
            }
        });
    });

    group.finish();
}

fn buffered_updates_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let mut group = c.benchmark_group("mmap-null-index-buffered");

    let hw_counter = HardwareCounterCell::new();

    // Create index with initial data, flush, then add more data (buffered)
    let dir = Builder::new()
        .prefix("null_index_buffered")
        .tempdir()
        .unwrap();
    let mut builder = MmapNullIndex::builder(dir.path()).unwrap();
    builder.init().unwrap();

    // Initial data
    for point_id in 0..NUM_POINTS {
        let has_value = rng.random_bool(0.7);
        let value = if has_value {
            Value::Number(42.into())
        } else {
            Value::Null
        };
        let payload = [&value];
        builder
            .add_point(point_id as PointOffsetType, &payload, &hw_counter)
            .unwrap();
    }

    let mut index = builder.finalize().unwrap();
    index.flusher()().unwrap(); // Flush to disk

    // Add buffered data
    for point_id in NUM_POINTS..(NUM_POINTS + ADDITIONAL_POINTS) {
        let has_value = rng.random_bool(0.7);
        let value = if has_value {
            Value::Number(42.into())
        } else {
            Value::Null
        };
        let payload = [&value];
        index
            .add_point(point_id as PointOffsetType, &payload, &hw_counter)
            .unwrap();
    }

    // Benchmark filtering with buffered data
    let has_values_filter = create_is_empty_filter(false);
    group.bench_function("filter-with-buffered-data", |b| {
        b.iter(|| {
            let results: Vec<_> = index
                .filter(&has_values_filter, &hw_counter)
                .unwrap()
                .take(10)
                .collect();
            black_box(results);
        });
    });

    // Benchmark flush operation
    group.bench_function("flush-buffered-updates", |b| {
        b.iter(|| {
            // Add a few points and flush
            for i in 0..10 {
                let point_id = (NUM_POINTS + ADDITIONAL_POINTS + i) as PointOffsetType;
                let value = Value::Number(i.into());
                let payload = [&value];
                index.add_point(point_id, &payload, &hw_counter).unwrap();
            }
            let _ = black_box(index.flusher()().ok());
        });
    });

    group.finish();
}

#[cfg(not(target_os = "windows"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = mmap_null_index_benchmark, buffered_updates_benchmark
}

#[cfg(target_os = "windows")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = mmap_null_index_benchmark, buffered_updates_benchmark
}

criterion_main!(benches);
