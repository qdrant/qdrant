#[cfg(not(target_os = "windows"))]
mod prof;

use std::hint::black_box;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use segment::index::field_index::map_index::MapIndex;
use segment::index::field_index::map_index::mmap_map_index::MmapMapIndex;
use segment::index::field_index::{FieldIndexBuilderTrait, PayloadFieldIndex};
use segment::types::{FieldCondition, Match, MatchValue, ValueVariants};
use serde_json::Value;
use tempfile::Builder;

const NUM_POINTS: u32 = 100_000;
const NUM_UNIQUE_VALUES: usize = 10_000;
const VALUES_PER_POINT: usize = 3;

fn build_keyword_data(seed: u64) -> Vec<Vec<String>> {
    let mut rng = StdRng::seed_from_u64(seed);
    let unique_values: Vec<String> = (0..NUM_UNIQUE_VALUES)
        .map(|i| format!("keyword_{i}"))
        .collect();

    (0..NUM_POINTS)
        .map(|_| {
            (0..VALUES_PER_POINT)
                .map(|_| unique_values[rng.random_range(0..NUM_UNIQUE_VALUES)].clone())
                .collect()
        })
        .collect()
}

fn build_int_data(seed: u64) -> Vec<Vec<i64>> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..NUM_POINTS)
        .map(|_| {
            (0..VALUES_PER_POINT)
                .map(|_| rng.random_range(0..NUM_UNIQUE_VALUES as i64))
                .collect()
        })
        .collect()
}

fn build_and_save_keyword_index(path: &std::path::Path, data: &[Vec<String>]) {
    let hw_counter = HardwareCounterCell::new();
    let mut builder = MapIndex::<str>::builder_mmap(path, false);
    builder.init().unwrap();
    for (idx, values) in data.iter().enumerate() {
        let json_values: Vec<Value> = values.iter().map(|v| Value::String(v.clone())).collect();
        let refs: Vec<&Value> = json_values.iter().collect();
        builder
            .add_point(idx as PointOffsetType, &refs, &hw_counter)
            .unwrap();
    }
    builder.finalize().unwrap();
}

fn build_and_save_int_index(path: &std::path::Path, data: &[Vec<i64>]) {
    let hw_counter = HardwareCounterCell::new();
    let mut builder = MapIndex::<i64>::builder_mmap(path, false);
    builder.init().unwrap();
    for (idx, values) in data.iter().enumerate() {
        let json_values: Vec<Value> = values.iter().map(|v| (*v).into()).collect();
        let refs: Vec<&Value> = json_values.iter().collect();
        builder
            .add_point(idx as PointOffsetType, &refs, &hw_counter)
            .unwrap();
    }
    builder.finalize().unwrap();
}

/// Open as ImmutableMapIndex (RAM HashMap structures, backed by mmap storage)
fn open_immutable<N>(path: &std::path::Path) -> MapIndex<N>
where
    N: segment::index::field_index::map_index::MapIndexKey + ?Sized,
    Vec<<N as segment::index::field_index::map_index::MapIndexKey>::Owned>:
        gridstore::Blob + Send + Sync,
{
    MapIndex::<N>::new_mmap(path, false).unwrap().unwrap()
}

/// Open as MmapMapIndex with pages populated into RAM (mmap data structures, not HashMaps)
fn open_mmap_populated<N>(path: &std::path::Path) -> MapIndex<N>
where
    N: segment::index::field_index::map_index::MapIndexKey + ?Sized,
    Vec<<N as segment::index::field_index::map_index::MapIndexKey>::Owned>:
        gridstore::Blob + Send + Sync,
{
    let mmap_index = MmapMapIndex::<N>::open(path, false).unwrap().unwrap();
    mmap_index.populate().unwrap();
    MapIndex::Mmap(Box::new(mmap_index))
}

fn bench_get_values_keyword(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_index/get_values/keyword");
    let data = build_keyword_data(42);
    let mut rng = StdRng::seed_from_u64(123);
    let query_points: Vec<PointOffsetType> =
        (0..1000).map(|_| rng.random_range(0..NUM_POINTS)).collect();

    let tmp = Builder::new().prefix("immutable_kw").tempdir().unwrap();
    build_and_save_keyword_index(tmp.path(), &data);

    let immutable_index = open_immutable::<str>(tmp.path());
    group.bench_function("immutable", |b| {
        let mut i = 0;
        b.iter(|| {
            let idx = query_points[i % query_points.len()];
            if let Some(vals) = immutable_index.get_values(idx) {
                for v in vals {
                    black_box(v);
                }
            }
            i += 1;
        });
    });
    drop(immutable_index);

    let mmap_index = open_mmap_populated::<str>(tmp.path());
    group.bench_function("mmap_populated", |b| {
        let mut i = 0;
        b.iter(|| {
            let idx = query_points[i % query_points.len()];
            if let Some(vals) = mmap_index.get_values(idx) {
                for v in vals {
                    black_box(v);
                }
            }
            i += 1;
        });
    });
    drop(mmap_index);

    group.finish();
}

fn bench_check_values_any_keyword(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_index/check_values_any/keyword");
    let data = build_keyword_data(42);
    let mut rng = StdRng::seed_from_u64(789);
    let query_points: Vec<PointOffsetType> =
        (0..1000).map(|_| rng.random_range(0..NUM_POINTS)).collect();
    let hw = HardwareCounterCell::new();

    let tmp = Builder::new().prefix("check_kw").tempdir().unwrap();
    build_and_save_keyword_index(tmp.path(), &data);

    let target = "keyword_42";
    let immutable_index = open_immutable::<str>(tmp.path());
    group.bench_function("immutable", |b| {
        let mut i = 0;
        b.iter(|| {
            let idx = query_points[i % query_points.len()];
            black_box(immutable_index.check_values_any(idx, &hw, |v| v == target));
            i += 1;
        });
    });
    drop(immutable_index);

    let mmap_index = open_mmap_populated::<str>(tmp.path());
    group.bench_function("mmap_populated", |b| {
        let mut i = 0;
        b.iter(|| {
            let idx = query_points[i % query_points.len()];
            black_box(mmap_index.check_values_any(idx, &hw, |v| v == target));
            i += 1;
        });
    });
    drop(mmap_index);

    group.finish();
}

fn bench_values_count_keyword(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_index/values_count/keyword");
    let data = build_keyword_data(42);
    let mut rng = StdRng::seed_from_u64(654);
    let query_points: Vec<PointOffsetType> =
        (0..1000).map(|_| rng.random_range(0..NUM_POINTS)).collect();

    let tmp = Builder::new().prefix("valcnt_kw").tempdir().unwrap();
    build_and_save_keyword_index(tmp.path(), &data);

    let immutable_index = open_immutable::<str>(tmp.path());
    group.bench_function("immutable", |b| {
        let mut i = 0;
        b.iter(|| {
            let idx = query_points[i % query_points.len()];
            black_box(immutable_index.values_count(idx));
            i += 1;
        });
    });
    drop(immutable_index);

    let mmap_index = open_mmap_populated::<str>(tmp.path());
    group.bench_function("mmap_populated", |b| {
        let mut i = 0;
        b.iter(|| {
            let idx = query_points[i % query_points.len()];
            black_box(mmap_index.values_count(idx));
            i += 1;
        });
    });
    drop(mmap_index);

    group.finish();
}

fn bench_filter_keyword(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_index/filter/keyword");
    let data = build_keyword_data(42);
    let mut rng = StdRng::seed_from_u64(999);
    let query_values: Vec<String> = (0..1000)
        .map(|_| format!("keyword_{}", rng.random_range(0..NUM_UNIQUE_VALUES)))
        .collect();
    let hw = HardwareCounterCell::new();

    let tmp = Builder::new().prefix("filter_kw").tempdir().unwrap();
    build_and_save_keyword_index(tmp.path(), &data);

    let immutable_index = open_immutable::<str>(tmp.path());
    group.bench_function("immutable", |b| {
        let mut i = 0;
        b.iter(|| {
            let val = &query_values[i % query_values.len()];
            let condition = FieldCondition::new_match(
                "test".parse().unwrap(),
                Match::Value(MatchValue {
                    value: ValueVariants::String(val.clone()),
                }),
            );
            if let Ok(Some(iter)) = immutable_index.filter(&condition, &hw) {
                black_box(iter.count());
            }
            i += 1;
        });
    });
    drop(immutable_index);

    let mmap_index = open_mmap_populated::<str>(tmp.path());
    group.bench_function("mmap_populated", |b| {
        let mut i = 0;
        b.iter(|| {
            let val = &query_values[i % query_values.len()];
            let condition = FieldCondition::new_match(
                "test".parse().unwrap(),
                Match::Value(MatchValue {
                    value: ValueVariants::String(val.clone()),
                }),
            );
            if let Ok(Some(iter)) = mmap_index.filter(&condition, &hw) {
                black_box(iter.count());
            }
            i += 1;
        });
    });
    drop(mmap_index);

    group.finish();
}

fn bench_get_values_int(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_index/get_values/int");
    let data = build_int_data(42);
    let mut rng = StdRng::seed_from_u64(111);
    let query_points: Vec<PointOffsetType> =
        (0..1000).map(|_| rng.random_range(0..NUM_POINTS)).collect();

    let tmp = Builder::new().prefix("immutable_int").tempdir().unwrap();
    build_and_save_int_index(tmp.path(), &data);

    let immutable_index = open_immutable::<i64>(tmp.path());
    group.bench_function("immutable", |b| {
        let mut i = 0;
        b.iter(|| {
            let idx = query_points[i % query_points.len()];
            if let Some(vals) = immutable_index.get_values(idx) {
                for v in vals {
                    black_box(v);
                }
            }
            i += 1;
        });
    });
    drop(immutable_index);

    let mmap_index = open_mmap_populated::<i64>(tmp.path());
    group.bench_function("mmap_populated", |b| {
        let mut i = 0;
        b.iter(|| {
            let idx = query_points[i % query_points.len()];
            if let Some(vals) = mmap_index.get_values(idx) {
                for v in vals {
                    black_box(v);
                }
            }
            i += 1;
        });
    });
    drop(mmap_index);

    group.finish();
}

fn bench_filter_int(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_index/filter/int");
    let data = build_int_data(42);
    let mut rng = StdRng::seed_from_u64(222);
    let query_values: Vec<i64> = (0..1000)
        .map(|_| rng.random_range(0..NUM_UNIQUE_VALUES as i64))
        .collect();
    let hw = HardwareCounterCell::new();

    let tmp = Builder::new().prefix("filter_int").tempdir().unwrap();
    build_and_save_int_index(tmp.path(), &data);

    let immutable_index = open_immutable::<i64>(tmp.path());
    group.bench_function("immutable", |b| {
        let mut i = 0;
        b.iter(|| {
            let val = query_values[i % query_values.len()];
            let condition = FieldCondition::new_match(
                "test".parse().unwrap(),
                Match::Value(MatchValue {
                    value: ValueVariants::Integer(val),
                }),
            );
            if let Ok(Some(iter)) = immutable_index.filter(&condition, &hw) {
                black_box(iter.count());
            }
            i += 1;
        });
    });
    drop(immutable_index);

    let mmap_index = open_mmap_populated::<i64>(tmp.path());
    group.bench_function("mmap_populated", |b| {
        let mut i = 0;
        b.iter(|| {
            let val = query_values[i % query_values.len()];
            let condition = FieldCondition::new_match(
                "test".parse().unwrap(),
                Match::Value(MatchValue {
                    value: ValueVariants::Integer(val),
                }),
            );
            if let Ok(Some(iter)) = mmap_index.filter(&condition, &hw) {
                black_box(iter.count());
            }
            i += 1;
        });
    });
    drop(mmap_index);

    group.finish();
}

fn bench_iter_values_keyword(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_index/iter_values/keyword");
    let data = build_keyword_data(42);

    let tmp = Builder::new().prefix("iterval_kw").tempdir().unwrap();
    build_and_save_keyword_index(tmp.path(), &data);

    let immutable_index = open_immutable::<str>(tmp.path());
    group.bench_function("immutable", |b| {
        b.iter(|| {
            let count = immutable_index.iter_values().count();
            black_box(count);
        });
    });
    drop(immutable_index);

    let mmap_index = open_mmap_populated::<str>(tmp.path());
    group.bench_function("mmap_populated", |b| {
        b.iter(|| {
            let count = mmap_index.iter_values().count();
            black_box(count);
        });
    });
    drop(mmap_index);

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets =
        bench_get_values_keyword,
        bench_check_values_any_keyword,
        bench_values_count_keyword,
        bench_filter_keyword,
        bench_get_values_int,
        bench_filter_int,
        bench_iter_values_keyword,
}

criterion_main!(benches);
