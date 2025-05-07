#[cfg(not(target_os = "windows"))]
mod prof;

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use common::budget::ResourcePermit;
use common::counter::hardware_counter::HardwareCounterCell;
use common::flags::{FeatureFlags, feature_flags};
use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, Criterion, black_box, criterion_group, criterion_main};
use rand::rng;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, only_default_vector};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::index_fixtures::random_vector;
use segment::fixtures::payload_fixtures::random_int_payload;
use segment::index::hnsw_index::num_rayon_threads;
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{
    Condition, Distance, FieldCondition, Filter, HnswConfig, Indexes, PayloadSchemaType,
    PayloadStorageType, Range, SearchParams, SegmentConfig, SeqNumberType, VectorDataConfig,
    VectorStorageType,
};
use tempfile::Builder;

fn segment_filtered_search_benchmark_with_flags(
    group: &mut BenchmarkGroup<'_, WallTime>,
    test_prefix: &str,
    feature_flags: FeatureFlags,
) {
    let stopped = AtomicBool::new(false);

    let dim = 8;
    let m = 8;
    let num_vectors: u64 = 10_000;
    let ef = 32;
    let ef_construct = 16;
    let distance = Distance::Cosine;
    let num_payload_values = 100;

    let mut rnd = rng();

    let segment_fill_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let tmp_dir = Builder::new().prefix("tmp_dir").tempdir().unwrap();

    let int_key = "int";

    let hw_counter = HardwareCounterCell::new();
    let permit_cpu_count = num_rayon_threads(1);
    let permit = ResourcePermit::dummy(permit_cpu_count as u32);

    let mut segment = build_simple_segment(segment_fill_dir.path(), dim, distance).unwrap();
    for n in 0..num_vectors {
        let idx = n.into();
        let vector = random_vector(&mut rnd, dim);

        let int_payload = random_int_payload(&mut rnd, 0..=num_payload_values);
        let payload = payload_json! {int_key: int_payload};

        segment
            .upsert_point(
                n as SeqNumberType,
                idx,
                only_default_vector(&vector),
                &hw_counter,
            )
            .unwrap();
        segment
            .set_full_payload(n as SeqNumberType, idx, &payload, &hw_counter)
            .unwrap();
    }
    let opnum = num_vectors + 1;
    segment
        .create_field_index(
            opnum,
            &JsonPath::new(int_key),
            Some(&PayloadSchemaType::Integer.into()),
            &hw_counter,
        )
        .unwrap();

    let mut segment_builder = SegmentBuilder::new(
        segment_dir.path(),
        tmp_dir.path(),
        &SegmentConfig {
            vector_data: HashMap::from([(
                DEFAULT_VECTOR_NAME.to_owned(),
                VectorDataConfig {
                    size: dim,
                    distance,
                    storage_type: VectorStorageType::Memory,
                    index: Indexes::Hnsw(HnswConfig {
                        m,
                        ef_construct,
                        full_scan_threshold: 100,
                        max_indexing_threads: 1,
                        on_disk: Some(false),
                        payload_m: None,
                    }),
                    quantization_config: None,
                    multivector_config: None,
                    datatype: None,
                },
            )]),
            sparse_vector_data: Default::default(),
            payload_storage_type: PayloadStorageType::InMemory,
        },
    )
    .unwrap();
    segment_builder.set_feature_flags(feature_flags);

    // segment_builder.add_indexed_field(JsonPath::new(int_key), PayloadSchemaType::Integer.into());
    segment_builder.update(&[&segment], &stopped).unwrap();

    let segment = segment_builder
        .build(permit, &stopped, &hw_counter)
        .unwrap();

    let left_range = 1;
    let right_range = num_payload_values;
    let filter = Filter::new_must(Condition::Field(FieldCondition::new_range(
        JsonPath::new(int_key),
        Range {
            lt: None,
            gt: None,
            gte: Some(f64::from(left_range)),
            lte: Some(f64::from(right_range as i32)),
        },
    )));

    let top = 5;
    group.bench_function(format!("{test_prefix}-plain"), |b| {
        b.iter(|| {
            let query = random_vector(&mut rnd, dim).into();

            let plain_result = segment
                .search(
                    DEFAULT_VECTOR_NAME,
                    &query,
                    &Default::default(),
                    &Default::default(),
                    Some(&filter),
                    top,
                    Some(&SearchParams {
                        hnsw_ef: Some(ef),
                        exact: true,
                        ..Default::default()
                    }),
                )
                .unwrap();

            black_box(plain_result)
        })
    });

    group.bench_function(format!("{test_prefix}-hnsw"), |b| {
        b.iter(|| {
            let query = random_vector(&mut rnd, dim).into();

            let hnsw_result = segment
                .search(
                    DEFAULT_VECTOR_NAME,
                    &query,
                    &Default::default(),
                    &Default::default(),
                    Some(&filter),
                    top,
                    None,
                )
                .unwrap();

            black_box(hnsw_result)
        })
    });
}

fn segment_filtered_search_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("segment-filtered-search-asymptotic");

    let mut feature_flags = feature_flags();
    feature_flags.payload_index_skip_rocksdb = true;
    segment_filtered_search_benchmark_with_flags(
        &mut group,
        "segment-filtered-search-prefetched-mmap",
        feature_flags,
    );

    feature_flags.payload_index_skip_rocksdb = false;
    segment_filtered_search_benchmark_with_flags(
        &mut group,
        "segment-filtered-search-prefetched-immutable-ram",
        feature_flags,
    );

    group.finish();
}

#[cfg(not(target_os = "windows"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = segment_filtered_search_benchmark
}

#[cfg(target_os = "windows")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = hnsw_benchmark
}

criterion_main!(benches);
