use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::budget::ResourcePermit;
use common::flags::FeatureFlags;
use common::types::TelemetryDetail;
use ordered_float::OrderedFloat;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use rstest::rstest;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, only_default_multi_vector};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::payload_fixtures::{random_int_payload, random_multi_vector};
use segment::fixtures::query_fixtures::{QueryVariant, random_multi_vec_query};
use segment::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
use segment::index::{PayloadIndex, VectorIndex};
use segment::segment_constructor::build_segment;
use segment::types::{
    Condition, Distance, FieldCondition, Filter, HnswConfig, Indexes, MultiVectorConfig,
    PayloadSchemaType, Range, SearchParams, SegmentConfig, SeqNumberType, VectorDataConfig,
    VectorStorageType,
};
use segment::vector_storage::VectorStorage;
use tempfile::Builder;

/// Check all cases with single vector per multi and several vectors per multi
#[rstest]
#[case::nearest_eq(QueryVariant::Nearest, 1, 32, 5)]
#[case::nearest_multi(QueryVariant::Nearest, 3, 64, 20)]
#[case::discovery_eq(QueryVariant::Discovery, 1, 128, 5)]
#[case::discovery_multi(QueryVariant::Discovery, 3, 128, 20)]
#[case::recobestscore_eq(QueryVariant::RecoBestScore, 1, 64, 5)]
#[case::recobestscore_multi(QueryVariant::RecoBestScore, 2, 64, 10)]
#[case::recosumscores_eq(QueryVariant::RecoSumScores, 1, 64, 5)]
#[case::recosumscores_multi(QueryVariant::RecoSumScores, 2, 64, 10)]
fn test_multi_filterable_hnsw(
    #[case] query_variant: QueryVariant,
    #[case] max_num_vector_per_points: usize,
    #[case] ef: usize,
    #[case] max_failures: usize, // out of 100
) {
    use common::counter::hardware_counter::HardwareCounterCell;
    use segment::json_path::JsonPath;
    use segment::payload_json;
    use segment::segment_constructor::VectorIndexBuildArgs;
    use segment::types::HnswGlobalConfig;

    let stopped = AtomicBool::new(false);

    let vector_dim = 8;
    let m = 8;
    let num_points: u64 = 5_000;
    let ef_construct = 16;
    let distance = Distance::Cosine;
    let full_scan_threshold = 8; // KB
    let num_payload_values = 2;

    let mut rng = StdRng::seed_from_u64(42);

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();

    let config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: vector_dim,
                distance,
                storage_type: VectorStorageType::default(),
                index: Indexes::Plain {}, // uses plain index for comparison
                quantization_config: None,
                multivector_config: Some(MultiVectorConfig::default()), // uses multivec config
                datatype: None,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };

    let int_key = "int";

    let hw_counter = HardwareCounterCell::new();

    let mut segment = build_segment(dir.path(), &config, true).unwrap();
    for n in 0..num_points {
        let idx = n.into();
        // Random number of vectors per multivec point
        let num_vector_for_point = rng.random_range(1..=max_num_vector_per_points);
        let multi_vec = random_multi_vector(&mut rng, vector_dim, num_vector_for_point);

        let int_payload = random_int_payload(&mut rng, num_payload_values..=num_payload_values);
        let payload = payload_json! {int_key: int_payload};

        let named_vectors = only_default_multi_vector(&multi_vec);
        segment
            .upsert_point(n as SeqNumberType, idx, named_vectors, &hw_counter)
            .unwrap();
        segment
            .set_full_payload(n as SeqNumberType, idx, &payload, &hw_counter)
            .unwrap();
    }
    assert_eq!(
        segment.vector_data[DEFAULT_VECTOR_NAME]
            .vector_storage
            .borrow()
            .total_vector_count(),
        num_points as usize
    );

    let payload_index_ptr = segment.payload_index.clone();
    payload_index_ptr
        .borrow_mut()
        .set_indexed(
            &JsonPath::new(int_key),
            PayloadSchemaType::Integer,
            &hw_counter,
        )
        .unwrap();

    let hnsw_config = HnswConfig {
        m,
        ef_construct,
        full_scan_threshold,
        max_indexing_threads: 2,
        on_disk: Some(false),
        payload_m: None,
        inline_storage: None,
    };

    let permit_cpu_count = 1; // single-threaded for deterministic build
    let permit = Arc::new(ResourcePermit::dummy(permit_cpu_count as u32));

    let vector_storage = &segment.vector_data[DEFAULT_VECTOR_NAME].vector_storage;
    let quantized_vectors = &segment.vector_data[DEFAULT_VECTOR_NAME].quantized_vectors;
    let hnsw_index = HNSWIndex::build(
        HnswIndexOpenArgs {
            path: hnsw_dir.path(),
            id_tracker: segment.id_tracker.clone(),
            vector_storage: vector_storage.clone(),
            quantized_vectors: quantized_vectors.clone(),
            payload_index: payload_index_ptr,
            hnsw_config,
        },
        VectorIndexBuildArgs {
            permit,
            old_indices: &[],
            gpu_device: None,
            rng: &mut rng,
            stopped: &stopped,
            hnsw_global_config: &HnswGlobalConfig::default(),
            feature_flags: FeatureFlags::default(),
        },
    )
    .unwrap();

    let top = 3;
    let mut hits = 0;
    let attempts = 100;
    for i in 0..attempts {
        // Random number of vectors per multivec query
        let num_vector_for_query = rng.random_range(1..=max_num_vector_per_points);
        let query =
            random_multi_vec_query(&query_variant, &mut rng, vector_dim, num_vector_for_query);

        let range_size = 40;
        let left_range = rng.random_range(0..400);
        let right_range = left_range + range_size;

        let filter = Filter::new_must(Condition::Field(FieldCondition::new_range(
            JsonPath::new(int_key),
            Range {
                lt: None,
                gt: None,
                gte: Some(OrderedFloat(f64::from(left_range))),
                lte: Some(OrderedFloat(f64::from(right_range))),
            },
        )));

        let filter_query = Some(&filter);

        let index_result = hnsw_index
            .search(
                &[&query],
                filter_query,
                top,
                Some(&SearchParams {
                    hnsw_ef: Some(ef),
                    ..Default::default()
                }),
                &Default::default(),
            )
            .unwrap();

        // check that search was performed using HNSW index
        assert_eq!(
            hnsw_index
                .get_telemetry_data(TelemetryDetail::default())
                .filtered_large_cardinality
                .count,
            i + 1
        );

        // segment uses a plain index by configuration
        let plain_result = segment.vector_data[DEFAULT_VECTOR_NAME]
            .vector_index
            .borrow()
            .search(&[&query], filter_query, top, None, &Default::default())
            .unwrap();

        if plain_result == index_result {
            hits += 1;
        } else {
            eprintln!("Attempt {i}/{attempts}");
            eprintln!("Different results for query {query:?}");
            eprintln!("plain_result = {plain_result:#?}");
            eprintln!("index_result = {index_result:#?}");
        }
    }
    assert!(
        attempts - hits <= max_failures,
        "hits: {hits}/{attempts} (expected less than {max_failures} failures)"
    ); // Not more than X% failures
    eprintln!("hits = {hits:#?} out of {attempts}");
}
