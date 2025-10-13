use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::budget::ResourcePermit;
use common::counter::hardware_counter::HardwareCounterCell;
use common::flags::FeatureFlags;
use rand::SeedableRng;
use rand::prelude::StdRng;
use segment::data_types::query_context::QueryContext;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, only_default_vector};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::index_fixtures::random_vector;
use segment::fixtures::payload_fixtures::random_int_payload;
use segment::index::VectorIndex;
use segment::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
use segment::index::hnsw_index::num_rayon_threads;
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment_constructor::VectorIndexBuildArgs;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{
    Condition, Distance, FieldCondition, Filter, HnswConfig, HnswGlobalConfig, PayloadSchemaType,
    SeqNumberType, WithPayload,
};
use tempfile::Builder;

#[test]
fn test_batch_and_single_request_equivalency() {
    let num_vectors: u64 = 1_000;
    let distance = Distance::Cosine;
    let num_payload_values = 2;
    let dim = 8;

    let mut rng = StdRng::seed_from_u64(42);

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let int_key = "int";

    let mut segment = build_simple_segment(dir.path(), dim, distance).unwrap();

    let hw_counter = HardwareCounterCell::new();

    segment
        .create_field_index(
            0,
            &JsonPath::new(int_key),
            Some(&PayloadSchemaType::Integer.into()),
            &hw_counter,
        )
        .unwrap();

    for n in 0..num_vectors {
        let idx = n.into();
        let vector = random_vector(&mut rng, dim);

        let int_payload = random_int_payload(&mut rng, num_payload_values..=num_payload_values);
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

    for _ in 0..10 {
        let query_vector_1 = random_vector(&mut rng, dim).into();
        let query_vector_2 = random_vector(&mut rng, dim).into();

        let payload_value = random_int_payload(&mut rng, 1..=1).pop().unwrap();

        let filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
            JsonPath::new(int_key),
            payload_value.into(),
        )));

        let search_res_1 = segment
            .search(
                DEFAULT_VECTOR_NAME,
                &query_vector_1,
                &WithPayload::default(),
                &false.into(),
                Some(&filter),
                10,
                None,
            )
            .unwrap();

        let search_res_2 = segment
            .search(
                DEFAULT_VECTOR_NAME,
                &query_vector_2,
                &WithPayload::default(),
                &false.into(),
                Some(&filter),
                10,
                None,
            )
            .unwrap();

        let query_context = QueryContext::default();
        let segment_query_context = query_context.get_segment_query_context();

        let batch_res = segment
            .search_batch(
                DEFAULT_VECTOR_NAME,
                &[&query_vector_1, &query_vector_2],
                &WithPayload::default(),
                &false.into(),
                Some(&filter),
                10,
                None,
                &segment_query_context,
            )
            .unwrap();

        assert_eq!(search_res_1, batch_res[0]);
        assert_eq!(search_res_2, batch_res[1]);
    }

    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();

    let stopped = AtomicBool::new(false);

    let payload_index_ptr = segment.payload_index.clone();

    let m = 8;
    let ef_construct = 100;
    let full_scan_threshold = 10000;

    let hnsw_config = HnswConfig {
        m,
        ef_construct,
        full_scan_threshold,
        max_indexing_threads: 2,
        on_disk: Some(false),
        payload_m: None,
        inline_storage: None,
    };

    let permit_cpu_count = num_rayon_threads(hnsw_config.max_indexing_threads);
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

    for _ in 0..10 {
        let query_vector_1 = random_vector(&mut rng, dim).into();
        let query_vector_2 = random_vector(&mut rng, dim).into();

        let payload_value = random_int_payload(&mut rng, 1..=1).pop().unwrap();

        let filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
            JsonPath::new(int_key),
            payload_value.into(),
        )));

        let search_res_1 = hnsw_index
            .search(
                &[&query_vector_1],
                Some(&filter),
                10,
                None,
                &Default::default(),
            )
            .unwrap();

        let search_res_2 = hnsw_index
            .search(
                &[&query_vector_2],
                Some(&filter),
                10,
                None,
                &Default::default(),
            )
            .unwrap();

        let batch_res = hnsw_index
            .search(
                &[&query_vector_1, &query_vector_2],
                Some(&filter),
                10,
                None,
                &Default::default(),
            )
            .unwrap();

        assert_eq!(search_res_1[0], batch_res[0]);
        assert_eq!(search_res_2[0], batch_res[1]);
    }
}
