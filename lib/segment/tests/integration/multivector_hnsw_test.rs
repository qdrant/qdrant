use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic_refcell::AtomicRefCell;
use common::budget::ResourcePermit;
use common::counter::hardware_counter::HardwareCounterCell;
use common::flags::FeatureFlags;
use rand::SeedableRng;
use rand::prelude::StdRng;
use segment::data_types::vectors::{
    DEFAULT_VECTOR_NAME, MultiDenseVectorInternal, QueryVector, TypedMultiDenseVectorRef,
    VectorElementType, VectorRef, only_default_vector,
};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::index_fixtures::random_vector;
use segment::fixtures::payload_fixtures::random_int_payload;
use segment::index::VectorIndex;
use segment::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment_constructor::VectorIndexBuildArgs;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{
    Condition, Distance, FieldCondition, Filter, HnswConfig, HnswGlobalConfig, MultiVectorConfig,
    PayloadSchemaType, SeqNumberType,
};
use segment::vector_storage::VectorStorage;
use segment::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::open_appendable_in_ram_multi_vector_storage_full;
use tempfile::Builder;

#[test]
fn test_single_multi_and_dense_hnsw_equivalency() {
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

    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let mut multi_storage = open_appendable_in_ram_multi_vector_storage_full(
        dir.path(),
        dim,
        distance,
        MultiVectorConfig::default(),
    )
    .unwrap();

    for n in 0..num_vectors {
        let idx = n.into();
        let vector = random_vector(&mut rng, dim);
        let preprocessed_vector = distance.preprocess_vector::<VectorElementType>(vector.clone());
        let vector_multi = MultiDenseVectorInternal::new(preprocessed_vector, vector.len());

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

        let internal_id = segment.id_tracker.borrow().internal_id(idx).unwrap();
        multi_storage
            .insert_vector(
                internal_id,
                VectorRef::MultiDense(TypedMultiDenseVectorRef::from(&vector_multi)),
                &hw_counter,
            )
            .unwrap();
    }

    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();

    let stopped = AtomicBool::new(false);

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

    // single threaded mode to guarantee equivalency between single and multi hnsw
    let permit = Arc::new(ResourcePermit::dummy(1));

    let vector_storage = &segment.vector_data[DEFAULT_VECTOR_NAME].vector_storage;
    let quantized_vectors = &segment.vector_data[DEFAULT_VECTOR_NAME].quantized_vectors;
    let hnsw_index_dense = HNSWIndex::build(
        HnswIndexOpenArgs {
            path: hnsw_dir.path(),
            id_tracker: segment.id_tracker.clone(),
            vector_storage: vector_storage.clone(),
            quantized_vectors: quantized_vectors.clone(),
            payload_index: segment.payload_index.clone(),
            hnsw_config: hnsw_config.clone(),
        },
        VectorIndexBuildArgs {
            permit: permit.clone(),
            old_indices: &[],
            gpu_device: None,
            rng: &mut rng,
            stopped: &stopped,
            hnsw_global_config: &HnswGlobalConfig::default(),
            feature_flags: FeatureFlags::default(),
        },
    )
    .unwrap();

    let multi_storage = Arc::new(AtomicRefCell::new(multi_storage));

    let hnsw_index_multi = HNSWIndex::open(HnswIndexOpenArgs {
        path: hnsw_dir.path(),
        id_tracker: segment.id_tracker.clone(),
        vector_storage: multi_storage,
        quantized_vectors: quantized_vectors.clone(),
        payload_index: segment.payload_index.clone(),
        hnsw_config,
    })
    .unwrap();

    for _ in 0..10 {
        let random_vector = random_vector(&mut rng, dim);
        let query_vector = random_vector.clone().into();
        let query_vector_multi = QueryVector::Nearest(vec![random_vector].try_into().unwrap());

        let payload_value = random_int_payload(&mut rng, 1..=1).pop().unwrap();

        let filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
            JsonPath::new(int_key),
            payload_value.into(),
        )));

        let search_res_dense = hnsw_index_dense
            .search(
                &[&query_vector],
                Some(&filter),
                10,
                None,
                &Default::default(),
            )
            .unwrap();

        let search_res_multi = hnsw_index_multi
            .search(
                &[&query_vector_multi],
                Some(&filter),
                10,
                None,
                &Default::default(),
            )
            .unwrap();

        assert_eq!(search_res_dense, search_res_multi);
    }
}
