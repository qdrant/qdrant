use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::budget::ResourcePermit;
use common::flags::FeatureFlags;
use common::types::{ScoredPointOffset, TelemetryDetail};
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use rstest::rstest;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, QueryVector, only_default_vector};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::payload_fixtures::{random_dense_byte_vector, random_int_payload};
use segment::fixtures::query_fixtures::QueryVariant;
use segment::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
use segment::index::{PayloadIndex, VectorIndex};
use segment::segment_constructor::build_segment;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{
    Condition, Distance, FieldCondition, Filter, HnswConfig, Indexes, Range, SearchParams,
    SegmentConfig, SeqNumberType, VectorDataConfig, VectorStorageDatatype, VectorStorageType,
};
use segment::vector_storage::VectorStorageEnum;
use tempfile::Builder;

fn random_query<R: Rng + ?Sized>(variant: &QueryVariant, rng: &mut R, dim: usize) -> QueryVector {
    segment::fixtures::query_fixtures::random_query(variant, rng, |rng| {
        random_dense_byte_vector(rng, dim).into()
    })
}

fn compare_search_result(result_a: &[Vec<ScoredPointOffset>], result_b: &[Vec<ScoredPointOffset>]) {
    assert_eq!(result_a.len(), result_b.len());
    for (a, b) in result_a.iter().zip(result_b) {
        assert_eq!(a.len(), b.len());
        for (a, b) in a.iter().zip(b) {
            assert_eq!(a.idx, b.idx);
            assert!((a.score - b.score).abs() < 1e-3);
        }
    }
}

#[rstest]
#[case::nearest(QueryVariant::Nearest, VectorStorageDatatype::Uint8, 32, 10)]
#[case::nearest(QueryVariant::Nearest, VectorStorageDatatype::Float16, 32, 10)]
#[case::discovery(QueryVariant::Discovery, VectorStorageDatatype::Uint8, 128, 20)]
#[case::reco_best_score(QueryVariant::RecoBestScore, VectorStorageDatatype::Float16, 64, 20)]
#[case::reco_sum_scores(QueryVariant::RecoSumScores, VectorStorageDatatype::Float16, 64, 20)]
fn test_byte_storage_hnsw(
    #[case] query_variant: QueryVariant,
    #[case] storage_data_type: VectorStorageDatatype,
    #[case] ef: usize,
    #[case] max_failures: usize, // out of 100
) {
    use common::counter::hardware_counter::HardwareCounterCell;
    use segment::json_path::JsonPath;
    use segment::payload_json;
    use segment::segment_constructor::VectorIndexBuildArgs;
    use segment::types::{HnswGlobalConfig, PayloadSchemaType};

    let stopped = AtomicBool::new(false);

    let dim = 8;
    let m = 8;
    let num_vectors: u64 = 5_000;
    let ef_construct = 16;
    let distance = Distance::Cosine;
    let full_scan_threshold = 0;
    let num_payload_values = 2;

    let mut rng = StdRng::seed_from_u64(42);

    let dir_float = Builder::new()
        .prefix("segment_dir_float")
        .tempdir()
        .unwrap();
    let dir_byte = Builder::new().prefix("segment_dir_byte").tempdir().unwrap();
    let hnsw_dir_byte = Builder::new().prefix("hnsw_dir_byte").tempdir().unwrap();

    let config_byte = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: dim,
                distance,
                storage_type: VectorStorageType::default(),
                index: Indexes::Plain {},
                quantization_config: None,
                multivector_config: None,
                datatype: Some(storage_data_type),
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };

    let int_key = "int";

    let mut segment_float = build_simple_segment(dir_float.path(), dim, distance).unwrap();
    let mut segment_byte = build_segment(dir_byte.path(), &config_byte, true).unwrap();
    // check that `segment_byte` uses byte or half storage
    {
        let borrowed_storage = segment_byte.vector_data[DEFAULT_VECTOR_NAME]
            .vector_storage
            .borrow();
        let raw_storage: &VectorStorageEnum = &borrowed_storage;
        #[cfg(feature = "rocksdb")]
        assert!(matches!(
            raw_storage,
            &VectorStorageEnum::DenseSimpleByte(_) | &VectorStorageEnum::DenseSimpleHalf(_),
        ));
        #[cfg(not(feature = "rocksdb"))]
        assert!(matches!(
            raw_storage,
            &VectorStorageEnum::DenseAppendableInRamByte(_)
                | &VectorStorageEnum::DenseAppendableInRamHalf(_),
        ));
    }

    for n in 0..num_vectors {
        let idx = n.into();
        let vector = random_dense_byte_vector(&mut rng, dim);

        let int_payload = random_int_payload(&mut rng, num_payload_values..=num_payload_values);
        let payload = payload_json! {int_key: int_payload};

        let hw_counter = HardwareCounterCell::new();

        segment_float
            .upsert_point(
                n as SeqNumberType,
                idx,
                only_default_vector(&vector),
                &hw_counter,
            )
            .unwrap();
        segment_float
            .set_full_payload(n as SeqNumberType, idx, &payload, &hw_counter)
            .unwrap();
        segment_byte
            .upsert_point(
                n as SeqNumberType,
                idx,
                only_default_vector(&vector),
                &hw_counter,
            )
            .unwrap();
        segment_byte
            .set_full_payload(n as SeqNumberType, idx, &payload, &hw_counter)
            .unwrap();
    }

    let hw_counter = HardwareCounterCell::new();

    segment_float
        .payload_index
        .borrow_mut()
        .set_indexed(
            &JsonPath::new(int_key),
            PayloadSchemaType::Integer,
            &hw_counter,
        )
        .unwrap();
    segment_byte
        .payload_index
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
    let hnsw_index_byte = HNSWIndex::build(
        HnswIndexOpenArgs {
            path: hnsw_dir_byte.path(),
            id_tracker: segment_byte.id_tracker.clone(),
            vector_storage: segment_byte.vector_data[DEFAULT_VECTOR_NAME]
                .vector_storage
                .clone(),
            quantized_vectors: segment_byte.vector_data[DEFAULT_VECTOR_NAME]
                .quantized_vectors
                .clone(),
            payload_index: segment_byte.payload_index.clone(),
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
        let query = random_query(&query_variant, &mut rng, dim);

        let range_size = 40;
        let left_range = rng.random_range(0..400);
        let right_range = left_range + range_size;

        let filter = Filter::new_must(Condition::Field(FieldCondition::new_range(
            JsonPath::new(int_key),
            Range {
                lt: None,
                gt: None,
                gte: Some(f64::from(left_range)),
                lte: Some(f64::from(right_range)),
            },
        )));

        let filter_query = Some(&filter);

        let index_result_byte = hnsw_index_byte
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
            hnsw_index_byte
                .get_telemetry_data(TelemetryDetail::default())
                .filtered_large_cardinality
                .count,
            i + 1
        );

        let plain_result_float = segment_float.vector_data[DEFAULT_VECTOR_NAME]
            .vector_index
            .borrow()
            .search(&[&query], filter_query, top, None, &Default::default())
            .unwrap();
        let plain_result_byte = segment_byte.vector_data[DEFAULT_VECTOR_NAME]
            .vector_index
            .borrow()
            .search(&[&query], filter_query, top, None, &Default::default())
            .unwrap();
        if storage_data_type == VectorStorageDatatype::Uint8 {
            compare_search_result(&plain_result_float, &plain_result_byte);
        }

        if plain_result_byte == index_result_byte {
            hits += 1;
        }
    }
    assert!(
        attempts - hits <= max_failures,
        "hits: {hits} of {attempts}"
    );
    eprintln!("hits = {hits:#?} out of {attempts}");
}
