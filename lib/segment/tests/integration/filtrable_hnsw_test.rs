use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::budget::ResourcePermit;
use common::counter::hardware_counter::HardwareCounterCell;
use common::flags::FeatureFlags;
use common::types::{PointOffsetType, TelemetryDetail};
use itertools::Itertools;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use rstest::rstest;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, QueryVector, only_default_vector};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::payload_fixtures::{random_int_payload, random_vector};
use segment::fixtures::query_fixtures::QueryVariant;
use segment::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
use segment::index::{PayloadIndex, VectorIndex};
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment_constructor::VectorIndexBuildArgs;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{
    Condition, Distance, FieldCondition, Filter, HnswConfig, HnswGlobalConfig, PayloadSchemaType,
    Range, SearchParams, SeqNumberType,
};
use tempfile::Builder;

fn random_query<R: Rng + ?Sized>(variant: &QueryVariant, rng: &mut R, dim: usize) -> QueryVector {
    segment::fixtures::query_fixtures::random_query(variant, rng, move |rng| {
        random_vector(rng, dim).into()
    })
}

#[rstest]
#[case::nearest(QueryVariant::Nearest, 32, 5)]
#[case::discovery(QueryVariant::Discovery, 128, 10)] // tests that check better precision are in `hnsw_discover_test.rs`
#[case::reco_best_score(QueryVariant::RecoBestScore, 64, 10)]
#[case::reco_sum_scores(QueryVariant::RecoSumScores, 64, 10)]
fn test_filterable_hnsw(
    #[case] query_variant: QueryVariant,
    #[case] ef: usize,
    #[case] max_failures: usize, // out of 100
) {
    _test_filterable_hnsw(query_variant, ef, max_failures);
}

fn _test_filterable_hnsw(
    query_variant: QueryVariant,
    ef: usize,
    max_failures: usize, // out of 100
) {
    let stopped = AtomicBool::new(false);

    let dim = 8;
    let m = 8;
    let num_vectors: u64 = 5_000;
    let ef_construct = 16;
    let distance = Distance::Cosine;
    let full_scan_threshold = 16; // KB
    let indexing_threshold = 500; // num vectors
    let num_payload_values = 2;

    let mut rng = StdRng::seed_from_u64(42);

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();

    let int_key = "int";

    let hw_counter = HardwareCounterCell::new();
    let mut segment = build_simple_segment(dir.path(), dim, distance).unwrap();
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

    let payload_index_ptr = segment.payload_index.clone();

    let hnsw_config = HnswConfig {
        m,
        ef_construct,
        full_scan_threshold,
        max_indexing_threads: 2,
        on_disk: Some(false),
        payload_m: None,
        inline_storage: None,
    };

    let vector_storage = &segment.vector_data[DEFAULT_VECTOR_NAME].vector_storage;
    let quantized_vectors = &segment.vector_data[DEFAULT_VECTOR_NAME].quantized_vectors;

    payload_index_ptr
        .borrow_mut()
        .set_indexed(
            &JsonPath::new(int_key),
            PayloadSchemaType::Integer,
            &hw_counter,
        )
        .unwrap();
    let borrowed_payload_index = payload_index_ptr.borrow();
    let blocks = borrowed_payload_index
        .payload_blocks(&JsonPath::new(int_key), indexing_threshold)
        .collect_vec();
    for block in blocks.iter() {
        assert!(
            block.condition.range.is_some(),
            "only range conditions should be generated for this type of payload"
        );
    }

    let mut coverage: HashMap<PointOffsetType, usize> = Default::default();
    let px = payload_index_ptr.borrow();
    for block in &blocks {
        let filter = Filter::new_must(Condition::Field(block.condition.clone()));
        let points = px.query_points(&filter, &hw_counter);
        for point in points {
            coverage.insert(point, coverage.get(&point).unwrap_or(&0) + 1);
        }
    }
    let expected_blocks = num_vectors as usize / indexing_threshold * 2;

    eprintln!("blocks.len() = {:#?}", blocks.len());
    assert!(
        (blocks.len() as i64 - expected_blocks as i64).abs() <= 3,
        "real number of payload blocks is too far from expected"
    );

    assert_eq!(
        coverage.len(),
        num_vectors as usize,
        "not all points are covered by payload blocks"
    );

    let permit_cpu_count = 1; // single-threaded for deterministic build
    let permit = Arc::new(ResourcePermit::dummy(permit_cpu_count as u32));
    let hnsw_index = HNSWIndex::build(
        HnswIndexOpenArgs {
            path: hnsw_dir.path(),
            id_tracker: segment.id_tracker.clone(),
            vector_storage: vector_storage.clone(),
            quantized_vectors: quantized_vectors.clone(),
            payload_index: payload_index_ptr.clone(),
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

        let plain_result = segment.vector_data[DEFAULT_VECTOR_NAME]
            .vector_index
            .borrow()
            .search(&[&query], filter_query, top, None, &Default::default())
            .unwrap();

        if plain_result == index_result {
            hits += 1;
        }
    }
    assert!(
        attempts - hits <= max_failures,
        "hits: {hits} of {attempts}"
    ); // Not more than X% failures
    eprintln!("hits = {hits:#?} out of {attempts}");
}
