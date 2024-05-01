use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use common::cpu::CpuPermit;
use common::types::{ScoredPointOffset, TelemetryDetail};
use itertools::Itertools;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use rstest::rstest;
use segment::data_types::vectors::{only_default_vector, QueryVector, DEFAULT_VECTOR_NAME};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::payload_fixtures::{random_dense_byte_vector, random_int_payload};
use segment::index::hnsw_index::graph_links::GraphLinksRam;
use segment::index::hnsw_index::hnsw::HNSWIndex;
use segment::index::{PayloadIndex, VectorIndex};
use segment::segment_constructor::build_segment;
use segment::types::{
    Condition, Distance, FieldCondition, Filter, HnswConfig, Indexes, Payload, Range, SearchParams,
    SegmentConfig, SeqNumberType, VectorDataConfig, VectorStorageDatatype, VectorStorageType,
};
use segment::vector_storage::query::context_query::ContextPair;
use segment::vector_storage::query::discovery_query::DiscoveryQuery;
use segment::vector_storage::query::reco_query::RecoQuery;
use segment::vector_storage::VectorStorageEnum;
use serde_json::json;
use tempfile::Builder;

use crate::utils::path;

const MAX_EXAMPLE_PAIRS: usize = 4;

enum QueryVariant {
    Nearest,
    RecommendBestScore,
    Discovery,
}

fn random_discovery_query<R: Rng + ?Sized>(rnd: &mut R, dim: usize) -> QueryVector {
    let num_pairs: usize = rnd.gen_range(1..MAX_EXAMPLE_PAIRS);

    let target = random_dense_byte_vector(rnd, dim).into();

    let pairs = (0..num_pairs)
        .map(|_| {
            let positive = random_dense_byte_vector(rnd, dim).into();
            let negative = random_dense_byte_vector(rnd, dim).into();
            ContextPair { positive, negative }
        })
        .collect_vec();

    DiscoveryQuery::new(target, pairs).into()
}

fn random_reco_query<R: Rng + ?Sized>(rnd: &mut R, dim: usize) -> QueryVector {
    let num_examples: usize = rnd.gen_range(1..MAX_EXAMPLE_PAIRS);

    let positive = (0..num_examples)
        .map(|_| random_dense_byte_vector(rnd, dim).into())
        .collect_vec();
    let negative = (0..num_examples)
        .map(|_| random_dense_byte_vector(rnd, dim).into())
        .collect_vec();

    RecoQuery::new(positive, negative).into()
}

fn random_query<R: Rng + ?Sized>(variant: &QueryVariant, rnd: &mut R, dim: usize) -> QueryVector {
    match variant {
        QueryVariant::Nearest => random_dense_byte_vector(rnd, dim).into(),
        QueryVariant::Discovery => random_discovery_query(rnd, dim),
        QueryVariant::RecommendBestScore => random_reco_query(rnd, dim),
    }
}

fn compare_search_result(result_a: &[Vec<ScoredPointOffset>], result_b: &[Vec<ScoredPointOffset>]) {
    assert_eq!(result_a.len(), result_b.len());
    for (a, b) in result_a.iter().zip(result_b) {
        assert_eq!(a.len(), b.len());
        for (a, b) in a.iter().zip(b) {
            assert_eq!(a.idx, b.idx);
            assert!((a.score - b.score).abs() < 1e-6);
        }
    }
}

#[rstest]
#[case::nearest(QueryVariant::Nearest, 32, 10)]
#[case::discovery(QueryVariant::Discovery, 128, 20)]
#[case::recommend(QueryVariant::RecommendBestScore, 64, 20)]
fn test_byte_storage_hnsw(
    #[case] query_variant: QueryVariant,
    #[case] ef: usize,
    #[case] max_failures: usize, // out of 100
) {
    use segment::index::hnsw_index::num_rayon_threads;
    use segment::types::PayloadSchemaType;

    let stopped = AtomicBool::new(false);

    let dim = 8;
    let m = 8;
    let num_vectors: u64 = 5_000;
    let ef_construct = 16;
    let distance = Distance::Cosine;
    let full_scan_threshold = 16; // KB
    let num_payload_values = 2;

    let mut rnd = StdRng::seed_from_u64(42);

    let dir_float = Builder::new()
        .prefix("segment_dir_float")
        .tempdir()
        .unwrap();
    let dir_byte = Builder::new().prefix("segment_dir_byte").tempdir().unwrap();
    let hnsw_dir_byte = Builder::new().prefix("hnsw_dir_byte").tempdir().unwrap();

    let config_float = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: dim,
                distance,
                storage_type: VectorStorageType::Memory,
                index: Indexes::Plain {},
                quantization_config: None,
                multi_vec_config: None,
                datatype: None,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };
    let config_byte = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: dim,
                distance,
                storage_type: VectorStorageType::Memory,
                index: Indexes::Plain {},
                quantization_config: None,
                multi_vec_config: None,
                datatype: Some(VectorStorageDatatype::Uint8),
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };

    let int_key = "int";

    let mut segment_float = build_segment(dir_float.path(), &config_float, true).unwrap();
    let mut segment_byte = build_segment(dir_byte.path(), &config_byte, true).unwrap();
    // check that `segment_byte` uses byte storage
    {
        let borrowed_storage = segment_byte.vector_data[DEFAULT_VECTOR_NAME]
            .vector_storage
            .borrow();
        let raw_storage: &VectorStorageEnum = &borrowed_storage;
        assert!(matches!(
            raw_storage,
            &VectorStorageEnum::DenseSimpleByte(_)
        ));
    }

    for n in 0..num_vectors {
        let idx = n.into();
        let vector = random_dense_byte_vector(&mut rnd, dim);

        let int_payload = random_int_payload(&mut rnd, num_payload_values..=num_payload_values);
        let payload: Payload = json!({int_key:int_payload,}).into();

        segment_float
            .upsert_point(n as SeqNumberType, idx, only_default_vector(&vector))
            .unwrap();
        segment_float
            .set_full_payload(n as SeqNumberType, idx, &payload)
            .unwrap();
        segment_byte
            .upsert_point(n as SeqNumberType, idx, only_default_vector(&vector))
            .unwrap();
        segment_byte
            .set_full_payload(n as SeqNumberType, idx, &payload)
            .unwrap();
    }

    segment_float
        .payload_index
        .borrow_mut()
        .set_indexed(&path(int_key), PayloadSchemaType::Integer.into())
        .unwrap();
    segment_byte
        .payload_index
        .borrow_mut()
        .set_indexed(&path(int_key), PayloadSchemaType::Integer.into())
        .unwrap();

    let hnsw_config = HnswConfig {
        m,
        ef_construct,
        full_scan_threshold,
        max_indexing_threads: 2,
        on_disk: Some(false),
        payload_m: None,
    };

    let permit_cpu_count = num_rayon_threads(hnsw_config.max_indexing_threads);
    let permit = Arc::new(CpuPermit::dummy(permit_cpu_count as u32));
    let mut hnsw_index_byte = HNSWIndex::<GraphLinksRam>::open(
        hnsw_dir_byte.path(),
        segment_byte.id_tracker.clone(),
        segment_byte.vector_data[DEFAULT_VECTOR_NAME]
            .vector_storage
            .clone(),
        segment_byte.vector_data[DEFAULT_VECTOR_NAME]
            .quantized_vectors
            .clone(),
        segment_byte.payload_index.clone(),
        hnsw_config,
    )
    .unwrap();

    hnsw_index_byte.build_index(permit, &stopped).unwrap();

    let top = 3;
    let mut hits = 0;
    let attempts = 100;
    for i in 0..attempts {
        let query = random_query(&query_variant, &mut rnd, dim);

        let range_size = 40;
        let left_range = rnd.gen_range(0..400);
        let right_range = left_range + range_size;

        let filter = Filter::new_must(Condition::Field(FieldCondition::new_range(
            path(int_key),
            Range {
                lt: None,
                gt: None,
                gte: Some(left_range as f64),
                lte: Some(right_range as f64),
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
            .search(
                &[&query],
                filter_query,
                top,
                None,
                &Default::default(),
            )
            .unwrap();
        let plain_result_byte = segment_byte.vector_data[DEFAULT_VECTOR_NAME]
            .vector_index
            .borrow()
            .search(
                &[&query],
                filter_query,
                top,
                None,
                &Default::default(),
            )
            .unwrap();
        compare_search_result(&plain_result_float, &plain_result_byte);

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
