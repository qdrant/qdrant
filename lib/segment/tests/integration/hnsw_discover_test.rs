use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use common::cpu::CpuPermit;
use itertools::Itertools;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use segment::data_types::vectors::{only_default_vector, QueryVector, DEFAULT_VECTOR_NAME};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::payload_fixtures::random_vector;
use segment::index::hnsw_index::graph_links::GraphLinksRam;
use segment::index::hnsw_index::hnsw::HNSWIndex;
use segment::index::hnsw_index::num_rayon_threads;
use segment::index::{PayloadIndex, VectorIndex};
use segment::segment_constructor::build_segment;
use segment::types::{
    Condition, Distance, FieldCondition, Filter, HnswConfig, Indexes, Payload, PayloadSchemaType,
    SearchParams, SegmentConfig, SeqNumberType, VectorDataConfig, VectorStorageType,
};
use segment::vector_storage::query::context_query::ContextPair;
use segment::vector_storage::query::discovery_query::DiscoveryQuery;
use serde_json::json;
use tempfile::Builder;

use crate::utils::path;

const MAX_EXAMPLE_PAIRS: usize = 3;

fn random_discovery_query<R: Rng + ?Sized>(rnd: &mut R, dim: usize) -> QueryVector {
    let num_pairs: usize = rnd.gen_range(1..MAX_EXAMPLE_PAIRS);

    let target = random_vector(rnd, dim).into();

    let pairs = (0..num_pairs)
        .map(|_| {
            let positive = random_vector(rnd, dim).into();
            let negative = random_vector(rnd, dim).into();
            ContextPair { positive, negative }
        })
        .collect_vec();

    DiscoveryQuery::new(target, pairs).into()
}

fn get_random_keyword_of<R: Rng + ?Sized>(num_options: usize, rnd: &mut R) -> String {
    let random_number = rnd.gen_range(0..num_options);
    format!("keyword_{}", random_number)
}

/// Checks discovery search precision when using hnsw index, this is different from the tests in
/// `filtrable_hnsw_test.rs` because it sets higher `m` and `ef_construct` parameters to get better precision
#[test]
fn hnsw_discover_precision() {
    let stopped = AtomicBool::new(false);

    let max_failures = 5; // out of 100
    let dim = 8;
    let m = 16;
    let num_vectors: u64 = 5_000;
    let ef = 32;
    let ef_construct = 64;
    let distance = Distance::Cosine;
    let full_scan_threshold = 16; // KB

    let mut rnd = StdRng::seed_from_u64(42);

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();

    let config = SegmentConfig {
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
        payload_storage_type: Default::default(),
        sparse_vector_data: Default::default(),
    };

    let mut segment = build_segment(dir.path(), &config, true).unwrap();

    for n in 0..num_vectors {
        let idx = n.into();
        let vector = random_vector(&mut rnd, dim);

        segment
            .upsert_point(n as SeqNumberType, idx, only_default_vector(&vector))
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
    };

    let permit_cpu_count = num_rayon_threads(hnsw_config.max_indexing_threads);
    let permit = Arc::new(CpuPermit::dummy(permit_cpu_count as u32));

    let vector_storage = &segment.vector_data[DEFAULT_VECTOR_NAME].vector_storage;
    let quantized_vectors = &segment.vector_data[DEFAULT_VECTOR_NAME].quantized_vectors;
    let mut hnsw_index = HNSWIndex::<GraphLinksRam>::open(
        hnsw_dir.path(),
        segment.id_tracker.clone(),
        vector_storage.clone(),
        quantized_vectors.clone(),
        payload_index_ptr.clone(),
        hnsw_config,
    )
    .unwrap();

    hnsw_index.build_index(permit, &stopped).unwrap();

    let top = 3;
    let mut discovery_hits = 0;
    let attempts = 100;
    for _i in 0..attempts {
        let query: QueryVector = random_discovery_query(&mut rnd, dim);

        let index_discovery_result = hnsw_index
            .search(
                &[&query],
                None,
                top,
                Some(&SearchParams {
                    hnsw_ef: Some(ef),
                    ..Default::default()
                }),
                &false.into(),
                &Default::default(),
            )
            .unwrap();

        let plain_discovery_result = segment.vector_data[DEFAULT_VECTOR_NAME]
            .vector_index
            .borrow()
            .search(
                &[&query],
                None,
                top,
                None,
                &false.into(),
                &Default::default(),
            )
            .unwrap();

        if plain_discovery_result == index_discovery_result {
            discovery_hits += 1;
        }
    }
    eprintln!("discovery_hits = {discovery_hits:#?} out of {attempts}");
    assert!(
        attempts - discovery_hits <= max_failures,
        "hits: {discovery_hits} of {attempts}"
    ); // Not more than X% failures
}

/// Same test as above but with payload index and filtering
#[test]
fn filtered_hnsw_discover_precision() {
    let stopped = AtomicBool::new(false);

    let max_failures = 5; // out of 100
    let dim = 8;
    let m = 16;
    let num_vectors: u64 = 5_000;
    let ef = 64;
    let ef_construct = 64;
    let distance = Distance::Cosine;
    let full_scan_threshold = 16; // KB
    let num_payload_values = 4;

    let mut rnd = StdRng::seed_from_u64(42);

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();

    let config = SegmentConfig {
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
        payload_storage_type: Default::default(),
        sparse_vector_data: Default::default(),
    };

    let keyword_key = "keyword";

    let mut segment = build_segment(dir.path(), &config, true).unwrap();
    for n in 0..num_vectors {
        let idx = n.into();
        let vector = random_vector(&mut rnd, dim);

        let keyword_payload = get_random_keyword_of(num_payload_values, &mut rnd);
        let payload: Payload = json!({keyword_key:keyword_payload,}).into();

        segment
            .upsert_point(n as SeqNumberType, idx, only_default_vector(&vector))
            .unwrap();
        segment
            .set_full_payload(n as SeqNumberType, idx, &payload)
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
    };

    let permit_cpu_count = num_rayon_threads(hnsw_config.max_indexing_threads);
    let permit = Arc::new(CpuPermit::dummy(permit_cpu_count as u32));

    let vector_storage = &segment.vector_data[DEFAULT_VECTOR_NAME].vector_storage;
    let quantized_vectors = &segment.vector_data[DEFAULT_VECTOR_NAME].quantized_vectors;
    let mut hnsw_index = HNSWIndex::<GraphLinksRam>::open(
        hnsw_dir.path(),
        segment.id_tracker.clone(),
        vector_storage.clone(),
        quantized_vectors.clone(),
        payload_index_ptr.clone(),
        hnsw_config,
    )
    .unwrap();

    payload_index_ptr
        .borrow_mut()
        .set_indexed(&path(keyword_key), PayloadSchemaType::Keyword.into())
        .unwrap();

    hnsw_index.build_index(permit, &stopped).unwrap();

    let top = 3;
    let mut discovery_hits = 0;
    let attempts = 100;
    for _i in 0..attempts {
        let filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
            path(keyword_key),
            get_random_keyword_of(num_payload_values, &mut rnd).into(),
        )));

        let filter_query = Some(&filter);

        let query: QueryVector = random_discovery_query(&mut rnd, dim);

        let index_discovery_result = hnsw_index
            .search(
                &[&query],
                filter_query,
                top,
                Some(&SearchParams {
                    hnsw_ef: Some(ef),
                    ..Default::default()
                }),
                &false.into(),
                &Default::default(),
            )
            .unwrap();

        let plain_discovery_result = segment.vector_data[DEFAULT_VECTOR_NAME]
            .vector_index
            .borrow()
            .search(
                &[&query],
                filter_query,
                top,
                None,
                &false.into(),
                &Default::default(),
            )
            .unwrap();

        if plain_discovery_result == index_discovery_result {
            discovery_hits += 1;
        }
    }

    eprintln!("discovery_hits = {discovery_hits:#?} out of {attempts}");
    assert!(
        attempts - discovery_hits <= max_failures,
        "hits: {discovery_hits} of {attempts}"
    ); // Not more than X% failures
}
