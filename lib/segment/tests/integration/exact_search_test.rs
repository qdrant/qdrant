use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use common::cpu::CpuPermit;
use common::types::PointOffsetType;
use itertools::Itertools;
use rand::{thread_rng, Rng};
use segment::data_types::vectors::{only_default_vector, DEFAULT_VECTOR_NAME};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::payload_fixtures::{random_int_payload, random_vector};
use segment::index::hnsw_index::graph_links::GraphLinksRam;
use segment::index::hnsw_index::hnsw::HNSWIndex;
use segment::index::hnsw_index::num_rayon_threads;
use segment::index::{PayloadIndex, VectorIndex};
use segment::segment_constructor::build_segment;
use segment::types::{
    Condition, Distance, FieldCondition, Filter, HnswConfig, Indexes, Payload, PayloadSchemaType,
    Range, SearchParams, SegmentConfig, SeqNumberType, VectorDataConfig, VectorStorageType,
};
use serde_json::json;
use tempfile::Builder;

use crate::utils::path;

#[test]
fn exact_search_test() {
    let stopped = AtomicBool::new(false);

    let dim = 8;
    let m = 8;
    let num_vectors: u64 = 5_000;
    let ef = 32;
    let ef_construct = 16;
    let distance = Distance::Cosine;
    let full_scan_threshold = 16; // KB
    let indexing_threshold = 500; // num vectors
    let num_payload_values = 2;

    let mut rnd = thread_rng();

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
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };

    let int_key = "int";

    let mut segment = build_segment(dir.path(), &config, true).unwrap();
    for n in 0..num_vectors {
        let idx = n.into();
        let vector = random_vector(&mut rnd, dim);

        let int_payload = random_int_payload(&mut rnd, num_payload_values..=num_payload_values);
        let payload: Payload = json!({int_key:int_payload,}).into();

        segment
            .upsert_point(n as SeqNumberType, idx, only_default_vector(&vector))
            .unwrap();
        segment
            .set_full_payload(n as SeqNumberType, idx, &payload)
            .unwrap();
    }
    // let opnum = num_vectors + 1;

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

    let mut hnsw_index = HNSWIndex::<GraphLinksRam>::open(
        hnsw_dir.path(),
        segment.id_tracker.clone(),
        segment.vector_data[DEFAULT_VECTOR_NAME]
            .vector_storage
            .clone(),
        segment.vector_data[DEFAULT_VECTOR_NAME]
            .quantized_vectors
            .clone(),
        payload_index_ptr.clone(),
        hnsw_config,
    )
    .unwrap();

    hnsw_index.build_index(permit.clone(), &stopped).unwrap();

    payload_index_ptr
        .borrow_mut()
        .set_indexed(&path(int_key), PayloadSchemaType::Integer.into())
        .unwrap();
    let borrowed_payload_index = payload_index_ptr.borrow();
    let blocks = borrowed_payload_index
        .payload_blocks(&path(int_key), indexing_threshold)
        .collect_vec();
    for block in blocks.iter() {
        assert!(
            block.condition.range.is_some(),
            "only range conditions should be generated for this type of payload"
        );
    }

    let mut coverage: HashMap<PointOffsetType, usize> = Default::default();
    for block in &blocks {
        let px = payload_index_ptr.borrow();
        let filter = Filter::new_must(Condition::Field(block.condition.clone()));
        let points = px.query_points(&filter);
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

    hnsw_index.build_index(permit, &stopped).unwrap();

    let top = 3;
    let attempts = 50;
    for _i in 0..attempts {
        let query = random_vector(&mut rnd, dim).into();

        let index_result = hnsw_index
            .search(
                &[&query],
                None,
                top,
                Some(&SearchParams {
                    hnsw_ef: Some(ef),
                    exact: true,
                    ..Default::default()
                }),
                &false.into(),
                &Default::default(),
            )
            .unwrap();
        let plain_result = segment.vector_data[DEFAULT_VECTOR_NAME]
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

        assert_eq!(
            index_result, plain_result,
            "Exact search is not equal to plain search"
        );

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
        let index_result = hnsw_index
            .search(
                &[&query],
                filter_query,
                top,
                Some(&SearchParams {
                    hnsw_ef: Some(ef),
                    exact: true,
                    ..Default::default()
                }),
                &false.into(),
                &Default::default(),
            )
            .unwrap();
        let plain_result = segment.vector_data[DEFAULT_VECTOR_NAME]
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

        assert_eq!(
            index_result, plain_result,
            "Exact search is not equal to plain search"
        );
    }
}
