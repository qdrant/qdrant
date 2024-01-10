use std::collections::{BTreeSet, HashMap};
use std::ops::Deref;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::types::{ScoreType, ScoredPointOffset};
use rand::rngs::StdRng;
use rand::SeedableRng;
use segment::data_types::vectors::{only_default_vector, QueryVector, DEFAULT_VECTOR_NAME};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::payload_fixtures::{random_vector, STR_KEY};
use segment::index::hnsw_index::graph_links::GraphLinksRam;
use segment::index::hnsw_index::hnsw::HNSWIndex;
use segment::index::{VectorIndex, VectorIndexEnum};
use segment::segment::Segment;
use segment::segment_constructor::build_segment;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::types::PayloadSchemaType::Keyword;
use segment::types::{
    CompressionRatio, Condition, Distance, FieldCondition, Filter, HnswConfig, Indexes, Payload,
    ProductQuantizationConfig, QuantizationConfig, QuantizationSearchParams,
    ScalarQuantizationConfig, SearchParams, SegmentConfig, VectorDataConfig, VectorStorageType,
};
use segment::vector;
use segment::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use serde_json::json;
use tempfile::Builder;

use crate::fixtures::segment::build_segment_1;

fn sames_count(a: &[Vec<ScoredPointOffset>], b: &[Vec<ScoredPointOffset>]) -> usize {
    a[0].iter()
        .map(|x| x.idx)
        .collect::<BTreeSet<_>>()
        .intersection(&b[0].iter().map(|x| x.idx).collect())
        .count()
}

fn hnsw_quantized_search_test(
    distance: Distance,
    num_vectors: u64,
    quantization_config: QuantizationConfig,
) {
    let stopped = AtomicBool::new(false);
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let quantized_data_path = dir.path();

    let payloads_count = 50;
    let dim = 131;
    let m = 16;
    let ef = 64;
    let ef_construct = 64;
    let top = 10;
    let attempts = 10;

    let mut rnd = StdRng::seed_from_u64(42);
    let mut op_num = 0;

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
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };

    let mut segment = build_segment(dir.path(), &config, true).unwrap();
    for n in 0..num_vectors {
        let idx = n.into();
        let vector = random_vector(&mut rnd, dim);
        segment
            .upsert_point(op_num, idx, only_default_vector(&vector))
            .unwrap();
        op_num += 1;
    }

    segment
        .create_field_index(op_num, STR_KEY, Some(&Keyword.into()))
        .unwrap();
    op_num += 1;
    for n in 0..payloads_count {
        let idx = n.into();
        let payload: Payload = json!(
            {
                STR_KEY: STR_KEY,
            }
        )
        .into();
        segment.set_full_payload(op_num, idx, &payload).unwrap();
        op_num += 1;
    }

    segment.vector_data.values_mut().for_each(|vector_storage| {
        let quantized_vectors = QuantizedVectors::create(
            &vector_storage.vector_storage.borrow(),
            &quantization_config,
            quantized_data_path,
            4,
            &stopped,
        )
        .unwrap();
        vector_storage.quantized_vectors = Arc::new(AtomicRefCell::new(Some(quantized_vectors)));
    });

    let hnsw_config = HnswConfig {
        m,
        ef_construct,
        full_scan_threshold: 2 * payloads_count as usize,
        max_indexing_threads: 2,
        on_disk: Some(false),
        payload_m: None,
    };

    let mut hnsw_index = HNSWIndex::<GraphLinksRam>::open(
        hnsw_dir.path(),
        segment.id_tracker.clone(),
        segment.vector_data[DEFAULT_VECTOR_NAME]
            .vector_storage
            .clone(),
        segment.vector_data[DEFAULT_VECTOR_NAME]
            .quantized_vectors
            .clone(),
        segment.payload_index.clone(),
        hnsw_config,
    )
    .unwrap();

    hnsw_index.build_index(&stopped).unwrap();

    let query_vectors = (0..attempts)
        .map(|_| random_vector(&mut rnd, dim).into())
        .collect::<Vec<_>>();
    let filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
        STR_KEY,
        STR_KEY.to_owned().into(),
    )));

    // check that quantized search is working
    // to check it, compare quantized search result with exact search result
    check_matches(&query_vectors, &segment, &hnsw_index, None, ef, top);
    check_matches(
        &query_vectors,
        &segment,
        &hnsw_index,
        Some(&filter),
        ef,
        top,
    );

    // check that oversampling is working
    // to check it, search with oversampling and check that results are not worse
    check_oversampling(&query_vectors, &hnsw_index, None, ef, top);
    check_oversampling(&query_vectors, &hnsw_index, Some(&filter), ef, top);

    // check that rescoring is working
    // to check it, set all vectors to zero and expect zero scores
    let zero_vector = vector![0.0; dim];
    for n in 0..num_vectors {
        let idx = n.into();
        segment
            .upsert_point(op_num, idx, only_default_vector(&zero_vector))
            .unwrap();
        op_num += 1;
    }
    check_rescoring(&query_vectors, &hnsw_index, None, ef, top);
    check_rescoring(&query_vectors, &hnsw_index, Some(&filter), ef, top);
}

fn check_matches(
    query_vectors: &[QueryVector],
    segment: &Segment,
    hnsw_index: &HNSWIndex<GraphLinksRam>,
    filter: Option<&Filter>,
    ef: usize,
    top: usize,
) {
    let exact_search_results = query_vectors
        .iter()
        .map(|query| {
            segment.vector_data[DEFAULT_VECTOR_NAME]
                .vector_index
                .borrow()
                .search(&[&query], filter, top, None, &false.into())
                .unwrap()
        })
        .collect::<Vec<_>>();

    let mut sames: usize = 0;
    let attempts = query_vectors.len();
    for (query, plain_result) in query_vectors.iter().zip(exact_search_results.iter()) {
        let index_result = hnsw_index
            .search(
                &[query],
                filter,
                top,
                Some(&SearchParams {
                    hnsw_ef: Some(ef),
                    ..Default::default()
                }),
                &false.into(),
            )
            .unwrap();
        sames += sames_count(&index_result, plain_result);
    }
    let acc = 100.0 * sames as f64 / (attempts * top) as f64;
    println!("sames = {sames}, attempts = {attempts}, top = {top}, acc = {acc}");
    assert!(acc > 40.0);
}

fn check_oversampling(
    query_vectors: &[QueryVector],
    hnsw_index: &HNSWIndex<GraphLinksRam>,
    filter: Option<&Filter>,
    ef: usize,
    top: usize,
) {
    for query in query_vectors {
        let ef_oversampling = ef / 8;

        let oversampling_1_result = hnsw_index
            .search(
                &[query],
                filter,
                top,
                Some(&SearchParams {
                    hnsw_ef: Some(ef_oversampling),
                    quantization: Some(QuantizationSearchParams {
                        rescore: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                &false.into(),
            )
            .unwrap();
        let best_1 = oversampling_1_result[0][0];
        let worst_1 = oversampling_1_result[0].last().unwrap();

        let oversampling_2_result = hnsw_index
            .search(
                &[&query],
                None,
                top,
                Some(&SearchParams {
                    hnsw_ef: Some(ef_oversampling),
                    quantization: Some(QuantizationSearchParams {
                        oversampling: Some(4.0),
                        rescore: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                &false.into(),
            )
            .unwrap();
        let best_2 = oversampling_2_result[0][0];
        let worst_2 = oversampling_2_result[0].last().unwrap();

        if best_2.score < best_1.score {
            println!("oversampling_1_result = {:?}", oversampling_1_result);
            println!("oversampling_2_result = {:?}", oversampling_2_result);
        }

        assert!(best_2.score >= best_1.score);
        assert!(worst_2.score >= worst_1.score);
    }
}

fn check_rescoring(
    query_vectors: &[QueryVector],
    hnsw_index: &HNSWIndex<GraphLinksRam>,
    filter: Option<&Filter>,
    ef: usize,
    top: usize,
) {
    for query in query_vectors.iter() {
        let index_result = hnsw_index
            .search(
                &[query],
                filter,
                top,
                Some(&SearchParams {
                    hnsw_ef: Some(ef),
                    quantization: Some(QuantizationSearchParams {
                        rescore: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                &false.into(),
            )
            .unwrap();
        for result in &index_result[0] {
            assert!(result.score < ScoreType::EPSILON);
        }
    }
}

#[test]
fn hnsw_quantized_search_cosine_test() {
    hnsw_quantized_search_test(
        Distance::Cosine,
        5003,
        ScalarQuantizationConfig {
            r#type: Default::default(),
            quantile: None,
            always_ram: None,
        }
        .into(),
    );
}

#[test]
fn hnsw_quantized_search_euclid_test() {
    hnsw_quantized_search_test(
        Distance::Euclid,
        5003,
        ScalarQuantizationConfig {
            r#type: Default::default(),
            quantile: None,
            always_ram: None,
        }
        .into(),
    );
}

#[test]
fn hnsw_quantized_search_manhattan_test() {
    hnsw_quantized_search_test(
        Distance::Manhattan,
        5003,
        ScalarQuantizationConfig {
            r#type: Default::default(),
            quantile: None,
            always_ram: None,
        }
        .into(),
    );
}

#[test]
fn hnsw_product_quantization_cosine_test() {
    hnsw_quantized_search_test(
        Distance::Cosine,
        1003,
        ProductQuantizationConfig {
            compression: CompressionRatio::X4,
            always_ram: Some(true),
        }
        .into(),
    );
}

#[test]
fn hnsw_product_quantization_euclid_test() {
    hnsw_quantized_search_test(
        Distance::Euclid,
        1003,
        ProductQuantizationConfig {
            compression: CompressionRatio::X4,
            always_ram: Some(true),
        }
        .into(),
    );
}

#[test]
fn hnsw_product_quantization_manhattan_test() {
    hnsw_quantized_search_test(
        Distance::Manhattan,
        1003,
        ProductQuantizationConfig {
            compression: CompressionRatio::X4,
            always_ram: Some(true),
        }
        .into(),
    );
}

#[test]
fn test_build_hnsw_using_quantization() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

    let stopped = AtomicBool::new(false);

    let segment1 = build_segment_1(dir.path());
    let mut config = segment1.segment_config.clone();
    let vector_data_config = config.vector_data.get_mut(DEFAULT_VECTOR_NAME).unwrap();
    vector_data_config.quantization_config = Some(
        ScalarQuantizationConfig {
            r#type: Default::default(),
            quantile: None,
            always_ram: None,
        }
        .into(),
    );
    vector_data_config.index = Indexes::Hnsw(HnswConfig {
        m: 16,
        ef_construct: 64,
        full_scan_threshold: 16,
        max_indexing_threads: 2,
        on_disk: Some(false),
        payload_m: None,
    });

    let mut builder = SegmentBuilder::new(dir.path(), temp_dir.path(), &config).unwrap();

    builder.update_from(&segment1, &stopped).unwrap();

    let built_segment: Segment = builder.build(&stopped).unwrap();

    // check if built segment has quantization and index
    assert!(built_segment.vector_data[DEFAULT_VECTOR_NAME]
        .quantized_vectors
        .borrow()
        .is_some());
    let borrowed_index = built_segment.vector_data[DEFAULT_VECTOR_NAME]
        .vector_index
        .borrow();
    match borrowed_index.deref() {
        VectorIndexEnum::HnswRam(hnsw_index) => {
            assert!(hnsw_index.get_quantized_vectors().borrow().is_some())
        }
        _ => panic!("unexpected vector index type"),
    }
}
