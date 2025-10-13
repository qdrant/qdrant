use std::collections::BTreeSet;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic_refcell::AtomicRefCell;
use common::budget::ResourcePermit;
use common::counter::hardware_counter::HardwareCounterCell;
use common::flags::FeatureFlags;
use common::types::{ScoreType, ScoredPointOffset};
use rand::SeedableRng;
use rand::rngs::StdRng;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, QueryVector, only_default_vector};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::payload_fixtures::{STR_KEY, random_vector};
use segment::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
use segment::index::hnsw_index::num_rayon_threads;
use segment::index::{VectorIndex, VectorIndexEnum};
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment::Segment;
use segment::segment_constructor::VectorIndexBuildArgs;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::PayloadSchemaType::Keyword;
use segment::types::{
    CompressionRatio, Condition, Distance, FieldCondition, Filter, HnswConfig, HnswGlobalConfig,
    Indexes, ProductQuantizationConfig, QuantizationConfig, QuantizationSearchParams,
    ScalarQuantizationConfig, SearchParams,
};
use segment::vector_storage::quantized::quantized_vectors::{
    QuantizedVectors, QuantizedVectorsStorageType,
};
use tempfile::Builder;

use crate::fixtures::segment::build_segment_1;

pub fn sames_count(a: &[Vec<ScoredPointOffset>], b: &[Vec<ScoredPointOffset>]) -> usize {
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

    let mut rng = StdRng::seed_from_u64(42);
    let mut op_num = 0;

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();

    let hw_counter = HardwareCounterCell::new();

    let mut segment = build_simple_segment(dir.path(), dim, distance).unwrap();
    for n in 0..num_vectors {
        let idx = n.into();
        let vector = random_vector(&mut rng, dim);
        segment
            .upsert_point(op_num, idx, only_default_vector(&vector), &hw_counter)
            .unwrap();
        op_num += 1;
    }

    segment
        .create_field_index(
            op_num,
            &JsonPath::new(STR_KEY),
            Some(&Keyword.into()),
            &hw_counter,
        )
        .unwrap();
    op_num += 1;
    for n in 0..payloads_count {
        let idx = n.into();
        let payload = payload_json! {STR_KEY: STR_KEY};
        segment
            .set_full_payload(op_num, idx, &payload, &hw_counter)
            .unwrap();
        op_num += 1;
    }

    segment.vector_data.values_mut().for_each(|vector_storage| {
        let quantized_vectors = QuantizedVectors::create(
            &vector_storage.vector_storage.borrow(),
            &quantization_config,
            QuantizedVectorsStorageType::Immutable,
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
        inline_storage: None,
    };

    let permit_cpu_count = 1; // single-threaded for deterministic build
    let permit = Arc::new(ResourcePermit::dummy(permit_cpu_count as u32));

    let hnsw_index = HNSWIndex::build(
        HnswIndexOpenArgs {
            path: hnsw_dir.path(),
            id_tracker: segment.id_tracker.clone(),
            vector_storage: segment.vector_data[DEFAULT_VECTOR_NAME]
                .vector_storage
                .clone(),
            quantized_vectors: segment.vector_data[DEFAULT_VECTOR_NAME]
                .quantized_vectors
                .clone(),
            payload_index: segment.payload_index.clone(),
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

    let query_vectors = (0..attempts)
        .map(|_| random_vector(&mut rng, dim).into())
        .collect::<Vec<_>>();
    let filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
        JsonPath::new(STR_KEY),
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
    let zero_vector = vec![0.0; dim];
    for n in 0..num_vectors {
        let idx = n.into();
        segment
            .upsert_point(op_num, idx, only_default_vector(&zero_vector), &hw_counter)
            .unwrap();
        op_num += 1;
    }
    check_rescoring(&query_vectors, &hnsw_index, None, ef, top);
    check_rescoring(&query_vectors, &hnsw_index, Some(&filter), ef, top);
}

pub fn check_matches(
    query_vectors: &[QueryVector],
    segment: &Segment,
    hnsw_index: &HNSWIndex,
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
                .search(&[query], filter, top, None, &Default::default())
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
                &Default::default(),
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
    hnsw_index: &HNSWIndex,
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
                &Default::default(),
            )
            .unwrap();
        let best_1 = oversampling_1_result[0][0];
        let worst_1 = oversampling_1_result[0].last().unwrap();

        let oversampling_2_result = hnsw_index
            .search(
                &[query],
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
                &Default::default(),
            )
            .unwrap();
        let best_2 = oversampling_2_result[0][0];
        let worst_2 = oversampling_2_result[0].last().unwrap();

        if best_2.score < best_1.score {
            println!("oversampling_1_result = {oversampling_1_result:?}");
            println!("oversampling_2_result = {oversampling_2_result:?}");
        }

        assert!(best_2.score >= best_1.score);
        assert!(worst_2.score >= worst_1.score);
    }
}

fn check_rescoring(
    query_vectors: &[QueryVector],
    hnsw_index: &HNSWIndex,
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
                &Default::default(),
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

    let mut rng = rand::rng();
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
        inline_storage: None,
    });

    let permit_cpu_count = num_rayon_threads(0);
    let permit = ResourcePermit::dummy(permit_cpu_count as u32);
    let hw_counter = HardwareCounterCell::new();

    let mut builder = SegmentBuilder::new(
        dir.path(),
        temp_dir.path(),
        &config,
        &HnswGlobalConfig::default(),
    )
    .unwrap();

    builder.update(&[&segment1], &stopped).unwrap();

    let built_segment: Segment = builder
        .build(permit, &stopped, &mut rng, &hw_counter)
        .unwrap();

    // check if built segment has quantization and index
    assert!(
        built_segment.vector_data[DEFAULT_VECTOR_NAME]
            .quantized_vectors
            .borrow()
            .is_some(),
    );
    let borrowed_index = built_segment.vector_data[DEFAULT_VECTOR_NAME]
        .vector_index
        .borrow();
    match borrowed_index.deref() {
        VectorIndexEnum::Hnsw(hnsw_index) => {
            assert!(hnsw_index.get_quantized_vectors().borrow().is_some())
        }
        _ => panic!("unexpected vector index type"),
    }
}
