use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::AtomicBool;

use rand::rngs::StdRng;
use rand::SeedableRng;
use segment::data_types::vectors::{only_default_vector, DEFAULT_VECTOR_NAME};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::payload_fixtures::random_vector;
use segment::index::hnsw_index::graph_links::GraphLinksRam;
use segment::index::hnsw_index::hnsw::HNSWIndex;
use segment::index::VectorIndex;
use segment::segment_constructor::build_segment;
use segment::types::{
    CompressionRatio, Distance, HnswConfig, Indexes, ProductQuantizationConfig, QuantizationConfig,
    QuantizationSearchParams, ScalarQuantizationConfig, SearchParams, SegmentConfig, SeqNumberType,
    VectorDataConfig, VectorStorageType,
};
use segment::vector_storage::{ScoredPointOffset, VectorStorage};
use tempfile::Builder;

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

    let dim = 131;
    let m = 16;
    let ef = 64;
    let ef_construct = 64;

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
            },
        )]),
        payload_storage_type: Default::default(),
    };

    let mut segment = build_segment(dir.path(), &config, true).unwrap();
    for n in 0..num_vectors {
        let idx = n.into();
        let vector = random_vector(&mut rnd, dim);
        segment
            .upsert_point(n as SeqNumberType, idx, only_default_vector(&vector))
            .unwrap();
    }
    segment.vector_data.values_mut().for_each(|vector_storage| {
        vector_storage
            .vector_storage
            .borrow_mut()
            .quantize(quantized_data_path, &quantization_config, 3, &stopped)
            .unwrap();
    });

    let hnsw_config = HnswConfig {
        m,
        ef_construct,
        full_scan_threshold: 0,
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
        segment.payload_index.clone(),
        hnsw_config,
    )
    .unwrap();

    hnsw_index.build_index(&stopped).unwrap();

    let top = 10;
    let attempts = 10;
    let mut sames: usize = 0;
    for _i in 0..attempts {
        let query = random_vector(&mut rnd, dim).into();
        let index_result = hnsw_index.search(
            &[&query],
            None,
            top,
            Some(&SearchParams {
                hnsw_ef: Some(ef),
                ..Default::default()
            }),
            &false.into(),
        );
        let plain_result = segment.vector_data[DEFAULT_VECTOR_NAME]
            .vector_index
            .borrow()
            .search(&[&query], None, top, None, &false.into());
        sames += sames_count(&index_result, &plain_result);
    }
    let acc = 100.0 * sames as f64 / (attempts * top) as f64;
    println!("sames = {sames}, attempts = {attempts}, top = {top}, acc = {acc}");
    assert!(acc > 40.0);

    // check oversampling
    for _i in 0..attempts {
        let ef_oversampling = ef / 8;
        let oversampling_query = random_vector(&mut rnd, dim).into();

        let oversampling_1_result = hnsw_index.search(
            &[&oversampling_query],
            None,
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
        );
        let best_1 = oversampling_1_result[0][0];
        let worst_1 = oversampling_1_result[0].last().unwrap();

        let oversampling_2_result = hnsw_index.search(
            &[&oversampling_query],
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
        );
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
