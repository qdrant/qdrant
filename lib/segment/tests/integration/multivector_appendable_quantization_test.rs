use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use rstest::rstest;
use segment::data_types::vectors::{
    DEFAULT_VECTOR_NAME, MultiDenseVectorInternal, QueryVector, only_default_multi_vector,
};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::payload_fixtures::random_multi_vector;
use segment::fixtures::query_fixtures::QueryVariant;
use segment::segment_constructor::build_segment;
use segment::types::{
    BinaryQuantizationConfig, Distance, Indexes, MultiVectorConfig, QuantizationSearchParams, ScoredPoint, SearchParams, SegmentConfig, SeqNumberType, VectorDataConfig, VectorStorageType
};
use tempfile::Builder;

const MAX_VECTORS_COUNT: usize = 5;

fn random_vector<R: Rng + ?Sized>(rng: &mut R, dim: usize) -> MultiDenseVectorInternal {
    let count = rng.random_range(1..=MAX_VECTORS_COUNT);
    let mut vector = random_multi_vector(rng, dim, count);
    // for BQ change range to [-0.5; 0.5]
    vector.flattened_vectors.iter_mut().for_each(|x| *x -= 0.5);
    vector
}

fn random_query<R: Rng + ?Sized>(variant: &QueryVariant, rng: &mut R, dim: usize) -> QueryVector {
    segment::fixtures::query_fixtures::random_query(variant, rng, |rng: &mut R| {
        random_vector(rng, dim).into()
    })
}

fn sames_count(a: &[ScoredPoint], b: &[ScoredPoint]) -> usize {
    a.iter()
        .map(|x| x.id)
        .collect::<BTreeSet<_>>()
        .intersection(&b.iter().map(|x| x.id).collect())
        .count()
}

#[rstest]
#[case::nearest_binary_dot(
    QueryVariant::Nearest,
    Distance::Dot,
    128, // dim
    32, // ef
    false,
    25., // min_acc out of 100
)]
fn test_multivector_quantization_hnsw(
    #[case] query_variant: QueryVariant,
    #[case] distance: Distance,
    #[case] dim: usize,
    #[case] ef: usize,
    #[case] on_disk: bool,
    #[case] min_acc: f64, // out of 100
) {
    use segment::payload_json;
    use segment::segment_constructor::VectorIndexBuildArgs;
    use segment::types::{HnswGlobalConfig, QuantizationConfig};

    let stopped = AtomicBool::new(false);

    let m = 8;
    let num_vectors: u64 = 1_000;
    let ef_construct = 16;
    let full_scan_threshold = 16; // KB
    let num_payload_values = 2;

    let mut rng = StdRng::seed_from_u64(42);

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let quantized_data_path = dir.path();
    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();

    let storage_type = if on_disk {
        VectorStorageType::ChunkedMmap
    } else {
        #[cfg(feature = "rocksdb")]
        {
            VectorStorageType::Memory
        }
        #[cfg(not(feature = "rocksdb"))]
        {
            VectorStorageType::InRamChunkedMmap
        }
    };
    let quantization_config = BinaryQuantizationConfig {
        always_ram: Some(false),
        encoding: None,
        query_encoding: None,
    };
    let config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: dim,
                distance,
                storage_type,
                index: Indexes::Plain {},
                quantization_config: Some(QuantizationConfig::from(quantization_config.clone())),
                multivector_config: Some(MultiVectorConfig::default()), // uses multivec config
                datatype: None,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };

    let mut segment = build_segment(dir.path(), &config, true).unwrap();
    let hw_counter = HardwareCounterCell::new();

    for n in 0..num_vectors {
        let idx = n.into();
        let vector = random_vector(&mut rng, dim);

        segment
            .upsert_point(
                n as SeqNumberType,
                idx,
                only_default_multi_vector(&vector),
                &hw_counter,
            )
            .unwrap();
    }

    let top = 5;
    let mut sames = 0;
    let attempts = 100;
    for _ in 0..attempts {
        let query = random_query(&query_variant, &mut rng, dim);

        let search_result = segment.search(
            DEFAULT_VECTOR_NAME,
            &query,
            &Default::default(),
            &Default::default(),
            None,
            top,
            Some(&SearchParams {
                hnsw_ef: Some(ef),
                quantization: Some(QuantizationSearchParams {
                    oversampling: None,
                    rescore: Some(false),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        ).unwrap();
        for score in search_result.iter().map(|r| r.score) {
            assert_eq!(score.fract(), 0.0, "BQ score is an integer number because of popcnt");
        }

        let plain_result = segment.search(
            DEFAULT_VECTOR_NAME,
            &query,
            &Default::default(),
            &Default::default(),
            None,
            top,
            Some(&SearchParams {
                    hnsw_ef: Some(ef),
                    quantization: Some(QuantizationSearchParams {
                        ignore: true,
                        ..Default::default()
                    }),
                    ..Default::default()
            }),
        ).unwrap();

        sames += sames_count(&plain_result, &search_result);
    }
    let acc = 100.0 * sames as f64 / (attempts * top) as f64;
    println!("sames = {sames}, attempts = {attempts}, top = {top}, acc = {acc}");
    assert!(acc > min_acc);
}
