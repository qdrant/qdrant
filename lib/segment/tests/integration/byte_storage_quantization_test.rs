use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::cpu::CpuPermit;
use common::types::ScoredPointOffset;
use itertools::Itertools;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use rstest::rstest;
use segment::data_types::vectors::{only_default_vector, QueryVector, DEFAULT_VECTOR_NAME};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::payload_fixtures::{random_dense_byte_vector, random_int_payload};
use segment::index::hnsw_index::graph_links::GraphLinksRam;
use segment::index::hnsw_index::hnsw::HNSWIndex;
use segment::index::hnsw_index::num_rayon_threads;
use segment::index::{PayloadIndex, VectorIndex};
use segment::segment_constructor::build_segment;
use segment::types::{
    BinaryQuantizationConfig, CompressionRatio, Condition, Distance, FieldCondition, Filter,
    HnswConfig, Indexes, Payload, PayloadSchemaType, ProductQuantizationConfig,
    QuantizationSearchParams, Range, ScalarQuantizationConfig, SearchParams, SegmentConfig,
    SeqNumberType, VectorDataConfig, VectorStorageDatatype, VectorStorageType,
};
use segment::vector_storage::quantized::quantized_vectors::QuantizedVectors;
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

enum QuantizationVariant {
    Scalar,
    PQ,
    Binary,
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

fn sames_count(a: &[Vec<ScoredPointOffset>], b: &[Vec<ScoredPointOffset>]) -> usize {
    a[0].iter()
        .map(|x| x.idx)
        .collect::<BTreeSet<_>>()
        .intersection(&b[0].iter().map(|x| x.idx).collect())
        .count()
}

#[rstest]
#[case::nearest_binary_dot(
    QueryVariant::Nearest,
    QuantizationVariant::Binary,
    Distance::Dot,
    128, // dim
    32, // ef
    5., // min_acc out of 100
)]
#[case::discovery_binary_dot(
    QueryVariant::Discovery,
    QuantizationVariant::Binary,
    Distance::Dot,
    128, // dim
    128, // ef
    1., // min_acc out of 100
)]
#[case::recommend_binary_dot(
    QueryVariant::RecommendBestScore,
    QuantizationVariant::Binary,
    Distance::Dot,
    128, // dim
    64, // ef
    3., // min_acc out of 100
)]
#[case::nearest_binary_cosine(
    QueryVariant::Nearest,
    QuantizationVariant::Binary,
    Distance::Cosine,
    128, // dim
    32, // ef
    25., // min_acc out of 100
)]
#[case::discovery_binary_cosine(
    QueryVariant::Discovery,
    QuantizationVariant::Binary,
    Distance::Cosine,
    128, // dim
    128, // ef
    20., // min_acc out of 100
)]
#[case::recommend_binary_cosine(
    QueryVariant::RecommendBestScore,
    QuantizationVariant::Binary,
    Distance::Cosine,
    128, // dim
    64, // ef
    20., // min_acc out of 100
)]
#[case::nearest_scalar_dot(
    QueryVariant::Nearest,
    QuantizationVariant::Scalar,
    Distance::Dot,
    32, // dim
    32, // ef
    5., // min_acc out of 100
)]
#[case::nearest_scalar_cosine(
    QueryVariant::Nearest,
    QuantizationVariant::Scalar,
    Distance::Cosine,
    32, // dim
    32, // ef
    20., // min_acc out of 100
)]
#[case::nearest_pq_dot(
    QueryVariant::Nearest,
    QuantizationVariant::PQ,
    Distance::Dot,
    16, // dim
    32, // ef
    10., // min_acc out of 100
)]
fn test_byte_storage_binary_quantization_hnsw(
    #[case] query_variant: QueryVariant,
    #[case] quantization_variant: QuantizationVariant,
    #[case] distance: Distance,
    #[case] dim: usize,
    #[case] ef: usize,
    #[case] min_acc: f64, // out of 100
) {
    let stopped = AtomicBool::new(false);

    let m = 8;
    let num_vectors: u64 = 5_000;
    let ef_construct = 16;
    let full_scan_threshold = 16; // KB
    let num_payload_values = 2;

    let mut rnd = StdRng::seed_from_u64(42);

    let dir_byte = Builder::new().prefix("segment_dir_byte").tempdir().unwrap();
    let quantized_data_path = dir_byte.path();
    let hnsw_dir_byte = Builder::new().prefix("hnsw_dir_byte").tempdir().unwrap();

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

        segment_byte
            .upsert_point(n as SeqNumberType, idx, only_default_vector(&vector))
            .unwrap();
        segment_byte
            .set_full_payload(n as SeqNumberType, idx, &payload)
            .unwrap();
    }

    segment_byte
        .payload_index
        .borrow_mut()
        .set_indexed(&path(int_key), PayloadSchemaType::Integer.into())
        .unwrap();

    let quantization_config = match quantization_variant {
        QuantizationVariant::Scalar => ScalarQuantizationConfig {
            r#type: Default::default(),
            quantile: None,
            always_ram: None,
        }
        .into(),
        QuantizationVariant::PQ => ProductQuantizationConfig {
            compression: CompressionRatio::X8,
            always_ram: None,
        }
        .into(),
        QuantizationVariant::Binary => BinaryQuantizationConfig { always_ram: None }.into(),
    };

    segment_byte
        .vector_data
        .values_mut()
        .for_each(|vector_storage| {
            let quantized_vectors = QuantizedVectors::create(
                &vector_storage.vector_storage.borrow(),
                &quantization_config,
                quantized_data_path,
                4,
                &stopped,
            )
            .unwrap();
            vector_storage.quantized_vectors =
                Arc::new(AtomicRefCell::new(Some(quantized_vectors)));
        });

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

    let top = 5;
    let mut sames = 0;
    let attempts = 100;
    for _ in 0..attempts {
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
                    quantization: Some(QuantizationSearchParams {
                        oversampling: Some(2.0),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                &stopped,
                usize::MAX,
            )
            .unwrap();

        let plain_result_byte = hnsw_index_byte
            .search(
                &[&query],
                filter_query,
                top,
                Some(&SearchParams {
                    hnsw_ef: Some(ef),
                    quantization: Some(QuantizationSearchParams {
                        ignore: true,
                        ..Default::default()
                    }),
                    exact: true,
                    ..Default::default()
                }),
                &stopped,
                usize::MAX,
            )
            .unwrap();

        sames += sames_count(&plain_result_byte, &index_result_byte);
    }
    let acc = 100.0 * sames as f64 / (attempts * top) as f64;
    println!("sames = {sames}, attempts = {attempts}, top = {top}, acc = {acc}");
    assert!(acc > min_acc);
}
