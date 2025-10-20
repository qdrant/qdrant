use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic_refcell::AtomicRefCell;
use common::budget::ResourcePermit;
use common::counter::hardware_counter::HardwareCounterCell;
use common::flags::FeatureFlags;
use common::types::ScoredPointOffset;
use ordered_float::OrderedFloat;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use rstest::rstest;
use segment::data_types::vectors::{
    DEFAULT_VECTOR_NAME, MultiDenseVectorInternal, QueryVector, only_default_multi_vector,
};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::payload_fixtures::{random_int_payload, random_multi_vector};
use segment::fixtures::query_fixtures::QueryVariant;
use segment::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
use segment::index::{PayloadIndex, VectorIndex};
use segment::json_path::JsonPath;
use segment::segment_constructor::build_segment;
use segment::types::{
    BinaryQuantizationConfig, CompressionRatio, Condition, Distance, FieldCondition, Filter,
    HnswConfig, Indexes, MultiVectorConfig, PayloadSchemaType, ProductQuantizationConfig,
    QuantizationSearchParams, Range, ScalarQuantizationConfig, SearchParams, SegmentConfig,
    SeqNumberType, VectorDataConfig, VectorStorageType,
};
use segment::vector_storage::quantized::quantized_vectors::{
    QuantizedVectors, QuantizedVectorsStorageType,
};
use tempfile::Builder;

const MAX_VECTORS_COUNT: usize = 3;

enum QuantizationVariant {
    Scalar,
    PQ,
    Binary,
}

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
    false,
    25., // min_acc out of 100
)]
#[case::discovery_binary_dot(
    QueryVariant::Discovery,
    QuantizationVariant::Binary,
    Distance::Dot,
    128, // dim
    128, // ef
    false,
    20., // min_acc out of 100
)]
#[case::recobestscore_binary_dot(
    QueryVariant::RecoBestScore,
    QuantizationVariant::Binary,
    Distance::Dot,
    128, // dim
    64, // ef
    false,
    20., // min_acc out of 100
)]
#[case::recosumscores_binary_dot(
    QueryVariant::RecoSumScores,
    QuantizationVariant::Binary,
    Distance::Dot,
    128, // dim
    64, // ef
    false,
    20., // min_acc out of 100
)]
#[case::nearest_binary_cosine(
    QueryVariant::Nearest,
    QuantizationVariant::Binary,
    Distance::Cosine,
    128, // dim
    32, // ef
    false,
    25., // min_acc out of 100
)]
#[case::discovery_binary_cosine(
    QueryVariant::Discovery,
    QuantizationVariant::Binary,
    Distance::Cosine,
    128, // dim
    128, // ef
    false,
    15., // min_acc out of 100
)]
#[case::recobestscore_binary_cosine(
    QueryVariant::RecoBestScore,
    QuantizationVariant::Binary,
    Distance::Cosine,
    128, // dim
    64, // ef
    false,
    15., // min_acc out of 100
)]
#[case::recosumscores_binary_cosine(
    QueryVariant::RecoSumScores,
    QuantizationVariant::Binary,
    Distance::Cosine,
    128, // dim
    64, // ef
    false,
    15., // min_acc out of 100
)]
#[case::nearest_scalar_dot(
    QueryVariant::Nearest,
    QuantizationVariant::Scalar,
    Distance::Dot,
    32, // dim
    32, // ef
    false,
    80., // min_acc out of 100
)]
#[case::nearest_scalar_cosine(
    QueryVariant::Nearest,
    QuantizationVariant::Scalar,
    Distance::Cosine,
    32, // dim
    32, // ef
    false,
    80., // min_acc out of 100
)]
#[case::nearest_pq_dot(
    QueryVariant::Nearest,
    QuantizationVariant::PQ,
    Distance::Dot,
    16, // dim
    32, // ef
    false,
    70., // min_acc out of 100
)]
#[case::nearest_scalar_cosine_on_disk(
    QueryVariant::Nearest,
    QuantizationVariant::Scalar,
    Distance::Cosine,
    32, // dim
    32, // ef
    true,
    80., // min_acc out of 100
)]
fn test_multivector_quantization_hnsw(
    #[case] query_variant: QueryVariant,
    #[case] quantization_variant: QuantizationVariant,
    #[case] distance: Distance,
    #[case] dim: usize,
    #[case] ef: usize,
    #[case] on_disk: bool,
    #[case] min_acc: f64, // out of 100
) {
    use segment::payload_json;
    use segment::segment_constructor::VectorIndexBuildArgs;
    use segment::types::HnswGlobalConfig;

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
    let config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: dim,
                distance,
                storage_type,
                index: Indexes::Plain {},
                quantization_config: None,
                multivector_config: Some(MultiVectorConfig::default()), // uses multivec config
                datatype: None,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };

    let int_key = "int";

    let mut segment = build_segment(dir.path(), &config, true).unwrap();

    let hw_counter = HardwareCounterCell::new();

    for n in 0..num_vectors {
        let idx = n.into();
        let vector = random_vector(&mut rng, dim);

        let int_payload = random_int_payload(&mut rng, num_payload_values..=num_payload_values);
        let payload = payload_json! {int_key: int_payload};

        segment
            .upsert_point(
                n as SeqNumberType,
                idx,
                only_default_multi_vector(&vector),
                &hw_counter,
            )
            .unwrap();
        segment
            .set_full_payload(n as SeqNumberType, idx, &payload, &hw_counter)
            .unwrap();
    }

    segment
        .payload_index
        .borrow_mut()
        .set_indexed(
            &JsonPath::new(int_key),
            PayloadSchemaType::Integer,
            &hw_counter,
        )
        .unwrap();

    let quantization_config = match quantization_variant {
        QuantizationVariant::Scalar => ScalarQuantizationConfig {
            r#type: Default::default(),
            quantile: None,
            always_ram: Some(false),
        }
        .into(),
        QuantizationVariant::PQ => ProductQuantizationConfig {
            compression: CompressionRatio::X8,
            always_ram: Some(false),
        }
        .into(),
        QuantizationVariant::Binary => BinaryQuantizationConfig {
            always_ram: Some(false),
            encoding: None,
            query_encoding: None,
        }
        .into(),
    };

    segment.vector_data.values_mut().for_each(|vector_storage| {
        {
            // test persistence, encode and save quantized vectors
            QuantizedVectors::create(
                &vector_storage.vector_storage.borrow(),
                &quantization_config,
                QuantizedVectorsStorageType::Immutable,
                quantized_data_path,
                4,
                &stopped,
            )
            .unwrap();
        }
        // test persistence, load quantized vectors
        let quantized_vectors = QuantizedVectors::load(
            &quantization_config,
            &vector_storage.vector_storage.borrow(),
            quantized_data_path,
            &stopped,
        )
        .unwrap()
        .unwrap();
        vector_storage.quantized_vectors = Arc::new(AtomicRefCell::new(Some(quantized_vectors)));
    });

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

    let top = 5;
    let mut sames = 0;
    let attempts = 100;
    for _ in 0..attempts {
        let query = random_query(&query_variant, &mut rng, dim);

        let range_size = 40;
        let left_range = rng.random_range(0..400);
        let right_range = left_range + range_size;

        let filter = Filter::new_must(Condition::Field(FieldCondition::new_range(
            JsonPath::new(int_key),
            Range {
                lt: None,
                gt: None,
                gte: Some(OrderedFloat(f64::from(left_range))),
                lte: Some(OrderedFloat(f64::from(right_range))),
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
                    quantization: Some(QuantizationSearchParams {
                        oversampling: Some(1.3),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                &Default::default(),
            )
            .unwrap();

        let plain_result = hnsw_index
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
                &Default::default(),
            )
            .unwrap();

        sames += sames_count(&plain_result, &index_result);
    }
    let acc = 100.0 * sames as f64 / (attempts * top) as f64;
    println!("sames = {sames}, attempts = {attempts}, top = {top}, acc = {acc}");
    assert!(acc > min_acc);
}
