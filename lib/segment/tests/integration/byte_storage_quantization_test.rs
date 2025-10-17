use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic_refcell::AtomicRefCell;
use common::budget::ResourcePermit;
use common::flags::FeatureFlags;
use common::types::ScoredPointOffset;
use ordered_float::OrderedFloat;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use rstest::rstest;
use segment::data_types::vectors::{
    DEFAULT_VECTOR_NAME, DenseVector, QueryVector, only_default_vector,
};
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::payload_fixtures::{random_dense_byte_vector, random_int_payload};
use segment::fixtures::query_fixtures::QueryVariant;
use segment::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
use segment::index::{PayloadIndex, VectorIndex};
use segment::segment_constructor::build_segment;
use segment::types::{
    BinaryQuantizationConfig, CompressionRatio, Condition, Distance, FieldCondition, Filter,
    HnswConfig, HnswGlobalConfig, Indexes, PayloadSchemaType, ProductQuantizationConfig,
    QuantizationSearchParams, Range, ScalarQuantizationConfig, SearchParams, SegmentConfig,
    SeqNumberType, VectorDataConfig, VectorStorageDatatype, VectorStorageType,
};
use segment::vector_storage::VectorStorageEnum;
use segment::vector_storage::quantized::quantized_vectors::{
    QuantizedVectors, QuantizedVectorsStorageType,
};
use tempfile::Builder;

enum QuantizationVariant {
    Scalar,
    PQ,
    Binary,
}

fn random_vector<R>(rnd_gen: &mut R, dim: usize, data_type: VectorStorageDatatype) -> DenseVector
where
    R: Rng + ?Sized,
{
    match data_type {
        VectorStorageDatatype::Float32 => unreachable!(),
        VectorStorageDatatype::Float16 => {
            let mut vector = segment::fixtures::payload_fixtures::random_vector(rnd_gen, dim);
            vector.iter_mut().for_each(|x| *x -= 0.5);
            vector
        }
        VectorStorageDatatype::Uint8 => random_dense_byte_vector(rnd_gen, dim),
    }
}

fn random_query<R: Rng + ?Sized>(
    variant: &QueryVariant,
    rng: &mut R,
    dim: usize,
    data_type: VectorStorageDatatype,
) -> QueryVector {
    segment::fixtures::query_fixtures::random_query(variant, rng, |rng| {
        random_vector(rng, dim, data_type).into()
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
    VectorStorageDatatype::Float16,
    QuantizationVariant::Binary,
    Distance::Dot,
    128, // dim
    32, // ef
    10., // min_acc out of 100
)]
#[case::nearest_binary_dot(
    QueryVariant::Nearest,
    VectorStorageDatatype::Uint8,
    QuantizationVariant::Binary,
    Distance::Dot,
    128, // dim
    32, // ef
    5., // min_acc out of 100
)]
#[case::discovery_binary_dot(
    QueryVariant::Discovery,
    VectorStorageDatatype::Uint8,
    QuantizationVariant::Binary,
    Distance::Dot,
    128, // dim
    128, // ef
    1., // min_acc out of 100
)]
#[case::recobestscore_binary_dot(
    QueryVariant::RecoBestScore,
    VectorStorageDatatype::Uint8,
    QuantizationVariant::Binary,
    Distance::Dot,
    128, // dim
    64, // ef
    1., // min_acc out of 100
)]
#[case::recosumscores_binary_dot(
    QueryVariant::RecoSumScores,
    VectorStorageDatatype::Uint8,
    QuantizationVariant::Binary,
    Distance::Dot,
    128, // dim
    64, // ef
    1., // min_acc out of 100
)]
#[case::nearest_binary_cosine(
    QueryVariant::Nearest,
    VectorStorageDatatype::Uint8,
    QuantizationVariant::Binary,
    Distance::Cosine,
    128, // dim
    32, // ef
    25., // min_acc out of 100
)]
#[case::discovery_binary_cosine(
    QueryVariant::Discovery,
    VectorStorageDatatype::Uint8,
    QuantizationVariant::Binary,
    Distance::Cosine,
    128, // dim
    128, // ef
    15., // min_acc out of 100
)]
#[case::recobestscore_binary_cosine(
    QueryVariant::RecoBestScore,
    VectorStorageDatatype::Uint8,
    QuantizationVariant::Binary,
    Distance::Cosine,
    128, // dim
    64, // ef
    15., // min_acc out of 100
)]
#[case::recosumscores_binary_cosine(
    QueryVariant::RecoSumScores,
    VectorStorageDatatype::Uint8,
    QuantizationVariant::Binary,
    Distance::Cosine,
    128, // dim
    64, // ef
    15., // min_acc out of 100
)]
#[case::nearest_scalar_dot(
    QueryVariant::Nearest,
    VectorStorageDatatype::Float16,
    QuantizationVariant::Scalar,
    Distance::Dot,
    32, // dim
    32, // ef
    80., // min_acc out of 100
)]
#[case::nearest_scalar_dot(
    QueryVariant::Nearest,
    VectorStorageDatatype::Uint8,
    QuantizationVariant::Scalar,
    Distance::Dot,
    32, // dim
    32, // ef
    80., // min_acc out of 100
)]
#[case::nearest_scalar_cosine(
    QueryVariant::Nearest,
    VectorStorageDatatype::Uint8,
    QuantizationVariant::Scalar,
    Distance::Cosine,
    32, // dim
    32, // ef
    80., // min_acc out of 100
)]
#[case::nearest_pq_dot(
    QueryVariant::Nearest,
    VectorStorageDatatype::Uint8,
    QuantizationVariant::PQ,
    Distance::Dot,
    16, // dim
    32, // ef
    70., // min_acc out of 100
)]
fn test_byte_storage_binary_quantization_hnsw(
    #[case] query_variant: QueryVariant,
    #[case] storage_data_type: VectorStorageDatatype,
    #[case] quantization_variant: QuantizationVariant,
    #[case] distance: Distance,
    #[case] dim: usize,
    #[case] ef: usize,
    #[case] min_acc: f64, // out of 100
) {
    use common::counter::hardware_counter::HardwareCounterCell;
    use segment::json_path::JsonPath;
    use segment::payload_json;
    use segment::segment_constructor::VectorIndexBuildArgs;

    let stopped = AtomicBool::new(false);

    let m = 8;
    let num_vectors: u64 = 5_000;
    let ef_construct = 16;
    let full_scan_threshold = 16; // KB
    let num_payload_values = 2;

    let mut rng = StdRng::seed_from_u64(42);

    let dir_byte = Builder::new().prefix("segment_dir_byte").tempdir().unwrap();
    let quantized_data_path = dir_byte.path();
    let hnsw_dir_byte = Builder::new().prefix("hnsw_dir_byte").tempdir().unwrap();

    let config_byte = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: dim,
                distance,
                storage_type: VectorStorageType::default(),
                index: Indexes::Plain {},
                quantization_config: None,
                multivector_config: None,
                datatype: Some(storage_data_type),
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };

    let int_key = "int";

    let mut segment_byte = build_segment(dir_byte.path(), &config_byte, true).unwrap();
    // check that `segment_byte` uses byte or half storage
    {
        let borrowed_storage = segment_byte.vector_data[DEFAULT_VECTOR_NAME]
            .vector_storage
            .borrow();
        let raw_storage: &VectorStorageEnum = &borrowed_storage;
        #[cfg(feature = "rocksdb")]
        assert!(matches!(
            raw_storage,
            &VectorStorageEnum::DenseSimpleByte(_) | &VectorStorageEnum::DenseSimpleHalf(_),
        ));
        #[cfg(not(feature = "rocksdb"))]
        assert!(matches!(
            raw_storage,
            &VectorStorageEnum::DenseAppendableInRamByte(_)
                | &VectorStorageEnum::DenseAppendableInRamHalf(_),
        ));
    }

    let hw_counter = HardwareCounterCell::new();

    for n in 0..num_vectors {
        let idx = n.into();
        let vector = random_vector(&mut rng, dim, storage_data_type);

        let int_payload = random_int_payload(&mut rng, num_payload_values..=num_payload_values);
        let payload = payload_json! {int_key: int_payload};

        segment_byte
            .upsert_point(
                n as SeqNumberType,
                idx,
                only_default_vector(&vector),
                &hw_counter,
            )
            .unwrap();
        segment_byte
            .set_full_payload(n as SeqNumberType, idx, &payload, &hw_counter)
            .unwrap();
    }

    segment_byte
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
            always_ram: None,
        }
        .into(),
        QuantizationVariant::PQ => ProductQuantizationConfig {
            compression: CompressionRatio::X8,
            always_ram: None,
        }
        .into(),
        QuantizationVariant::Binary => BinaryQuantizationConfig {
            always_ram: None,
            encoding: None,
            query_encoding: None,
        }
        .into(),
    };

    segment_byte
        .vector_data
        .values_mut()
        .for_each(|vector_storage| {
            let quantized_vectors = QuantizedVectors::create(
                &vector_storage.vector_storage.borrow(),
                &quantization_config,
                QuantizedVectorsStorageType::Immutable,
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
        inline_storage: None,
    };

    let permit_cpu_count = 1; // single-threaded for deterministic build
    let permit = Arc::new(ResourcePermit::dummy(permit_cpu_count as u32));
    let hnsw_index_byte = HNSWIndex::build(
        HnswIndexOpenArgs {
            path: hnsw_dir_byte.path(),
            id_tracker: segment_byte.id_tracker.clone(),
            vector_storage: segment_byte.vector_data[DEFAULT_VECTOR_NAME]
                .vector_storage
                .clone(),
            quantized_vectors: segment_byte.vector_data[DEFAULT_VECTOR_NAME]
                .quantized_vectors
                .clone(),
            payload_index: segment_byte.payload_index.clone(),
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
        let query = random_query(&query_variant, &mut rng, dim, storage_data_type);

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
                &Default::default(),
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
                &Default::default(),
            )
            .unwrap();

        sames += sames_count(&plain_result_byte, &index_result_byte);
    }
    let acc = 100.0 * sames as f64 / (attempts * top) as f64;
    println!("sames = {sames}, attempts = {attempts}, top = {top}, acc = {acc}");
    assert!(acc > min_acc);
}
