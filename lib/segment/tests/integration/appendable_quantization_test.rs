use std::collections::{BTreeSet, HashMap};
use common::counter::hardware_counter::HardwareCounterCell;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use rstest::rstest;
use segment::data_types::vectors::{
    only_default_multi_vector, only_default_vector, MultiDenseVectorInternal, QueryVector, DEFAULT_VECTOR_NAME
};
use segment::types::QuantizationConfig;
use segment::entry::entry_point::SegmentEntry;
use segment::fixtures::payload_fixtures::random_multi_vector;
use segment::fixtures::payload_fixtures::random_vector;
use segment::fixtures::query_fixtures::QueryVariant;
use segment::segment_constructor::build_segment;
use segment::types::{BinaryQuantizationQueryEncoding, BinaryQuantizationEncoding,
    BinaryQuantizationConfig, Distance, Indexes, MultiVectorConfig, QuantizationSearchParams, ScoredPoint, SearchParams, SegmentConfig, SeqNumberType, VectorDataConfig, VectorStorageType
};
use tempfile::Builder;

const MAX_VECTORS_COUNT: usize = 4;

fn random_vector_multi<R: Rng + ?Sized>(rng: &mut R, dim: usize) -> MultiDenseVectorInternal {
    let count = rng.random_range(1..=MAX_VECTORS_COUNT);
    let mut vector = random_multi_vector(rng, dim, count);
    // for BQ change range to [-0.5; 0.5]
    vector.flattened_vectors.iter_mut().for_each(|x| *x -= 0.5);
    vector
}

fn random_query_multi<R: Rng + ?Sized>(rng: &mut R, dim: usize) -> QueryVector {
    segment::fixtures::query_fixtures::random_query(&QueryVariant::Nearest, rng, |rng: &mut R| {
        random_vector_multi(rng, dim).into()
    })
}

fn random_query<R: Rng + ?Sized>(rng: &mut R, dim: usize) -> QueryVector {
    segment::fixtures::query_fixtures::random_query(&QueryVariant::Nearest, rng, |rng: &mut R| {
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
#[case::appendable_ram(
    250,
    false,
    15.,
    None,
    None,
)]
#[case::appendable_mmap(
    200,
    true,
    15.,
    None,
    None,
)]
#[case::encoding_2bit(
    200,
    true,
    15.,
    Some(BinaryQuantizationEncoding::TwoBits),
    None,
)]
#[case::query_encoding_8bit(
    200,
    true,
    15.,
    None,
    Some(BinaryQuantizationQueryEncoding::Scalar8Bits),
)]
fn test_appendable_quantization_dense(
    #[case] dim: usize,
    #[case] on_disk: bool,
    #[case] min_acc: f64, // min_acc out of 100
    #[case] encoding: Option<BinaryQuantizationEncoding>,
    #[case] query_encoding: Option<BinaryQuantizationQueryEncoding>
) {
    let distance = Distance::Dot;
    let num_vectors: u64 = 300;
    let mut rng = StdRng::seed_from_u64(42);
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

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
        always_ram: Some(false), // same as vector storage
        encoding,
        query_encoding,
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
                multivector_config: None,
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
                only_default_vector(&vector),
                &hw_counter,
            )
            .unwrap();
    }

    let top = 5;
    let mut sames = 0;
    let attempts = 30;
    for _ in 0..attempts {
        let query = random_query(&mut rng, dim);

        let search_result = segment.search(
            DEFAULT_VECTOR_NAME,
            &query,
            &Default::default(),
            &Default::default(),
            None,
            top,
            Some(&SearchParams {
                quantization: Some(QuantizationSearchParams {
                    oversampling: None,
                    rescore: Some(false),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        ).unwrap();

        let plain_result = segment.search(
            DEFAULT_VECTOR_NAME,
            &query,
            &Default::default(),
            &Default::default(),
            None,
            top,
            Some(&SearchParams {
                    quantization: Some(QuantizationSearchParams {
                        ignore: true,
                        ..Default::default()
                    }),
                    ..Default::default()
            }),
        ).unwrap();

        sames += sames_count(&plain_result, &search_result);
    }
    assert_ne!(sames, attempts * top, "All results are the same, not expected because binary quantization must have an acc loss");

    let acc = 100.0 * sames as f64 / (attempts * top) as f64;
    println!("sames = {sames}, attempts = {attempts}, top = {top}, acc = {acc}");
    assert!(acc > min_acc);
}

#[rstest]
#[case::appendable_ram(
    250,
    false,
    15.,
    None,
    None,
)]
#[case::appendable_mmap(
    200,
    true,
    15.,
    None,
    None,
)]
#[case::encoding_2bit(
    200,
    true,
    15.,
    Some(BinaryQuantizationEncoding::TwoBits),
    None,
)]
#[case::query_encoding_8bit(
    200,
    true,
    15.,
    None,
    Some(BinaryQuantizationQueryEncoding::Scalar8Bits),
)]
fn test_appendable_quantization_multi(
    #[case] dim: usize,
    #[case] on_disk: bool,
    #[case] min_acc: f64, // min_acc out of 100
    #[case] encoding: Option<BinaryQuantizationEncoding>,
    #[case] query_encoding: Option<BinaryQuantizationQueryEncoding>
) {
    let distance = Distance::Dot;
    let num_vectors: u64 = 300;
    let mut rng = StdRng::seed_from_u64(42);
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

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
        always_ram: Some(false), // same as vector storage
        encoding,
        query_encoding,
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
                multivector_config: Some(MultiVectorConfig::default()),
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
        let vector = random_vector_multi(&mut rng, dim);

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
    let attempts = 30;
    for _ in 0..attempts {
        let query = random_query_multi(&mut rng, dim);

        let search_result = segment.search(
            DEFAULT_VECTOR_NAME,
            &query,
            &Default::default(),
            &Default::default(),
            None,
            top,
            Some(&SearchParams {
                quantization: Some(QuantizationSearchParams {
                    oversampling: None,
                    rescore: Some(false),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        ).unwrap();

        let plain_result = segment.search(
            DEFAULT_VECTOR_NAME,
            &query,
            &Default::default(),
            &Default::default(),
            None,
            top,
            Some(&SearchParams {
                    quantization: Some(QuantizationSearchParams {
                        ignore: true,
                        ..Default::default()
                    }),
                    ..Default::default()
            }),
        ).unwrap();

        sames += sames_count(&plain_result, &search_result);
    }
    assert_ne!(sames, attempts * top, "All results are the same, not expected because binary quantization must have an acc loss");

    let acc = 100.0 * sames as f64 / (attempts * top) as f64;
    println!("sames = {sames}, attempts = {attempts}, top = {top}, acc = {acc}");
    assert!(acc > min_acc);
}
