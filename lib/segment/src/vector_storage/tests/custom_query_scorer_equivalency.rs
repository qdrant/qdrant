use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::{error, result};

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use itertools::Itertools;
use rand::rngs::StdRng;
use rand::seq::IteratorRandom;
use rand::{Rng, SeedableRng, rng};
use rstest::rstest;

use super::utils::sampler;
use crate::common::rocksdb_wrapper;
use crate::data_types::vectors::{QueryVector, VectorElementType, VectorInternal};
use crate::fixtures::payload_context_fixture::FixtureIdTracker;
use crate::id_tracker::id_tracker_base::IdTracker;
use crate::types::{
    BinaryQuantizationConfig, Distance, ProductQuantizationConfig, QuantizationConfig,
    ScalarQuantizationConfig,
};
#[cfg(target_os = "linux")]
use crate::vector_storage::dense::memmap_dense_vector_storage::open_memmap_vector_storage_with_async_io;
use crate::vector_storage::dense::simple_dense_vector_storage::open_simple_dense_vector_storage;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::query::{ContextPair, ContextQuery, DiscoveryQuery, RecoQuery};
use crate::vector_storage::tests::utils::score;
use crate::vector_storage::vector_storage_base::VectorStorage;
use crate::vector_storage::{VectorStorageEnum, new_raw_scorer_for_test};

const DIMS: usize = 128;
const NUM_POINTS: usize = 600;
const DISTANCE: Distance = Distance::Dot;
const MAX_EXAMPLES: usize = 10;
const SAMPLE_SIZE: usize = 100;
const SEED: u64 = 42;

type Result<T, E = Error> = result::Result<T, E>;
type Error = Box<dyn error::Error>;

type WithQuantization = (
    QuantizationConfig,
    Box<dyn Iterator<Item = VectorElementType>>,
);

fn random_query<R: Rng + ?Sized>(
    query_variant: &QueryVariant,
    rnd: &mut R,
    sampler: &mut impl Iterator<Item = f32>,
) -> QueryVector {
    match query_variant {
        QueryVariant::RecoBestScore => {
            QueryVector::RecommendBestScore(random_reco_query(rnd, sampler))
        }
        QueryVariant::RecoSumScores => {
            QueryVector::RecommendSumScores(random_reco_query(rnd, sampler))
        }
        QueryVariant::Discovery => random_discovery_query(rnd, sampler),
        QueryVariant::Context => random_context_query(rnd, sampler),
    }
}

fn random_reco_query<R: Rng + ?Sized>(
    rnd: &mut R,
    sampler: &mut impl Iterator<Item = f32>,
) -> RecoQuery<VectorInternal> {
    let num_positives: usize = rnd.random_range(0..MAX_EXAMPLES);
    let num_negatives: usize = rnd.random_range(1..MAX_EXAMPLES);

    let positives = (0..num_positives)
        .map(|_| sampler.take(DIMS).collect_vec().into())
        .collect_vec();

    let negatives = (0..num_negatives)
        .map(|_| sampler.take(DIMS).collect_vec().into())
        .collect_vec();

    RecoQuery::new(positives, negatives)
}

fn random_discovery_query<R: Rng + ?Sized>(
    rnd: &mut R,
    sampler: &mut impl Iterator<Item = f32>,
) -> QueryVector {
    let num_pairs: usize = rnd.random_range(0..MAX_EXAMPLES);

    let target = sampler.take(DIMS).collect_vec().into();

    let pairs = (0..num_pairs)
        .map(|_| {
            let positive = sampler.take(DIMS).collect_vec().into();
            let negative = sampler.take(DIMS).collect_vec().into();
            ContextPair { positive, negative }
        })
        .collect_vec();

    DiscoveryQuery::new(target, pairs).into()
}

fn random_context_query<R: Rng + ?Sized>(
    rnd: &mut R,
    sampler: &mut impl Iterator<Item = f32>,
) -> QueryVector {
    let num_pairs: usize = rnd.random_range(0..MAX_EXAMPLES);

    let pairs = (0..num_pairs)
        .map(|_| {
            let positive = sampler.take(DIMS).collect_vec().into();
            let negative = sampler.take(DIMS).collect_vec().into();
            ContextPair { positive, negative }
        })
        .collect_vec();

    ContextQuery::new(pairs).into()
}

fn ram_storage(dir: &Path) -> VectorStorageEnum {
    open_simple_dense_vector_storage(
        rocksdb_wrapper::open_db(dir, &[rocksdb_wrapper::DB_VECTOR_CF]).unwrap(),
        rocksdb_wrapper::DB_VECTOR_CF,
        DIMS,
        DISTANCE,
        &AtomicBool::new(false),
    )
    .unwrap()
}

#[cfg(target_os = "linux")]
fn async_memmap_storage(dir: &std::path::Path) -> VectorStorageEnum {
    open_memmap_vector_storage_with_async_io(dir, DIMS, DISTANCE, true).unwrap()
}

fn scalar_u8() -> WithQuantization {
    let config = ScalarQuantizationConfig {
        r#type: crate::types::ScalarType::Int8,
        quantile: Some(0.5),
        always_ram: Some(true),
    }
    .into();

    let sampler = {
        let rng = StdRng::seed_from_u64(SEED);
        Box::new(rng.sample_iter(rand_distr::Normal::new(0.0, 8.0).unwrap()))
    };

    (config, sampler)
}

fn product_x4() -> WithQuantization {
    let config = ProductQuantizationConfig {
        compression: crate::types::CompressionRatio::X4,
        always_ram: Some(true),
    }
    .into();

    let sampler = {
        let rng = rng();
        Box::new(rng.sample_iter(rand::distr::StandardUniform))
    };

    (config, sampler)
}

fn binary() -> WithQuantization {
    let config = BinaryQuantizationConfig {
        always_ram: Some(true),
    }
    .into();

    let sampler = {
        let rng = StdRng::seed_from_u64(SEED);
        Box::new(
            rng.sample_iter(rand::distr::Uniform::new_inclusive(-1.0, 1.0).unwrap())
                .map(|x| f32::from(x as u8)),
        )
    };

    (config, sampler)
}

enum QueryVariant {
    RecoBestScore,
    RecoSumScores,
    Discovery,
    Context,
}

fn scoring_equivalency(
    query_variant: QueryVariant,
    other_storage: impl FnOnce(&std::path::Path) -> VectorStorageEnum,
    with_quantization: Option<WithQuantization>,
) -> Result<()> {
    let (quant_config, quant_sampler) = with_quantization
        .map(|v| (Some(v.0), Some(v.1)))
        .unwrap_or_default();

    let raw_dir = tempfile::Builder::new().prefix("raw-storage").tempdir()?;

    let db = rocksdb_wrapper::open_db(raw_dir.path(), &[rocksdb_wrapper::DB_VECTOR_CF])?;

    let mut raw_storage = open_simple_dense_vector_storage(
        db,
        rocksdb_wrapper::DB_VECTOR_CF,
        DIMS,
        DISTANCE,
        &AtomicBool::default(),
    )?;

    let mut rng = StdRng::seed_from_u64(SEED);
    let mut sampler = quant_sampler.unwrap_or(Box::new(sampler(rng.clone())));

    super::utils::insert_distributed_vectors(DIMS, &mut raw_storage, NUM_POINTS, &mut sampler)?;

    let mut id_tracker = FixtureIdTracker::new(NUM_POINTS);
    super::utils::delete_random_vectors(
        &mut rng,
        &mut raw_storage,
        &mut id_tracker,
        NUM_POINTS / 10,
    )?;

    let other_dir = tempfile::Builder::new().prefix("other-storage").tempdir()?;

    let mut other_storage = other_storage(other_dir.path());

    let mut iter = (0..NUM_POINTS).map(|i| {
        let i = i as PointOffsetType;
        let vec = raw_storage.get_vector(i);
        let deleted = raw_storage.is_deleted_vector(i);
        (vec, deleted)
    });
    other_storage.update_from(&mut iter, &Default::default())?;

    let quant_dir = tempfile::Builder::new().prefix("quant-storage").tempdir()?;
    let quantized_vectors = if let Some(config) = &quant_config {
        Some(QuantizedVectors::create(
            &other_storage,
            config,
            quant_dir.path(),
            4,
            &AtomicBool::new(false),
        )?)
    } else {
        None
    };

    let attempts = 50;
    for i in 0..attempts {
        let query = random_query(&query_variant, &mut rng, &mut sampler);

        let raw_scorer = new_raw_scorer_for_test(
            query.clone(),
            &raw_storage,
            id_tracker.deleted_point_bitslice(),
        )
        .unwrap();

        let is_stopped = AtomicBool::new(false);

        let other_scorer = match &quantized_vectors {
            Some(quantized_storage) => quantized_storage
                .raw_scorer(
                    query.clone(),
                    id_tracker.deleted_point_bitslice(),
                    other_storage.deleted_vector_bitslice(),
                    &is_stopped,
                    HardwareCounterCell::new(),
                )
                .unwrap(),
            None => new_raw_scorer_for_test(
                query.clone(),
                &other_storage,
                id_tracker.deleted_point_bitslice(),
            )
            .unwrap(),
        };

        let points =
            (0..other_storage.total_vector_count() as _).choose_multiple(&mut rng, SAMPLE_SIZE);

        let raw_scores = score(&*raw_scorer, &points);
        let other_scores = score(&*other_scorer, &points);

        // Compare scores
        if quantized_vectors.is_none() {
            // both calculations are done on raw vectors, so score should be exactly the same
            assert_eq!(
                raw_scores, other_scores,
                "Scorer results are not equal, attempt: {i}, query: {query:?}"
            );
        } else {
            // Quantization is used for the other storage, so score should be similar
            // but not necessarily the exact same. Recommend query has a step function,
            // so small differences in similarities can lead to very different scores

            let top = SAMPLE_SIZE / 10;

            let raw_top: HashSet<_> = raw_scores
                .iter()
                .sorted()
                .rev()
                .take(top)
                .map(|p| p.idx)
                .collect();
            let other_top: HashSet<_> = other_scores
                .iter()
                .sorted()
                .rev()
                .take(top)
                .map(|p| p.idx)
                .collect();

            let intersection = raw_top.intersection(&other_top).count();

            assert!(
                (intersection as f32 / top as f32) >= 0.7, // at least 70% of top 10% results should be shared
                "Top results from scorers are not similar, attempt {i}:
                top raw: {raw_top:?},
                top other: {other_top:?}
                only {intersection} of {top} top results are shared",
            );
        }
    }

    Ok(())
}

#[rstest]
fn compare_scoring_equivalency(
    #[values(
        QueryVariant::RecoBestScore,
        QueryVariant::RecoSumScores,
        QueryVariant::Discovery,
        QueryVariant::Context
    )]
    query_variant: QueryVariant,
    #[values(ram_storage)] other_storage: impl FnOnce(&std::path::Path) -> VectorStorageEnum,
    #[values(None, Some(product_x4()), Some(scalar_u8()), Some(binary()))]
    quantization_config: Option<WithQuantization>,
) -> Result<()> {
    scoring_equivalency(query_variant, other_storage, quantization_config)
}

#[cfg(target_os = "linux")]
#[rstest]
fn async_compare_scoring_equivalency(
    #[values(
        QueryVariant::RecoBestScore,
        QueryVariant::RecoSumScores,
        QueryVariant::Discovery,
        QueryVariant::Context
    )]
    query_variant: QueryVariant,
    #[values(async_memmap_storage)] other_storage: impl FnOnce(&std::path::Path) -> VectorStorageEnum,
) -> Result<()> {
    scoring_equivalency(query_variant, other_storage, None)
}
