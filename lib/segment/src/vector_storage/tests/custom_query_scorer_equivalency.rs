use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::{error, result};

use atomic_refcell::AtomicRefCell;
use itertools::Itertools;
use rand::rngs::StdRng;
use rand::seq::IteratorRandom;
use rand::{thread_rng, Rng, SeedableRng};
use rstest::rstest;

use super::utils::sampler;
use crate::common::rocksdb_wrapper;
use crate::data_types::vectors::{QueryVector, VectorElementType};
use crate::fixtures::payload_context_fixture::FixtureIdTracker;
use crate::id_tracker::id_tracker_base::IdTracker;
use crate::types::{
    BinaryQuantizationConfig, Distance, ProductQuantizationConfig, QuantizationConfig,
    ScalarQuantizationConfig,
};
#[cfg(target_os = "linux")]
use crate::vector_storage::memmap_vector_storage::open_memmap_vector_storage_with_async_io;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::query::context_query::{ContextPair, ContextQuery};
use crate::vector_storage::query::discovery_query::DiscoveryQuery;
use crate::vector_storage::query::reco_query::RecoQuery;
use crate::vector_storage::simple_dense_vector_storage::open_simple_vector_storage;
use crate::vector_storage::tests::utils::score;
use crate::vector_storage::{new_raw_scorer, VectorStorage, VectorStorageEnum};

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
    sampler: &mut impl Iterator<Item = VectorElementType>,
) -> QueryVector {
    match query_variant {
        QueryVariant::Recommend => random_reco_query(rnd, sampler),
        QueryVariant::Discovery => random_discovery_query(rnd, sampler),
        QueryVariant::Context => random_context_query(rnd, sampler),
    }
}

fn random_reco_query<R: Rng + ?Sized>(
    rnd: &mut R,
    sampler: &mut impl Iterator<Item = VectorElementType>,
) -> QueryVector {
    let num_positives: usize = rnd.gen_range(0..MAX_EXAMPLES);
    let num_negatives: usize = rnd.gen_range(1..MAX_EXAMPLES);

    let positives = (0..num_positives)
        .map(|_| sampler.take(DIMS).collect_vec().into())
        .collect_vec();

    let negatives = (0..num_negatives)
        .map(|_| sampler.take(DIMS).collect_vec().into())
        .collect_vec();

    RecoQuery::new(positives, negatives).into()
}

fn random_discovery_query<R: Rng + ?Sized>(
    rnd: &mut R,
    sampler: &mut impl Iterator<Item = VectorElementType>,
) -> QueryVector {
    let num_pairs: usize = rnd.gen_range(0..MAX_EXAMPLES);

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
    sampler: &mut impl Iterator<Item = VectorElementType>,
) -> QueryVector {
    let num_pairs: usize = rnd.gen_range(0..MAX_EXAMPLES);

    let pairs = (0..num_pairs)
        .map(|_| {
            let positive = sampler.take(DIMS).collect_vec().into();
            let negative = sampler.take(DIMS).collect_vec().into();
            ContextPair { positive, negative }
        })
        .collect_vec();

    ContextQuery::new(pairs).into()
}

fn ram_storage(dir: &Path) -> AtomicRefCell<VectorStorageEnum> {
    let storage = open_simple_vector_storage(
        rocksdb_wrapper::open_db(dir, &[rocksdb_wrapper::DB_VECTOR_CF]).unwrap(),
        rocksdb_wrapper::DB_VECTOR_CF,
        DIMS,
        DISTANCE,
    )
    .unwrap();

    Arc::into_inner(storage).unwrap()
}

#[cfg(target_os = "linux")]
fn async_memmap_storage(dir: &std::path::Path) -> AtomicRefCell<VectorStorageEnum> {
    let storage = open_memmap_vector_storage_with_async_io(dir, DIMS, DISTANCE, true).unwrap();
    Arc::into_inner(storage).unwrap()
}

fn scalar_u8() -> Option<WithQuantization> {
    let config = ScalarQuantizationConfig {
        r#type: crate::types::ScalarType::Int8,
        quantile: Some(0.5),
        always_ram: Some(true),
    }
    .into();

    let sampler = {
        let rng = StdRng::seed_from_u64(SEED);
        Box::new(
            rng.sample_iter(
                rand_distr::Normal::new(
                    num_traits::NumCast::from(0.0).unwrap(),
                    num_traits::NumCast::from(8.0).unwrap(),
                )
                .unwrap(),
            ),
        )
    };

    Some((config, sampler))
}

fn product_x4() -> Option<WithQuantization> {
    let config = ProductQuantizationConfig {
        compression: crate::types::CompressionRatio::X4,
        always_ram: Some(true),
    }
    .into();

    let sampler = {
        let rng = thread_rng();
        Box::new(rng.sample_iter(rand::distributions::Standard))
    };

    Some((config, sampler))
}

fn binary() -> Option<WithQuantization> {
    let config = BinaryQuantizationConfig {
        always_ram: Some(true),
    }
    .into();

    let sampler = {
        let rng = StdRng::seed_from_u64(SEED);
        Box::new(
            rng.sample_iter(rand::distributions::Uniform::new_inclusive(-1.0, 1.0))
                .map(|x| num_traits::NumCast::from(x as u8).unwrap()),
        )
    };

    Some((config, sampler))
}

enum QueryVariant {
    Recommend,
    Discovery,
    Context,
}

fn scoring_equivalency(
    query_variant: QueryVariant,
    other_storage: impl FnOnce(&std::path::Path) -> AtomicRefCell<VectorStorageEnum>,
    with_quantization: Option<WithQuantization>,
) -> Result<()> {
    let (quant_config, quant_sampler) = with_quantization
        .map(|v| (Some(v.0), Some(v.1)))
        .unwrap_or_default();

    let raw_dir = tempfile::Builder::new().prefix("raw-storage").tempdir()?;

    let db = rocksdb_wrapper::open_db(raw_dir.path(), &[rocksdb_wrapper::DB_VECTOR_CF])?;

    let raw_storage =
        open_simple_vector_storage(db, rocksdb_wrapper::DB_VECTOR_CF, DIMS, DISTANCE)?;

    let mut raw_storage = raw_storage.borrow_mut();

    let mut rng = StdRng::seed_from_u64(SEED);
    let mut sampler = quant_sampler.unwrap_or(Box::new(sampler(rng.clone())));

    super::utils::insert_distributed_vectors(&mut *raw_storage, NUM_POINTS, &mut sampler)?;

    let mut id_tracker = FixtureIdTracker::new(NUM_POINTS);
    super::utils::delete_random_vectors(
        &mut rng,
        &mut *raw_storage,
        &mut id_tracker,
        NUM_POINTS / 10,
    )?;

    let other_dir = tempfile::Builder::new().prefix("other-storage").tempdir()?;

    let other_storage = other_storage(other_dir.path());
    let mut other_storage = other_storage.borrow_mut();

    other_storage.update_from(&raw_storage, &mut (0..NUM_POINTS as _), &Default::default())?;

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

        let raw_scorer = new_raw_scorer(
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
                )
                .unwrap(),
            None => new_raw_scorer(
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
                "Scorer results are not equal, attempt: {}, query: {:?}",
                i, query
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
        QueryVariant::Recommend,
        QueryVariant::Discovery,
        QueryVariant::Context
    )]
    query_variant: QueryVariant,
    #[values(ram_storage)] other_storage: impl FnOnce(
        &std::path::Path,
    ) -> AtomicRefCell<VectorStorageEnum>,

    #[values(None, product_x4(), scalar_u8(), binary())] quantization_config: Option<
        WithQuantization,
    >,
) -> Result<()> {
    scoring_equivalency(query_variant, other_storage, quantization_config)
}

#[cfg(target_os = "linux")]
#[rstest]
fn async_compare_scoring_equivalency(
    #[values(
        QueryVariant::Recommend,
        QueryVariant::Discovery,
        QueryVariant::Context
    )]
    query_variant: QueryVariant,

    #[values(async_memmap_storage)] other_storage: impl FnOnce(
        &std::path::Path,
    ) -> AtomicRefCell<VectorStorageEnum>,
) -> Result<()> {
    scoring_equivalency(query_variant, other_storage, None)
}
