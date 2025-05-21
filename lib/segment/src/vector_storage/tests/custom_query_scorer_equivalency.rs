use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::{error, result};

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use itertools::Itertools;
use rand::rngs::StdRng;
use rand::seq::IteratorRandom;
use rand::{Rng, SeedableRng};
use rstest::rstest;

use super::utils::sampler;
use crate::data_types::vectors::{QueryVector, VectorElementType};
use crate::fixtures::payload_context_fixture::FixtureIdTracker;
use crate::fixtures::query_fixtures::QueryVariant;
use crate::id_tracker::id_tracker_base::IdTracker;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::types::{
    BinaryQuantizationConfig, Distance, ProductQuantizationConfig, QuantizationConfig,
    ScalarQuantizationConfig,
};
use crate::vector_storage::VectorStorageEnum;
#[cfg(target_os = "linux")]
use crate::vector_storage::dense::memmap_dense_vector_storage::open_memmap_vector_storage_with_async_io;
use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::vector_storage_base::VectorStorage;

const DIMS: usize = 128;
const NUM_POINTS: usize = 600;
const DISTANCE: Distance = Distance::Dot;
const SAMPLE_SIZE: usize = 100;
const SEED: u64 = 42;

type Result<T, E = Error> = result::Result<T, E>;
type Error = Box<dyn error::Error>;

type Sampler<'a> = Box<dyn Iterator<Item = VectorElementType> + 'a>;

type SamplerGenerator = Box<dyn for<'a> Fn(&'a mut StdRng) -> Sampler<'a>>;

type WithQuantization = (QuantizationConfig, SamplerGenerator);

fn random_query<R: Rng + ?Sized>(
    query_variant: &QueryVariant,
    rng: &mut R,
    gen_sampler: &dyn Fn(&mut R) -> Sampler,
) -> QueryVector {
    crate::fixtures::query_fixtures::random_query(query_variant, rng, |rng| {
        gen_sampler(rng).take(DIMS).collect_vec().into()
    })
}

fn ram_storage(_dir: &Path) -> VectorStorageEnum {
    new_volatile_dense_vector_storage(DIMS, DISTANCE)
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

    let sampler: SamplerGenerator = Box::new(|rng: &mut StdRng| {
        Box::new(rng.sample_iter(rand_distr::Normal::new(0.0f32, 8.0).unwrap()))
    });

    (config, sampler)
}

fn product_x4() -> WithQuantization {
    let config = ProductQuantizationConfig {
        compression: crate::types::CompressionRatio::X4,
        always_ram: Some(true),
    }
    .into();

    let sampler: SamplerGenerator =
        Box::new(|rng: &mut StdRng| Box::new(rng.sample_iter(rand::distr::StandardUniform)));

    (config, sampler)
}

fn binary() -> WithQuantization {
    let config = BinaryQuantizationConfig {
        always_ram: Some(true),
    }
    .into();

    let sampler: SamplerGenerator = Box::new(|rng: &mut StdRng| {
        Box::new(
            rng.sample_iter(rand::distr::Uniform::new_inclusive(-1.0, 1.0).unwrap())
                .map(|x| f32::from(x as u8)),
        )
    });

    (config, sampler)
}

fn scoring_equivalency(
    query_variant: QueryVariant,
    other_storage: impl FnOnce(&std::path::Path) -> VectorStorageEnum,
    with_quantization: Option<WithQuantization>,
) -> Result<()> {
    let (quant_config, quant_sampler) = with_quantization
        .map(|v| (Some(v.0), Some(v.1)))
        .unwrap_or_default();

    let mut raw_storage = new_volatile_dense_vector_storage(DIMS, DISTANCE);

    let mut rng = StdRng::seed_from_u64(SEED);
    let gen_sampler = quant_sampler.unwrap_or_else(|| Box::new(|rng| Box::new(sampler(rng))));

    super::utils::insert_distributed_vectors(
        DIMS,
        &mut raw_storage,
        NUM_POINTS,
        &mut gen_sampler(&mut rng.clone()),
    )?;

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
        let query = random_query(&query_variant, &mut rng, &gen_sampler);

        let mut scorer = FilteredScorer::new_for_test(
            query.clone(),
            &raw_storage,
            id_tracker.deleted_point_bitslice(),
        );

        let mut other_scorer = FilteredScorer::new(
            query.clone(),
            &other_storage,
            quantized_vectors.as_ref(),
            None,
            id_tracker.deleted_point_bitslice(),
            HardwareCounterCell::new(),
        )?;

        let points =
            (0..other_storage.total_vector_count() as _).choose_multiple(&mut rng, SAMPLE_SIZE);

        let scores = scorer.score_points(&mut points.clone(), 0).collect_vec();
        let other_scores = other_scorer
            .score_points(&mut points.clone(), 0)
            .collect_vec();

        // Compare scores
        if quantized_vectors.is_none() {
            // both calculations are done on raw vectors, so score should be exactly the same
            assert_eq!(
                scores, other_scores,
                "Scorer results are not equal, attempt: {i}, query: {query:?}"
            );
        } else {
            // Quantization is used for the other storage, so score should be similar
            // but not necessarily the exact same. Recommend query has a step function,
            // so small differences in similarities can lead to very different scores

            let top = SAMPLE_SIZE / 10;

            let raw_top: HashSet<_> = scores
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
