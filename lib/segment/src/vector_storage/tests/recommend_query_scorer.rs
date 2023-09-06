use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::{error, result};

use atomic_refcell::AtomicRefCell;
use itertools::Itertools;
use rand::seq::IteratorRandom;
use rand::{thread_rng, Rng};
use rstest::rstest;

use crate::common::rocksdb_wrapper;
use crate::data_types::vectors::QueryVector;
use crate::fixtures::payload_context_fixture::FixtureIdTracker;
use crate::fixtures::payload_fixtures::random_vector;
use crate::id_tracker::id_tracker_base::IdTracker;
use crate::types::{
    Distance, ProductQuantization, ProductQuantizationConfig, QuantizationConfig,
    ScalarQuantization, ScalarQuantizationConfig,
};
#[cfg(target_os = "linux")]
use crate::vector_storage::memmap_vector_storage::open_memmap_vector_storage_with_async_io;
use crate::vector_storage::query::RecoQuery;
use crate::vector_storage::simple_vector_storage::open_simple_vector_storage;
use crate::vector_storage::tests::utils::score;
use crate::vector_storage::{new_raw_scorer, VectorStorage, VectorStorageEnum};
const DIMS: usize = 512;
const NUM_POINTS: usize = 5_000;
const NUM_DELETE_POINTS: usize = 300;
const DISTANCE: Distance = Distance::Dot;
const MAX_EXAMPLES: usize = 100;

type Result<T, E = Error> = result::Result<T, E>;
type Error = Box<dyn error::Error>;

fn random_reco_query<R: Rng + ?Sized>(rnd: &mut R, dim: usize) -> QueryVector {
    let num_positives: usize = rnd.gen_range(0..MAX_EXAMPLES);
    let num_negatives: usize = rnd.gen_range(0..MAX_EXAMPLES);

    let positives = (0..num_positives)
        .map(|_| random_vector(rnd, dim))
        .collect_vec();

    let negatives = (0..num_negatives)
        .map(|_| random_vector(rnd, dim))
        .collect_vec();

    QueryVector::Recommend(RecoQuery::new(positives, negatives))
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

fn u8_quant_config() -> Option<QuantizationConfig> {
    Some(QuantizationConfig::Scalar(ScalarQuantization {
        scalar: ScalarQuantizationConfig {
            r#type: crate::types::ScalarType::Int8,
            quantile: None,
            always_ram: Some(true),
        },
    }))
}

// fn pq4_quant_config() -> Option<QuantizationConfig> {
//     Some(QuantizationConfig::Product(ProductQuantization {
//         product: ProductQuantizationConfig {
//             compression: crate::types::CompressionRatio::X4,
//             always_ram: Some(true),
//         },
//     }))
// }

fn scoring_equivalency(
    other_storage: impl FnOnce(&std::path::Path) -> AtomicRefCell<VectorStorageEnum>,
    quantization_config: Option<QuantizationConfig>,
) -> Result<()> {
    let other_dir = tempfile::Builder::new().prefix("other-storage").tempdir()?;

    let mut rng = thread_rng();

    let storage = other_storage(other_dir.path());
    let mut storage = storage.borrow_mut();

    let mut id_tracker = FixtureIdTracker::new(NUM_POINTS);

    let raw_dir = tempfile::Builder::new().prefix("raw-storage").tempdir()?;

    let db = rocksdb_wrapper::open_db(raw_dir.path(), &[rocksdb_wrapper::DB_VECTOR_CF])?;

    let raw_storage =
        open_simple_vector_storage(db, rocksdb_wrapper::DB_VECTOR_CF, DIMS, DISTANCE)?;

    let mut raw_storage = raw_storage.borrow_mut();

    super::utils::insert_random_vectors(&mut rng, &mut *raw_storage, NUM_POINTS)?;
    super::utils::delete_random_vectors(
        &mut rng,
        &mut *raw_storage,
        &mut id_tracker,
        NUM_DELETE_POINTS,
    )?;

    storage.update_from(&raw_storage, &mut (0..NUM_POINTS as _), &Default::default())?;

    let quant_dir = tempfile::Builder::new().prefix("quant-storage").tempdir()?;
    if let Some(quantization_config) = quantization_config {
        dbg!("beginning quantization");
        storage.quantize(
            quant_dir.path(),
            &quantization_config,
            1,
            &AtomicBool::new(false),
        )?;
        dbg!("finished quantization");
    }

    let attempts = 50;
    for _i in 0..attempts {
        let query = random_reco_query(&mut rng, DIMS);
        // let query: QueryVector = random_vector(&mut rng, DIMS).into();

        let raw_scorer = new_raw_scorer(
            query.clone(),
            &raw_storage,
            id_tracker.deleted_point_bitslice(),
        );

        let is_stopped = AtomicBool::new(false);

        let scorer = match storage.quantized_storage() {
            Some(quantized_storage) => quantized_storage.raw_scorer(
                query,
                id_tracker.deleted_point_bitslice(),
                storage.deleted_vector_bitslice(),
                &is_stopped,
            ),
            None => new_raw_scorer(query, &storage, id_tracker.deleted_point_bitslice()),
        };

        let points = rng.gen_range(1..10);
        let points = (0..storage.total_vector_count() as _).choose_multiple(&mut rng, points);

        let raw_res = score(&*raw_scorer, &points);
        let other_res = score(&*scorer, &points);

        if storage.quantized_storage().is_none() {
            // both calculations are done on raw vectors, so score should be exactly the same
            assert_eq!(
                raw_res, other_res,
                "Scorer results are not equal, attempt: {}",
                _i
            );
        } else {
            // quantization is used for the other storage, so score should be similar but not necessarily the exact same
            raw_res
                .iter()
                .zip(other_res.iter())
                .for_each(|(raw, other)| {
                    assert!(
                        (raw.score - other.score).abs() / raw.score < 0.05,
                        "Scorer results are not similar, attempt: {}",
                        _i
                    );
                });
        }
    }

    Ok(())
}

#[rstest]
fn compare_scoring_equivalency(
    #[values(ram_storage)] other_storage: impl FnOnce(
        &std::path::Path,
    ) -> AtomicRefCell<VectorStorageEnum>,

    #[values(
        None,
        // pq4_quant_config(), 
        u8_quant_config()
    )]
    quantization_config: Option<QuantizationConfig>,
) -> Result<()> {
    scoring_equivalency(other_storage, quantization_config)
}

#[cfg(target_os = "linux")]
#[rstest]
fn async_compare_scoring_equivalency(
    #[values(async_memmap_storage)] other_storage: impl FnOnce(
        &std::path::Path,
    ) -> AtomicRefCell<VectorStorageEnum>,
) -> Result<()> {
    scoring_equivalency(other_storage, None)
}
