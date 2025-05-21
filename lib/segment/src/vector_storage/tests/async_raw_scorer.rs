use bitvec::slice::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use itertools::Itertools;
use rand::SeedableRng as _;
use rand::seq::IteratorRandom as _;

use super::utils::{Result, delete_random_vectors, insert_distributed_vectors, sampler};
use crate::data_types::vectors::QueryVector;
use crate::fixtures::payload_context_fixture::FixtureIdTracker;
use crate::id_tracker::IdTracker;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::types::Distance;
use crate::vector_storage::VectorStorageEnum;
use crate::vector_storage::dense::memmap_dense_vector_storage::open_memmap_vector_storage_with_async_io;
use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
use crate::vector_storage::vector_storage_base::VectorStorage;

#[test]
fn async_raw_scorer_cosine() -> Result<()> {
    test_async_raw_scorer_defaults(Distance::Cosine)
}

#[test]
fn async_raw_scorer_euclid() -> Result<()> {
    test_async_raw_scorer_defaults(Distance::Euclid)
}

#[test]
fn async_raw_scorer_manhattan() -> Result<()> {
    test_async_raw_scorer_defaults(Distance::Manhattan)
}

#[test]
fn async_raw_scorer_dot() -> Result<()> {
    test_async_raw_scorer_defaults(Distance::Dot)
}

fn test_async_raw_scorer_defaults(distance: Distance) -> Result<()> {
    test_async_raw_scorer(6942, 128, distance, 1024, 128, 256)
}

fn test_async_raw_scorer(
    seed: u64,
    dim: usize,
    distance: Distance,
    points: usize,
    delete: usize,
    score: usize,
) -> Result<()> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);

    let dir = tempfile::Builder::new()
        .prefix("immutable-storage")
        .tempdir()?;

    let mut storage = open_memmap_vector_storage_with_async_io(dir.path(), dim, distance, true)?;

    let mut id_tracker = FixtureIdTracker::new(points);

    {
        let mut volatile_storage = new_volatile_dense_vector_storage(dim, distance);

        insert_random_vectors(&mut rng, dim, &mut volatile_storage, points)?;
        delete_random_vectors(&mut rng, &mut volatile_storage, &mut id_tracker, delete)?;

        let mut iter = (0..points).map(|i| {
            let i = i as PointOffsetType;
            let vec = volatile_storage.get_vector(i);
            let deleted = volatile_storage.is_deleted_vector(i);
            (vec, deleted)
        });
        storage.update_from(&mut iter, &Default::default())?;
    }

    for _ in 0..score {
        test_random_score(&mut rng, dim, &storage, id_tracker.deleted_point_bitslice())?;
    }

    Ok(())
}

fn insert_random_vectors(
    rng: &mut impl rand::Rng,
    dim: usize,
    storage: &mut VectorStorageEnum,
    vectors: usize,
) -> Result<()> {
    insert_distributed_vectors(dim, storage, vectors, &mut sampler(rng))
}

fn test_random_score(
    mut rng: impl rand::Rng,
    dim: usize,
    storage: &VectorStorageEnum,
    deleted_points: &BitSlice,
) -> Result<()> {
    let query: QueryVector = sampler(&mut rng).take(dim).collect_vec().into();

    let mut scorer = FilteredScorer::new_for_test(query.clone(), storage, deleted_points);

    let mut async_scorer = FilteredScorer::new(
        query,
        storage,
        None,
        None,
        deleted_points,
        HardwareCounterCell::new(),
    )?;

    let points = rng.random_range(1..storage.total_vector_count());
    let points = (0..storage.total_vector_count() as _).choose_multiple(&mut rng, points);

    let res = scorer.score_points(&mut points.clone(), 0).collect_vec();
    let async_res = async_scorer
        .score_points(&mut points.clone(), 0)
        .collect_vec();

    assert_eq!(res, async_res);
    Ok(())
}
