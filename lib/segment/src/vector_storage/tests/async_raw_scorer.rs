use std::sync::atomic::AtomicBool;

use bitvec::slice::BitSlice;
use itertools::Itertools;
use rand::seq::IteratorRandom as _;
use rand::SeedableRng as _;

use super::utils::{delete_random_vectors, insert_distributed_vectors, sampler, score, Result};
use crate::common::rocksdb_wrapper;
use crate::data_types::vectors::QueryVector;
use crate::fixtures::payload_context_fixture::FixtureIdTracker;
use crate::id_tracker::IdTracker;
use crate::types::Distance;
use crate::vector_storage::memmap_vector_storage::open_memmap_vector_storage_with_async_io;
use crate::vector_storage::simple_dense_vector_storage::open_simple_vector_storage;
use crate::vector_storage::vector_storage_base::VectorStorage;
use crate::vector_storage::{async_raw_scorer, new_raw_scorer, VectorStorageEnum};

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

    let storage = open_memmap_vector_storage_with_async_io(dir.path(), dim, distance, true)?;
    let mut storage = storage.borrow_mut();

    let mut id_tracker = FixtureIdTracker::new(points);

    {
        let dir = tempfile::Builder::new()
            .prefix("mutable-storage")
            .tempdir()?;

        let db = rocksdb_wrapper::open_db(dir.path(), &[rocksdb_wrapper::DB_VECTOR_CF])?;

        let mutable_storage =
            open_simple_vector_storage(db, rocksdb_wrapper::DB_VECTOR_CF, 4, distance)?;

        let mut mutable_storage = mutable_storage.borrow_mut();

        insert_random_vectors(&mut rng, &mut *mutable_storage, points)?;
        delete_random_vectors(&mut rng, &mut *mutable_storage, &mut id_tracker, delete)?;

        storage.update_from(&mutable_storage, &mut (0..points as _), &Default::default())?;
    }

    for _ in 0..score {
        test_random_score(&mut rng, &storage, id_tracker.deleted_point_bitslice())?;
    }

    Ok(())
}
fn insert_random_vectors(
    rng: &mut impl rand::Rng,
    storage: &mut impl VectorStorage,
    vectors: usize,
) -> Result<()> {
    insert_distributed_vectors(storage, vectors, &mut sampler(rng))
}

fn test_random_score(
    mut rng: impl rand::Rng,
    storage: &VectorStorageEnum,
    deleted_points: &BitSlice,
) -> Result<()> {
    let query: QueryVector = sampler(&mut rng)
        .take(storage.vector_dim())
        .collect_vec()
        .into();

    let raw_scorer = new_raw_scorer(query.clone(), storage, deleted_points).unwrap();

    let is_stopped = AtomicBool::new(false);
    let async_raw_scorer = if let VectorStorageEnum::Memmap(storage) = storage {
        async_raw_scorer::new(query, storage, deleted_points, &is_stopped)?
    } else {
        unreachable!();
    };

    let points = rng.gen_range(1..storage.total_vector_count());
    let points = (0..storage.total_vector_count() as _).choose_multiple(&mut rng, points);

    let res = score(&*raw_scorer, &points);
    let async_res = score(&*async_raw_scorer, &points);

    assert_eq!(res, async_res);

    Ok(())
}
