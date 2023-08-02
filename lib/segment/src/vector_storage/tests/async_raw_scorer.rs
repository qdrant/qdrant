use std::sync::atomic::AtomicBool;
use std::{error, result};

use bitvec::slice::BitSlice;
use rand::seq::IteratorRandom as _;
use rand::SeedableRng as _;

use crate::common::rocksdb_wrapper;
use crate::fixtures::payload_context_fixture::FixtureIdTracker;
use crate::id_tracker::IdTracker;
use crate::types::{Distance, PointOffsetType};
use crate::vector_storage::common::set_async_scorer;
use crate::vector_storage::memmap_vector_storage::open_memmap_vector_storage;
use crate::vector_storage::simple_vector_storage::open_simple_vector_storage;
use crate::vector_storage::vector_storage_base::VectorStorage;
use crate::vector_storage::{
    async_raw_scorer, new_raw_scorer, RawScorer, ScoredPointOffset, VectorStorageEnum,
};

#[test]
fn async_raw_scorer_cosine() -> Result<()> {
    test_async_raw_scorer_defaults(Distance::Cosine)
}

#[test]
fn async_raw_scorer_euclid() -> Result<()> {
    test_async_raw_scorer_defaults(Distance::Euclid)
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
    set_async_scorer(true);

    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);

    let dir = tempfile::Builder::new()
        .prefix("immutable-storage")
        .tempdir()?;

    let storage = open_memmap_vector_storage(dir.path(), dim, distance)?;
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
    let start = storage.total_vector_count() as u32;
    let end = start + vectors as u32;

    let mut vector = vec![0.; storage.vector_dim()];
    let mut sampler = sampler(rng);

    for offset in start..end {
        for (item, value) in vector.iter_mut().zip(&mut sampler) {
            *item = value;
        }

        storage.insert_vector(offset, &vector)?;
    }

    Ok(())
}

fn delete_random_vectors(
    rng: &mut impl rand::Rng,
    storage: &mut impl VectorStorage,
    id_tracker: &mut impl IdTracker,
    vectors: usize,
) -> Result<()> {
    let offsets = (0..storage.total_vector_count() as _).choose_multiple(rng, vectors);

    for offset in offsets {
        storage.delete_vector(offset)?;
        id_tracker.drop(crate::types::ExtendedPointId::NumId(offset.into()))?;
    }

    Ok(())
}

fn test_random_score(
    mut rng: impl rand::Rng,
    storage: &VectorStorageEnum,
    deleted_points: &BitSlice,
) -> Result<()> {
    let query: Vec<_> = sampler(&mut rng).take(storage.vector_dim()).collect();

    let raw_scorer = new_raw_scorer(query.clone(), storage, deleted_points);

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

fn score(scorer: &dyn RawScorer, points: &[PointOffsetType]) -> Vec<ScoredPointOffset> {
    let mut scores = vec![Default::default(); points.len()];
    let scored = scorer.score_points(points, &mut scores);
    scores.resize_with(scored, Default::default);
    scores
}

fn sampler(rng: impl rand::Rng) -> impl Iterator<Item = f32> {
    rng.sample_iter(rand::distributions::Uniform::new_inclusive(-1., 1.))
}

type Result<T, E = Error> = result::Result<T, E>;
type Error = Box<dyn error::Error>;
