use std::sync::Arc;
use std::{error, result};

use atomic_refcell::AtomicRefCell;

use crate::common::rocksdb_wrapper;
use crate::fixtures::payload_context_fixture::FixtureIdTracker;
use crate::id_tracker::IdTracker as _;
use crate::types::{Distance, PointOffsetType};
use crate::vector_storage::memmap_vector_storage::open_memmap_vector_storage;
use crate::vector_storage::simple_vector_storage::open_simple_vector_storage;
use crate::vector_storage::vector_storage_base::VectorStorage as _;
use crate::vector_storage::{
    async_raw_scorer, new_raw_scorer, ScoredPointOffset, VectorStorageEnum,
};

type Result<T, E = Error> = result::Result<T, E>;
type Error = Box<dyn error::Error>;

#[test]
fn async_raw_scorer_dot() -> Result<()> {
    async_raw_scorer(Distance::Dot)
}

fn async_raw_scorer(distance: Distance) -> Result<()> {
    let dir = tempfile::Builder::new()
        .prefix("immutable-storage")
        .tempdir()?;

    let points = vec![
        vec![1.0, 0.0, 1.0, 1.0],
        vec![1.0, 0.0, 1.0, 0.0],
        vec![1.0, 1.0, 1.0, 1.0],
        vec![1.0, 1.0, 0.0, 1.0],
        vec![1.0, 0.0, 0.0, 0.0],
    ];

    let id_tracker = Arc::new(AtomicRefCell::new(FixtureIdTracker::new(points.len())));
    let storage = open_memmap_vector_storage(dir.path(), 4, distance)?;

    let id_tracker = id_tracker.borrow_mut();
    let mut storage = storage.borrow_mut();

    {
        let dir = tempfile::Builder::new()
            .prefix("mutable-storage")
            .tempdir()?;
        let db = rocksdb_wrapper::open_db(dir.path(), &[rocksdb_wrapper::DB_VECTOR_CF])?;
        let mutable_storage =
            open_simple_vector_storage(db, rocksdb_wrapper::DB_VECTOR_CF, 4, distance)?;
        let mut mutable_storage = mutable_storage.borrow_mut();

        for (index, vector) in points.iter().enumerate() {
            mutable_storage.insert_vector(index as PointOffsetType, vector)?;
        }

        storage.update_from(
            &mutable_storage,
            &mut (0..points.len() as _),
            &Default::default(),
        )?;
    }

    let query = vec![-1.0, -1.0, -1.0, -1.0];
    let query_points: Vec<PointOffsetType> = vec![0, 2, 4];

    let raw_scorer = new_raw_scorer(query.clone(), &storage, id_tracker.deleted_point_bitslice());

    let async_raw_scorer = if let VectorStorageEnum::Memmap(storage) = &*storage {
        async_raw_scorer::new(query, storage, id_tracker.deleted_point_bitslice())?
    } else {
        unreachable!();
    };

    let mut res = vec![ScoredPointOffset { idx: 0, score: 0. }; query_points.len()];
    let res_count = raw_scorer.score_points(&query_points, &mut res);
    res.resize(res_count, ScoredPointOffset { idx: 0, score: 0. });

    assert_eq!(res.len(), 3);
    assert_eq!(res[0].idx, 0);
    assert_eq!(res[1].idx, 2);
    assert_eq!(res[2].idx, 4);

    assert_eq!(res[2].score, -1.0);

    let mut async_res = vec![ScoredPointOffset { idx: 0, score: 0. }; query_points.len()];
    let res_count = async_raw_scorer.score_points(&query_points, &mut async_res);
    async_res.resize(res_count, ScoredPointOffset { idx: 0, score: 0. });

    assert_eq!(res, async_res);

    Ok(())
}
