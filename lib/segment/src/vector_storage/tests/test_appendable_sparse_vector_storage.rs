use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic_refcell::AtomicRefCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use sparse::common::sparse_vector::SparseVector;
use tempfile::Builder;

use crate::common::rocksdb_wrapper::{DB_VECTOR_CF, open_db};
use crate::data_types::vectors::QueryVector;
use crate::fixtures::payload_context_fixture::FixtureIdTracker;
use crate::id_tracker::IdTrackerSS;
use crate::vector_storage::query::RecoQuery;
use crate::vector_storage::sparse::mmap_sparse_vector_storage::MmapSparseVectorStorage;
use crate::vector_storage::sparse::simple_sparse_vector_storage::open_simple_sparse_vector_storage;
use crate::vector_storage::{
    DEFAULT_STOPPED, VectorStorage, VectorStorageEnum, new_raw_scorer_for_test,
};

fn do_test_delete_points(storage: &mut VectorStorageEnum) {
    let points: Vec<SparseVector> = vec![
        vec![(0, 1.0), (2, 1.0), (3, 1.0)],
        vec![(0, 1.0), (2, 1.0)],
        vec![(0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0)],
        vec![(0, 1.0), (1, 1.0), (3, 1.0)],
        vec![(0, 1.0)],
    ]
    .into_iter()
    .map(|v| v.try_into().unwrap())
    .collect();

    let delete_mask = [false, false, true, true, false];
    let id_tracker: Arc<AtomicRefCell<IdTrackerSS>> =
        Arc::new(AtomicRefCell::new(FixtureIdTracker::new(points.len())));

    let borrowed_id_tracker = id_tracker.borrow_mut();

    let hw_counter = HardwareCounterCell::new();

    // Insert all points
    for (i, vec) in points.iter().enumerate() {
        storage
            .insert_vector(i as PointOffsetType, vec.into(), &hw_counter)
            .unwrap();
    }

    // Check that all points are inserted
    for (i, vec) in points.iter().enumerate() {
        let stored_vec = storage.get_vector(i as PointOffsetType);
        let sparse: &SparseVector = stored_vec.as_vec_ref().try_into().unwrap();
        assert_eq!(sparse, vec);
    }

    // Delete select number of points
    delete_mask
        .into_iter()
        .enumerate()
        .filter(|(_, d)| *d)
        .for_each(|(i, _)| {
            storage.delete_vector(i as PointOffsetType).unwrap();
        });
    assert_eq!(
        storage.deleted_vector_count(),
        2,
        "2 vectors must be deleted"
    );

    // Check that deleted points are deleted through raw scorer
    // Because raw scorer for nearest Query is incorrect
    // (nearest search is processed using inverted index),
    // use Recommend query to simulate nearest search
    let vector: SparseVector = vec![(0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0)]
        .try_into()
        .unwrap();
    let query_vector = QueryVector::RecommendBestScore(RecoQuery {
        positives: vec![vector.into()],
        negatives: vec![],
    });
    // Because nearest search for raw scorer is incorrect,
    let scorer = new_raw_scorer_for_test(
        query_vector,
        storage,
        borrowed_id_tracker.deleted_point_bitslice(),
    )
    .unwrap();
    let closest = scorer
        .peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 5, &DEFAULT_STOPPED)
        .unwrap();
    drop(scorer);
    assert_eq!(closest.len(), 3, "must have 3 vectors, 2 are deleted");
    assert_eq!(closest[0].idx, 0);
    assert_eq!(closest[1].idx, 1);
    assert_eq!(closest[2].idx, 4);

    // Delete 1, re-delete 2
    storage.delete_vector(1 as PointOffsetType).unwrap();
    storage.delete_vector(2 as PointOffsetType).unwrap();
    assert_eq!(
        storage.deleted_vector_count(),
        3,
        "3 vectors must be deleted"
    );

    // Delete all
    storage.delete_vector(0 as PointOffsetType).unwrap();
    storage.delete_vector(4 as PointOffsetType).unwrap();
    assert_eq!(
        storage.deleted_vector_count(),
        5,
        "all vectors must be deleted"
    );
}

fn do_test_update_from_delete_points(storage: &mut VectorStorageEnum) {
    let points: Vec<Option<SparseVector>> = vec![
        Some(vec![(0, 1.0), (2, 1.0), (3, 1.0)]),
        Some(vec![(0, 1.0), (2, 1.0)]),
        None,
        None,
        Some(vec![(0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0)]),
        Some(vec![(0, 1.0), (1, 1.0), (3, 1.0)]),
        None,
    ]
    .into_iter()
    .map(|opt| opt.map(|v| v.try_into().unwrap()))
    .collect();

    let id_tracker: Arc<AtomicRefCell<IdTrackerSS>> =
        Arc::new(AtomicRefCell::new(FixtureIdTracker::new(points.len())));

    let hw_counter = HardwareCounterCell::new();

    let borrowed_id_tracker = id_tracker.borrow_mut();
    {
        let dir2 = Builder::new().prefix("db_dir").tempdir().unwrap();
        let db = open_db(dir2.path(), &[DB_VECTOR_CF]).unwrap();
        let mut storage2 =
            open_simple_sparse_vector_storage(db, DB_VECTOR_CF, &AtomicBool::new(false)).unwrap();

        points.iter().enumerate().for_each(|(i, opt_vec)| {
            if let Some(vec) = opt_vec {
                storage2
                    .insert_vector(i as PointOffsetType, vec.into(), &hw_counter)
                    .unwrap();
            } else {
                storage2.delete_vector(i as PointOffsetType).unwrap();
            }
        });

        let mut iter = (0..points.len()).map(|i| {
            let i = i as PointOffsetType;
            let vec = storage2.get_vector(i);
            let deleted = storage2.is_deleted_vector(i);
            (vec, deleted)
        });
        storage.update_from(&mut iter, &Default::default()).unwrap();
    }

    assert_eq!(
        storage.deleted_vector_count(),
        3,
        "3 vectors must be deleted from other storage"
    );

    // Check that deleted points are deleted through raw scorer
    // Because raw scorer for nearest Query is incorrect
    // (nearest search is processed using inverted index),
    // use Recommend query to simulate nearest search
    let vector: SparseVector = vec![(0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0)]
        .try_into()
        .unwrap();
    let query_vector = QueryVector::RecommendBestScore(RecoQuery {
        positives: vec![vector.into()],
        negatives: vec![],
    });
    let scorer = new_raw_scorer_for_test(
        query_vector,
        storage,
        borrowed_id_tracker.deleted_point_bitslice(),
    )
    .unwrap();
    let closest = scorer
        .peek_top_iter(&mut [0, 1, 2, 3, 4, 5].iter().cloned(), 5, &DEFAULT_STOPPED)
        .unwrap();
    drop(scorer);

    assert_eq!(
        closest.len(),
        4,
        "must have 4 vectors, 3 are deleted. closest = {closest:?}"
    );
    assert_eq!(closest[0].idx, 4);
    assert_eq!(closest[1].idx, 0);
    assert_eq!(closest[2].idx, 5);
    assert_eq!(closest[3].idx, 1);

    // Delete all
    storage.delete_vector(0 as PointOffsetType).unwrap();
    storage.delete_vector(1 as PointOffsetType).unwrap();
    storage.delete_vector(4 as PointOffsetType).unwrap();
    storage.delete_vector(5 as PointOffsetType).unwrap();
    assert_eq!(
        storage.deleted_vector_count(),
        7,
        "all vectors must be deleted"
    );
}

fn do_test_persistence(open: impl Fn(&Path) -> VectorStorageEnum) {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let mut storage = open(dir.path());

    let points = vec![
        vec![(0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0)],
        vec![(0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0)],
        vec![(0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0)],
        vec![(0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0)],
        vec![(0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0)],
    ]
    .into_iter()
    .map(|v| v.try_into().unwrap())
    .collect::<Vec<SparseVector>>();

    let hw_counter = HardwareCounterCell::new();

    points.iter().enumerate().for_each(|(i, vec)| {
        storage
            .insert_vector(i as PointOffsetType, vec.into(), &hw_counter)
            .unwrap();
    });

    // Delete selective vectors
    storage.delete_vector(1).unwrap();
    storage.delete_vector(3).unwrap();
    storage.flusher()().unwrap();

    let deleted_vector_count = storage.deleted_vector_count();
    let available_vector_count = storage.available_vector_count();

    drop(storage);

    // Re-open storage and verify state
    let storage = open(dir.path());

    // Check deleted vectors are still marked as deleted
    assert!(storage.is_deleted_vector(1));
    assert!(storage.get_vector_opt(1).is_none());

    assert!(storage.is_deleted_vector(3));
    assert!(storage.get_vector_opt(3).is_none());

    // Check non-deleted vectors still have correct data
    let verify_idx = [0, 2, 4];
    for idx in verify_idx {
        let stored = storage.get_vector(idx);
        let sparse: &SparseVector = stored.as_vec_ref().try_into().unwrap();
        assert_eq!(sparse, &points[idx as usize]);
    }
    assert_eq!(storage.deleted_vector_count(), 2);

    assert_eq!(storage.deleted_vector_count(), deleted_vector_count);
    assert_eq!(storage.available_vector_count(), available_vector_count);
}

#[test]
fn test_delete_points_in_simple_sparse_vector_storage() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

    {
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let mut storage =
            open_simple_sparse_vector_storage(db, DB_VECTOR_CF, &AtomicBool::new(false)).unwrap();
        do_test_delete_points(&mut storage);
        storage.flusher()().unwrap();
    }
    let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
    let _storage =
        open_simple_sparse_vector_storage(db, DB_VECTOR_CF, &AtomicBool::new(false)).unwrap();
}

#[test]
fn test_delete_points_in_mmap_sparse_vector_storage() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let mut storage =
        VectorStorageEnum::SparseMmap(MmapSparseVectorStorage::open_or_create(dir.path()).unwrap());
    do_test_delete_points(&mut storage);

    storage.flusher()().unwrap();

    drop(storage);

    let _storage = MmapSparseVectorStorage::open_or_create(dir.path()).unwrap();
}

#[test]
fn test_update_from_delete_points_simple_sparse_vector_storage() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    {
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let mut storage =
            open_simple_sparse_vector_storage(db, DB_VECTOR_CF, &AtomicBool::new(false)).unwrap();
        do_test_update_from_delete_points(&mut storage);
        storage.flusher()().unwrap();
    }

    let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
    let _storage =
        open_simple_sparse_vector_storage(db, DB_VECTOR_CF, &AtomicBool::new(false)).unwrap();
}

#[test]
fn test_update_from_delete_points_mmap_sparse_vector_storage() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

    let mut storage =
        VectorStorageEnum::SparseMmap(MmapSparseVectorStorage::open_or_create(dir.path()).unwrap());

    do_test_update_from_delete_points(&mut storage);
    storage.flusher()().unwrap();

    drop(storage);

    let _storage =
        VectorStorageEnum::SparseMmap(MmapSparseVectorStorage::open_or_create(dir.path()).unwrap());
}

#[test]
fn test_persistence_in_mmap_sparse_vector_storage() {
    do_test_persistence(|path| {
        VectorStorageEnum::SparseMmap(MmapSparseVectorStorage::open_or_create(path).unwrap())
    });
}

#[test]
fn test_persistence_in_simple_sparse_vector_storage() {
    do_test_persistence(|path| {
        let db = open_db(path, &[DB_VECTOR_CF]).unwrap();
        open_simple_sparse_vector_storage(db, DB_VECTOR_CF, &AtomicBool::new(false)).unwrap()
    });
}
