use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::types::PointOffsetType;
use sparse::common::sparse_vector::SparseVector;
use tempfile::Builder;

use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
use crate::types::Distance;
use crate::vector_storage::simple_sparse_vector_storage::open_simple_sparse_vector_storage;
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

fn do_test_delete_points(storage: Arc<AtomicRefCell<VectorStorageEnum>>) {
    let points: Vec<SparseVector> = vec![
        vec![(0, 1.0), (2, 1.0), (3, 1.0)].into(),
        vec![(0, 1.0), (2, 1.0)].into(),
        vec![(0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0)].into(),
        vec![(0, 1.0), (1, 1.0), (3, 1.0)].into(),
        vec![(0, 1.0)].into(),
    ];
    let delete_mask = [false, false, true, true, false];
    let mut borrowed_storage = storage.borrow_mut();

    // Insert all points
    for (i, vec) in points.iter().enumerate() {
        borrowed_storage
            .insert_vector(i as PointOffsetType, vec.into())
            .unwrap();
    }

    // Check that all points are inserted
    for (i, vec) in points.iter().enumerate() {
        let stored_vec = borrowed_storage.get_vector(i as PointOffsetType);
        let sparse: &SparseVector = stored_vec.try_into().unwrap();
        assert_eq!(sparse, vec);
    }

    // Delete select number of points
    delete_mask
        .into_iter()
        .enumerate()
        .filter(|(_, d)| *d)
        .for_each(|(i, _)| {
            borrowed_storage
                .delete_vector(i as PointOffsetType)
                .unwrap();
        });
    assert_eq!(
        borrowed_storage.deleted_vector_count(),
        2,
        "2 vectors must be deleted"
    );

    // Delete 1, re-delete 2
    borrowed_storage
        .delete_vector(1 as PointOffsetType)
        .unwrap();
    borrowed_storage
        .delete_vector(2 as PointOffsetType)
        .unwrap();
    assert_eq!(
        borrowed_storage.deleted_vector_count(),
        3,
        "3 vectors must be deleted"
    );

    // Delete all
    borrowed_storage
        .delete_vector(0 as PointOffsetType)
        .unwrap();
    borrowed_storage
        .delete_vector(4 as PointOffsetType)
        .unwrap();
    assert_eq!(
        borrowed_storage.deleted_vector_count(),
        5,
        "all vectors must be deleted"
    );
}

fn do_test_update_from_delete_points(storage: Arc<AtomicRefCell<VectorStorageEnum>>) {
    let points: Vec<SparseVector> = vec![
        vec![(0, 1.0), (2, 1.0), (3, 1.0)].into(),
        vec![(0, 1.0), (2, 1.0)].into(),
        vec![(0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0)].into(),
        vec![(0, 1.0), (1, 1.0), (3, 1.0)].into(),
        vec![(0, 1.0)].into(),
    ];
    let delete_mask = [false, false, true, true, false];
    let mut borrowed_storage = storage.borrow_mut();

    {
        let dir2 = Builder::new().prefix("db_dir").tempdir().unwrap();
        let db = open_db(dir2.path(), &[DB_VECTOR_CF]).unwrap();
        let storage2 = open_simple_sparse_vector_storage(db, DB_VECTOR_CF, Distance::Dot).unwrap();
        {
            let mut borrowed_storage2 = storage2.borrow_mut();
            points.iter().enumerate().for_each(|(i, vec)| {
                borrowed_storage2
                    .insert_vector(i as PointOffsetType, vec.into())
                    .unwrap();
                if delete_mask[i] {
                    borrowed_storage2
                        .delete_vector(i as PointOffsetType)
                        .unwrap();
                }
            });
        }
        borrowed_storage
            .update_from(
                &storage2.borrow(),
                &mut Box::new(0..points.len() as u32),
                &Default::default(),
            )
            .unwrap();
    }

    assert_eq!(
        borrowed_storage.deleted_vector_count(),
        2,
        "2 vectors must be deleted from other storage"
    );

    // Delete all
    borrowed_storage
        .delete_vector(0 as PointOffsetType)
        .unwrap();
    borrowed_storage
        .delete_vector(1 as PointOffsetType)
        .unwrap();
    borrowed_storage
        .delete_vector(4 as PointOffsetType)
        .unwrap();
    assert_eq!(
        borrowed_storage.deleted_vector_count(),
        5,
        "all vectors must be deleted"
    );
}

#[test]
fn test_delete_points_in_simple_sparse_vector_storage() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

    {
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let storage = open_simple_sparse_vector_storage(db, DB_VECTOR_CF, Distance::Dot).unwrap();
        do_test_delete_points(storage.clone());
        storage.borrow().flusher()().unwrap();
    }
    let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
    let _storage = open_simple_sparse_vector_storage(db, DB_VECTOR_CF, Distance::Dot).unwrap();
}

#[test]
fn test_update_from_delete_points_simple_sparse_vector_storage() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    {
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let storage = open_simple_sparse_vector_storage(db, DB_VECTOR_CF, Distance::Dot).unwrap();
        do_test_update_from_delete_points(storage.clone());
        storage.borrow().flusher()().unwrap();
    }

    let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
    let _storage = open_simple_sparse_vector_storage(db, DB_VECTOR_CF, Distance::Dot).unwrap();
}
