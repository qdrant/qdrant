use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::types::PointOffsetType;
use tempfile::Builder;

use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
use crate::data_types::vectors::{DenseVector, MultiDenseVector};
use crate::types::Distance;
use crate::vector_storage::simple_multi_dense_vector_storage::open_simple_multi_dense_vector_storage;
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

fn multi_points_fixtures() -> Vec<MultiDenseVector> {
    let mut vec = Vec::new();
    for i in 0..5 {
        let value = i as f32;
        let multi = vec![
            vec![value, 0.0, value, value],
            vec![value, 0.0, value, 0.0],
            vec![value, value, value, value],
            vec![value, value, 0.0, value],
            vec![value, 0.0, 0.0, value],
        ];
        vec.push(multi);
    }
    vec
}

fn do_test_delete_points(storage: Arc<AtomicRefCell<VectorStorageEnum>>) {
    let points = multi_points_fixtures();

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
        let multi_dense: &[DenseVector] = stored_vec.as_vec_ref().try_into().unwrap();
        assert_eq!(multi_dense, vec.as_slice());
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
    // TODO(multi-dense-vector): assert points are deleted by searching through raw scorer
}

fn do_test_update_from_delete_points(storage: Arc<AtomicRefCell<VectorStorageEnum>>) {
    let points = multi_points_fixtures();

    let delete_mask = [false, false, true, true, false];

    let mut borrowed_storage = storage.borrow_mut();

    {
        let dir2 = Builder::new().prefix("db_dir").tempdir().unwrap();
        let db = open_db(dir2.path(), &[DB_VECTOR_CF]).unwrap();
        let storage2 = open_simple_multi_dense_vector_storage(
            db,
            DB_VECTOR_CF,
            4,
            Distance::Dot,
            &AtomicBool::new(false),
        )
        .unwrap();
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

    // TODO(multi-dense-vector): assert points are deleted by searching through raw scorer
}

#[test]
fn test_delete_points_in_simple_multi_dense_vector_storage() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

    {
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let storage = open_simple_multi_dense_vector_storage(
            db,
            DB_VECTOR_CF,
            4,
            Distance::Dot,
            &AtomicBool::new(false),
        )
        .unwrap();
        do_test_delete_points(storage.clone());
        storage.borrow().flusher()().unwrap();
    }
    let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
    let _storage = open_simple_multi_dense_vector_storage(
        db,
        DB_VECTOR_CF,
        4,
        Distance::Dot,
        &AtomicBool::new(false),
    )
    .unwrap();
}

#[test]
fn test_update_from_delete_points_simple_multi_dense_vector_storage() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    {
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let storage = open_simple_multi_dense_vector_storage(
            db,
            DB_VECTOR_CF,
            4,
            Distance::Dot,
            &AtomicBool::new(false),
        )
        .unwrap();
        do_test_update_from_delete_points(storage.clone());
        storage.borrow().flusher()().unwrap();
    }

    let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
    let _storage = open_simple_multi_dense_vector_storage(
        db,
        DB_VECTOR_CF,
        4,
        Distance::Dot,
        &AtomicBool::new(false),
    )
    .unwrap();
}
