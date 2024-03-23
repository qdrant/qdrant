use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::types::{PointOffsetType, ScoredPointOffset};
use tempfile::Builder;

use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
use crate::data_types::vectors::QueryVector;
use crate::fixtures::payload_context_fixture::FixtureIdTracker;
use crate::id_tracker::{IdTracker, IdTrackerSS};
use crate::types::{Distance, PointIdType, QuantizationConfig, ScalarQuantizationConfig};
use crate::vector_storage::dense::appendable_mmap_dense_vector_storage::open_appendable_memmap_vector_storage;
use crate::vector_storage::dense::simple_dense_vector_storage::open_simple_vector_storage;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{new_raw_scorer, VectorStorage, VectorStorageEnum};

fn do_test_delete_points(storage: Arc<AtomicRefCell<VectorStorageEnum>>) {
    let points = [
        vec![1.0, 0.0, 1.0, 1.0],
        vec![1.0, 0.0, 1.0, 0.0],
        vec![1.0, 1.0, 1.0, 1.0],
        vec![1.0, 1.0, 0.0, 1.0],
        vec![1.0, 0.0, 0.0, 0.0],
    ];
    let delete_mask = [false, false, true, true, false];
    let id_tracker: Arc<AtomicRefCell<IdTrackerSS>> =
        Arc::new(AtomicRefCell::new(FixtureIdTracker::new(points.len())));

    let borrowed_id_tracker = id_tracker.borrow_mut();
    let mut borrowed_storage = storage.borrow_mut();

    for (i, vec) in points.iter().enumerate() {
        borrowed_storage
            .insert_vector(i as PointOffsetType, vec.as_slice().into())
            .unwrap();
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

    let vector = vec![0.0, 1.0, 1.1, 1.0];
    let query = vector.as_slice().into();
    let closest = new_raw_scorer(
        query,
        &borrowed_storage,
        borrowed_id_tracker.deleted_point_bitslice(),
    )
    .unwrap()
    .peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 5);
    assert_eq!(closest.len(), 3, "must have 3 vectors, 2 are deleted");
    assert_eq!(closest[0].idx, 0);
    assert_eq!(closest[1].idx, 1);
    assert_eq!(closest[2].idx, 4);

    // Delete 1, redelete 2
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

    let vector = vec![1.0, 0.0, 0.0, 0.0];
    let query = vector.as_slice().into();
    let closest = new_raw_scorer(
        query,
        &borrowed_storage,
        borrowed_id_tracker.deleted_point_bitslice(),
    )
    .unwrap()
    .peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 5);
    assert_eq!(closest.len(), 2, "must have 2 vectors, 3 are deleted");
    assert_eq!(closest[0].idx, 4);
    assert_eq!(closest[1].idx, 0);

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

    let vector = vec![1.0, 0.0, 0.0, 0.0];
    let query = vector.as_slice().into();
    let closest = new_raw_scorer(
        query,
        &borrowed_storage,
        borrowed_id_tracker.deleted_point_bitslice(),
    )
    .unwrap()
    .peek_top_all(5);
    assert!(closest.is_empty(), "must have no results, all deleted");
}

fn do_test_update_from_delete_points(storage: Arc<AtomicRefCell<VectorStorageEnum>>) {
    let points = [
        vec![1.0, 0.0, 1.0, 1.0],
        vec![1.0, 0.0, 1.0, 0.0],
        vec![1.0, 1.0, 1.0, 1.0],
        vec![1.0, 1.0, 0.0, 1.0],
        vec![1.0, 0.0, 0.0, 0.0],
    ];
    let delete_mask = [false, false, true, true, false];
    let id_tracker: Arc<AtomicRefCell<IdTrackerSS>> =
        Arc::new(AtomicRefCell::new(FixtureIdTracker::new(points.len())));
    let borrowed_id_tracker = id_tracker.borrow_mut();
    let mut borrowed_storage = storage.borrow_mut();

    {
        let dir2 = Builder::new().prefix("db_dir").tempdir().unwrap();
        let db = open_db(dir2.path(), &[DB_VECTOR_CF]).unwrap();
        let storage2 =
            open_simple_vector_storage(db, DB_VECTOR_CF, 4, Distance::Dot, &AtomicBool::new(false))
                .unwrap();
        {
            let mut borrowed_storage2 = storage2.borrow_mut();
            points.iter().enumerate().for_each(|(i, vec)| {
                borrowed_storage2
                    .insert_vector(i as PointOffsetType, vec.as_slice().into())
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

    let vector = vec![0.0, 1.0, 1.1, 1.0];
    let query = vector.as_slice().into();

    let closest = new_raw_scorer(
        query,
        &borrowed_storage,
        borrowed_id_tracker.deleted_point_bitslice(),
    )
    .unwrap()
    .peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 5);
    assert_eq!(closest.len(), 3, "must have 3 vectors, 2 are deleted");
    assert_eq!(closest[0].idx, 0);
    assert_eq!(closest[1].idx, 1);
    assert_eq!(closest[2].idx, 4);

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

fn do_test_score_points(storage: Arc<AtomicRefCell<VectorStorageEnum>>) {
    let points = [
        vec![1.0, 0.0, 1.0, 1.0],
        vec![1.0, 0.0, 1.0, 0.0],
        vec![1.0, 1.0, 1.0, 1.0],
        vec![1.0, 1.0, 0.0, 1.0],
        vec![1.0, 0.0, 0.0, 0.0],
    ];
    let id_tracker: Arc<AtomicRefCell<IdTrackerSS>> =
        Arc::new(AtomicRefCell::new(FixtureIdTracker::new(points.len())));
    let mut borrowed_id_tracker = id_tracker.borrow_mut();
    let mut borrowed_storage = storage.borrow_mut();

    for (i, vec) in points.iter().enumerate() {
        borrowed_storage
            .insert_vector(i as PointOffsetType, vec.as_slice().into())
            .unwrap();
    }

    let query: QueryVector = [0.0, 1.0, 1.1, 1.0].into();

    let closest = new_raw_scorer(
        query.clone(),
        &borrowed_storage,
        borrowed_id_tracker.deleted_point_bitslice(),
    )
    .unwrap()
    .peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 2);

    let top_idx = match closest.first() {
        Some(scored_point) => {
            assert_eq!(scored_point.idx, 2);
            scored_point.idx
        }
        None => panic!("No close vector found!"),
    };

    borrowed_id_tracker
        .drop(PointIdType::NumId(top_idx as u64))
        .unwrap();

    let raw_scorer = new_raw_scorer(
        query,
        &borrowed_storage,
        borrowed_id_tracker.deleted_point_bitslice(),
    )
    .unwrap();
    let closest = raw_scorer.peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 2);

    let query_points = vec![0, 1, 2, 3, 4];

    let mut raw_res1 = vec![ScoredPointOffset { idx: 0, score: 0. }; query_points.len()];
    let raw_res1_count = raw_scorer.score_points(&query_points, &mut raw_res1);
    raw_res1.resize(raw_res1_count, ScoredPointOffset { idx: 0, score: 0. });

    let mut raw_res2 = vec![ScoredPointOffset { idx: 0, score: 0. }; query_points.len()];
    let raw_res2_count = raw_scorer.score_points(&query_points, &mut raw_res2);
    raw_res2.resize(raw_res2_count, ScoredPointOffset { idx: 0, score: 0. });

    assert_eq!(raw_res1, raw_res2);

    match closest.first() {
        Some(scored_point) => {
            assert_ne!(scored_point.idx, 2);
            assert_eq!(&raw_res1[scored_point.idx as usize], scored_point);
        }
        None => panic!("No close vector found!"),
    };

    let all_ids1: Vec<_> = borrowed_id_tracker.iter_ids().collect();
    let all_ids2: Vec<_> = borrowed_id_tracker.iter_ids().collect();

    assert_eq!(all_ids1, all_ids2);

    assert!(!all_ids1.contains(&top_idx))
}

fn test_score_quantized_points(storage: Arc<AtomicRefCell<VectorStorageEnum>>) {
    let points = [
        vec![1.0, 0.0, 1.0, 1.0],
        vec![1.0, 0.0, 1.0, 0.0],
        vec![1.0, 1.0, 1.0, 1.0],
        vec![1.0, 1.0, 0.0, 1.0],
        vec![1.0, 0.0, 0.0, 0.0],
    ];
    let id_tracker = Arc::new(AtomicRefCell::new(FixtureIdTracker::new(points.len())));
    let mut borrowed_storage = storage.borrow_mut();
    let borrowed_id_tracker = id_tracker.borrow_mut();

    for (i, vec) in points.iter().enumerate() {
        borrowed_storage
            .insert_vector(i as PointOffsetType, vec.as_slice().into())
            .unwrap();
    }

    let config: QuantizationConfig = ScalarQuantizationConfig {
        r#type: Default::default(),
        quantile: None,
        always_ram: None,
    }
    .into();

    let dir = Builder::new()
        .prefix("quantization_path")
        .tempdir()
        .unwrap();

    let stopped = AtomicBool::new(false);
    let quantized_vectors =
        QuantizedVectors::create(&borrowed_storage, &config, dir.path(), 1, &stopped).unwrap();

    let query: QueryVector = vec![0.5, 0.5, 0.5, 0.5].into();

    let scorer_quant = quantized_vectors
        .raw_scorer(
            query.clone(),
            borrowed_id_tracker.deleted_point_bitslice(),
            borrowed_storage.deleted_vector_bitslice(),
            &stopped,
        )
        .unwrap();
    let scorer_orig = new_raw_scorer(
        query.clone(),
        &borrowed_storage,
        borrowed_id_tracker.deleted_point_bitslice(),
    )
    .unwrap();
    for i in 0..5 {
        let quant = scorer_quant.score_point(i);
        let orig = scorer_orig.score_point(i);
        assert!((orig - quant).abs() < 0.15);

        let quant = scorer_quant.score_internal(0, i);
        let orig = scorer_orig.score_internal(0, i);
        assert!((orig - quant).abs() < 0.15);
    }

    let files = borrowed_storage.files();
    let quantization_files = quantized_vectors.files();

    // test save-load
    let quantized_vectors = QuantizedVectors::load(&borrowed_storage, dir.path()).unwrap();
    assert_eq!(files, borrowed_storage.files());
    assert_eq!(quantization_files, quantized_vectors.files());

    let scorer_quant = quantized_vectors
        .raw_scorer(
            query.clone(),
            borrowed_id_tracker.deleted_point_bitslice(),
            borrowed_storage.deleted_vector_bitslice(),
            &stopped,
        )
        .unwrap();
    let scorer_orig = new_raw_scorer(
        query,
        &borrowed_storage,
        borrowed_id_tracker.deleted_point_bitslice(),
    )
    .unwrap();
    for i in 0..5 {
        let quant = scorer_quant.score_point(i);
        let orig = scorer_orig.score_point(i);
        assert!((orig - quant).abs() < 0.15);

        let quant = scorer_quant.score_internal(0, i);
        let orig = scorer_orig.score_internal(0, i);
        assert!((orig - quant).abs() < 0.15);
    }
}

#[test]
fn test_delete_points_in_simple_vector_storages() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

    {
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let storage =
            open_simple_vector_storage(db, DB_VECTOR_CF, 4, Distance::Dot, &AtomicBool::new(false))
                .unwrap();
        do_test_delete_points(storage.clone());
        storage.borrow().flusher()().unwrap();
    }
    let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
    let _storage =
        open_simple_vector_storage(db, DB_VECTOR_CF, 4, Distance::Dot, &AtomicBool::new(false))
            .unwrap();
}

#[test]
fn test_update_from_delete_points_simple_vector_storages() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    {
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let storage =
            open_simple_vector_storage(db, DB_VECTOR_CF, 4, Distance::Dot, &AtomicBool::new(false))
                .unwrap();
        do_test_update_from_delete_points(storage.clone());
        storage.borrow().flusher()().unwrap();
    }

    let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
    let _storage =
        open_simple_vector_storage(db, DB_VECTOR_CF, 4, Distance::Dot, &AtomicBool::new(false))
            .unwrap();
}

#[test]
fn test_score_points_in_simple_vector_storages() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    {
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let storage =
            open_simple_vector_storage(db, DB_VECTOR_CF, 4, Distance::Dot, &AtomicBool::new(false))
                .unwrap();
        do_test_score_points(storage.clone());
        storage.borrow().flusher()().unwrap();
    }

    let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
    let _storage =
        open_simple_vector_storage(db, DB_VECTOR_CF, 4, Distance::Dot, &AtomicBool::new(false))
            .unwrap();
}

#[test]
fn test_score_quantized_points_simple_vector_storages() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    {
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let storage =
            open_simple_vector_storage(db, DB_VECTOR_CF, 4, Distance::Dot, &AtomicBool::new(false))
                .unwrap();
        test_score_quantized_points(storage.clone());
        storage.borrow().flusher()().unwrap();
    }

    let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
    let _storage =
        open_simple_vector_storage(db, DB_VECTOR_CF, 4, Distance::Dot, &AtomicBool::new(false))
            .unwrap();
}

// ----------------------------------------------

#[test]
fn test_delete_points_in_appendable_memmap_vector_storages() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    {
        let storage = open_appendable_memmap_vector_storage(
            dir.path(),
            4,
            Distance::Dot,
            &AtomicBool::new(false),
        )
        .unwrap();
        do_test_delete_points(storage.clone());
        storage.borrow().flusher()().unwrap();
    }

    let _storage = open_appendable_memmap_vector_storage(
        dir.path(),
        4,
        Distance::Dot,
        &AtomicBool::new(false),
    )
    .unwrap();
}

#[test]
fn test_update_from_delete_points_appendable_memmap_vector_storages() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    {
        let storage = open_appendable_memmap_vector_storage(
            dir.path(),
            4,
            Distance::Dot,
            &AtomicBool::new(false),
        )
        .unwrap();

        do_test_update_from_delete_points(storage.clone());
        storage.borrow().flusher()().unwrap();
    }

    let _storage = open_appendable_memmap_vector_storage(
        dir.path(),
        4,
        Distance::Dot,
        &AtomicBool::new(false),
    )
    .unwrap();
}

#[test]
fn test_score_points_in_appendable_memmap_vector_storages() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    {
        let storage = open_appendable_memmap_vector_storage(
            dir.path(),
            4,
            Distance::Dot,
            &AtomicBool::new(false),
        )
        .unwrap();
        do_test_score_points(storage.clone());
        storage.borrow().flusher()().unwrap();
    }

    let _storage = open_appendable_memmap_vector_storage(
        dir.path(),
        4,
        Distance::Dot,
        &AtomicBool::new(false),
    )
    .unwrap();
}

#[test]
fn test_score_quantized_points_appendable_memmap_vector_storages() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    {
        let storage = open_appendable_memmap_vector_storage(
            dir.path(),
            4,
            Distance::Dot,
            &AtomicBool::new(false),
        )
        .unwrap();
        test_score_quantized_points(storage.clone());
        storage.borrow().flusher()().unwrap();
    }

    let _storage = open_appendable_memmap_vector_storage(
        dir.path(),
        4,
        Distance::Dot,
        &AtomicBool::new(false),
    )
    .unwrap();
}
