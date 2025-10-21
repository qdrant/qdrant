use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic_refcell::AtomicRefCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use tempfile::Builder;

#[cfg(feature = "rocksdb")]
use crate::common::rocksdb_wrapper::{DB_VECTOR_CF, open_db};
use crate::data_types::vectors::QueryVector;
use crate::fixtures::payload_context_fixture::FixtureIdTracker;
use crate::id_tracker::IdTrackerSS;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::types::{Distance, PointIdType, QuantizationConfig, ScalarQuantizationConfig};
use crate::vector_storage::dense::appendable_dense_vector_storage::open_appendable_memmap_vector_storage;
#[cfg(feature = "rocksdb")]
use crate::vector_storage::dense::simple_dense_vector_storage::open_simple_dense_full_vector_storage;
use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedVectors, QuantizedVectorsStorageType,
};
use crate::vector_storage::{
    DEFAULT_STOPPED, Random, VectorStorage, VectorStorageEnum, new_raw_scorer,
};

fn do_test_delete_points(storage: &mut VectorStorageEnum) {
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

    let hw_counter = HardwareCounterCell::new();

    for (i, vec) in points.iter().enumerate() {
        storage
            .insert_vector(i as PointOffsetType, vec.as_slice().into(), &hw_counter)
            .unwrap();
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
        "2 vectors must be deleted",
    );

    let vector = vec![0.0, 1.0, 1.1, 1.0];
    let query = vector.as_slice().into();
    let scorer =
        FilteredScorer::new_for_test(query, storage, borrowed_id_tracker.deleted_point_bitslice());
    let closest = scorer
        .peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 5, &DEFAULT_STOPPED)
        .unwrap();
    assert_eq!(closest.len(), 3, "must have 3 vectors, 2 are deleted");
    assert_eq!(closest[0].idx, 0);
    assert_eq!(closest[1].idx, 1);
    assert_eq!(closest[2].idx, 4);
    drop(scorer);

    // Delete 1, redelete 2
    storage.delete_vector(1 as PointOffsetType).unwrap();
    storage.delete_vector(2 as PointOffsetType).unwrap();
    assert_eq!(
        storage.deleted_vector_count(),
        3,
        "3 vectors must be deleted"
    );

    let vector = vec![1.0, 0.0, 0.0, 0.0];
    let query = vector.as_slice().into();
    let scorer =
        FilteredScorer::new_for_test(query, storage, borrowed_id_tracker.deleted_point_bitslice());
    let closest = scorer
        .peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 5, &DEFAULT_STOPPED)
        .unwrap();
    assert_eq!(closest.len(), 2, "must have 2 vectors, 3 are deleted");
    assert_eq!(closest[0].idx, 4);
    assert_eq!(closest[1].idx, 0);
    drop(scorer);

    // Delete all
    storage.delete_vector(0 as PointOffsetType).unwrap();
    storage.delete_vector(4 as PointOffsetType).unwrap();
    assert_eq!(
        storage.deleted_vector_count(),
        5,
        "all vectors must be deleted",
    );

    let vector = vec![1.0, 0.0, 0.0, 0.0];
    let query = vector.as_slice().into();
    let scorer =
        FilteredScorer::new_for_test(query, storage, borrowed_id_tracker.deleted_point_bitslice());
    let closest = scorer.peek_top_all(5, &DEFAULT_STOPPED).unwrap();
    assert!(closest.is_empty(), "must have no results, all deleted");
}

fn do_test_update_from_delete_points(storage: &mut VectorStorageEnum) {
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

    let hw_counter = HardwareCounterCell::new();
    {
        let mut storage2 = new_volatile_dense_vector_storage(4, Distance::Dot);
        {
            points.iter().enumerate().for_each(|(i, vec)| {
                storage2
                    .insert_vector(i as PointOffsetType, vec.as_slice().into(), &hw_counter)
                    .unwrap();
                if delete_mask[i] {
                    storage2.delete_vector(i as PointOffsetType).unwrap();
                }
            });
        }
        let mut iter = (0..points.len()).map(|i| {
            let i = i as PointOffsetType;
            let vec = storage2.get_vector::<Random>(i);
            let deleted = storage2.is_deleted_vector(i);
            (vec, deleted)
        });
        storage.update_from(&mut iter, &Default::default()).unwrap();
    }

    assert_eq!(
        storage.deleted_vector_count(),
        2,
        "2 vectors must be deleted from other storage",
    );

    let vector = vec![0.0, 1.0, 1.1, 1.0];
    let query = vector.as_slice().into();

    let scorer =
        FilteredScorer::new_for_test(query, storage, borrowed_id_tracker.deleted_point_bitslice());
    let closest = scorer
        .peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 5, &DEFAULT_STOPPED)
        .unwrap();
    drop(scorer);
    assert_eq!(closest.len(), 3, "must have 3 vectors, 2 are deleted");
    assert_eq!(closest[0].idx, 0);
    assert_eq!(closest[1].idx, 1);
    assert_eq!(closest[2].idx, 4);

    // Delete all
    storage.delete_vector(0 as PointOffsetType).unwrap();
    storage.delete_vector(1 as PointOffsetType).unwrap();
    storage.delete_vector(4 as PointOffsetType).unwrap();
    assert_eq!(
        storage.deleted_vector_count(),
        5,
        "all vectors must be deleted",
    );
}

fn do_test_score_points(storage: &mut VectorStorageEnum) {
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

    let hw_counter = HardwareCounterCell::new();

    for (i, vec) in points.iter().enumerate() {
        storage
            .insert_vector(i as PointOffsetType, vec.as_slice().into(), &hw_counter)
            .unwrap();
    }

    let query: QueryVector = [0.0, 1.0, 1.1, 1.0].into();

    let scorer = FilteredScorer::new_for_test(
        query.clone(),
        storage,
        borrowed_id_tracker.deleted_point_bitslice(),
    );
    let closest = scorer
        .peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 2, &DEFAULT_STOPPED)
        .unwrap();
    drop(scorer);

    let top_idx = match closest.first() {
        Some(scored_point) => {
            assert_eq!(scored_point.idx, 2);
            scored_point.idx
        }
        None => panic!("No close vector found!"),
    };

    borrowed_id_tracker
        .drop(PointIdType::NumId(u64::from(top_idx)))
        .unwrap();

    let mut raw_scorer = FilteredScorer::new(
        query,
        storage,
        None,
        None,
        borrowed_id_tracker.deleted_point_bitslice(),
        HardwareCounterCell::new(),
    )
    .unwrap();
    let closest = raw_scorer
        .peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 2, &DEFAULT_STOPPED)
        .unwrap();

    let query_points = vec![0, 1, 2, 3, 4];

    let raw_res1 = raw_scorer
        .score_points(&mut query_points.clone(), 0)
        .collect::<Vec<_>>();

    let raw_res2 = raw_scorer
        .score_points(&mut query_points.clone(), 0)
        .collect::<Vec<_>>();

    assert_eq!(raw_res1, raw_res2);

    match closest.first() {
        Some(scored_point) => {
            assert_ne!(scored_point.idx, 2);
            assert_eq!(&raw_res1[scored_point.idx as usize], scored_point);
        }
        None => panic!("No close vector found!"),
    };

    let all_ids1: Vec<_> = borrowed_id_tracker.iter_internal().collect();
    let all_ids2: Vec<_> = borrowed_id_tracker.iter_internal().collect();

    assert_eq!(all_ids1, all_ids2);

    assert!(!all_ids1.contains(&top_idx))
}

fn test_score_quantized_points(storage: &mut VectorStorageEnum) {
    let points = [
        vec![1.0, 0.0, 1.0, 1.0],
        vec![1.0, 0.0, 1.0, 0.0],
        vec![1.0, 1.0, 1.0, 1.0],
        vec![1.0, 1.0, 0.0, 1.0],
        vec![1.0, 0.0, 0.0, 0.0],
    ];

    let hw_counter = HardwareCounterCell::new();
    for (i, vec) in points.iter().enumerate() {
        storage
            .insert_vector(i as PointOffsetType, vec.as_slice().into(), &hw_counter)
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

    let quantized_vectors = QuantizedVectors::create(
        storage,
        &config,
        QuantizedVectorsStorageType::Immutable,
        dir.path(),
        1,
        &stopped,
    )
    .unwrap();

    let query: QueryVector = vec![0.5, 0.5, 0.5, 0.5].into();
    let scorer_quant = quantized_vectors
        .raw_scorer(query.clone(), HardwareCounterCell::new())
        .unwrap();
    let scorer_orig = new_raw_scorer(query.clone(), storage, HardwareCounterCell::new()).unwrap();
    for i in 0..5 {
        let quant = scorer_quant.score_point(i);
        let orig = scorer_orig.score_point(i);
        assert!((orig - quant).abs() < 0.15);

        let quant = scorer_quant.score_internal(0, i);
        let orig = scorer_orig.score_internal(0, i);
        assert!((orig - quant).abs() < 0.15);
    }

    let files = storage.files();
    let quantization_files = quantized_vectors.files();

    // test save-load
    let quantized_vectors = QuantizedVectors::load(&config, storage, dir.path(), &stopped)
        .unwrap()
        .unwrap();
    assert_eq!(files, storage.files());
    assert_eq!(quantization_files, quantized_vectors.files());

    let scorer_quant = quantized_vectors
        .raw_scorer(query.clone(), HardwareCounterCell::new())
        .unwrap();
    let scorer_orig = new_raw_scorer(query, storage, HardwareCounterCell::new()).unwrap();
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
#[cfg(feature = "rocksdb")]
fn test_delete_points_in_simple_vector_storages() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

    {
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let mut storage = open_simple_dense_full_vector_storage(
            db,
            DB_VECTOR_CF,
            4,
            Distance::Dot,
            &AtomicBool::new(false),
        )
        .unwrap();
        do_test_delete_points(&mut storage);
        storage.flusher()().unwrap();
    }

    let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
    let _storage = open_simple_dense_full_vector_storage(
        db,
        DB_VECTOR_CF,
        4,
        Distance::Dot,
        &AtomicBool::new(false),
    )
    .unwrap();
}

#[test]
#[cfg(feature = "rocksdb")]
fn test_update_from_delete_points_simple_vector_storages() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    {
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let mut storage = open_simple_dense_full_vector_storage(
            db,
            DB_VECTOR_CF,
            4,
            Distance::Dot,
            &AtomicBool::new(false),
        )
        .unwrap();
        do_test_update_from_delete_points(&mut storage);
        storage.flusher()().unwrap();
    }

    let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
    let _storage = open_simple_dense_full_vector_storage(
        db,
        DB_VECTOR_CF,
        4,
        Distance::Dot,
        &AtomicBool::new(false),
    )
    .unwrap();
}

#[test]
#[cfg(feature = "rocksdb")]
fn test_score_points_in_simple_vector_storages() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    {
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let mut storage = open_simple_dense_full_vector_storage(
            db,
            DB_VECTOR_CF,
            4,
            Distance::Dot,
            &AtomicBool::new(false),
        )
        .unwrap();
        do_test_score_points(&mut storage);
        storage.flusher()().unwrap();
    }

    let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
    let _storage = open_simple_dense_full_vector_storage(
        db,
        DB_VECTOR_CF,
        4,
        Distance::Dot,
        &AtomicBool::new(false),
    )
    .unwrap();
}

#[test]
#[cfg(feature = "rocksdb")]
fn test_score_quantized_points_simple_vector_storages() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    {
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let mut storage = open_simple_dense_full_vector_storage(
            db,
            DB_VECTOR_CF,
            4,
            Distance::Dot,
            &AtomicBool::new(false),
        )
        .unwrap();
        test_score_quantized_points(&mut storage);
        storage.flusher()().unwrap();
    }

    let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
    let _storage = open_simple_dense_full_vector_storage(
        db,
        DB_VECTOR_CF,
        4,
        Distance::Dot,
        &AtomicBool::new(false),
    )
    .unwrap();
}

// ----------------------------------------------

#[test]
fn test_delete_points_in_volatile_vector_storages() {
    let mut storage = new_volatile_dense_vector_storage(4, Distance::Dot);
    do_test_delete_points(&mut storage);
}

#[test]
fn test_update_from_delete_points_volatile_vector_storages() {
    let mut storage = new_volatile_dense_vector_storage(4, Distance::Dot);
    do_test_update_from_delete_points(&mut storage);
}

#[test]
fn test_score_points_in_volatile_vector_storages() {
    let mut storage = new_volatile_dense_vector_storage(4, Distance::Dot);
    do_test_score_points(&mut storage);
}

#[test]
fn test_score_quantized_points_volatile_vector_storages() {
    let mut storage = new_volatile_dense_vector_storage(4, Distance::Dot);
    test_score_quantized_points(&mut storage);
}

// ----------------------------------------------

#[test]
fn test_delete_points_in_appendable_memmap_vector_storages() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    {
        let mut storage =
            open_appendable_memmap_vector_storage(dir.path(), 4, Distance::Dot).unwrap();
        do_test_delete_points(&mut storage);
        storage.flusher()().unwrap();
    }

    let _storage = open_appendable_memmap_vector_storage(dir.path(), 4, Distance::Dot).unwrap();
}

#[test]
fn test_update_from_delete_points_appendable_memmap_vector_storages() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    {
        let mut storage =
            open_appendable_memmap_vector_storage(dir.path(), 4, Distance::Dot).unwrap();

        do_test_update_from_delete_points(&mut storage);
        storage.flusher()().unwrap();
    }

    let _storage = open_appendable_memmap_vector_storage(dir.path(), 4, Distance::Dot).unwrap();
}

#[test]
fn test_score_points_in_appendable_memmap_vector_storages() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    {
        let mut storage =
            open_appendable_memmap_vector_storage(dir.path(), 4, Distance::Dot).unwrap();
        do_test_score_points(&mut storage);
        storage.flusher()().unwrap();
    }

    let _storage = open_appendable_memmap_vector_storage(dir.path(), 4, Distance::Dot).unwrap();
}

#[test]
fn test_score_quantized_points_appendable_memmap_vector_storages() {
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    {
        let mut storage =
            open_appendable_memmap_vector_storage(dir.path(), 4, Distance::Dot).unwrap();
        test_score_quantized_points(&mut storage);
        storage.flusher()().unwrap();
    }

    let _storage = open_appendable_memmap_vector_storage(dir.path(), 4, Distance::Dot).unwrap();
}
