use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::types::PointOffsetType;
use common::validation::MAX_MULTIVECTOR_FLATTENED_LEN;
use rstest::rstest;
use tempfile::Builder;

use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
use crate::data_types::vectors::{
    MultiDenseVectorInternal, QueryVector, TypedMultiDenseVectorRef, VectorElementType, VectorRef,
};
use crate::fixtures::payload_context_fixture::FixtureIdTracker;
use crate::id_tracker::IdTrackerSS;
use crate::types::{Distance, MultiVectorConfig};
use crate::vector_storage::common::CHUNK_SIZE;
use crate::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::open_appendable_memmap_multi_vector_storage;
use crate::vector_storage::multi_dense::simple_multi_dense_vector_storage::open_simple_multi_dense_vector_storage;
use crate::vector_storage::{new_raw_scorer, MultiVectorStorage, VectorStorage, VectorStorageEnum};

#[derive(Clone, Copy)]
enum MultiDenseStorageType {
    SimpleRamFloat,
    AppendableMmapFloat,
}

fn multi_points_fixtures(vec_count: usize, vec_dim: usize) -> Vec<MultiDenseVectorInternal> {
    let mut multis: Vec<MultiDenseVectorInternal> = Vec::new();
    for i in 0..vec_count {
        let value = i as f32;
        // hardcoded 5 inner vectors
        let vectors = vec![
            vec![value; vec_dim],
            vec![value; vec_dim],
            vec![value; vec_dim],
            vec![value; vec_dim],
            vec![value; vec_dim],
        ];
        let multi = MultiDenseVectorInternal::try_from(vectors).unwrap();
        multis.push(multi);
    }
    multis
}

fn do_test_delete_points(vector_dim: usize, vec_count: usize, storage: &mut VectorStorageEnum) {
    let points = multi_points_fixtures(vec_count, vector_dim);

    let delete_mask = [false, false, true, true, false];

    let id_tracker: Arc<AtomicRefCell<IdTrackerSS>> =
        Arc::new(AtomicRefCell::new(FixtureIdTracker::new(points.len())));

    let borrowed_id_tracker = id_tracker.borrow_mut();

    // Insert all points
    for (i, vec) in points.iter().enumerate() {
        storage
            .insert_vector(i as PointOffsetType, vec.into())
            .unwrap();
    }
    // Check that all points are inserted
    for (i, vec) in points.iter().enumerate() {
        let stored_vec = storage.get_vector(i as PointOffsetType);
        let multi_dense: TypedMultiDenseVectorRef<_> = stored_vec.as_vec_ref().try_into().unwrap();
        assert_eq!(multi_dense.to_owned(), vec.clone());
    }
    // Check that all points are inserted #2
    {
        let orig_iter = points.iter().flat_map(|multivec| multivec.multi_vectors());
        match storage as &VectorStorageEnum {
            VectorStorageEnum::DenseSimple(_) => unreachable!(),
            VectorStorageEnum::DenseSimpleByte(_) => unreachable!(),
            VectorStorageEnum::DenseSimpleHalf(_) => unreachable!(),
            VectorStorageEnum::DenseMemmap(_) => unreachable!(),
            VectorStorageEnum::DenseMemmapByte(_) => unreachable!(),
            VectorStorageEnum::DenseMemmapHalf(_) => unreachable!(),
            VectorStorageEnum::DenseAppendableMemmap(_) => unreachable!(),
            VectorStorageEnum::DenseAppendableMemmapByte(_) => unreachable!(),
            VectorStorageEnum::DenseAppendableMemmapHalf(_) => unreachable!(),
            VectorStorageEnum::SparseSimple(_) => unreachable!(),
            VectorStorageEnum::SparseMmap(_) => unreachable!(),
            VectorStorageEnum::MultiDenseSimple(v) => {
                for (orig, vec) in orig_iter.zip(v.iterate_inner_vectors()) {
                    assert_eq!(orig, vec);
                }
            }
            VectorStorageEnum::MultiDenseSimpleByte(_) => unreachable!(),
            VectorStorageEnum::MultiDenseSimpleHalf(_) => unreachable!(),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => {
                for (orig, vec) in orig_iter.zip(v.iterate_inner_vectors()) {
                    assert_eq!(orig, vec);
                }
            }
            VectorStorageEnum::MultiDenseAppendableMemmapByte(_) => unreachable!(),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(_) => unreachable!(),
            VectorStorageEnum::DenseAppendableInRam(_) => unreachable!(),
            VectorStorageEnum::DenseAppendableInRamByte(_) => unreachable!(),
            VectorStorageEnum::DenseAppendableInRamHalf(_) => unreachable!(),
            VectorStorageEnum::MultiDenseAppendableInRam(_) => unreachable!(),
            VectorStorageEnum::MultiDenseAppendableInRamByte(_) => unreachable!(),
            VectorStorageEnum::MultiDenseAppendableInRamHalf(_) => unreachable!(),
        };
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
    let vector: Vec<Vec<f32>> = vec![vec![2.0; vector_dim]];
    let query = QueryVector::Nearest(vector.try_into().unwrap());
    let scorer =
        new_raw_scorer(query, storage, borrowed_id_tracker.deleted_point_bitslice()).unwrap();
    let closest = scorer.peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 5);
    scorer.take_hardware_counter().discard_results();
    drop(scorer);
    assert_eq!(closest.len(), 3, "must have 3 vectors, 2 are deleted");
    assert_eq!(closest[0].idx, 4);
    assert_eq!(closest[1].idx, 1);
    assert_eq!(closest[2].idx, 0);

    // Delete 1, redelete 2
    storage.delete_vector(1 as PointOffsetType).unwrap();
    storage.delete_vector(2 as PointOffsetType).unwrap();
    assert_eq!(
        storage.deleted_vector_count(),
        3,
        "3 vectors must be deleted"
    );

    let vector: Vec<Vec<f32>> = vec![vec![1.0; vector_dim]];
    let query = QueryVector::Nearest(vector.try_into().unwrap());
    let scorer =
        new_raw_scorer(query, storage, borrowed_id_tracker.deleted_point_bitslice()).unwrap();
    let closest = scorer.peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 5);
    scorer.take_hardware_counter().discard_results();
    drop(scorer);
    assert_eq!(closest.len(), 2, "must have 2 vectors, 3 are deleted");
    assert_eq!(closest[0].idx, 4);
    assert_eq!(closest[1].idx, 0);

    // Delete all
    storage.delete_vector(0 as PointOffsetType).unwrap();
    storage.delete_vector(4 as PointOffsetType).unwrap();
    assert_eq!(
        storage.deleted_vector_count(),
        5,
        "all vectors must be deleted"
    );

    let vector: Vec<Vec<f32>> = vec![vec![1.0; vector_dim]];
    let query = QueryVector::Nearest(vector.try_into().unwrap());
    let scorer =
        new_raw_scorer(query, storage, borrowed_id_tracker.deleted_point_bitslice()).unwrap();
    let closest = scorer.peek_top_all(5);
    scorer.take_hardware_counter().discard_results();
    assert!(closest.is_empty(), "must have no results, all deleted");
}

fn do_test_update_from_delete_points(
    vector_dim: usize,
    vec_count: usize,
    storage: &mut VectorStorageEnum,
) {
    let points = multi_points_fixtures(vec_count, vector_dim);

    let delete_mask = [false, false, true, true, false];

    let id_tracker: Arc<AtomicRefCell<IdTrackerSS>> =
        Arc::new(AtomicRefCell::new(FixtureIdTracker::new(points.len())));
    let borrowed_id_tracker = id_tracker.borrow_mut();

    {
        let dir2 = Builder::new().prefix("db_dir").tempdir().unwrap();
        let db = open_db(dir2.path(), &[DB_VECTOR_CF]).unwrap();
        let mut storage2 = open_simple_multi_dense_vector_storage(
            db,
            DB_VECTOR_CF,
            vector_dim,
            Distance::Dot,
            MultiVectorConfig::default(),
            &AtomicBool::new(false),
        )
        .unwrap();
        {
            points.iter().enumerate().for_each(|(i, vec)| {
                storage2
                    .insert_vector(i as PointOffsetType, vec.into())
                    .unwrap();
                if delete_mask[i] {
                    storage2.delete_vector(i as PointOffsetType).unwrap();
                }
            });
        }
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
        2,
        "2 vectors must be deleted from other storage"
    );

    let vector: Vec<Vec<f32>> = vec![vec![1.0; vector_dim]];

    let query = QueryVector::Nearest(vector.try_into().unwrap());

    let scorer =
        new_raw_scorer(query, storage, borrowed_id_tracker.deleted_point_bitslice()).unwrap();
    let closest = scorer.peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 5);
    scorer.take_hardware_counter().discard_results();
    drop(scorer);
    assert_eq!(closest.len(), 3, "must have 3 vectors, 2 are deleted");
    assert_eq!(closest[0].idx, 4);
    assert_eq!(closest[1].idx, 1);
    assert_eq!(closest[2].idx, 0);

    // Delete all
    storage.delete_vector(0 as PointOffsetType).unwrap();
    storage.delete_vector(1 as PointOffsetType).unwrap();
    storage.delete_vector(4 as PointOffsetType).unwrap();
    assert_eq!(
        storage.deleted_vector_count(),
        5,
        "all vectors must be deleted"
    );
}

fn create_vector_storage(
    storage_type: MultiDenseStorageType,
    vec_dim: usize,
    path: &Path,
) -> VectorStorageEnum {
    match storage_type {
        MultiDenseStorageType::SimpleRamFloat => {
            let db = open_db(path, &[DB_VECTOR_CF]).unwrap();
            open_simple_multi_dense_vector_storage(
                db,
                DB_VECTOR_CF,
                vec_dim,
                Distance::Dot,
                MultiVectorConfig::default(),
                &AtomicBool::new(false),
            )
            .unwrap()
        }
        MultiDenseStorageType::AppendableMmapFloat => open_appendable_memmap_multi_vector_storage(
            path,
            vec_dim,
            Distance::Dot,
            MultiVectorConfig::default(),
        )
        .unwrap(),
    }
}

#[rstest]
fn test_delete_points_in_multi_dense_vector_storage(
    #[values(
        MultiDenseStorageType::SimpleRamFloat,
        MultiDenseStorageType::AppendableMmapFloat
    )]
    storage_type: MultiDenseStorageType,
) {
    let vec_dim = 1024;
    let vec_count = 5;
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let total_vector_count = {
        let mut storage = create_vector_storage(storage_type, vec_dim, dir.path());
        do_test_delete_points(vec_dim, vec_count, &mut storage);
        let count = storage.total_vector_count();
        storage.flusher()().unwrap();
        count
    };
    let storage = create_vector_storage(storage_type, vec_dim, dir.path());
    assert_eq!(
        storage.total_vector_count(),
        total_vector_count,
        "total vector count must be the same"
    );
    // retrieve all vectors from storage
    for id in 0..total_vector_count {
        assert!(storage.get_vector_opt(id as PointOffsetType).is_some());
    }
}

#[rstest]
fn test_update_from_delete_points_multi_dense_vector_storage(
    #[values(
        MultiDenseStorageType::SimpleRamFloat,
        MultiDenseStorageType::AppendableMmapFloat
    )]
    storage_type: MultiDenseStorageType,
) {
    let vec_dim = 1024;
    let vec_count = 5;
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let total_vector_count = {
        let mut storage = create_vector_storage(storage_type, vec_dim, dir.path());
        do_test_update_from_delete_points(vec_dim, vec_count, &mut storage);
        let count = storage.total_vector_count();
        storage.flusher()().unwrap();
        count
    };
    let storage = create_vector_storage(storage_type, vec_dim, dir.path());
    assert_eq!(
        storage.total_vector_count(),
        total_vector_count,
        "total vector count must be the same"
    );
    // retrieve all vectors from storage
    for id in 0..total_vector_count {
        assert!(storage.get_vector_opt(id as PointOffsetType).is_some());
    }
}

#[rstest]
fn test_large_multi_dense_vector_storage(
    #[values(
        MultiDenseStorageType::SimpleRamFloat,
        MultiDenseStorageType::AppendableMmapFloat
    )]
    storage_type: MultiDenseStorageType,
) {
    assert!(MAX_MULTIVECTOR_FLATTENED_LEN * std::mem::size_of::<VectorElementType>() < CHUNK_SIZE);

    let vec_dim = 100_000;
    let vec_count = 100;
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let mut storage = create_vector_storage(storage_type, vec_dim, dir.path());

    let vectors = vec![vec![0.0; vec_dim]; vec_count];
    let multivec = MultiDenseVectorInternal::try_from(vectors).unwrap();

    let result = storage.insert_vector(0, VectorRef::from(&multivec));
    match result {
        Ok(_) => {
            panic!("Inserting vector should fail");
        }
        Err(e) => {
            assert!(e.to_string().contains("too large"));
        }
    }
}
