use std::mem::size_of;
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use bitvec::slice::BitSlice;
use bitvec::vec::BitVec;
use log::debug;
use parking_lot::RwLock;
use rocksdb::DB;
use serde::{Deserialize, Serialize};

use super::chunked_vectors::ChunkedVectors;
use super::vector_storage_base::VectorStorage;
use super::VectorStorageEnum;
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::common::Flusher;
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::{check_process_stopped, OperationError, OperationResult};
use crate::types::{Distance, PointOffsetType, QuantizationConfig};
use crate::vector_storage::quantized::quantized_vectors_base::{
    QuantizedVectors, QuantizedVectorsStorage,
};

/// In-memory vector storage with on-update persistence using `store`
pub struct SimpleVectorStorage {
    dim: usize,
    distance: Distance,
    vectors: ChunkedVectors<VectorElementType>,
    quantized_vectors: Option<QuantizedVectorsStorage>,
    db_wrapper: DatabaseColumnWrapper,
    update_buffer: StoredRecord,
    /// BitVec for deleted flags. Grows dynamically upto last set flag.
    deleted: BitVec,
    deleted_count: usize,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct StoredRecord {
    pub deleted: bool,
    pub vector: Vec<VectorElementType>,
}

pub fn open_simple_vector_storage(
    database: Arc<RwLock<DB>>,
    database_column_name: &str,
    dim: usize,
    distance: Distance,
) -> OperationResult<Arc<AtomicRefCell<VectorStorageEnum>>> {
    let mut vectors = ChunkedVectors::new(dim);
    let mut deleted = BitVec::new();
    let mut deleted_count = 0;

    let db_wrapper = DatabaseColumnWrapper::new(database, database_column_name);
    for (key, value) in db_wrapper.lock_db().iter()? {
        let point_id: PointOffsetType = bincode::deserialize(&key)
            .map_err(|_| OperationError::service_error("cannot deserialize point id from db"))?;
        let stored_record: StoredRecord = bincode::deserialize(&value)
            .map_err(|_| OperationError::service_error("cannot deserialize record from db"))?;

        // Set deleted flag
        if stored_record.deleted {
            deleted_count += 1;
            bitvec_set_deleted(&mut deleted, point_id, true);
        }

        vectors.insert(point_id, &stored_record.vector);
    }

    debug!("Segment vectors: {}", vectors.len());
    debug!(
        "Estimated segment size {} MB",
        vectors.len() * dim * size_of::<VectorElementType>() / 1024 / 1024
    );

    Ok(Arc::new(AtomicRefCell::new(VectorStorageEnum::Simple(
        SimpleVectorStorage {
            dim,
            distance,
            vectors,
            quantized_vectors: None,
            db_wrapper,
            update_buffer: StoredRecord {
                deleted: false,
                vector: vec![0.; dim],
            },
            deleted,
            deleted_count,
        },
    ))))
}

impl SimpleVectorStorage {
    /// Set deleted flag for given key. Returns previous deleted state.
    #[inline]
    fn set_deleted(&mut self, key: PointOffsetType, deleted: bool) {
        if self.vectors.len() <= key as usize {
            return;
        }

        let previous = bitvec_set_deleted(&mut self.deleted, key, deleted);
        if !previous && deleted {
            self.deleted_count += 1;
        } else if previous && !deleted {
            self.deleted_count -= 1;
        }
    }

    fn update_stored(
        &mut self,
        key: PointOffsetType,
        deleted: bool,
        vector: Option<&[VectorElementType]>,
    ) -> OperationResult<()> {
        // Write vector state to buffer record
        let record = &mut self.update_buffer;
        record.deleted = deleted;
        if let Some(vector) = vector {
            record.vector.copy_from_slice(vector);
        }

        // Store updated record
        self.db_wrapper.put(
            bincode::serialize(&key).unwrap(),
            bincode::serialize(&record).unwrap(),
        )?;

        Ok(())
    }
}

impl VectorStorage for SimpleVectorStorage {
    fn vector_dim(&self) -> usize {
        self.dim
    }

    fn distance(&self) -> Distance {
        self.distance
    }

    fn total_vector_count(&self) -> usize {
        self.vectors.len()
    }

    fn get_vector(&self, key: PointOffsetType) -> &[VectorElementType] {
        self.vectors.get(key)
    }

    fn insert_vector(
        &mut self,
        key: PointOffsetType,
        vector: &[VectorElementType],
    ) -> OperationResult<()> {
        self.vectors.insert(key, vector);
        self.set_deleted(key, false);
        self.update_stored(key, false, Some(vector))?;
        Ok(())
    }

    fn update_from(
        &mut self,
        other: &VectorStorageEnum,
        other_ids: &mut dyn Iterator<Item = PointOffsetType>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let start_index = self.vectors.len() as PointOffsetType;
        for point_id in other_ids {
            check_process_stopped(stopped)?;
            // Do not perform preprocessing - vectors should be already processed
            let other_deleted = other.is_deleted(point_id);
            let other_vector = other.get_vector(point_id);
            let new_id = self.vectors.push(other_vector);
            self.set_deleted(new_id, other_deleted);
            self.update_stored(new_id, false, Some(other_vector))?;
        }
        let end_index = self.vectors.len() as PointOffsetType;
        Ok(start_index..end_index)
    }

    fn flusher(&self) -> Flusher {
        self.db_wrapper.flusher()
    }

    fn quantize(
        &mut self,
        path: &Path,
        quantization_config: &QuantizationConfig,
    ) -> OperationResult<()> {
        let vector_data_iterator = (0..self.vectors.len() as u32).map(|i| self.vectors.get(i));
        self.quantized_vectors = Some(QuantizedVectorsStorage::create(
            vector_data_iterator,
            quantization_config,
            self.distance,
            self.dim,
            self.vectors.len(),
            path,
            false,
        )?);
        Ok(())
    }

    fn load_quantization(&mut self, path: &Path) -> OperationResult<()> {
        if QuantizedVectorsStorage::check_exists(path) {
            self.quantized_vectors =
                Some(QuantizedVectorsStorage::load(path, false, self.distance)?);
        }
        Ok(())
    }

    fn quantized_storage(&self) -> Option<&QuantizedVectorsStorage> {
        self.quantized_vectors.as_ref()
    }

    fn files(&self) -> Vec<std::path::PathBuf> {
        if let Some(quantized_vectors) = &self.quantized_vectors {
            quantized_vectors.files()
        } else {
            vec![]
        }
    }

    fn delete(&mut self, key: PointOffsetType) -> OperationResult<()> {
        self.set_deleted(key, true);
        self.update_stored(key, true, None)?;
        Ok(())
    }

    fn is_deleted(&self, key: PointOffsetType) -> bool {
        self.deleted.get(key as usize).map(|b| *b).unwrap_or(false)
    }

    fn deleted_count(&self) -> usize {
        self.deleted_count
    }

    fn deleted_bitslice(&self) -> &BitSlice {
        self.deleted.as_bitslice()
    }
}

/// Set deleted state in given bitvec.
///
/// Grows bitvec if it is not big enough.
///
/// Returns the previous state.
#[inline]
fn bitvec_set_deleted(bitvec: &mut BitVec, point_id: PointOffsetType, deleted: bool) -> bool {
    if bitvec.len() > point_id as usize {
        // Set deleted flag if bitvec is large enough
        unsafe { bitvec.replace_unchecked(point_id as usize, deleted) }
    } else if deleted {
        // Bitvec is too small; grow and set the deletion flag
        bitvec.resize(point_id as usize + 1, false);
        unsafe { bitvec.set_unchecked(point_id as usize, true) };
        false
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;
    use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
    use crate::fixtures::payload_context_fixture::FixtureIdTracker;
    use crate::id_tracker::{IdTracker, IdTrackerSS};
    use crate::types::{PointIdType, ScalarQuantizationConfig};
    use crate::vector_storage::{new_raw_scorer, ScoredPointOffset};

    #[test]
    fn test_delete_points() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let points = vec![
            vec![1.0, 0.0, 1.0, 1.0],
            vec![1.0, 0.0, 1.0, 0.0],
            vec![1.0, 1.0, 1.0, 1.0],
            vec![1.0, 1.0, 0.0, 1.0],
            vec![1.0, 0.0, 0.0, 0.0],
        ];
        let delete_mask = [false, false, true, true, false];
        let id_tracker: Arc<AtomicRefCell<IdTrackerSS>> =
            Arc::new(AtomicRefCell::new(FixtureIdTracker::new(points.len())));
        let storage = open_simple_vector_storage(db, DB_VECTOR_CF, 4, Distance::Dot).unwrap();
        let borrowed_id_tracker = id_tracker.borrow_mut();
        let mut borrowed_storage = storage.borrow_mut();

        for (i, vec) in points.iter().enumerate() {
            borrowed_storage
                .insert_vector(i as PointOffsetType, vec)
                .unwrap();
        }

        // Delete select number of points
        delete_mask
            .into_iter()
            .enumerate()
            .filter(|(_, d)| *d)
            .for_each(|(i, _)| {
                borrowed_storage.delete(i as PointOffsetType).unwrap();
            });
        assert_eq!(
            borrowed_storage.deleted_count(),
            2,
            "2 vectors must be deleted"
        );

        let query: Vec<VectorElementType> = vec![0.0, 1.0, 1.1, 1.0];
        let closest = new_raw_scorer(
            query,
            &borrowed_storage,
            borrowed_id_tracker.deleted_bitvec(),
        )
        .peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 5);
        assert_eq!(closest.len(), 3, "must have 3 vectors, 2 are deleted");
        assert_eq!(closest[0].idx, 0);
        assert_eq!(closest[1].idx, 1);
        assert_eq!(closest[2].idx, 4);

        // Delete 1, redelete 2
        borrowed_storage.delete(1 as PointOffsetType).unwrap();
        borrowed_storage.delete(2 as PointOffsetType).unwrap();
        assert_eq!(
            borrowed_storage.deleted_count(),
            3,
            "3 vectors must be deleted"
        );

        let query: Vec<VectorElementType> = vec![1.0, 0.0, 0.0, 0.0];
        let closest = new_raw_scorer(
            query,
            &borrowed_storage,
            borrowed_id_tracker.deleted_bitvec(),
        )
        .peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 5);
        assert_eq!(closest.len(), 2, "must have 2 vectors, 3 are deleted");
        assert_eq!(closest[0].idx, 4);
        assert_eq!(closest[1].idx, 0);

        // Delete all
        borrowed_storage.delete(0 as PointOffsetType).unwrap();
        borrowed_storage.delete(4 as PointOffsetType).unwrap();
        assert_eq!(
            borrowed_storage.deleted_count(),
            5,
            "all vectors must be deleted"
        );

        let query: Vec<VectorElementType> = vec![1.0, 0.0, 0.0, 0.0];
        let closest = new_raw_scorer(
            query,
            &borrowed_storage,
            borrowed_id_tracker.deleted_bitvec(),
        )
        .peek_top_all(5);
        assert!(closest.is_empty(), "must have no results, all deleted");
    }

    /// Test that deleted points are properly transferred when updating from other storage.
    #[test]
    fn test_update_from_delete_points() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let points = vec![
            vec![1.0, 0.0, 1.0, 1.0],
            vec![1.0, 0.0, 1.0, 0.0],
            vec![1.0, 1.0, 1.0, 1.0],
            vec![1.0, 1.0, 0.0, 1.0],
            vec![1.0, 0.0, 0.0, 0.0],
        ];
        let delete_mask = [false, false, true, true, false];
        let id_tracker: Arc<AtomicRefCell<IdTrackerSS>> =
            Arc::new(AtomicRefCell::new(FixtureIdTracker::new(points.len())));
        let storage = open_simple_vector_storage(db, DB_VECTOR_CF, 4, Distance::Dot).unwrap();
        let borrowed_id_tracker = id_tracker.borrow_mut();
        let mut borrowed_storage = storage.borrow_mut();

        {
            let dir2 = Builder::new().prefix("db_dir").tempdir().unwrap();
            let db = open_db(dir2.path(), &[DB_VECTOR_CF]).unwrap();
            let storage2 = open_simple_vector_storage(db, DB_VECTOR_CF, 4, Distance::Dot).unwrap();
            {
                let mut borrowed_storage2 = storage2.borrow_mut();
                points.iter().enumerate().for_each(|(i, vec)| {
                    borrowed_storage2
                        .insert_vector(i as PointOffsetType, vec)
                        .unwrap();
                    if delete_mask[i] {
                        borrowed_storage2.delete(i as PointOffsetType).unwrap();
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
            borrowed_storage.deleted_count(),
            2,
            "2 vectors must be deleted from other storage"
        );

        let query: Vec<VectorElementType> = vec![0.0, 1.0, 1.1, 1.0];
        let closest = new_raw_scorer(
            query,
            &borrowed_storage,
            borrowed_id_tracker.deleted_bitvec(),
        )
        .peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 5);
        assert_eq!(closest.len(), 3, "must have 3 vectors, 2 are deleted");
        assert_eq!(closest[0].idx, 0);
        assert_eq!(closest[1].idx, 1);
        assert_eq!(closest[2].idx, 4);

        // Delete all
        borrowed_storage.delete(0 as PointOffsetType).unwrap();
        borrowed_storage.delete(1 as PointOffsetType).unwrap();
        borrowed_storage.delete(4 as PointOffsetType).unwrap();
        assert_eq!(
            borrowed_storage.deleted_count(),
            5,
            "all vectors must be deleted"
        );
    }

    #[test]
    fn test_score_points() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let points = vec![
            vec![1.0, 0.0, 1.0, 1.0],
            vec![1.0, 0.0, 1.0, 0.0],
            vec![1.0, 1.0, 1.0, 1.0],
            vec![1.0, 1.0, 0.0, 1.0],
            vec![1.0, 0.0, 0.0, 0.0],
        ];
        let id_tracker: Arc<AtomicRefCell<IdTrackerSS>> =
            Arc::new(AtomicRefCell::new(FixtureIdTracker::new(points.len())));
        let storage = open_simple_vector_storage(db, DB_VECTOR_CF, 4, Distance::Dot).unwrap();
        let mut borrowed_id_tracker = id_tracker.borrow_mut();
        let mut borrowed_storage = storage.borrow_mut();

        for (i, vec) in points.iter().enumerate() {
            borrowed_storage
                .insert_vector(i as PointOffsetType, vec)
                .unwrap();
        }

        let query: Vec<VectorElementType> = vec![0.0, 1.0, 1.1, 1.0];

        let closest = new_raw_scorer(
            query.clone(),
            &borrowed_storage,
            borrowed_id_tracker.deleted_bitvec(),
        )
        .peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 2);

        let top_idx = match closest.get(0) {
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
            borrowed_id_tracker.deleted_bitvec(),
        );
        let closest = raw_scorer.peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 2);

        let query_points = vec![0, 1, 2, 3, 4];

        let mut raw_res1 = vec![ScoredPointOffset { idx: 0, score: 0. }; query_points.len()];
        let raw_res1_count = raw_scorer.score_points(&query_points, &mut raw_res1);
        raw_res1.resize(raw_res1_count, ScoredPointOffset { idx: 0, score: 0. });

        let mut raw_res2 = vec![ScoredPointOffset { idx: 0, score: 0. }; query_points.len()];
        let raw_res2_count = raw_scorer.score_points(&query_points, &mut raw_res2);
        raw_res2.resize(raw_res2_count, ScoredPointOffset { idx: 0, score: 0. });

        assert_eq!(raw_res1, raw_res2);

        match closest.get(0) {
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

    #[test]
    fn test_score_quantized_points() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let points = vec![
            vec![1.0, 0.0, 1.0, 1.0],
            vec![1.0, 0.0, 1.0, 0.0],
            vec![1.0, 1.0, 1.0, 1.0],
            vec![1.0, 1.0, 0.0, 1.0],
            vec![1.0, 0.0, 0.0, 0.0],
        ];
        let id_tracker = Arc::new(AtomicRefCell::new(FixtureIdTracker::new(points.len())));
        let storage = open_simple_vector_storage(db, DB_VECTOR_CF, 4, Distance::Dot).unwrap();
        let mut borrowed_storage = storage.borrow_mut();
        let borrowed_id_tracker = id_tracker.borrow_mut();

        for (i, vec) in points.iter().enumerate() {
            borrowed_storage
                .insert_vector(i as PointOffsetType, vec)
                .unwrap();
        }

        let config: QuantizationConfig = ScalarQuantizationConfig {
            r#type: Default::default(),
            quantile: None,
            always_ram: None,
        }
        .into();

        borrowed_storage.quantize(dir.path(), &config).unwrap();

        let query = vec![0.5, 0.5, 0.5, 0.5];

        {
            let scorer_quant = borrowed_storage
                .quantized_storage()
                .unwrap()
                .raw_scorer(&query, borrowed_id_tracker.deleted_bitvec());
            let scorer_orig = new_raw_scorer(
                query.clone(),
                &borrowed_storage,
                borrowed_id_tracker.deleted_bitvec(),
            );
            for i in 0..5 {
                let quant = scorer_quant.score_point(i);
                let orig = scorer_orig.score_point(i);
                assert!((orig - quant).abs() < 0.15);

                let quant = scorer_quant.score_internal(0, i);
                let orig = scorer_orig.score_internal(0, i);
                assert!((orig - quant).abs() < 0.15);
            }
        }

        // test save-load
        borrowed_storage.load_quantization(dir.path()).unwrap();

        let scorer_quant = borrowed_storage
            .quantized_storage()
            .unwrap()
            .raw_scorer(&query, borrowed_id_tracker.deleted_bitvec());
        let scorer_orig = new_raw_scorer(
            query.clone(),
            &borrowed_storage,
            borrowed_id_tracker.deleted_bitvec(),
        );
        for i in 0..5 {
            let quant = scorer_quant.score_point(i);
            let orig = scorer_orig.score_point(i);
            assert!((orig - quant).abs() < 0.15);

            let quant = scorer_quant.score_internal(0, i);
            let orig = scorer_orig.score_internal(0, i);
            assert!((orig - quant).abs() < 0.15);
        }
    }
}
