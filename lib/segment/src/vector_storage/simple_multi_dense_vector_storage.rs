use std::ops::Range;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use bitvec::prelude::{BitSlice, BitVec};
use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;

use super::MultiVectorStorage;
use crate::common::operation_error::{check_process_stopped, OperationError, OperationResult};
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::common::Flusher;
use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::{DenseVector, MultiDenseVector, VectorRef};
use crate::types::Distance;
use crate::vector_storage::bitvec::bitvec_set_deleted;
use crate::vector_storage::common::StoredRecord;
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

type StoredMultiDenseVector = StoredRecord<MultiDenseVector>;

/// In-memory vector storage with on-update persistence using `store`
pub struct SimpleMultiDenseVectorStorage {
    dim: usize,
    distance: Distance,
    /// Keep vectors in memory
    vectors: Vec<MultiDenseVector>,
    db_wrapper: DatabaseColumnWrapper,
    update_buffer: StoredMultiDenseVector,
    /// BitVec for deleted flags. Grows dynamically upto last set flag.
    deleted: BitVec,
    /// Current number of deleted vectors.
    deleted_count: usize,
}

#[allow(unused)]
pub fn open_simple_multi_dense_vector_storage(
    database: Arc<RwLock<DB>>,
    database_column_name: &str,
    dim: usize,
    distance: Distance,
    stopped: &AtomicBool,
) -> OperationResult<Arc<AtomicRefCell<VectorStorageEnum>>> {
    let mut vectors = vec![];
    let (mut deleted, mut deleted_count) = (BitVec::new(), 0);
    let db_wrapper = DatabaseColumnWrapper::new(database, database_column_name);
    db_wrapper.lock_db().iter()?;
    for (key, value) in db_wrapper.lock_db().iter()? {
        let point_id: PointOffsetType = bincode::deserialize(&key)
            .map_err(|_| OperationError::service_error("cannot deserialize point id from db"))?;
        let stored_record: StoredMultiDenseVector = bincode::deserialize(&value)
            .map_err(|_| OperationError::service_error("cannot deserialize record from db"))?;

        // Propagate deleted flag
        if stored_record.deleted {
            bitvec_set_deleted(&mut deleted, point_id, true);
            deleted_count += 1;
        }
        let point_id_usize = point_id as usize;
        if point_id_usize >= vectors.len() {
            vectors.resize(point_id_usize + 1, vec![]);
        }
        vectors[point_id_usize] = stored_record.vector;

        check_process_stopped(stopped)?;
    }

    Ok(Arc::new(AtomicRefCell::new(
        VectorStorageEnum::MultiDenseSimple(SimpleMultiDenseVectorStorage {
            dim,
            distance,
            vectors,
            db_wrapper,
            update_buffer: StoredMultiDenseVector {
                deleted: false,
                vector: MultiDenseVector::default(),
            },
            deleted,
            deleted_count,
        }),
    )))
}

impl SimpleMultiDenseVectorStorage {
    /// Set deleted flag for given key. Returns previous deleted state.
    #[inline]
    fn set_deleted(&mut self, key: PointOffsetType, deleted: bool) -> bool {
        if key as usize >= self.vectors.len() {
            return false;
        }
        let was_deleted = bitvec_set_deleted(&mut self.deleted, key, deleted);
        if was_deleted != deleted {
            if !was_deleted {
                self.deleted_count += 1;
            } else {
                self.deleted_count = self.deleted_count.saturating_sub(1);
            }
        }
        was_deleted
    }

    fn update_stored(
        &mut self,
        key: PointOffsetType,
        deleted: bool,
        vector: Option<MultiDenseVector>,
    ) -> OperationResult<()> {
        // Write vector state to buffer record
        let record = &mut self.update_buffer;
        record.deleted = deleted;
        if let Some(vector) = vector {
            record.vector = vector;
        }

        // Store updated record
        self.db_wrapper.put(
            bincode::serialize(&key).unwrap(),
            bincode::serialize(&record).unwrap(),
        )?;

        Ok(())
    }
}

impl MultiVectorStorage for SimpleMultiDenseVectorStorage {
    fn get_multi(&self, key: PointOffsetType) -> &MultiDenseVector {
        self.vectors.get(key as usize).expect("vector not found")
    }
}

impl VectorStorage for SimpleMultiDenseVectorStorage {
    fn vector_dim(&self) -> usize {
        self.dim
    }

    fn distance(&self) -> Distance {
        self.distance
    }

    fn is_on_disk(&self) -> bool {
        false
    }

    fn total_vector_count(&self) -> usize {
        self.vectors.len()
    }

    fn get_vector(&self, key: PointOffsetType) -> CowVector {
        let multi_dense_vector = self.vectors.get(key as usize).expect("vector not found");
        CowVector::from(multi_dense_vector.as_slice())
    }

    fn insert_vector(&mut self, key: PointOffsetType, vector: VectorRef) -> OperationResult<()> {
        let vector: &[DenseVector] = vector.try_into()?;
        let multi_vector = vector.to_vec();
        let key_usize = key as usize;
        if key_usize >= self.vectors.len() {
            self.vectors.resize(key_usize + 1, vec![]);
        }
        self.vectors[key_usize] = multi_vector.clone();
        self.set_deleted(key, false);
        self.update_stored(key, false, Some(multi_vector))?;
        Ok(())
    }

    fn update_from(
        &mut self,
        other: &VectorStorageEnum,
        other_ids: &mut impl Iterator<Item = PointOffsetType>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let start_index = self.vectors.len() as PointOffsetType;
        for point_id in other_ids {
            check_process_stopped(stopped)?;
            // Do not perform preprocessing - vectors should be already processed
            let other_vector = other.get_vector(point_id);
            let other_vector: &[DenseVector] = other_vector.as_vec_ref().try_into()?;
            let other_multi_vector = other_vector.to_vec();
            let other_deleted = other.is_deleted_vector(point_id);
            self.vectors.push(other_multi_vector.clone());
            let new_id = self.vectors.len() as PointOffsetType - 1;
            self.set_deleted(new_id, other_deleted);
            self.update_stored(new_id, other_deleted, Some(other_multi_vector))?;
        }
        let end_index = self.vectors.len() as PointOffsetType;
        Ok(start_index..end_index)
    }

    fn flusher(&self) -> Flusher {
        self.db_wrapper.flusher()
    }

    fn files(&self) -> Vec<std::path::PathBuf> {
        vec![]
    }

    fn delete_vector(&mut self, key: PointOffsetType) -> OperationResult<bool> {
        let is_deleted = !self.set_deleted(key, true);
        if is_deleted {
            self.update_stored(key, true, None)?;
        }
        Ok(is_deleted)
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        self.deleted.get(key as usize).map(|b| *b).unwrap_or(false)
    }

    fn deleted_vector_count(&self) -> usize {
        self.deleted_count
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        self.deleted.as_bitslice()
    }
}
