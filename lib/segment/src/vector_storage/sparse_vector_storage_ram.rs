use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use bitvec::slice::BitSlice;
use bitvec::vec::BitVec;
use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;
use sparse::common::sparse_vector::SparseVector;

use super::simple_vector_storage::bitvec_set_deleted;
use super::vector_storage_base::VectorStorage;
use super::VectorStorageEnum;
use crate::common::operation_error::{check_process_stopped, OperationError, OperationResult};
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::common::Flusher;
use crate::data_types::vectors::VectorRef;
use crate::types::Distance;

pub struct SparseVectorStorage {
    total_weights: usize,
    vectors: Vec<SparseVector>,
    db_wrapper: DatabaseColumnWrapper,
    /// BitVec for deleted flags. Grows dynamically upto last set flag.
    deleted: BitVec,
    /// Current number of deleted vectors.
    deleted_count: usize,
}

pub fn open_sparse_vector_storage(
    database: Arc<RwLock<DB>>,
    database_column_name: &str,
) -> OperationResult<Arc<AtomicRefCell<VectorStorageEnum>>> {
    let db_wrapper = DatabaseColumnWrapper::new(database, database_column_name);
    let mut vectors = Vec::new();
    let mut total_weights = 0;

    let (mut deleted, mut deleted_count) = (BitVec::new(), 0);
    for (key, value) in db_wrapper.lock_db().iter()? {
        let point_id: PointOffsetType = bincode::deserialize(&key)
            .map_err(|_| OperationError::service_error("cannot deserialize point id from db"))?;

        vectors.resize_with(
            std::cmp::max(vectors.len(), point_id as usize + 1),
            Default::default,
        );
        if value.is_empty() {
            bitvec_set_deleted(&mut deleted, point_id, true);
            deleted_count += 1;
        } else {
            let sparse_vector: SparseVector = bincode::deserialize(&value).map_err(|_| {
                OperationError::service_error("cannot deserialize point id from db")
            })?;
            total_weights += sparse_vector.indices.len();
            vectors[point_id as usize] = sparse_vector;
        };
    }

    Ok(Arc::new(AtomicRefCell::new(VectorStorageEnum::SparseRam(
        SparseVectorStorage {
            total_weights,
            vectors,
            db_wrapper,
            deleted,
            deleted_count,
        },
    ))))
}

impl SparseVectorStorage {
    fn set_deleted(&mut self, key: PointOffsetType, deleted: bool) -> bool {
        if key as usize >= self.vectors.len() {
            return false;
        }
        let was_deleted = bitvec_set_deleted(&mut self.deleted, key, deleted);
        if was_deleted != deleted {
            if !was_deleted {
                self.deleted_count += 1;
            } else {
                self.deleted_count -= 1;
            }
        }
        was_deleted
    }

    fn update_stored(
        &mut self,
        key: PointOffsetType,
        deleted: bool,
        vector: Option<&SparseVector>,
    ) -> OperationResult<()> {
        let record = if !deleted {
            if let Some(vector) = vector {
                SparseVector::encode(vector)
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };
        self.db_wrapper.put(
            bincode::serialize(&key).unwrap(),
            bincode::serialize(&record).unwrap(),
        )?;
        Ok(())
    }
}

impl VectorStorage for SparseVectorStorage {
    fn vector_dim(&self) -> usize {
        let exist_count = self.total_vector_count() - self.deleted_count;
        if exist_count != 0 && self.total_weights != 0 {
            self.total_weights / (self.total_vector_count() - self.deleted_count)
        } else {
            1
        }
    }

    fn distance(&self) -> Distance {
        Distance::Dot
    }

    fn is_on_disk(&self) -> bool {
        false
    }

    fn total_vector_count(&self) -> usize {
        self.vectors.len()
    }

    fn get_vector(&self, key: PointOffsetType) -> VectorRef {
        let result = self.vectors.get(key as usize).unwrap();
        result.into()
    }

    fn insert_vector(&mut self, key: PointOffsetType, vector: VectorRef) -> OperationResult<()> {
        let vector: &SparseVector = vector.try_into()?;
        self.vectors.resize_with(
            std::cmp::max(self.vectors.len(), key as usize + 1),
            Default::default,
        );
        self.vectors[key as usize] = vector.clone();
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
            let other_vector: &SparseVector = other.get_vector(point_id).try_into()?;
            let other_deleted = other.is_deleted_vector(point_id);
            let new_id = self.vectors.len() as PointOffsetType;
            self.vectors.push(other_vector.clone());
            self.set_deleted(new_id, other_deleted);
            self.update_stored(new_id, other_deleted, Some(other_vector))?;
        }
        let end_index = self.vectors.len() as PointOffsetType;
        Ok(start_index..end_index)
    }

    fn flusher(&self) -> Flusher {
        self.db_wrapper.flusher()
    }

    fn files(&self) -> Vec<PathBuf> {
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
