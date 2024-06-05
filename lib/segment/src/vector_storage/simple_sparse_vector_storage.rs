use std::ops::Range;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use bitvec::prelude::{BitSlice, BitVec};
use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;
use sparse::common::sparse_vector::SparseVector;

use super::SparseVectorStorage;
use crate::common::operation_error::{check_process_stopped, OperationError, OperationResult};
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::common::Flusher;
use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::VectorRef;
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::bitvec::bitvec_set_deleted;
use crate::vector_storage::common::StoredRecord;
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

pub const SPARSE_VECTOR_DISTANCE: Distance = Distance::Dot;

type StoredSparseVector = StoredRecord<SparseVector>;

/// In-memory vector storage with on-update persistence using `store`
pub struct SimpleSparseVectorStorage {
    db_wrapper: DatabaseColumnWrapper,
    update_buffer: StoredSparseVector,
    /// BitVec for deleted flags. Grows dynamically upto last set flag.
    deleted: BitVec,
    /// Current number of deleted vectors.
    deleted_count: usize,
    total_vector_count: usize,
    /// Total number of non-zero elements in all vectors. Used to estimate average vector size.
    total_sparse_size: usize,
}

pub fn open_simple_sparse_vector_storage(
    database: Arc<RwLock<DB>>,
    database_column_name: &str,
    stopped: &AtomicBool,
) -> OperationResult<VectorStorageEnum> {
    let (mut deleted, mut deleted_count) = (BitVec::new(), 0);
    let db_wrapper = DatabaseColumnWrapper::new(database, database_column_name);

    let mut total_vector_count = 0;
    let mut total_sparse_size = 0;
    db_wrapper.lock_db().iter()?;
    for (key, value) in db_wrapper.lock_db().iter()? {
        let point_id: PointOffsetType = bincode::deserialize(&key)
            .map_err(|_| OperationError::service_error("cannot deserialize point id from db"))?;
        let stored_record: StoredSparseVector = bincode::deserialize(&value)
            .map_err(|_| OperationError::service_error("cannot deserialize record from db"))?;

        // Propagate deleted flag
        if stored_record.deleted {
            bitvec_set_deleted(&mut deleted, point_id, true);
            deleted_count += 1;
        }
        total_vector_count = std::cmp::max(total_vector_count, point_id as usize + 1);
        total_sparse_size += stored_record.vector.values.len();

        check_process_stopped(stopped)?;
    }

    Ok(VectorStorageEnum::SparseSimple(SimpleSparseVectorStorage {
        db_wrapper,
        update_buffer: StoredSparseVector {
            deleted: false,
            vector: SparseVector::default(),
        },
        deleted,
        deleted_count,
        total_vector_count,
        total_sparse_size,
    }))
}

impl SimpleSparseVectorStorage {
    /// Set deleted flag for given key. Returns previous deleted state.
    #[inline]
    fn set_deleted(&mut self, key: PointOffsetType, deleted: bool) -> bool {
        if key as usize >= self.total_vector_count {
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
        vector: Option<&SparseVector>,
    ) -> OperationResult<()> {
        // Write vector state to buffer record
        let record = &mut self.update_buffer;
        record.deleted = deleted;
        if let Some(vector) = vector {
            if deleted {
                self.total_sparse_size = self.total_sparse_size.saturating_sub(vector.values.len());
            } else {
                self.total_sparse_size += vector.values.len();
            }
            record.vector = vector.clone();
        }

        // Store updated record
        self.db_wrapper.put(
            bincode::serialize(&key).unwrap(),
            bincode::serialize(&record).unwrap(),
        )?;

        Ok(())
    }
}

impl SparseVectorStorage for SimpleSparseVectorStorage {
    fn get_sparse(&self, key: PointOffsetType) -> OperationResult<SparseVector> {
        let bin_key = bincode::serialize(&key)
            .map_err(|_| OperationError::service_error("Cannot serialize sparse vector key"))?;
        let data = self.db_wrapper.get(bin_key)?;
        let record: StoredSparseVector = bincode::deserialize(&data).map_err(|_| {
            OperationError::service_error("Cannot deserialize sparse vector from db")
        })?;
        Ok(record.vector)
    }
}

impl VectorStorage for SimpleSparseVectorStorage {
    fn distance(&self) -> Distance {
        SPARSE_VECTOR_DISTANCE
    }

    fn datatype(&self) -> VectorStorageDatatype {
        VectorStorageDatatype::Float32
    }

    fn is_on_disk(&self) -> bool {
        true
    }

    fn total_vector_count(&self) -> usize {
        self.total_vector_count
    }

    fn available_size_in_bytes(&self) -> usize {
        self.total_sparse_size * (std::mem::size_of::<f32>() + std::mem::size_of::<u32>())
    }

    fn get_vector(&self, key: PointOffsetType) -> CowVector {
        let vector = self.get_vector_opt(key);
        debug_assert!(vector.is_some());
        vector.unwrap_or_else(CowVector::default_sparse)
    }

    fn get_vector_opt(&self, key: PointOffsetType) -> Option<CowVector> {
        // ignore any error
        self.get_sparse(key).ok().map(CowVector::from)
    }

    fn insert_vector(&mut self, key: PointOffsetType, vector: VectorRef) -> OperationResult<()> {
        let vector: &SparseVector = vector.try_into()?;
        debug_assert!(vector.is_sorted());
        self.total_vector_count = std::cmp::max(self.total_vector_count, key as usize + 1);
        self.set_deleted(key, false);
        self.update_stored(key, false, Some(vector))?;
        Ok(())
    }

    fn update_from(
        &mut self,
        other: &VectorStorageEnum,
        other_ids: &mut impl Iterator<Item = PointOffsetType>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let start_index = self.total_vector_count as PointOffsetType;
        for point_id in other_ids {
            check_process_stopped(stopped)?;
            // Do not perform preprocessing - vectors should be already processed
            let other_vector = other.get_vector(point_id);
            let other_vector = other_vector.as_vec_ref().try_into()?;
            let other_deleted = other.is_deleted_vector(point_id);
            let new_id = self.total_vector_count as PointOffsetType;
            self.total_vector_count += 1;
            self.set_deleted(new_id, other_deleted);
            self.update_stored(new_id, other_deleted, Some(other_vector))?;
        }
        Ok(start_index..self.total_vector_count as PointOffsetType)
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
