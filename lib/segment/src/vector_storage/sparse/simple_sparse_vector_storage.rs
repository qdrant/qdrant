use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use bitvec::prelude::{BitSlice, BitVec};
use common::counter::hardware_counter::HardwareCounterCell;
use common::ext::BitSliceExt as _;
use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;
use sparse::common::sparse_vector::SparseVector;
use sparse::common::types::{DimId, DimWeight};

use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::common::rocksdb_buffered_update_wrapper::DatabaseColumnScheduledUpdateWrapper;
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::VectorRef;
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::bitvec::bitvec_set_deleted;
use crate::vector_storage::common::StoredRecord;
use crate::vector_storage::{SparseVectorStorage, VectorStorage, VectorStorageEnum};

pub const SPARSE_VECTOR_DISTANCE: Distance = Distance::Dot;

type StoredSparseVector = StoredRecord<SparseVector>;

/// In-memory vector storage with on-update persistence using `store`
#[derive(Debug)]
pub struct SimpleSparseVectorStorage {
    /// Database wrapper which only persists on flush
    db_wrapper: DatabaseColumnScheduledUpdateWrapper,
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
    let db_wrapper = DatabaseColumnScheduledUpdateWrapper::new(db_wrapper);
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
        } else {
            total_sparse_size += stored_record.vector.values.len();
        }
        total_vector_count = total_vector_count.max(point_id as usize + 1);

        check_process_stopped(stopped)?;
    }

    Ok(VectorStorageEnum::SparseSimple(SimpleSparseVectorStorage {
        db_wrapper,
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
        if !deleted && key as usize >= self.total_vector_count {
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
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        // Write vector state to buffer record
        let record = StoredSparseVector {
            deleted,
            vector: vector.cloned().unwrap_or_default(),
        };
        if let Some(vector) = vector {
            if deleted {
                self.total_sparse_size = self.total_sparse_size.saturating_sub(vector.values.len());
            } else {
                self.total_sparse_size += vector.values.len();
            }
        }

        let key_enc = bincode::serialize(&key).unwrap();
        let record_enc = bincode::serialize(&record).unwrap();

        hw_counter
            .vector_io_write_counter()
            .incr_delta(key_enc.len() + record_enc.len());

        // Store updated record
        self.db_wrapper.put(key_enc, record_enc)?;

        Ok(())
    }

    pub fn size_of_available_vectors_in_bytes(&self) -> usize {
        if self.total_vector_count == 0 {
            return 0;
        }
        let available_fraction =
            (self.total_vector_count - self.deleted_count) as f32 / self.total_vector_count as f32;
        let available_size = (self.total_sparse_size as f32 * available_fraction) as usize;
        available_size * (std::mem::size_of::<DimWeight>() + std::mem::size_of::<DimId>())
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

    fn get_sparse_opt(&self, key: PointOffsetType) -> OperationResult<Option<SparseVector>> {
        let bin_key = bincode::serialize(&key)
            .map_err(|_| OperationError::service_error("Cannot serialize sparse vector key"))?;
        if let Some(data) = self.db_wrapper.get_opt(bin_key)? {
            let StoredSparseVector { deleted, vector } =
                bincode::deserialize(&data).map_err(|_| {
                    OperationError::service_error("Cannot deserialize sparse vector from db")
                })?;
            if deleted {
                return Ok(None);
            }
            Ok(Some(vector))
        } else {
            Ok(None)
        }
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

    fn get_vector(&self, key: PointOffsetType) -> CowVector {
        let vector = self.get_vector_opt(key);
        vector.unwrap_or_else(CowVector::default_sparse)
    }

    /// Get vector by key, if it exists.
    ///
    /// ignore any error
    fn get_vector_opt(&self, key: PointOffsetType) -> Option<CowVector> {
        match self.get_sparse_opt(key) {
            Ok(Some(vector)) => Some(CowVector::from(vector)),
            _ => None,
        }
    }

    fn insert_vector(
        &mut self,
        key: PointOffsetType,
        vector: VectorRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let vector: &SparseVector = vector.try_into()?;
        debug_assert!(vector.is_sorted());
        self.total_vector_count = std::cmp::max(self.total_vector_count, key as usize + 1);
        self.set_deleted(key, false);
        self.update_stored(key, false, Some(vector), hw_counter)?;
        Ok(())
    }

    fn update_from<'a>(
        &mut self,
        other_vectors: &'a mut impl Iterator<Item = (CowVector<'a>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let start_index = self.total_vector_count as PointOffsetType;
        let disposed_hw = HardwareCounterCell::disposable(); // This function is only used for internal operations.
        for (other_vector, other_deleted) in other_vectors {
            check_process_stopped(stopped)?;
            // Do not perform preprocessing - vectors should be already processed
            let other_vector = other_vector.as_vec_ref().try_into()?;
            let new_id = self.total_vector_count as PointOffsetType;
            self.total_vector_count += 1;
            self.set_deleted(new_id, other_deleted);
            self.update_stored(new_id, other_deleted, Some(other_vector), &disposed_hw)?;
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
            let old_vector = self.get_sparse_opt(key).ok().flatten();
            self.update_stored(
                key,
                true,
                old_vector.as_ref(),
                &HardwareCounterCell::disposable(), // We don't measure deletions
            )?;
        }
        Ok(is_deleted)
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        self.deleted.get_bit(key as usize).unwrap_or(false)
    }

    fn deleted_vector_count(&self) -> usize {
        self.deleted_count
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        self.deleted.as_bitslice()
    }
}
