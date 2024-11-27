use std::ops::{ControlFlow, Range};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bitvec::slice::BitSlice;
use bitvec::vec::BitVec;
use blob_store::BlobStore;
use common::iterator_ext::IteratorExt;
use common::types::PointOffsetType;
use parking_lot::RwLock;
use sparse::common::sparse_vector::SparseVector;
use sparse::common::types::{DimId, DimWeight};

use super::simple_sparse_vector_storage::SPARSE_VECTOR_DISTANCE;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::VectorRef;
use crate::types::VectorStorageDatatype;
use crate::vector_storage::bitvec::bitvec_set_deleted;
use crate::vector_storage::{SparseVectorStorage, VectorStorage};

/// Memory-mapped mutable sparse vector storage.
#[derive(Debug)]
pub struct MmapSparseVectorStorage {
    storage: Arc<RwLock<BlobStore<SparseVector>>>,
    /// BitVec for deleted flags. Grows dynamically upto last set flag.
    deleted: BitVec,
    /// Current number of deleted vectors.
    deleted_count: usize,
    /// Maximum point offset in the storage + 1. This also means the total amount of point offsets
    next_point_offset: usize,
    /// Total number of non-zero elements in all vectors. Used to estimate average vector size.
    total_sparse_size: usize,
}

impl MmapSparseVectorStorage {
    pub fn open_or_create(path: &Path, stopped: &AtomicBool) -> OperationResult<Self> {
        let path = path.to_path_buf();
        let storage: BlobStore<SparseVector> = BlobStore::open_or_create(path, Default::default())
            .map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to open mmap sparse vector storage: {err}"
                ))
            })?;

        Self::load(storage, stopped)
    }

    fn load(storage: BlobStore<SparseVector>, stopped: &AtomicBool) -> OperationResult<Self> {
        let mut deleted = BitVec::new();
        let mut deleted_count = 0;
        let mut next_point_offset = 0;
        let mut total_sparse_size = 0;
        const CHECK_STOP_INTERVAL: usize = 100;

        storage
            .for_each_unfiltered(|point_id, opt_vector| {
                if let Some(vector) = opt_vector {
                    total_sparse_size += vector.values.len();
                } else {
                    // Propagate deleted flag
                    bitvec_set_deleted(&mut deleted, point_id, true);
                    deleted_count += 1;
                }

                next_point_offset = next_point_offset.max(point_id as usize + 1);

                if next_point_offset % CHECK_STOP_INTERVAL == 0 && stopped.load(Ordering::Relaxed) {
                    return ControlFlow::Break("Process cancelled".to_string());
                }

                ControlFlow::Continue(())
            })
            .map_err(OperationError::service_error)?;

        let storage = Arc::new(RwLock::new(storage));

        Ok(Self {
            storage,
            deleted,
            deleted_count,
            next_point_offset,
            total_sparse_size,
        })
    }

    #[inline]
    fn set_deleted(&mut self, key: PointOffsetType, deleted: bool) -> bool {
        if !deleted && key as usize >= self.next_point_offset {
            return false;
        }
        let previously_deleted = bitvec_set_deleted(&mut self.deleted, key, deleted);
        // update deleted_count if it changed
        match (previously_deleted, deleted) {
            (false, true) => self.deleted_count += 1,
            (true, false) => self.deleted_count = self.deleted_count.saturating_sub(1),
            _ => {}
        }
        previously_deleted
    }

    fn update_stored(
        &mut self,
        key: PointOffsetType,
        vector: Option<&SparseVector>,
    ) -> OperationResult<()> {
        let mut storage_guard = self.storage.write();
        if let Some(vector) = vector {
            // upsert vector
            if let Some(old_vector) = storage_guard.get_value(key) {
                // it is an update
                self.total_sparse_size = self
                    .total_sparse_size
                    .saturating_sub(old_vector.values.len());
            }

            self.total_sparse_size += vector.values.len();
            storage_guard
                .put_value(key, vector)
                .map_err(OperationError::service_error)?;
        } else {
            // delete vector
            if let Some(old_vector) = storage_guard.delete_value(key) {
                self.total_sparse_size = self
                    .total_sparse_size
                    .saturating_sub(old_vector.values.len());
            }
        }

        self.next_point_offset = std::cmp::max(self.next_point_offset, key as usize + 1);

        Ok(())
    }
}

impl SparseVectorStorage for MmapSparseVectorStorage {
    fn get_sparse(
        &self,
        key: PointOffsetType,
    ) -> crate::common::operation_error::OperationResult<SparseVector> {
        self.get_sparse_opt(key)?
            .ok_or_else(|| OperationError::service_error(format!("Key {key} not found")))
    }

    fn get_sparse_opt(
        &self,
        key: PointOffsetType,
    ) -> crate::common::operation_error::OperationResult<Option<SparseVector>> {
        Ok(self.storage.read().get_value(key))
    }
}

impl VectorStorage for MmapSparseVectorStorage {
    fn distance(&self) -> crate::types::Distance {
        SPARSE_VECTOR_DISTANCE
    }

    fn datatype(&self) -> crate::types::VectorStorageDatatype {
        VectorStorageDatatype::Float32
    }

    fn is_on_disk(&self) -> bool {
        true
    }

    fn total_vector_count(&self) -> usize {
        self.next_point_offset
    }

    fn size_of_available_vectors_in_bytes(&self) -> usize {
        if self.next_point_offset == 0 {
            return 0;
        }
        let available_fraction =
            (self.next_point_offset - self.deleted_count) as f32 / self.next_point_offset as f32;
        let available_size = (self.total_sparse_size as f32 * available_fraction) as usize;
        available_size * (std::mem::size_of::<DimWeight>() + std::mem::size_of::<DimId>())
    }

    fn get_vector(&self, key: PointOffsetType) -> CowVector {
        let vector = self.get_vector_opt(key);
        vector.unwrap_or_else(CowVector::default_sparse)
    }

    /// Get vector by key, if it exists.
    ///
    /// Ignore any error
    fn get_vector_opt(&self, key: PointOffsetType) -> Option<CowVector> {
        match self.get_sparse_opt(key) {
            Ok(Some(vector)) => Some(CowVector::from(vector)),
            _ => None,
        }
    }

    fn insert_vector(&mut self, key: PointOffsetType, vector: VectorRef) -> OperationResult<()> {
        let vector = <&SparseVector>::try_from(vector)?;
        debug_assert!(vector.is_sorted());
        self.set_deleted(key, false);
        self.update_stored(key, Some(vector))?;
        Ok(())
    }

    fn update_from<'a>(
        &mut self,
        other_vectors: &'a mut impl Iterator<Item = (CowVector<'a>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let start_index = self.next_point_offset as PointOffsetType;
        for (other_vector, other_deleted) in
            other_vectors.check_stop(|| stopped.load(Ordering::Relaxed))
        {
            // Do not perform preprocessing - vectors should be already processed
            let other_vector = other_vector.as_vec_ref().try_into()?;
            let new_id = self.next_point_offset as PointOffsetType;
            self.next_point_offset += 1;
            self.set_deleted(new_id, other_deleted);

            let vector = (!other_deleted).then_some(other_vector);
            self.update_stored(new_id, vector)?;
        }
        Ok(start_index..self.next_point_offset as PointOffsetType)
    }

    fn flusher(&self) -> crate::common::Flusher {
        let storage = self.storage.clone();
        Box::new(move || {
            storage.read().flush().map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to flush mmap sparse vector storage: {err}"
                ))
            })?;
            Ok(())
        })
    }

    fn files(&self) -> Vec<std::path::PathBuf> {
        self.storage.read().files()
    }

    fn delete_vector(
        &mut self,
        key: common::types::PointOffsetType,
    ) -> crate::common::operation_error::OperationResult<bool> {
        let was_deleted = !self.set_deleted(key, true);

        self.update_stored(key, None)?;

        Ok(was_deleted)
    }

    fn is_deleted_vector(&self, key: common::types::PointOffsetType) -> bool {
        self.deleted.get(key as usize).map(|b| *b).unwrap_or(false)
    }

    fn deleted_vector_count(&self) -> usize {
        self.deleted_count
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        self.deleted.as_bitslice()
    }
}
