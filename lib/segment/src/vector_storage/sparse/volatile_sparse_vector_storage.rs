use std::ops::Range;
use std::sync::atomic::AtomicBool;

use bitvec::prelude::{BitSlice, BitVec};
use common::counter::hardware_counter::HardwareCounterCell;
use common::ext::BitSliceExt as _;
use common::types::PointOffsetType;
use sparse::common::sparse_vector::SparseVector;
use sparse::common::types::{DimId, DimWeight};

use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::VectorRef;
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::bitvec::bitvec_set_deleted;
use crate::vector_storage::{SparseVectorStorage, VectorStorage, VectorStorageEnum};

pub const SPARSE_VECTOR_DISTANCE: Distance = Distance::Dot;

/// In-memory vector storage with on-update persistence using `store`
#[derive(Default, Debug)]
pub struct VolatileSparseVectorStorage {
    vectors: Vec<Option<SparseVector>>,
    /// BitVec for deleted flags. Grows dynamically upto last set flag.
    deleted: BitVec,
    /// Current number of deleted vectors.
    deleted_count: usize,
    total_vector_count: usize,
    /// Total number of non-zero elements in all vectors. Used to estimate average vector size.
    total_sparse_size: usize,
}

pub fn new_volatile_sparse_vector_storage() -> VectorStorageEnum {
    VectorStorageEnum::SparseVolatile(VolatileSparseVectorStorage::default())
}

impl VolatileSparseVectorStorage {
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
    ) {
        // Resize sparse vector container if needed
        if key as usize >= self.vectors.len() {
            if deleted {
                return;
            }
            self.vectors.resize(key as usize + 1, None);
        }

        let entry = &mut self.vectors[key as usize];

        // Update bookkeeping of total sparse size
        let elements_removed = entry.as_ref().map_or(0, |v| v.indices.len());
        let elements_added = vector
            .as_ref()
            .filter(|_| !deleted)
            .map_or(0, |v| v.indices.len());
        self.total_sparse_size = self
            .total_sparse_size
            .saturating_sub(elements_removed)
            .saturating_add(elements_added);

        if deleted {
            entry.take();
        } else {
            *entry = vector.cloned();
        }
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

impl SparseVectorStorage for VolatileSparseVectorStorage {
    fn get_sparse(&self, key: PointOffsetType) -> OperationResult<SparseVector> {
        let vector = self
            .get_sparse_opt(key)?
            .ok_or_else(|| OperationError::service_error("Sparse vector not found in storage"))?;
        Ok(vector)
    }

    fn get_sparse_sequential(&self, key: PointOffsetType) -> OperationResult<SparseVector> {
        // Already in memory, so no sequential optimizations available.
        self.get_sparse(key)
    }

    fn get_sparse_opt(&self, key: PointOffsetType) -> OperationResult<Option<SparseVector>> {
        let opt_vector = self.vectors.get(key as usize).cloned().flatten();
        Ok(opt_vector)
    }
}

impl VectorStorage for VolatileSparseVectorStorage {
    fn distance(&self) -> Distance {
        SPARSE_VECTOR_DISTANCE
    }

    fn datatype(&self) -> VectorStorageDatatype {
        VectorStorageDatatype::Float32
    }

    fn is_on_disk(&self) -> bool {
        false
    }

    fn total_vector_count(&self) -> usize {
        self.total_vector_count
    }

    fn get_vector(&self, key: PointOffsetType) -> CowVector {
        let vector = self.get_vector_opt(key);
        vector.unwrap_or_else(CowVector::default_sparse)
    }

    fn get_vector_sequential(&self, key: PointOffsetType) -> CowVector {
        // In memory, so no sequential read optimization.
        self.get_vector(key)
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
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let vector: &SparseVector = vector.try_into()?;
        debug_assert!(vector.is_sorted());
        self.total_vector_count = std::cmp::max(self.total_vector_count, key as usize + 1);
        self.set_deleted(key, false);
        self.update_stored(key, false, Some(vector));
        Ok(())
    }

    fn update_from<'a>(
        &mut self,
        other_vectors: &'a mut impl Iterator<Item = (CowVector<'a>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let start_index = self.total_vector_count as PointOffsetType;
        for (other_vector, other_deleted) in other_vectors {
            check_process_stopped(stopped)?;
            // Do not perform preprocessing - vectors should be already processed
            let other_vector = other_vector.as_vec_ref().try_into()?;
            let new_id = self.total_vector_count as PointOffsetType;
            self.total_vector_count += 1;
            self.set_deleted(new_id, other_deleted);
            self.update_stored(new_id, other_deleted, Some(other_vector));
        }
        Ok(start_index..self.total_vector_count as PointOffsetType)
    }

    fn flusher(&self) -> Flusher {
        Box::new(|| Ok(()))
    }

    fn files(&self) -> Vec<std::path::PathBuf> {
        vec![]
    }

    fn delete_vector(&mut self, key: PointOffsetType) -> OperationResult<bool> {
        let is_deleted = !self.set_deleted(key, true);
        if is_deleted {
            let old_vector = self.get_sparse_opt(key).ok().flatten();
            self.update_stored(key, true, old_vector.as_ref());
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
