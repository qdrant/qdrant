use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use common::bitvec::{BitSlice, BitVec};
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::AccessPattern;
use common::types::PointOffsetType;
use sparse::common::sparse_vector::SparseVector;

use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::VectorRef;
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::sparse::SPARSE_VECTOR_DISTANCE;
use crate::vector_storage::{
    SparseVectorStorage, VectorStorage, VectorStorageEnum, VectorStorageRead,
};

/// Placeholder sparse vector storage that contains no data.
///
/// All vectors are reported as deleted. Used for newly created sparse named vectors
/// on immutable segments where no actual vector data exists yet.
/// Reconstructed from config on segment load (no files on disk).
#[derive(Debug)]
pub struct EmptySparseVectorStorage {
    /// Number of points in this storage (all reported as deleted)
    num_points: usize,
    /// All-ones bitvec indicating every vector is deleted
    deleted_bitvec: BitVec,
}

impl EmptySparseVectorStorage {
    pub fn new(num_points: usize) -> Self {
        Self {
            num_points,
            deleted_bitvec: BitVec::repeat(true, num_points),
        }
    }

    /// Update the number of points. All points are marked as deleted.
    pub fn set_num_points(&mut self, num_points: usize) {
        self.num_points = num_points;
        self.deleted_bitvec.resize(num_points, true);
    }
}

pub fn new_empty_sparse_vector_storage(num_points: usize) -> VectorStorageEnum {
    VectorStorageEnum::EmptySparse(EmptySparseVectorStorage::new(num_points))
}

impl SparseVectorStorage for EmptySparseVectorStorage {
    fn get_sparse<P: AccessPattern>(&self, _key: PointOffsetType) -> OperationResult<SparseVector> {
        Ok(SparseVector::default())
    }

    fn get_sparse_opt<P: AccessPattern>(
        &self,
        _key: PointOffsetType,
    ) -> OperationResult<Option<SparseVector>> {
        Ok(None)
    }

    fn for_each_in_sparse_batch<F>(
        &self,
        _keys: &[PointOffsetType],
        _callback: F,
    ) -> OperationResult<()>
    where
        F: FnMut(usize, SparseVector),
    {
        Ok(())
    }
}

impl VectorStorageRead for EmptySparseVectorStorage {
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
        self.num_points
    }

    fn get_vector<P: AccessPattern>(&self, _key: PointOffsetType) -> CowVector<'_> {
        debug_assert!(false, "get_vector called on EmptySparseVectorStorage");
        CowVector::default_sparse()
    }

    fn get_vector_opt<P: AccessPattern>(&self, _key: PointOffsetType) -> Option<CowVector<'_>> {
        None
    }

    fn is_deleted_vector(&self, _key: PointOffsetType) -> bool {
        true
    }

    fn deleted_vector_count(&self) -> usize {
        self.num_points
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        self.deleted_bitvec.as_bitslice()
    }
}

impl VectorStorage for EmptySparseVectorStorage {
    fn insert_vector(
        &mut self,
        _key: PointOffsetType,
        _vector: VectorRef,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        Err(OperationError::service_error(
            "Cannot insert into empty sparse vector storage",
        ))
    }

    fn update_from<'a>(
        &mut self,
        _other_vectors: &'a mut impl Iterator<Item = (CowVector<'a>, bool)>,
        _stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        Err(OperationError::service_error(
            "Cannot update empty sparse vector storage",
        ))
    }

    fn flusher(&self) -> Flusher {
        Box::new(|| Ok(()))
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![]
    }

    fn delete_vector(&mut self, _key: PointOffsetType) -> OperationResult<bool> {
        Ok(false) // Already deleted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_sparse_basic_contract() {
        let storage = EmptySparseVectorStorage::new(500);

        assert_eq!(storage.distance(), Distance::Dot);
        assert_eq!(storage.datatype(), VectorStorageDatatype::Float32);
        assert!(storage.is_on_disk());
        assert_eq!(storage.total_vector_count(), 500);
        assert_eq!(storage.available_vector_count(), 0);
        assert_eq!(storage.deleted_vector_count(), 500);
        assert_eq!(storage.deleted_vector_bitslice().len(), 500);
        assert!(storage.is_deleted_vector(0));
        assert!(storage.is_deleted_vector(499));
        assert!(storage.files().is_empty());

        assert!(
            storage
                .get_vector_opt::<common::generic_consts::Random>(0)
                .is_none()
        );
        assert!(
            storage
                .get_sparse_opt::<common::generic_consts::Random>(0)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_empty_sparse_set_num_points() {
        let mut storage = EmptySparseVectorStorage::new(0);
        assert_eq!(storage.total_vector_count(), 0);

        storage.set_num_points(1000);
        assert_eq!(storage.total_vector_count(), 1000);
        assert_eq!(storage.deleted_vector_count(), 1000);
    }
}
