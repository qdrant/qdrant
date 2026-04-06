use std::borrow::Cow;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use common::bitvec::{BitSlice, BitVec};
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::AccessPattern;
use common::types::PointOffsetType;

use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::{VectorElementType, VectorRef};
use crate::types::{Distance, MultiVectorConfig, VectorStorageDatatype};
use crate::vector_storage::{DenseVectorStorage, VectorStorage, VectorStorageEnum};

/// Placeholder vector storage that contains no data.
///
/// All vectors are reported as deleted. Used for newly created named vectors
/// on immutable segments where no actual vector data exists yet.
/// Reconstructed from config on segment load (no files on disk).
#[derive(Debug)]
pub struct EmptyDenseVectorStorage {
    distance: Distance,
    dim: usize,
    datatype: VectorStorageDatatype,
    is_on_disk: bool,
    multi_vector_config: Option<MultiVectorConfig>,
    /// Number of points in this storage (all reported as deleted)
    num_points: usize,
    /// All-ones bitvec indicating every vector is deleted
    deleted_bitvec: BitVec,
}

impl EmptyDenseVectorStorage {
    pub fn new(
        dim: usize,
        distance: Distance,
        datatype: VectorStorageDatatype,
        is_on_disk: bool,
        multi_vector_config: Option<MultiVectorConfig>,
        num_points: usize,
    ) -> Self {
        Self {
            distance,
            dim,
            datatype,
            is_on_disk,
            multi_vector_config,
            num_points,
            deleted_bitvec: BitVec::repeat(true, num_points),
        }
    }

    /// Update the number of points. All points are marked as deleted.
    pub fn set_num_points(&mut self, num_points: usize) {
        self.num_points = num_points;
        self.deleted_bitvec.resize(num_points, true);
    }

    pub fn multi_vector_config(&self) -> Option<&MultiVectorConfig> {
        self.multi_vector_config.as_ref()
    }
}

pub fn new_empty_dense_vector_storage(
    dim: usize,
    distance: Distance,
    datatype: VectorStorageDatatype,
    is_on_disk: bool,
    multi_vector_config: Option<MultiVectorConfig>,
    num_points: usize,
) -> VectorStorageEnum {
    VectorStorageEnum::EmptyDense(EmptyDenseVectorStorage::new(
        dim,
        distance,
        datatype,
        is_on_disk,
        multi_vector_config,
        num_points,
    ))
}

impl DenseVectorStorage<VectorElementType> for EmptyDenseVectorStorage {
    fn vector_dim(&self) -> usize {
        self.dim
    }

    fn get_dense<P: AccessPattern>(&self, _key: PointOffsetType) -> Cow<'_, [VectorElementType]> {
        debug_assert!(false, "get_dense called on EmptyDenseVectorStorage");
        Cow::Owned(vec![0.0; self.dim])
    }
}

impl VectorStorage for EmptyDenseVectorStorage {
    fn distance(&self) -> Distance {
        self.distance
    }

    fn datatype(&self) -> VectorStorageDatatype {
        self.datatype
    }

    fn is_on_disk(&self) -> bool {
        self.is_on_disk
    }

    fn total_vector_count(&self) -> usize {
        self.num_points
    }

    fn get_vector<P: AccessPattern>(&self, _key: PointOffsetType) -> CowVector<'_> {
        debug_assert!(false, "get_vector called on EmptyDenseVectorStorage");
        CowVector::from(vec![0.0; self.dim])
    }

    fn get_vector_opt<P: AccessPattern>(&self, _key: PointOffsetType) -> Option<CowVector<'_>> {
        None
    }

    fn insert_vector(
        &mut self,
        _key: PointOffsetType,
        _vector: VectorRef,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        Err(OperationError::service_error(
            "Cannot insert into empty vector storage",
        ))
    }

    fn update_from<'a>(
        &mut self,
        _other_vectors: &'a mut impl Iterator<Item = (CowVector<'a>, bool)>,
        _stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        Err(OperationError::service_error(
            "Cannot update empty vector storage",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_dense_basic_contract() {
        let storage = EmptyDenseVectorStorage::new(
            128,
            Distance::Cosine,
            VectorStorageDatatype::Float32,
            true,
            None,
            1000,
        );

        assert_eq!(storage.distance(), Distance::Cosine);
        assert_eq!(storage.datatype(), VectorStorageDatatype::Float32);
        assert!(storage.is_on_disk());
        assert_eq!(storage.total_vector_count(), 1000);
        assert_eq!(storage.available_vector_count(), 0);
        assert_eq!(storage.deleted_vector_count(), 1000);
        assert_eq!(storage.deleted_vector_bitslice().len(), 1000);
        assert!(storage.is_deleted_vector(0));
        assert!(storage.is_deleted_vector(999));
        assert_eq!(storage.vector_dim(), 128);
        assert!(storage.files().is_empty());
        assert!(storage.multi_vector_config().is_none());

        // get_vector_opt always returns None
        assert!(
            storage
                .get_vector_opt::<common::generic_consts::Random>(0)
                .is_none()
        );
        assert!(
            storage
                .get_vector_opt::<common::generic_consts::Random>(500)
                .is_none()
        );
    }

    #[test]
    fn test_empty_dense_respects_on_disk_flag() {
        let storage_on_disk = EmptyDenseVectorStorage::new(
            64,
            Distance::Dot,
            VectorStorageDatatype::Float32,
            true,
            None,
            0,
        );
        assert!(storage_on_disk.is_on_disk());

        let storage_in_ram = EmptyDenseVectorStorage::new(
            64,
            Distance::Dot,
            VectorStorageDatatype::Float32,
            false,
            None,
            0,
        );
        assert!(!storage_in_ram.is_on_disk());
    }

    #[test]
    fn test_empty_dense_multi_vector_config() {
        let multi_cfg = MultiVectorConfig {
            comparator: crate::types::MultiVectorComparator::MaxSim,
        };
        let storage = EmptyDenseVectorStorage::new(
            64,
            Distance::Cosine,
            VectorStorageDatatype::Float32,
            true,
            Some(multi_cfg),
            0,
        );
        assert!(storage.multi_vector_config().is_some());
    }

    #[test]
    fn test_empty_dense_set_num_points() {
        let mut storage = EmptyDenseVectorStorage::new(
            64,
            Distance::Dot,
            VectorStorageDatatype::Float32,
            true,
            None,
            0,
        );
        assert_eq!(storage.total_vector_count(), 0);
        assert_eq!(storage.deleted_vector_count(), 0);

        storage.set_num_points(500);
        assert_eq!(storage.total_vector_count(), 500);
        assert_eq!(storage.deleted_vector_count(), 500);
        assert_eq!(storage.deleted_vector_bitslice().len(), 500);
    }

    #[test]
    fn test_empty_dense_insert_errors() {
        let mut storage = EmptyDenseVectorStorage::new(
            4,
            Distance::Cosine,
            VectorStorageDatatype::Float32,
            true,
            None,
            10,
        );
        let vector = vec![1.0, 2.0, 3.0, 4.0];
        let result = storage.insert_vector(
            0,
            VectorRef::from(&vector),
            &HardwareCounterCell::disposable(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_dense_delete_is_noop() {
        let mut storage = EmptyDenseVectorStorage::new(
            4,
            Distance::Cosine,
            VectorStorageDatatype::Float32,
            true,
            None,
            10,
        );
        // delete_vector returns false because it was already deleted
        assert!(!storage.delete_vector(0).unwrap());
    }
}
