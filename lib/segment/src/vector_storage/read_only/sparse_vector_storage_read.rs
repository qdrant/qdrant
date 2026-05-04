use std::path::Path;

use common::bitvec::BitSlice;
use common::generic_consts::AccessPattern;
use common::types::PointOffsetType;
use sparse::common::sparse_vector::SparseVector;

use crate::common::operation_error::OperationResult;
use crate::data_types::named_vectors::CowVector;
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::read_only::VectorStorageReadEnum;
use crate::vector_storage::sparse::mmap_sparse_vector_storage::MmapSparseVectorStorage;
use crate::vector_storage::{SparseVectorStorage, VectorStorageRead};

/// Read-only newtype wrapper around [`MmapSparseVectorStorage`].
///
/// Exposes only [`VectorStorageRead`] and [`SparseVectorStorage`].
pub struct ReadOnlySparseVectorStorage(MmapSparseVectorStorage);

impl ReadOnlySparseVectorStorage {
    pub fn open(path: &Path) -> OperationResult<Self> {
        let storage = MmapSparseVectorStorage::open_read_only(path)?;
        Ok(Self(storage))
    }
}

impl SparseVectorStorage for ReadOnlySparseVectorStorage {
    fn get_sparse<P: AccessPattern>(&self, key: PointOffsetType) -> OperationResult<SparseVector> {
        self.0.get_sparse::<P>(key)
    }

    fn get_sparse_opt<P: AccessPattern>(
        &self,
        key: PointOffsetType,
    ) -> OperationResult<Option<SparseVector>> {
        self.0.get_sparse_opt::<P>(key)
    }

    fn for_each_in_sparse_batch<F>(
        &self,
        keys: &[PointOffsetType],
        callback: F,
    ) -> OperationResult<()>
    where
        F: FnMut(usize, SparseVector),
    {
        self.0.for_each_in_sparse_batch(keys, callback)
    }
}

impl VectorStorageRead for ReadOnlySparseVectorStorage {
    fn distance(&self) -> Distance {
        self.0.distance()
    }

    fn datatype(&self) -> VectorStorageDatatype {
        self.0.datatype()
    }

    fn is_on_disk(&self) -> bool {
        self.0.is_on_disk()
    }

    fn total_vector_count(&self) -> usize {
        self.0.total_vector_count()
    }

    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> CowVector<'_> {
        self.0.get_vector::<P>(key)
    }

    fn read_vectors<P: AccessPattern>(
        &self,
        keys: impl IntoIterator<Item = PointOffsetType>,
        callback: impl FnMut(PointOffsetType, CowVector<'_>),
    ) {
        self.0.read_vectors::<P>(keys, callback);
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        self.0.get_vector_opt::<P>(key)
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        self.0.is_deleted_vector(key)
    }

    fn deleted_vector_count(&self) -> usize {
        self.0.deleted_vector_count()
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        self.0.deleted_vector_bitslice()
    }
}

/// Open a sparse mmap vector storage as read-only.
pub fn open_read_only_sparse_vector_storage(path: &Path) -> OperationResult<VectorStorageReadEnum> {
    let storage = ReadOnlySparseVectorStorage::open(path)?;
    Ok(VectorStorageReadEnum::Sparse(Box::new(storage)))
}
