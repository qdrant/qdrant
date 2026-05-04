use std::borrow::Cow;
use std::path::Path;

use common::bitvec::BitSlice;
use common::generic_consts::AccessPattern;
use common::mmap::AdviceSetting;
use common::types::PointOffsetType;

use crate::common::operation_error::OperationResult;
use crate::data_types::named_vectors::CowVector;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{VectorElementType, VectorElementTypeByte, VectorElementTypeHalf};
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::dense::appendable_dense_vector_storage::{
    AppendableMmapDenseVectorStorage, open_appendable_memmap_vector_storage_impl_read_only,
};
use crate::vector_storage::read_only::VectorStorageReadEnum;
use crate::vector_storage::{DenseVectorStorage, VectorStorageRead};

/// Read-only newtype wrapper around [`AppendableMmapDenseVectorStorage`].
///
/// Exposes only [`VectorStorageRead`] and [`DenseVectorStorage`].
pub struct ReadOnlyAppendableDenseVectorStorage<T: PrimitiveVectorElement>(
    AppendableMmapDenseVectorStorage<T>,
);

impl<T: PrimitiveVectorElement> ReadOnlyAppendableDenseVectorStorage<T> {
    pub fn open(
        path: &Path,
        dim: usize,
        distance: Distance,
        madvise: AdviceSetting,
        populate: bool,
    ) -> OperationResult<Self> {
        let storage = open_appendable_memmap_vector_storage_impl_read_only::<T>(
            path, dim, distance, madvise, populate,
        )?;
        Ok(Self(storage))
    }
}

impl<T: PrimitiveVectorElement> DenseVectorStorage<T> for ReadOnlyAppendableDenseVectorStorage<T> {
    fn vector_dim(&self) -> usize {
        self.0.vector_dim()
    }

    fn get_dense<P: AccessPattern>(&self, key: PointOffsetType) -> Cow<'_, [T]> {
        self.0.get_dense::<P>(key)
    }

    fn for_each_in_dense_batch<F: FnMut(usize, &[T])>(&self, keys: &[PointOffsetType], f: F) {
        self.0.for_each_in_dense_batch(keys, f);
    }
}

impl<T: PrimitiveVectorElement> VectorStorageRead for ReadOnlyAppendableDenseVectorStorage<T> {
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

/// Open an appendable mmap dense vector storage as read-only.
pub fn open_read_only_appendable_dense_vector_storage(
    storage_element_type: VectorStorageDatatype,
    path: &Path,
    dim: usize,
    distance: Distance,
    madvise: AdviceSetting,
    populate: bool,
) -> OperationResult<VectorStorageReadEnum> {
    match storage_element_type {
        VectorStorageDatatype::Float32 => {
            let storage = ReadOnlyAppendableDenseVectorStorage::<VectorElementType>::open(
                path, dim, distance, madvise, populate,
            )?;
            Ok(VectorStorageReadEnum::DenseAppendable(Box::new(storage)))
        }
        VectorStorageDatatype::Uint8 => {
            let storage = ReadOnlyAppendableDenseVectorStorage::<VectorElementTypeByte>::open(
                path, dim, distance, madvise, populate,
            )?;
            Ok(VectorStorageReadEnum::DenseAppendableByte(Box::new(
                storage,
            )))
        }
        VectorStorageDatatype::Float16 => {
            let storage = ReadOnlyAppendableDenseVectorStorage::<VectorElementTypeHalf>::open(
                path, dim, distance, madvise, populate,
            )?;
            Ok(VectorStorageReadEnum::DenseAppendableHalf(Box::new(
                storage,
            )))
        }
    }
}
