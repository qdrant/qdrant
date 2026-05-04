use std::borrow::Cow;
use std::path::Path;

use common::bitvec::BitSlice;
use common::generic_consts::AccessPattern;
use common::mmap::AdviceSetting;
use common::types::PointOffsetType;

use crate::common::operation_error::OperationResult;
use crate::data_types::named_vectors::{CowMultiVector, CowVector};
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{
    TypedMultiDenseVectorRef, VectorElementType, VectorElementTypeByte, VectorElementTypeHalf,
};
use crate::types::{Distance, MultiVectorConfig, VectorStorageDatatype};
use crate::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::{
    AppendableMmapMultiDenseVectorStorage,
    open_appendable_memmap_multi_vector_storage_impl_read_only,
};
use crate::vector_storage::read_only::VectorStorageReadEnum;
use crate::vector_storage::{MultiVectorStorage, VectorStorageRead};

/// Read-only newtype wrapper around [`AppendableMmapMultiDenseVectorStorage`].
///
/// Exposes only [`VectorStorageRead`] and [`MultiVectorStorage`].
pub struct ReadOnlyMultiDenseVectorStorage<T: PrimitiveVectorElement>(
    AppendableMmapMultiDenseVectorStorage<T>,
);

impl<T: PrimitiveVectorElement> ReadOnlyMultiDenseVectorStorage<T> {
    pub fn open(
        path: &Path,
        dim: usize,
        distance: Distance,
        multi_vector_config: MultiVectorConfig,
        madvise: AdviceSetting,
        populate: bool,
    ) -> OperationResult<Self> {
        let storage = open_appendable_memmap_multi_vector_storage_impl_read_only::<T>(
            path,
            dim,
            distance,
            multi_vector_config,
            madvise,
            populate,
        )?;
        Ok(Self(storage))
    }
}

impl<T: PrimitiveVectorElement> MultiVectorStorage<T> for ReadOnlyMultiDenseVectorStorage<T> {
    fn vector_dim(&self) -> usize {
        self.0.vector_dim()
    }

    fn get_multi<P: AccessPattern>(&self, key: PointOffsetType) -> CowMultiVector<'_, T> {
        self.0.get_multi::<P>(key)
    }

    fn get_multi_opt<P: AccessPattern>(
        &self,
        key: PointOffsetType,
    ) -> Option<CowMultiVector<'_, T>> {
        self.0.get_multi_opt::<P>(key)
    }

    fn for_each_in_batch_multi<F>(&self, keys: &[PointOffsetType], callback: F)
    where
        F: FnMut(usize, TypedMultiDenseVectorRef<'_, T>),
    {
        self.0.for_each_in_batch_multi(keys, callback);
    }

    fn iterate_inner_vectors(&self) -> impl Iterator<Item = Cow<'_, [T]>> + Clone + Send {
        self.0.iterate_inner_vectors()
    }

    fn multi_vector_config(&self) -> &MultiVectorConfig {
        self.0.multi_vector_config()
    }

    fn size_of_available_vectors_in_bytes(&self) -> usize {
        self.0.size_of_available_vectors_in_bytes()
    }
}

impl<T: PrimitiveVectorElement> VectorStorageRead for ReadOnlyMultiDenseVectorStorage<T> {
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

/// Open an appendable mmap multi-dense vector storage as read-only.
pub fn open_read_only_multi_dense_vector_storage(
    storage_element_type: VectorStorageDatatype,
    path: &Path,
    dim: usize,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
    madvise: AdviceSetting,
    populate: bool,
) -> OperationResult<VectorStorageReadEnum> {
    match storage_element_type {
        VectorStorageDatatype::Float32 => {
            let storage = ReadOnlyMultiDenseVectorStorage::<VectorElementType>::open(
                path,
                dim,
                distance,
                multi_vector_config,
                madvise,
                populate,
            )?;
            Ok(VectorStorageReadEnum::MultiDense(Box::new(storage)))
        }
        VectorStorageDatatype::Uint8 => {
            let storage = ReadOnlyMultiDenseVectorStorage::<VectorElementTypeByte>::open(
                path,
                dim,
                distance,
                multi_vector_config,
                madvise,
                populate,
            )?;
            Ok(VectorStorageReadEnum::MultiDenseByte(Box::new(storage)))
        }
        VectorStorageDatatype::Float16 => {
            let storage = ReadOnlyMultiDenseVectorStorage::<VectorElementTypeHalf>::open(
                path,
                dim,
                distance,
                multi_vector_config,
                madvise,
                populate,
            )?;
            Ok(VectorStorageReadEnum::MultiDenseHalf(Box::new(storage)))
        }
    }
}
