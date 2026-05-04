use std::borrow::Cow;
use std::path::Path;

use common::bitvec::BitSlice;
use common::generic_consts::AccessPattern;
use common::types::PointOffsetType;
#[cfg(target_os = "linux")]
use common::universal_io::IoUringFile;
use common::universal_io::{MmapFile, UniversalRead};

use crate::common::operation_error::OperationResult;
use crate::data_types::named_vectors::CowVector;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{VectorElementType, VectorElementTypeByte, VectorElementTypeHalf};
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::dense::dense_vector_storage::{
    DenseVectorStorageImpl, open_dense_vector_storage_impl_read_only,
};
use crate::vector_storage::read_only::VectorStorageReadEnum;
use crate::vector_storage::{DenseVectorStorage, VectorStorageRead};

/// Read-only newtype wrapper around [`DenseVectorStorageImpl`].
///
/// Exposes only [`VectorStorageRead`] and [`DenseVectorStorage`].
pub struct ReadOnlyDenseVectorStorage<T, S = MmapFile>(DenseVectorStorageImpl<T, S>)
where
    T: PrimitiveVectorElement,
    S: UniversalRead<T>;

impl<T, S> ReadOnlyDenseVectorStorage<T, S>
where
    T: PrimitiveVectorElement,
    S: UniversalRead<T>,
{
    pub fn open(
        path: &Path,
        dim: usize,
        distance: Distance,
        populate: bool,
    ) -> OperationResult<Self> {
        let storage =
            open_dense_vector_storage_impl_read_only::<T, S>(path, dim, distance, populate)?;
        Ok(Self(storage))
    }
}

impl<T, S> DenseVectorStorage<T> for ReadOnlyDenseVectorStorage<T, S>
where
    T: PrimitiveVectorElement,
    S: UniversalRead<T>,
{
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

impl<T, S> VectorStorageRead for ReadOnlyDenseVectorStorage<T, S>
where
    T: PrimitiveVectorElement,
    S: UniversalRead<T>,
{
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

/// Open an immutable mmap (or io_uring) dense vector storage as read-only.
///
/// On Linux with `with_uring = true`, attempts io_uring backend first and falls
/// back to mmap on failure (matching the writable counterpart).
pub fn open_read_only_dense_vector_storage(
    storage_element_type: VectorStorageDatatype,
    path: &Path,
    dim: usize,
    distance: Distance,
    populate: bool,
    with_uring: bool,
) -> OperationResult<VectorStorageReadEnum> {
    // prevent "unused variable" warning on non-Linux
    let _ = with_uring;

    #[cfg(target_os = "linux")]
    if with_uring {
        let result: OperationResult<VectorStorageReadEnum> = match storage_element_type {
            VectorStorageDatatype::Float32 => ReadOnlyDenseVectorStorage::<
                VectorElementType,
                IoUringFile,
            >::open(path, dim, distance, populate)
            .map(|s| VectorStorageReadEnum::DenseUring(Box::new(s))),
            VectorStorageDatatype::Uint8 => ReadOnlyDenseVectorStorage::<
                VectorElementTypeByte,
                IoUringFile,
            >::open(path, dim, distance, populate)
            .map(|s| VectorStorageReadEnum::DenseUringByte(Box::new(s))),
            VectorStorageDatatype::Float16 => ReadOnlyDenseVectorStorage::<
                VectorElementTypeHalf,
                IoUringFile,
            >::open(path, dim, distance, populate)
            .map(|s| VectorStorageReadEnum::DenseUringHalf(Box::new(s))),
        };
        match result {
            Ok(storage) => return Ok(storage),
            Err(err) => log::error!("failed to open io_uring based vector storage: {err}"),
        }
    }

    match storage_element_type {
        VectorStorageDatatype::Float32 => {
            let storage = ReadOnlyDenseVectorStorage::<VectorElementType, _>::open(
                path, dim, distance, populate,
            )?;
            Ok(VectorStorageReadEnum::Dense(Box::new(storage)))
        }
        VectorStorageDatatype::Uint8 => {
            let storage = ReadOnlyDenseVectorStorage::<VectorElementTypeByte, _>::open(
                path, dim, distance, populate,
            )?;
            Ok(VectorStorageReadEnum::DenseByte(Box::new(storage)))
        }
        VectorStorageDatatype::Float16 => {
            let storage = ReadOnlyDenseVectorStorage::<VectorElementTypeHalf, _>::open(
                path, dim, distance, populate,
            )?;
            Ok(VectorStorageReadEnum::DenseHalf(Box::new(storage)))
        }
    }
}
