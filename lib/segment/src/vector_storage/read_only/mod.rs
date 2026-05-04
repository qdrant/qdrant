mod appendable_dense_vector_storage_read;
mod dense_vector_storage_read;
mod multi_dense_vector_storage_read;
mod sparse_vector_storage_read;
pub use appendable_dense_vector_storage_read::{
    ReadOnlyAppendableDenseVectorStorage, open_read_only_appendable_dense_vector_storage,
};
use common::bitvec::BitSlice;
use common::generic_consts::AccessPattern;
use common::types::PointOffsetType;
#[cfg(target_os = "linux")]
use common::universal_io::IoUringFile;
pub use dense_vector_storage_read::{
    ReadOnlyDenseVectorStorage, open_read_only_dense_vector_storage,
};
pub use multi_dense_vector_storage_read::{
    ReadOnlyMultiDenseVectorStorage, open_read_only_multi_dense_vector_storage,
};
pub use sparse_vector_storage_read::{
    ReadOnlySparseVectorStorage, open_read_only_sparse_vector_storage,
};

use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::{VectorElementType, VectorElementTypeByte, VectorElementTypeHalf};
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::VectorStorageRead;

/// Read-only counterpart of [`super::super::VectorStorageEnum`].
///
/// Wraps each on-disk storage type with its [`super`] read-only variant.
/// Volatile, empty and test-only variants are intentionally absent.
pub enum VectorStorageReadEnum {
    Dense(Box<ReadOnlyDenseVectorStorage<VectorElementType>>),
    DenseByte(Box<ReadOnlyDenseVectorStorage<VectorElementTypeByte>>),
    DenseHalf(Box<ReadOnlyDenseVectorStorage<VectorElementTypeHalf>>),

    #[cfg(target_os = "linux")]
    DenseUring(Box<ReadOnlyDenseVectorStorage<VectorElementType, IoUringFile>>),
    #[cfg(target_os = "linux")]
    DenseUringByte(Box<ReadOnlyDenseVectorStorage<VectorElementTypeByte, IoUringFile>>),
    #[cfg(target_os = "linux")]
    DenseUringHalf(Box<ReadOnlyDenseVectorStorage<VectorElementTypeHalf, IoUringFile>>),

    DenseAppendable(Box<ReadOnlyAppendableDenseVectorStorage<VectorElementType>>),
    DenseAppendableByte(Box<ReadOnlyAppendableDenseVectorStorage<VectorElementTypeByte>>),
    DenseAppendableHalf(Box<ReadOnlyAppendableDenseVectorStorage<VectorElementTypeHalf>>),

    MultiDense(Box<ReadOnlyMultiDenseVectorStorage<VectorElementType>>),
    MultiDenseByte(Box<ReadOnlyMultiDenseVectorStorage<VectorElementTypeByte>>),
    MultiDenseHalf(Box<ReadOnlyMultiDenseVectorStorage<VectorElementTypeHalf>>),

    Sparse(Box<ReadOnlySparseVectorStorage>),
}

impl VectorStorageRead for VectorStorageReadEnum {
    fn distance(&self) -> Distance {
        match self {
            VectorStorageReadEnum::Dense(s) => s.distance(),
            VectorStorageReadEnum::DenseByte(s) => s.distance(),
            VectorStorageReadEnum::DenseHalf(s) => s.distance(),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUring(s) => s.distance(),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUringByte(s) => s.distance(),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUringHalf(s) => s.distance(),
            VectorStorageReadEnum::DenseAppendable(s) => s.distance(),
            VectorStorageReadEnum::DenseAppendableByte(s) => s.distance(),
            VectorStorageReadEnum::DenseAppendableHalf(s) => s.distance(),
            VectorStorageReadEnum::MultiDense(s) => s.distance(),
            VectorStorageReadEnum::MultiDenseByte(s) => s.distance(),
            VectorStorageReadEnum::MultiDenseHalf(s) => s.distance(),
            VectorStorageReadEnum::Sparse(s) => s.distance(),
        }
    }

    fn datatype(&self) -> VectorStorageDatatype {
        match self {
            VectorStorageReadEnum::Dense(s) => s.datatype(),
            VectorStorageReadEnum::DenseByte(s) => s.datatype(),
            VectorStorageReadEnum::DenseHalf(s) => s.datatype(),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUring(s) => s.datatype(),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUringByte(s) => s.datatype(),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUringHalf(s) => s.datatype(),
            VectorStorageReadEnum::DenseAppendable(s) => s.datatype(),
            VectorStorageReadEnum::DenseAppendableByte(s) => s.datatype(),
            VectorStorageReadEnum::DenseAppendableHalf(s) => s.datatype(),
            VectorStorageReadEnum::MultiDense(s) => s.datatype(),
            VectorStorageReadEnum::MultiDenseByte(s) => s.datatype(),
            VectorStorageReadEnum::MultiDenseHalf(s) => s.datatype(),
            VectorStorageReadEnum::Sparse(s) => s.datatype(),
        }
    }

    fn is_on_disk(&self) -> bool {
        match self {
            VectorStorageReadEnum::Dense(s) => s.is_on_disk(),
            VectorStorageReadEnum::DenseByte(s) => s.is_on_disk(),
            VectorStorageReadEnum::DenseHalf(s) => s.is_on_disk(),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUring(s) => s.is_on_disk(),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUringByte(s) => s.is_on_disk(),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUringHalf(s) => s.is_on_disk(),
            VectorStorageReadEnum::DenseAppendable(s) => s.is_on_disk(),
            VectorStorageReadEnum::DenseAppendableByte(s) => s.is_on_disk(),
            VectorStorageReadEnum::DenseAppendableHalf(s) => s.is_on_disk(),
            VectorStorageReadEnum::MultiDense(s) => s.is_on_disk(),
            VectorStorageReadEnum::MultiDenseByte(s) => s.is_on_disk(),
            VectorStorageReadEnum::MultiDenseHalf(s) => s.is_on_disk(),
            VectorStorageReadEnum::Sparse(s) => s.is_on_disk(),
        }
    }

    fn total_vector_count(&self) -> usize {
        match self {
            VectorStorageReadEnum::Dense(s) => s.total_vector_count(),
            VectorStorageReadEnum::DenseByte(s) => s.total_vector_count(),
            VectorStorageReadEnum::DenseHalf(s) => s.total_vector_count(),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUring(s) => s.total_vector_count(),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUringByte(s) => s.total_vector_count(),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUringHalf(s) => s.total_vector_count(),
            VectorStorageReadEnum::DenseAppendable(s) => s.total_vector_count(),
            VectorStorageReadEnum::DenseAppendableByte(s) => s.total_vector_count(),
            VectorStorageReadEnum::DenseAppendableHalf(s) => s.total_vector_count(),
            VectorStorageReadEnum::MultiDense(s) => s.total_vector_count(),
            VectorStorageReadEnum::MultiDenseByte(s) => s.total_vector_count(),
            VectorStorageReadEnum::MultiDenseHalf(s) => s.total_vector_count(),
            VectorStorageReadEnum::Sparse(s) => s.total_vector_count(),
        }
    }

    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> CowVector<'_> {
        match self {
            VectorStorageReadEnum::Dense(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::DenseByte(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::DenseHalf(s) => s.get_vector::<P>(key),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUring(s) => s.get_vector::<P>(key),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUringByte(s) => s.get_vector::<P>(key),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUringHalf(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::DenseAppendable(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::DenseAppendableByte(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::DenseAppendableHalf(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::MultiDense(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::MultiDenseByte(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::MultiDenseHalf(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::Sparse(s) => s.get_vector::<P>(key),
        }
    }

    fn read_vectors<P: AccessPattern>(
        &self,
        keys: impl IntoIterator<Item = PointOffsetType>,
        callback: impl FnMut(PointOffsetType, CowVector<'_>),
    ) {
        match self {
            VectorStorageReadEnum::Dense(s) => s.read_vectors::<P>(keys, callback),
            VectorStorageReadEnum::DenseByte(s) => s.read_vectors::<P>(keys, callback),
            VectorStorageReadEnum::DenseHalf(s) => s.read_vectors::<P>(keys, callback),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUring(s) => s.read_vectors::<P>(keys, callback),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUringByte(s) => s.read_vectors::<P>(keys, callback),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUringHalf(s) => s.read_vectors::<P>(keys, callback),
            VectorStorageReadEnum::DenseAppendable(s) => s.read_vectors::<P>(keys, callback),
            VectorStorageReadEnum::DenseAppendableByte(s) => s.read_vectors::<P>(keys, callback),
            VectorStorageReadEnum::DenseAppendableHalf(s) => s.read_vectors::<P>(keys, callback),
            VectorStorageReadEnum::MultiDense(s) => s.read_vectors::<P>(keys, callback),
            VectorStorageReadEnum::MultiDenseByte(s) => s.read_vectors::<P>(keys, callback),
            VectorStorageReadEnum::MultiDenseHalf(s) => s.read_vectors::<P>(keys, callback),
            VectorStorageReadEnum::Sparse(s) => s.read_vectors::<P>(keys, callback),
        }
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        match self {
            VectorStorageReadEnum::Dense(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::DenseByte(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::DenseHalf(s) => s.get_vector_opt::<P>(key),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUring(s) => s.get_vector_opt::<P>(key),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUringByte(s) => s.get_vector_opt::<P>(key),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUringHalf(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::DenseAppendable(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::DenseAppendableByte(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::DenseAppendableHalf(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::MultiDense(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::MultiDenseByte(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::MultiDenseHalf(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::Sparse(s) => s.get_vector_opt::<P>(key),
        }
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        match self {
            VectorStorageReadEnum::Dense(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::DenseByte(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::DenseHalf(s) => s.is_deleted_vector(key),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUring(s) => s.is_deleted_vector(key),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUringByte(s) => s.is_deleted_vector(key),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUringHalf(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::DenseAppendable(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::DenseAppendableByte(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::DenseAppendableHalf(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::MultiDense(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::MultiDenseByte(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::MultiDenseHalf(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::Sparse(s) => s.is_deleted_vector(key),
        }
    }

    fn deleted_vector_count(&self) -> usize {
        match self {
            VectorStorageReadEnum::Dense(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::DenseByte(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::DenseHalf(s) => s.deleted_vector_count(),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUring(s) => s.deleted_vector_count(),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUringByte(s) => s.deleted_vector_count(),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUringHalf(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::DenseAppendable(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::DenseAppendableByte(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::DenseAppendableHalf(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::MultiDense(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::MultiDenseByte(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::MultiDenseHalf(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::Sparse(s) => s.deleted_vector_count(),
        }
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        match self {
            VectorStorageReadEnum::Dense(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::DenseByte(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::DenseHalf(s) => s.deleted_vector_bitslice(),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUring(s) => s.deleted_vector_bitslice(),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUringByte(s) => s.deleted_vector_bitslice(),
            #[cfg(target_os = "linux")]
            VectorStorageReadEnum::DenseUringHalf(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::DenseAppendable(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::DenseAppendableByte(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::DenseAppendableHalf(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::MultiDense(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::MultiDenseByte(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::MultiDenseHalf(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::Sparse(s) => s.deleted_vector_bitslice(),
        }
    }
}
