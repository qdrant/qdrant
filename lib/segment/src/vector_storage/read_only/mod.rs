use common::bitvec::BitSlice;
use common::generic_consts::AccessPattern;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::{VectorElementType, VectorElementTypeByte, VectorElementTypeHalf};
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::VectorStorageRead;
use crate::vector_storage::dense::dense_vector_storage::DenseVectorStorageImpl;
use crate::vector_storage::dense::read_only::chucked_vector_storage::ReadOnlyChunkedDenseVectorStorage;
use crate::vector_storage::multi_dense::read_only::chunked_vector_storage::ReadOnlyChunkedMultiDenseVectorStorage;
use crate::vector_storage::sparse::read_only::sparse_vector_storage::ReadOnlySparseVectorStorage;

/// Read-only counterpart of [`super::super::VectorStorageEnum`].
///
/// Wraps each on-disk storage type with its [`super`] read-only variant.
/// Volatile, empty and test-only variants are intentionally absent.
pub enum VectorStorageReadEnum<S: UniversalRead> {
    Dense(Box<DenseVectorStorageImpl<VectorElementType, S>>),
    DenseByte(Box<DenseVectorStorageImpl<VectorElementTypeByte, S>>),
    DenseHalf(Box<DenseVectorStorageImpl<VectorElementTypeHalf, S>>),
    DenseChunked(Box<ReadOnlyChunkedDenseVectorStorage<VectorElementType, S>>),
    DenseChunkedByte(Box<ReadOnlyChunkedDenseVectorStorage<VectorElementTypeByte, S>>),
    DenseChunkedHalf(Box<ReadOnlyChunkedDenseVectorStorage<VectorElementTypeHalf, S>>),
    MultiDenseChunked(Box<ReadOnlyChunkedMultiDenseVectorStorage<VectorElementType, S>>),
    MultiDenseChunkedByte(Box<ReadOnlyChunkedMultiDenseVectorStorage<VectorElementTypeByte, S>>),
    MultiDenseChunkedHalf(Box<ReadOnlyChunkedMultiDenseVectorStorage<VectorElementTypeHalf, S>>),
    Sparse(Box<ReadOnlySparseVectorStorage>),
}

impl<S: UniversalRead> VectorStorageRead for VectorStorageReadEnum<S> {
    fn distance(&self) -> Distance {
        match self {
            VectorStorageReadEnum::Dense(s) => s.distance(),
            VectorStorageReadEnum::DenseByte(s) => s.distance(),
            VectorStorageReadEnum::DenseHalf(s) => s.distance(),
            VectorStorageReadEnum::DenseChunked(s) => s.distance(),
            VectorStorageReadEnum::DenseChunkedByte(s) => s.distance(),
            VectorStorageReadEnum::DenseChunkedHalf(s) => s.distance(),
            VectorStorageReadEnum::MultiDenseChunked(s) => s.distance(),
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => s.distance(),
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => s.distance(),
            VectorStorageReadEnum::Sparse(s) => s.distance(),
        }
    }

    fn datatype(&self) -> VectorStorageDatatype {
        match self {
            VectorStorageReadEnum::Dense(s) => s.datatype(),
            VectorStorageReadEnum::DenseByte(s) => s.datatype(),
            VectorStorageReadEnum::DenseHalf(s) => s.datatype(),
            VectorStorageReadEnum::DenseChunked(s) => s.datatype(),
            VectorStorageReadEnum::DenseChunkedByte(s) => s.datatype(),
            VectorStorageReadEnum::DenseChunkedHalf(s) => s.datatype(),
            VectorStorageReadEnum::MultiDenseChunked(s) => s.datatype(),
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => s.datatype(),
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => s.datatype(),
            VectorStorageReadEnum::Sparse(s) => s.datatype(),
        }
    }

    fn is_on_disk(&self) -> bool {
        match self {
            VectorStorageReadEnum::Dense(s) => s.is_on_disk(),
            VectorStorageReadEnum::DenseByte(s) => s.is_on_disk(),
            VectorStorageReadEnum::DenseHalf(s) => s.is_on_disk(),
            VectorStorageReadEnum::DenseChunked(s) => s.is_on_disk(),
            VectorStorageReadEnum::DenseChunkedByte(s) => s.is_on_disk(),
            VectorStorageReadEnum::DenseChunkedHalf(s) => s.is_on_disk(),
            VectorStorageReadEnum::MultiDenseChunked(s) => s.is_on_disk(),
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => s.is_on_disk(),
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => s.is_on_disk(),
            VectorStorageReadEnum::Sparse(s) => s.is_on_disk(),
        }
    }

    fn total_vector_count(&self) -> usize {
        match self {
            VectorStorageReadEnum::Dense(s) => s.total_vector_count(),
            VectorStorageReadEnum::DenseByte(s) => s.total_vector_count(),
            VectorStorageReadEnum::DenseHalf(s) => s.total_vector_count(),
            VectorStorageReadEnum::DenseChunked(s) => s.total_vector_count(),
            VectorStorageReadEnum::DenseChunkedByte(s) => s.total_vector_count(),
            VectorStorageReadEnum::DenseChunkedHalf(s) => s.total_vector_count(),
            VectorStorageReadEnum::MultiDenseChunked(s) => s.total_vector_count(),
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => s.total_vector_count(),
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => s.total_vector_count(),
            VectorStorageReadEnum::Sparse(s) => s.total_vector_count(),
        }
    }

    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> CowVector<'_> {
        match self {
            VectorStorageReadEnum::Dense(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::DenseByte(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::DenseHalf(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::DenseChunked(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::DenseChunkedByte(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::DenseChunkedHalf(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::MultiDenseChunked(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => s.get_vector::<P>(key),
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
            VectorStorageReadEnum::DenseChunked(s) => s.read_vectors::<P>(keys, callback),
            VectorStorageReadEnum::DenseChunkedByte(s) => s.read_vectors::<P>(keys, callback),
            VectorStorageReadEnum::DenseChunkedHalf(s) => s.read_vectors::<P>(keys, callback),
            VectorStorageReadEnum::MultiDenseChunked(s) => s.read_vectors::<P>(keys, callback),
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => s.read_vectors::<P>(keys, callback),
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => s.read_vectors::<P>(keys, callback),
            VectorStorageReadEnum::Sparse(s) => s.read_vectors::<P>(keys, callback),
        }
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        match self {
            VectorStorageReadEnum::Dense(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::DenseByte(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::DenseHalf(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::DenseChunked(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::DenseChunkedByte(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::DenseChunkedHalf(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::MultiDenseChunked(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::Sparse(s) => s.get_vector_opt::<P>(key),
        }
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        match self {
            VectorStorageReadEnum::Dense(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::DenseByte(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::DenseHalf(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::DenseChunked(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::DenseChunkedByte(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::DenseChunkedHalf(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::MultiDenseChunked(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::Sparse(s) => s.is_deleted_vector(key),
        }
    }

    fn deleted_vector_count(&self) -> usize {
        match self {
            VectorStorageReadEnum::Dense(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::DenseByte(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::DenseHalf(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::DenseChunked(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::DenseChunkedByte(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::DenseChunkedHalf(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::MultiDenseChunked(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::Sparse(s) => s.deleted_vector_count(),
        }
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        match self {
            VectorStorageReadEnum::Dense(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::DenseByte(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::DenseHalf(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::DenseChunked(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::DenseChunkedByte(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::DenseChunkedHalf(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::MultiDenseChunked(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::Sparse(s) => s.deleted_vector_bitslice(),
        }
    }
}
