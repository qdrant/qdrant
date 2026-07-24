use common::bitvec::BitSlice;
use common::generic_consts::AccessPattern;
use common::types::PointOffsetType;
use common::universal_io::{UniversalRead, UserData};

use super::VectorStorageReadEnum;
use crate::common::operation_error::OperationResult;
use crate::data_types::named_vectors::CowVector;
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::{
    DenseTQVectorStorageRead, DenseVectorStorageRead, MultiTQVectorStorageRead,
    MultiVectorStorageRead, SparseVectorStorageRead, VectorStorageRead,
};

impl<S: UniversalRead> VectorStorageRead for VectorStorageReadEnum<S> {
    fn size_of_available_vectors_in_bytes(&self) -> usize {
        match self {
            VectorStorageReadEnum::Dense(s) => s.size_of_available_vectors_in_bytes(),
            VectorStorageReadEnum::DenseByte(s) => s.size_of_available_vectors_in_bytes(),
            VectorStorageReadEnum::DenseHalf(s) => s.size_of_available_vectors_in_bytes(),
            VectorStorageReadEnum::DenseChunked(s) => s.size_of_available_vectors_in_bytes(),
            VectorStorageReadEnum::DenseChunkedByte(s) => s.size_of_available_vectors_in_bytes(),
            VectorStorageReadEnum::DenseChunkedHalf(s) => s.size_of_available_vectors_in_bytes(),
            VectorStorageReadEnum::MultiDenseChunked(s) => s.size_of_available_vectors_in_bytes(),
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => {
                s.size_of_available_vectors_in_bytes()
            }
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => {
                s.size_of_available_vectors_in_bytes()
            }
            VectorStorageReadEnum::DenseTurbo(s) => s.size_of_available_vectors_in_bytes(),
            VectorStorageReadEnum::DenseTurboChunked(s) => s.size_of_available_vectors_in_bytes(),
            VectorStorageReadEnum::MultiDenseTurbo(s) => s.size_of_available_vectors_in_bytes(),
            VectorStorageReadEnum::Sparse(s) => s.size_of_available_vectors_in_bytes(),
        }
    }

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
            VectorStorageReadEnum::DenseTurbo(s) => s.distance(),
            VectorStorageReadEnum::DenseTurboChunked(s) => s.distance(),
            VectorStorageReadEnum::MultiDenseTurbo(s) => s.distance(),
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
            VectorStorageReadEnum::DenseTurbo(s) => s.datatype(),
            VectorStorageReadEnum::DenseTurboChunked(s) => s.datatype(),
            VectorStorageReadEnum::MultiDenseTurbo(s) => s.datatype(),
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
            VectorStorageReadEnum::DenseTurbo(s) => s.is_on_disk(),
            VectorStorageReadEnum::DenseTurboChunked(s) => s.is_on_disk(),
            VectorStorageReadEnum::MultiDenseTurbo(s) => s.is_on_disk(),
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
            VectorStorageReadEnum::DenseTurbo(s) => s.total_vector_count(),
            VectorStorageReadEnum::DenseTurboChunked(s) => s.total_vector_count(),
            VectorStorageReadEnum::MultiDenseTurbo(s) => s.total_vector_count(),
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
            VectorStorageReadEnum::DenseTurbo(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::DenseTurboChunked(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::MultiDenseTurbo(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::Sparse(s) => s.get_vector::<P>(key),
        }
    }

    fn read_vectors<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        callback: impl FnMut(U, PointOffsetType, CowVector<'_>),
    ) {
        match self {
            VectorStorageReadEnum::Dense(s) => s.read_vectors::<P, U>(keys, callback),
            VectorStorageReadEnum::DenseByte(s) => s.read_vectors::<P, U>(keys, callback),
            VectorStorageReadEnum::DenseHalf(s) => s.read_vectors::<P, U>(keys, callback),
            VectorStorageReadEnum::DenseChunked(s) => s.read_vectors::<P, U>(keys, callback),
            VectorStorageReadEnum::DenseChunkedByte(s) => s.read_vectors::<P, U>(keys, callback),
            VectorStorageReadEnum::DenseChunkedHalf(s) => s.read_vectors::<P, U>(keys, callback),
            VectorStorageReadEnum::MultiDenseChunked(s) => s.read_vectors::<P, U>(keys, callback),
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => {
                s.read_vectors::<P, U>(keys, callback)
            }
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => {
                s.read_vectors::<P, U>(keys, callback)
            }
            VectorStorageReadEnum::DenseTurbo(s) => s.read_vectors::<P, U>(keys, callback),
            VectorStorageReadEnum::DenseTurboChunked(s) => s.read_vectors::<P, U>(keys, callback),
            VectorStorageReadEnum::MultiDenseTurbo(s) => s.read_vectors::<P, U>(keys, callback),
            VectorStorageReadEnum::Sparse(s) => s.read_vectors::<P, U>(keys, callback),
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
            VectorStorageReadEnum::DenseTurbo(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::DenseTurboChunked(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::MultiDenseTurbo(s) => s.get_vector_opt::<P>(key),
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
            VectorStorageReadEnum::DenseTurbo(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::DenseTurboChunked(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::MultiDenseTurbo(s) => s.is_deleted_vector(key),
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
            VectorStorageReadEnum::DenseTurbo(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::DenseTurboChunked(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::MultiDenseTurbo(s) => s.deleted_vector_count(),
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
            VectorStorageReadEnum::DenseTurbo(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::DenseTurboChunked(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::MultiDenseTurbo(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::Sparse(s) => s.deleted_vector_bitslice(),
        }
    }

    fn with_vector_bytes_opt<P: AccessPattern, R>(
        &self,
        key: PointOffsetType,
        f: impl FnOnce(&[u8]) -> R,
    ) -> OperationResult<Option<R>> {
        match self {
            VectorStorageReadEnum::Dense(s) => Ok(s.with_dense_bytes_opt::<P, R>(key, f)),
            VectorStorageReadEnum::DenseByte(s) => Ok(s.with_dense_bytes_opt::<P, R>(key, f)),
            VectorStorageReadEnum::DenseHalf(s) => Ok(s.with_dense_bytes_opt::<P, R>(key, f)),
            VectorStorageReadEnum::DenseChunked(s) => Ok(s.with_dense_bytes_opt::<P, R>(key, f)),
            VectorStorageReadEnum::DenseChunkedByte(s) => {
                Ok(s.with_dense_bytes_opt::<P, R>(key, f))
            }
            VectorStorageReadEnum::DenseChunkedHalf(s) => {
                Ok(s.with_dense_bytes_opt::<P, R>(key, f))
            }
            VectorStorageReadEnum::MultiDenseChunked(s) => {
                Ok(s.with_multi_bytes_opt::<P, R>(key, f))
            }
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => {
                Ok(s.with_multi_bytes_opt::<P, R>(key, f))
            }
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => {
                Ok(s.with_multi_bytes_opt::<P, R>(key, f))
            }
            VectorStorageReadEnum::DenseTurbo(s) => Ok(s.with_dense_tq_bytes_opt::<P, R>(key, f)),
            VectorStorageReadEnum::DenseTurboChunked(s) => {
                Ok(s.with_dense_tq_bytes_opt::<P, R>(key, f))
            }
            VectorStorageReadEnum::MultiDenseTurbo(s) => {
                Ok(s.with_multi_tq_bytes_opt::<P, R>(key, f))
            }
            VectorStorageReadEnum::Sparse(s) => s.with_sparse_bytes_opt::<P, R>(key, f),
        }
    }

    fn vector_bytes_opt<P: AccessPattern>(
        &self,
        key: PointOffsetType,
    ) -> OperationResult<Option<Vec<u8>>> {
        match self {
            // Sparse already allocates on serialize — return it without a copy.
            VectorStorageReadEnum::Sparse(s) => s.sparse_bytes_opt::<P>(key),
            // Others expose borrowed bytes; one copy out is unavoidable.
            VectorStorageReadEnum::Dense(_)
            | VectorStorageReadEnum::DenseByte(_)
            | VectorStorageReadEnum::DenseHalf(_)
            | VectorStorageReadEnum::DenseChunked(_)
            | VectorStorageReadEnum::DenseChunkedByte(_)
            | VectorStorageReadEnum::DenseChunkedHalf(_)
            | VectorStorageReadEnum::MultiDenseChunked(_)
            | VectorStorageReadEnum::MultiDenseChunkedByte(_)
            | VectorStorageReadEnum::MultiDenseChunkedHalf(_)
            | VectorStorageReadEnum::DenseTurbo(_)
            | VectorStorageReadEnum::DenseTurboChunked(_)
            | VectorStorageReadEnum::MultiDenseTurbo(_) => {
                self.with_vector_bytes_opt::<P, _>(key, <[u8]>::to_vec)
            }
        }
    }

    fn available_vector_count(&self) -> usize {
        match self {
            VectorStorageReadEnum::Dense(s) => s.available_vector_count(),
            VectorStorageReadEnum::DenseByte(s) => s.available_vector_count(),
            VectorStorageReadEnum::DenseHalf(s) => s.available_vector_count(),
            VectorStorageReadEnum::DenseChunked(s) => s.available_vector_count(),
            VectorStorageReadEnum::DenseChunkedByte(s) => s.available_vector_count(),
            VectorStorageReadEnum::DenseChunkedHalf(s) => s.available_vector_count(),
            VectorStorageReadEnum::MultiDenseChunked(s) => s.available_vector_count(),
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => s.available_vector_count(),
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => s.available_vector_count(),
            VectorStorageReadEnum::DenseTurbo(s) => s.available_vector_count(),
            VectorStorageReadEnum::DenseTurboChunked(s) => s.available_vector_count(),
            VectorStorageReadEnum::MultiDenseTurbo(s) => s.available_vector_count(),
            VectorStorageReadEnum::Sparse(s) => s.available_vector_count(),
        }
    }

    fn read_vector_bytes<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        callback: impl FnMut(U, PointOffsetType, Vec<u8>),
    ) -> OperationResult<()> {
        match self {
            VectorStorageReadEnum::Dense(s) => s.read_vector_bytes::<P, U>(keys, callback),
            VectorStorageReadEnum::DenseByte(s) => s.read_vector_bytes::<P, U>(keys, callback),
            VectorStorageReadEnum::DenseHalf(s) => s.read_vector_bytes::<P, U>(keys, callback),
            VectorStorageReadEnum::DenseChunked(s) => s.read_vector_bytes::<P, U>(keys, callback),
            VectorStorageReadEnum::DenseChunkedByte(s) => {
                s.read_vector_bytes::<P, U>(keys, callback)
            }
            VectorStorageReadEnum::DenseChunkedHalf(s) => {
                s.read_vector_bytes::<P, U>(keys, callback)
            }
            VectorStorageReadEnum::MultiDenseChunked(s) => {
                s.read_vector_bytes::<P, U>(keys, callback)
            }
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => {
                s.read_vector_bytes::<P, U>(keys, callback)
            }
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => {
                s.read_vector_bytes::<P, U>(keys, callback)
            }
            VectorStorageReadEnum::DenseTurbo(s) => s.read_vector_bytes::<P, U>(keys, callback),
            VectorStorageReadEnum::DenseTurboChunked(s) => {
                s.read_vector_bytes::<P, U>(keys, callback)
            }
            VectorStorageReadEnum::MultiDenseTurbo(s) => {
                s.read_vector_bytes::<P, U>(keys, callback)
            }
            VectorStorageReadEnum::Sparse(s) => s.read_vector_bytes::<P, U>(keys, callback),
        }
    }
}
