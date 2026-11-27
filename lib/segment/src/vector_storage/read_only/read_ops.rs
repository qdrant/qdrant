use common::bitvec::BitSlice;
use common::generic_consts::AccessPattern;
use common::types::PointOffsetType;
use common::universal_io::{UniversalRead, UserData};

use super::VectorStorageReadEnum;
use crate::common::operation_error::OperationResult;
use crate::data_types::named_vectors::CowVector;
use crate::types::{Distance, IoBackend, VectorStorageDatatype};
use crate::vector_storage::{
    DenseTQVectorStorageRead, DenseVectorStorageRead, MultiTQVectorStorageRead,
    MultiVectorStorageRead, SparseVectorStorageRead, VectorStorageRead,
};

impl<S: UniversalRead> VectorStorageRead for VectorStorageReadEnum<S> {
    /// A read-only segment does not choose per component: every variant reads through `S`.
    fn io_backend(&self) -> Option<IoBackend> {
        IoBackend::from_universal_kind(S::kind())
    }

    fn size_of_available_vectors_in_bytes(&self) -> usize {
        for_all_vector_storage_read!(self, s => s.size_of_available_vectors_in_bytes())
    }

    fn distance(&self) -> Distance {
        for_all_vector_storage_read!(self, s => s.distance())
    }

    fn datatype(&self) -> VectorStorageDatatype {
        for_all_vector_storage_read!(self, s => s.datatype())
    }

    fn is_cold(&self) -> bool {
        for_all_vector_storage_read!(self, s => s.is_cold())
    }

    fn total_vector_count(&self) -> usize {
        for_all_vector_storage_read!(self, s => s.total_vector_count())
    }

    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> CowVector<'_> {
        for_all_vector_storage_read!(self, s => s.get_vector::<P>(key))
    }

    fn read_vectors<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        callback: impl FnMut(U, PointOffsetType, CowVector<'_>),
    ) -> OperationResult<()> {
        for_all_vector_storage_read!(self, s => s.read_vectors::<P, U>(keys, callback))
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        for_all_vector_storage_read!(self, s => s.get_vector_opt::<P>(key))
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        for_all_vector_storage_read!(self, s => s.is_deleted_vector(key))
    }

    fn deleted_vector_count(&self) -> usize {
        for_all_vector_storage_read!(self, s => s.deleted_vector_count())
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        for_all_vector_storage_read!(self, s => s.deleted_vector_bitslice())
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
            VectorStorageReadEnum::DenseGraphInline(s) => {
                Ok(s.with_dense_bytes_opt::<P, R>(key, f))
            }
            VectorStorageReadEnum::DenseGraphInlineByte(s) => {
                Ok(s.with_dense_bytes_opt::<P, R>(key, f))
            }
            VectorStorageReadEnum::DenseGraphInlineHalf(s) => {
                Ok(s.with_dense_bytes_opt::<P, R>(key, f))
            }
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
            VectorStorageReadEnum::DenseTurboGraphInline(s) => {
                Ok(s.with_dense_tq_bytes_opt::<P, R>(key, f))
            }
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
            | VectorStorageReadEnum::DenseGraphInline(_)
            | VectorStorageReadEnum::DenseGraphInlineByte(_)
            | VectorStorageReadEnum::DenseGraphInlineHalf(_)
            | VectorStorageReadEnum::DenseChunked(_)
            | VectorStorageReadEnum::DenseChunkedByte(_)
            | VectorStorageReadEnum::DenseChunkedHalf(_)
            | VectorStorageReadEnum::MultiDenseChunked(_)
            | VectorStorageReadEnum::MultiDenseChunkedByte(_)
            | VectorStorageReadEnum::MultiDenseChunkedHalf(_)
            | VectorStorageReadEnum::DenseTurbo(_)
            | VectorStorageReadEnum::DenseTurboGraphInline(_)
            | VectorStorageReadEnum::DenseTurboChunked(_)
            | VectorStorageReadEnum::MultiDenseTurbo(_) => {
                self.with_vector_bytes_opt::<P, _>(key, <[u8]>::to_vec)
            }
        }
    }

    fn available_vector_count(&self) -> usize {
        for_all_vector_storage_read!(self, s => s.available_vector_count())
    }

    fn read_vector_bytes<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        callback: impl FnMut(U, PointOffsetType, Vec<u8>),
    ) -> OperationResult<()> {
        for_all_vector_storage_read!(self, s => s.read_vector_bytes::<P, U>(keys, callback))
    }
}
