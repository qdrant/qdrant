use std::borrow::Cow;
use std::sync::atomic::AtomicBool;

use sparse::common::sparse_vector::SparseVector;

use super::vector_storage_base::VectorStorageEnum;
use super::{
    DenseTQVectorStorage, DenseVectorStorage, MultiTQVectorStorage, MultiVectorStorage,
    SparseVectorStorage, VectorStorageRead as _,
};
use crate::common::operation_error::OperationResult;
use crate::data_types::named_vectors::CowMultiVector;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::TypedMultiDenseVector;

// Append `count` deleted placeholders in the storage's own element type. The
// placeholder content is irrelevant — every entry is flagged deleted — so we
// just use zero vectors of the right dimension.

fn fill_dense<T: PrimitiveVectorElement>(
    storage: &mut impl DenseVectorStorage<T>,
    count: usize,
    stopped: &AtomicBool,
) -> OperationResult<()> {
    let dim = storage.vector_dim();
    let placeholder: Cow<[T]> = Cow::Owned(vec![T::default(); dim]);
    let mut iter = std::iter::repeat_n((placeholder, true), count);
    storage.update_from(&mut iter, stopped)?;
    Ok(())
}

pub(super) fn fill_turbo(
    storage: &mut impl DenseTQVectorStorage,
    count: usize,
    stopped: &AtomicBool,
) -> OperationResult<()> {
    let size = storage.quantized_vector_size();
    let placeholder: Cow<[u8]> = Cow::Owned(vec![0u8; size]);
    let mut iter = std::iter::repeat_n((placeholder, true), count);
    storage.update_from(&mut iter, stopped)?;
    Ok(())
}

pub(super) fn fill_turbo_multi(
    storage: &mut impl MultiTQVectorStorage,
    count: usize,
    stopped: &AtomicBool,
) -> OperationResult<()> {
    // One zero inner record per placeholder point; the deleted flag is carried
    // separately, so the content is irrelevant.
    let size = storage.quantized_vector_size();
    let placeholder: Cow<[u8]> = Cow::Owned(vec![0u8; size]);
    let mut iter = std::iter::repeat_n((placeholder, true), count);
    storage.update_from(&mut iter, stopped)?;
    Ok(())
}

fn fill_multi<T: PrimitiveVectorElement>(
    storage: &mut impl MultiVectorStorage<T>,
    count: usize,
    stopped: &AtomicBool,
) -> OperationResult<()> {
    let dim = storage.vector_dim();
    let placeholder = CowMultiVector::Owned(TypedMultiDenseVector::<T>::placeholder(dim));
    let mut iter = std::iter::repeat_n((placeholder, true), count);
    storage.update_from(&mut iter, stopped)?;
    Ok(())
}

fn fill_sparse(
    storage: &mut impl SparseVectorStorage,
    count: usize,
    stopped: &AtomicBool,
) -> OperationResult<()> {
    let placeholder = Cow::Owned(SparseVector::default());
    let mut iter = std::iter::repeat_n((placeholder, true), count);
    storage.update_from(&mut iter, stopped)?;
    Ok(())
}

impl VectorStorageEnum {
    /// Ensure the storage has at least `num_points` entries, with any newly
    /// created entries marked as deleted.
    ///
    /// This is needed when a new named vector is added to a segment that
    /// already contains points — the storage must be sized to match the
    /// id_tracker's point count.
    pub fn prefill_deleted_entries(&mut self, num_points: usize) -> OperationResult<()> {
        if num_points == 0 || self.total_vector_count() >= num_points {
            return Ok(());
        }

        let count = num_points - self.total_vector_count();
        let stopped = AtomicBool::new(false);

        match self {
            // Empty storages can adjust their size directly.
            VectorStorageEnum::EmptyDense(v) => {
                v.set_num_points(num_points);
                Ok(())
            }
            VectorStorageEnum::EmptySparse(v) => {
                v.set_num_points(num_points);
                Ok(())
            }

            VectorStorageEnum::DenseVolatile(v) => fill_dense(v, count, &stopped),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => fill_dense(v, count, &stopped),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => fill_dense(v, count, &stopped),
            VectorStorageEnum::DenseMemmap(v) => fill_dense(&mut **v, count, &stopped),
            VectorStorageEnum::DenseMemmapByte(v) => fill_dense(&mut **v, count, &stopped),
            VectorStorageEnum::DenseMemmapHalf(v) => fill_dense(&mut **v, count, &stopped),

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => fill_dense(&mut **v, count, &stopped),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => fill_dense(&mut **v, count, &stopped),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => fill_dense(&mut **v, count, &stopped),

            VectorStorageEnum::DenseAppendableMemmap(v) => fill_dense(&mut **v, count, &stopped),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => {
                fill_dense(&mut **v, count, &stopped)
            }
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => {
                fill_dense(&mut **v, count, &stopped)
            }

            VectorStorageEnum::DenseTurboMemmap(v) => fill_turbo(&mut **v, count, &stopped),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseTurboUring(v) => fill_turbo(&mut **v, count, &stopped),
            VectorStorageEnum::DenseTurboAppendableMemmap(v) => fill_turbo(&mut **v, count, &stopped),
            VectorStorageEnum::MultiDenseTurbo(v) => fill_turbo_multi(&mut **v, count, &stopped),

            VectorStorageEnum::MultiDenseVolatile(v) => fill_multi(v, count, &stopped),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(v) => fill_multi(v, count, &stopped),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(v) => fill_multi(v, count, &stopped),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => {
                fill_multi(&mut **v, count, &stopped)
            }
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => {
                fill_multi(&mut **v, count, &stopped)
            }
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => {
                fill_multi(&mut **v, count, &stopped)
            }

            VectorStorageEnum::SparseVolatile(v) => fill_sparse(v, count, &stopped),
            VectorStorageEnum::SparseMmap(v) => fill_sparse(v, count, &stopped),
        }
    }
}
