use std::sync::atomic::AtomicBool;

use super::vector_storage_base::VectorStorageEnum;
use super::{VectorStorage as _, VectorStorageRead as _};
use crate::common::operation_error::OperationResult;
use crate::data_types::named_vectors::CowVector;

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

        let entries_to_add = num_points - self.total_vector_count();

        match self {
            // Empty storages can adjust their size directly.
            VectorStorageEnum::EmptyDense(v) => {
                v.set_num_points(num_points);
                return Ok(());
            }
            VectorStorageEnum::EmptySparse(v) => {
                v.set_num_points(num_points);
                return Ok(());
            }

            // All other storages need to be extended via update_from.
            // We just need to know which default vector to use.
            VectorStorageEnum::DenseVolatile(_)
            | VectorStorageEnum::DenseMemmap(_)
            | VectorStorageEnum::DenseMemmapByte(_)
            | VectorStorageEnum::DenseMemmapHalf(_)
            | VectorStorageEnum::DenseAppendableMemmap(_)
            | VectorStorageEnum::DenseAppendableMemmapByte(_)
            | VectorStorageEnum::DenseAppendableMemmapHalf(_)
            | VectorStorageEnum::MultiDenseVolatile(_)
            | VectorStorageEnum::MultiDenseAppendableMemmap(_)
            | VectorStorageEnum::MultiDenseAppendableMemmapByte(_)
            | VectorStorageEnum::MultiDenseAppendableMemmapHalf(_) => {}

            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(_)
            | VectorStorageEnum::DenseVolatileHalf(_)
            | VectorStorageEnum::MultiDenseVolatileByte(_)
            | VectorStorageEnum::MultiDenseVolatileHalf(_) => {}

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(_)
            | VectorStorageEnum::DenseUringByte(_)
            | VectorStorageEnum::DenseUringHalf(_) => {}

            VectorStorageEnum::SparseVolatile(_) | VectorStorageEnum::SparseMmap(_) => {
                let stopped = AtomicBool::new(false);
                let mut iter =
                    std::iter::repeat_n((CowVector::default_sparse(), true), entries_to_add);
                self.update_from(&mut iter, &stopped)?;
                return Ok(());
            }
        }

        // Dense / multi-dense: fill with zero vectors of the appropriate dimension.
        let default_vector = CowVector::from(self.default_vector());
        let stopped = AtomicBool::new(false);
        let mut iter = std::iter::repeat_n((default_vector, true), entries_to_add);
        self.update_from(&mut iter, &stopped)?;

        Ok(())
    }
}
