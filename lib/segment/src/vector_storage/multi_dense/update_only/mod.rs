#[cfg(test)]
mod tests;

use std::borrow::Cow;
use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalAppend;

use super::appendable_mmap_multi_dense_vector_storage::{
    DELETED_DIR_PATH, MultivectorMmapOffset, OFFSETS_DIR_PATH, VECTORS_DIR_PATH,
};
use crate::common::flags::update_only_stored_flags::UpdateOnlyStoredFlags;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{TypedMultiDenseVectorRef, VectorElementType, VectorRef};
use crate::vector_storage::VectorOffsetType;
use crate::vector_storage::chunked_vectors::update_only::UpdateOnlyChunkedVectors;
use crate::vector_storage::update_only::VectorToStore;

/// Writes what [`AppendableMmapMultiDenseVectorStorage`] persists: the inner
/// vectors flat, the per-point row ranges into them, and the deleted flags.
///
/// Unlike the single-vector storages, rows are not indexed by point slot — a
/// point owns a run of them — so this writer tracks where the row space ends.
///
/// [`AppendableMmapMultiDenseVectorStorage`]: super::appendable_mmap_multi_dense_vector_storage::AppendableMmapMultiDenseVectorStorage
pub struct UpdateOnlyMultiDenseVectorStorage<
    T: PrimitiveVectorElement,
    S: UniversalAppend + 'static,
> {
    /// Flat inner-vector space: one row per inner vector.
    vectors: UpdateOnlyChunkedVectors<T, S>,
    /// Maps each point slot to its row range.
    offsets: UpdateOnlyChunkedVectors<MultivectorMmapOffset, S>,
    deleted: UpdateOnlyStoredFlags,
    dim: usize,
    /// One past the last row in use, carried across the batch.
    next_row: usize,
}

impl<T: PrimitiveVectorElement, S: UniversalAppend + 'static>
    UpdateOnlyMultiDenseVectorStorage<T, S>
{
    /// Open the storage at `path` for appending, creating it if it is not there
    /// yet.
    pub fn open(fs: S::Fs, path: &Path, dim: usize) -> OperationResult<Self> {
        let deleted = UpdateOnlyStoredFlags::open(&fs, &path.join(DELETED_DIR_PATH))?;
        let vectors =
            UpdateOnlyChunkedVectors::open(fs.clone(), &path.join(VECTORS_DIR_PATH), dim)?;
        // One offset entry per point, so the "vector" is a single element.
        let offsets = UpdateOnlyChunkedVectors::open(fs, &path.join(OFFSETS_DIR_PATH), 1)?;
        let next_row = vectors.stored_len()?;

        Ok(Self {
            vectors,
            offsets,
            deleted,
            dim,
            next_row,
        })
    }

    /// Append one multi-vector per point of a batch, starting at `start_slot`,
    /// and persist them.
    pub fn append_many<'a>(
        &mut self,
        fs: &S::Fs,
        start_slot: PointOffsetType,
        vectors: impl IntoIterator<Item = VectorToStore<'a>>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let batch_start = self.next_row;
        let mut rows: Vec<T> = Vec::new();
        let mut offsets = Vec::new();
        let mut missing = Vec::new();

        for (offset, vector) in vectors.into_iter().enumerate() {
            let flattened = match vector {
                VectorToStore::Decoded(vector) => self.flatten_decoded(vector)?,
                VectorToStore::Raw(bytes) => self.flatten_raw(bytes)?,
                VectorToStore::Missing => {
                    // A point with no multi-vector here owns no rows at all: the
                    // offset entry is what says so, and unlike the single-vector
                    // storages there is no slot in the row space to keep aligned.
                    missing.push(start_slot + offset as PointOffsetType);
                    Vec::new()
                }
            };

            let count = flattened.len() / self.dim;
            rows.extend_from_slice(&flattened);
            offsets.push(MultivectorMmapOffset {
                offset: self.next_row as PointOffsetType,
                count: count as PointOffsetType,
                capacity: count as PointOffsetType,
            });
            self.next_row += count;
        }

        self.vectors.append_many(
            batch_start as VectorOffsetType,
            rows.chunks_exact(self.dim),
            hw_counter,
        )?;

        self.offsets.append_many(
            start_slot as VectorOffsetType,
            offsets.iter().map(std::slice::from_ref),
            hw_counter,
        )?;

        for slot in missing {
            self.deleted.set(slot, true);
        }

        self.deleted.flush(fs, hw_counter)
    }

    fn flatten_decoded(&self, vector: VectorRef<'_>) -> OperationResult<Vec<T>> {
        let multi = TypedMultiDenseVectorRef::<VectorElementType>::try_from(vector)?;
        if multi.dim != self.dim {
            return Err(OperationError::WrongVectorDimension {
                expected_dim: self.dim,
                received_dim: multi.dim,
            });
        }
        Ok(T::slice_from_float_cow(Cow::Borrowed(multi.flattened_vectors)).into_owned())
    }

    /// Storage-native bytes are the flattened inner vectors packed as `[T]`.
    fn flatten_raw(&self, bytes: &[u8]) -> OperationResult<Vec<T>> {
        let row_size = self.dim * size_of::<T>();
        if !bytes.len().is_multiple_of(row_size) {
            return Err(OperationError::malformed_vector_blob(format!(
                "Malformed multi vector blob of {} bytes, not a whole number of {row_size}-byte \
                 inner vectors",
                bytes.len(),
            )));
        }
        Ok(bytemuck::allocation::pod_collect_to_vec(bytes))
    }
}
