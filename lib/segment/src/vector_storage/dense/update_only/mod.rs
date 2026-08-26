#[cfg(test)]
mod tests;

use std::borrow::Cow;
use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalAppend;

use super::appendable_dense_vector_storage::{DELETED_DIR_PATH, VECTORS_DIR_PATH};
use crate::common::flags::update_only_stored_flags::UpdateOnlyStoredFlags;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::VectorElementType;
use crate::vector_storage::VectorOffsetType;
use crate::vector_storage::chunked_vectors::update_only::UpdateOnlyChunkedVectors;
use crate::vector_storage::update_only::VectorToStore;

/// Writes what [`AppendableMmapDenseVectorStorage`] persists: the vectors, and
/// the flags marking which slots hold no vector of their own.
///
/// [`AppendableMmapDenseVectorStorage`]: super::appendable_dense_vector_storage::AppendableMmapDenseVectorStorage
pub struct UpdateOnlyDenseVectorStorage<T: PrimitiveVectorElement, S: UniversalAppend + 'static> {
    vectors: UpdateOnlyChunkedVectors<T, S>,
    deleted: UpdateOnlyStoredFlags<S>,
    dim: usize,
}

impl<T: PrimitiveVectorElement, S: UniversalAppend + 'static> UpdateOnlyDenseVectorStorage<T, S> {
    /// Open the storage at `path` for appending, creating it if it is not there
    /// yet.
    pub fn open(fs: S::Fs, path: &Path, dim: usize) -> OperationResult<Self> {
        Ok(Self {
            vectors: UpdateOnlyChunkedVectors::open(fs.clone(), &path.join(VECTORS_DIR_PATH), dim)?,
            deleted: UpdateOnlyStoredFlags::open(fs, &path.join(DELETED_DIR_PATH))?,
            dim,
        })
    }

    /// Append one vector per point of a batch, starting at `start_slot`, and
    /// persist them.
    ///
    /// Slots are consecutive from `start_slot`: every point of the batch takes
    /// one, whether or not it has a vector here. `start_slot` must be the slot
    /// this storage ends at, since the vectors are stored positionally.
    pub fn append_many<'a>(
        &mut self,
        start_slot: PointOffsetType,
        vectors: impl IntoIterator<Item = VectorToStore<'a>>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        // Decoded whole rather than streamed: a chunk append takes the run as
        // one slice-of-slices, and the borrow of a decoded vector cannot
        // outlive the conversion that produced it.
        let mut run: Vec<Vec<T>> = Vec::new();
        let mut missing = Vec::new();

        for (offset, vector) in vectors.into_iter().enumerate() {
            let elements = match vector {
                VectorToStore::Decoded(vector) => {
                    let vector: &[VectorElementType] = vector.try_into()?;
                    T::slice_from_float_cow(Cow::Borrowed(vector)).into_owned()
                }
                VectorToStore::Raw(bytes) => self.decode_raw(bytes)?,
                VectorToStore::Missing => {
                    // A placeholder keeps the slot numbering aligned with the id
                    // tracker; the flag is what says there is no vector here.
                    missing.push(start_slot + offset as PointOffsetType);
                    vec![T::default(); self.dim]
                }
            };

            if elements.len() != self.dim {
                return Err(OperationError::WrongVectorDimension {
                    expected_dim: self.dim,
                    received_dim: elements.len(),
                });
            }
            run.push(elements);
        }

        self.vectors.append_many(
            start_slot as VectorOffsetType,
            run.iter().map(Vec::as_slice),
            hw_counter,
        )?;

        // Only the missing ones are flagged. A slot this writer has not flagged
        // reads as not deleted, and the mask is explicitly allowed to be shorter
        // than the vector count — so a batch where every point has a vector
        // rewrites no mask at all.
        for slot in missing {
            self.deleted.set(slot, true);
        }

        self.deleted.flush(hw_counter)
    }

    /// Storage-native bytes are a packed `[T]` of exactly one vector, the form
    /// [`with_dense_bytes_opt`][1] hands out.
    ///
    /// [1]: crate::vector_storage::DenseVectorStorageRead::with_dense_bytes_opt
    fn decode_raw(&self, bytes: &[u8]) -> OperationResult<Vec<T>> {
        let expected_size = self.dim * size_of::<T>();
        if bytes.len() != expected_size {
            // `MalformedVectorBlob`, not a service error: a blob that reached
            // the WAL is skipped on replay rather than crash-looping recovery.
            return Err(OperationError::malformed_vector_blob(format!(
                "Malformed dense vector blob of {} bytes, expected {expected_size}",
                bytes.len(),
            )));
        }

        Ok(bytemuck::allocation::pod_collect_to_vec(bytes))
    }
}
