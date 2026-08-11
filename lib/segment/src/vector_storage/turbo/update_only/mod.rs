#[cfg(test)]
mod tests;

use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalAppend;
use quantization::turboquant::quantization::TurboQuantizer;

use super::shared::{self, DELETED_DIR_PATH, VECTORS_DIR_PATH};
use crate::common::flags::update_only_stored_flags::UpdateOnlyStoredFlags;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::vectors::VectorElementType;
use crate::types::Distance;
use crate::vector_storage::VectorOffsetType;
use crate::vector_storage::chunked_vectors::update_only::UpdateOnlyChunkedVectors;
use crate::vector_storage::update_only::VectorToStore;

/// Writes what [`AppendableMmapTurboVectorStorage`] persists: the encoded
/// vectors, and the flags marking which slots hold none.
///
/// The quantizer is rebuilt from the dimension and distance rather than read
/// back, exactly as the writable side builds it — it carries no learned state,
/// so the two encode identically.
///
/// [`AppendableMmapTurboVectorStorage`]: super::appendable_turbo_vector_storage::AppendableMmapTurboVectorStorage
pub struct UpdateOnlyTurboVectorStorage<S: UniversalAppend + 'static> {
    vectors: UpdateOnlyChunkedVectors<u8, S>,
    deleted: UpdateOnlyStoredFlags<S>,
    quantizer: TurboQuantizer,
    /// Scratch for the padded, rotated vector `quantize` writes through.
    quantization_buffer: Vec<f64>,
    dim: usize,
}

impl<S: UniversalAppend + 'static> UpdateOnlyTurboVectorStorage<S> {
    /// Open the storage at `path` for appending, creating it if it is not there
    /// yet.
    pub fn open(fs: S::Fs, path: &Path, dim: usize, distance: Distance) -> OperationResult<Self> {
        let quantizer = shared::build_quantizer(dim, distance);
        let quantization_buffer = vec![0.0; quantizer.get_padded_dim()];
        let vectors = UpdateOnlyChunkedVectors::open(
            fs.clone(),
            &path.join(VECTORS_DIR_PATH),
            quantizer.quantized_size(),
        )?;
        let deleted = UpdateOnlyStoredFlags::open(fs, &path.join(DELETED_DIR_PATH))?;

        Ok(Self {
            vectors,
            deleted,
            quantizer,
            quantization_buffer,
            dim,
        })
    }

    /// Append one encoded vector per point of a batch, starting at `start_slot`,
    /// and persist them.
    ///
    /// As in the plain dense storage, a point with no vector here still takes
    /// its slot — holding an encoded zero vector — and is flagged deleted.
    pub fn append_many<'a>(
        &mut self,
        start_slot: PointOffsetType,
        vectors: impl IntoIterator<Item = VectorToStore<'a>>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let encoded_size = self.quantizer.quantized_size();
        let mut run: Vec<Vec<u8>> = Vec::new();
        let mut missing = Vec::new();

        for (offset, vector) in vectors.into_iter().enumerate() {
            let encoded = match vector {
                VectorToStore::Decoded(vector) => {
                    let dense: &[VectorElementType] = vector.try_into()?;
                    self.quantizer
                        .quantize(dense, &mut self.quantization_buffer)
                }
                VectorToStore::Raw(bytes) => {
                    if bytes.len() != encoded_size {
                        return Err(OperationError::malformed_vector_blob(format!(
                            "Malformed dense TQ blob of {} bytes, expected {encoded_size}",
                            bytes.len(),
                        )));
                    }
                    bytes.to_vec()
                }
                VectorToStore::Missing => {
                    missing.push(start_slot + offset as PointOffsetType);
                    let zeros = vec![0.0; self.dim];
                    self.quantizer
                        .quantize(&zeros, &mut self.quantization_buffer)
                }
            };
            run.push(encoded);
        }

        self.vectors.append_many(
            start_slot as VectorOffsetType,
            run.iter().map(Vec::as_slice),
            hw_counter,
        )?;

        for slot in missing {
            self.deleted.set(slot, true);
        }

        self.deleted.flush(hw_counter)
    }
}
