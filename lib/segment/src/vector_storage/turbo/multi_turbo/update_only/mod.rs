use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalAppend;
use quantization::turboquant::quantization::TurboQuantizer;

use super::super::shared::{self, DELETED_DIR_PATH, VECTORS_DIR_PATH};
use super::appendable_mmap_multi_turbo_vector_storage::OFFSETS_DIR_PATH;
use crate::common::flags::update_only_stored_flags::UpdateOnlyStoredFlags;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::vectors::{TypedMultiDenseVectorRef, VectorElementType, VectorRef};
use crate::types::Distance;
use crate::vector_storage::VectorOffsetType;
use crate::vector_storage::chunked_vectors::update_only::UpdateOnlyChunkedVectors;
use crate::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::MultivectorMmapOffset;
use crate::vector_storage::update_only::VectorToStore;

/// Writes what [`AppendableMmapMultiTurboVectorStorage`] persists: the encoded
/// inner vectors flat, the per-point row ranges into them, and the deleted
/// flags.
///
/// The multivector counterpart of [`UpdateOnlyTurboVectorStorage`][1]: it places
/// runs of rows the way [`UpdateOnlyMultiDenseVectorStorage`][2] does, and
/// encodes them the way the turbo writer does.
///
/// [`AppendableMmapMultiTurboVectorStorage`]: super::appendable_mmap_multi_turbo_vector_storage::AppendableMmapMultiTurboVectorStorage
/// [1]: crate::vector_storage::turbo::update_only::UpdateOnlyTurboVectorStorage
/// [2]: crate::vector_storage::multi_dense::update_only::UpdateOnlyMultiDenseVectorStorage
pub struct UpdateOnlyMultiTurboVectorStorage<S: UniversalAppend + 'static> {
    vectors: UpdateOnlyChunkedVectors<u8, S>,
    offsets: UpdateOnlyChunkedVectors<MultivectorMmapOffset, S>,
    deleted: UpdateOnlyStoredFlags<S>,
    quantizer: TurboQuantizer,
    quantization_buffer: Vec<f64>,
    dim: usize,
    /// One past the last row in use, carried across the batch.
    next_row: usize,
}

impl<S: UniversalAppend + 'static> UpdateOnlyMultiTurboVectorStorage<S> {
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
        let offsets = UpdateOnlyChunkedVectors::open(fs.clone(), &path.join(OFFSETS_DIR_PATH), 1)?;
        let deleted = UpdateOnlyStoredFlags::open(fs, &path.join(DELETED_DIR_PATH))?;
        let next_row = vectors.stored_len()?;

        Ok(Self {
            vectors,
            offsets,
            deleted,
            quantizer,
            quantization_buffer,
            dim,
            next_row,
        })
    }

    /// Append one encoded multi-vector per point of a batch, starting at
    /// `start_slot`, and persist them.
    pub fn append_many<'a>(
        &mut self,
        start_slot: PointOffsetType,
        vectors: impl IntoIterator<Item = VectorToStore<'a>>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let encoded_size = self.quantizer.quantized_size();
        // As in the plain multivector storage, a run that would straddle a chunk
        // skips to the next one, so a batch's rows are not always one span.
        let mut spans: Vec<(usize, Vec<u8>)> = Vec::new();
        let mut offsets = Vec::new();
        let mut missing = Vec::new();

        for (offset, vector) in vectors.into_iter().enumerate() {
            let encoded = match vector {
                VectorToStore::Decoded(vector) => self.encode_decoded(vector)?,
                VectorToStore::Raw(bytes) => self.encode_raw(bytes)?,
                VectorToStore::Missing => {
                    missing.push(start_slot + offset as PointOffsetType);
                    Vec::new()
                }
            };

            let count = encoded.len() / encoded_size;
            let row = self.place(count)?;
            offsets.push(MultivectorMmapOffset {
                offset: row as PointOffsetType,
                count: count as PointOffsetType,
                capacity: count as PointOffsetType,
            });

            match spans.last_mut() {
                Some((start, rows)) if *start + rows.len() / encoded_size == row => {
                    rows.extend_from_slice(&encoded);
                }
                _ if encoded.is_empty() => {}
                _ => spans.push((row, encoded)),
            }
            self.next_row = row + count;
        }

        for (start, rows) in &spans {
            self.vectors.append_many(
                *start as VectorOffsetType,
                rows.chunks_exact(encoded_size),
                hw_counter,
            )?;
        }

        self.offsets.append_many(
            start_slot as VectorOffsetType,
            offsets.iter().map(std::slice::from_ref),
            hw_counter,
        )?;

        for slot in missing {
            self.deleted.set(slot, true);
        }

        self.deleted.flush(hw_counter)
    }

    /// Where a run of `count` rows goes: at the end, or at the start of the next
    /// chunk when it would otherwise straddle a chunk boundary.
    fn place(&self, count: usize) -> OperationResult<usize> {
        let remaining = self.vectors.remaining_chunk_keys(self.next_row);
        if count > remaining {
            let max = self.vectors.remaining_chunk_keys(0);
            if count > max {
                return Err(OperationError::service_error(format!(
                    "Cannot insert a multi vector of {count} inner vectors, a chunk holds {max}",
                )));
            }
            return Ok(self.next_row + remaining);
        }
        Ok(self.next_row)
    }

    /// Encode every inner vector, back to back.
    fn encode_decoded(&mut self, vector: VectorRef<'_>) -> OperationResult<Vec<u8>> {
        let multi = TypedMultiDenseVectorRef::<VectorElementType>::try_from(vector)?;
        if multi.dim != self.dim {
            return Err(OperationError::WrongVectorDimension {
                expected_dim: self.dim,
                received_dim: multi.dim,
            });
        }

        let mut encoded =
            Vec::with_capacity(multi.vectors_count() * self.quantizer.quantized_size());
        for inner in multi.flattened_vectors.chunks_exact(multi.dim) {
            encoded.extend_from_slice(
                &self
                    .quantizer
                    .quantize(inner, &mut self.quantization_buffer),
            );
        }
        Ok(encoded)
    }

    /// Storage-native bytes are the encoded inner vectors, already packed.
    fn encode_raw(&self, bytes: &[u8]) -> OperationResult<Vec<u8>> {
        let encoded_size = self.quantizer.quantized_size();
        if bytes.len() % encoded_size != 0 {
            return Err(OperationError::malformed_vector_blob(format!(
                "Malformed multi TQ blob of {} bytes, not a whole number of {encoded_size}-byte \
                 encoded inner vectors",
                bytes.len(),
            )));
        }
        Ok(bytes.to_vec())
    }
}
