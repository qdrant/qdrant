//! The write half of quantized vectors, for update-only appendable segments.
//!
//! Scoped to what an update-only segment needs: dense (single-vector) Binary and TurboQuant
//! quantization only — the two methods [`QuantizationConfig::supports_appendable`] allows, and
//! the two this stack currently wires up (multivector support is a follow-up: it needs its own
//! append-only offsets storage, mirroring [`MultivectorOffsetsStorageChunked`] the same way this
//! mirrors [`QuantizedChunkedStorage`]).
//!
//! [`Self::open`] only reopens an overlay that already exists on disk (unlike
//! [`QuantizedVectors::load`]'s `count == 0` auto-create, which this otherwise mirrors) — it
//! never guesses from file absence whether one should be created. Building a fresh overlay for a
//! genuinely new segment is out of scope here: nothing in this stack constructs the first
//! appendable segment of a collection yet, so there is no real caller for that path today.
//!
//! Reuses the `quantization` crate's encoding logic — `EncodedVectorsBin`/`EncodedVectorsTQ` —
//! almost entirely unchanged: creation goes through their existing `encode`, already generic
//! over the storage backend; reopening to resume appending goes through the new
//! `reopen_for_write` (added alongside `load`, which additionally validates a stored vector by
//! reading it back — a read this write-only storage cannot serve, and a guarantee a resuming
//! writer doesn't need: every vector it writes is sized from the same metadata `load` and
//! `reopen_for_write` both read). Both `reopen_for_write` and `upsert_vector`/`flusher` only
//! require [`EncodedStorageWrite`] — the write-only half of [`EncodedStorage`], split out so a
//! storage that can never serve a read doesn't have to fake one. The only new storage-layer code
//! is [`UpdateOnlyQuantizedChunkedStorage`], an [`EncodedStorageWrite`] backed by
//! [`UpdateOnlyChunkedVectors`] instead of positional [`ChunkedVectors`] writes, writing files
//! in the exact layout [`QuantizedChunkedStorage`] reads — so a promoted segment's quantized
//! data reads through the existing, unmodified reader.
//!
//! [`MultivectorOffsetsStorageChunked`]: crate::vector_storage::quantized::quantized_multivector_storage::MultivectorOffsetsStorageChunked
//! [`ChunkedVectors`]: crate::vector_storage::chunked_vectors::ChunkedVectors
//! [`UpdateOnlyChunkedVectors`]: crate::vector_storage::chunked_vectors::update_only::UpdateOnlyChunkedVectors
//! [`EncodedStorage`]: quantization::EncodedStorage
//! [`EncodedStorageWrite`]: quantization::EncodedStorageWrite

#[cfg(test)]
mod tests;

use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{UniversalAppend, UniversalReadFileOps as _, read_json_via};
use quantization::encoded_vectors_binary::EncodedVectorsBin;
use quantization::encoded_vectors_tq::EncodedVectorsTQ;

use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::vectors::VectorRef;
use crate::types::QuantizationConfig;
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::UpdateOnlyQuantizedChunkedStorage;
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedVectors, QuantizedVectorsConfig,
};

enum UpdateOnlyQuantizedVectorStorage<S: UniversalAppend + 'static> {
    Binary(Box<EncodedVectorsBin<u128, UpdateOnlyQuantizedChunkedStorage<S>>>),
    Turbo(Box<EncodedVectorsTQ<UpdateOnlyQuantizedChunkedStorage<S>>>),
}

/// The write half of a dense vector's quantized overlay, for one update-only appendable
/// segment. Opened alongside the raw [`UpdateOnlyDenseVectorStorage`] it shadows, when the
/// segment's quantization config supports it.
///
/// [`UpdateOnlyDenseVectorStorage`]: crate::vector_storage::dense::update_only::UpdateOnlyDenseVectorStorage
pub struct UpdateOnlyQuantizedVectors<S: UniversalAppend + 'static> {
    storage: UpdateOnlyQuantizedVectorStorage<S>,
}

impl<S: UniversalAppend + 'static> UpdateOnlyQuantizedVectors<S> {
    /// Reopen the quantized overlay persisted at `path`, if one is there.
    ///
    /// This never creates anything: whether a vector gets a quantized overlay is a decision made
    /// once, by whatever builds a fresh segment — not something `open` should infer from file
    /// absence. Returns `None` when nothing was persisted, e.g. quantization was never configured
    /// for this vector, or the configured method didn't support incremental appends (Scalar,
    /// Product — see [`QuantizationConfig::supports_appendable`]) at creation time.
    pub fn open(fs: S::Fs, path: &Path) -> OperationResult<Option<Self>> {
        let config_path = QuantizedVectors::get_config_path(path);
        if !fs.exists(&config_path)? {
            return Ok(None);
        }
        let config: QuantizedVectorsConfig = read_json_via(&fs, &config_path)?;
        Self::open_existing(fs, config, path).map(Some)
    }

    /// Reopen a previously-persisted overlay to resume appending: reads the fitted metadata
    /// (encoding, stats) needed to keep encoding consistently, through
    /// [`EncodedVectorsBin::reopen_for_write`]/[`EncodedVectorsTQ::reopen_for_write`] — not
    /// `load`, which additionally reads back a stored vector to validate it, a read this
    /// write-only storage cannot serve. A writer resuming appends doesn't need that guarantee:
    /// every vector it writes is sized from this same metadata, so the invariant `load`'s check
    /// protects holds by construction here, not by verification.
    fn open_existing(
        fs: S::Fs,
        config: QuantizedVectorsConfig,
        path: &Path,
    ) -> OperationResult<Self> {
        let meta_path = QuantizedVectors::get_meta_path(path);
        let data_path = QuantizedVectors::get_data_path(path, config.storage_type);
        let quantized_vector_size = config.quantized_vector_size(false);
        let meta_fs = fs.clone();

        let storage = match &config.quantization_config {
            QuantizationConfig::Binary(_) => {
                let backend = UpdateOnlyQuantizedChunkedStorage::open(
                    fs,
                    data_path.as_path(),
                    quantized_vector_size,
                )?;
                UpdateOnlyQuantizedVectorStorage::Binary(Box::new(
                    EncodedVectorsBin::reopen_for_write(&meta_fs, backend, &meta_path)?,
                ))
            }
            QuantizationConfig::Turbo(_) => {
                let backend = UpdateOnlyQuantizedChunkedStorage::open(
                    fs,
                    data_path.as_path(),
                    quantized_vector_size,
                )?;
                UpdateOnlyQuantizedVectorStorage::Turbo(Box::new(
                    EncodedVectorsTQ::reopen_for_write(&meta_fs, backend, &meta_path)?,
                ))
            }
            QuantizationConfig::Scalar(_) | QuantizationConfig::Product(_) => {
                return Err(OperationError::service_error(
                    "Scalar/Product quantization do not support appendable storage, but a \
                     persisted update-only quantized overlay config names one",
                ));
            }
        };

        Ok(Self { storage })
    }

    /// Encode and persist `vector` for `id`. `id` must be the current end of the overlay: like
    /// every other update-only storage, this never rewrites a slot in place.
    pub fn upsert_vector(
        &mut self,
        id: PointOffsetType,
        vector: VectorRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let VectorRef::Dense(vector) = vector else {
            return Err(OperationError::WrongMulti);
        };
        match &mut self.storage {
            UpdateOnlyQuantizedVectorStorage::Binary(q) => {
                Ok(q.upsert_vector(id, vector, hw_counter)?)
            }
            UpdateOnlyQuantizedVectorStorage::Turbo(q) => {
                Ok(q.upsert_vector(id, vector, hw_counter)?)
            }
        }
    }

    pub fn flusher(&self) -> Flusher {
        let flusher = match &self.storage {
            UpdateOnlyQuantizedVectorStorage::Binary(q) => q.flusher(),
            UpdateOnlyQuantizedVectorStorage::Turbo(q) => q.flusher(),
        };
        Box::new(move || flusher().map_err(OperationError::from))
    }
}
