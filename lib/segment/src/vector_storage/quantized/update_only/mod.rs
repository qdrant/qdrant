//! The write half of quantized vectors, for update-only appendable segments.
//!
//! Mirrors [`QuantizedVectors`]'s auto-create-on-appendable-segment path (see
//! [`QuantizedVectors::load`]), but scoped to what an update-only segment needs: dense
//! (single-vector) Binary and TurboQuant quantization only — the two methods
//! [`QuantizationConfig::supports_appendable`] allows, and the two this stack currently
//! wires up (multivector support is a follow-up: it needs its own append-only offsets
//! storage, mirroring [`MultivectorOffsetsStorageChunked`] the same way this mirrors
//! [`QuantizedChunkedStorage`]).
//!
//! Reuses the `quantization` crate's encoding logic — `EncodedVectorsBin`/`EncodedVectorsTQ` —
//! almost entirely unchanged: creation goes through their existing `encode`, already generic
//! over the storage backend; reopening to resume appending goes through the new
//! `reopen_for_write` (added alongside `load`, which additionally validates a stored vector by
//! reading it back — a read this write-only storage cannot serve, and a guarantee a resuming
//! writer doesn't need: every vector it writes is sized from the same metadata `load` and
//! `reopen_for_write` both read). The only new storage-layer code is
//! [`UpdateOnlyQuantizedChunkedStorage`], an [`EncodedStorage`] backed by
//! [`UpdateOnlyChunkedVectors`] instead of positional [`ChunkedVectors`] writes, writing files
//! in the exact layout [`QuantizedChunkedStorage`] reads — so a promoted segment's quantized
//! data reads through the existing, unmodified reader.
//!
//! [`MultivectorOffsetsStorageChunked`]: crate::vector_storage::quantized::quantized_multivector_storage::MultivectorOffsetsStorageChunked
//! [`ChunkedVectors`]: crate::vector_storage::chunked_vectors::ChunkedVectors
//! [`UpdateOnlyChunkedVectors`]: crate::vector_storage::chunked_vectors::update_only::UpdateOnlyChunkedVectors
//! [`EncodedStorage`]: quantization::EncodedStorage

#[cfg(test)]
mod tests;

use std::path::Path;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{
    UniversalAppend, UniversalReadFileOps as _, UniversalWriteFileOps as _, read_json_via,
};
use quantization::EncodedVectors as _;
use quantization::encoded_vectors_binary::{self, EncodedVectorsBin};
use quantization::encoded_vectors_tq::{self, EncodedVectorsTQ};
use quantization::turboquant::{TQMode, TQRotation};

use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::vectors::{VectorElementType, VectorRef};
use crate::types::{BinaryQuantization, Distance, QuantizationConfig, TurboQuantization};
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::{
    UpdateOnlyQuantizedChunkedStorage, UpdateOnlyQuantizedChunkedStorageBuilder,
};
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedVectors, QuantizedVectorsConfig, QuantizedVectorsStorageType,
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
    /// Open the quantized overlay at `path`, creating an empty one if it is not there yet and
    /// `quantization_config` is given.
    ///
    /// Returns `None` — same as [`QuantizedVectors::load`] — when: no overlay is persisted yet
    /// and either the `appendable_quantization` feature flag is off, no `quantization_config` is
    /// configured, or the configured method does not support incremental appends (Scalar,
    /// Product — see [`QuantizationConfig::supports_appendable`]).
    pub fn open(
        fs: S::Fs,
        quantization_config: Option<&QuantizationConfig>,
        distance: Distance,
        dim: usize,
        path: &Path,
    ) -> OperationResult<Option<Self>> {
        let config_path = QuantizedVectors::get_config_path(path);
        if fs.exists(&config_path)? {
            let config: QuantizedVectorsConfig = read_json_via(&fs, &config_path)?;
            return Ok(Some(Self::open_existing(fs, config, path)?));
        }

        if !common::flags::feature_flags().appendable_quantization {
            return Ok(None);
        }
        let Some(quantization_config) = quantization_config else {
            return Ok(None);
        };
        if !quantization_config.supports_appendable() {
            return Ok(None);
        }

        Self::create(fs, quantization_config, distance, dim, path).map(Some)
    }

    /// Auto-create an empty overlay for a fresh appendable segment. An update-only segment
    /// always starts with zero points, so there is nothing to fit stats from — matching
    /// [`QuantizedVectors::load`]'s `count == 0` auto-create short-circuit.
    fn create(
        fs: S::Fs,
        quantization_config: &QuantizationConfig,
        distance: Distance,
        dim: usize,
        path: &Path,
    ) -> OperationResult<Self> {
        let storage_type = QuantizedVectorsStorageType::Mutable;
        let vector_parameters = QuantizedVectors::construct_vector_parameters(
            quantization_config,
            distance,
            dim,
            0,
            storage_type,
        );
        let meta_path = QuantizedVectors::get_meta_path(path);
        let data_path = QuantizedVectors::get_data_path(path, storage_type);
        let stopped = AtomicBool::new(false);
        let no_vectors = std::iter::empty::<&[VectorElementType]>();
        let config_fs = fs.clone();

        let storage = match quantization_config {
            QuantizationConfig::Binary(BinaryQuantization { binary }) => {
                let encoding = QuantizedVectors::convert_binary_encoding(binary.encoding);
                let query_encoding =
                    QuantizedVectors::convert_binary_query_encoding(binary.query_encoding);
                let quantized_vector_size =
                    encoded_vectors_binary::get_quantized_vector_size_from_params::<u128>(
                        vector_parameters.dim,
                        encoding,
                    );
                let storage_builder = UpdateOnlyQuantizedChunkedStorageBuilder::new(
                    fs,
                    data_path.as_path(),
                    quantized_vector_size,
                )?;
                let encoded = EncodedVectorsBin::encode(
                    no_vectors,
                    storage_builder,
                    &vector_parameters,
                    encoding,
                    query_encoding,
                    Some(meta_path.as_path()),
                    &stopped,
                )?;
                UpdateOnlyQuantizedVectorStorage::Binary(Box::new(encoded))
            }
            QuantizationConfig::Turbo(TurboQuantization { turbo }) => {
                let bits = QuantizedVectors::convert_tq_bits(turbo.bits.unwrap_or_default());
                let mode = TQMode::Plus;
                let quantized_vector_size =
                    encoded_vectors_tq::get_quantized_vector_size(&vector_parameters, bits, mode);
                let storage_builder = UpdateOnlyQuantizedChunkedStorageBuilder::new(
                    fs,
                    data_path.as_path(),
                    quantized_vector_size,
                )?;
                let encoded = EncodedVectorsTQ::encode(
                    no_vectors,
                    storage_builder,
                    &vector_parameters,
                    0,
                    bits,
                    mode,
                    TQRotation::Padded,
                    false,
                    1,
                    Some(meta_path.as_path()),
                    &stopped,
                )?;
                UpdateOnlyQuantizedVectorStorage::Turbo(Box::new(encoded))
            }
            QuantizationConfig::Scalar(_) | QuantizationConfig::Product(_) => {
                return Err(OperationError::service_error(
                    "Scalar/Product quantization do not support appendable storage; this must \
                     be filtered out by `supports_appendable()` before reaching here",
                ));
            }
        };

        let config = QuantizedVectorsConfig {
            quantization_config: quantization_config.clone(),
            vector_parameters,
            storage_type,
        };
        let bytes = serde_json::to_vec(&config).map_err(|err| {
            OperationError::service_error(format!(
                "failed to serialize quantized vectors config: {err}"
            ))
        })?;
        config_fs.atomic_save(&QuantizedVectors::get_config_path(path), &bytes)?;

        Ok(Self { storage })
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
