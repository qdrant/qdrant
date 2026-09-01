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
//! `reopen_for_write` both read). Both `reopen_for_write` and `upsert_many`/`flusher` only
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

use std::borrow::Cow;
use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{UniversalAppend, UniversalReadFileOps as _, read_json_via};
use quantization::encoded_vectors_binary::EncodedVectorsBin;
use quantization::encoded_vectors_tq::EncodedVectorsTQ;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{VectorElementType, VectorElementTypeByte, VectorElementTypeHalf};
use crate::types::{Distance, QuantizationConfig, VectorDataConfig, VectorStorageDatatype};
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::UpdateOnlyQuantizedChunkedStorage;
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedVectors, QuantizedVectorsConfig,
};
use crate::vector_storage::update_only::VectorToStore;

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
    config: QuantizedVectorsConfig,
    /// Raw-storage properties, needed to decode a [`VectorToStore::Raw`].
    distance: Distance,
    datatype: VectorStorageDatatype,
}

impl<S: UniversalAppend + 'static> UpdateOnlyQuantizedVectors<S> {
    /// Reopen the quantized overlay persisted at `path`, if one is there.
    ///
    /// This never creates anything: whether a vector gets a quantized overlay is a decision made
    /// once, by whatever builds a fresh segment — not something `open` should infer from file
    /// absence. Returns `None` when nothing was persisted, e.g. quantization was never configured
    /// for this vector, or the configured method didn't support incremental appends (Scalar,
    /// Product — see [`QuantizationConfig::supports_appendable`]) at creation time.
    pub fn open(
        fs: S::Fs,
        path: &Path,
        vector_config: &VectorDataConfig,
    ) -> OperationResult<Option<Self>> {
        let datatype = vector_config.datatype.unwrap_or_default();
        // Multivector and Turbo4-datatype vectors never get an overlay, so they return
        // `None` outright.
        if vector_config.multivector_config.is_some() || datatype == VectorStorageDatatype::Turbo4 {
            return Ok(None);
        }

        let config_path = QuantizedVectors::get_config_path(path);
        if !fs.exists(&config_path)? {
            return Ok(None);
        }
        let config: QuantizedVectorsConfig = read_json_via(&fs, &config_path)?;
        Self::open_existing(fs, config, path, vector_config.distance, datatype).map(Some)
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
        distance: Distance,
        datatype: VectorStorageDatatype,
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

        Ok(Self {
            storage,
            config,
            distance,
            datatype,
        })
    }

    /// Encode and persist one row per point of a batch, starting at `start_slot` — the current
    /// end of the overlay, since slots are never rewritten in place.
    ///
    /// Every point takes a row (a missing vector as an all-zero placeholder), keeping row `k` in
    /// lockstep with slot `k` of the raw storage this overlay shadows.
    pub fn append_many<'a>(
        &mut self,
        start_slot: PointOffsetType,
        vectors: impl IntoIterator<Item = VectorToStore<'a>>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        // Decoded whole rather than streamed: an owned row must outlive the storage call.
        let placeholder = vec![0.0 as VectorElementType; self.dim()];
        let mut run: Vec<Cow<[VectorElementType]>> = Vec::new();
        for vector in vectors {
            run.push(match vector {
                VectorToStore::Decoded(vector) => Cow::Borrowed(vector.try_into()?),
                VectorToStore::Raw(bytes) => self.decode_raw(bytes)?,
                VectorToStore::Missing => Cow::Borrowed(placeholder.as_slice()),
            });
        }

        let rows = run.iter().map(Cow::as_ref);
        match &mut self.storage {
            UpdateOnlyQuantizedVectorStorage::Binary(q) => {
                Ok(q.append_many(start_slot, rows, hw_counter)?)
            }
            UpdateOnlyQuantizedVectorStorage::Turbo(q) => {
                Ok(q.append_many(start_slot, rows, hw_counter)?)
            }
        }
    }

    /// The dimensionality of the (dense, unrotated) vector this overlay quantizes.
    fn dim(&self) -> usize {
        self.config.vector_parameters.dim
    }

    /// Reconstruct the `f32` form of a raw, storage-native vector, preprocessing it per the
    /// persisted config — the source of truth over whatever live config the caller has.
    fn decode_raw<'a>(&self, bytes: &'a [u8]) -> OperationResult<Cow<'a, [VectorElementType]>> {
        match self.datatype {
            VectorStorageDatatype::Float32 => self.decode_raw_as::<VectorElementType>(bytes),
            VectorStorageDatatype::Uint8 => self.decode_raw_as::<VectorElementTypeByte>(bytes),
            VectorStorageDatatype::Float16 => self.decode_raw_as::<VectorElementTypeHalf>(bytes),
            VectorStorageDatatype::Turbo4 => {
                unreachable!("`Self::open` opens no overlay for a Turbo4-datatype vector")
            }
        }
    }

    /// Mirrors `QuantizedVectors::create_impl` on the non-update-only path.
    fn decode_raw_as<'a, T: PrimitiveVectorElement>(
        &self,
        bytes: &'a [u8],
    ) -> OperationResult<Cow<'a, [VectorElementType]>> {
        let expected_size = self.dim() * size_of::<T>();
        if bytes.len() != expected_size {
            // `MalformedVectorBlob`, not a service error: a blob that reached
            // the WAL is skipped on replay rather than crash-looping recovery.
            return Err(OperationError::malformed_vector_blob(format!(
                "Malformed dense vector blob of {} bytes, expected {expected_size}",
                bytes.len(),
            )));
        }

        // A misaligned blob decodes through a copy.
        let vector: Cow<'a, [T]> = match bytemuck::try_cast_slice(bytes) {
            Ok(slice) => Cow::Borrowed(slice),
            Err(_) => Cow::Owned(bytemuck::allocation::pod_collect_to_vec(bytes)),
        };

        Ok(T::quantization_preprocess(
            &self.config.quantization_config,
            self.distance,
            vector,
        ))
    }
}
