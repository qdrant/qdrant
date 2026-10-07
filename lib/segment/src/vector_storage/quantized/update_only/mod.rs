//! The write half of quantized vectors, for update-only appendable segments.
//!
//! Scoped to what an update-only segment needs: dense (single-vector) Binary and TurboQuant
//! quantization only, the two methods [`QuantizationConfig::supports_appendable`] allows.
//!
//! [`UpdateOnlyQuantizedVectors::open`] only reopens an overlay that already exists on disk. It
//! loads the fitted metadata through the `quantization` crate's standalone encoders and appends
//! the encoded rows in the layout [`QuantizedChunkedStorage`] reads, so a promoted segment's
//! quantized data reads through the existing reader.
//!
//! [`QuantizedChunkedStorage`]: crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedStorage

#[cfg(test)]
mod tests;

use std::borrow::Cow;
use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{UniversalAppendFs, read_json_via};
use quantization::encoded_vectors_binary::EncoderBin;
use quantization::encoded_vectors_tq::EncoderTQ;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{VectorElementType, VectorElementTypeByte, VectorElementTypeHalf};
use crate::types::{Distance, QuantizationConfig, VectorDataConfig, VectorStorageDatatype};
use crate::vector_storage::VectorOffsetType;
use crate::vector_storage::chunked_vectors::update_only::UpdateOnlyChunkedVectors;
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedVectors, QuantizedVectorsConfig,
};
use crate::vector_storage::update_only::VectorToStore;

enum Encoder {
    Binary(EncoderBin<u128>),
    Turbo(Box<EncoderTQ>),
}

impl Encoder {
    fn encode(&mut self, vector: &[VectorElementType]) -> Vec<u8> {
        match self {
            Encoder::Binary(encoder) => encoder.encode(vector).as_bytes().to_vec(),
            Encoder::Turbo(encoder) => encoder.encode(vector),
        }
    }
}

/// The write half of a dense vector's quantized overlay, for one update-only appendable
/// segment. Opened alongside the raw [`UpdateOnlyDenseVectorStorage`] it shadows, when the
/// segment's quantization config supports it.
///
/// [`UpdateOnlyDenseVectorStorage`]: crate::vector_storage::dense::update_only::UpdateOnlyDenseVectorStorage
pub struct UpdateOnlyQuantizedVectors {
    encoder: Encoder,
    vectors: UpdateOnlyChunkedVectors<u8>,
    config: QuantizedVectorsConfig,
    /// Raw-storage properties, needed to decode a [`VectorToStore::Raw`].
    distance: Distance,
    datatype: VectorStorageDatatype,
}

impl UpdateOnlyQuantizedVectors {
    /// Reopen the quantized overlay persisted at `path`, if one is there.
    ///
    /// This never creates anything: whether a vector gets a quantized overlay is a decision made
    /// once, by whatever builds a fresh segment — not something `open` should infer from file
    /// absence. Returns `None` when nothing was persisted, e.g. quantization was never configured
    /// for this vector, or the configured method didn't support incremental appends (Scalar,
    /// Product — see [`QuantizationConfig::supports_appendable`]) at creation time.
    pub fn open<Fs: UniversalAppendFs>(
        fs: &Fs,
        path: &Path,
        vector_config: &VectorDataConfig,
    ) -> OperationResult<Option<Self>> {
        let datatype = vector_config.datatype.unwrap_or_default();
        // Multivector and TurboQuant-datatype vectors never get an overlay, so they
        // return `None` outright.
        if vector_config.multivector_config.is_some()
            || matches!(
                datatype,
                VectorStorageDatatype::Turbo4 | VectorStorageDatatype::Turbo8
            )
        {
            return Ok(None);
        }

        let config_path = QuantizedVectors::get_config_path(path);
        if !fs.exists(&config_path)? {
            return Ok(None);
        }
        let config: QuantizedVectorsConfig = read_json_via(fs, &config_path)?;
        Self::open_existing(fs, config, path, vector_config.distance, datatype).map(Some)
    }

    fn open_existing<Fs: UniversalAppendFs>(
        fs: &Fs,
        config: QuantizedVectorsConfig,
        path: &Path,
        distance: Distance,
        datatype: VectorStorageDatatype,
    ) -> OperationResult<Self> {
        let meta_path = QuantizedVectors::get_meta_path(path);
        let data_path = QuantizedVectors::get_data_path(path, config.storage_type);

        let encoder = match &config.quantization_config {
            QuantizationConfig::Binary(_) => Encoder::Binary(EncoderBin::load(fs, &meta_path)?),
            QuantizationConfig::Turbo(_) => {
                Encoder::Turbo(Box::new(EncoderTQ::load(fs, &meta_path)?))
            }
            QuantizationConfig::Scalar(_) | QuantizationConfig::Product(_) => {
                return Err(OperationError::service_error(
                    "Scalar/Product quantization do not support appendable storage, but a \
                     persisted update-only quantized overlay config names one",
                ));
            }
        };
        let vectors =
            UpdateOnlyChunkedVectors::open(fs, &data_path, config.quantized_vector_size(false))?;

        Ok(Self {
            encoder,
            vectors,
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
    pub fn append_many<'a, Fs: UniversalAppendFs>(
        &mut self,
        fs: &Fs,
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

        let rows: Vec<Vec<u8>> = run
            .iter()
            .map(|vector| self.encoder.encode(vector))
            .collect();
        self.vectors.append_many(
            fs,
            start_slot as VectorOffsetType,
            rows.iter().map(Vec::as_slice),
            hw_counter,
        )
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
            VectorStorageDatatype::Turbo4 | VectorStorageDatatype::Turbo8 => {
                unreachable!("`Self::open` opens no overlay for a TurboQuant-datatype vector")
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
