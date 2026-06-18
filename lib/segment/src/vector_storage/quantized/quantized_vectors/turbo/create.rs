use std::path::Path;
use std::sync::atomic::AtomicBool;

use quantization::encoded_vectors_tq;
use quantization::encoded_vectors_tq::EncodedVectorsTQ;
use quantization::turboquant::{TQMode, TQRotation};

use super::super::{
    QuantizedVectorStorage, QuantizedVectors, QuantizedVectorsStorageType, READ_FS, ReadFile,
};
use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::VectorElementType;
use crate::types::{MultiVectorConfig, TurboQuantQuantizationConfig};
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedStorageBuilder;
use crate::vector_storage::quantized::quantized_multivector_storage::{
    MultivectorOffset, MultivectorOffsetsStorageChunked, MultivectorOffsetsStorageMmap,
    MultivectorOffsetsStorageRam, QuantizedMultivectorStorage,
};
use crate::vector_storage::quantized::quantized_ram_storage::QuantizedRamStorageBuilder;
use crate::vector_storage::quantized::quantized_storage::QuantizedStorageBuilder;

impl QuantizedVectors {
    #[allow(clippy::too_many_arguments)]
    pub(in super::super) fn create_turbo<'a>(
        vectors: impl Iterator<Item = impl AsRef<[VectorElementType]> + 'a> + Clone + 'a,
        vector_parameters: &quantization::VectorParameters,
        vectors_count: usize,
        turbo_config: &TurboQuantQuantizationConfig,
        rotation: TQRotation,
        input_already_rotated: bool,
        storage_type: QuantizedVectorsStorageType,
        path: &Path,
        on_disk_vector_storage: bool,
        max_threads: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<QuantizedVectorStorage> {
        let bits = Self::convert_tq_bits(turbo_config.bits.unwrap_or_default());
        let mode = TQMode::Plus;
        let quantized_vector_size =
            encoded_vectors_tq::get_quantized_vector_size(vector_parameters, bits, mode);
        let meta_path = Self::get_meta_path(path);
        let data_path = Self::get_data_path(path, storage_type);
        let in_ram = Self::is_ram(turbo_config.always_ram, on_disk_vector_storage);

        match (in_ram, storage_type) {
            (_, QuantizedVectorsStorageType::Mutable) => {
                let storage_builder = QuantizedChunkedStorageBuilder::<ReadFile>::new(
                    READ_FS,
                    data_path.as_path(),
                    quantized_vector_size,
                    in_ram,
                )?;
                Ok(QuantizedVectorStorage::TQChunkedMmap(
                    EncodedVectorsTQ::encode(
                        vectors,
                        storage_builder,
                        vector_parameters,
                        vectors_count,
                        bits,
                        mode,
                        rotation,
                        input_already_rotated,
                        max_threads,
                        Some(meta_path.as_path()),
                        stopped,
                    )?,
                ))
            }
            (true, QuantizedVectorsStorageType::Immutable) => {
                let storage_builder = QuantizedRamStorageBuilder::new(
                    data_path.as_path(),
                    vectors_count,
                    quantized_vector_size,
                )?;
                Ok(QuantizedVectorStorage::TQRam(EncodedVectorsTQ::encode(
                    vectors,
                    storage_builder,
                    vector_parameters,
                    vectors_count,
                    bits,
                    mode,
                    rotation,
                    input_already_rotated,
                    max_threads,
                    Some(meta_path.as_path()),
                    stopped,
                )?))
            }
            (false, QuantizedVectorsStorageType::Immutable) => {
                let storage_builder = QuantizedStorageBuilder::new(
                    data_path.as_path(),
                    vectors_count,
                    quantized_vector_size,
                )?;
                Ok(QuantizedVectorStorage::TQMmap(EncodedVectorsTQ::encode(
                    vectors,
                    storage_builder,
                    vector_parameters,
                    vectors_count,
                    bits,
                    mode,
                    rotation,
                    input_already_rotated,
                    max_threads,
                    Some(meta_path.as_path()),
                    stopped,
                )?))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(in super::super) fn create_turbo_multi<'a>(
        vectors: impl Iterator<Item = impl AsRef<[VectorElementType]> + 'a> + Clone + 'a,
        offsets: impl Iterator<Item = MultivectorOffset>,
        vector_parameters: &quantization::VectorParameters,
        vectors_count: usize,
        inner_vectors_count: usize,
        turbo_config: &TurboQuantQuantizationConfig,
        rotation: TQRotation,
        input_already_rotated: bool,
        storage_type: QuantizedVectorsStorageType,
        multi_vector_config: MultiVectorConfig,
        path: &Path,
        on_disk_vector_storage: bool,
        max_threads: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<QuantizedVectorStorage> {
        let bits = Self::convert_tq_bits(turbo_config.bits.unwrap_or_default());
        let mode = TQMode::Plus;
        let quantized_vector_size =
            encoded_vectors_tq::get_quantized_vector_size(vector_parameters, bits, mode);
        let meta_path = Self::get_meta_path(path);
        let data_path = Self::get_data_path(path, storage_type);
        let offsets_path = Self::get_offsets_path(path, storage_type);
        let in_ram = Self::is_ram(turbo_config.always_ram, on_disk_vector_storage);

        match (in_ram, storage_type) {
            (_, QuantizedVectorsStorageType::Mutable) => {
                let storage_builder = QuantizedChunkedStorageBuilder::<ReadFile>::new(
                    READ_FS,
                    data_path.as_path(),
                    quantized_vector_size,
                    in_ram,
                )?;
                let quantized_storage = EncodedVectorsTQ::encode(
                    vectors,
                    storage_builder,
                    vector_parameters,
                    inner_vectors_count,
                    bits,
                    mode,
                    rotation,
                    input_already_rotated,
                    max_threads,
                    Some(meta_path.as_path()),
                    stopped,
                )?;
                let offsets = MultivectorOffsetsStorageChunked::<ReadFile>::create(
                    READ_FS,
                    &offsets_path,
                    offsets,
                    in_ram,
                )?;
                Ok(QuantizedVectorStorage::TQChunkedMmapMulti(
                    QuantizedMultivectorStorage::new(
                        vector_parameters.dim,
                        quantized_storage,
                        offsets,
                        multi_vector_config,
                    ),
                ))
            }
            (true, QuantizedVectorsStorageType::Immutable) => {
                let storage_builder = QuantizedRamStorageBuilder::new(
                    data_path.as_path(),
                    inner_vectors_count,
                    quantized_vector_size,
                )?;
                let quantized_storage = EncodedVectorsTQ::encode(
                    vectors,
                    storage_builder,
                    vector_parameters,
                    inner_vectors_count,
                    bits,
                    mode,
                    rotation,
                    input_already_rotated,
                    max_threads,
                    Some(meta_path.as_path()),
                    stopped,
                )?;
                let offsets = MultivectorOffsetsStorageRam::create(&offsets_path, offsets)?;
                Ok(QuantizedVectorStorage::TQRamMulti(
                    QuantizedMultivectorStorage::new(
                        vector_parameters.dim,
                        quantized_storage,
                        offsets,
                        multi_vector_config,
                    ),
                ))
            }
            (false, QuantizedVectorsStorageType::Immutable) => {
                let storage_builder = QuantizedStorageBuilder::new(
                    data_path.as_path(),
                    inner_vectors_count,
                    quantized_vector_size,
                )?;
                let quantized_storage = EncodedVectorsTQ::encode(
                    vectors,
                    storage_builder,
                    vector_parameters,
                    inner_vectors_count,
                    bits,
                    mode,
                    rotation,
                    input_already_rotated,
                    max_threads,
                    Some(meta_path.as_path()),
                    stopped,
                )?;
                let offsets =
                    MultivectorOffsetsStorageMmap::create(&offsets_path, offsets, vectors_count)?;
                Ok(QuantizedVectorStorage::TQMmapMulti(
                    QuantizedMultivectorStorage::new(
                        vector_parameters.dim,
                        quantized_storage,
                        offsets,
                        multi_vector_config,
                    ),
                ))
            }
        }
    }
}
