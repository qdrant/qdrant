use std::path::Path;
use std::sync::atomic::AtomicBool;

use quantization::encoded_vectors_binary;
use quantization::encoded_vectors_binary::EncodedVectorsBin;

use super::super::{
    QuantizedVectorStorage, QuantizedVectors, QuantizedVectorsStorageType, READ_FS, ReadFile,
};
use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::VectorElementType;
use crate::types::{BinaryQuantizationConfig, MultiVectorConfig};
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedStorageBuilder;
use crate::vector_storage::quantized::quantized_multivector_storage::{
    MultivectorOffset, MultivectorOffsetsStorageChunked, MultivectorOffsetsStorageMmap,
    MultivectorOffsetsStorageRam, QuantizedMultivectorStorage,
};
use crate::vector_storage::quantized::quantized_ram_storage::QuantizedRamStorageBuilder;
use crate::vector_storage::quantized::quantized_storage::QuantizedStorageBuilder;

impl QuantizedVectors {
    #[allow(clippy::too_many_arguments)]
    pub(in super::super) fn create_binary<'a>(
        vectors: impl Iterator<Item = impl AsRef<[VectorElementType]> + 'a> + Clone,
        vector_parameters: &quantization::VectorParameters,
        vectors_count: usize,
        binary_config: &BinaryQuantizationConfig,
        storage_type: QuantizedVectorsStorageType,
        path: &Path,
        on_disk_vector_storage: bool,
        stopped: &AtomicBool,
    ) -> OperationResult<QuantizedVectorStorage> {
        let encoding = Self::convert_binary_encoding(binary_config.encoding);
        let query_encoding = Self::convert_binary_query_encoding(binary_config.query_encoding);
        let quantized_vector_size = encoded_vectors_binary::get_quantized_vector_size_from_params::<
            u128,
        >(vector_parameters.dim, encoding);
        let meta_path = Self::get_meta_path(path);
        let data_path = Self::get_data_path(path, storage_type);
        let in_ram = Self::is_ram(binary_config.memory_placement(), on_disk_vector_storage);

        match (in_ram, storage_type) {
            (_, QuantizedVectorsStorageType::Mutable) => {
                let storage_builder = QuantizedChunkedStorageBuilder::<ReadFile>::new(
                    READ_FS,
                    data_path.as_path(),
                    quantized_vector_size,
                    in_ram,
                )?;
                Ok(QuantizedVectorStorage::BinaryChunkedMmap(
                    EncodedVectorsBin::encode(
                        vectors,
                        storage_builder,
                        vector_parameters,
                        encoding,
                        query_encoding,
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
                Ok(QuantizedVectorStorage::BinaryRam(
                    EncodedVectorsBin::encode(
                        vectors,
                        storage_builder,
                        vector_parameters,
                        encoding,
                        query_encoding,
                        Some(meta_path.as_path()),
                        stopped,
                    )?,
                ))
            }
            (false, QuantizedVectorsStorageType::Immutable) => {
                let storage_builder = QuantizedStorageBuilder::new(
                    data_path.as_path(),
                    vectors_count,
                    quantized_vector_size,
                )?;
                Ok(QuantizedVectorStorage::BinaryMmap(
                    EncodedVectorsBin::encode(
                        vectors,
                        storage_builder,
                        vector_parameters,
                        encoding,
                        query_encoding,
                        Some(meta_path.as_path()),
                        stopped,
                    )?,
                ))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(in super::super) fn create_binary_multi<'a>(
        vectors: impl Iterator<Item = impl AsRef<[VectorElementType]> + 'a> + Clone,
        offsets: impl Iterator<Item = MultivectorOffset>,
        vector_parameters: &quantization::VectorParameters,
        vectors_count: usize,
        inner_vectors_count: usize,
        binary_config: &BinaryQuantizationConfig,
        storage_type: QuantizedVectorsStorageType,
        multi_vector_config: MultiVectorConfig,
        path: &Path,
        on_disk_vector_storage: bool,
        stopped: &AtomicBool,
    ) -> OperationResult<QuantizedVectorStorage> {
        let encoding = Self::convert_binary_encoding(binary_config.encoding);
        let query_encoding = Self::convert_binary_query_encoding(binary_config.query_encoding);
        let quantized_vector_size = encoded_vectors_binary::get_quantized_vector_size_from_params::<
            u8,
        >(vector_parameters.dim, encoding);
        let meta_path = Self::get_meta_path(path);
        let data_path = Self::get_data_path(path, storage_type);
        let offsets_path = Self::get_offsets_path(path, storage_type);
        let in_ram = Self::is_ram(binary_config.memory_placement(), on_disk_vector_storage);

        match (in_ram, storage_type) {
            (_, QuantizedVectorsStorageType::Mutable) => {
                let storage_builder = QuantizedChunkedStorageBuilder::<ReadFile>::new(
                    READ_FS,
                    data_path.as_path(),
                    quantized_vector_size,
                    in_ram,
                )?;
                let quantized_storage = EncodedVectorsBin::encode(
                    vectors,
                    storage_builder,
                    vector_parameters,
                    encoding,
                    query_encoding,
                    Some(meta_path.as_path()),
                    stopped,
                )?;
                let offsets = MultivectorOffsetsStorageChunked::<ReadFile>::create(
                    READ_FS,
                    &offsets_path,
                    offsets,
                    in_ram,
                )?;
                Ok(QuantizedVectorStorage::BinaryChunkedMmapMulti(
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
                let quantized_storage = EncodedVectorsBin::encode(
                    vectors,
                    storage_builder,
                    vector_parameters,
                    encoding,
                    query_encoding,
                    Some(meta_path.as_path()),
                    stopped,
                )?;
                let offsets = MultivectorOffsetsStorageRam::create(&offsets_path, offsets)?;
                Ok(QuantizedVectorStorage::BinaryRamMulti(
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
                let quantized_storage = EncodedVectorsBin::encode(
                    vectors,
                    storage_builder,
                    vector_parameters,
                    encoding,
                    query_encoding,
                    Some(meta_path.as_path()),
                    stopped,
                )?;
                let offsets =
                    MultivectorOffsetsStorageMmap::create(&offsets_path, offsets, vectors_count)?;
                Ok(QuantizedVectorStorage::BinaryMmapMulti(
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
