use std::path::Path;

use common::universal_io::{MmapFile, MmapFs};
use quantization::encoded_vectors_binary;
use quantization::encoded_vectors_binary::EncodedVectorsBin;

use super::super::{
    QuantizedVectorStorage, QuantizedVectors, QuantizedVectorsConfig, QuantizedVectorsStorageType,
};
use crate::common::operation_error::OperationResult;
use crate::types::{BinaryQuantizationConfig, MultiVectorConfig};
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedMmapStorage;
use crate::vector_storage::quantized::quantized_multivector_storage::{
    MultivectorOffsetsStorageChunkedMmap, MultivectorOffsetsStorageMmap,
    MultivectorOffsetsStorageRam, QuantizedMultivectorStorage,
};
use crate::vector_storage::quantized::quantized_ram_storage::QuantizedRamStorage;
use crate::vector_storage::quantized::quantized_storage::QuantizedStorage;
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

/// The read-write binary quantization path always operates on local files.
const READ_FS: MmapFs = MmapFs;

impl QuantizedVectors {
    pub(in super::super) fn load_binary(
        vector_storage: &VectorStorageEnum,
        path: &Path,
        config: &QuantizedVectorsConfig,
        binary_config: &BinaryQuantizationConfig,
    ) -> OperationResult<QuantizedVectorStorage> {
        let on_disk_vector_storage = vector_storage.is_on_disk();
        let data_path = Self::get_data_path(path, config.storage_type);
        let meta_path = Self::get_meta_path(path);
        let in_ram = Self::is_ram(binary_config.always_ram, on_disk_vector_storage);

        match (in_ram, config.storage_type) {
            (_, QuantizedVectorsStorageType::Mutable) => {
                let quantized_vector_size =
                    encoded_vectors_binary::get_quantized_vector_size_from_params::<u128>(
                        config.vector_parameters.dim,
                        Self::convert_binary_encoding(binary_config.encoding),
                    );
                let quantization_storage = QuantizedChunkedMmapStorage::new(
                    data_path.as_path(),
                    quantized_vector_size,
                    in_ram,
                )?;
                Ok(QuantizedVectorStorage::BinaryChunkedMmap(
                    EncodedVectorsBin::load(&READ_FS, quantization_storage, meta_path.as_path())?,
                ))
            }
            (true, QuantizedVectorsStorageType::Immutable) => {
                let quantized_vector_size =
                    encoded_vectors_binary::get_quantized_vector_size_from_params::<u128>(
                        config.vector_parameters.dim,
                        Self::convert_binary_encoding(binary_config.encoding),
                    );
                let quantized_vectors_storage = QuantizedRamStorage::from_file::<MmapFile>(
                    &READ_FS,
                    data_path.as_path(),
                    quantized_vector_size,
                )?;
                Ok(QuantizedVectorStorage::BinaryRam(EncodedVectorsBin::load(
                    &READ_FS,
                    quantized_vectors_storage,
                    &meta_path,
                )?))
            }
            (false, QuantizedVectorsStorageType::Immutable) => {
                let quantized_vector_size =
                    encoded_vectors_binary::get_quantized_vector_size_from_params::<u128>(
                        config.vector_parameters.dim,
                        Self::convert_binary_encoding(binary_config.encoding),
                    );
                let quantized_vectors_storage = QuantizedStorage::from_file(
                    &READ_FS,
                    data_path.as_path(),
                    quantized_vector_size,
                )?;
                Ok(QuantizedVectorStorage::BinaryMmap(EncodedVectorsBin::load(
                    &READ_FS,
                    quantized_vectors_storage,
                    &meta_path,
                )?))
            }
        }
    }

    pub(in super::super) fn load_binary_multi(
        vector_storage: &VectorStorageEnum,
        path: &Path,
        config: &QuantizedVectorsConfig,
        binary_config: &BinaryQuantizationConfig,
        multivector_config: &MultiVectorConfig,
    ) -> OperationResult<QuantizedVectorStorage> {
        let on_disk_vector_storage = vector_storage.is_on_disk();
        let data_path = Self::get_data_path(path, config.storage_type);
        let meta_path = Self::get_meta_path(path);
        let offsets_path = Self::get_offsets_path(path, config.storage_type);
        let in_ram = Self::is_ram(binary_config.always_ram, on_disk_vector_storage);

        match (in_ram, config.storage_type) {
            (_, QuantizedVectorsStorageType::Mutable) => {
                let quantized_vector_size =
                    encoded_vectors_binary::get_quantized_vector_size_from_params::<u8>(
                        config.vector_parameters.dim,
                        Self::convert_binary_encoding(binary_config.encoding),
                    );
                let quantization_storage = QuantizedChunkedMmapStorage::new(
                    data_path.as_path(),
                    quantized_vector_size,
                    in_ram,
                )?;
                let inner_storage =
                    EncodedVectorsBin::load(&READ_FS, quantization_storage, meta_path.as_path())?;
                let offsets_storage =
                    MultivectorOffsetsStorageChunkedMmap::load(offsets_path.as_path(), in_ram)?;

                Ok(QuantizedVectorStorage::BinaryChunkedMmapMulti(
                    QuantizedMultivectorStorage::new(
                        config.vector_parameters.dim,
                        inner_storage,
                        offsets_storage,
                        *multivector_config,
                    ),
                ))
            }
            (true, QuantizedVectorsStorageType::Immutable) => {
                let quantized_vector_size =
                    encoded_vectors_binary::get_quantized_vector_size_from_params::<u8>(
                        config.vector_parameters.dim,
                        Self::convert_binary_encoding(binary_config.encoding),
                    );
                let inner_vectors_storage = QuantizedRamStorage::from_file::<MmapFile>(
                    &READ_FS,
                    data_path.as_path(),
                    quantized_vector_size,
                )?;
                let inner_vectors_storage =
                    EncodedVectorsBin::load(&READ_FS, inner_vectors_storage, &meta_path)?;
                let offsets = MultivectorOffsetsStorageRam::load(&offsets_path)?;
                Ok(QuantizedVectorStorage::BinaryRamMulti(
                    QuantizedMultivectorStorage::new(
                        config.vector_parameters.dim,
                        inner_vectors_storage,
                        offsets,
                        *multivector_config,
                    ),
                ))
            }
            (false, QuantizedVectorsStorageType::Immutable) => {
                let quantized_vector_size =
                    encoded_vectors_binary::get_quantized_vector_size_from_params::<u8>(
                        config.vector_parameters.dim,
                        Self::convert_binary_encoding(binary_config.encoding),
                    );
                let inner_vectors_storage = QuantizedStorage::from_file(
                    &READ_FS,
                    data_path.as_path(),
                    quantized_vector_size,
                )?;
                let inner_vectors_storage =
                    EncodedVectorsBin::load(&READ_FS, inner_vectors_storage, &meta_path)?;
                let offsets = MultivectorOffsetsStorageMmap::load(&offsets_path)?;
                Ok(QuantizedVectorStorage::BinaryMmapMulti(
                    QuantizedMultivectorStorage::new(
                        config.vector_parameters.dim,
                        inner_vectors_storage,
                        offsets,
                        *multivector_config,
                    ),
                ))
            }
        }
    }
}
