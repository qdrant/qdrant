use std::path::Path;

use common::universal_io::MmapFs;
use quantization::encoded_vectors_u8;
use quantization::encoded_vectors_u8::EncodedVectorsU8;

use super::super::{QuantizedVectorStorage, QuantizedVectors, QuantizedVectorsConfig};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::types::{MultiVectorConfig, ScalarQuantizationConfig};
use crate::vector_storage::quantized::quantized_multivector_storage::{
    MultivectorOffsetsStorageMmap, MultivectorOffsetsStorageRam, QuantizedMultivectorStorage,
};
use crate::vector_storage::quantized::quantized_ram_storage::QuantizedRamStorage;
use crate::vector_storage::quantized::quantized_storage::QuantizedStorage;
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

impl QuantizedVectors {
    pub(in super::super) fn load_scalar(
        vector_storage: &VectorStorageEnum,
        path: &Path,
        config: &QuantizedVectorsConfig,
        scalar_config: &ScalarQuantizationConfig,
    ) -> OperationResult<QuantizedVectorStorage> {
        if !config.storage_type.is_immutable() {
            return Err(OperationError::service_error(
                "Mutable quantized storage is not supported for Scalar Quantization",
            ));
        }

        let on_disk_vector_storage = vector_storage.is_on_disk();
        let data_path = Self::get_data_path(path, config.storage_type);
        let meta_path = Self::get_meta_path(path);
        if Self::is_ram(scalar_config.always_ram, on_disk_vector_storage) {
            let quantized_vector_size =
                encoded_vectors_u8::get_quantized_vector_size(&config.vector_parameters);
            let quantized_vectors_storage =
                QuantizedRamStorage::from_file(data_path.as_path(), quantized_vector_size)?;
            Ok(QuantizedVectorStorage::ScalarRam(EncodedVectorsU8::load(
                quantized_vectors_storage,
                &meta_path,
            )?))
        } else {
            let quantized_vector_size =
                encoded_vectors_u8::get_quantized_vector_size(&config.vector_parameters);
            let quantized_vectors_storage =
                QuantizedStorage::from_file(&MmapFs, data_path.as_path(), quantized_vector_size)?;
            Ok(QuantizedVectorStorage::ScalarMmap(EncodedVectorsU8::load(
                quantized_vectors_storage,
                &meta_path,
            )?))
        }
    }

    pub(in super::super) fn load_scalar_multi(
        vector_storage: &VectorStorageEnum,
        path: &Path,
        config: &QuantizedVectorsConfig,
        scalar_config: &ScalarQuantizationConfig,
        multivector_config: &MultiVectorConfig,
    ) -> OperationResult<QuantizedVectorStorage> {
        if !config.storage_type.is_immutable() {
            return Err(OperationError::service_error(
                "Mutable quantized multivector storage is not supported for Scalar Quantization",
            ));
        }

        let on_disk_vector_storage = vector_storage.is_on_disk();
        let data_path = Self::get_data_path(path, config.storage_type);
        let meta_path = Self::get_meta_path(path);
        let offsets_path = Self::get_offsets_path(path, config.storage_type);
        if Self::is_ram(scalar_config.always_ram, on_disk_vector_storage) {
            let quantized_vector_size =
                encoded_vectors_u8::get_quantized_vector_size(&config.vector_parameters);
            let inner_vectors_storage =
                QuantizedRamStorage::from_file(data_path.as_path(), quantized_vector_size)?;
            let inner_vectors_storage = EncodedVectorsU8::load(inner_vectors_storage, &meta_path)?;
            let offsets = MultivectorOffsetsStorageRam::load(&offsets_path)?;
            Ok(QuantizedVectorStorage::ScalarRamMulti(
                QuantizedMultivectorStorage::new(
                    config.vector_parameters.dim,
                    inner_vectors_storage,
                    offsets,
                    *multivector_config,
                ),
            ))
        } else {
            let quantized_vector_size =
                encoded_vectors_u8::get_quantized_vector_size(&config.vector_parameters);
            let inner_vectors_storage =
                QuantizedStorage::from_file(&MmapFs, data_path.as_path(), quantized_vector_size)?;
            let inner_vectors_storage = EncodedVectorsU8::load(inner_vectors_storage, &meta_path)?;
            let offsets = MultivectorOffsetsStorageMmap::load(&offsets_path)?;
            Ok(QuantizedVectorStorage::ScalarMmapMulti(
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
