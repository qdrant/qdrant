use std::path::Path;
use std::sync::atomic::AtomicBool;

use quantization::encoded_vectors_u8;
use quantization::encoded_vectors_u8::EncodedVectorsU8;

use super::super::{QuantizedVectorStorage, QuantizedVectors, QuantizedVectorsStorageType};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::vectors::VectorElementType;
use crate::types::{MultiVectorConfig, ScalarQuantizationConfig};
use crate::vector_storage::quantized::quantized_multivector_storage::{
    MultivectorOffset, MultivectorOffsetsStorageMmap, MultivectorOffsetsStorageRam,
    QuantizedMultivectorStorage,
};
use crate::vector_storage::quantized::quantized_ram_storage::QuantizedRamStorageBuilder;
use crate::vector_storage::quantized::quantized_storage::QuantizedStorageBuilder;

impl QuantizedVectors {
    #[allow(clippy::too_many_arguments)]
    pub(in super::super) fn create_scalar<'a>(
        vectors: impl Iterator<Item = impl AsRef<[VectorElementType]> + Send + Sync + 'a> + Clone,
        vector_parameters: &quantization::VectorParameters,
        vectors_count: usize,
        scalar_config: &ScalarQuantizationConfig,
        storage_type: QuantizedVectorsStorageType,
        path: &Path,
        on_disk_vector_storage: bool,
        stopped: &AtomicBool,
    ) -> OperationResult<QuantizedVectorStorage> {
        if !storage_type.is_immutable() {
            return Err(OperationError::service_error(
                "Mutable scalar quantization is not supported",
            ));
        }

        let encoding = Self::convert_scalar_encoding(scalar_config.r#type);
        let quantized_vector_size =
            encoded_vectors_u8::get_quantized_vector_size(vector_parameters);
        let meta_path = Self::get_meta_path(path);
        let data_path = Self::get_data_path(path, storage_type);
        let in_ram = Self::is_ram(scalar_config.memory_placement(), on_disk_vector_storage);
        if in_ram {
            let storage_builder = QuantizedRamStorageBuilder::new(
                data_path.as_path(),
                vectors_count,
                quantized_vector_size,
            )?;
            Ok(QuantizedVectorStorage::ScalarRam(EncodedVectorsU8::encode(
                vectors,
                storage_builder,
                vector_parameters,
                vectors_count,
                scalar_config.quantile,
                encoding,
                Some(meta_path.as_path()),
                stopped,
            )?))
        } else {
            let storage_builder = QuantizedStorageBuilder::new(
                data_path.as_path(),
                vectors_count,
                quantized_vector_size,
            )?;
            Ok(QuantizedVectorStorage::ScalarMmap(
                EncodedVectorsU8::encode(
                    vectors,
                    storage_builder,
                    vector_parameters,
                    vectors_count,
                    scalar_config.quantile,
                    encoding,
                    Some(meta_path.as_path()),
                    stopped,
                )?,
            ))
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(in super::super) fn create_scalar_multi<'a>(
        vectors: impl Iterator<Item = impl AsRef<[VectorElementType]> + Send + Sync + 'a> + Clone,
        offsets: impl Iterator<Item = MultivectorOffset>,
        vector_parameters: &quantization::VectorParameters,
        vectors_count: usize,
        inner_vectors_count: usize,
        scalar_config: &ScalarQuantizationConfig,
        storage_type: QuantizedVectorsStorageType,
        multi_vector_config: MultiVectorConfig,
        path: &Path,
        on_disk_vector_storage: bool,
        stopped: &AtomicBool,
    ) -> OperationResult<QuantizedVectorStorage> {
        if !storage_type.is_immutable() {
            return Err(OperationError::service_error(
                "Mutable scalar quantization is not supported",
            ));
        }

        let encoding = Self::convert_scalar_encoding(scalar_config.r#type);
        let quantized_vector_size =
            encoded_vectors_u8::get_quantized_vector_size(vector_parameters);
        let meta_path = Self::get_meta_path(path);
        let data_path = Self::get_data_path(path, storage_type);
        let offsets_path = Self::get_offsets_path(path, storage_type);
        let in_ram = Self::is_ram(scalar_config.memory_placement(), on_disk_vector_storage);
        if in_ram {
            let storage_builder = QuantizedRamStorageBuilder::new(
                data_path.as_path(),
                inner_vectors_count,
                quantized_vector_size,
            )?;
            let quantized_storage = EncodedVectorsU8::encode(
                vectors,
                storage_builder,
                vector_parameters,
                inner_vectors_count,
                scalar_config.quantile,
                encoding,
                Some(meta_path.as_path()),
                stopped,
            )?;
            let offsets = MultivectorOffsetsStorageRam::create(&offsets_path, offsets)?;
            Ok(QuantizedVectorStorage::ScalarRamMulti(
                QuantizedMultivectorStorage::new(
                    vector_parameters.dim,
                    quantized_storage,
                    offsets,
                    multi_vector_config,
                ),
            ))
        } else {
            let storage_builder = QuantizedStorageBuilder::new(
                data_path.as_path(),
                inner_vectors_count,
                quantized_vector_size,
            )?;
            let quantized_storage = EncodedVectorsU8::encode(
                vectors,
                storage_builder,
                vector_parameters,
                inner_vectors_count,
                scalar_config.quantile,
                encoding,
                Some(meta_path.as_path()),
                stopped,
            )?;
            let offsets =
                MultivectorOffsetsStorageMmap::create(&offsets_path, offsets, vectors_count)?;
            Ok(QuantizedVectorStorage::ScalarMmapMulti(
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
