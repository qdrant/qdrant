use std::path::Path;
use std::sync::atomic::AtomicBool;

use quantization::{EncodedVectorsPQ, encoded_vectors_pq};

use super::super::{QuantizedVectorStorage, QuantizedVectors, QuantizedVectorsStorageType};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::vectors::VectorElementType;
use crate::types::{MultiVectorConfig, ProductQuantizationConfig};
use crate::vector_storage::quantized::quantized_multivector_storage::{
    MultivectorOffset, MultivectorOffsetsStorageMmap, MultivectorOffsetsStorageRam,
    QuantizedMultivectorStorage,
};
use crate::vector_storage::quantized::quantized_ram_storage::QuantizedRamStorageBuilder;
use crate::vector_storage::quantized::quantized_storage::QuantizedStorageBuilder;

impl QuantizedVectors {
    #[allow(clippy::too_many_arguments)]
    pub(in super::super) fn create_pq<'a>(
        vectors: impl Iterator<Item = impl AsRef<[VectorElementType]> + 'a> + Clone + Send,
        vector_parameters: &quantization::VectorParameters,
        vectors_count: usize,
        pq_config: &ProductQuantizationConfig,
        storage_type: QuantizedVectorsStorageType,
        path: &Path,
        on_disk_vector_storage: bool,
        max_threads: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<QuantizedVectorStorage> {
        if !storage_type.is_immutable() {
            return Err(OperationError::service_error(
                "Mutable product quantization is not supported",
            ));
        }

        let bucket_size = Self::get_bucket_size(pq_config.compression);
        let quantized_vector_size =
            encoded_vectors_pq::get_quantized_vector_size(vector_parameters, bucket_size);
        let meta_path = Self::get_meta_path(path);
        let data_path = Self::get_data_path(path, storage_type);
        let in_ram = Self::is_ram(pq_config.memory_placement(), on_disk_vector_storage);
        if in_ram {
            let storage_builder = QuantizedRamStorageBuilder::new(
                data_path.as_path(),
                vectors_count,
                quantized_vector_size,
            )?;
            Ok(QuantizedVectorStorage::PQRam(EncodedVectorsPQ::encode(
                vectors,
                storage_builder,
                vector_parameters,
                vectors_count,
                bucket_size,
                max_threads,
                Some(meta_path.as_path()),
                stopped,
            )?))
        } else {
            let storage_builder = QuantizedStorageBuilder::new(
                data_path.as_path(),
                vectors_count,
                quantized_vector_size,
            )?;
            Ok(QuantizedVectorStorage::PQMmap(EncodedVectorsPQ::encode(
                vectors,
                storage_builder,
                vector_parameters,
                vectors_count,
                bucket_size,
                max_threads,
                Some(meta_path.as_path()),
                stopped,
            )?))
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(in super::super) fn create_pq_multi<'a>(
        vectors: impl Iterator<Item = impl AsRef<[VectorElementType]> + 'a> + Clone + Send,
        offsets: impl Iterator<Item = MultivectorOffset>,
        vector_parameters: &quantization::VectorParameters,
        vectors_count: usize,
        inner_vectors_count: usize,
        pq_config: &ProductQuantizationConfig,
        storage_type: QuantizedVectorsStorageType,
        multi_vector_config: MultiVectorConfig,
        path: &Path,
        on_disk_vector_storage: bool,
        max_threads: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<QuantizedVectorStorage> {
        if !storage_type.is_immutable() {
            return Err(OperationError::service_error(
                "Mutable product quantization is not supported",
            ));
        }

        let bucket_size = Self::get_bucket_size(pq_config.compression);
        let quantized_vector_size =
            encoded_vectors_pq::get_quantized_vector_size(vector_parameters, bucket_size);
        let meta_path = Self::get_meta_path(path);
        let data_path = Self::get_data_path(path, storage_type);
        let offsets_path = Self::get_offsets_path(path, storage_type);
        let in_ram = Self::is_ram(pq_config.memory_placement(), on_disk_vector_storage);
        if in_ram {
            let storage_builder = QuantizedRamStorageBuilder::new(
                data_path.as_path(),
                inner_vectors_count,
                quantized_vector_size,
            )?;
            let quantized_storage = EncodedVectorsPQ::encode(
                vectors,
                storage_builder,
                vector_parameters,
                inner_vectors_count,
                bucket_size,
                max_threads,
                Some(meta_path.as_path()),
                stopped,
            )?;
            let offsets = MultivectorOffsetsStorageRam::create(&offsets_path, offsets)?;
            Ok(QuantizedVectorStorage::PQRamMulti(
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
            let quantized_storage = EncodedVectorsPQ::encode(
                vectors,
                storage_builder,
                vector_parameters,
                inner_vectors_count,
                bucket_size,
                max_threads,
                Some(meta_path.as_path()),
                stopped,
            )?;
            let offsets =
                MultivectorOffsetsStorageMmap::create(&offsets_path, offsets, vectors_count)?;
            Ok(QuantizedVectorStorage::PQMmapMulti(
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
