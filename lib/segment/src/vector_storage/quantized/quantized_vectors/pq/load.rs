use std::path::Path;

use common::universal_io::{MmapFile, MmapFs};
use quantization::EncodedVectorsPQ;

use super::super::{QuantizedVectorStorage, QuantizedVectors, QuantizedVectorsConfig};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::types::{MultiVectorConfig, ProductQuantizationConfig};
use crate::vector_storage::quantized::quantized_multivector_storage::{
    MultivectorOffsetsStorageMmap, MultivectorOffsetsStorageRam, QuantizedMultivectorStorage,
};
use crate::vector_storage::quantized::quantized_ram_storage::QuantizedRamStorage;
use crate::vector_storage::quantized::quantized_storage::QuantizedStorage;
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

/// The read-write path always operates on local files.
const READ_FS: MmapFs = MmapFs;

impl QuantizedVectors {
    pub(in super::super) fn load_pq(
        vector_storage: &VectorStorageEnum,
        path: &Path,
        config: &QuantizedVectorsConfig,
        pq_config: &ProductQuantizationConfig,
    ) -> OperationResult<QuantizedVectorStorage> {
        if !config.storage_type.is_immutable() {
            return Err(OperationError::service_error(
                "Mutable quantized storage is not supported for Product Quantization",
            ));
        }

        let on_disk_vector_storage = vector_storage.is_on_disk();
        let data_path = Self::get_data_path(path, config.storage_type);
        let meta_path = Self::get_meta_path(path);
        let quantized_vector_size = config.quantized_vector_size(false);
        if Self::is_ram(pq_config.always_ram, on_disk_vector_storage) {
            let quantized_vectors_storage = QuantizedRamStorage::from_file::<MmapFile>(
                &READ_FS,
                data_path.as_path(),
                quantized_vector_size,
            )?;
            Ok(QuantizedVectorStorage::PQRam(EncodedVectorsPQ::load(
                &READ_FS,
                quantized_vectors_storage,
                &meta_path,
            )?))
        } else {
            let quantized_vectors_storage =
                QuantizedStorage::from_file(&READ_FS, data_path.as_path(), quantized_vector_size)?;
            Ok(QuantizedVectorStorage::PQMmap(EncodedVectorsPQ::load(
                &READ_FS,
                quantized_vectors_storage,
                &meta_path,
            )?))
        }
    }

    pub(in super::super) fn load_pq_multi(
        vector_storage: &VectorStorageEnum,
        path: &Path,
        config: &QuantizedVectorsConfig,
        pq_config: &ProductQuantizationConfig,
        multivector_config: &MultiVectorConfig,
    ) -> OperationResult<QuantizedVectorStorage> {
        if !config.storage_type.is_immutable() {
            return Err(OperationError::service_error(
                "Mutable quantized multivector storage is not supported for Product Quantization",
            ));
        }

        let on_disk_vector_storage = vector_storage.is_on_disk();
        let data_path = Self::get_data_path(path, config.storage_type);
        let meta_path = Self::get_meta_path(path);
        let offsets_path = Self::get_offsets_path(path, config.storage_type);
        let quantized_vector_size = config.quantized_vector_size(true);
        if Self::is_ram(pq_config.always_ram, on_disk_vector_storage) {
            let inner_vectors_storage = QuantizedRamStorage::from_file::<MmapFile>(
                &READ_FS,
                data_path.as_path(),
                quantized_vector_size,
            )?;
            let inner_vectors_storage =
                EncodedVectorsPQ::load(&READ_FS, inner_vectors_storage, &meta_path)?;
            let offsets = MultivectorOffsetsStorageRam::load(&offsets_path)?;
            Ok(QuantizedVectorStorage::PQRamMulti(
                QuantizedMultivectorStorage::new(
                    config.vector_parameters.dim,
                    inner_vectors_storage,
                    offsets,
                    *multivector_config,
                ),
            ))
        } else {
            let inner_vectors_storage =
                QuantizedStorage::from_file(&READ_FS, data_path.as_path(), quantized_vector_size)?;
            let inner_vectors_storage =
                EncodedVectorsPQ::load(&READ_FS, inner_vectors_storage, &meta_path)?;
            let offsets = MultivectorOffsetsStorageMmap::load(&offsets_path)?;
            Ok(QuantizedVectorStorage::PQMmapMulti(
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
