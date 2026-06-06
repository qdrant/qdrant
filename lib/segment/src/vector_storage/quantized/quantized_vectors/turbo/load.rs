use std::path::Path;

use common::universal_io::MmapFs;
use quantization::encoded_vectors_tq;
use quantization::encoded_vectors_tq::EncodedVectorsTQ;
use quantization::turboquant::TQMode;

use super::super::{
    QuantizedVectorStorage, QuantizedVectors, QuantizedVectorsConfig, QuantizedVectorsStorageType,
};
use crate::common::operation_error::OperationResult;
use crate::types::{MultiVectorConfig, TurboQuantQuantizationConfig};
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedMmapStorage;
use crate::vector_storage::quantized::quantized_multivector_storage::{
    MultivectorOffsetsStorageChunkedMmap, MultivectorOffsetsStorageMmap,
    MultivectorOffsetsStorageRam, QuantizedMultivectorStorage,
};
use crate::vector_storage::quantized::quantized_ram_storage::QuantizedRamStorage;
use crate::vector_storage::quantized::quantized_storage::QuantizedStorage;
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

impl QuantizedVectors {
    pub(in super::super) fn load_turbo(
        vector_storage: &VectorStorageEnum,
        path: &Path,
        config: &QuantizedVectorsConfig,
        turbo_config: &TurboQuantQuantizationConfig,
    ) -> OperationResult<QuantizedVectorStorage> {
        let bits = Self::convert_tq_bits(turbo_config.bits.unwrap_or_default());
        let mode = TQMode::Plus;

        let on_disk_vector_storage = vector_storage.is_on_disk();
        let data_path = Self::get_data_path(path, config.storage_type);
        let meta_path = Self::get_meta_path(path);
        let in_ram = Self::is_ram(turbo_config.always_ram, on_disk_vector_storage);

        match (in_ram, config.storage_type) {
            (_, QuantizedVectorsStorageType::Mutable) => {
                let quantized_vector_size = encoded_vectors_tq::get_quantized_vector_size(
                    &config.vector_parameters,
                    bits,
                    mode,
                );
                let quantization_storage = QuantizedChunkedMmapStorage::new(
                    data_path.as_path(),
                    quantized_vector_size,
                    in_ram,
                )?;
                Ok(QuantizedVectorStorage::TQChunkedMmap(
                    EncodedVectorsTQ::load(quantization_storage, meta_path.as_path())?,
                ))
            }
            (true, QuantizedVectorsStorageType::Immutable) => {
                let quantized_vector_size = encoded_vectors_tq::get_quantized_vector_size(
                    &config.vector_parameters,
                    bits,
                    mode,
                );
                let quantized_vectors_storage =
                    QuantizedRamStorage::from_file(data_path.as_path(), quantized_vector_size)?;
                Ok(QuantizedVectorStorage::TQRam(EncodedVectorsTQ::load(
                    quantized_vectors_storage,
                    &meta_path,
                )?))
            }
            (false, QuantizedVectorsStorageType::Immutable) => {
                let quantized_vector_size = encoded_vectors_tq::get_quantized_vector_size(
                    &config.vector_parameters,
                    bits,
                    mode,
                );
                let quantized_vectors_storage = QuantizedStorage::from_file(
                    &MmapFs,
                    data_path.as_path(),
                    quantized_vector_size,
                )?;
                Ok(QuantizedVectorStorage::TQMmap(EncodedVectorsTQ::load(
                    quantized_vectors_storage,
                    &meta_path,
                )?))
            }
        }
    }

    pub(in super::super) fn load_turbo_multi(
        vector_storage: &VectorStorageEnum,
        path: &Path,
        config: &QuantizedVectorsConfig,
        turbo_config: &TurboQuantQuantizationConfig,
        multivector_config: &MultiVectorConfig,
    ) -> OperationResult<QuantizedVectorStorage> {
        let bits = Self::convert_tq_bits(turbo_config.bits.unwrap_or_default());
        let mode = TQMode::Plus;

        let on_disk_vector_storage = vector_storage.is_on_disk();
        let data_path = Self::get_data_path(path, config.storage_type);
        let meta_path = Self::get_meta_path(path);
        let offsets_path = Self::get_offsets_path(path, config.storage_type);
        let in_ram = Self::is_ram(turbo_config.always_ram, on_disk_vector_storage);

        match (in_ram, config.storage_type) {
            (_, QuantizedVectorsStorageType::Mutable) => {
                let quantized_vector_size = encoded_vectors_tq::get_quantized_vector_size(
                    &config.vector_parameters,
                    bits,
                    mode,
                );
                let quantization_storage = QuantizedChunkedMmapStorage::new(
                    data_path.as_path(),
                    quantized_vector_size,
                    in_ram,
                )?;
                let inner_storage =
                    EncodedVectorsTQ::load(quantization_storage, meta_path.as_path())?;
                let offsets_storage =
                    MultivectorOffsetsStorageChunkedMmap::load(offsets_path.as_path(), in_ram)?;

                Ok(QuantizedVectorStorage::TQChunkedMmapMulti(
                    QuantizedMultivectorStorage::new(
                        config.vector_parameters.dim,
                        inner_storage,
                        offsets_storage,
                        *multivector_config,
                    ),
                ))
            }
            (true, QuantizedVectorsStorageType::Immutable) => {
                let quantized_vector_size = encoded_vectors_tq::get_quantized_vector_size(
                    &config.vector_parameters,
                    bits,
                    mode,
                );
                let inner_vectors_storage =
                    QuantizedRamStorage::from_file(data_path.as_path(), quantized_vector_size)?;
                let inner_vectors_storage =
                    EncodedVectorsTQ::load(inner_vectors_storage, &meta_path)?;
                let offsets = MultivectorOffsetsStorageRam::load(&offsets_path)?;
                Ok(QuantizedVectorStorage::TQRamMulti(
                    QuantizedMultivectorStorage::new(
                        config.vector_parameters.dim,
                        inner_vectors_storage,
                        offsets,
                        *multivector_config,
                    ),
                ))
            }
            (false, QuantizedVectorsStorageType::Immutable) => {
                let quantized_vector_size = encoded_vectors_tq::get_quantized_vector_size(
                    &config.vector_parameters,
                    bits,
                    mode,
                );
                let inner_vectors_storage = QuantizedStorage::from_file(
                    &MmapFs,
                    data_path.as_path(),
                    quantized_vector_size,
                )?;
                let inner_vectors_storage =
                    EncodedVectorsTQ::load(inner_vectors_storage, &meta_path)?;
                let offsets = MultivectorOffsetsStorageMmap::load(&offsets_path)?;
                Ok(QuantizedVectorStorage::TQMmapMulti(
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
