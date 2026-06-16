use std::path::Path;
use std::sync::atomic::AtomicBool;

use common::fs::atomic_save_json;
use common::generic_consts::{Random, Sequential};
use common::types::PointOffsetType;

use super::{
    QUANTIZED_CONFIG_PATH, QuantizedVectors, QuantizedVectorsConfig, QuantizedVectorsStorageType,
};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::types::{
    BinaryQuantization, ProductQuantization, QuantizationConfig, ScalarQuantization,
    TurboQuantization,
};
use crate::vector_storage::quantized::quantized_multivector_storage::MultivectorOffset;
use crate::vector_storage::{DenseVectorStorage, MultiVectorStorage, VectorStorageEnum};

impl QuantizedVectors {
    pub fn create(
        vector_storage: &VectorStorageEnum,
        quantization_config: &QuantizationConfig,
        storage_type: QuantizedVectorsStorageType,
        path: &Path,
        max_threads: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<Self> {
        match vector_storage {
            VectorStorageEnum::DenseVolatile(v) => Self::create_impl(
                v,
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => Self::create_impl(
                v,
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => Self::create_impl(
                v,
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
            VectorStorageEnum::DenseMemmap(v) => Self::create_impl(
                v.as_ref(),
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
            VectorStorageEnum::DenseMemmapByte(v) => Self::create_impl(
                v.as_ref(),
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
            VectorStorageEnum::DenseMemmapHalf(v) => Self::create_impl(
                v.as_ref(),
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => Self::create_impl(
                v.as_ref(),
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => Self::create_impl(
                v.as_ref(),
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => Self::create_impl(
                v.as_ref(),
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
            VectorStorageEnum::DenseAppendableMemmap(v) => Self::create_impl(
                v.as_ref(),
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => Self::create_impl(
                v.as_ref(),
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => Self::create_impl(
                v.as_ref(),
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
            VectorStorageEnum::DenseTurbo(_) => Err(OperationError::service_error(
                "Cannot quantize a Turbo4 vector storage; it is already quantized",
            )),
            VectorStorageEnum::MultiDenseTurbo(_) => Err(OperationError::service_error(
                "Cannot quantize a Turbo4 vector storage; it is already quantized",
            )),
            VectorStorageEnum::SparseVolatile(_) => Err(OperationError::WrongSparse),
            VectorStorageEnum::SparseMmap(_) => Err(OperationError::WrongSparse),
            VectorStorageEnum::MultiDenseVolatile(v) => Self::create_multi_impl(
                v,
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(v) => Self::create_multi_impl(
                v,
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(v) => Self::create_multi_impl(
                v,
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => Self::create_multi_impl(
                v.as_ref(),
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => Self::create_multi_impl(
                v.as_ref(),
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => Self::create_multi_impl(
                v.as_ref(),
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
            VectorStorageEnum::EmptyDense(v) => Self::create_impl(
                v,
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
            VectorStorageEnum::EmptySparse(_) => Err(OperationError::WrongSparse),
        }
    }

    fn create_impl<
        TElement: PrimitiveVectorElement,
        TVectorStorage: DenseVectorStorage<TElement> + Send + Sync,
    >(
        vector_storage: &TVectorStorage,
        quantization_config: &QuantizationConfig,
        storage_type: QuantizedVectorsStorageType,
        path: &Path,
        max_threads: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<Self> {
        let dim = vector_storage.vector_dim();
        let count = vector_storage.total_vector_count();
        let distance = vector_storage.distance();
        let datatype = vector_storage.datatype();
        let vectors = (0..count as PointOffsetType).map(|i| {
            let vector = vector_storage.get_dense::<Sequential>(i);
            PrimitiveVectorElement::quantization_preprocess(quantization_config, distance, vector)
        });
        let on_disk_vector_storage = vector_storage.is_on_disk();

        let vector_parameters = Self::construct_vector_parameters(
            quantization_config,
            distance,
            dim,
            count,
            storage_type,
        );

        let quantized_storage = match quantization_config {
            QuantizationConfig::Scalar(ScalarQuantization {
                scalar: scalar_config,
            }) => Self::create_scalar(
                vectors,
                &vector_parameters,
                count,
                scalar_config,
                storage_type,
                path,
                on_disk_vector_storage,
                stopped,
            )?,
            QuantizationConfig::Product(ProductQuantization { product: pq_config }) => {
                Self::create_pq(
                    vectors,
                    &vector_parameters,
                    count,
                    pq_config,
                    storage_type,
                    path,
                    on_disk_vector_storage,
                    max_threads,
                    stopped,
                )?
            }
            QuantizationConfig::Binary(BinaryQuantization {
                binary: binary_config,
            }) => Self::create_binary(
                vectors,
                &vector_parameters,
                count,
                binary_config,
                storage_type,
                path,
                on_disk_vector_storage,
                stopped,
            )?,
            QuantizationConfig::Turbo(TurboQuantization {
                turbo: turbo_config,
            }) => Self::create_turbo(
                vectors,
                &vector_parameters,
                count,
                turbo_config,
                storage_type,
                path,
                on_disk_vector_storage,
                max_threads,
                stopped,
            )?,
        };

        let quantized_vectors_config = QuantizedVectorsConfig {
            quantization_config: quantization_config.clone(),
            vector_parameters,
            storage_type,
        };

        let quantized_vectors = QuantizedVectors {
            storage_impl: quantized_storage,
            config: quantized_vectors_config,
            path: path.to_path_buf(),
            distance,
            datatype,
        };

        atomic_save_json(&path.join(QUANTIZED_CONFIG_PATH), &quantized_vectors.config)?;
        Ok(quantized_vectors)
    }

    fn create_multi_impl<
        TElement: PrimitiveVectorElement + 'static,
        TVectorStorage: MultiVectorStorage<TElement> + Send + Sync,
    >(
        vector_storage: &TVectorStorage,
        quantization_config: &QuantizationConfig,
        storage_type: QuantizedVectorsStorageType,
        path: &Path,
        max_threads: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<Self> {
        let dim = vector_storage.vector_dim();
        let distance = vector_storage.distance();
        let datatype = vector_storage.datatype();
        let multi_vector_config = *vector_storage.multi_vector_config();
        let vectors = vector_storage.iterate_inner_vectors().map(|vector| {
            PrimitiveVectorElement::quantization_preprocess(quantization_config, distance, vector)
        });
        let inner_vectors_count = vectors.clone().count();
        let vectors_count = vector_storage.total_vector_count();
        let on_disk_vector_storage = vector_storage.is_on_disk();

        let vector_parameters = Self::construct_vector_parameters(
            quantization_config,
            distance,
            dim,
            inner_vectors_count,
            storage_type,
        );

        let offsets = (0..vectors_count as PointOffsetType)
            .map(|idx| {
                vector_storage
                    .get_multi::<Random>(idx)
                    .as_ref()
                    .vectors_count() as PointOffsetType
            })
            .scan(0, |offset_acc, multi_vector_len| {
                let offset = *offset_acc;
                *offset_acc += multi_vector_len;
                Some(MultivectorOffset {
                    start: offset,
                    count: multi_vector_len,
                })
            });

        let quantized_storage = match quantization_config {
            QuantizationConfig::Scalar(ScalarQuantization {
                scalar: scalar_config,
            }) => Self::create_scalar_multi(
                vectors,
                offsets,
                &vector_parameters,
                vectors_count,
                inner_vectors_count,
                scalar_config,
                storage_type,
                multi_vector_config,
                path,
                on_disk_vector_storage,
                stopped,
            )?,
            QuantizationConfig::Product(ProductQuantization { product: pq_config }) => {
                Self::create_pq_multi(
                    vectors,
                    offsets,
                    &vector_parameters,
                    vectors_count,
                    inner_vectors_count,
                    pq_config,
                    storage_type,
                    multi_vector_config,
                    path,
                    on_disk_vector_storage,
                    max_threads,
                    stopped,
                )?
            }
            QuantizationConfig::Binary(BinaryQuantization {
                binary: binary_config,
            }) => Self::create_binary_multi(
                vectors,
                offsets,
                &vector_parameters,
                vectors_count,
                inner_vectors_count,
                binary_config,
                storage_type,
                multi_vector_config,
                path,
                on_disk_vector_storage,
                stopped,
            )?,
            QuantizationConfig::Turbo(TurboQuantization {
                turbo: turbo_config,
            }) => Self::create_turbo_multi(
                vectors,
                offsets,
                &vector_parameters,
                vectors_count,
                inner_vectors_count,
                turbo_config,
                storage_type,
                multi_vector_config,
                path,
                on_disk_vector_storage,
                max_threads,
                stopped,
            )?,
        };

        let quantized_vectors_config = QuantizedVectorsConfig {
            quantization_config: quantization_config.clone(),
            vector_parameters,
            storage_type,
        };

        let quantized_vectors = QuantizedVectors {
            storage_impl: quantized_storage,
            config: quantized_vectors_config,
            path: path.to_path_buf(),
            distance,
            datatype,
        };

        atomic_save_json(&path.join(QUANTIZED_CONFIG_PATH), &quantized_vectors.config)?;
        Ok(quantized_vectors)
    }
}
