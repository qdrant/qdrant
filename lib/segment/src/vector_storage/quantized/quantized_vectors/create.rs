use std::path::Path;
use std::sync::atomic::AtomicBool;

use common::fs::atomic_save_json;
use common::generic_consts::{Random, Sequential};
use common::types::PointOffsetType;
use quantization::turboquant::TQRotation;

use super::{
    QUANTIZED_CONFIG_PATH, QuantizedVectors, QuantizedVectorsConfig, QuantizedVectorsStorageType,
    should_keep_source_rotated,
};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::VectorElementType;
use crate::types::{
    BinaryQuantization, Distance, MultiVectorConfig, ProductQuantization, QuantizationConfig,
    ScalarQuantization, TurboQuantization, VectorStorageDatatype,
};
use crate::vector_storage::quantized::quantized_multivector_storage::MultivectorOffset;
use crate::vector_storage::turbo::TurboVectorStorage;
use crate::vector_storage::turbo::multi::TurboMultiVectorStorage;
use crate::vector_storage::{
    DenseTQVectorStorageRead, DenseVectorStorage, MultiTQVectorStorageRead, MultiVectorStorage,
    VectorStorageEnum, VectorStorageRead,
};

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
            VectorStorageEnum::DenseTurbo(v) => Self::create_turbo_impl(
                v,
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
            VectorStorageEnum::MultiDenseTurbo(v) => Self::create_turbo_multi_impl(
                v,
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
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

        Self::quantize_dense(
            vectors,
            quantization_config,
            distance,
            datatype,
            false,
            dim,
            count,
            on_disk_vector_storage,
            storage_type,
            path,
            max_threads,
            stopped,
        )
    }

    fn create_turbo_impl(
        vector_storage: &TurboVectorStorage,
        quantization_config: &QuantizationConfig,
        storage_type: QuantizedVectorsStorageType,
        path: &Path,
        max_threads: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<Self> {
        let dim = vector_storage.vector_dim();
        let count = vector_storage.total_vector_count();
        let distance = vector_storage.distance();
        let on_disk_vector_storage = vector_storage.is_on_disk();

        let datatype = vector_storage.datatype();
        let keep_rotated = should_keep_source_rotated(datatype, quantization_config, distance);

        let vectors = (0..count as PointOffsetType)
            .map(move |i| vector_storage.get_dense_for_requantization(i, keep_rotated));

        Self::quantize_dense(
            vectors,
            quantization_config,
            distance,
            datatype,
            keep_rotated,
            dim,
            count,
            on_disk_vector_storage,
            storage_type,
            path,
            max_threads,
            stopped,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn quantize_dense<'a>(
        vectors: impl Iterator<Item = impl AsRef<[VectorElementType]> + Send + Sync + 'a> + Clone + Send,
        quantization_config: &QuantizationConfig,
        distance: Distance,
        datatype: VectorStorageDatatype,
        keep_rotated: bool,
        dim: usize,
        count: usize,
        on_disk_vector_storage: bool,
        storage_type: QuantizedVectorsStorageType,
        path: &Path,
        max_threads: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<Self> {
        let vector_parameters = Self::construct_vector_parameters(
            quantization_config,
            distance,
            dim,
            count,
            storage_type,
        );

        let (tq_rotation, input_already_rotated) = turbo_target_rotation(keep_rotated);

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
                tq_rotation,
                input_already_rotated,
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

        let offsets = (0..vectors_count as PointOffsetType)
            .map(|idx| {
                vector_storage
                    .get_multi::<Random>(idx)
                    .as_ref()
                    .vectors_count() as PointOffsetType
            })
            .scan(0, accumulate_offset);

        Self::quantize_multi(
            vectors,
            offsets,
            quantization_config,
            distance,
            datatype,
            false,
            dim,
            vectors_count,
            inner_vectors_count,
            multi_vector_config,
            on_disk_vector_storage,
            storage_type,
            path,
            max_threads,
            stopped,
        )
    }

    /// Multivector counterpart of [`Self::create_turbo_impl`].
    fn create_turbo_multi_impl(
        vector_storage: &TurboMultiVectorStorage,
        quantization_config: &QuantizationConfig,
        storage_type: QuantizedVectorsStorageType,
        path: &Path,
        max_threads: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<Self> {
        let dim = vector_storage.vector_dim();
        let distance = vector_storage.distance();
        let multi_vector_config = *vector_storage.multi_vector_config();
        let vectors_count = vector_storage.total_vector_count();
        let on_disk_vector_storage = vector_storage.is_on_disk();

        let datatype = vector_storage.datatype();
        let keep_rotated = should_keep_source_rotated(datatype, quantization_config, distance);

        let vectors = (0..vectors_count as PointOffsetType).flat_map(move |key| {
            vector_storage.get_inner_dense_for_requantization(key, keep_rotated)
        });
        let inner_vectors_count: usize = (0..vectors_count as PointOffsetType)
            .map(|key| vector_storage.point_inner_vectors_count(key))
            .sum();

        let offsets = (0..vectors_count as PointOffsetType)
            .map(|key| vector_storage.point_inner_vectors_count(key) as PointOffsetType)
            .scan(0, accumulate_offset);

        Self::quantize_multi(
            vectors,
            offsets,
            quantization_config,
            distance,
            datatype,
            keep_rotated,
            dim,
            vectors_count,
            inner_vectors_count,
            multi_vector_config,
            on_disk_vector_storage,
            storage_type,
            path,
            max_threads,
            stopped,
        )
    }

    /// Shared tail of the multi-vector create paths. See [`Self::quantize_dense`].
    #[allow(clippy::too_many_arguments)]
    fn quantize_multi<'a>(
        vectors: impl Iterator<Item = impl AsRef<[VectorElementType]> + Send + Sync + 'a> + Clone + Send,
        offsets: impl Iterator<Item = MultivectorOffset>,
        quantization_config: &QuantizationConfig,
        distance: Distance,
        datatype: VectorStorageDatatype,
        keep_rotated: bool,
        dim: usize,
        vectors_count: usize,
        inner_vectors_count: usize,
        multi_vector_config: MultiVectorConfig,
        on_disk_vector_storage: bool,
        storage_type: QuantizedVectorsStorageType,
        path: &Path,
        max_threads: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<Self> {
        let vector_parameters = Self::construct_vector_parameters(
            quantization_config,
            distance,
            dim,
            inner_vectors_count,
            storage_type,
        );

        let (tq_rotation, input_already_rotated) = turbo_target_rotation(keep_rotated);

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
                tq_rotation,
                input_already_rotated,
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

/// Resolve the TurboQuant target's rotation and encode-time skip flag from
/// whether the source rotation is kept (see [`should_keep_source_rotated`]). A
/// kept rotation means the input is already in the source's unpadded rotated
/// space, so encoding must not rotate it again; otherwise it rotates into the
/// padding as usual.
fn turbo_target_rotation(keep_rotated: bool) -> (TQRotation, bool) {
    if keep_rotated {
        (TQRotation::Unpadded, true)
    } else {
        (TQRotation::Padded, false)
    }
}

/// Running multivector offset accumulator for `Iterator::scan`: emits the start
/// offset for each multivector and advances by its inner-vector count.
// `scan` requires the callback to return `Option`, so the wrap is mandatory here.
#[allow(clippy::unnecessary_wraps)]
fn accumulate_offset(
    offset_acc: &mut PointOffsetType,
    multi_vector_len: PointOffsetType,
) -> Option<MultivectorOffset> {
    let offset = *offset_acc;
    *offset_acc += multi_vector_len;
    Some(MultivectorOffset {
        start: offset,
        count: multi_vector_len,
    })
}
