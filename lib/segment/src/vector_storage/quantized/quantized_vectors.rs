use std::alloc::Layout;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use io::file_operations::{atomic_save_json, read_json};
use memory::fadvise::clear_disk_cache;
use quantization::encoded_vectors_binary::EncodedVectorsBin;
use quantization::{EncodedVectors, EncodedVectorsPQ, EncodedVectorsU8};
use serde::{Deserialize, Serialize};

use super::quantized_multivector_storage::{
    MultivectorOffset, MultivectorOffsetsStorage, MultivectorOffsetsStorageMmap,
    QuantizedMultivectorStorage, create_offsets_file_from_iter,
};
use super::quantized_scorer_builder::QuantizedScorerBuilder;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{QueryVector, VectorElementType};
use crate::types::{
    BinaryQuantization, BinaryQuantizationConfig, BinaryQuantizationEncoding,
    BinaryQuantizationQueryEncoding, CompressionRatio, Distance, MultiVectorConfig,
    ProductQuantization, ProductQuantizationConfig, QuantizationConfig, ScalarQuantization,
    ScalarQuantizationConfig, VectorStorageDatatype,
};
use crate::vector_storage::quantized::quantized_mmap_storage::{
    QuantizedMmapStorage, QuantizedMmapStorageBuilder,
};
use crate::vector_storage::quantized::quantized_query_scorer::{
    InternalScorerUnsupported, QuantizedQueryScorer,
};
use crate::vector_storage::quantized::quantized_ram_storage::{
    QuantizedRamStorage, QuantizedRamStorageBuilder,
};
use crate::vector_storage::query_scorer::QueryScorerBytes;
use crate::vector_storage::{
    DenseVectorStorage, MultiVectorStorage, RawScorer, RawScorerImpl, VectorStorage,
    VectorStorageEnum,
};

pub const QUANTIZED_CONFIG_PATH: &str = "quantized.config.json";
pub const QUANTIZED_DATA_PATH: &str = "quantized.data";
pub const QUANTIZED_META_PATH: &str = "quantized.meta.json";
pub const QUANTIZED_OFFSETS_PATH: &str = "quantized.offsets.data";

#[derive(Deserialize, Serialize, Clone)]
pub struct QuantizedVectorsConfig {
    pub quantization_config: QuantizationConfig,
    pub vector_parameters: quantization::VectorParameters,
}

impl fmt::Debug for QuantizedVectorsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuantizedVectorsConfig")
            .field("quantization_config", &self.quantization_config)
            .finish_non_exhaustive()
    }
}

type ScalarRamMulti =
    QuantizedMultivectorStorage<EncodedVectorsU8<QuantizedRamStorage>, Vec<MultivectorOffset>>;

type ScalarMmapMulti = QuantizedMultivectorStorage<
    EncodedVectorsU8<QuantizedMmapStorage>,
    MultivectorOffsetsStorageMmap,
>;

type PQRamMulti =
    QuantizedMultivectorStorage<EncodedVectorsPQ<QuantizedRamStorage>, Vec<MultivectorOffset>>;

type PQMmapMulti = QuantizedMultivectorStorage<
    EncodedVectorsPQ<QuantizedMmapStorage>,
    MultivectorOffsetsStorageMmap,
>;

type BinaryRamMulti =
    QuantizedMultivectorStorage<EncodedVectorsBin<u8, QuantizedRamStorage>, Vec<MultivectorOffset>>;

type BinaryMmapMulti = QuantizedMultivectorStorage<
    EncodedVectorsBin<u8, QuantizedMmapStorage>,
    MultivectorOffsetsStorageMmap,
>;

pub enum QuantizedVectorStorage {
    ScalarRam(EncodedVectorsU8<QuantizedRamStorage>),
    ScalarMmap(EncodedVectorsU8<QuantizedMmapStorage>),
    PQRam(EncodedVectorsPQ<QuantizedRamStorage>),
    PQMmap(EncodedVectorsPQ<QuantizedMmapStorage>),
    BinaryRam(EncodedVectorsBin<u128, QuantizedRamStorage>),
    BinaryMmap(EncodedVectorsBin<u128, QuantizedMmapStorage>),
    ScalarRamMulti(ScalarRamMulti),
    ScalarMmapMulti(ScalarMmapMulti),
    PQRamMulti(PQRamMulti),
    PQMmapMulti(PQMmapMulti),
    BinaryRamMulti(BinaryRamMulti),
    BinaryMmapMulti(BinaryMmapMulti),
}

impl QuantizedVectorStorage {
    pub fn is_on_disk(&self) -> bool {
        match self {
            QuantizedVectorStorage::ScalarRam(_) => false,
            QuantizedVectorStorage::ScalarMmap(_) => true,
            QuantizedVectorStorage::PQRam(_) => false,
            QuantizedVectorStorage::PQMmap(_) => true,
            QuantizedVectorStorage::BinaryRam(_) => false,
            QuantizedVectorStorage::BinaryMmap(_) => true,
            QuantizedVectorStorage::ScalarRamMulti(_) => false,
            QuantizedVectorStorage::ScalarMmapMulti(_) => true,
            QuantizedVectorStorage::PQRamMulti(_) => false,
            QuantizedVectorStorage::PQMmapMulti(_) => true,
            QuantizedVectorStorage::BinaryRamMulti(_) => false,
            QuantizedVectorStorage::BinaryMmapMulti(_) => true,
        }
    }
}

impl fmt::Debug for QuantizedVectorStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("QuantizedVectorStorage").finish()
    }
}

#[derive(Debug)]
pub struct QuantizedVectors {
    storage_impl: QuantizedVectorStorage,
    config: QuantizedVectorsConfig,
    path: PathBuf,
    distance: Distance,
    datatype: VectorStorageDatatype,
}

impl QuantizedVectors {
    pub fn config(&self) -> &QuantizedVectorsConfig {
        &self.config
    }

    pub fn default_rescoring(&self) -> bool {
        matches!(
            self.storage_impl,
            QuantizedVectorStorage::BinaryRam(_) | QuantizedVectorStorage::BinaryMmap(_)
        )
    }

    pub fn is_multivector(&self) -> bool {
        match self.storage_impl {
            QuantizedVectorStorage::ScalarRam(_) => false,
            QuantizedVectorStorage::ScalarMmap(_) => false,
            QuantizedVectorStorage::PQRam(_) => false,
            QuantizedVectorStorage::PQMmap(_) => false,
            QuantizedVectorStorage::BinaryRam(_) => false,
            QuantizedVectorStorage::BinaryMmap(_) => false,
            QuantizedVectorStorage::ScalarRamMulti(_) => true,
            QuantizedVectorStorage::ScalarMmapMulti(_) => true,
            QuantizedVectorStorage::PQRamMulti(_) => true,
            QuantizedVectorStorage::PQMmapMulti(_) => true,
            QuantizedVectorStorage::BinaryRamMulti(_) => true,
            QuantizedVectorStorage::BinaryMmapMulti(_) => true,
        }
    }

    /// Get layout for a single quantized vector.
    ///
    /// I.e. the size of a single vector in bytes, and the required alignment.
    pub fn get_quantized_vector_layout(&self) -> OperationResult<Layout> {
        match &self.storage_impl {
            QuantizedVectorStorage::ScalarRam(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::ScalarMmap(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::PQRam(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::PQMmap(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::BinaryRam(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::BinaryMmap(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::ScalarRamMulti(_)
            | QuantizedVectorStorage::ScalarMmapMulti(_)
            | QuantizedVectorStorage::PQRamMulti(_)
            | QuantizedVectorStorage::PQMmapMulti(_)
            | QuantizedVectorStorage::BinaryRamMulti(_)
            | QuantizedVectorStorage::BinaryMmapMulti(_) => Err(OperationError::service_error(
                "Cannot get quantized vector layout from multivector storage",
            )),
        }
    }

    pub fn get_quantized_vector(&self, id: PointOffsetType) -> &[u8] {
        match &self.storage_impl {
            QuantizedVectorStorage::ScalarRam(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::ScalarMmap(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::PQRam(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::PQMmap(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::BinaryRam(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::BinaryMmap(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::ScalarRamMulti(_)
            | QuantizedVectorStorage::ScalarMmapMulti(_)
            | QuantizedVectorStorage::PQRamMulti(_)
            | QuantizedVectorStorage::PQMmapMulti(_)
            | QuantizedVectorStorage::BinaryRamMulti(_)
            | QuantizedVectorStorage::BinaryMmapMulti(_) => {
                panic!("Cannot get quantized vector from multivector storage");
            }
        }
    }

    pub fn raw_scorer<'a>(
        &'a self,
        query: QueryVector,
        hardware_counter: HardwareCounterCell,
    ) -> OperationResult<Box<dyn RawScorer + 'a>> {
        QuantizedScorerBuilder::new(
            &self.storage_impl,
            &self.config.quantization_config,
            query,
            &self.distance,
            self.datatype,
            hardware_counter,
        )
        .build_raw_scorer()
    }

    /// Build a raw scorer for the specified `point_id`.
    /// If not supported, return [`InternalScorerUnsupported`] with the original `hardware_counter`.
    pub fn raw_internal_scorer<'a>(
        &'a self,
        point_id: PointOffsetType,
        hardware_counter: HardwareCounterCell,
    ) -> Result<Box<dyn RawScorer + 'a>, InternalScorerUnsupported> {
        fn build<'a, TEncodedVectors: quantization::EncodedVectors>(
            point_id: PointOffsetType,
            quantized_data: &'a TEncodedVectors,
            hardware_counter: HardwareCounterCell,
        ) -> Result<Box<dyn RawScorer + 'a>, InternalScorerUnsupported> {
            let query_scorer =
                QuantizedQueryScorer::new_internal(point_id, quantized_data, hardware_counter)?;
            Ok(Box::new(RawScorerImpl { query_scorer }))
        }

        match &self.storage_impl {
            QuantizedVectorStorage::ScalarRam(storage) => {
                build(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::ScalarMmap(storage) => {
                build(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::PQRam(storage) => build(point_id, storage, hardware_counter),
            QuantizedVectorStorage::PQMmap(storage) => build(point_id, storage, hardware_counter),
            QuantizedVectorStorage::BinaryRam(storage) => {
                build(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::BinaryMmap(storage) => {
                build(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::ScalarRamMulti(storage) => {
                build(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::ScalarMmapMulti(storage) => {
                build(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::PQRamMulti(storage) => {
                build(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::PQMmapMulti(storage) => {
                build(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::BinaryRamMulti(storage) => {
                build(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::BinaryMmapMulti(storage) => {
                build(point_id, storage, hardware_counter)
            }
        }
    }

    pub fn query_scorer_bytes<'a>(
        &'a self,
        query: QueryVector,
        hardware_counter: HardwareCounterCell,
    ) -> OperationResult<Box<dyn QueryScorerBytes + 'a>> {
        QuantizedScorerBuilder::new(
            &self.storage_impl,
            &self.config.quantization_config,
            query,
            &self.distance,
            self.datatype,
            hardware_counter,
        )
        .build_query_scorer_bytes()
    }

    pub fn save_to(&self, path: &Path) -> OperationResult<()> {
        let data_path = path.join(QUANTIZED_DATA_PATH);
        let meta_path = path.join(QUANTIZED_META_PATH);
        let offsets_path = path.join(QUANTIZED_OFFSETS_PATH);
        match &self.storage_impl {
            QuantizedVectorStorage::ScalarRam(storage) => storage.save(&data_path, &meta_path)?,
            QuantizedVectorStorage::ScalarMmap(storage) => storage.save(&data_path, &meta_path)?,
            QuantizedVectorStorage::PQRam(storage) => storage.save(&data_path, &meta_path)?,
            QuantizedVectorStorage::PQMmap(storage) => storage.save(&data_path, &meta_path)?,
            QuantizedVectorStorage::BinaryRam(storage) => storage.save(&data_path, &meta_path)?,
            QuantizedVectorStorage::BinaryMmap(storage) => storage.save(&data_path, &meta_path)?,
            QuantizedVectorStorage::ScalarRamMulti(storage) => {
                storage.save_multi(&data_path, &meta_path, &offsets_path)?
            }
            QuantizedVectorStorage::ScalarMmapMulti(storage) => {
                storage.save_multi(&data_path, &meta_path, &offsets_path)?
            }
            QuantizedVectorStorage::PQRamMulti(storage) => {
                storage.save_multi(&data_path, &meta_path, &offsets_path)?
            }
            QuantizedVectorStorage::PQMmapMulti(storage) => {
                storage.save_multi(&data_path, &meta_path, &offsets_path)?
            }
            QuantizedVectorStorage::BinaryRamMulti(storage) => {
                storage.save_multi(&data_path, &meta_path, &offsets_path)?
            }
            QuantizedVectorStorage::BinaryMmapMulti(storage) => {
                storage.save_multi(&data_path, &meta_path, &offsets_path)?
            }
        };
        Ok(())
    }

    pub fn files(&self) -> Vec<PathBuf> {
        let mut files = vec![
            // Config files
            self.path.join(QUANTIZED_CONFIG_PATH),
            // Storage file
            self.path.join(QUANTIZED_DATA_PATH),
            // Meta file
            self.path.join(QUANTIZED_META_PATH),
        ];
        if self.is_multivector() {
            files.push(self.path.join(QUANTIZED_OFFSETS_PATH));
        }
        files
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        self.files() // quantized vectors are always immutable
    }

    pub fn create(
        vector_storage: &VectorStorageEnum,
        quantization_config: &QuantizationConfig,
        path: &Path,
        max_threads: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<Self> {
        match vector_storage {
            #[cfg(feature = "rocksdb")]
            VectorStorageEnum::DenseSimple(v) => {
                Self::create_impl(v, quantization_config, path, max_threads, stopped)
            }
            #[cfg(feature = "rocksdb")]
            VectorStorageEnum::DenseSimpleByte(v) => {
                Self::create_impl(v, quantization_config, path, max_threads, stopped)
            }
            #[cfg(feature = "rocksdb")]
            VectorStorageEnum::DenseSimpleHalf(v) => {
                Self::create_impl(v, quantization_config, path, max_threads, stopped)
            }
            VectorStorageEnum::DenseVolatile(v) => {
                Self::create_impl(v, quantization_config, path, max_threads, stopped)
            }
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => {
                Self::create_impl(v, quantization_config, path, max_threads, stopped)
            }
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => {
                Self::create_impl(v, quantization_config, path, max_threads, stopped)
            }
            VectorStorageEnum::DenseMemmap(v) => {
                Self::create_impl(v.as_ref(), quantization_config, path, max_threads, stopped)
            }
            VectorStorageEnum::DenseMemmapByte(v) => {
                Self::create_impl(v.as_ref(), quantization_config, path, max_threads, stopped)
            }
            VectorStorageEnum::DenseMemmapHalf(v) => {
                Self::create_impl(v.as_ref(), quantization_config, path, max_threads, stopped)
            }
            VectorStorageEnum::DenseAppendableMemmap(v) => {
                Self::create_impl(v.as_ref(), quantization_config, path, max_threads, stopped)
            }
            VectorStorageEnum::DenseAppendableMemmapByte(v) => {
                Self::create_impl(v.as_ref(), quantization_config, path, max_threads, stopped)
            }
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => {
                Self::create_impl(v.as_ref(), quantization_config, path, max_threads, stopped)
            }
            VectorStorageEnum::DenseAppendableInRam(v) => {
                Self::create_impl(v.as_ref(), quantization_config, path, max_threads, stopped)
            }
            VectorStorageEnum::DenseAppendableInRamByte(v) => {
                Self::create_impl(v.as_ref(), quantization_config, path, max_threads, stopped)
            }
            VectorStorageEnum::DenseAppendableInRamHalf(v) => {
                Self::create_impl(v.as_ref(), quantization_config, path, max_threads, stopped)
            }
            #[cfg(feature = "rocksdb")]
            VectorStorageEnum::SparseSimple(_) => Err(OperationError::WrongSparse),
            VectorStorageEnum::SparseVolatile(_) => Err(OperationError::WrongSparse),
            VectorStorageEnum::SparseMmap(_) => Err(OperationError::WrongSparse),
            #[cfg(feature = "rocksdb")]
            VectorStorageEnum::MultiDenseSimple(v) => {
                Self::create_multi_impl(v, quantization_config, path, max_threads, stopped)
            }
            #[cfg(feature = "rocksdb")]
            VectorStorageEnum::MultiDenseSimpleByte(v) => {
                Self::create_multi_impl(v, quantization_config, path, max_threads, stopped)
            }
            #[cfg(feature = "rocksdb")]
            VectorStorageEnum::MultiDenseSimpleHalf(v) => {
                Self::create_multi_impl(v, quantization_config, path, max_threads, stopped)
            }
            VectorStorageEnum::MultiDenseVolatile(v) => {
                Self::create_multi_impl(v, quantization_config, path, max_threads, stopped)
            }
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(v) => {
                Self::create_multi_impl(v, quantization_config, path, max_threads, stopped)
            }
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(v) => {
                Self::create_multi_impl(v, quantization_config, path, max_threads, stopped)
            }
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => {
                Self::create_multi_impl(v.as_ref(), quantization_config, path, max_threads, stopped)
            }
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => {
                Self::create_multi_impl(v.as_ref(), quantization_config, path, max_threads, stopped)
            }
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => {
                Self::create_multi_impl(v.as_ref(), quantization_config, path, max_threads, stopped)
            }
            VectorStorageEnum::MultiDenseAppendableInRam(v) => {
                Self::create_multi_impl(v.as_ref(), quantization_config, path, max_threads, stopped)
            }
            VectorStorageEnum::MultiDenseAppendableInRamByte(v) => {
                Self::create_multi_impl(v.as_ref(), quantization_config, path, max_threads, stopped)
            }
            VectorStorageEnum::MultiDenseAppendableInRamHalf(v) => {
                Self::create_multi_impl(v.as_ref(), quantization_config, path, max_threads, stopped)
            }
        }
    }

    fn create_impl<
        TElement: PrimitiveVectorElement,
        TVectorStorage: DenseVectorStorage<TElement> + Send + Sync,
    >(
        vector_storage: &TVectorStorage,
        quantization_config: &QuantizationConfig,
        path: &Path,
        max_threads: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<Self> {
        let dim = vector_storage.vector_dim();
        let count = vector_storage.total_vector_count();
        let distance = vector_storage.distance();
        let datatype = vector_storage.datatype();
        let vectors = (0..count as PointOffsetType).map(|i| {
            PrimitiveVectorElement::quantization_preprocess(
                quantization_config,
                distance,
                vector_storage.get_dense_sequential(i),
            )
        });
        let on_disk_vector_storage = vector_storage.is_on_disk();

        let vector_parameters = Self::construct_vector_parameters(distance, dim, count);

        let quantized_storage = match quantization_config {
            QuantizationConfig::Scalar(ScalarQuantization {
                scalar: scalar_config,
            }) => Self::create_scalar(
                vectors,
                &vector_parameters,
                count,
                scalar_config,
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
                path,
                on_disk_vector_storage,
                stopped,
            )?,
        };

        let quantized_vectors_config = QuantizedVectorsConfig {
            quantization_config: quantization_config.clone(),
            vector_parameters,
        };

        let quantized_vectors = QuantizedVectors {
            storage_impl: quantized_storage,
            config: quantized_vectors_config,
            path: path.to_path_buf(),
            distance,
            datatype,
        };

        quantized_vectors.save_to(path)?;
        atomic_save_json(&path.join(QUANTIZED_CONFIG_PATH), &quantized_vectors.config)?;
        Ok(quantized_vectors)
    }

    fn create_multi_impl<
        TElement: PrimitiveVectorElement + 'static,
        TVectorStorage: MultiVectorStorage<TElement> + Send + Sync,
    >(
        vector_storage: &TVectorStorage,
        quantization_config: &QuantizationConfig,
        path: &Path,
        max_threads: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<Self> {
        let dim = vector_storage.vector_dim();
        let distance = vector_storage.distance();
        let datatype = vector_storage.datatype();
        let multi_vector_config = *vector_storage.multi_vector_config();
        let vectors = vector_storage.iterate_inner_vectors().map(|v| {
            PrimitiveVectorElement::quantization_preprocess(quantization_config, distance, v)
        });
        let inner_vectors_count = vectors.clone().count();
        let vectors_count = vector_storage.total_vector_count();
        let on_disk_vector_storage = vector_storage.is_on_disk();

        let vector_parameters =
            Self::construct_vector_parameters(distance, dim, inner_vectors_count);

        let offsets = (0..vectors_count as PointOffsetType)
            .map(|idx| vector_storage.get_multi(idx).vectors_count() as PointOffsetType)
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
                multi_vector_config,
                path,
                on_disk_vector_storage,
                stopped,
            )?,
        };

        let quantized_vectors_config = QuantizedVectorsConfig {
            quantization_config: quantization_config.clone(),
            vector_parameters,
        };

        let quantized_vectors = QuantizedVectors {
            storage_impl: quantized_storage,
            config: quantized_vectors_config,
            path: path.to_path_buf(),
            distance,
            datatype,
        };

        quantized_vectors.save_to(path)?;
        atomic_save_json(&path.join(QUANTIZED_CONFIG_PATH), &quantized_vectors.config)?;
        Ok(quantized_vectors)
    }

    pub fn config_exists(path: &Path) -> bool {
        path.join(QUANTIZED_CONFIG_PATH).exists()
    }

    pub fn load(vector_storage: &VectorStorageEnum, path: &Path) -> OperationResult<Self> {
        let on_disk_vector_storage = vector_storage.is_on_disk();
        let distance = vector_storage.distance();
        let datatype = vector_storage.datatype();

        let data_path = path.join(QUANTIZED_DATA_PATH);
        let meta_path = path.join(QUANTIZED_META_PATH);
        let config_path = path.join(QUANTIZED_CONFIG_PATH);
        let config: QuantizedVectorsConfig = read_json(&config_path)?;
        let quantized_store = if let Some(multivector_config) =
            vector_storage.try_multi_vector_config()
        {
            let offsets_path = path.join(QUANTIZED_OFFSETS_PATH);
            match &config.quantization_config {
                QuantizationConfig::Scalar(ScalarQuantization { scalar }) => {
                    if Self::is_ram(scalar.always_ram, on_disk_vector_storage) {
                        QuantizedVectorStorage::ScalarRamMulti(
                            QuantizedMultivectorStorage::load_multi(
                                &data_path,
                                &meta_path,
                                &offsets_path,
                                &config.vector_parameters,
                                multivector_config,
                            )?,
                        )
                    } else {
                        QuantizedVectorStorage::ScalarMmapMulti(
                            QuantizedMultivectorStorage::load_multi(
                                &data_path,
                                &meta_path,
                                &offsets_path,
                                &config.vector_parameters,
                                multivector_config,
                            )?,
                        )
                    }
                }
                QuantizationConfig::Product(ProductQuantization { product: pq }) => {
                    if Self::is_ram(pq.always_ram, on_disk_vector_storage) {
                        QuantizedVectorStorage::PQRamMulti(QuantizedMultivectorStorage::load_multi(
                            &data_path,
                            &meta_path,
                            &offsets_path,
                            &config.vector_parameters,
                            multivector_config,
                        )?)
                    } else {
                        QuantizedVectorStorage::PQMmapMulti(
                            QuantizedMultivectorStorage::load_multi(
                                &data_path,
                                &meta_path,
                                &offsets_path,
                                &config.vector_parameters,
                                multivector_config,
                            )?,
                        )
                    }
                }
                QuantizationConfig::Binary(BinaryQuantization { binary }) => {
                    if Self::is_ram(binary.always_ram, on_disk_vector_storage) {
                        QuantizedVectorStorage::BinaryRamMulti(
                            QuantizedMultivectorStorage::load_multi(
                                &data_path,
                                &meta_path,
                                &offsets_path,
                                &config.vector_parameters,
                                multivector_config,
                            )?,
                        )
                    } else {
                        QuantizedVectorStorage::BinaryMmapMulti(
                            QuantizedMultivectorStorage::load_multi(
                                &data_path,
                                &meta_path,
                                &offsets_path,
                                &config.vector_parameters,
                                multivector_config,
                            )?,
                        )
                    }
                }
            }
        } else {
            match &config.quantization_config {
                QuantizationConfig::Scalar(ScalarQuantization { scalar }) => {
                    if Self::is_ram(scalar.always_ram, on_disk_vector_storage) {
                        QuantizedVectorStorage::ScalarRam(EncodedVectorsU8::load(
                            &data_path,
                            &meta_path,
                            &config.vector_parameters,
                        )?)
                    } else {
                        QuantizedVectorStorage::ScalarMmap(EncodedVectorsU8::load(
                            &data_path,
                            &meta_path,
                            &config.vector_parameters,
                        )?)
                    }
                }
                QuantizationConfig::Product(ProductQuantization { product: pq }) => {
                    if Self::is_ram(pq.always_ram, on_disk_vector_storage) {
                        QuantizedVectorStorage::PQRam(EncodedVectorsPQ::load(
                            &data_path,
                            &meta_path,
                            &config.vector_parameters,
                        )?)
                    } else {
                        QuantizedVectorStorage::PQMmap(EncodedVectorsPQ::load(
                            &data_path,
                            &meta_path,
                            &config.vector_parameters,
                        )?)
                    }
                }
                QuantizationConfig::Binary(BinaryQuantization { binary }) => {
                    if Self::is_ram(binary.always_ram, on_disk_vector_storage) {
                        QuantizedVectorStorage::BinaryRam(EncodedVectorsBin::load(
                            &data_path,
                            &meta_path,
                            &config.vector_parameters,
                        )?)
                    } else {
                        QuantizedVectorStorage::BinaryMmap(EncodedVectorsBin::load(
                            &data_path,
                            &meta_path,
                            &config.vector_parameters,
                        )?)
                    }
                }
            }
        };

        Ok(QuantizedVectors {
            storage_impl: quantized_store,
            config,
            path: path.to_path_buf(),
            distance,
            datatype,
        })
    }

    fn create_scalar<'a>(
        vectors: impl Iterator<Item = impl AsRef<[VectorElementType]> + 'a> + Clone,
        vector_parameters: &quantization::VectorParameters,
        vectors_count: usize,
        scalar_config: &ScalarQuantizationConfig,
        path: &Path,
        on_disk_vector_storage: bool,
        stopped: &AtomicBool,
    ) -> OperationResult<QuantizedVectorStorage> {
        let quantized_vector_size =
            EncodedVectorsU8::<QuantizedMmapStorage>::get_quantized_vector_size(vector_parameters);
        let in_ram = Self::is_ram(scalar_config.always_ram, on_disk_vector_storage);
        if in_ram {
            let storage_builder =
                QuantizedRamStorageBuilder::new(vectors_count, quantized_vector_size)?;
            Ok(QuantizedVectorStorage::ScalarRam(EncodedVectorsU8::encode(
                vectors,
                storage_builder,
                vector_parameters,
                vectors_count,
                scalar_config.quantile,
                stopped,
            )?))
        } else {
            let mmap_data_path = path.join(QUANTIZED_DATA_PATH);
            let storage_builder = QuantizedMmapStorageBuilder::new(
                mmap_data_path.as_path(),
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
                    stopped,
                )?,
            ))
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn create_scalar_multi<'a>(
        vectors: impl Iterator<Item = impl AsRef<[VectorElementType]> + 'a> + Clone,
        offsets: impl Iterator<Item = MultivectorOffset>,
        vector_parameters: &quantization::VectorParameters,
        vectors_count: usize,
        inner_vectors_count: usize,
        scalar_config: &ScalarQuantizationConfig,
        multi_vector_config: MultiVectorConfig,
        path: &Path,
        on_disk_vector_storage: bool,
        stopped: &AtomicBool,
    ) -> OperationResult<QuantizedVectorStorage> {
        let quantized_vector_size =
            EncodedVectorsU8::<QuantizedMmapStorage>::get_quantized_vector_size(vector_parameters);
        let in_ram = Self::is_ram(scalar_config.always_ram, on_disk_vector_storage);
        if in_ram {
            let storage_builder =
                QuantizedRamStorageBuilder::new(inner_vectors_count, quantized_vector_size)?;
            let quantized_storage = EncodedVectorsU8::encode(
                vectors,
                storage_builder,
                vector_parameters,
                inner_vectors_count,
                scalar_config.quantile,
                stopped,
            )?;
            Ok(QuantizedVectorStorage::ScalarRamMulti(
                QuantizedMultivectorStorage::new(
                    vector_parameters.dim,
                    quantized_storage,
                    offsets.collect(),
                    multi_vector_config,
                ),
            ))
        } else {
            let mmap_data_path = path.join(QUANTIZED_DATA_PATH);
            let storage_builder = QuantizedMmapStorageBuilder::new(
                mmap_data_path.as_path(),
                inner_vectors_count,
                quantized_vector_size,
            )?;
            let quantized_storage = EncodedVectorsU8::encode(
                vectors,
                storage_builder,
                vector_parameters,
                inner_vectors_count,
                scalar_config.quantile,
                stopped,
            )?;
            let offsets_path = path.join(QUANTIZED_OFFSETS_PATH);
            create_offsets_file_from_iter(&offsets_path, vectors_count, offsets)?;
            Ok(QuantizedVectorStorage::ScalarMmapMulti(
                QuantizedMultivectorStorage::new(
                    vector_parameters.dim,
                    quantized_storage,
                    MultivectorOffsetsStorage::load(&offsets_path)?,
                    multi_vector_config,
                ),
            ))
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn create_pq<'a>(
        vectors: impl Iterator<Item = impl AsRef<[VectorElementType]> + 'a> + Clone + Send,
        vector_parameters: &quantization::VectorParameters,
        vectors_count: usize,
        pq_config: &ProductQuantizationConfig,
        path: &Path,
        on_disk_vector_storage: bool,
        max_threads: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<QuantizedVectorStorage> {
        let bucket_size = Self::get_bucket_size(pq_config.compression);
        let quantized_vector_size =
            EncodedVectorsPQ::<QuantizedMmapStorage>::get_quantized_vector_size(
                vector_parameters,
                bucket_size,
            );
        let in_ram = Self::is_ram(pq_config.always_ram, on_disk_vector_storage);
        if in_ram {
            let storage_builder =
                QuantizedRamStorageBuilder::new(vectors_count, quantized_vector_size)?;
            Ok(QuantizedVectorStorage::PQRam(EncodedVectorsPQ::encode(
                vectors,
                storage_builder,
                vector_parameters,
                vectors_count,
                bucket_size,
                max_threads,
                stopped,
            )?))
        } else {
            let mmap_data_path = path.join(QUANTIZED_DATA_PATH);
            let storage_builder = QuantizedMmapStorageBuilder::new(
                mmap_data_path.as_path(),
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
                stopped,
            )?))
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn create_pq_multi<'a>(
        vectors: impl Iterator<Item = impl AsRef<[VectorElementType]> + 'a> + Clone + Send,
        offsets: impl Iterator<Item = MultivectorOffset>,
        vector_parameters: &quantization::VectorParameters,
        vectors_count: usize,
        inner_vectors_count: usize,
        pq_config: &ProductQuantizationConfig,
        multi_vector_config: MultiVectorConfig,
        path: &Path,
        on_disk_vector_storage: bool,
        max_threads: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<QuantizedVectorStorage> {
        let bucket_size = Self::get_bucket_size(pq_config.compression);
        let quantized_vector_size =
            EncodedVectorsPQ::<QuantizedMmapStorage>::get_quantized_vector_size(
                vector_parameters,
                bucket_size,
            );
        let in_ram = Self::is_ram(pq_config.always_ram, on_disk_vector_storage);
        if in_ram {
            let storage_builder =
                QuantizedRamStorageBuilder::new(inner_vectors_count, quantized_vector_size)?;
            let quantized_storage = EncodedVectorsPQ::encode(
                vectors,
                storage_builder,
                vector_parameters,
                inner_vectors_count,
                bucket_size,
                max_threads,
                stopped,
            )?;
            Ok(QuantizedVectorStorage::PQRamMulti(
                QuantizedMultivectorStorage::new(
                    vector_parameters.dim,
                    quantized_storage,
                    offsets.collect(),
                    multi_vector_config,
                ),
            ))
        } else {
            let mmap_data_path = path.join(QUANTIZED_DATA_PATH);
            let storage_builder = QuantizedMmapStorageBuilder::new(
                mmap_data_path.as_path(),
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
                stopped,
            )?;
            let offsets_path = path.join(QUANTIZED_OFFSETS_PATH);
            create_offsets_file_from_iter(&offsets_path, vectors_count, offsets)?;
            Ok(QuantizedVectorStorage::PQMmapMulti(
                QuantizedMultivectorStorage::new(
                    vector_parameters.dim,
                    quantized_storage,
                    MultivectorOffsetsStorage::load(&offsets_path)?,
                    multi_vector_config,
                ),
            ))
        }
    }

    fn create_binary<'a>(
        vectors: impl Iterator<Item = impl AsRef<[VectorElementType]> + 'a> + Clone,
        vector_parameters: &quantization::VectorParameters,
        vectors_count: usize,
        binary_config: &BinaryQuantizationConfig,
        path: &Path,
        on_disk_vector_storage: bool,
        stopped: &AtomicBool,
    ) -> OperationResult<QuantizedVectorStorage> {
        let encoding = match binary_config.encoding {
            Some(BinaryQuantizationEncoding::OneBit) => {
                quantization::encoded_vectors_binary::Encoding::OneBit
            }
            Some(BinaryQuantizationEncoding::TwoBits) => {
                quantization::encoded_vectors_binary::Encoding::TwoBits
            }
            Some(BinaryQuantizationEncoding::OneAndHalfBits) => {
                quantization::encoded_vectors_binary::Encoding::OneAndHalfBits
            }
            None => quantization::encoded_vectors_binary::Encoding::OneBit,
        };
        let query_encoding = match binary_config.query_encoding {
            Some(BinaryQuantizationQueryEncoding::Scalar4Bits) => {
                quantization::encoded_vectors_binary::QueryEncoding::Scalar4bits
            }
            Some(BinaryQuantizationQueryEncoding::Scalar8Bits) => {
                quantization::encoded_vectors_binary::QueryEncoding::Scalar8bits
            }
            Some(BinaryQuantizationQueryEncoding::Binary) => {
                quantization::encoded_vectors_binary::QueryEncoding::SameAsStorage
            }
            Some(BinaryQuantizationQueryEncoding::Default) => {
                quantization::encoded_vectors_binary::QueryEncoding::SameAsStorage
            }
            None => quantization::encoded_vectors_binary::QueryEncoding::SameAsStorage,
        };
        let quantized_vector_size =
            EncodedVectorsBin::<u128, QuantizedMmapStorage>::get_quantized_vector_size_from_params(
                vector_parameters.dim,
                encoding,
            );
        let in_ram = Self::is_ram(binary_config.always_ram, on_disk_vector_storage);
        if in_ram {
            let storage_builder =
                QuantizedRamStorageBuilder::new(vectors_count, quantized_vector_size)?;
            Ok(QuantizedVectorStorage::BinaryRam(
                EncodedVectorsBin::encode(
                    vectors,
                    storage_builder,
                    vector_parameters,
                    encoding,
                    query_encoding,
                    stopped,
                )?,
            ))
        } else {
            let mmap_data_path = path.join(QUANTIZED_DATA_PATH);
            let storage_builder = QuantizedMmapStorageBuilder::new(
                mmap_data_path.as_path(),
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
                    stopped,
                )?,
            ))
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn create_binary_multi<'a>(
        vectors: impl Iterator<Item = impl AsRef<[VectorElementType]> + 'a> + Clone,
        offsets: impl Iterator<Item = MultivectorOffset>,
        vector_parameters: &quantization::VectorParameters,
        vectors_count: usize,
        inner_vectors_count: usize,
        binary_config: &BinaryQuantizationConfig,
        multi_vector_config: MultiVectorConfig,
        path: &Path,
        on_disk_vector_storage: bool,
        stopped: &AtomicBool,
    ) -> OperationResult<QuantizedVectorStorage> {
        let encoding = match binary_config.encoding {
            Some(BinaryQuantizationEncoding::OneBit) => {
                quantization::encoded_vectors_binary::Encoding::OneBit
            }
            Some(BinaryQuantizationEncoding::TwoBits) => {
                quantization::encoded_vectors_binary::Encoding::TwoBits
            }
            Some(BinaryQuantizationEncoding::OneAndHalfBits) => {
                quantization::encoded_vectors_binary::Encoding::OneAndHalfBits
            }
            None => quantization::encoded_vectors_binary::Encoding::OneBit,
        };
        let query_encoding = match binary_config.query_encoding {
            Some(BinaryQuantizationQueryEncoding::Scalar4Bits) => {
                quantization::encoded_vectors_binary::QueryEncoding::Scalar4bits
            }
            Some(BinaryQuantizationQueryEncoding::Scalar8Bits) => {
                quantization::encoded_vectors_binary::QueryEncoding::Scalar8bits
            }
            Some(BinaryQuantizationQueryEncoding::Binary) => {
                quantization::encoded_vectors_binary::QueryEncoding::SameAsStorage
            }
            Some(BinaryQuantizationQueryEncoding::Default) => {
                quantization::encoded_vectors_binary::QueryEncoding::SameAsStorage
            }
            None => quantization::encoded_vectors_binary::QueryEncoding::SameAsStorage,
        };
        let quantized_vector_size =
            EncodedVectorsBin::<u8, QuantizedMmapStorage>::get_quantized_vector_size_from_params(
                vector_parameters.dim,
                encoding,
            );
        let in_ram = Self::is_ram(binary_config.always_ram, on_disk_vector_storage);
        if in_ram {
            let storage_builder =
                QuantizedRamStorageBuilder::new(inner_vectors_count, quantized_vector_size)?;
            let quantized_storage = EncodedVectorsBin::encode(
                vectors,
                storage_builder,
                vector_parameters,
                encoding,
                query_encoding,
                stopped,
            )?;
            Ok(QuantizedVectorStorage::BinaryRamMulti(
                QuantizedMultivectorStorage::new(
                    vector_parameters.dim,
                    quantized_storage,
                    offsets.collect(),
                    multi_vector_config,
                ),
            ))
        } else {
            let mmap_data_path = path.join(QUANTIZED_DATA_PATH);
            let storage_builder = QuantizedMmapStorageBuilder::new(
                mmap_data_path.as_path(),
                inner_vectors_count,
                quantized_vector_size,
            )?;
            let quantized_storage = EncodedVectorsBin::encode(
                vectors,
                storage_builder,
                vector_parameters,
                encoding,
                query_encoding,
                stopped,
            )?;
            let offsets_path = path.join(QUANTIZED_OFFSETS_PATH);
            create_offsets_file_from_iter(&offsets_path, vectors_count, offsets)?;
            Ok(QuantizedVectorStorage::BinaryMmapMulti(
                QuantizedMultivectorStorage::new(
                    vector_parameters.dim,
                    quantized_storage,
                    MultivectorOffsetsStorage::load(&offsets_path)?,
                    multi_vector_config,
                ),
            ))
        }
    }

    fn is_ram(always_ram: Option<bool>, on_disk_vector_storage: bool) -> bool {
        !on_disk_vector_storage || always_ram == Some(true)
    }

    fn construct_vector_parameters(
        distance: Distance,
        dim: usize,
        count: usize,
    ) -> quantization::VectorParameters {
        quantization::VectorParameters {
            dim,
            deprecated_count: Some(count),
            distance_type: match distance {
                Distance::Cosine => quantization::DistanceType::Dot,
                Distance::Euclid => quantization::DistanceType::L2,
                Distance::Dot => quantization::DistanceType::Dot,
                Distance::Manhattan => quantization::DistanceType::L1,
            },
            invert: distance == Distance::Euclid || distance == Distance::Manhattan,
        }
    }

    fn get_bucket_size(compression: CompressionRatio) -> usize {
        match compression {
            CompressionRatio::X4 => 1,
            CompressionRatio::X8 => 2,
            CompressionRatio::X16 => 4,
            CompressionRatio::X32 => 8,
            CompressionRatio::X64 => 16,
        }
    }

    pub fn get_storage(&self) -> &QuantizedVectorStorage {
        &self.storage_impl
    }

    pub fn populate(&self) -> OperationResult<()> {
        match &self.storage_impl {
            QuantizedVectorStorage::ScalarRam(_) => {} // not mmap
            QuantizedVectorStorage::ScalarMmap(storage) => storage.storage().populate(),
            QuantizedVectorStorage::PQRam(_) => {}
            QuantizedVectorStorage::PQMmap(storage) => storage.storage().populate(),
            QuantizedVectorStorage::BinaryRam(_) => {}
            QuantizedVectorStorage::BinaryMmap(storage) => storage.storage().populate(),
            QuantizedVectorStorage::ScalarRamMulti(_) => {}
            QuantizedVectorStorage::ScalarMmapMulti(storage) => {
                storage.storage().storage().populate()
            }
            QuantizedVectorStorage::PQRamMulti(_) => {}
            QuantizedVectorStorage::PQMmapMulti(storage) => storage.storage().storage().populate(),
            QuantizedVectorStorage::BinaryRamMulti(_) => {}
            QuantizedVectorStorage::BinaryMmapMulti(storage) => {
                storage.storage().storage().populate()
            }
        }
        Ok(())
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        for file in self.files() {
            clear_disk_cache(&file)?;
        }
        Ok(())
    }

    pub fn flusher(&self) -> Flusher {
        let flusher = match &self.storage_impl {
            QuantizedVectorStorage::ScalarRam(q) => q.flusher(),
            QuantizedVectorStorage::ScalarMmap(q) => q.flusher(),
            QuantizedVectorStorage::PQRam(q) => q.flusher(),
            QuantizedVectorStorage::PQMmap(q) => q.flusher(),
            QuantizedVectorStorage::BinaryRam(q) => q.flusher(),
            QuantizedVectorStorage::BinaryMmap(q) => q.flusher(),
            QuantizedVectorStorage::ScalarRamMulti(q) => q.flusher(),
            QuantizedVectorStorage::ScalarMmapMulti(q) => q.flusher(),
            QuantizedVectorStorage::PQRamMulti(q) => q.flusher(),
            QuantizedVectorStorage::PQMmapMulti(q) => q.flusher(),
            QuantizedVectorStorage::BinaryRamMulti(q) => q.flusher(),
            QuantizedVectorStorage::BinaryMmapMulti(q) => q.flusher(),
        };
        Box::new(move || flusher().map_err(OperationError::from))
    }
}
