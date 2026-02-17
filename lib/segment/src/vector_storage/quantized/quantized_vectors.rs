use std::alloc::Layout;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::fs::{atomic_save_json, clear_disk_cache, read_json};
use common::types::PointOffsetType;
use quantization::encoded_vectors_binary::EncodedVectorsBin;
use quantization::encoded_vectors_u8::ScalarQuantizationMethod;
use quantization::{EncodedVectors, EncodedVectorsPQ, EncodedVectorsU8};
use serde::{Deserialize, Serialize};

use super::quantized_multivector_storage::{
    MultivectorOffset, MultivectorOffsetsStorageMmap, QuantizedMultivectorStorage,
};
use super::quantized_scorer_builder::QuantizedScorerBuilder;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{QueryVector, VectorElementType, VectorRef};
use crate::types::{
    BinaryQuantization, BinaryQuantizationConfig, BinaryQuantizationEncoding,
    BinaryQuantizationQueryEncoding, CompressionRatio, Distance, MultiVectorConfig,
    ProductQuantization, ProductQuantizationConfig, QuantizationConfig, ScalarQuantization,
    ScalarQuantizationConfig, ScalarType, VectorStorageDatatype,
};
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::{
    QuantizedChunkedMmapStorage, QuantizedChunkedMmapStorageBuilder,
};
use crate::vector_storage::quantized::quantized_mmap_storage::{
    QuantizedMmapStorage, QuantizedMmapStorageBuilder,
};
use crate::vector_storage::quantized::quantized_multivector_storage::{
    MultivectorOffsetsStorageChunkedMmap, MultivectorOffsetsStorageRam,
};
use crate::vector_storage::quantized::quantized_query_scorer::{
    InternalScorerUnsupported, QuantizedQueryScorer,
};
use crate::vector_storage::quantized::quantized_ram_storage::{
    QuantizedRamStorage, QuantizedRamStorageBuilder,
};
use crate::vector_storage::{
    DenseVectorStorage, MultiVectorStorage, Random, RawScorer, RawScorerImpl, Sequential,
    VectorStorage, VectorStorageEnum,
};

pub const QUANTIZED_CONFIG_PATH: &str = "quantized.config.json";
pub const QUANTIZED_DATA_PATH: &str = "quantized.data";
pub const QUANTIZED_APPENDABLE_DATA_PATH: &str = "quantized_data";
pub const QUANTIZED_META_PATH: &str = "quantized.meta.json";
pub const QUANTIZED_OFFSETS_PATH: &str = "quantized.offsets.data";
pub const QUANTIZED_APPENDABLE_OFFSETS_PATH: &str = "quantized_offsets_data";

#[derive(Deserialize, Serialize, Clone)]
pub struct QuantizedVectorsConfig {
    pub quantization_config: QuantizationConfig,
    pub vector_parameters: quantization::VectorParameters,
    #[serde(default)]
    #[serde(skip_serializing_if = "QuantizedVectorsStorageType::is_immutable")]
    pub storage_type: QuantizedVectorsStorageType,
}

impl fmt::Debug for QuantizedVectorsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuantizedVectorsConfig")
            .field("quantization_config", &self.quantization_config)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize, Serialize, Clone, Copy, Debug, Eq, PartialEq, Default)]
pub enum QuantizedVectorsStorageType {
    #[default]
    Immutable,
    Mutable,
}

impl QuantizedVectorsStorageType {
    pub fn is_immutable(&self) -> bool {
        matches!(self, QuantizedVectorsStorageType::Immutable)
    }
}

type ScalarRamMulti = QuantizedMultivectorStorage<
    EncodedVectorsU8<QuantizedRamStorage>,
    MultivectorOffsetsStorageRam,
>;
type ScalarMmapMulti = QuantizedMultivectorStorage<
    EncodedVectorsU8<QuantizedMmapStorage>,
    MultivectorOffsetsStorageMmap,
>;

type ScalarChunkedMmapMulti = QuantizedMultivectorStorage<
    EncodedVectorsU8<QuantizedChunkedMmapStorage>,
    MultivectorOffsetsStorageChunkedMmap,
>;

type PQRamMulti = QuantizedMultivectorStorage<
    EncodedVectorsPQ<QuantizedRamStorage>,
    MultivectorOffsetsStorageRam,
>;
type PQMmapMulti = QuantizedMultivectorStorage<
    EncodedVectorsPQ<QuantizedMmapStorage>,
    MultivectorOffsetsStorageMmap,
>;

type PQChunkedMmapMulti = QuantizedMultivectorStorage<
    EncodedVectorsPQ<QuantizedChunkedMmapStorage>,
    MultivectorOffsetsStorageChunkedMmap,
>;

type BinaryRamMulti = QuantizedMultivectorStorage<
    EncodedVectorsBin<u8, QuantizedRamStorage>,
    MultivectorOffsetsStorageRam,
>;
type BinaryMmapMulti = QuantizedMultivectorStorage<
    EncodedVectorsBin<u8, QuantizedMmapStorage>,
    MultivectorOffsetsStorageMmap,
>;

type BinaryChunkedMmapMulti = QuantizedMultivectorStorage<
    EncodedVectorsBin<u8, QuantizedChunkedMmapStorage>,
    MultivectorOffsetsStorageChunkedMmap,
>;

pub enum QuantizedVectorStorage {
    ScalarRam(EncodedVectorsU8<QuantizedRamStorage>),
    ScalarMmap(EncodedVectorsU8<QuantizedMmapStorage>),
    ScalarChunkedMmap(EncodedVectorsU8<QuantizedChunkedMmapStorage>),
    PQRam(EncodedVectorsPQ<QuantizedRamStorage>),
    PQMmap(EncodedVectorsPQ<QuantizedMmapStorage>),
    PQChunkedMmap(EncodedVectorsPQ<QuantizedChunkedMmapStorage>),
    BinaryRam(EncodedVectorsBin<u128, QuantizedRamStorage>),
    BinaryMmap(EncodedVectorsBin<u128, QuantizedMmapStorage>),
    BinaryChunkedMmap(EncodedVectorsBin<u128, QuantizedChunkedMmapStorage>),
    ScalarRamMulti(ScalarRamMulti),
    ScalarMmapMulti(ScalarMmapMulti),
    ScalarChunkedMmapMulti(ScalarChunkedMmapMulti),
    PQRamMulti(PQRamMulti),
    PQMmapMulti(PQMmapMulti),
    PQChunkedMmapMulti(PQChunkedMmapMulti),
    BinaryRamMulti(BinaryRamMulti),
    BinaryMmapMulti(BinaryMmapMulti),
    BinaryChunkedMmapMulti(BinaryChunkedMmapMulti),
}

impl QuantizedVectorStorage {
    pub fn is_on_disk(&self) -> bool {
        match self {
            QuantizedVectorStorage::ScalarRam(q) => q.is_on_disk(),
            QuantizedVectorStorage::ScalarMmap(q) => q.is_on_disk(),
            QuantizedVectorStorage::ScalarChunkedMmap(q) => q.is_on_disk(),
            QuantizedVectorStorage::PQRam(q) => q.is_on_disk(),
            QuantizedVectorStorage::PQMmap(q) => q.is_on_disk(),
            QuantizedVectorStorage::PQChunkedMmap(q) => q.is_on_disk(),
            QuantizedVectorStorage::BinaryRam(q) => q.is_on_disk(),
            QuantizedVectorStorage::BinaryMmap(q) => q.is_on_disk(),
            QuantizedVectorStorage::BinaryChunkedMmap(q) => q.is_on_disk(),
            QuantizedVectorStorage::ScalarRamMulti(q) => q.is_on_disk(),
            QuantizedVectorStorage::ScalarMmapMulti(q) => q.is_on_disk(),
            QuantizedVectorStorage::ScalarChunkedMmapMulti(q) => q.is_on_disk(),
            QuantizedVectorStorage::PQRamMulti(q) => q.is_on_disk(),
            QuantizedVectorStorage::PQMmapMulti(q) => q.is_on_disk(),
            QuantizedVectorStorage::PQChunkedMmapMulti(q) => q.is_on_disk(),
            QuantizedVectorStorage::BinaryRamMulti(q) => q.is_on_disk(),
            QuantizedVectorStorage::BinaryMmapMulti(q) => q.is_on_disk(),
            QuantizedVectorStorage::BinaryChunkedMmapMulti(q) => q.is_on_disk(),
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
        match self.storage_impl {
            QuantizedVectorStorage::ScalarRam(_) => false,
            QuantizedVectorStorage::ScalarMmap(_) => false,
            QuantizedVectorStorage::ScalarChunkedMmap(_) => false,
            QuantizedVectorStorage::PQRam(_) => false,
            QuantizedVectorStorage::PQMmap(_) => false,
            QuantizedVectorStorage::PQChunkedMmap(_) => false,
            QuantizedVectorStorage::BinaryRam(_) => true,
            QuantizedVectorStorage::BinaryMmap(_) => true,
            QuantizedVectorStorage::BinaryChunkedMmap(_) => true,
            QuantizedVectorStorage::ScalarRamMulti(_) => false,
            QuantizedVectorStorage::ScalarMmapMulti(_) => false,
            QuantizedVectorStorage::ScalarChunkedMmapMulti(_) => false,
            QuantizedVectorStorage::PQRamMulti(_) => false,
            QuantizedVectorStorage::PQMmapMulti(_) => false,
            QuantizedVectorStorage::PQChunkedMmapMulti(_) => false,
            QuantizedVectorStorage::BinaryRamMulti(_) => true,
            QuantizedVectorStorage::BinaryMmapMulti(_) => true,
            QuantizedVectorStorage::BinaryChunkedMmapMulti(_) => true,
        }
    }

    pub fn is_multivector(&self) -> bool {
        match self.storage_impl {
            QuantizedVectorStorage::ScalarRam(_) => false,
            QuantizedVectorStorage::ScalarMmap(_) => false,
            QuantizedVectorStorage::ScalarChunkedMmap(_) => false,
            QuantizedVectorStorage::PQRam(_) => false,
            QuantizedVectorStorage::PQMmap(_) => false,
            QuantizedVectorStorage::PQChunkedMmap(_) => false,
            QuantizedVectorStorage::BinaryRam(_) => false,
            QuantizedVectorStorage::BinaryMmap(_) => false,
            QuantizedVectorStorage::BinaryChunkedMmap(_) => false,
            QuantizedVectorStorage::ScalarRamMulti(_) => true,
            QuantizedVectorStorage::ScalarMmapMulti(_) => true,
            QuantizedVectorStorage::ScalarChunkedMmapMulti(_) => true,
            QuantizedVectorStorage::PQRamMulti(_) => true,
            QuantizedVectorStorage::PQMmapMulti(_) => true,
            QuantizedVectorStorage::PQChunkedMmapMulti(_) => true,
            QuantizedVectorStorage::BinaryRamMulti(_) => true,
            QuantizedVectorStorage::BinaryMmapMulti(_) => true,
            QuantizedVectorStorage::BinaryChunkedMmapMulti(_) => true,
        }
    }

    /// Get layout for a single quantized vector.
    ///
    /// I.e. the size of a single vector in bytes, and the required alignment.
    pub fn get_quantized_vector_layout(&self) -> OperationResult<Layout> {
        match &self.storage_impl {
            QuantizedVectorStorage::ScalarRam(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::ScalarMmap(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::ScalarChunkedMmap(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::PQRam(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::PQMmap(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::PQChunkedMmap(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::BinaryRam(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::BinaryMmap(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::BinaryChunkedMmap(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::ScalarRamMulti(_)
            | QuantizedVectorStorage::ScalarMmapMulti(_)
            | QuantizedVectorStorage::ScalarChunkedMmapMulti(_)
            | QuantizedVectorStorage::PQRamMulti(_)
            | QuantizedVectorStorage::PQMmapMulti(_)
            | QuantizedVectorStorage::PQChunkedMmapMulti(_)
            | QuantizedVectorStorage::BinaryRamMulti(_)
            | QuantizedVectorStorage::BinaryMmapMulti(_)
            | QuantizedVectorStorage::BinaryChunkedMmapMulti(_) => {
                Err(OperationError::service_error(
                    "Cannot get quantized vector layout from multivector storage",
                ))
            }
        }
    }

    pub fn get_quantized_vector(&self, id: PointOffsetType) -> &[u8] {
        match &self.storage_impl {
            QuantizedVectorStorage::ScalarRam(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::ScalarMmap(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::ScalarChunkedMmap(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::PQRam(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::PQMmap(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::PQChunkedMmap(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::BinaryRam(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::BinaryMmap(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::BinaryChunkedMmap(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::ScalarRamMulti(_)
            | QuantizedVectorStorage::ScalarMmapMulti(_)
            | QuantizedVectorStorage::ScalarChunkedMmapMulti(_)
            | QuantizedVectorStorage::PQRamMulti(_)
            | QuantizedVectorStorage::PQMmapMulti(_)
            | QuantizedVectorStorage::PQChunkedMmapMulti(_)
            | QuantizedVectorStorage::BinaryRamMulti(_)
            | QuantizedVectorStorage::BinaryMmapMulti(_)
            | QuantizedVectorStorage::BinaryChunkedMmapMulti(_) => {
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
        .build()
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
            QuantizedVectorStorage::ScalarChunkedMmap(storage) => {
                build(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::PQRam(storage) => build(point_id, storage, hardware_counter),
            QuantizedVectorStorage::PQMmap(storage) => build(point_id, storage, hardware_counter),
            QuantizedVectorStorage::PQChunkedMmap(storage) => {
                build(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::BinaryRam(storage) => {
                build(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::BinaryMmap(storage) => {
                build(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::BinaryChunkedMmap(storage) => {
                build(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::ScalarRamMulti(storage) => {
                build(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::ScalarMmapMulti(storage) => {
                build(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::ScalarChunkedMmapMulti(storage) => {
                build(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::PQRamMulti(storage) => {
                build(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::PQMmapMulti(storage) => {
                build(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::PQChunkedMmapMulti(storage) => {
                build(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::BinaryRamMulti(storage) => {
                build(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::BinaryMmapMulti(storage) => {
                build(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::BinaryChunkedMmapMulti(storage) => {
                build(point_id, storage, hardware_counter)
            }
        }
    }

    fn get_config_path(path: &Path) -> PathBuf {
        path.join(QUANTIZED_CONFIG_PATH)
    }

    fn get_data_path(path: &Path, storage_type: QuantizedVectorsStorageType) -> PathBuf {
        match storage_type {
            QuantizedVectorsStorageType::Immutable => path.join(QUANTIZED_DATA_PATH),
            QuantizedVectorsStorageType::Mutable => path.join(QUANTIZED_APPENDABLE_DATA_PATH),
        }
    }

    fn get_meta_path(path: &Path) -> PathBuf {
        path.join(QUANTIZED_META_PATH)
    }

    fn get_offsets_path(path: &Path, storage_type: QuantizedVectorsStorageType) -> PathBuf {
        match storage_type {
            QuantizedVectorsStorageType::Immutable => path.join(QUANTIZED_OFFSETS_PATH),
            QuantizedVectorsStorageType::Mutable => path.join(QUANTIZED_APPENDABLE_OFFSETS_PATH),
        }
    }

    pub fn files(&self) -> Vec<PathBuf> {
        let mut files = match &self.storage_impl {
            QuantizedVectorStorage::ScalarRam(q) => q.files(),
            QuantizedVectorStorage::ScalarMmap(q) => q.files(),
            QuantizedVectorStorage::ScalarChunkedMmap(q) => q.files(),
            QuantizedVectorStorage::PQRam(q) => q.files(),
            QuantizedVectorStorage::PQMmap(q) => q.files(),
            QuantizedVectorStorage::PQChunkedMmap(q) => q.files(),
            QuantizedVectorStorage::BinaryRam(q) => q.files(),
            QuantizedVectorStorage::BinaryMmap(q) => q.files(),
            QuantizedVectorStorage::BinaryChunkedMmap(q) => q.files(),
            QuantizedVectorStorage::ScalarRamMulti(q) => q.files(),
            QuantizedVectorStorage::ScalarMmapMulti(q) => q.files(),
            QuantizedVectorStorage::ScalarChunkedMmapMulti(q) => q.files(),
            QuantizedVectorStorage::PQRamMulti(q) => q.files(),
            QuantizedVectorStorage::PQMmapMulti(q) => q.files(),
            QuantizedVectorStorage::PQChunkedMmapMulti(q) => q.files(),
            QuantizedVectorStorage::BinaryRamMulti(q) => q.files(),
            QuantizedVectorStorage::BinaryMmapMulti(q) => q.files(),
            QuantizedVectorStorage::BinaryChunkedMmapMulti(q) => q.files(),
        };
        files.push(self.path.join(QUANTIZED_CONFIG_PATH));
        files
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        let mut files = match &self.storage_impl {
            QuantizedVectorStorage::ScalarRam(q) => q.immutable_files(),
            QuantizedVectorStorage::ScalarMmap(q) => q.immutable_files(),
            QuantizedVectorStorage::ScalarChunkedMmap(q) => q.immutable_files(),
            QuantizedVectorStorage::PQRam(q) => q.immutable_files(),
            QuantizedVectorStorage::PQMmap(q) => q.immutable_files(),
            QuantizedVectorStorage::PQChunkedMmap(q) => q.immutable_files(),
            QuantizedVectorStorage::BinaryRam(q) => q.immutable_files(),
            QuantizedVectorStorage::BinaryMmap(q) => q.immutable_files(),
            QuantizedVectorStorage::BinaryChunkedMmap(q) => q.immutable_files(),
            QuantizedVectorStorage::ScalarRamMulti(q) => q.immutable_files(),
            QuantizedVectorStorage::ScalarMmapMulti(q) => q.immutable_files(),
            QuantizedVectorStorage::ScalarChunkedMmapMulti(q) => q.immutable_files(),
            QuantizedVectorStorage::PQRamMulti(q) => q.immutable_files(),
            QuantizedVectorStorage::PQMmapMulti(q) => q.immutable_files(),
            QuantizedVectorStorage::PQChunkedMmapMulti(q) => q.immutable_files(),
            QuantizedVectorStorage::BinaryRamMulti(q) => q.immutable_files(),
            QuantizedVectorStorage::BinaryMmapMulti(q) => q.immutable_files(),
            QuantizedVectorStorage::BinaryChunkedMmapMulti(q) => q.immutable_files(),
        };
        files.push(self.path.join(QUANTIZED_CONFIG_PATH));
        files
    }

    pub fn create(
        vector_storage: &VectorStorageEnum,
        quantization_config: &QuantizationConfig,
        storage_type: QuantizedVectorsStorageType,
        path: &Path,
        max_threads: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<Self> {
        match vector_storage {
            #[cfg(feature = "rocksdb")]
            VectorStorageEnum::DenseSimple(v) => Self::create_impl(
                v,
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
            #[cfg(feature = "rocksdb")]
            VectorStorageEnum::DenseSimpleByte(v) => Self::create_impl(
                v,
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
            #[cfg(feature = "rocksdb")]
            VectorStorageEnum::DenseSimpleHalf(v) => Self::create_impl(
                v,
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
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
            #[cfg(feature = "rocksdb")]
            VectorStorageEnum::SparseSimple(_) => Err(OperationError::WrongSparse),
            VectorStorageEnum::SparseVolatile(_) => Err(OperationError::WrongSparse),
            VectorStorageEnum::SparseMmap(_) => Err(OperationError::WrongSparse),
            #[cfg(feature = "rocksdb")]
            VectorStorageEnum::MultiDenseSimple(v) => Self::create_multi_impl(
                v,
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
            #[cfg(feature = "rocksdb")]
            VectorStorageEnum::MultiDenseSimpleByte(v) => Self::create_multi_impl(
                v,
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
            #[cfg(feature = "rocksdb")]
            VectorStorageEnum::MultiDenseSimpleHalf(v) => Self::create_multi_impl(
                v,
                quantization_config,
                storage_type,
                path,
                max_threads,
                stopped,
            ),
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
            PrimitiveVectorElement::quantization_preprocess(
                quantization_config,
                distance,
                vector_storage.get_dense::<Sequential>(i),
            )
        });
        let on_disk_vector_storage = vector_storage.is_on_disk();

        let vector_parameters =
            Self::construct_vector_parameters(distance, dim, count, storage_type);

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
        let vectors = vector_storage.iterate_inner_vectors().map(|v| {
            PrimitiveVectorElement::quantization_preprocess(quantization_config, distance, v)
        });
        let inner_vectors_count = vectors.clone().count();
        let vectors_count = vector_storage.total_vector_count();
        let on_disk_vector_storage = vector_storage.is_on_disk();

        let vector_parameters =
            Self::construct_vector_parameters(distance, dim, inner_vectors_count, storage_type);

        let offsets = (0..vectors_count as PointOffsetType)
            .map(|idx| vector_storage.get_multi::<Random>(idx).vectors_count() as PointOffsetType)
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

    pub fn load(
        quantization_config: &QuantizationConfig,
        vector_storage: &VectorStorageEnum,
        path: &Path,
        stopped: &AtomicBool,
    ) -> OperationResult<Option<Self>> {
        let config_path = Self::get_config_path(path);
        if config_path.exists() {
            let config: QuantizedVectorsConfig = read_json(&config_path)?;
            return Ok(Some(Self::load_impl(config, vector_storage, path)?));
        }

        // If we don't have an appendable quantization feature, do not create a new one.
        if !common::flags::feature_flags().appendable_quantization {
            return Ok(None);
        }

        let quantized_vectors = Self::create(
            vector_storage,
            quantization_config,
            QuantizedVectorsStorageType::Mutable,
            path,
            1,
            stopped,
        )?;
        Ok(Some(quantized_vectors))
    }

    pub fn load_impl(
        config: QuantizedVectorsConfig,
        vector_storage: &VectorStorageEnum,
        path: &Path,
    ) -> OperationResult<Self> {
        let quantized_store = if let Some(multivector_config) =
            vector_storage.try_multi_vector_config()
        {
            match &config.quantization_config {
                QuantizationConfig::Scalar(ScalarQuantization { scalar }) => {
                    Self::load_scalar_multi(
                        vector_storage,
                        path,
                        &config,
                        scalar,
                        multivector_config,
                    )?
                }
                QuantizationConfig::Product(ProductQuantization { product }) => {
                    Self::load_pq_multi(vector_storage, path, &config, product, multivector_config)?
                }
                QuantizationConfig::Binary(BinaryQuantization { binary }) => {
                    Self::load_binary_multi(
                        vector_storage,
                        path,
                        &config,
                        binary,
                        multivector_config,
                    )?
                }
            }
        } else {
            match &config.quantization_config {
                QuantizationConfig::Scalar(ScalarQuantization { scalar }) => {
                    Self::load_scalar(vector_storage, path, &config, scalar)?
                }
                QuantizationConfig::Product(ProductQuantization { product }) => {
                    Self::load_pq(vector_storage, path, &config, product)?
                }
                QuantizationConfig::Binary(BinaryQuantization { binary }) => {
                    Self::load_binary(vector_storage, path, &config, binary)?
                }
            }
        };

        let distance = vector_storage.distance();
        let datatype = vector_storage.datatype();
        Ok(QuantizedVectors {
            storage_impl: quantized_store,
            config,
            path: path.to_path_buf(),
            distance,
            datatype,
        })
    }

    fn load_scalar(
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
                EncodedVectorsU8::<QuantizedRamStorage>::get_quantized_vector_size(
                    &config.vector_parameters,
                );
            let quantized_vectors_storage =
                QuantizedRamStorage::from_file(data_path.as_path(), quantized_vector_size)?;
            Ok(QuantizedVectorStorage::ScalarRam(EncodedVectorsU8::load(
                quantized_vectors_storage,
                &meta_path,
            )?))
        } else {
            let quantized_vector_size =
                EncodedVectorsU8::<QuantizedMmapStorage>::get_quantized_vector_size(
                    &config.vector_parameters,
                );
            let quantized_vectors_storage =
                QuantizedMmapStorage::from_file(data_path.as_path(), quantized_vector_size)?;
            Ok(QuantizedVectorStorage::ScalarMmap(EncodedVectorsU8::load(
                quantized_vectors_storage,
                &meta_path,
            )?))
        }
    }

    fn load_scalar_multi(
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
                EncodedVectorsU8::<QuantizedRamStorage>::get_quantized_vector_size(
                    &config.vector_parameters,
                );
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
                EncodedVectorsU8::<QuantizedMmapStorage>::get_quantized_vector_size(
                    &config.vector_parameters,
                );
            let inner_vectors_storage =
                QuantizedMmapStorage::from_file(data_path.as_path(), quantized_vector_size)?;
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

    fn load_pq(
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
        if Self::is_ram(pq_config.always_ram, on_disk_vector_storage) {
            let bucket_size = Self::get_bucket_size(pq_config.compression);
            let quantized_vector_size =
                EncodedVectorsPQ::<QuantizedRamStorage>::get_quantized_vector_size(
                    &config.vector_parameters,
                    bucket_size,
                );
            let quantized_vectors_storage =
                QuantizedRamStorage::from_file(data_path.as_path(), quantized_vector_size)?;
            Ok(QuantizedVectorStorage::PQRam(EncodedVectorsPQ::load(
                quantized_vectors_storage,
                &meta_path,
            )?))
        } else {
            let bucket_size = Self::get_bucket_size(pq_config.compression);
            let quantized_vector_size =
                EncodedVectorsPQ::<QuantizedMmapStorage>::get_quantized_vector_size(
                    &config.vector_parameters,
                    bucket_size,
                );
            let quantized_vectors_storage =
                QuantizedMmapStorage::from_file(data_path.as_path(), quantized_vector_size)?;
            Ok(QuantizedVectorStorage::PQMmap(EncodedVectorsPQ::load(
                quantized_vectors_storage,
                &meta_path,
            )?))
        }
    }

    fn load_pq_multi(
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
        if Self::is_ram(pq_config.always_ram, on_disk_vector_storage) {
            let bucket_size = Self::get_bucket_size(pq_config.compression);
            let quantized_vector_size =
                EncodedVectorsPQ::<QuantizedRamStorage>::get_quantized_vector_size(
                    &config.vector_parameters,
                    bucket_size,
                );
            let inner_vectors_storage =
                QuantizedRamStorage::from_file(data_path.as_path(), quantized_vector_size)?;
            let inner_vectors_storage = EncodedVectorsPQ::load(inner_vectors_storage, &meta_path)?;
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
            let bucket_size = Self::get_bucket_size(pq_config.compression);
            let quantized_vector_size =
                EncodedVectorsPQ::<QuantizedMmapStorage>::get_quantized_vector_size(
                    &config.vector_parameters,
                    bucket_size,
                );
            let inner_vectors_storage =
                QuantizedMmapStorage::from_file(data_path.as_path(), quantized_vector_size)?;
            let inner_vectors_storage = EncodedVectorsPQ::load(inner_vectors_storage, &meta_path)?;
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

    fn load_binary(
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
                EncodedVectorsBin::<u128, QuantizedChunkedMmapStorage>::get_quantized_vector_size_from_params(
                    config.vector_parameters.dim,
                    Self::convert_binary_encoding(binary_config.encoding),
                );
                let quantization_storage = QuantizedChunkedMmapStorage::new(
                    data_path.as_path(),
                    quantized_vector_size,
                    in_ram,
                )?;
                Ok(QuantizedVectorStorage::BinaryChunkedMmap(
                    EncodedVectorsBin::load(quantization_storage, meta_path.as_path())?,
                ))
            }
            (true, QuantizedVectorsStorageType::Immutable) => {
                let quantized_vector_size =
                EncodedVectorsBin::<u128, QuantizedRamStorage>::get_quantized_vector_size_from_params(
                    config.vector_parameters.dim,
                    Self::convert_binary_encoding(binary_config.encoding),
                );
                let quantized_vectors_storage =
                    QuantizedRamStorage::from_file(data_path.as_path(), quantized_vector_size)?;
                Ok(QuantizedVectorStorage::BinaryRam(EncodedVectorsBin::load(
                    quantized_vectors_storage,
                    &meta_path,
                )?))
            }
            (false, QuantizedVectorsStorageType::Immutable) => {
                let quantized_vector_size =
                EncodedVectorsBin::<u128, QuantizedMmapStorage>::get_quantized_vector_size_from_params(
                    config.vector_parameters.dim,
                    Self::convert_binary_encoding(binary_config.encoding),
                );
                let quantized_vectors_storage =
                    QuantizedMmapStorage::from_file(data_path.as_path(), quantized_vector_size)?;
                Ok(QuantizedVectorStorage::BinaryMmap(EncodedVectorsBin::load(
                    quantized_vectors_storage,
                    &meta_path,
                )?))
            }
        }
    }

    fn load_binary_multi(
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
                EncodedVectorsBin::<u8, QuantizedChunkedMmapStorage>::get_quantized_vector_size_from_params(
                    config.vector_parameters.dim,
                    Self::convert_binary_encoding(binary_config.encoding),
                );
                let quantization_storage = QuantizedChunkedMmapStorage::new(
                    data_path.as_path(),
                    quantized_vector_size,
                    in_ram,
                )?;
                let inner_storage =
                    EncodedVectorsBin::load(quantization_storage, meta_path.as_path())?;
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
                EncodedVectorsBin::<u8, QuantizedRamStorage>::get_quantized_vector_size_from_params(
                    config.vector_parameters.dim,
                    Self::convert_binary_encoding(binary_config.encoding),
                );
                let inner_vectors_storage =
                    QuantizedRamStorage::from_file(data_path.as_path(), quantized_vector_size)?;
                let inner_vectors_storage =
                    EncodedVectorsBin::load(inner_vectors_storage, &meta_path)?;
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
                EncodedVectorsBin::<u8, QuantizedMmapStorage>::get_quantized_vector_size_from_params(
                    config.vector_parameters.dim,
                    Self::convert_binary_encoding(binary_config.encoding),
                );
                let inner_vectors_storage =
                    QuantizedMmapStorage::from_file(data_path.as_path(), quantized_vector_size)?;
                let inner_vectors_storage =
                    EncodedVectorsBin::load(inner_vectors_storage, &meta_path)?;
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

    #[allow(clippy::too_many_arguments)]
    fn create_scalar<'a>(
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
            EncodedVectorsU8::<QuantizedMmapStorage>::get_quantized_vector_size(vector_parameters);
        let meta_path = Self::get_meta_path(path);
        let data_path = Self::get_data_path(path, storage_type);
        let in_ram = Self::is_ram(scalar_config.always_ram, on_disk_vector_storage);
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
            let storage_builder = QuantizedMmapStorageBuilder::new(
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
    fn create_scalar_multi<'a>(
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
            EncodedVectorsU8::<QuantizedMmapStorage>::get_quantized_vector_size(vector_parameters);
        let meta_path = Self::get_meta_path(path);
        let data_path = Self::get_data_path(path, storage_type);
        let offsets_path = Self::get_offsets_path(path, storage_type);
        let in_ram = Self::is_ram(scalar_config.always_ram, on_disk_vector_storage);
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
            let storage_builder = QuantizedMmapStorageBuilder::new(
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

    #[allow(clippy::too_many_arguments)]
    fn create_pq<'a>(
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
            EncodedVectorsPQ::<QuantizedMmapStorage>::get_quantized_vector_size(
                vector_parameters,
                bucket_size,
            );
        let meta_path = Self::get_meta_path(path);
        let data_path = Self::get_data_path(path, storage_type);
        let in_ram = Self::is_ram(pq_config.always_ram, on_disk_vector_storage);
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
            let storage_builder = QuantizedMmapStorageBuilder::new(
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
    fn create_pq_multi<'a>(
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
            EncodedVectorsPQ::<QuantizedMmapStorage>::get_quantized_vector_size(
                vector_parameters,
                bucket_size,
            );
        let meta_path = Self::get_meta_path(path);
        let data_path = Self::get_data_path(path, storage_type);
        let offsets_path = Self::get_offsets_path(path, storage_type);
        let in_ram = Self::is_ram(pq_config.always_ram, on_disk_vector_storage);
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
            let storage_builder = QuantizedMmapStorageBuilder::new(
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

    #[allow(clippy::too_many_arguments)]
    fn create_binary<'a>(
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
        let quantized_vector_size =
            EncodedVectorsBin::<u128, QuantizedMmapStorage>::get_quantized_vector_size_from_params(
                vector_parameters.dim,
                encoding,
            );
        let meta_path = Self::get_meta_path(path);
        let data_path = Self::get_data_path(path, storage_type);
        let in_ram = Self::is_ram(binary_config.always_ram, on_disk_vector_storage);

        match (in_ram, storage_type) {
            (_, QuantizedVectorsStorageType::Mutable) => {
                let storage_builder = QuantizedChunkedMmapStorageBuilder::new(
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
                let storage_builder = QuantizedMmapStorageBuilder::new(
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
    fn create_binary_multi<'a>(
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
        let quantized_vector_size =
            EncodedVectorsBin::<u8, QuantizedMmapStorage>::get_quantized_vector_size_from_params(
                vector_parameters.dim,
                encoding,
            );
        let meta_path = Self::get_meta_path(path);
        let data_path = Self::get_data_path(path, storage_type);
        let offsets_path = Self::get_offsets_path(path, storage_type);
        let in_ram = Self::is_ram(binary_config.always_ram, on_disk_vector_storage);

        match (in_ram, storage_type) {
            (_, QuantizedVectorsStorageType::Mutable) => {
                let storage_builder = QuantizedChunkedMmapStorageBuilder::new(
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
                let offsets =
                    MultivectorOffsetsStorageChunkedMmap::create(&offsets_path, offsets, in_ram)?;
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
                let storage_builder = QuantizedMmapStorageBuilder::new(
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

    fn is_ram(always_ram: Option<bool>, on_disk_vector_storage: bool) -> bool {
        !on_disk_vector_storage || always_ram == Some(true)
    }

    fn convert_binary_encoding(
        encoding: Option<BinaryQuantizationEncoding>,
    ) -> quantization::encoded_vectors_binary::Encoding {
        match encoding {
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
        }
    }

    fn convert_binary_query_encoding(
        query_encoding: Option<BinaryQuantizationQueryEncoding>,
    ) -> quantization::encoded_vectors_binary::QueryEncoding {
        match query_encoding {
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
        }
    }

    fn convert_scalar_encoding(encoding: ScalarType) -> ScalarQuantizationMethod {
        match encoding {
            ScalarType::Int8 => ScalarQuantizationMethod::Int8,
        }
    }

    fn construct_vector_parameters(
        distance: Distance,
        dim: usize,
        deprecated_count: usize,
        storage_type: QuantizedVectorsStorageType,
    ) -> quantization::VectorParameters {
        quantization::VectorParameters {
            dim,
            deprecated_count: match storage_type {
                QuantizedVectorsStorageType::Mutable => None,
                QuantizedVectorsStorageType::Immutable => Some(deprecated_count),
            },
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
            QuantizedVectorStorage::ScalarChunkedMmap(storage) => storage.storage().populate()?,
            QuantizedVectorStorage::PQRam(_) => {}
            QuantizedVectorStorage::PQMmap(storage) => storage.storage().populate(),
            QuantizedVectorStorage::PQChunkedMmap(storage) => storage.storage().populate()?,
            QuantizedVectorStorage::BinaryRam(_) => {}
            QuantizedVectorStorage::BinaryMmap(storage) => storage.storage().populate(),
            QuantizedVectorStorage::BinaryChunkedMmap(storage) => storage.storage().populate()?,
            QuantizedVectorStorage::ScalarRamMulti(_) => {}
            QuantizedVectorStorage::ScalarMmapMulti(storage) => {
                storage.storage().storage().populate();
                storage.offsets_storage().populate()?;
            }
            QuantizedVectorStorage::ScalarChunkedMmapMulti(storage) => {
                storage.storage().storage().populate()?;
                storage.offsets_storage().populate()?;
            }
            QuantizedVectorStorage::PQRamMulti(_) => {}
            QuantizedVectorStorage::PQMmapMulti(storage) => {
                storage.storage().storage().populate();
                storage.offsets_storage().populate()?;
            }
            QuantizedVectorStorage::PQChunkedMmapMulti(storage) => {
                storage.storage().storage().populate()?;
                storage.offsets_storage().populate()?;
            }
            QuantizedVectorStorage::BinaryRamMulti(_) => {}
            QuantizedVectorStorage::BinaryMmapMulti(storage) => {
                storage.storage().storage().populate();
                storage.offsets_storage().populate()?;
            }
            QuantizedVectorStorage::BinaryChunkedMmapMulti(storage) => {
                storage.storage().storage().populate()?;
                storage.offsets_storage().populate()?;
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
            QuantizedVectorStorage::ScalarChunkedMmap(q) => q.flusher(),
            QuantizedVectorStorage::PQRam(q) => q.flusher(),
            QuantizedVectorStorage::PQMmap(q) => q.flusher(),
            QuantizedVectorStorage::PQChunkedMmap(q) => q.flusher(),
            QuantizedVectorStorage::BinaryRam(q) => q.flusher(),
            QuantizedVectorStorage::BinaryMmap(q) => q.flusher(),
            QuantizedVectorStorage::BinaryChunkedMmap(q) => q.flusher(),
            QuantizedVectorStorage::ScalarRamMulti(q) => q.flusher(),
            QuantizedVectorStorage::ScalarMmapMulti(q) => q.flusher(),
            QuantizedVectorStorage::ScalarChunkedMmapMulti(q) => q.flusher(),
            QuantizedVectorStorage::PQRamMulti(q) => q.flusher(),
            QuantizedVectorStorage::PQMmapMulti(q) => q.flusher(),
            QuantizedVectorStorage::PQChunkedMmapMulti(q) => q.flusher(),
            QuantizedVectorStorage::BinaryRamMulti(q) => q.flusher(),
            QuantizedVectorStorage::BinaryMmapMulti(q) => q.flusher(),
            QuantizedVectorStorage::BinaryChunkedMmapMulti(q) => q.flusher(),
        };
        Box::new(move || flusher().map_err(OperationError::from))
    }

    pub fn upsert_vector(
        &mut self,
        id: PointOffsetType,
        vector: VectorRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match &mut self.storage_impl {
            QuantizedVectorStorage::ScalarRam(q) => {
                Self::upsert_vector_dense(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::ScalarMmap(q) => {
                Self::upsert_vector_dense(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::ScalarChunkedMmap(q) => {
                Self::upsert_vector_dense(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::PQRam(q) => {
                Self::upsert_vector_dense(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::PQMmap(q) => {
                Self::upsert_vector_dense(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::PQChunkedMmap(q) => {
                Self::upsert_vector_dense(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::BinaryRam(q) => {
                Self::upsert_vector_dense(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::BinaryMmap(q) => {
                Self::upsert_vector_dense(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::BinaryChunkedMmap(q) => {
                Self::upsert_vector_dense(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::ScalarRamMulti(q) => {
                Self::upsert_vector_multi(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::ScalarMmapMulti(q) => {
                Self::upsert_vector_multi(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::ScalarChunkedMmapMulti(q) => {
                Self::upsert_vector_multi(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::PQRamMulti(q) => {
                Self::upsert_vector_multi(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::PQMmapMulti(q) => {
                Self::upsert_vector_multi(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::PQChunkedMmapMulti(q) => {
                Self::upsert_vector_multi(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::BinaryRamMulti(q) => {
                Self::upsert_vector_multi(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::BinaryMmapMulti(q) => {
                Self::upsert_vector_multi(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::BinaryChunkedMmapMulti(q) => {
                Self::upsert_vector_multi(q, id, vector, hw_counter)
            }
        }
    }

    fn upsert_vector_dense(
        quantization_storage: &mut impl quantization::EncodedVectors,
        id: PointOffsetType,
        vector: VectorRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        if let VectorRef::Dense(vector) = vector {
            Ok(quantization_storage.upsert_vector(id, vector, hw_counter)?)
        } else {
            Err(OperationError::WrongMulti)
        }
    }

    fn upsert_vector_multi(
        quantization_storage: &mut impl quantization::EncodedVectors,
        id: PointOffsetType,
        vector: VectorRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        if let VectorRef::MultiDense(vector) = vector {
            Ok(quantization_storage.upsert_vector(id, vector.flattened_vectors, hw_counter)?)
        } else {
            Err(OperationError::WrongMulti)
        }
    }
}
