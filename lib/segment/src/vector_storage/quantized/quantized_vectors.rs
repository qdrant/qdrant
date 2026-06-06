mod accessors;
mod binary;
mod config;
mod create;
mod load;
mod pq;
mod scalar;
mod storage;
mod turbo;

use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use quantization::encoded_vectors_u8::ScalarQuantizationMethod;
use quantization::turboquant::TQBits;

pub use self::config::{
    QUANTIZED_APPENDABLE_DATA_PATH, QUANTIZED_APPENDABLE_OFFSETS_PATH, QUANTIZED_CONFIG_PATH,
    QUANTIZED_DATA_PATH, QUANTIZED_META_PATH, QUANTIZED_OFFSETS_PATH, QuantizedVectorsConfig,
    QuantizedVectorsStorageType,
};
pub use self::storage::QuantizedVectorStorage;
use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::QueryVector;
use crate::types::{
    BinaryQuantizationEncoding, BinaryQuantizationQueryEncoding, CompressionRatio, Distance,
    QuantizationConfig, ScalarType, TurboQuantBitSize, VectorStorageDatatype,
};
use crate::vector_storage::quantized::quantized_multi_query_scorer::QuantizedMultiQueryScorer;
use crate::vector_storage::quantized::quantized_multivector_storage::{
    MultivectorOffsetsStorage, QuantizedMultivectorStorage,
};
use crate::vector_storage::quantized::quantized_query_scorer::{
    InternalScorerUnsupported, QuantizedQueryScorer,
};
use crate::vector_storage::quantized::quantized_scorer_builder::QuantizedScorerBuilder;
use crate::vector_storage::{RawScorer, RawScorerImpl};

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

        fn build_multi<'a, QuantizedStorage, OffsetStorage>(
            point_id: PointOffsetType,
            quantized_data: &'a QuantizedMultivectorStorage<QuantizedStorage, OffsetStorage>,
            hardware_counter: HardwareCounterCell,
        ) -> Result<Box<dyn RawScorer + 'a>, InternalScorerUnsupported>
        where
            QuantizedStorage: quantization::EncodedVectors + 'a,
            OffsetStorage: MultivectorOffsetsStorage + 'a,
        {
            let query_scorer = QuantizedMultiQueryScorer::new_internal(
                point_id,
                quantized_data,
                hardware_counter,
            )?;

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
            QuantizedVectorStorage::TQRam(storage) => build(point_id, storage, hardware_counter),
            QuantizedVectorStorage::TQMmap(storage) => build(point_id, storage, hardware_counter),
            QuantizedVectorStorage::TQChunkedMmap(storage) => {
                build(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::ScalarRamMulti(storage) => {
                build_multi(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::ScalarMmapMulti(storage) => {
                build_multi(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::ScalarChunkedMmapMulti(storage) => {
                build_multi(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::PQRamMulti(storage) => {
                build_multi(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::PQMmapMulti(storage) => {
                build_multi(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::PQChunkedMmapMulti(storage) => {
                build_multi(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::BinaryRamMulti(storage) => {
                build_multi(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::BinaryMmapMulti(storage) => {
                build_multi(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::BinaryChunkedMmapMulti(storage) => {
                build_multi(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::TQRamMulti(storage) => {
                build_multi(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::TQMmapMulti(storage) => {
                build_multi(point_id, storage, hardware_counter)
            }
            QuantizedVectorStorage::TQChunkedMmapMulti(storage) => {
                build_multi(point_id, storage, hardware_counter)
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

    fn is_ram(always_ram: Option<bool>, on_disk_vector_storage: bool) -> bool {
        // Low-memory mode forces the mmap (on-disk) backend regardless of the
        // persisted `always_ram` flag. The on-disk byte layout is identical,
        // so flipping back later still works without rebuild.
        if common::low_memory::low_memory_mode().prefer_disk() {
            return false;
        }
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

    fn convert_tq_bits(bits: TurboQuantBitSize) -> TQBits {
        match bits {
            TurboQuantBitSize::Bits1 => TQBits::Bits1,
            TurboQuantBitSize::Bits1_5 => TQBits::Bits1_5,
            TurboQuantBitSize::Bits2 => TQBits::Bits2,
            TurboQuantBitSize::Bits4 => TQBits::Bits4,
        }
    }

    fn tq_bits_default_rescoring(bits: TQBits) -> bool {
        match bits {
            TQBits::Bits1 | TQBits::Bits1_5 | TQBits::Bits2 => true,
            TQBits::Bits4 => false,
        }
    }

    fn construct_vector_parameters(
        quantization_config: &QuantizationConfig,
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
                Distance::Cosine => match quantization_config {
                    // Because of backwards compatibility,
                    // we have to use Dot product for scalar, pq and binary quantization when distance is Cosine.
                    // Only TurboQuant has a difference between Cosine and Dot product.
                    QuantizationConfig::Scalar(_) => quantization::DistanceType::Dot,
                    QuantizationConfig::Product(_) => quantization::DistanceType::Dot,
                    QuantizationConfig::Binary(_) => quantization::DistanceType::Dot,
                    QuantizationConfig::Turbo(_) => quantization::DistanceType::Cosine,
                },
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
}

impl crate::common::memory_usage::MemoryReporter for QuantizedVectors {
    fn memory_usage(&self) -> crate::common::memory_usage::ComponentMemoryUsage {
        use crate::common::memory_usage::{ComponentMemoryUsage, FileStorageIntent};

        let files = self.files();
        let heap_bytes = self.storage_impl.heap_size_bytes() as u64;

        // Either always_ram, then we only load on in ram and track heap_bytes
        // Or full on_disk, and we don't preload anything
        let intent = FileStorageIntent::OnDisk;

        if heap_bytes > 0 {
            ComponentMemoryUsage::from_files_and_ram(files, intent, heap_bytes)
        } else {
            ComponentMemoryUsage::from_files(files, intent)
        }
    }
}
