mod accessors;
mod binary;
mod config;
mod create;
mod load;
mod pq;
mod read_access;
mod read_only;
mod scalar;
mod storage;
mod turbo;

use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, MmapFs};
use quantization::encoded_vectors_u8::ScalarQuantizationMethod;
use quantization::turboquant::TQBits;

/// Local-file backend ([`MmapFile`]) shared by every persisted (mmap / chunked) quantized
/// storage variant. Paired with the [`READ_FS`] value handle.
pub(in crate::vector_storage::quantized) type ReadFile = MmapFile;

/// Value handle for the [`ReadFile`] backend. The create and load paths always operate on
/// local files, so they pass this single instance instead of constructing one each time.
pub(in crate::vector_storage::quantized) const READ_FS: MmapFs = MmapFs;

pub(in crate::vector_storage::quantized) use self::config::QuantizedStorageKind;
pub use self::config::{
    QUANTIZED_APPENDABLE_DATA_PATH, QUANTIZED_APPENDABLE_OFFSETS_PATH, QUANTIZED_CONFIG_PATH,
    QUANTIZED_DATA_PATH, QUANTIZED_META_PATH, QUANTIZED_OFFSETS_PATH, QuantizedVectorsConfig,
    QuantizedVectorsStorageType,
};
pub use self::read_access::QuantizedVectorsRead;
pub use self::read_only::{ReadOnlyQuantizedVectorStorage, ReadOnlyQuantizedVectors};
pub use self::storage::QuantizedVectorStorage;
use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::QueryVector;
use crate::types::{
    BinaryQuantization, BinaryQuantizationEncoding, BinaryQuantizationQueryEncoding,
    CompressionRatio, Distance, ProductQuantization, QuantizationConfig, ScalarType,
    TurboQuantBitSize, TurboQuantization, VectorStorageDatatype,
};
use crate::vector_storage::RawScorer;
use crate::vector_storage::quantized::quantized_query_scorer::InternalScorerUnsupported;
use crate::vector_storage::quantized::quantized_scorer_builder::{
    QuantizedScorerDispatch, build_quantized_raw_scorer,
};

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
        build_quantized_raw_scorer(
            &self.storage_impl,
            &self.config.quantization_config,
            &self.distance,
            self.datatype,
            self.storage_impl.is_on_disk(),
            query,
            hardware_counter,
        )
    }

    /// Build a raw scorer for the specified `point_id`.
    /// If not supported, return [`InternalScorerUnsupported`] with the original `hardware_counter`.
    pub fn raw_internal_scorer<'a>(
        &'a self,
        point_id: PointOffsetType,
        hardware_counter: HardwareCounterCell,
    ) -> Result<Box<dyn RawScorer + 'a>, InternalScorerUnsupported> {
        self.storage_impl
            .raw_internal_scorer(point_id, hardware_counter)
    }

    pub(in crate::vector_storage::quantized) fn get_config_path(path: &Path) -> PathBuf {
        path.join(QUANTIZED_CONFIG_PATH)
    }

    pub(in crate::vector_storage::quantized) fn get_data_path(
        path: &Path,
        storage_type: QuantizedVectorsStorageType,
    ) -> PathBuf {
        match storage_type {
            QuantizedVectorsStorageType::Immutable => path.join(QUANTIZED_DATA_PATH),
            QuantizedVectorsStorageType::Mutable => path.join(QUANTIZED_APPENDABLE_DATA_PATH),
        }
    }

    pub(in crate::vector_storage::quantized) fn get_meta_path(path: &Path) -> PathBuf {
        path.join(QUANTIZED_META_PATH)
    }

    pub(in crate::vector_storage::quantized) fn get_offsets_path(
        path: &Path,
        storage_type: QuantizedVectorsStorageType,
    ) -> PathBuf {
        match storage_type {
            QuantizedVectorsStorageType::Immutable => path.join(QUANTIZED_OFFSETS_PATH),
            QuantizedVectorsStorageType::Mutable => path.join(QUANTIZED_APPENDABLE_OFFSETS_PATH),
        }
    }

    pub(in crate::vector_storage::quantized) fn is_ram(
        always_ram: Option<bool>,
        on_disk_vector_storage: bool,
    ) -> bool {
        // Low-memory mode forces the mmap (on-disk) backend regardless of the
        // persisted `always_ram` flag. The on-disk byte layout is identical,
        // so flipping back later still works without rebuild.
        if common::low_memory::low_memory_mode().prefer_disk() {
            return false;
        }
        !on_disk_vector_storage || always_ram == Some(true)
    }

    pub(in crate::vector_storage::quantized) fn convert_binary_encoding(
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

    pub(in crate::vector_storage::quantized) fn convert_tq_bits(bits: TurboQuantBitSize) -> TQBits {
        match bits {
            TurboQuantBitSize::Bits1 => TQBits::Bits1,
            TurboQuantBitSize::Bits1_5 => TQBits::Bits1_5,
            TurboQuantBitSize::Bits2 => TQBits::Bits2,
            TurboQuantBitSize::Bits4 => TQBits::Bits4,
        }
    }

    pub(in crate::vector_storage::quantized) fn tq_bits_default_rescoring(bits: TQBits) -> bool {
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

    pub(in crate::vector_storage::quantized) fn get_bucket_size(
        compression: CompressionRatio,
    ) -> usize {
        match compression {
            CompressionRatio::X4 => 1,
            CompressionRatio::X8 => 2,
            CompressionRatio::X16 => 4,
            CompressionRatio::X32 => 8,
            CompressionRatio::X64 => 16,
        }
    }

    /// Size in bytes of a single quantized vector on disk.
    ///
    /// Single source of truth for the on-disk stride, shared by the create, load and
    /// read-only-open paths so they cannot disagree about how the data is laid out.
    /// `is_multi` selects the multi-vector layout — notably binary quantization packs
    /// multi-vector storage into `u8` words but single-vector storage into `u128`.
    pub(in crate::vector_storage::quantized) fn quantized_vector_size(
        quantization_config: &QuantizationConfig,
        vector_parameters: &quantization::VectorParameters,
        is_multi: bool,
    ) -> usize {
        match quantization_config {
            QuantizationConfig::Scalar(_) => {
                quantization::encoded_vectors_u8::get_quantized_vector_size(vector_parameters)
            }
            QuantizationConfig::Product(ProductQuantization { product }) => {
                let bucket_size = Self::get_bucket_size(product.compression);
                quantization::encoded_vectors_pq::get_quantized_vector_size(
                    vector_parameters,
                    bucket_size,
                )
            }
            QuantizationConfig::Binary(BinaryQuantization { binary }) => {
                let encoding = Self::convert_binary_encoding(binary.encoding);
                if is_multi {
                    quantization::encoded_vectors_binary::get_quantized_vector_size_from_params::<u8>(
                        vector_parameters.dim,
                        encoding,
                    )
                } else {
                    quantization::encoded_vectors_binary::get_quantized_vector_size_from_params::<
                        u128,
                    >(vector_parameters.dim, encoding)
                }
            }
            QuantizationConfig::Turbo(TurboQuantization { turbo }) => {
                let bits = Self::convert_tq_bits(turbo.bits.unwrap_or_default());
                quantization::encoded_vectors_tq::get_quantized_vector_size(
                    vector_parameters,
                    bits,
                    quantization::turboquant::TQMode::Plus,
                )
            }
        }
    }

    pub fn get_storage(&self) -> &QuantizedVectorStorage {
        &self.storage_impl
    }
}

impl QuantizedVectorsRead for QuantizedVectors {
    fn config(&self) -> &QuantizedVectorsConfig {
        self.config()
    }

    fn default_rescoring(&self) -> bool {
        self.default_rescoring()
    }

    fn raw_scorer<'a>(
        &'a self,
        query: QueryVector,
        hardware_counter: HardwareCounterCell,
    ) -> OperationResult<Box<dyn RawScorer + 'a>> {
        self.raw_scorer(query, hardware_counter)
    }

    fn raw_internal_scorer<'a>(
        &'a self,
        point_id: PointOffsetType,
        hardware_counter: HardwareCounterCell,
    ) -> Result<Box<dyn RawScorer + 'a>, InternalScorerUnsupported> {
        self.raw_internal_scorer(point_id, hardware_counter)
    }
}

/// Whether to keep a TurboQuant-datatype source in its rotated space when
/// re-quantizing (vectors stay rotated, the secondary TurboQuant reuses the same
/// `Unpadded` rotation to rotate queries) rather than rotating the vectors back.
///
/// True only for a Turbo4 source re-quantized with TurboQuant, excluding:
/// - Manhattan — the Hadamard rotation does not preserve L1;
/// - `Bits1_5` targets — they encode extra precision by rotating into the x1.5
///   padding, so they require a `Padded` rotation and can't reuse the source's
///   `Unpadded` one (`TurboQuantizer::new` asserts `Bits1_5 requires Padded`).
pub fn should_keep_source_rotated(
    source_datatype: VectorStorageDatatype,
    quantization_config: &QuantizationConfig,
    distance: Distance,
) -> bool {
    let QuantizationConfig::Turbo(turbo) = quantization_config else {
        return false;
    };
    source_datatype == VectorStorageDatatype::Turbo4
        && turbo.turbo.bits.unwrap_or_default() != TurboQuantBitSize::Bits1_5
        && distance != Distance::Manhattan
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
