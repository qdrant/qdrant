use std::path::Path;
use std::sync::atomic::AtomicBool;

use common::fs::read_json;
use quantization::EncodedVectorsPQ;
use quantization::encoded_vectors_binary::EncodedVectorsBin;
use quantization::encoded_vectors_tq::EncodedVectorsTQ;
use quantization::encoded_vectors_u8::EncodedVectorsU8;

use super::{
    QuantizedStorageKind, QuantizedVectorStorage, QuantizedVectors, QuantizedVectorsConfig,
    QuantizedVectorsStorageType, READ_FS, ReadFile,
};
use crate::common::operation_error::OperationResult;
use crate::types::{Memory, MultiVectorConfig, QuantizationConfig};
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedStorage;
use crate::vector_storage::quantized::quantized_multivector_storage::{
    MultivectorOffsetsStorageChunked, MultivectorOffsetsStorageMmap, MultivectorOffsetsStorageRam,
    QuantizedMultivectorStorage,
};
use crate::vector_storage::quantized::quantized_ram_storage::QuantizedRamStorage;
use crate::vector_storage::quantized::quantized_storage::QuantizedStorage;
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

impl QuantizedVectors {
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

        // Only quantization methods that support appendable storage can be auto-created
        // here with `Mutable` storage type. Other methods (Scalar, Product) would fail
        // inside `create_impl`/`create_multi_impl` because their `create_*` helpers reject
        // `Mutable`.
        if !quantization_config.supports_appendable() {
            return Ok(None);
        }

        // Auto-create only initializes an empty quantized container for
        // a fresh appendable segment — `count == 0` short-circuits the
        // pre-pass, so the thread budget here is irrelevant. Any
        // re-quantization with actual data goes through
        // `SegmentBuilder::build` under a permit-bounded `max_threads`.
        let max_threads = 1;
        let quantized_vectors = Self::create(
            vector_storage,
            quantization_config,
            QuantizedVectorsStorageType::Mutable,
            path,
            max_threads,
            stopped,
        )?;
        Ok(Some(quantized_vectors))
    }

    pub fn load_impl(
        config: QuantizedVectorsConfig,
        vector_storage: &VectorStorageEnum,
        path: &Path,
    ) -> OperationResult<Self> {
        let on_disk_vector_storage = vector_storage.is_on_disk();
        let quantized_store = match vector_storage.try_multi_vector_config() {
            Some(multivector_config) => {
                Self::load_multi(&config, path, multivector_config, on_disk_vector_storage)?
            }
            None => Self::load_single(&config, path, on_disk_vector_storage)?,
        };

        let distance = vector_storage.distance();
        let datatype = vector_storage.datatype();
        let quantized_vectors = QuantizedVectors {
            storage_impl: quantized_store,
            config,
            path: path.to_path_buf(),
            distance,
            datatype,
        };

        // For the cached placement quantized vectors stay mmap-backed, but the page cache is
        // primed on load
        if quantized_vectors
            .config
            .memory_placement(on_disk_vector_storage)
            == Memory::Cached
        {
            quantized_vectors.populate()?;
        }

        Ok(quantized_vectors)
    }

    fn load_single(
        config: &QuantizedVectorsConfig,
        path: &Path,
        on_disk_vector_storage: bool,
    ) -> OperationResult<QuantizedVectorStorage> {
        let data_path = Self::get_data_path(path, config.storage_type);
        let meta_path = Self::get_meta_path(path);
        let size = config.quantized_vector_size(false);
        let in_ram = config.is_ram(on_disk_vector_storage);

        // Open the flat (RAM / mmap) or appendable chunked storage selected for this config.
        let ram = || QuantizedRamStorage::open::<ReadFile>(&READ_FS, data_path.as_path(), size);
        let mmap = || QuantizedStorage::open(&READ_FS, data_path.as_path(), size);
        let chunked =
            || QuantizedChunkedStorage::<ReadFile>::new(READ_FS, data_path.as_path(), size, in_ram);

        let storage =
            match config.storage_kind(on_disk_vector_storage)? {
                QuantizedStorageKind::ScalarRam => QuantizedVectorStorage::ScalarRam(
                    EncodedVectorsU8::load(&READ_FS, ram()?, &meta_path)?,
                ),
                QuantizedStorageKind::ScalarMmap => QuantizedVectorStorage::ScalarMmap(
                    EncodedVectorsU8::load(&READ_FS, mmap()?, &meta_path)?,
                ),
                QuantizedStorageKind::PqRam => QuantizedVectorStorage::PQRam(
                    EncodedVectorsPQ::load(&READ_FS, ram()?, &meta_path)?,
                ),
                QuantizedStorageKind::PqMmap => QuantizedVectorStorage::PQMmap(
                    EncodedVectorsPQ::load(&READ_FS, mmap()?, &meta_path)?,
                ),
                QuantizedStorageKind::BinaryRam => QuantizedVectorStorage::BinaryRam(
                    EncodedVectorsBin::load(&READ_FS, ram()?, &meta_path)?,
                ),
                QuantizedStorageKind::BinaryMmap => QuantizedVectorStorage::BinaryMmap(
                    EncodedVectorsBin::load(&READ_FS, mmap()?, &meta_path)?,
                ),
                QuantizedStorageKind::BinaryChunked => QuantizedVectorStorage::BinaryChunkedMmap(
                    EncodedVectorsBin::load(&READ_FS, chunked()?, &meta_path)?,
                ),
                QuantizedStorageKind::TqRam => QuantizedVectorStorage::TQRam(
                    EncodedVectorsTQ::load(&READ_FS, ram()?, &meta_path)?,
                ),
                QuantizedStorageKind::TqMmap => QuantizedVectorStorage::TQMmap(
                    EncodedVectorsTQ::load(&READ_FS, mmap()?, &meta_path)?,
                ),
                QuantizedStorageKind::TqChunked => QuantizedVectorStorage::TQChunkedMmap(
                    EncodedVectorsTQ::load(&READ_FS, chunked()?, &meta_path)?,
                ),
            };
        Ok(storage)
    }

    fn load_multi(
        config: &QuantizedVectorsConfig,
        path: &Path,
        multivector_config: &MultiVectorConfig,
        on_disk_vector_storage: bool,
    ) -> OperationResult<QuantizedVectorStorage> {
        let data_path = Self::get_data_path(path, config.storage_type);
        let meta_path = Self::get_meta_path(path);
        let offsets_path = Self::get_offsets_path(path, config.storage_type);
        let dim = config.vector_parameters.dim;
        let size = config.quantized_vector_size(true);
        let in_ram = config.is_ram(on_disk_vector_storage);

        // Open the inner quantized storage and the matching offsets storage for the
        // selected backend.
        let ram = || QuantizedRamStorage::open::<ReadFile>(&READ_FS, data_path.as_path(), size);
        let mmap = || QuantizedStorage::open(&READ_FS, data_path.as_path(), size);
        let chunked =
            || QuantizedChunkedStorage::<ReadFile>::new(READ_FS, data_path.as_path(), size, in_ram);
        let ram_offsets = || MultivectorOffsetsStorageRam::load(&offsets_path);
        let mmap_offsets = || MultivectorOffsetsStorageMmap::load(&offsets_path);
        let chunked_offsets =
            || MultivectorOffsetsStorageChunked::<ReadFile>::load(READ_FS, &offsets_path, in_ram);

        let storage = match config.storage_kind(on_disk_vector_storage)? {
            QuantizedStorageKind::ScalarRam => {
                let inner = EncodedVectorsU8::load(&READ_FS, ram()?, &meta_path)?;
                QuantizedVectorStorage::ScalarRamMulti(QuantizedMultivectorStorage::new(
                    dim,
                    inner,
                    ram_offsets()?,
                    *multivector_config,
                ))
            }
            QuantizedStorageKind::ScalarMmap => {
                let inner = EncodedVectorsU8::load(&READ_FS, mmap()?, &meta_path)?;
                QuantizedVectorStorage::ScalarMmapMulti(QuantizedMultivectorStorage::new(
                    dim,
                    inner,
                    mmap_offsets()?,
                    *multivector_config,
                ))
            }
            QuantizedStorageKind::PqRam => {
                let inner = EncodedVectorsPQ::load(&READ_FS, ram()?, &meta_path)?;
                QuantizedVectorStorage::PQRamMulti(QuantizedMultivectorStorage::new(
                    dim,
                    inner,
                    ram_offsets()?,
                    *multivector_config,
                ))
            }
            QuantizedStorageKind::PqMmap => {
                let inner = EncodedVectorsPQ::load(&READ_FS, mmap()?, &meta_path)?;
                QuantizedVectorStorage::PQMmapMulti(QuantizedMultivectorStorage::new(
                    dim,
                    inner,
                    mmap_offsets()?,
                    *multivector_config,
                ))
            }
            QuantizedStorageKind::BinaryRam => {
                let inner = EncodedVectorsBin::load(&READ_FS, ram()?, &meta_path)?;
                QuantizedVectorStorage::BinaryRamMulti(QuantizedMultivectorStorage::new(
                    dim,
                    inner,
                    ram_offsets()?,
                    *multivector_config,
                ))
            }
            QuantizedStorageKind::BinaryMmap => {
                let inner = EncodedVectorsBin::load(&READ_FS, mmap()?, &meta_path)?;
                QuantizedVectorStorage::BinaryMmapMulti(QuantizedMultivectorStorage::new(
                    dim,
                    inner,
                    mmap_offsets()?,
                    *multivector_config,
                ))
            }
            QuantizedStorageKind::BinaryChunked => {
                let inner = EncodedVectorsBin::load(&READ_FS, chunked()?, &meta_path)?;
                QuantizedVectorStorage::BinaryChunkedMmapMulti(QuantizedMultivectorStorage::new(
                    dim,
                    inner,
                    chunked_offsets()?,
                    *multivector_config,
                ))
            }
            QuantizedStorageKind::TqRam => {
                let inner = EncodedVectorsTQ::load(&READ_FS, ram()?, &meta_path)?;
                QuantizedVectorStorage::TQRamMulti(QuantizedMultivectorStorage::new(
                    dim,
                    inner,
                    ram_offsets()?,
                    *multivector_config,
                ))
            }
            QuantizedStorageKind::TqMmap => {
                let inner = EncodedVectorsTQ::load(&READ_FS, mmap()?, &meta_path)?;
                QuantizedVectorStorage::TQMmapMulti(QuantizedMultivectorStorage::new(
                    dim,
                    inner,
                    mmap_offsets()?,
                    *multivector_config,
                ))
            }
            QuantizedStorageKind::TqChunked => {
                let inner = EncodedVectorsTQ::load(&READ_FS, chunked()?, &meta_path)?;
                QuantizedVectorStorage::TQChunkedMmapMulti(QuantizedMultivectorStorage::new(
                    dim,
                    inner,
                    chunked_offsets()?,
                    *multivector_config,
                ))
            }
        };
        Ok(storage)
    }
}
