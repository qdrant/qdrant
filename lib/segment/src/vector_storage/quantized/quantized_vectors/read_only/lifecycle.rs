use std::path::Path;

use common::universal_io::{CachedReadFs, OkNotFound, UniversalRead, read_json_via};
use quantization::EncodedVectorsPQ;
use quantization::encoded_vectors_binary::EncodedVectorsBin;
use quantization::encoded_vectors_tq::EncodedVectorsTQ;
use quantization::encoded_vectors_u8::EncodedVectorsU8;

use super::ReadOnlyQuantizedVectors;
use super::storage::ReadOnlyQuantizedVectorStorage;
use crate::common::operation_error::OperationResult;
use crate::types::{Distance, MultiVectorConfig, VectorStorageDatatype};
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedStorageRead;
use crate::vector_storage::quantized::quantized_multivector_storage::{
    MultivectorOffsetsStorageChunkedRead, MultivectorOffsetsStorageMmap,
    MultivectorOffsetsStorageRam, QuantizedMultivectorStorage,
};
use crate::vector_storage::quantized::quantized_ram_storage::QuantizedRamStorage;
use crate::vector_storage::quantized::quantized_storage::QuantizedStorage;
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedStorageKind, QuantizedVectors, QuantizedVectorsConfig,
};

impl<S: UniversalRead> ReadOnlyQuantizedVectors<S> {
    /// Open existing quantized vectors read-only through the [`UniversalRead`] backend `S`.
    ///
    /// Returns `Ok(None)` when no quantization config is present at `path`. Every read —
    /// config, per-method metadata, quantized data and multivector offsets — goes through `S`;
    /// nothing is read with direct filesystem access and nothing is written. Both on-disk
    /// layouts are supported read-only: the immutable flat format and the appendable chunked
    /// format (the latter only produced by Binary/TurboQuant). Unlike
    /// [`QuantizedVectors::load`], this never creates or quantizes anything.
    ///
    /// `distance`, `datatype`, `multivector_config` and `on_disk_vector_storage` describe
    /// the original (source) vector storage this quantization was built for.
    pub fn open(
        fs: &CachedReadFs<S::Fs>,
        path: &Path,
        distance: Distance,
        datatype: VectorStorageDatatype,
        multivector_config: Option<&MultiVectorConfig>,
        on_disk_vector_storage: bool,
    ) -> OperationResult<Option<Self>> {
        let config_path = QuantizedVectors::get_config_path(path);
        let config: Option<QuantizedVectorsConfig> =
            read_json_via(fs, &config_path).ok_not_found()?;
        let Some(config) = config else {
            return Ok(None);
        };

        let storage_impl = match multivector_config {
            Some(multivector_config) => Self::open_multi(
                fs,
                path,
                &config,
                multivector_config,
                on_disk_vector_storage,
            )?,
            None => Self::open_single(fs, path, &config, on_disk_vector_storage)?,
        };

        Ok(Some(Self::new(
            storage_impl,
            config,
            path.to_path_buf(),
            distance,
            datatype,
        )))
    }

    fn open_single(
        fs: &CachedReadFs<S::Fs>,
        path: &Path,
        config: &QuantizedVectorsConfig,
        on_disk_vector_storage: bool,
    ) -> OperationResult<ReadOnlyQuantizedVectorStorage<S>> {
        let data_path = QuantizedVectors::get_data_path(path, config.storage_type);
        let meta_path = QuantizedVectors::get_meta_path(path);
        let size = config.quantized_vector_size(false);

        // Open the flat (RAM / mmap) or chunked storage selected for this config.
        let ram = || QuantizedRamStorage::from_file::<S>(fs, &data_path, size);
        let mmap = || QuantizedStorage::<S>::from_file(fs, &data_path, size);
        let chunked = || QuantizedChunkedStorageRead::<S>::open(fs, &data_path, size);

        let storage = match config.storage_kind(on_disk_vector_storage)? {
            QuantizedStorageKind::ScalarRam => ReadOnlyQuantizedVectorStorage::ScalarRam(
                EncodedVectorsU8::load(fs, ram()?, &meta_path)?,
            ),
            QuantizedStorageKind::ScalarMmap => ReadOnlyQuantizedVectorStorage::ScalarMmap(
                EncodedVectorsU8::load(fs, mmap()?, &meta_path)?,
            ),
            QuantizedStorageKind::PqRam => ReadOnlyQuantizedVectorStorage::PQRam(
                EncodedVectorsPQ::load(fs, ram()?, &meta_path)?,
            ),
            QuantizedStorageKind::PqMmap => ReadOnlyQuantizedVectorStorage::PQMmap(
                EncodedVectorsPQ::load(fs, mmap()?, &meta_path)?,
            ),
            QuantizedStorageKind::BinaryRam => ReadOnlyQuantizedVectorStorage::BinaryRam(
                EncodedVectorsBin::load(fs, ram()?, &meta_path)?,
            ),
            QuantizedStorageKind::BinaryMmap => ReadOnlyQuantizedVectorStorage::BinaryMmap(
                EncodedVectorsBin::load(fs, mmap()?, &meta_path)?,
            ),
            QuantizedStorageKind::BinaryChunked => ReadOnlyQuantizedVectorStorage::BinaryChunked(
                EncodedVectorsBin::load(fs, chunked()?, &meta_path)?,
            ),
            QuantizedStorageKind::TqRam => ReadOnlyQuantizedVectorStorage::TQRam(
                EncodedVectorsTQ::load(fs, ram()?, &meta_path)?,
            ),
            QuantizedStorageKind::TqMmap => ReadOnlyQuantizedVectorStorage::TQMmap(
                EncodedVectorsTQ::load(fs, mmap()?, &meta_path)?,
            ),
            QuantizedStorageKind::TqChunked => ReadOnlyQuantizedVectorStorage::TQChunked(
                EncodedVectorsTQ::load(fs, chunked()?, &meta_path)?,
            ),
        };
        Ok(storage)
    }

    fn open_multi(
        fs: &CachedReadFs<S::Fs>,
        path: &Path,
        config: &QuantizedVectorsConfig,
        multivector_config: &MultiVectorConfig,
        on_disk_vector_storage: bool,
    ) -> OperationResult<ReadOnlyQuantizedVectorStorage<S>> {
        let data_path = QuantizedVectors::get_data_path(path, config.storage_type);
        let meta_path = QuantizedVectors::get_meta_path(path);
        let offsets_path = QuantizedVectors::get_offsets_path(path, config.storage_type);
        let dim = config.vector_parameters.dim;
        let size = config.quantized_vector_size(true);

        // Open the inner quantized storage and the matching offsets storage for the
        // selected backend.
        let ram = || QuantizedRamStorage::from_file::<S>(fs, &data_path, size);
        let mmap = || QuantizedStorage::<S>::from_file(fs, &data_path, size);
        let chunked = || QuantizedChunkedStorageRead::<S>::open(fs, &data_path, size);
        let ram_offsets = || MultivectorOffsetsStorageRam::open::<S>(fs, &offsets_path);
        let mmap_offsets = || MultivectorOffsetsStorageMmap::<S>::open(fs, &offsets_path);
        let chunked_offsets = || MultivectorOffsetsStorageChunkedRead::<S>::open(fs, &offsets_path);

        let storage = match config.storage_kind(on_disk_vector_storage)? {
            QuantizedStorageKind::ScalarRam => {
                let inner = EncodedVectorsU8::load(fs, ram()?, &meta_path)?;
                ReadOnlyQuantizedVectorStorage::ScalarRamMulti(QuantizedMultivectorStorage::new(
                    dim,
                    inner,
                    ram_offsets()?,
                    *multivector_config,
                ))
            }
            QuantizedStorageKind::ScalarMmap => {
                let inner = EncodedVectorsU8::load(fs, mmap()?, &meta_path)?;
                ReadOnlyQuantizedVectorStorage::ScalarMmapMulti(QuantizedMultivectorStorage::new(
                    dim,
                    inner,
                    mmap_offsets()?,
                    *multivector_config,
                ))
            }
            QuantizedStorageKind::PqRam => {
                let inner = EncodedVectorsPQ::load(fs, ram()?, &meta_path)?;
                ReadOnlyQuantizedVectorStorage::PQRamMulti(QuantizedMultivectorStorage::new(
                    dim,
                    inner,
                    ram_offsets()?,
                    *multivector_config,
                ))
            }
            QuantizedStorageKind::PqMmap => {
                let inner = EncodedVectorsPQ::load(fs, mmap()?, &meta_path)?;
                ReadOnlyQuantizedVectorStorage::PQMmapMulti(QuantizedMultivectorStorage::new(
                    dim,
                    inner,
                    mmap_offsets()?,
                    *multivector_config,
                ))
            }
            QuantizedStorageKind::BinaryRam => {
                let inner = EncodedVectorsBin::load(fs, ram()?, &meta_path)?;
                ReadOnlyQuantizedVectorStorage::BinaryRamMulti(QuantizedMultivectorStorage::new(
                    dim,
                    inner,
                    ram_offsets()?,
                    *multivector_config,
                ))
            }
            QuantizedStorageKind::BinaryMmap => {
                let inner = EncodedVectorsBin::load(fs, mmap()?, &meta_path)?;
                ReadOnlyQuantizedVectorStorage::BinaryMmapMulti(QuantizedMultivectorStorage::new(
                    dim,
                    inner,
                    mmap_offsets()?,
                    *multivector_config,
                ))
            }
            QuantizedStorageKind::BinaryChunked => {
                let inner = EncodedVectorsBin::load(fs, chunked()?, &meta_path)?;
                ReadOnlyQuantizedVectorStorage::BinaryChunkedMulti(
                    QuantizedMultivectorStorage::new(
                        dim,
                        inner,
                        chunked_offsets()?,
                        *multivector_config,
                    ),
                )
            }
            QuantizedStorageKind::TqRam => {
                let inner = EncodedVectorsTQ::load(fs, ram()?, &meta_path)?;
                ReadOnlyQuantizedVectorStorage::TQRamMulti(QuantizedMultivectorStorage::new(
                    dim,
                    inner,
                    ram_offsets()?,
                    *multivector_config,
                ))
            }
            QuantizedStorageKind::TqMmap => {
                let inner = EncodedVectorsTQ::load(fs, mmap()?, &meta_path)?;
                ReadOnlyQuantizedVectorStorage::TQMmapMulti(QuantizedMultivectorStorage::new(
                    dim,
                    inner,
                    mmap_offsets()?,
                    *multivector_config,
                ))
            }
            QuantizedStorageKind::TqChunked => {
                let inner = EncodedVectorsTQ::load(fs, chunked()?, &meta_path)?;
                ReadOnlyQuantizedVectorStorage::TQChunkedMulti(QuantizedMultivectorStorage::new(
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
