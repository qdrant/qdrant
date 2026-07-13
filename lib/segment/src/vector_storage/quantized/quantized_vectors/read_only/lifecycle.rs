use std::path::Path;

use common::universal_io::{
    CachedReadFs, OkNotFound, Populate, UniversalRead, UniversalReadFs, read_json_via,
};
use quantization::EncodedVectorsPQ;
use quantization::encoded_vectors_binary::EncodedVectorsBin;
use quantization::encoded_vectors_tq::EncodedVectorsTQ;
use quantization::encoded_vectors_u8::EncodedVectorsU8;
use strum::IntoEnumIterator;

use super::ReadOnlyQuantizedVectors;
use super::storage::ReadOnlyQuantizedVectorStorage;
use crate::common::operation_error::OperationResult;
use crate::types::{Distance, Memory, MultiVectorConfig, VectorDataConfig, VectorStorageDatatype};
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedStorageRead;
use crate::vector_storage::quantized::quantized_multivector_storage::{
    MultivectorOffsetsStorageChunkedRead, MultivectorOffsetsStorageMmap,
    MultivectorOffsetsStorageRam, QuantizedMultivectorStorage,
};
use crate::vector_storage::quantized::quantized_ram_storage::QuantizedRamStorage;
use crate::vector_storage::quantized::quantized_storage::QuantizedStorage;
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedStorageKind, QuantizedVectors, QuantizedVectorsConfig, QuantizedVectorsStorageType,
};

impl<S: UniversalRead> ReadOnlyQuantizedVectors<S> {
    /// Schedule background prefetch of every file [`Self::open`] will read —
    /// without reading the quantization config first.
    ///
    /// Waiting on the config would put a round-trip on the critical path, so
    /// the persisted layout is probed against the filesystem's listing
    /// snapshot instead: both layout candidates (the immutable flat files and
    /// the appendable chunked directories) are scheduled, and a segment only
    /// ever contains the one the real config selects. The config itself is
    /// scheduled for `open` to consume; an absent config means quantization
    /// isn't built, like `open`'s `Ok(None)`.
    ///
    /// `vector_config` is the segment-side config of the quantized vector: a
    /// no-op when it carries no quantization config. Its effective placement
    /// decides warmth: `pinned` selects the RAM loaders (which read the data
    /// in full on open), `cached` keeps the data mmap-backed with the page
    /// cache primed, `cold` reads lazily except the first vector, which the
    /// load reads to validate the stored vector size.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        path: &Path,
        vector_config: &VectorDataConfig,
    ) -> OperationResult<()> {
        let Some(quantization_config) = &vector_config.quantization_config else {
            return Ok(());
        };
        let multivector = vector_config.multivector_config.is_some();
        let on_disk_vector_storage = vector_config.storage_type.is_on_disk();

        // Config; `open` reads it off the parked handle.
        let config_path = QuantizedVectors::get_config_path(path);
        if fs
            .schedule_prefetch(&config_path, None, None)
            .ok_not_found()?
            .is_none()
        {
            return Ok(());
        }

        // Per-method metadata
        fs.schedule_prefetch(&QuantizedVectors::get_meta_path(path), None, None)?;

        let placement = QuantizedVectors::memory_placement(
            quantization_config.memory_placement(),
            on_disk_vector_storage,
        );
        let populate = match placement {
            Memory::Cached | Memory::Pinned => Populate::PreferBackground,
            Memory::Cold => Populate::No,
        };

        // The load reads the first vector off the data (from the first chunk,
        // in the chunked layout) to validate the stored vector size; populate
        // that prefix, so the validation doesn't cost a round-trip. The size
        // is derived from the segment config alone, exactly like the build.
        let vector_parameters = QuantizedVectors::construct_vector_parameters(
            quantization_config,
            vector_config.distance,
            vector_config.size,
            // The count is irrelevant to the vector size.
            0,
            QuantizedVectorsStorageType::Mutable,
        );
        let first_vector_size = QuantizedVectors::quantized_vector_size(
            quantization_config,
            &vector_parameters,
            multivector,
        );
        let data_populate = populate.or_partial(0..first_vector_size as u64);

        // Probe the layout candidates of every storage type. The file names
        // depend only on the storage type — not on the quantization method —
        // and the exhaustive iteration guarantees full coverage: a storage
        // type (and so a layout) cannot be added without the compiler
        // pointing here.
        for storage_type in QuantizedVectorsStorageType::iter() {
            let data_path = QuantizedVectors::get_data_path(path, storage_type);
            let offsets_path = QuantizedVectors::get_offsets_path(path, storage_type);
            match storage_type {
                // Immutable (flat) files
                QuantizedVectorsStorageType::Immutable => {
                    if fs.exists(&data_path)? {
                        match placement {
                            Memory::Pinned => QuantizedRamStorage::preopen(fs, &data_path)?,
                            Memory::Cached | Memory::Cold => {
                                QuantizedStorage::<S>::preopen(fs, &data_path, data_populate)?
                            }
                        }
                    }
                    if multivector && fs.exists(&offsets_path)? {
                        match placement {
                            Memory::Pinned => {
                                MultivectorOffsetsStorageRam::preopen(fs, &offsets_path)?
                            }
                            Memory::Cached | Memory::Cold => {
                                MultivectorOffsetsStorageMmap::<S>::preopen(
                                    fs,
                                    &offsets_path,
                                    populate,
                                )?
                            }
                        }
                    }
                }
                // Appendable (chunked) directories; absent ones are tolerated
                // — they schedule nothing.
                QuantizedVectorsStorageType::Mutable => {
                    QuantizedChunkedStorageRead::<S>::preopen(fs, &data_path, data_populate)
                        .ok_not_found()?;
                    if multivector {
                        MultivectorOffsetsStorageChunkedRead::<S>::preopen(
                            fs,
                            &offsets_path,
                            populate,
                        )
                        .ok_not_found()?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Open existing quantized vectors read-only through the [`UniversalRead`] backend `S`.
    ///
    /// Returns `Ok(None)` when no quantization config is present at `path`. Every read —
    /// config, per-method metadata, quantized data and multivector offsets — goes through
    /// `S`; nothing is read with direct filesystem access and nothing is written. Both
    /// on-disk layouts are supported read-only: the immutable flat format and the appendable
    /// chunked format (the latter only produced by Binary/TurboQuant). Unlike
    /// [`QuantizedVectors::load`], this never creates or quantizes anything.
    ///
    /// `distance`, `datatype`, `multivector_config` and `on_disk_vector_storage` describe
    /// the original (source) vector storage this quantization was built for.
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
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

        let memory_placement = config.memory_placement(on_disk_vector_storage);

        let quantized_vectors =
            Self::new(storage_impl, config, path.to_path_buf(), distance, datatype);

        // For the cached placement quantized vectors stay mmap-backed, but the
        // page cache is primed on load — mirroring [`QuantizedVectors::load`].
        if memory_placement == Memory::Cached {
            quantized_vectors.populate()?;
        }

        Ok(Some(quantized_vectors))
    }

    fn open_single(
        fs: &impl UniversalReadFs<File = S>,
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
        fs: &impl UniversalReadFs<File = S>,
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
