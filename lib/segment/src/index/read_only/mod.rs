mod live_reload;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::low_memory::low_memory_mode;
use common::storage_version::VERSION_FILE;
use common::types::{ScoredPointOffset, TelemetryDetail};
use common::universal_io::{CachedReadFs, OkNotFound, Populate, UniversalReadFs};
use half::f16;
use sparse::common::types::{DimId, QuantizedU8};
use sparse::index::inverted_index::inverted_index_compressed_immutable_ram::InvertedIndexCompressedImmutableRam;
use sparse::index::inverted_index::inverted_index_compressed_mmap::{
    self as inverted_index_compressed_mmap, InvertedIndexCompressedMmap,
};
use sparse::index::inverted_index::{InvertedIndex, InvertedIndexReadOnly};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::query_context::VectorQueryContext;
use crate::data_types::vectors::QueryVector;
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::index::UniversalReadExt;
use crate::index::hnsw_index::hnsw::read_only::ReadOnlyHNSWIndex;
use crate::index::plain_vector_index::read_only::ReadOnlyPlainVectorIndex;
use crate::index::sparse_index::indices_tracker::IndicesTracker;
use crate::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use crate::index::sparse_index::sparse_vector_index::read_only::{
    ReadOnlySparseVectorIndex, ReadOnlySparseVectorIndexOpenArgs,
};
use crate::index::struct_payload_index::read_only::ReadOnlyStructPayloadIndex;
use crate::index::vector_index_base::VectorIndexRead;
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{
    Filter, Indexes, SearchParams, SparseVectorDataConfig, VectorDataConfig, VectorStorageDatatype,
};
use crate::vector_storage::quantized::quantized_vectors::ReadOnlyQuantizedVectors;
use crate::vector_storage::read_only::VectorStorageReadEnum;

/// Read-only counterpart of [`super::VectorIndexEnum`].
///
/// Wraps each read-capable index type with its read-only newtype. The mutable
/// RAM sparse index is intentionally absent because it has no persisted
/// read-only representation.
pub enum VectorIndexReadEnum<S: UniversalReadExt + 'static> {
    Plain(Box<ReadOnlyPlainVectorIndex<S>>),
    Hnsw(Box<ReadOnlyHNSWIndex<S>>),
    SparseCompressedImmutableRamF32(
        Box<ReadOnlySparseVectorIndex<S, InvertedIndexCompressedImmutableRam<f32>>>,
    ),
    SparseCompressedImmutableRamF16(
        Box<ReadOnlySparseVectorIndex<S, InvertedIndexCompressedImmutableRam<f16>>>,
    ),
    SparseCompressedImmutableRamU8(
        Box<ReadOnlySparseVectorIndex<S, InvertedIndexCompressedImmutableRam<QuantizedU8>>>,
    ),
    SparseCompressedStoredF32(
        Box<ReadOnlySparseVectorIndex<S, InvertedIndexCompressedMmap<f32, S>>>,
    ),
    SparseCompressedStoredF16(
        Box<ReadOnlySparseVectorIndex<S, InvertedIndexCompressedMmap<f16, S>>>,
    ),
    SparseCompressedStoredU8(
        Box<ReadOnlySparseVectorIndex<S, InvertedIndexCompressedMmap<QuantizedU8, S>>>,
    ),
}

/// Shared read-only backends plus the `fs`/`path` an index opens its files from.
pub struct ReadOnlyVectorIndexOpenArgs<
    'a,
    S: UniversalReadExt + 'static,
    Fs: UniversalReadFs<File = S>,
> {
    pub fs: &'a Fs,
    pub path: &'a Path,
    pub id_tracker: Arc<AtomicRefCell<ReadOnlyIdTrackerEnum<S>>>,
    pub vector_storage: Arc<AtomicRefCell<VectorStorageReadEnum<S>>>,
    pub payload_index: Arc<AtomicRefCell<ReadOnlyStructPayloadIndex<S>>>,
    pub quantized_vectors: Arc<AtomicRefCell<Option<ReadOnlyQuantizedVectors<S>>>>,
}

impl<S: UniversalReadExt + 'static> VectorIndexReadEnum<S> {
    /// Schedule background prefetch of every file [`Self::open`] will read,
    /// dispatching on `vector_config` the same way. The plain index opens no
    /// files.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        vector_config: &VectorDataConfig,
        path: &Path,
    ) -> OperationResult<()> {
        match &vector_config.index {
            Indexes::Plain {} => Ok(()),
            Indexes::Hnsw(hnsw_config) => ReadOnlyHNSWIndex::<S>::preopen(fs, path, hnsw_config),
        }
    }

    /// Schedule background prefetch of every file [`Self::open_sparse`] will
    /// read: the sparse index config, the inverted index, its version file
    /// and the indices tracker. All readable variants share the
    /// compressed-mmap on-disk format, so the datatype plays no role here;
    /// the (effective) index type only decides whether the index data is
    /// populated by the prefetch (the immutable-RAM open reads it in full) or
    /// parked cold (the mmap open reads it lazily) — and it comes from the
    /// segment config's `sparse_vector_config`, so nothing is read here.
    ///
    /// An absent index config file means the index isn't persisted: nothing
    /// is scheduled, and `open_sparse` is the one to report it.
    pub fn preopen_sparse(
        fs: &impl CachedReadFs<File = S>,
        sparse_vector_config: &SparseVectorDataConfig,
        path: &Path,
    ) -> OperationResult<()> {
        // Sparse index config; `open_sparse` reads it off the parked handle.
        let config_path = SparseIndexConfig::get_config_path(path);
        if fs
            .schedule_prefetch(&config_path, None, None)
            .ok_not_found()?
            .is_none()
        {
            return Ok(());
        }

        // Low-memory mode downgrades `ImmutableRam` to `Mmap` (same on-disk format).
        let effective_index_type = match sparse_vector_config.index.index_type {
            SparseIndexType::ImmutableRam if low_memory_mode().prefer_disk() => {
                SparseIndexType::Mmap
            }
            SparseIndexType::ImmutableRam => SparseIndexType::ImmutableRam,
            SparseIndexType::MutableRam => SparseIndexType::MutableRam,
            SparseIndexType::Mmap => SparseIndexType::Mmap,
        };

        let populate = match effective_index_type {
            SparseIndexType::MutableRam => {
                return Err(OperationError::service_error(
                    "MutableRam sparse index has no read-only representation",
                ));
            }
            SparseIndexType::ImmutableRam => Populate::PreferBackground,
            SparseIndexType::Mmap => Populate::No,
        };

        // Inverted index
        inverted_index_compressed_mmap::preopen(fs, path, populate)?;

        // Version check
        fs.schedule_prefetch(&path.join(VERSION_FILE), None, None)?;

        // Indices tracker
        fs.schedule_prefetch(&IndicesTracker::file_path(path), None, None)?;

        Ok(())
    }

    /// Open the read-only dense vector index from its config (sparse: follow-up).
    pub fn open<Fs: UniversalReadFs<File = S>>(
        vector_config: &VectorDataConfig,
        args: ReadOnlyVectorIndexOpenArgs<'_, S, Fs>,
    ) -> OperationResult<Self>
    where
        // The HNSW graph keeps its universal-IO storage handle alive behind a
        // boxed trait object, which must outlive the index.
        S: 'static,
    {
        let ReadOnlyVectorIndexOpenArgs {
            fs,
            path,
            id_tracker,
            vector_storage,
            payload_index,
            quantized_vectors,
        } = args;
        Ok(match &vector_config.index {
            Indexes::Plain {} => Self::Plain(Box::new(ReadOnlyPlainVectorIndex::open(
                id_tracker,
                vector_storage,
                quantized_vectors,
                payload_index,
            )?)),
            Indexes::Hnsw(hnsw_config) => Self::Hnsw(Box::new(ReadOnlyHNSWIndex::open(
                fs,
                path,
                id_tracker,
                vector_storage,
                quantized_vectors,
                payload_index,
                *hnsw_config,
            )?)),
        })
    }

    /// Open the read-only sparse vector index from its persisted [`SparseIndexConfig`],
    /// mirroring `create_sparse_vector_index`'s `(index_type, datatype)` selection.
    /// `MutableRam` has no read-only representation.
    pub fn open_sparse<Fs: UniversalReadFs<File = S>>(
        args: ReadOnlyVectorIndexOpenArgs<'_, S, Fs>,
    ) -> OperationResult<Self> {
        let ReadOnlyVectorIndexOpenArgs {
            fs,
            path,
            id_tracker,
            vector_storage,
            payload_index,
            quantized_vectors: _,
        } = args;

        let config =
            SparseIndexConfig::load_universal(fs, &SparseIndexConfig::get_config_path(path))?;

        // Low-memory mode downgrades `ImmutableRam` to `Mmap` (same on-disk format).
        let effective_index_type = match config.index_type {
            SparseIndexType::ImmutableRam if low_memory_mode().prefer_disk() => {
                SparseIndexType::Mmap
            }
            SparseIndexType::ImmutableRam => SparseIndexType::ImmutableRam,
            SparseIndexType::MutableRam => SparseIndexType::MutableRam,
            SparseIndexType::Mmap => SparseIndexType::Mmap,
        };

        let args = ReadOnlySparseVectorIndexOpenArgs {
            fs,
            config,
            id_tracker,
            vector_storage,
            payload_index,
            path,
        };

        fn open<S, Fs, TInvertedIndex>(
            args: ReadOnlySparseVectorIndexOpenArgs<'_, S, Fs>,
        ) -> OperationResult<Box<ReadOnlySparseVectorIndex<S, TInvertedIndex>>>
        where
            S: UniversalReadExt + 'static,
            Fs: UniversalReadFs<File = S>,
            TInvertedIndex: InvertedIndexReadOnly<S>,
        {
            Ok(Box::new(ReadOnlySparseVectorIndex::open(args)?))
        }

        let index = match (effective_index_type, config.datatype.unwrap_or_default()) {
            (SparseIndexType::MutableRam, _) => {
                return Err(OperationError::service_error(
                    "MutableRam sparse index has no read-only representation",
                ));
            }
            (SparseIndexType::ImmutableRam, VectorStorageDatatype::Float32) => {
                Self::SparseCompressedImmutableRamF32(open(args)?)
            }
            (SparseIndexType::Mmap, VectorStorageDatatype::Float32) => {
                Self::SparseCompressedStoredF32(open(args)?)
            }
            (SparseIndexType::ImmutableRam, VectorStorageDatatype::Float16) => {
                Self::SparseCompressedImmutableRamF16(open(args)?)
            }
            (SparseIndexType::Mmap, VectorStorageDatatype::Float16) => {
                Self::SparseCompressedStoredF16(open(args)?)
            }
            (SparseIndexType::ImmutableRam, VectorStorageDatatype::Uint8) => {
                Self::SparseCompressedImmutableRamU8(open(args)?)
            }
            (SparseIndexType::Mmap, VectorStorageDatatype::Uint8) => {
                Self::SparseCompressedStoredU8(open(args)?)
            }
            (
                SparseIndexType::ImmutableRam | SparseIndexType::Mmap,
                VectorStorageDatatype::Turbo4,
            ) => {
                return Err(OperationError::service_error(
                    "Turbo4 datatype storage is not yet supported",
                ));
            }
        };
        Ok(index)
    }

    /// Returns true if underlying index files are configured to stay on disk.
    pub fn is_on_disk(&self) -> bool {
        match self {
            Self::Plain(_) => false,
            Self::Hnsw(index) => index.is_on_disk(),
            Self::SparseCompressedImmutableRamF32(index) => index.inverted_index().is_on_disk(),
            Self::SparseCompressedImmutableRamF16(index) => index.inverted_index().is_on_disk(),
            Self::SparseCompressedImmutableRamU8(index) => index.inverted_index().is_on_disk(),
            Self::SparseCompressedStoredF32(index) => index.inverted_index().is_on_disk(),
            Self::SparseCompressedStoredF16(index) => index.inverted_index().is_on_disk(),
            Self::SparseCompressedStoredU8(index) => index.inverted_index().is_on_disk(),
        }
    }

    pub fn populate(&self) -> OperationResult<()> {
        match self {
            Self::Plain(_) => {}
            Self::Hnsw(index) => index.populate()?,
            Self::SparseCompressedImmutableRamF32(_) => {}
            Self::SparseCompressedImmutableRamF16(_) => {}
            Self::SparseCompressedImmutableRamU8(_) => {}
            Self::SparseCompressedStoredF32(index) => index.inverted_index().populate()?,
            Self::SparseCompressedStoredF16(index) => index.inverted_index().populate()?,
            Self::SparseCompressedStoredU8(index) => index.inverted_index().populate()?,
        };
        Ok(())
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            Self::Plain(_) => {}
            Self::Hnsw(index) => index.clear_cache()?,
            Self::SparseCompressedImmutableRamF32(_) => {}
            Self::SparseCompressedImmutableRamF16(_) => {}
            Self::SparseCompressedImmutableRamU8(_) => {}
            Self::SparseCompressedStoredF32(index) => index.inverted_index().clear_cache()?,
            Self::SparseCompressedStoredF16(index) => index.inverted_index().clear_cache()?,
            Self::SparseCompressedStoredU8(index) => index.inverted_index().clear_cache()?,
        };
        Ok(())
    }
}

impl<S: UniversalReadExt + 'static> VectorIndexRead for VectorIndexReadEnum<S> {
    fn search(
        &self,
        vectors: &[&QueryVector],
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        match self {
            Self::Plain(index) => index.search(vectors, filter, top, params, query_context),
            Self::Hnsw(index) => index.search(vectors, filter, top, params, query_context),
            Self::SparseCompressedImmutableRamF32(index) => {
                index.search(vectors, filter, top, params, query_context)
            }
            Self::SparseCompressedImmutableRamF16(index) => {
                index.search(vectors, filter, top, params, query_context)
            }
            Self::SparseCompressedImmutableRamU8(index) => {
                index.search(vectors, filter, top, params, query_context)
            }
            Self::SparseCompressedStoredF32(index) => {
                index.search(vectors, filter, top, params, query_context)
            }
            Self::SparseCompressedStoredF16(index) => {
                index.search(vectors, filter, top, params, query_context)
            }
            Self::SparseCompressedStoredU8(index) => {
                index.search(vectors, filter, top, params, query_context)
            }
        }
    }

    fn get_telemetry_data(&self, detail: TelemetryDetail) -> VectorIndexSearchesTelemetry {
        match self {
            Self::Plain(index) => index.get_telemetry_data(detail),
            Self::Hnsw(index) => index.get_telemetry_data(detail),
            Self::SparseCompressedImmutableRamF32(index) => index.get_telemetry_data(detail),
            Self::SparseCompressedImmutableRamF16(index) => index.get_telemetry_data(detail),
            Self::SparseCompressedImmutableRamU8(index) => index.get_telemetry_data(detail),
            Self::SparseCompressedStoredF32(index) => index.get_telemetry_data(detail),
            Self::SparseCompressedStoredF16(index) => index.get_telemetry_data(detail),
            Self::SparseCompressedStoredU8(index) => index.get_telemetry_data(detail),
        }
    }

    fn indexed_vector_count(&self) -> usize {
        match self {
            Self::Plain(index) => index.indexed_vector_count(),
            Self::Hnsw(index) => index.indexed_vector_count(),
            Self::SparseCompressedImmutableRamF32(index) => index.indexed_vector_count(),
            Self::SparseCompressedImmutableRamF16(index) => index.indexed_vector_count(),
            Self::SparseCompressedImmutableRamU8(index) => index.indexed_vector_count(),
            Self::SparseCompressedStoredF32(index) => index.indexed_vector_count(),
            Self::SparseCompressedStoredF16(index) => index.indexed_vector_count(),
            Self::SparseCompressedStoredU8(index) => index.indexed_vector_count(),
        }
    }

    fn size_of_searchable_vectors_in_bytes(&self) -> usize {
        match self {
            Self::Plain(index) => index.size_of_searchable_vectors_in_bytes(),
            Self::Hnsw(index) => index.size_of_searchable_vectors_in_bytes(),
            Self::SparseCompressedImmutableRamF32(index) => {
                index.size_of_searchable_vectors_in_bytes()
            }
            Self::SparseCompressedImmutableRamF16(index) => {
                index.size_of_searchable_vectors_in_bytes()
            }
            Self::SparseCompressedImmutableRamU8(index) => {
                index.size_of_searchable_vectors_in_bytes()
            }
            Self::SparseCompressedStoredF32(index) => index.size_of_searchable_vectors_in_bytes(),
            Self::SparseCompressedStoredF16(index) => index.size_of_searchable_vectors_in_bytes(),
            Self::SparseCompressedStoredU8(index) => index.size_of_searchable_vectors_in_bytes(),
        }
    }

    fn fill_idf_statistics(
        &self,
        idf: &mut HashMap<DimId, usize>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            Self::Plain(index) => index.fill_idf_statistics(idf, hw_counter),
            Self::Hnsw(index) => index.fill_idf_statistics(idf, hw_counter),
            Self::SparseCompressedImmutableRamF32(index) => {
                index.fill_idf_statistics(idf, hw_counter)
            }
            Self::SparseCompressedImmutableRamF16(index) => {
                index.fill_idf_statistics(idf, hw_counter)
            }
            Self::SparseCompressedImmutableRamU8(index) => {
                index.fill_idf_statistics(idf, hw_counter)
            }
            Self::SparseCompressedStoredF32(index) => index.fill_idf_statistics(idf, hw_counter),
            Self::SparseCompressedStoredF16(index) => index.fill_idf_statistics(idf, hw_counter),
            Self::SparseCompressedStoredU8(index) => index.fill_idf_statistics(idf, hw_counter),
        }
    }

    fn is_index(&self) -> bool {
        match self {
            Self::Plain(index) => index.is_index(),
            Self::Hnsw(index) => index.is_index(),
            Self::SparseCompressedImmutableRamF32(index) => index.is_index(),
            Self::SparseCompressedImmutableRamF16(index) => index.is_index(),
            Self::SparseCompressedImmutableRamU8(index) => index.is_index(),
            Self::SparseCompressedStoredF32(index) => index.is_index(),
            Self::SparseCompressedStoredF16(index) => index.is_index(),
            Self::SparseCompressedStoredU8(index) => index.is_index(),
        }
    }
}
