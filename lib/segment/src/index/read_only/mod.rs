use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::low_memory::low_memory_mode;
use common::types::{ScoredPointOffset, TelemetryDetail};
use common::universal_io::{MmapFile, UniversalRead};
use half::f16;
use sparse::common::types::{DimId, QuantizedU8};
use sparse::index::inverted_index::InvertedIndex;
use sparse::index::inverted_index::inverted_index_compressed_immutable_ram::InvertedIndexCompressedImmutableRam;
use sparse::index::inverted_index::inverted_index_compressed_mmap::InvertedIndexCompressedMmap;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::query_context::VectorQueryContext;
use crate::data_types::vectors::QueryVector;
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::index::hnsw_index::hnsw::read_only::ReadOnlyHNSWIndex;
use crate::index::plain_vector_index::read_only::ReadOnlyPlainVectorIndex;
use crate::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use crate::index::sparse_index::sparse_vector_index::read_only::ReadOnlySparseVectorIndex;
use crate::index::struct_payload_index::read_only::ReadOnlyStructPayloadIndex;
use crate::index::vector_index_base::VectorIndexRead;
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{Filter, Indexes, SearchParams, VectorDataConfig, VectorStorageDatatype};
use crate::vector_storage::quantized::quantized_vectors::ReadOnlyQuantizedVectors;
use crate::vector_storage::read_only::VectorStorageReadEnum;

/// Read-only counterpart of [`super::VectorIndexEnum`].
///
/// Wraps each read-capable index type with its read-only newtype. The mutable
/// RAM sparse index is intentionally absent because it has no persisted
/// read-only representation.
pub enum VectorIndexReadEnum<S: UniversalRead> {
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
    SparseCompressedMmapF32(
        Box<ReadOnlySparseVectorIndex<S, InvertedIndexCompressedMmap<f32, MmapFile>>>,
    ),
    SparseCompressedMmapF16(
        Box<ReadOnlySparseVectorIndex<S, InvertedIndexCompressedMmap<f16, MmapFile>>>,
    ),
    SparseCompressedMmapU8(
        Box<ReadOnlySparseVectorIndex<S, InvertedIndexCompressedMmap<QuantizedU8, MmapFile>>>,
    ),
}

/// Shared read-only backends plus the `fs`/`path` an index opens its files from.
pub struct ReadOnlyVectorIndexOpenArgs<'a, S: UniversalRead> {
    pub fs: &'a S::Fs,
    pub path: &'a Path,
    pub id_tracker: Arc<AtomicRefCell<ReadOnlyIdTrackerEnum<S>>>,
    pub vector_storage: Arc<AtomicRefCell<VectorStorageReadEnum<S>>>,
    pub payload_index: Arc<AtomicRefCell<ReadOnlyStructPayloadIndex<S>>>,
    pub quantized_vectors: Arc<AtomicRefCell<Option<ReadOnlyQuantizedVectors<S>>>>,
}

impl<S: UniversalRead> VectorIndexReadEnum<S> {
    /// Open the read-only dense vector index from its config (sparse: follow-up).
    #[allow(dead_code)] // pending: read-only segment constructor
    pub fn open(
        vector_config: &VectorDataConfig,
        args: ReadOnlyVectorIndexOpenArgs<'_, S>,
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
    #[allow(dead_code)] // pending: read-only segment constructor
    pub fn open_sparse(args: ReadOnlyVectorIndexOpenArgs<'_, S>) -> OperationResult<Self> {
        let config = SparseIndexConfig::load(&SparseIndexConfig::get_config_path(args.path))?;

        // Low-memory mode downgrades `ImmutableRam` to `Mmap` (same on-disk format).
        let effective_index_type = match config.index_type {
            SparseIndexType::ImmutableRam if low_memory_mode().prefer_disk() => {
                SparseIndexType::Mmap
            }
            SparseIndexType::ImmutableRam => SparseIndexType::ImmutableRam,
            SparseIndexType::MutableRam => SparseIndexType::MutableRam,
            SparseIndexType::Mmap => SparseIndexType::Mmap,
        };

        let ReadOnlyVectorIndexOpenArgs {
            fs: _,
            path,
            id_tracker,
            vector_storage,
            payload_index,
            quantized_vectors: _,
        } = args;

        let index = match (effective_index_type, config.datatype.unwrap_or_default()) {
            (SparseIndexType::MutableRam, _) => {
                return Err(OperationError::service_error(
                    "MutableRam sparse index has no read-only representation",
                ));
            }
            (SparseIndexType::ImmutableRam, VectorStorageDatatype::Float32) => {
                Self::SparseCompressedImmutableRamF32(Box::new(ReadOnlySparseVectorIndex::open(
                    id_tracker,
                    vector_storage,
                    payload_index,
                    path,
                )?))
            }
            (SparseIndexType::Mmap, VectorStorageDatatype::Float32) => {
                Self::SparseCompressedMmapF32(Box::new(ReadOnlySparseVectorIndex::open(
                    id_tracker,
                    vector_storage,
                    payload_index,
                    path,
                )?))
            }
            (SparseIndexType::ImmutableRam, VectorStorageDatatype::Float16) => {
                Self::SparseCompressedImmutableRamF16(Box::new(ReadOnlySparseVectorIndex::open(
                    id_tracker,
                    vector_storage,
                    payload_index,
                    path,
                )?))
            }
            (SparseIndexType::Mmap, VectorStorageDatatype::Float16) => {
                Self::SparseCompressedMmapF16(Box::new(ReadOnlySparseVectorIndex::open(
                    id_tracker,
                    vector_storage,
                    payload_index,
                    path,
                )?))
            }
            (SparseIndexType::ImmutableRam, VectorStorageDatatype::Uint8) => {
                Self::SparseCompressedImmutableRamU8(Box::new(ReadOnlySparseVectorIndex::open(
                    id_tracker,
                    vector_storage,
                    payload_index,
                    path,
                )?))
            }
            (SparseIndexType::Mmap, VectorStorageDatatype::Uint8) => {
                Self::SparseCompressedMmapU8(Box::new(ReadOnlySparseVectorIndex::open(
                    id_tracker,
                    vector_storage,
                    payload_index,
                    path,
                )?))
            }
            (_, VectorStorageDatatype::Turbo4) => {
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
            Self::SparseCompressedMmapF32(index) => index.inverted_index().is_on_disk(),
            Self::SparseCompressedMmapF16(index) => index.inverted_index().is_on_disk(),
            Self::SparseCompressedMmapU8(index) => index.inverted_index().is_on_disk(),
        }
    }

    pub fn populate(&self) -> OperationResult<()> {
        match self {
            Self::Plain(_) => {}
            Self::Hnsw(index) => index.populate()?,
            Self::SparseCompressedImmutableRamF32(_) => {}
            Self::SparseCompressedImmutableRamF16(_) => {}
            Self::SparseCompressedImmutableRamU8(_) => {}
            Self::SparseCompressedMmapF32(index) => index.inverted_index().populate()?,
            Self::SparseCompressedMmapF16(index) => index.inverted_index().populate()?,
            Self::SparseCompressedMmapU8(index) => index.inverted_index().populate()?,
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
            Self::SparseCompressedMmapF32(index) => index.inverted_index().clear_cache()?,
            Self::SparseCompressedMmapF16(index) => index.inverted_index().clear_cache()?,
            Self::SparseCompressedMmapU8(index) => index.inverted_index().clear_cache()?,
        };
        Ok(())
    }
}

impl<S: UniversalRead> VectorIndexRead for VectorIndexReadEnum<S> {
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
            Self::SparseCompressedMmapF32(index) => {
                index.search(vectors, filter, top, params, query_context)
            }
            Self::SparseCompressedMmapF16(index) => {
                index.search(vectors, filter, top, params, query_context)
            }
            Self::SparseCompressedMmapU8(index) => {
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
            Self::SparseCompressedMmapF32(index) => index.get_telemetry_data(detail),
            Self::SparseCompressedMmapF16(index) => index.get_telemetry_data(detail),
            Self::SparseCompressedMmapU8(index) => index.get_telemetry_data(detail),
        }
    }

    fn indexed_vector_count(&self) -> usize {
        match self {
            Self::Plain(index) => index.indexed_vector_count(),
            Self::Hnsw(index) => index.indexed_vector_count(),
            Self::SparseCompressedImmutableRamF32(index) => index.indexed_vector_count(),
            Self::SparseCompressedImmutableRamF16(index) => index.indexed_vector_count(),
            Self::SparseCompressedImmutableRamU8(index) => index.indexed_vector_count(),
            Self::SparseCompressedMmapF32(index) => index.indexed_vector_count(),
            Self::SparseCompressedMmapF16(index) => index.indexed_vector_count(),
            Self::SparseCompressedMmapU8(index) => index.indexed_vector_count(),
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
            Self::SparseCompressedMmapF32(index) => index.size_of_searchable_vectors_in_bytes(),
            Self::SparseCompressedMmapF16(index) => index.size_of_searchable_vectors_in_bytes(),
            Self::SparseCompressedMmapU8(index) => index.size_of_searchable_vectors_in_bytes(),
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
            Self::SparseCompressedMmapF32(index) => index.fill_idf_statistics(idf, hw_counter),
            Self::SparseCompressedMmapF16(index) => index.fill_idf_statistics(idf, hw_counter),
            Self::SparseCompressedMmapU8(index) => index.fill_idf_statistics(idf, hw_counter),
        }
    }

    fn is_index(&self) -> bool {
        match self {
            Self::Plain(index) => index.is_index(),
            Self::Hnsw(index) => index.is_index(),
            Self::SparseCompressedImmutableRamF32(index) => index.is_index(),
            Self::SparseCompressedImmutableRamF16(index) => index.is_index(),
            Self::SparseCompressedImmutableRamU8(index) => index.is_index(),
            Self::SparseCompressedMmapF32(index) => index.is_index(),
            Self::SparseCompressedMmapF16(index) => index.is_index(),
            Self::SparseCompressedMmapU8(index) => index.is_index(),
        }
    }
}
