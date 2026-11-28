use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use common::types::{PointOffsetType, ScoredPointOffset, TelemetryDetail};
use common::universal_io::MmapFile;
use half::f16;
use sparse::common::types::{DimId, QuantizedU8};
use sparse::index::inverted_index::InvertedIndex;
use sparse::index::inverted_index::inverted_index_compressed_immutable_ram::InvertedIndexCompressedImmutableRam;
use sparse::index::inverted_index::inverted_index_compressed_mmap::InvertedIndexCompressedMmap;
use sparse::index::inverted_index::inverted_index_ram::InvertedIndexRam;

use super::hnsw_index::hnsw::HNSWIndex;
use super::plain_vector_index::PlainVectorIndex;
use super::sparse_index::sparse_vector_index::SparseVectorIndex;
use crate::common::operation_error::OperationResult;
use crate::data_types::query_context::VectorQueryContext;
use crate::data_types::vectors::{QueryVector, VectorRef};
use crate::index::field_index::CardinalityEstimation;
use crate::index::query_optimization::filter_checks::FilterChecks;
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{Filter, SearchParams};

/// Read-only trait for vector index.
///
/// Defines all read operations on a vector index. Search and retrieval logic
/// only requires this trait, which makes it possible to implement read-only
/// segments without duplicating index code.
pub trait VectorIndexRead {
    /// Return list of Ids with fitting
    fn search(
        &self,
        vectors: &[&QueryVector],
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>>;

    /// How [`Self::search`] of `query_vectors` would use a filter that matches about
    /// `cardinality` points: evaluate it on every candidate, or check points lazily as it
    /// visits them. Makes the same choice of strategy as [`Self::search`].
    ///
    /// No default on purpose: a new index must state how it evaluates filters.
    fn filter_checks(
        &self,
        query_vectors: &[QueryVector],
        cardinality: &CardinalityEstimation,
        top: usize,
        params: Option<&SearchParams>,
    ) -> OperationResult<FilterChecks>;

    fn get_telemetry_data(&self, detail: TelemetryDetail) -> VectorIndexSearchesTelemetry;

    /// The number of indexed vectors, currently accessible
    fn indexed_vector_count(&self) -> usize;

    /// Total size of all searchable vectors in bytes.
    fn size_of_searchable_vectors_in_bytes(&self) -> usize;

    /// Augment the IDF stats for the given dimensions over the given corpus
    /// and return the number of documents contributing to them: indexed
    /// vectors matching the corpus filter, or all indexed vectors when
    /// `corpus` is `None` (global statistics).
    ///
    /// Most indexes don't track IDF and should contribute no df counts.
    /// Sparse-vector indexes are the only ones that contribute. No default is
    /// provided on purpose so a new index implementation cannot silently skip
    /// this.
    fn fill_idf_statistics(
        &self,
        idf: &mut HashMap<DimId, usize>,
        corpus: Option<&Filter>,
        is_stopped: &AtomicBool,
    ) -> OperationResult<usize>;

    /// Whether this is a "real" index rather than a plain (full-scan) one.
    ///
    /// Used by reporting code to decide whether to count vectors as indexed.
    fn is_index(&self) -> bool;
}

/// Trait for vector index with mutating operations.
pub trait VectorIndex: VectorIndexRead {
    fn files(&self) -> Vec<PathBuf>;

    fn immutable_files(&self) -> Vec<PathBuf> {
        Vec::new()
    }

    /// Update index for a single vector
    ///
    /// # Arguments
    /// - `id` - sequential vector id, offset in the vector storage
    /// - `vector` - new vector value,
    ///   if None - vector will be removed from the index marked as deleted in storage.
    ///   Note: inserting None vector is not equal to removing vector from the storage.
    ///   Unlike removing, it will always result in storage growth.
    ///   Proper removing should be performed by the optimizer.
    fn update_vector(
        &mut self,
        id: PointOffsetType,
        vector: Option<VectorRef>,
    ) -> OperationResult<()>;

    /// Byte-blob analogue of [`VectorIndex::update_vector`]: `vector` is the
    /// storage-native serialized form (the [`retrieve_raw`] format), letting
    /// requantized storages ingest bytes verbatim instead of a lossy
    /// decode/re-encode round-trip. `None` behaves exactly like
    /// `update_vector(id, None, _)`.
    ///
    /// [`retrieve_raw`]: crate::entry::entry_point::ReadSegmentEntry::retrieve_raw
    fn update_vector_raw(
        &mut self,
        id: PointOffsetType,
        vector: Option<&[u8]>,
    ) -> OperationResult<()>;
}

#[derive(Debug)]
pub enum VectorIndexEnum {
    Plain(PlainVectorIndex),
    Hnsw(HNSWIndex),
    SparseRam(SparseVectorIndex<InvertedIndexRam>),
    SparseCompressedImmutableRamF32(SparseVectorIndex<InvertedIndexCompressedImmutableRam<f32>>),
    SparseCompressedImmutableRamF16(SparseVectorIndex<InvertedIndexCompressedImmutableRam<f16>>),
    SparseCompressedImmutableRamU8(
        SparseVectorIndex<InvertedIndexCompressedImmutableRam<QuantizedU8>>,
    ),
    SparseCompressedMmapF32(SparseVectorIndex<InvertedIndexCompressedMmap<f32, MmapFile>>),
    SparseCompressedMmapF16(SparseVectorIndex<InvertedIndexCompressedMmap<f16, MmapFile>>),
    SparseCompressedMmapU8(SparseVectorIndex<InvertedIndexCompressedMmap<QuantizedU8, MmapFile>>),
}

macro_rules! for_all_vector_indexes {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            VectorIndexEnum::Plain($pattern) => $result,
            VectorIndexEnum::Hnsw($pattern) => $result,
            VectorIndexEnum::SparseRam($pattern) => $result,
            VectorIndexEnum::SparseCompressedImmutableRamF32($pattern) => $result,
            VectorIndexEnum::SparseCompressedImmutableRamF16($pattern) => $result,
            VectorIndexEnum::SparseCompressedImmutableRamU8($pattern) => $result,
            VectorIndexEnum::SparseCompressedMmapF32($pattern) => $result,
            VectorIndexEnum::SparseCompressedMmapF16($pattern) => $result,
            VectorIndexEnum::SparseCompressedMmapU8($pattern) => $result,
        }
    };
}

impl VectorIndexEnum {
    /// Whether the index was opened cold: left on disk and paged in on demand, so
    /// reads may hit the disk. False for heap data and for mmaps populated on open.
    pub fn is_cold(&self) -> bool {
        match self {
            Self::Plain(_) => false,
            Self::Hnsw(index) => index.is_cold(),
            Self::SparseRam(index) => index.inverted_index().is_cold(),
            Self::SparseCompressedImmutableRamF32(index) => index.inverted_index().is_cold(),
            Self::SparseCompressedImmutableRamF16(index) => index.inverted_index().is_cold(),
            Self::SparseCompressedImmutableRamU8(index) => index.inverted_index().is_cold(),
            Self::SparseCompressedMmapF32(index) => index.inverted_index().is_cold(),
            Self::SparseCompressedMmapF16(index) => index.inverted_index().is_cold(),
            Self::SparseCompressedMmapU8(index) => index.inverted_index().is_cold(),
        }
    }

    pub fn populate(&self) -> OperationResult<()> {
        match self {
            Self::Plain(_) => {}
            Self::Hnsw(index) => index.populate()?,
            Self::SparseRam(_) => {}
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
            Self::SparseRam(_) => {}
            Self::SparseCompressedImmutableRamF32(_) => {}
            Self::SparseCompressedImmutableRamF16(_) => {}
            Self::SparseCompressedImmutableRamU8(_) => {}
            Self::SparseCompressedMmapF32(index) => index.inverted_index().clear_cache()?,
            Self::SparseCompressedMmapF16(index) => index.inverted_index().clear_cache()?,
            Self::SparseCompressedMmapU8(index) => index.inverted_index().clear_cache()?,
        };
        Ok(())
    }

    /// Sparse elements in the index, `None` for a dense one. An immutable index keeps a deleted
    /// point's elements until it is rebuilt.
    pub fn sparse_element_count(&self) -> OperationResult<Option<usize>> {
        let count = match self {
            Self::Plain(_) | Self::Hnsw(_) => return Ok(None),
            Self::SparseRam(index) => index.inverted_index().total_element_count()?,
            Self::SparseCompressedImmutableRamF32(index) => {
                index.inverted_index().total_element_count()?
            }
            Self::SparseCompressedImmutableRamF16(index) => {
                index.inverted_index().total_element_count()?
            }
            Self::SparseCompressedImmutableRamU8(index) => {
                index.inverted_index().total_element_count()?
            }
            Self::SparseCompressedMmapF32(index) => index.inverted_index().total_element_count()?,
            Self::SparseCompressedMmapF16(index) => index.inverted_index().total_element_count()?,
            Self::SparseCompressedMmapU8(index) => index.inverted_index().total_element_count()?,
        };
        Ok(Some(count))
    }

    pub fn as_hnsw(&self) -> Option<&HNSWIndex> {
        match self {
            VectorIndexEnum::Plain(_) => None,
            VectorIndexEnum::Hnsw(index) => Some(index),
            VectorIndexEnum::SparseRam(_) => None,
            VectorIndexEnum::SparseCompressedImmutableRamF32(_) => None,
            VectorIndexEnum::SparseCompressedImmutableRamF16(_) => None,
            VectorIndexEnum::SparseCompressedImmutableRamU8(_) => None,
            VectorIndexEnum::SparseCompressedMmapF32(_) => None,
            VectorIndexEnum::SparseCompressedMmapF16(_) => None,
            VectorIndexEnum::SparseCompressedMmapU8(_) => None,
        }
    }
}

impl VectorIndexRead for VectorIndexEnum {
    fn search(
        &self,
        vectors: &[&QueryVector],
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        for_all_vector_indexes!(self, index => index.search(vectors, filter, top, params, query_context))
    }

    fn filter_checks(
        &self,
        query_vectors: &[QueryVector],
        cardinality: &CardinalityEstimation,
        top: usize,
        params: Option<&SearchParams>,
    ) -> OperationResult<FilterChecks> {
        for_all_vector_indexes!(self, index => index.filter_checks(query_vectors, cardinality, top, params))
    }

    fn get_telemetry_data(&self, detail: TelemetryDetail) -> VectorIndexSearchesTelemetry {
        for_all_vector_indexes!(self, index => index.get_telemetry_data(detail))
    }

    fn indexed_vector_count(&self) -> usize {
        for_all_vector_indexes!(self, index => index.indexed_vector_count())
    }

    fn size_of_searchable_vectors_in_bytes(&self) -> usize {
        for_all_vector_indexes!(self, index => index.size_of_searchable_vectors_in_bytes())
    }

    fn is_index(&self) -> bool {
        match self {
            Self::Plain(_) => false,
            Self::Hnsw(_)
            | Self::SparseRam(_)
            | Self::SparseCompressedImmutableRamF32(_)
            | Self::SparseCompressedImmutableRamF16(_)
            | Self::SparseCompressedImmutableRamU8(_)
            | Self::SparseCompressedMmapF32(_)
            | Self::SparseCompressedMmapF16(_)
            | Self::SparseCompressedMmapU8(_) => true,
        }
    }

    fn fill_idf_statistics(
        &self,
        idf: &mut HashMap<DimId, usize>,
        corpus: Option<&Filter>,
        is_stopped: &AtomicBool,
    ) -> OperationResult<usize> {
        match self {
            // Dense indexes contribute no df counts; document count matches
            // the previous segment-level `indexed_vector_count` accounting.
            Self::Plain(index) => Ok(match corpus {
                None => index.indexed_vector_count(),
                Some(_) => 0,
            }),
            Self::Hnsw(index) => Ok(match corpus {
                None => index.indexed_vector_count(),
                Some(_) => 0,
            }),
            Self::SparseRam(index) => index.fill_idf_statistics(idf, corpus, is_stopped),
            Self::SparseCompressedImmutableRamF32(index) => {
                index.fill_idf_statistics(idf, corpus, is_stopped)
            }
            Self::SparseCompressedImmutableRamF16(index) => {
                index.fill_idf_statistics(idf, corpus, is_stopped)
            }
            Self::SparseCompressedImmutableRamU8(index) => {
                index.fill_idf_statistics(idf, corpus, is_stopped)
            }
            Self::SparseCompressedMmapF32(index) => {
                index.fill_idf_statistics(idf, corpus, is_stopped)
            }
            Self::SparseCompressedMmapF16(index) => {
                index.fill_idf_statistics(idf, corpus, is_stopped)
            }
            Self::SparseCompressedMmapU8(index) => {
                index.fill_idf_statistics(idf, corpus, is_stopped)
            }
        }
    }
}

impl VectorIndex for VectorIndexEnum {
    fn files(&self) -> Vec<PathBuf> {
        for_all_vector_indexes!(self, index => index.files())
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        for_all_vector_indexes!(self, index => index.immutable_files())
    }

    fn update_vector(
        &mut self,
        id: PointOffsetType,
        vector: Option<VectorRef>,
    ) -> OperationResult<()> {
        for_all_vector_indexes!(self, index => index.update_vector(id, vector))
    }

    fn update_vector_raw(
        &mut self,
        id: PointOffsetType,
        vector: Option<&[u8]>,
    ) -> OperationResult<()> {
        for_all_vector_indexes!(self, index => index.update_vector_raw(id, vector))
    }
}
