use std::collections::HashMap;
use std::path::PathBuf;

use common::types::{PointOffsetType, ScoredPointOffset, TelemetryDetail};
use half::f16;
use sparse::common::types::{DimId, QuantizedU8};
use sparse::index::inverted_index::inverted_index_compressed_immutable_ram::InvertedIndexCompressedImmutableRam;
use sparse::index::inverted_index::inverted_index_compressed_mmap::InvertedIndexCompressedMmap;
use sparse::index::inverted_index::inverted_index_immutable_ram::InvertedIndexImmutableRam;
use sparse::index::inverted_index::inverted_index_mmap::InvertedIndexMmap;
use sparse::index::inverted_index::inverted_index_ram::InvertedIndexRam;

use super::hnsw_index::hnsw::HNSWIndex;
use super::plain_vector_index::PlainVectorIndex;
use super::sparse_index::sparse_vector_index::SparseVectorIndex;
use crate::common::operation_error::OperationResult;
use crate::data_types::query_context::VectorQueryContext;
use crate::data_types::vectors::{QueryVector, VectorRef};
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{Filter, SearchParams, SeqNumberType};

/// Trait for vector searching
pub trait VectorIndex {
    /// Return list of Ids with fitting
    fn search(
        &self,
        vectors: &[&QueryVector],
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>>;

    fn get_telemetry_data(&self, detail: TelemetryDetail) -> VectorIndexSearchesTelemetry;

    fn files(&self) -> Vec<PathBuf>;

    fn versioned_files(&self) -> Vec<(PathBuf, SeqNumberType)> {
        Vec::new()
    }

    /// The number of indexed vectors, currently accessible
    fn indexed_vector_count(&self) -> usize;

    /// Total size of all searchable vectors in bytes.
    fn size_of_searchable_vectors_in_bytes(&self) -> usize;

    /// Update index for a single vector
    ///
    /// # Arguments
    /// - `id` - sequential vector id, offset in the vector storage
    /// - `vector` - new vector value,
    ///        if None - vector will be removed from the index marked as deleted in storage.
    ///        Note: inserting None vector is not equal to removing vector from the storage.
    ///              Unlike removing, it will always result in storage growth.
    ///              Proper removing should be performed by the optimizer.
    fn update_vector(
        &mut self,
        id: PointOffsetType,
        vector: Option<VectorRef>,
    ) -> OperationResult<()>;
}

#[derive(Debug)]
pub enum VectorIndexEnum {
    Plain(PlainVectorIndex),
    Hnsw(HNSWIndex),
    SparseRam(SparseVectorIndex<InvertedIndexRam>),
    SparseImmutableRam(SparseVectorIndex<InvertedIndexImmutableRam>),
    SparseMmap(SparseVectorIndex<InvertedIndexMmap>),
    SparseCompressedImmutableRamF32(SparseVectorIndex<InvertedIndexCompressedImmutableRam<f32>>),
    SparseCompressedImmutableRamF16(SparseVectorIndex<InvertedIndexCompressedImmutableRam<f16>>),
    SparseCompressedImmutableRamU8(
        SparseVectorIndex<InvertedIndexCompressedImmutableRam<QuantizedU8>>,
    ),
    SparseCompressedMmapF32(SparseVectorIndex<InvertedIndexCompressedMmap<f32>>),
    SparseCompressedMmapF16(SparseVectorIndex<InvertedIndexCompressedMmap<f16>>),
    SparseCompressedMmapU8(SparseVectorIndex<InvertedIndexCompressedMmap<QuantizedU8>>),
}

impl VectorIndexEnum {
    pub fn is_index(&self) -> bool {
        match self {
            Self::Plain(_) => false,
            Self::Hnsw(_) => true,
            Self::SparseRam(_) => true,
            Self::SparseImmutableRam(_) => true,
            Self::SparseMmap(_) => true,
            Self::SparseCompressedImmutableRamF32(_) => true,
            Self::SparseCompressedImmutableRamF16(_) => true,
            Self::SparseCompressedImmutableRamU8(_) => true,
            Self::SparseCompressedMmapF32(_) => true,
            Self::SparseCompressedMmapF16(_) => true,
            Self::SparseCompressedMmapU8(_) => true,
        }
    }

    pub fn fill_idf_statistics(&self, idf: &mut HashMap<DimId, usize>) {
        match self {
            Self::Plain(_) | Self::Hnsw(_) => (),
            Self::SparseRam(index) => index.fill_idf_statistics(idf),
            Self::SparseImmutableRam(index) => index.fill_idf_statistics(idf),
            Self::SparseMmap(index) => index.fill_idf_statistics(idf),
            Self::SparseCompressedImmutableRamF32(index) => index.fill_idf_statistics(idf),
            Self::SparseCompressedImmutableRamF16(index) => index.fill_idf_statistics(idf),
            Self::SparseCompressedImmutableRamU8(index) => index.fill_idf_statistics(idf),
            Self::SparseCompressedMmapF32(index) => index.fill_idf_statistics(idf),
            Self::SparseCompressedMmapF16(index) => index.fill_idf_statistics(idf),
            Self::SparseCompressedMmapU8(index) => index.fill_idf_statistics(idf),
        }
    }
}

impl VectorIndex for VectorIndexEnum {
    fn search(
        &self,
        vectors: &[&QueryVector],
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        match self {
            VectorIndexEnum::Plain(index) => {
                index.search(vectors, filter, top, params, query_context)
            }
            VectorIndexEnum::Hnsw(index) => {
                index.search(vectors, filter, top, params, query_context)
            }
            VectorIndexEnum::SparseRam(index) => {
                index.search(vectors, filter, top, params, query_context)
            }
            VectorIndexEnum::SparseImmutableRam(index) => {
                index.search(vectors, filter, top, params, query_context)
            }
            VectorIndexEnum::SparseMmap(index) => {
                index.search(vectors, filter, top, params, query_context)
            }
            VectorIndexEnum::SparseCompressedImmutableRamF32(index) => {
                index.search(vectors, filter, top, params, query_context)
            }
            VectorIndexEnum::SparseCompressedImmutableRamF16(index) => {
                index.search(vectors, filter, top, params, query_context)
            }
            VectorIndexEnum::SparseCompressedImmutableRamU8(index) => {
                index.search(vectors, filter, top, params, query_context)
            }
            VectorIndexEnum::SparseCompressedMmapF32(index) => {
                index.search(vectors, filter, top, params, query_context)
            }
            VectorIndexEnum::SparseCompressedMmapF16(index) => {
                index.search(vectors, filter, top, params, query_context)
            }
            VectorIndexEnum::SparseCompressedMmapU8(index) => {
                index.search(vectors, filter, top, params, query_context)
            }
        }
    }

    fn get_telemetry_data(&self, detail: TelemetryDetail) -> VectorIndexSearchesTelemetry {
        match self {
            VectorIndexEnum::Plain(index) => index.get_telemetry_data(detail),
            VectorIndexEnum::Hnsw(index) => index.get_telemetry_data(detail),
            VectorIndexEnum::SparseRam(index) => index.get_telemetry_data(detail),
            VectorIndexEnum::SparseImmutableRam(index) => index.get_telemetry_data(detail),
            VectorIndexEnum::SparseMmap(index) => index.get_telemetry_data(detail),
            VectorIndexEnum::SparseCompressedImmutableRamF32(index) => {
                index.get_telemetry_data(detail)
            }
            VectorIndexEnum::SparseCompressedImmutableRamF16(index) => {
                index.get_telemetry_data(detail)
            }
            VectorIndexEnum::SparseCompressedImmutableRamU8(index) => {
                index.get_telemetry_data(detail)
            }
            VectorIndexEnum::SparseCompressedMmapF32(index) => index.get_telemetry_data(detail),
            VectorIndexEnum::SparseCompressedMmapF16(index) => index.get_telemetry_data(detail),
            VectorIndexEnum::SparseCompressedMmapU8(index) => index.get_telemetry_data(detail),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            VectorIndexEnum::Plain(index) => index.files(),
            VectorIndexEnum::Hnsw(index) => index.files(),
            VectorIndexEnum::SparseRam(index) => index.files(),
            VectorIndexEnum::SparseImmutableRam(index) => index.files(),
            VectorIndexEnum::SparseMmap(index) => index.files(),
            VectorIndexEnum::SparseCompressedImmutableRamF32(index) => index.files(),
            VectorIndexEnum::SparseCompressedImmutableRamF16(index) => index.files(),
            VectorIndexEnum::SparseCompressedImmutableRamU8(index) => index.files(),
            VectorIndexEnum::SparseCompressedMmapF32(index) => index.files(),
            VectorIndexEnum::SparseCompressedMmapF16(index) => index.files(),
            VectorIndexEnum::SparseCompressedMmapU8(index) => index.files(),
        }
    }

    fn indexed_vector_count(&self) -> usize {
        match self {
            Self::Plain(index) => index.indexed_vector_count(),
            Self::Hnsw(index) => index.indexed_vector_count(),
            Self::SparseRam(index) => index.indexed_vector_count(),
            Self::SparseImmutableRam(index) => index.indexed_vector_count(),
            Self::SparseMmap(index) => index.indexed_vector_count(),
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
            Self::SparseRam(index) => index.size_of_searchable_vectors_in_bytes(),
            Self::SparseImmutableRam(index) => index.size_of_searchable_vectors_in_bytes(),
            Self::SparseMmap(index) => index.size_of_searchable_vectors_in_bytes(),
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

    fn update_vector(
        &mut self,
        id: PointOffsetType,
        vector: Option<VectorRef>,
    ) -> OperationResult<()> {
        match self {
            Self::Plain(index) => index.update_vector(id, vector),
            Self::Hnsw(index) => index.update_vector(id, vector),
            Self::SparseRam(index) => index.update_vector(id, vector),
            Self::SparseImmutableRam(index) => index.update_vector(id, vector),
            Self::SparseMmap(index) => index.update_vector(id, vector),
            Self::SparseCompressedImmutableRamF32(index) => index.update_vector(id, vector),
            Self::SparseCompressedImmutableRamF16(index) => index.update_vector(id, vector),
            Self::SparseCompressedImmutableRamU8(index) => index.update_vector(id, vector),
            Self::SparseCompressedMmapF32(index) => index.update_vector(id, vector),
            Self::SparseCompressedMmapF16(index) => index.update_vector(id, vector),
            Self::SparseCompressedMmapU8(index) => index.update_vector(id, vector),
        }
    }
}
