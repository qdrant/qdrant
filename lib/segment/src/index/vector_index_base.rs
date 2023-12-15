use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use common::types::{PointOffsetType, ScoredPointOffset};
use sparse::index::inverted_index::inverted_index_mmap::InvertedIndexMmap;
use sparse::index::inverted_index::inverted_index_ram::InvertedIndexRam;

use super::hnsw_index::graph_links::{GraphLinksMmap, GraphLinksRam};
use super::hnsw_index::hnsw::HNSWIndex;
use super::plain_payload_index::PlainIndex;
use super::sparse_index::sparse_vector_index::SparseVectorIndex;
use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::{QueryVector, VectorRef};
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{Filter, SearchParams};

/// Trait for vector searching
pub trait VectorIndex {
    /// Return list of Ids with fitting
    fn search(
        &self,
        vectors: &[&QueryVector],
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>>;

    /// Force internal index rebuild.
    fn build_index(&mut self, stopped: &AtomicBool) -> OperationResult<()>;

    fn get_telemetry_data(&self) -> VectorIndexSearchesTelemetry;

    fn files(&self) -> Vec<PathBuf>;

    /// The number of indexed vectors, currently accessible
    fn indexed_vector_count(&self) -> usize;

    /// Update index for a single vector
    fn update_vector(&mut self, id: PointOffsetType, vector: VectorRef) -> OperationResult<()>;
}

pub enum VectorIndexEnum {
    Plain(PlainIndex),
    HnswRam(HNSWIndex<GraphLinksRam>),
    HnswMmap(HNSWIndex<GraphLinksMmap>),
    SparseRam(SparseVectorIndex<InvertedIndexRam>),
    SparseMmap(SparseVectorIndex<InvertedIndexMmap>),
}

impl VectorIndexEnum {
    pub fn is_index(&self) -> bool {
        match self {
            Self::Plain(_) => false,
            Self::HnswRam(_) => true,
            Self::HnswMmap(_) => true,
            Self::SparseRam(_) => true,
            Self::SparseMmap(_) => true,
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
        is_stopped: &AtomicBool,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        match self {
            VectorIndexEnum::Plain(index) => index.search(vectors, filter, top, params, is_stopped),
            VectorIndexEnum::HnswRam(index) => {
                index.search(vectors, filter, top, params, is_stopped)
            }
            VectorIndexEnum::HnswMmap(index) => {
                index.search(vectors, filter, top, params, is_stopped)
            }
            VectorIndexEnum::SparseRam(index) => {
                index.search(vectors, filter, top, params, is_stopped)
            }
            VectorIndexEnum::SparseMmap(index) => {
                index.search(vectors, filter, top, params, is_stopped)
            }
        }
    }

    fn build_index(&mut self, stopped: &AtomicBool) -> OperationResult<()> {
        match self {
            VectorIndexEnum::Plain(index) => index.build_index(stopped),
            VectorIndexEnum::HnswRam(index) => index.build_index(stopped),
            VectorIndexEnum::HnswMmap(index) => index.build_index(stopped),
            VectorIndexEnum::SparseRam(index) => index.build_index(stopped),
            VectorIndexEnum::SparseMmap(index) => index.build_index(stopped),
        }
    }

    fn get_telemetry_data(&self) -> VectorIndexSearchesTelemetry {
        match self {
            VectorIndexEnum::Plain(index) => index.get_telemetry_data(),
            VectorIndexEnum::HnswRam(index) => index.get_telemetry_data(),
            VectorIndexEnum::HnswMmap(index) => index.get_telemetry_data(),
            VectorIndexEnum::SparseRam(index) => index.get_telemetry_data(),
            VectorIndexEnum::SparseMmap(index) => index.get_telemetry_data(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            VectorIndexEnum::Plain(index) => index.files(),
            VectorIndexEnum::HnswRam(index) => index.files(),
            VectorIndexEnum::HnswMmap(index) => index.files(),
            VectorIndexEnum::SparseRam(index) => index.files(),
            VectorIndexEnum::SparseMmap(index) => index.files(),
        }
    }

    fn indexed_vector_count(&self) -> usize {
        match self {
            Self::Plain(index) => index.indexed_vector_count(),
            Self::HnswRam(index) => index.indexed_vector_count(),
            Self::HnswMmap(index) => index.indexed_vector_count(),
            Self::SparseRam(index) => index.indexed_vector_count(),
            Self::SparseMmap(index) => index.indexed_vector_count(),
        }
    }

    fn update_vector(&mut self, id: PointOffsetType, vector: VectorRef) -> OperationResult<()> {
        match self {
            Self::Plain(index) => index.update_vector(id, vector),
            Self::HnswRam(index) => index.update_vector(id, vector),
            Self::HnswMmap(index) => index.update_vector(id, vector),
            Self::SparseRam(index) => index.update_vector(id, vector),
            Self::SparseMmap(index) => index.update_vector(id, vector),
        }
    }
}
