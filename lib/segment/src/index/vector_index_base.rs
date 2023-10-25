use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use common::types::{PointOffsetType, ScoredPointOffset};
use sparse::index::inverted_index::inverted_index_ram::InvertedIndexRam;

use super::hnsw_index::graph_links::{GraphLinksMmap, GraphLinksRam};
use super::hnsw_index::hnsw::HNSWIndex;
use super::plain_payload_index::PlainIndex;
use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::QueryVector;
use crate::index::sparse_index::sparse_vector_index::SparseVectorIndex;
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

    fn update(&mut self, _idx: PointOffsetType) -> OperationResult<()> {
        Ok(())
    }
}

pub enum VectorIndexEnum {
    Plain(PlainIndex),
    HnswRam(HNSWIndex<GraphLinksRam>),
    HnswMmap(HNSWIndex<GraphLinksMmap>),
    Sparse(SparseVectorIndex<InvertedIndexRam>),
}

impl VectorIndexEnum {
    pub fn is_index(&self) -> bool {
        match self {
            Self::Plain(_) => false,
            Self::HnswRam(_) => true,
            Self::HnswMmap(_) => true,
            Self::Sparse(_) => true,
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
            VectorIndexEnum::Sparse(index) => {
                index.search(vectors, filter, top, params, is_stopped)
            }
        }
    }

    fn build_index(&mut self, stopped: &AtomicBool) -> OperationResult<()> {
        match self {
            VectorIndexEnum::Plain(index) => index.build_index(stopped),
            VectorIndexEnum::HnswRam(index) => index.build_index(stopped),
            VectorIndexEnum::HnswMmap(index) => index.build_index(stopped),
            VectorIndexEnum::Sparse(index) => index.build_index(stopped),
        }
    }

    fn get_telemetry_data(&self) -> VectorIndexSearchesTelemetry {
        match self {
            VectorIndexEnum::Plain(index) => index.get_telemetry_data(),
            VectorIndexEnum::HnswRam(index) => index.get_telemetry_data(),
            VectorIndexEnum::HnswMmap(index) => index.get_telemetry_data(),
            VectorIndexEnum::Sparse(index) => index.get_telemetry_data(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            VectorIndexEnum::Plain(index) => index.files(),
            VectorIndexEnum::HnswRam(index) => index.files(),
            VectorIndexEnum::HnswMmap(index) => index.files(),
            VectorIndexEnum::Sparse(index) => index.files(),
        }
    }

    fn indexed_vector_count(&self) -> usize {
        match self {
            Self::Plain(index) => index.indexed_vector_count(),
            Self::HnswRam(index) => index.indexed_vector_count(),
            Self::HnswMmap(index) => index.indexed_vector_count(),
            Self::Sparse(index) => index.indexed_vector_count(),
        }
    }

    fn update(&mut self, idx: PointOffsetType) -> OperationResult<()> {
        match self {
            Self::Plain(index) => index.update(idx),
            Self::HnswRam(index) => index.update(idx),
            Self::HnswMmap(index) => index.update(idx),
            Self::Sparse(index) => index.update(idx),
        }
    }
}
