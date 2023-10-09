use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use common::types::ScoredPointOffset;
use sparse::index::inverted_index::InvertedIndex;

use super::hnsw_index::graph_links::{GraphLinksMmap, GraphLinksRam};
use super::hnsw_index::hnsw::HNSWIndex;
use super::plain_payload_index::PlainIndex;
use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::QueryVector;
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
    ) -> Vec<Vec<ScoredPointOffset>>;

    /// Force internal index rebuild.
    fn build_index(&mut self, stopped: &AtomicBool) -> OperationResult<()>;

    fn get_telemetry_data(&self) -> VectorIndexSearchesTelemetry;

    fn files(&self) -> Vec<PathBuf>;

    /// The number of indexed vectors, currently accessible
    fn indexed_vector_count(&self) -> usize;
}

pub enum VectorIndexEnum {
    Plain(PlainIndex),
    HnswRam(HNSWIndex<GraphLinksRam>),
    HnswMmap(HNSWIndex<GraphLinksMmap>),
    SparseRam(InvertedIndex),
    SparseMmap(InvertedIndex),
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
    ) -> Vec<Vec<ScoredPointOffset>> {
        match self {
            VectorIndexEnum::Plain(index) => index.search(vectors, filter, top, params, is_stopped),
            VectorIndexEnum::HnswRam(index) => {
                index.search(vectors, filter, top, params, is_stopped)
            }
            VectorIndexEnum::HnswMmap(index) => {
                index.search(vectors, filter, top, params, is_stopped)
            }
            // TODO needs a SparseVector object as input
            VectorIndexEnum::SparseRam(_index) => {
                unimplemented!("SparseRam search not implemented")
            }
            VectorIndexEnum::SparseMmap(_index) => {
                unimplemented!("SparseMmap search not implemented")
            }
        }
    }

    fn build_index(&mut self, stopped: &AtomicBool) -> OperationResult<()> {
        match self {
            VectorIndexEnum::Plain(index) => index.build_index(stopped),
            VectorIndexEnum::HnswRam(index) => index.build_index(stopped),
            VectorIndexEnum::HnswMmap(index) => index.build_index(stopped),
            // TODO OperationResult should be pushed to common?
            VectorIndexEnum::SparseRam(index) => Ok(index.build_index(stopped)?),
            VectorIndexEnum::SparseMmap(index) => Ok(index.build_index(stopped)?),
        }
    }

    fn get_telemetry_data(&self) -> VectorIndexSearchesTelemetry {
        match self {
            VectorIndexEnum::Plain(index) => index.get_telemetry_data(),
            VectorIndexEnum::HnswRam(index) => index.get_telemetry_data(),
            VectorIndexEnum::HnswMmap(index) => index.get_telemetry_data(),
            // TODO VectorIndexSearchesTelemetry should be pushed to common?
            VectorIndexEnum::SparseRam(_index) => {
                unimplemented!("SparseRam telemetry not implemented")
            }
            VectorIndexEnum::SparseMmap(_index) => {
                unimplemented!("SparseMmap telemetry not implemented")
            }
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
}
