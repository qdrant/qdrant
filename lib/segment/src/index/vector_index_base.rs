use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use super::hnsw_index::graph_links::{GraphLinksMmap, GraphLinksRam};
use super::hnsw_index::hnsw::HNSWIndex;
use super::plain_payload_index::PlainIndex;
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::OperationResult;
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{Filter, SearchParams};
use crate::vector_storage::ScoredPointOffset;

/// Trait for vector searching
pub trait VectorIndex {
    /// Return list of Ids with fitting
    fn search(
        &self,
        vectors: &[&[VectorElementType]],
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

    /// Whether this vector index type support appending.
    fn is_appendable(&self) -> bool;
}

pub enum VectorIndexEnum {
    Plain(PlainIndex),
    HnswRam(HNSWIndex<GraphLinksRam>),
    HnswMmap(HNSWIndex<GraphLinksMmap>),
}

impl VectorIndexEnum {
    pub fn is_index(&self) -> bool {
        match self {
            Self::Plain(_) => false,
            Self::HnswRam(_) => true,
            Self::HnswMmap(_) => true,
        }
    }
}

impl VectorIndex for VectorIndexEnum {
    fn search(
        &self,
        vectors: &[&[VectorElementType]],
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
        }
    }

    fn build_index(&mut self, stopped: &AtomicBool) -> OperationResult<()> {
        match self {
            VectorIndexEnum::Plain(index) => index.build_index(stopped),
            VectorIndexEnum::HnswRam(index) => index.build_index(stopped),
            VectorIndexEnum::HnswMmap(index) => index.build_index(stopped),
        }
    }

    fn get_telemetry_data(&self) -> VectorIndexSearchesTelemetry {
        match self {
            VectorIndexEnum::Plain(index) => index.get_telemetry_data(),
            VectorIndexEnum::HnswRam(index) => index.get_telemetry_data(),
            VectorIndexEnum::HnswMmap(index) => index.get_telemetry_data(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            VectorIndexEnum::Plain(index) => index.files(),
            VectorIndexEnum::HnswRam(index) => index.files(),
            VectorIndexEnum::HnswMmap(index) => index.files(),
        }
    }

    fn indexed_vector_count(&self) -> usize {
        match self {
            Self::Plain(index) => index.indexed_vector_count(),
            Self::HnswRam(index) => index.indexed_vector_count(),
            Self::HnswMmap(index) => index.indexed_vector_count(),
        }
    }

    fn is_appendable(&self) -> bool {
        match self {
            Self::Plain(index) => index.is_appendable(),
            Self::HnswRam(index) => index.is_appendable(),
            Self::HnswMmap(index) => index.is_appendable(),
        }
    }
}
