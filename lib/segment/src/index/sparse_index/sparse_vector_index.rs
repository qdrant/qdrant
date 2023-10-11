use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::types::ScoredPointOffset;
use sparse::common::sparse_vector::SparseVector;
use sparse::index::inverted_index::InvertedIndex;

use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::QueryVector;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::index::{PayloadIndex, VectorIndex};
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{Filter, SearchParams};

pub struct SparseVectorIndex {
    inverted_index: InvertedIndex,
    payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
}

impl VectorIndex for SparseVectorIndex {
    fn search(
        &self,
        vectors: &[&QueryVector],
        filter: Option<&Filter>,
        top: usize,
        _params: Option<&SearchParams>,
        is_stopped: &AtomicBool,
    ) -> Vec<Vec<ScoredPointOffset>> {
        let sparse_vectors: Vec<SparseVector> = vectors
            .iter()
            .map(|v| match v {
                QueryVector::NearestSparse(sparse) => sparse.clone(),
                _ => panic!("SparseIndex::search() called with non-sparse query vector"), // TODO error
            })
            .collect();
        match filter {
            None => sparse_vectors
                .iter()
                .map(|v| self.inverted_index.search(v.clone(), top, is_stopped))
                .collect(),
            Some(query_filter) => {
                let payload_index = self.payload_index.borrow();
                let _available_vector_count = self.indexed_vector_count();
                let _query_point_cardinality = payload_index.estimate_cardinality(query_filter);
                todo!("filtering not implemented")
            }
        }
    }

    fn build_index(&mut self, _stopped: &AtomicBool) -> OperationResult<()> {
        Ok(())
    }

    // TODO sparse index telemetry
    fn get_telemetry_data(&self) -> VectorIndexSearchesTelemetry {
        VectorIndexSearchesTelemetry::default()
    }

    fn files(&self) -> Vec<PathBuf> {
        match &self.inverted_index {
            InvertedIndex::Ram(_index) => vec![],
            InvertedIndex::Mmap(index) => index.files(),
        }
    }

    fn indexed_vector_count(&self) -> usize {
        match &self.inverted_index {
            InvertedIndex::Ram(index) => index.indexed_vector_count(),
            InvertedIndex::Mmap(index) => index.indexed_vector_count(),
        }
    }
}
