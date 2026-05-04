use std::collections::HashMap;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{ScoredPointOffset, TelemetryDetail};
use sparse::common::types::DimId;
use sparse::index::inverted_index::InvertedIndex;

use crate::common::operation_error::OperationResult;
use crate::data_types::query_context::VectorQueryContext;
use crate::data_types::vectors::QueryVector;
use crate::index::sparse_index::sparse_vector_index::SparseVectorIndex;
use crate::index::vector_index_base::VectorIndexRead;
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{Filter, SearchParams};

/// Read-only newtype wrapper around [`SparseVectorIndex`].
///
/// Exposes only [`VectorIndexRead`].
#[derive(Debug)]
pub struct ReadOnlySparseVectorIndex<TInvertedIndex: InvertedIndex>(
    SparseVectorIndex<TInvertedIndex>,
);

impl<TInvertedIndex: InvertedIndex> ReadOnlySparseVectorIndex<TInvertedIndex> {
    pub fn is_on_disk(&self) -> bool {
        self.0.inverted_index().is_on_disk()
    }

    pub(super) fn inner(&self) -> &SparseVectorIndex<TInvertedIndex> {
        &self.0
    }
}

impl<TInvertedIndex: InvertedIndex> VectorIndexRead for ReadOnlySparseVectorIndex<TInvertedIndex> {
    fn search(
        &self,
        vectors: &[&QueryVector],
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        self.0.search(vectors, filter, top, params, query_context)
    }

    fn get_telemetry_data(&self, detail: TelemetryDetail) -> VectorIndexSearchesTelemetry {
        self.0.get_telemetry_data(detail)
    }

    fn indexed_vector_count(&self) -> usize {
        self.0.indexed_vector_count()
    }

    fn size_of_searchable_vectors_in_bytes(&self) -> usize {
        self.0.size_of_searchable_vectors_in_bytes()
    }

    fn fill_idf_statistics(
        &self,
        idf: &mut HashMap<DimId, usize>,
        hw_counter: &HardwareCounterCell,
    ) {
        self.0.fill_idf_statistics(idf, hw_counter);
    }

    fn is_index(&self) -> bool {
        self.0.is_index()
    }
}
