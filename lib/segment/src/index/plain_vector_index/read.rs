use std::collections::HashMap;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{ScoredPointOffset, TelemetryDetail};
use sparse::common::types::DimId;

use super::PlainVectorIndex;
use crate::common::operation_error::OperationResult;
use crate::data_types::query_context::VectorQueryContext;
use crate::data_types::vectors::QueryVector;
use crate::index::VectorIndexRead;
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{Filter, SearchParams};

impl PlainVectorIndex {
    pub fn is_small_enough_for_unindexed_search(
        &self,
        search_optimized_threshold_kb: usize,
        filter: Option<&Filter>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        self.with_view(|view| {
            view.is_small_enough_for_unindexed_search(
                search_optimized_threshold_kb,
                filter,
                hw_counter,
            )
        })
    }
}

impl VectorIndexRead for PlainVectorIndex {
    fn search(
        &self,
        query_vectors: &[&QueryVector],
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        self.with_view(|view| view.search(query_vectors, filter, top, params, query_context))
    }

    fn get_telemetry_data(&self, detail: TelemetryDetail) -> VectorIndexSearchesTelemetry {
        self.with_view(|view| view.get_telemetry_data(detail))
    }

    fn indexed_vector_count(&self) -> usize {
        self.with_view(|view| view.indexed_vector_count())
    }

    fn size_of_searchable_vectors_in_bytes(&self) -> usize {
        self.with_view(|view| view.size_of_searchable_vectors_in_bytes())
    }

    fn fill_idf_statistics(
        &self,
        idf: &mut HashMap<DimId, usize>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.with_view(|view| view.fill_idf_statistics(idf, hw_counter))
    }

    fn is_index(&self) -> bool {
        self.with_view(|view| view.is_index())
    }
}
