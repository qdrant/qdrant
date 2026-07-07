use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoredPointOffset, TelemetryDetail};
use sparse::common::types::DimId;
use sparse::index::inverted_index::InvertedIndex;

use super::ReadOnlySparseVectorIndex;
use crate::common::operation_error::OperationResult;
use crate::data_types::query_context::VectorQueryContext;
use crate::data_types::vectors::QueryVector;
use crate::index::{UniversalReadExt, VectorIndexRead};
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{Filter, SearchParams};

impl<S: UniversalReadExt, TInvertedIndex: InvertedIndex> VectorIndexRead
    for ReadOnlySparseVectorIndex<S, TInvertedIndex>
{
    fn search(
        &self,
        vectors: &[&QueryVector],
        filter: Option<&Filter>,
        top: usize,
        _params: Option<&SearchParams>,
        query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        self.with_view(|view| view.search(vectors, filter, top, query_context))
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

    fn fill_idf_statistics_filtered(
        &self,
        idf: &mut HashMap<DimId, usize>,
        filtered_points: &[PointOffsetType],
        hw_counter: &HardwareCounterCell,
        is_stopped: &AtomicBool,
    ) -> OperationResult<usize> {
        self.with_view(|view| {
            view.fill_idf_statistics_filtered(idf, filtered_points, hw_counter, is_stopped)
        })
    }

    fn is_index(&self) -> bool {
        true
    }
}
