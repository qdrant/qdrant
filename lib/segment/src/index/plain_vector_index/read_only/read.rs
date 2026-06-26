use std::collections::HashMap;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{ScoredPointOffset, TelemetryDetail};
use sparse::common::types::DimId;

use super::ReadOnlyPlainVectorIndex;
use crate::common::operation_error::OperationResult;
use crate::common::operation_time_statistics::OperationDurationStatistics;
use crate::data_types::query_context::VectorQueryContext;
use crate::data_types::vectors::QueryVector;
use crate::index::{UniversalReadExt, VectorIndexRead};
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{Filter, SearchParams};
use crate::vector_storage::VectorStorageRead;

impl<S: UniversalReadExt> VectorIndexRead for ReadOnlyPlainVectorIndex<S> {
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
        VectorIndexSearchesTelemetry {
            index_name: None,
            unfiltered_plain: self
                .unfiltered_searches_telemetry
                .lock()
                .get_statistics(detail),
            filtered_plain: self
                .filtered_searches_telemetry
                .lock()
                .get_statistics(detail),
            unfiltered_hnsw: OperationDurationStatistics::default(),
            filtered_small_cardinality: OperationDurationStatistics::default(),
            filtered_large_cardinality: OperationDurationStatistics::default(),
            filtered_exact: OperationDurationStatistics::default(),
            filtered_sparse: Default::default(),
            unfiltered_exact: OperationDurationStatistics::default(),
            unfiltered_sparse: OperationDurationStatistics::default(),
        }
    }

    fn indexed_vector_count(&self) -> usize {
        0
    }

    fn size_of_searchable_vectors_in_bytes(&self) -> usize {
        self.vector_storage
            .borrow()
            .size_of_available_vectors_in_bytes()
    }

    fn fill_idf_statistics(
        &self,
        _idf: &mut HashMap<DimId, usize>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        // Plain (dense) index doesn't track IDF.
        Ok(())
    }

    fn is_index(&self) -> bool {
        false
    }
}
