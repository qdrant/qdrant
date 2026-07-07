use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoredPointOffset, TelemetryDetail};
use sparse::common::types::DimId;

use super::HNSWIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::query_context::VectorQueryContext;
use crate::data_types::vectors::{QueryVector, VectorRef};
use crate::index::hnsw_index::config::HnswGraphConfig;
use crate::index::{VectorIndex, VectorIndexRead};
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{Filter, SearchParams};
use crate::vector_storage::VectorStorageRead;

impl VectorIndexRead for HNSWIndex {
    fn search(
        &self,
        vectors: &[&QueryVector],
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        self.with_view(|view| view.search(vectors, filter, top, params, query_context))
    }

    fn get_telemetry_data(&self, detail: TelemetryDetail) -> VectorIndexSearchesTelemetry {
        let tm = &self.searches_telemetry;
        VectorIndexSearchesTelemetry {
            index_name: None,
            unfiltered_plain: tm.unfiltered_plain.lock().get_statistics(detail),
            filtered_plain: tm.filtered_plain.lock().get_statistics(detail),
            unfiltered_hnsw: tm.unfiltered_hnsw.lock().get_statistics(detail),
            filtered_small_cardinality: tm.small_cardinality.lock().get_statistics(detail),
            filtered_large_cardinality: tm.large_cardinality.lock().get_statistics(detail),
            filtered_exact: tm.exact_filtered.lock().get_statistics(detail),
            filtered_sparse: Default::default(),
            unfiltered_exact: tm.exact_unfiltered.lock().get_statistics(detail),
            unfiltered_sparse: Default::default(),
        }
    }

    fn indexed_vector_count(&self) -> usize {
        self.config
            .indexed_vector_count
            // If indexed vector count is unknown, fall back to number of points
            .unwrap_or_else(|| self.graph.num_points())
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
        // HNSW (dense) index doesn't track IDF.
        Ok(())
    }

    fn fill_idf_statistics_filtered(
        &self,
        _idf: &mut HashMap<DimId, usize>,
        _filtered_points: &[PointOffsetType],
        _hw_counter: &HardwareCounterCell,
        _is_stopped: &AtomicBool,
    ) -> OperationResult<usize> {
        // HNSW (dense) index doesn't track IDF.
        Ok(0)
    }

    fn is_index(&self) -> bool {
        true
    }
}

impl VectorIndex for HNSWIndex {
    fn files(&self) -> Vec<PathBuf> {
        let mut files = self.graph.files(&self.path);
        let config_path = HnswGraphConfig::get_config_path(&self.path);
        if config_path.exists() {
            files.push(config_path);
        }
        files
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.files() // All HNSW index files are immutable 😎
    }

    fn update_vector(
        &mut self,
        _id: PointOffsetType,
        _vector: Option<VectorRef>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        Err(OperationError::service_error("Cannot update HNSW index"))
    }
}
