use std::path::PathBuf;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoredPointOffset, TelemetryDetail};
use parking_lot::Mutex;

use super::hnsw_index::point_scorer::BatchFilteredSearcher;
use crate::common::BYTES_IN_KB;
use crate::common::operation_error::OperationResult;
use crate::common::operation_time_statistics::{
    OperationDurationStatistics, OperationDurationsAggregator, ScopeDurationMeasurer,
};
use crate::data_types::query_context::VectorQueryContext;
use crate::data_types::vectors::{QueryVector, VectorRef};
use crate::id_tracker::{IdTracker, IdTrackerEnum};
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::index::vector_index_search_common::{
    get_oversampled_top, is_quantized_search, postprocess_search_result,
};
use crate::index::{PayloadIndex, VectorIndex};
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{Filter, SearchParams};
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

#[derive(Debug)]
pub struct PlainVectorIndex {
    id_tracker: Arc<AtomicRefCell<IdTrackerEnum>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    quantized_vectors: Arc<AtomicRefCell<Option<QuantizedVectors>>>,
    payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    filtered_searches_telemetry: Arc<Mutex<OperationDurationsAggregator>>,
    unfiltered_searches_telemetry: Arc<Mutex<OperationDurationsAggregator>>,
}

impl PlainVectorIndex {
    pub fn new(
        id_tracker: Arc<AtomicRefCell<IdTrackerEnum>>,
        vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
        quantized_vectors: Arc<AtomicRefCell<Option<QuantizedVectors>>>,
        payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    ) -> PlainVectorIndex {
        PlainVectorIndex {
            id_tracker,
            vector_storage,
            quantized_vectors,
            payload_index,
            filtered_searches_telemetry: OperationDurationsAggregator::new(),
            unfiltered_searches_telemetry: OperationDurationsAggregator::new(),
        }
    }

    pub fn is_small_enough_for_unindexed_search(
        &self,
        search_optimized_threshold_kb: usize,
        filter: Option<&Filter>,
        hw_counter: &HardwareCounterCell,
    ) -> bool {
        let vector_storage = self.vector_storage.borrow();
        let available_vector_count = vector_storage.available_vector_count();
        if available_vector_count == 0 {
            return true;
        }

        let vector_size_bytes =
            vector_storage.size_of_available_vectors_in_bytes() / available_vector_count;
        let indexing_threshold_bytes = search_optimized_threshold_kb * BYTES_IN_KB;

        if let Some(payload_filter) = filter {
            let payload_index = self.payload_index.borrow();
            let cardinality = payload_index.estimate_cardinality(payload_filter, hw_counter);
            let scan_size = vector_size_bytes.saturating_mul(cardinality.max);
            scan_size <= indexing_threshold_bytes
        } else {
            let vector_storage_size = vector_size_bytes.saturating_mul(available_vector_count);
            vector_storage_size <= indexing_threshold_bytes
        }
    }
}

impl VectorIndex for PlainVectorIndex {
    fn search(
        &self,
        query_vectors: &[&QueryVector],
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        let is_indexed_only = params.map(|p| p.indexed_only).unwrap_or(false);
        if is_indexed_only
            && !self.is_small_enough_for_unindexed_search(
                query_context.search_optimized_threshold_kb(),
                filter,
                &query_context.hardware_counter(),
            )
        {
            return Ok(vec![vec![]; query_vectors.len()]);
        }
        if top == 0 {
            return Ok(vec![vec![]; query_vectors.len()]);
        }

        let is_stopped = query_context.is_stopped();

        let hw_counter = query_context.hardware_counter();

        let _timer = ScopeDurationMeasurer::new(if filter.is_some() {
            &self.filtered_searches_telemetry
        } else {
            &self.unfiltered_searches_telemetry
        });
        let vector_storage = self.vector_storage.borrow();
        let quantized_storage = self.quantized_vectors.borrow();
        let id_tracker = self.id_tracker.borrow();
        let deleted_points = query_context
            .deleted_points()
            .unwrap_or_else(|| id_tracker.deleted_point_bitslice());
        let quantization_enabled = is_quantized_search(quantized_storage.as_ref(), params);
        let quantized_vectors = quantization_enabled
            .then_some(quantized_storage.as_ref())
            .flatten();
        let oversampled_top = get_oversampled_top(quantized_storage.as_ref(), params, top);
        let batch_searcher = BatchFilteredSearcher::new(
            query_vectors,
            &vector_storage,
            quantized_vectors,
            None,
            oversampled_top,
            deleted_points,
            query_context.hardware_counter(),
        )?;

        let mut search_results = match filter {
            Some(filter) => {
                let payload_index = self.payload_index.borrow();
                let filtered_ids_vec = payload_index.query_points(filter, &hw_counter, &is_stopped);
                batch_searcher.peek_top_iter(&mut filtered_ids_vec.iter().copied(), &is_stopped)?
            }
            None => batch_searcher.peek_top_all(&is_stopped)?,
        };

        for (search_result, query_vector) in search_results.iter_mut().zip(query_vectors) {
            *search_result = postprocess_search_result(
                std::mem::take(search_result),
                deleted_points,
                &vector_storage,
                quantized_storage.as_ref(),
                query_vector,
                params,
                top,
                query_context.hardware_counter(),
            )?;
        }
        Ok(search_results)
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

    fn files(&self) -> Vec<PathBuf> {
        vec![]
    }

    fn indexed_vector_count(&self) -> usize {
        0
    }

    fn size_of_searchable_vectors_in_bytes(&self) -> usize {
        self.vector_storage
            .borrow()
            .size_of_available_vectors_in_bytes()
    }

    fn update_vector(
        &mut self,
        id: PointOffsetType,
        vector: Option<VectorRef>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let mut vector_storage = self.vector_storage.borrow_mut();

        if let Some(vector) = vector {
            vector_storage.insert_vector(id, vector, hw_counter)?;

            let mut quantized_vectors = self.quantized_vectors.borrow_mut();
            if let Some(quantized_vectors) = quantized_vectors.as_mut() {
                quantized_vectors.upsert_vector(id, vector, hw_counter)?;
            }
        } else {
            if id as usize >= vector_storage.total_vector_count() {
                debug_assert!(id as usize == vector_storage.total_vector_count());
                // Vector doesn't exist in the storage
                // Insert default vector to keep the sequence
                let default_vector = vector_storage.default_vector();
                vector_storage.insert_vector(id, VectorRef::from(&default_vector), hw_counter)?;
            }
            vector_storage.delete_vector(id)?;
        }

        Ok(())
    }
}
