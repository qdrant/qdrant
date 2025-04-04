use std::path::PathBuf;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoredPointOffset, TelemetryDetail};
use parking_lot::Mutex;

use crate::common::BYTES_IN_KB;
use crate::common::operation_error::OperationResult;
use crate::common::operation_time_statistics::{
    OperationDurationStatistics, OperationDurationsAggregator, ScopeDurationMeasurer,
};
use crate::data_types::query_context::VectorQueryContext;
use crate::data_types::vectors::{QueryVector, VectorRef};
use crate::id_tracker::IdTrackerSS;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::index::{PayloadIndex, VectorIndex};
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{Filter, SearchParams};
use crate::vector_storage::{VectorStorage, VectorStorageEnum, new_raw_scorer};

#[derive(Debug)]
pub struct PlainVectorIndex {
    id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    filtered_searches_telemetry: Arc<Mutex<OperationDurationsAggregator>>,
    unfiltered_searches_telemetry: Arc<Mutex<OperationDurationsAggregator>>,
}

impl PlainVectorIndex {
    pub fn new(
        id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
        vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
        payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    ) -> PlainVectorIndex {
        PlainVectorIndex {
            id_tracker,
            vector_storage,
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
        if available_vector_count > 0 {
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
        } else {
            true
        }
    }
}

impl VectorIndex for PlainVectorIndex {
    fn search(
        &self,
        vectors: &[&QueryVector],
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
            return Ok(vec![vec![]; vectors.len()]);
        }

        let is_stopped = query_context.is_stopped();

        let hw_counter = query_context.hardware_counter();

        match filter {
            Some(filter) => {
                let _timer = ScopeDurationMeasurer::new(&self.filtered_searches_telemetry);
                let id_tracker = self.id_tracker.borrow();
                let payload_index = self.payload_index.borrow();
                let vector_storage = self.vector_storage.borrow();
                let filtered_ids_vec = payload_index.query_points(filter, &hw_counter);
                let deleted_points = query_context
                    .deleted_points()
                    .unwrap_or_else(|| id_tracker.deleted_point_bitslice());
                vectors
                    .iter()
                    .map(|&vector| {
                        new_raw_scorer(
                            vector.to_owned(),
                            &vector_storage,
                            deleted_points,
                            query_context.hardware_counter(),
                        )
                        .and_then(|scorer| {
                            Ok(scorer.peek_top_iter(
                                &mut filtered_ids_vec.iter().copied(),
                                top,
                                &is_stopped,
                            )?)
                        })
                    })
                    .collect()
            }
            None => {
                let _timer = ScopeDurationMeasurer::new(&self.unfiltered_searches_telemetry);
                let vector_storage = self.vector_storage.borrow();
                let id_tracker = self.id_tracker.borrow();
                let deleted_points = query_context
                    .deleted_points()
                    .unwrap_or_else(|| id_tracker.deleted_point_bitslice());
                vectors
                    .iter()
                    .map(|&vector| {
                        new_raw_scorer(
                            vector.to_owned(),
                            &vector_storage,
                            deleted_points,
                            query_context.hardware_counter(),
                        )
                        .and_then(|scorer| Ok(scorer.peek_top_all(top, &is_stopped)?))
                    })
                    .collect()
            }
        }
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
