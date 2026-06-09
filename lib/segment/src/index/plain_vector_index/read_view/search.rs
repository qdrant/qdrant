use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{DeferredBehavior, ScoredPointOffset};

use super::PlainVectorIndexReadView;
use crate::common::BYTES_IN_KB;
use crate::common::operation_error::OperationResult;
use crate::common::operation_time_statistics::ScopeDurationMeasurer;
use crate::data_types::query_context::VectorQueryContext;
use crate::data_types::vectors::QueryVector;
use crate::id_tracker::IdTrackerRead;
use crate::index::PayloadIndexRead;
use crate::index::hnsw_index::point_scorer::BatchFilteredSearcher;
use crate::index::vector_index_search_common::{
    get_oversampled_top, is_quantized_search, postprocess_search_result,
};
use crate::types::{Filter, SearchParams};
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectorsRead;
use crate::vector_storage::{RawScorerBuilder, VectorStorageRead};

impl<'a, I, V, Q, P> PlainVectorIndexReadView<'a, I, V, Q, P>
where
    I: IdTrackerRead,
    V: VectorStorageRead + RawScorerBuilder,
    Q: QuantizedVectorsRead,
    P: PayloadIndexRead,
{
    pub fn is_small_enough_for_unindexed_search(
        &self,
        search_optimized_threshold_kb: usize,
        filter: Option<&Filter>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        let available_vector_count = self.vector_storage.available_vector_count();
        if available_vector_count == 0 {
            return Ok(true);
        }

        let vector_size_bytes =
            self.vector_storage.size_of_available_vectors_in_bytes() / available_vector_count;
        let indexing_threshold_bytes = search_optimized_threshold_kb * BYTES_IN_KB;

        if let Some(payload_filter) = filter {
            let cardinality = self
                .payload_index
                .estimate_cardinality(payload_filter, hw_counter)?;
            let scan_size = vector_size_bytes.saturating_mul(cardinality.max);
            Ok(scan_size <= indexing_threshold_bytes)
        } else {
            let vector_storage_size = vector_size_bytes.saturating_mul(available_vector_count);
            Ok(vector_storage_size <= indexing_threshold_bytes)
        }
    }

    pub fn search(
        &self,
        query_vectors: &[&QueryVector],
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        let is_indexed_only = params.is_some_and(|p| p.indexed_only);
        if is_indexed_only
            && !self.is_small_enough_for_unindexed_search(
                query_context.search_optimized_threshold_kb(),
                filter,
                &query_context.hardware_counter(),
            )?
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
        let deleted_points = query_context
            .deleted_points()
            .unwrap_or_else(|| self.id_tracker.deleted_point_bitslice());
        let quantization_enabled = is_quantized_search(self.quantized_vectors, params);
        let quantized_vectors = quantization_enabled
            .then_some(self.quantized_vectors)
            .flatten();
        let oversampled_top = get_oversampled_top(self.quantized_vectors, params, top);
        let batch_searcher = BatchFilteredSearcher::new(
            query_vectors,
            self.vector_storage,
            quantized_vectors,
            None,
            oversampled_top,
            deleted_points,
            query_context.hardware_counter(),
        )?;

        let mut search_results = match filter {
            Some(filter) => {
                let filtered_ids_vec =
                    self.payload_index
                        .query_points(filter, &hw_counter, &is_stopped)?;
                batch_searcher.peek_top_iter(filtered_ids_vec.iter().copied(), &is_stopped)?
            }
            None => {
                let iter = self
                    .id_tracker
                    .point_mappings()
                    .filter_deferred_and_deleted(
                        batch_searcher.iter_not_deleted(),
                        DeferredBehavior::VisibleOnly,
                    );
                batch_searcher.peek_top_iter(iter, &is_stopped)?
            }
        };

        for (search_result, query_vector) in search_results.iter_mut().zip(query_vectors) {
            *search_result = postprocess_search_result(
                std::mem::take(search_result),
                deleted_points,
                self.vector_storage,
                self.quantized_vectors,
                query_vector,
                params,
                top,
                query_context.hardware_counter(),
            )?;
        }
        Ok(search_results)
    }
}
