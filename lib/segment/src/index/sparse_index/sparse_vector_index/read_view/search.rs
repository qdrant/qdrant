use std::collections::HashMap;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{DeferredBehavior, PointOffsetType, ScoredPointOffset, TelemetryDetail};
use itertools::Itertools;
use sparse::common::sparse_vector::SparseVector;
use sparse::common::types::DimId;
use sparse::index::inverted_index::InvertedIndex;
use sparse::index::search_context::SearchContext;

use super::SparseVectorIndexReadView;
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::common::operation_time_statistics::ScopeDurationMeasurer;
use crate::data_types::query_context::VectorQueryContext;
use crate::data_types::vectors::{QueryVector, VectorInternal};
use crate::id_tracker::IdTrackerRead;
use crate::index::PayloadIndexRead;
use crate::index::field_index::CardinalityEstimation;
use crate::index::hnsw_index::point_scorer::BatchFilteredSearcher;
use crate::index::query_estimator::adjust_to_available_vectors;
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{DEFAULT_SPARSE_FULL_SCAN_THRESHOLD, Filter};
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::query::TransformInto;
use crate::vector_storage::{RawScorerBuilder, VectorStorageRead, check_deleted_condition};

impl<I, V, P, TInvertedIndex> SparseVectorIndexReadView<'_, I, V, P, TInvertedIndex>
where
    I: IdTrackerRead,
    V: VectorStorageRead + RawScorerBuilder,
    P: PayloadIndexRead,
    TInvertedIndex: InvertedIndex,
{
    /// Search a batch of query vectors, remapping idf weights when required.
    pub fn search(
        &self,
        vectors: &[&QueryVector],
        filter: Option<&Filter>,
        top: usize,
        query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        let mut results = Vec::with_capacity(vectors.len());
        let mut prefiltered_points = None;

        for vector in vectors {
            check_process_stopped(&query_context.is_stopped())?;

            let search_results = if query_context.is_require_idf() {
                let vector = (*vector).clone().transform(|mut vector| {
                    match &mut vector {
                        VectorInternal::Dense(_) | VectorInternal::MultiDense(_) => {
                            return Err(OperationError::WrongSparse);
                        }
                        VectorInternal::Sparse(sparse) => {
                            query_context.remap_idf_weights(&sparse.indices, &mut sparse.values)
                        }
                    }

                    Ok(vector)
                })?;

                self.search_query(&vector, filter, top, &mut prefiltered_points, query_context)?
            } else {
                self.search_query(vector, filter, top, &mut prefiltered_points, query_context)?
            };

            results.push(search_results);
        }
        Ok(results)
    }

    pub fn get_telemetry_data(&self, detail: TelemetryDetail) -> VectorIndexSearchesTelemetry {
        self.searches_telemetry.get_telemetry_data(detail)
    }

    pub fn indexed_vector_count(&self) -> usize {
        self.inverted_index.vector_count()
    }

    pub fn size_of_searchable_vectors_in_bytes(&self) -> usize {
        self.inverted_index.total_sparse_vectors_size()
    }

    /// Update statistics for idf-dot similarity.
    pub fn fill_idf_statistics(
        &self,
        idf: &mut HashMap<DimId, usize>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let iter = idf.iter_mut().filter_map(|(dim_id, count)| {
            let offset = self.indices_tracker.remap_index(*dim_id)?;
            Some((count, offset))
        });
        self.inverted_index
            .posting_list_len_batch(iter, hw_counter, |count, len| {
                *count += len;
                Ok(())
            })?;
        Ok(())
    }

    fn get_query_cardinality(
        &self,
        filter: &Filter,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<CardinalityEstimation> {
        let available_vector_count = self.vector_storage.available_vector_count();
        let query_point_cardinality = self
            .payload_index
            .estimate_cardinality(filter, hw_counter)?;
        Ok(adjust_to_available_vectors(
            query_point_cardinality,
            available_vector_count,
            self.id_tracker.available_point_count(),
        ))
    }

    // Search using raw scorer
    fn search_scored(
        &self,
        query_vector: &QueryVector,
        filter: Option<&Filter>,
        top: usize,
        prefiltered_points: &mut Option<Vec<PointOffsetType>>,
        vector_query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        let deleted_point_bitslice = vector_query_context
            .deleted_points()
            .unwrap_or(self.id_tracker.deleted_point_bitslice());

        let is_stopped = vector_query_context.is_stopped();

        let searcher = BatchFilteredSearcher::new(
            &[query_vector],
            self.vector_storage,
            None::<&QuantizedVectors>,
            None,
            top,
            deleted_point_bitslice,
            vector_query_context.hardware_counter(),
        )?;
        let hw_counter = vector_query_context.hardware_counter();
        let mut results = match filter {
            Some(filter) => {
                let filtered_points = match prefiltered_points {
                    // `prefiltered_points` always contains visible points only so we don't need additional filtering here.
                    Some(filtered_points) => filtered_points.iter().copied(),
                    None => {
                        let filtered_points =
                            self.payload_index
                                .query_points(filter, &hw_counter, &is_stopped)?;
                        *prefiltered_points = Some(filtered_points);
                        prefiltered_points.as_ref().unwrap().iter().copied()
                    }
                };
                searcher.peek_top_iter(filtered_points, &is_stopped)?
            }
            None => {
                let iter = self
                    .id_tracker
                    .point_mappings()
                    .filter_deferred_and_deleted(
                        searcher.iter_not_deleted(),
                        DeferredBehavior::VisibleOnly,
                    );
                searcher.peek_top_iter(iter, &is_stopped)?
            }
        };
        let res = results.pop().expect("single element results");
        Ok(res)
    }

    pub fn search_plain(
        &self,
        sparse_vector: &SparseVector,
        filter: &Filter,
        top: usize,
        prefiltered_points: &mut Option<Vec<PointOffsetType>>,
        vector_query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        let is_stopped = vector_query_context.is_stopped();

        let deleted_point_bitslice = vector_query_context
            .deleted_points()
            .unwrap_or(self.id_tracker.deleted_point_bitslice());
        let deleted_vectors = self.vector_storage.deleted_vector_bitslice();

        let hw_counter = vector_query_context.hardware_counter();

        let ids = match prefiltered_points {
            // Deferred points get filtered in the `None` case and are added to `prefiltered_points`.
            // In the `Some` case, we iterate over this set of points,
            // so no additional filtering is required in that case.
            Some(filtered_points) => filtered_points.iter(),
            None => {
                let filtered_points =
                    self.payload_index
                        .query_points(filter, &hw_counter, &is_stopped)?;
                *prefiltered_points = Some(filtered_points);
                prefiltered_points.as_ref().unwrap().iter()
            }
        }
        .copied()
        .filter(|&idx| check_deleted_condition(idx, deleted_vectors, deleted_point_bitslice))
        .collect_vec();

        let sparse_vector = self.indices_tracker.remap_vector(sparse_vector.clone());
        let mut scratch = self.search_scratch_pool.get();
        let mut hw_counter = vector_query_context.hardware_counter();
        let is_index_on_disk = self.config.index_type.is_on_disk();
        if is_index_on_disk {
            hw_counter.set_vector_io_read_multiplier(1);
        } else {
            hw_counter.set_vector_io_read_multiplier(0);
        }

        let mut search_context = SearchContext::new(
            sparse_vector,
            top,
            self.inverted_index,
            &mut scratch,
            &is_stopped,
            &hw_counter,
        )?;
        let search_result = search_context.plain_search(&ids);
        Ok(search_result)
    }

    // search using sparse vector inverted index
    fn search_sparse(
        &self,
        sparse_vector: &SparseVector,
        filter: Option<&Filter>,
        top: usize,
        vector_query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        let deleted_point_bitslice = vector_query_context
            .deleted_points()
            .unwrap_or(self.id_tracker.deleted_point_bitslice());
        let deleted_vectors = self.vector_storage.deleted_vector_bitslice();

        let not_deleted_condition = |idx: PointOffsetType| -> bool {
            check_deleted_condition(idx, deleted_vectors, deleted_point_bitslice)
        };

        let is_stopped = vector_query_context.is_stopped();

        let sparse_vector = self.indices_tracker.remap_vector(sparse_vector.clone());
        let mut scratch = self.search_scratch_pool.get();
        let mut hw_counter = vector_query_context.hardware_counter();
        let is_index_on_disk = self.config.index_type.is_on_disk();
        if is_index_on_disk {
            hw_counter.set_vector_io_read_multiplier(1);
        } else {
            hw_counter.set_vector_io_read_multiplier(0);
        }

        let mut search_context = SearchContext::new(
            sparse_vector,
            top,
            self.inverted_index,
            &mut scratch,
            &is_stopped,
            &hw_counter,
        )?;

        match filter {
            Some(filter) => {
                let filter_context = self.payload_index.filter_context(filter, &hw_counter)?;
                let matches_filter_condition = |idx: PointOffsetType| -> bool {
                    not_deleted_condition(idx) && filter_context.check(idx)
                };
                Ok(search_context.search(&matches_filter_condition))
            }
            None => Ok(search_context.search(&not_deleted_condition)),
        }
    }

    fn search_nearest_query(
        &self,
        vector: &SparseVector,
        filter: Option<&Filter>,
        top: usize,
        prefiltered_points: &mut Option<Vec<PointOffsetType>>,
        vector_query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        if vector.is_empty() {
            return Ok(vec![]);
        }

        match filter {
            Some(filter) => {
                // if cardinality is small - use plain search
                let query_cardinality =
                    self.get_query_cardinality(filter, &vector_query_context.hardware_counter())?;
                let threshold = self
                    .config
                    .full_scan_threshold
                    .unwrap_or(DEFAULT_SPARSE_FULL_SCAN_THRESHOLD);
                if query_cardinality.max < threshold {
                    let _timer =
                        ScopeDurationMeasurer::new(&self.searches_telemetry.small_cardinality);
                    self.search_plain(
                        vector,
                        filter,
                        top,
                        prefiltered_points,
                        vector_query_context,
                    )
                } else {
                    let _timer =
                        ScopeDurationMeasurer::new(&self.searches_telemetry.filtered_sparse);
                    self.search_sparse(vector, Some(filter), top, vector_query_context)
                }
            }
            None => {
                let _timer = ScopeDurationMeasurer::new(&self.searches_telemetry.unfiltered_sparse);
                self.search_sparse(vector, filter, top, vector_query_context)
            }
        }
    }

    pub fn search_query(
        &self,
        query_vector: &QueryVector,
        filter: Option<&Filter>,
        top: usize,
        prefiltered_points: &mut Option<Vec<PointOffsetType>>,
        vector_query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        if top == 0 {
            return Ok(vec![]);
        }

        match query_vector {
            QueryVector::Nearest(vector) => self.search_nearest_query(
                vector.try_into()?,
                filter,
                top,
                prefiltered_points,
                vector_query_context,
            ),
            QueryVector::RecommendBestScore(_)
            | QueryVector::RecommendSumScores(_)
            | QueryVector::Discover(_)
            | QueryVector::Context(_)
            | QueryVector::FeedbackNaive(_) => {
                let _timer = if filter.is_some() {
                    ScopeDurationMeasurer::new(&self.searches_telemetry.filtered_plain)
                } else {
                    ScopeDurationMeasurer::new(&self.searches_telemetry.unfiltered_plain)
                };
                self.search_scored(
                    query_vector,
                    filter,
                    top,
                    prefiltered_points,
                    vector_query_context,
                )
            }
        }
    }
}
