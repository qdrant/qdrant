use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{DeferredBehavior, PointOffsetType, ScoredPointOffset};

use super::HNSWIndexReadView;
use crate::common::operation_error::OperationResult;
use crate::data_types::query_context::VectorQueryContext;
use crate::data_types::vectors::{QueryVector, VectorInternal};
use crate::id_tracker::IdTrackerRead;
use crate::index::PayloadIndexRead;
use crate::index::hnsw_index::graph_layers::{GraphLayersWithVectors, SearchAlgorithm};
use crate::index::hnsw_index::point_scorer::{BatchFilteredSearcher, FilteredScorer};
use crate::index::query_estimator::adjust_to_available_vectors;
use crate::index::query_optimization::optimized_filter::OptimizedFilter;
use crate::index::vector_index_search_common::{
    get_oversampled_top, is_quantized_search, postprocess_search_result,
};
use crate::types::{ACORN_MAX_SELECTIVITY_DEFAULT, Filter, SearchParams};
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectorsRead;
use crate::vector_storage::query::DiscoverQuery;
use crate::vector_storage::{RawScorerBuilder, VectorStorageRead};

impl<'a, I, V, Q, P> HNSWIndexReadView<'a, I, V, Q, P>
where
    I: IdTrackerRead,
    V: VectorStorageRead + RawScorerBuilder,
    Q: QuantizedVectorsRead,
    P: PayloadIndexRead,
{
    pub(super) fn search_with_graph(
        &self,
        vector: &QueryVector,
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        custom_entry_points: Option<&[PointOffsetType]>,
        vector_query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        let ef = params
            .and_then(|params| params.hnsw_ef)
            .unwrap_or(self.config.ef);
        let acorn_enabled = params
            .and_then(|params| params.acorn)
            .is_some_and(|acorn| acorn.enable);
        let acorn_max_selectivity = params
            .and_then(|params| params.acorn)
            .and_then(|acorn| acorn.max_selectivity)
            .map_or(ACORN_MAX_SELECTIVITY_DEFAULT, |v| *v);

        let is_stopped = vector_query_context.is_stopped();

        let deleted_points = vector_query_context
            .deleted_points()
            .unwrap_or_else(|| self.id_tracker.deleted_point_bitslice());

        let hw_counter = vector_query_context.hardware_counter();
        let oversampled_top = get_oversampled_top(self.quantized_vectors, params, top);

        let mut algorithm = SearchAlgorithm::Hnsw;
        if acorn_enabled
            && self.config.m0 != 0
            && let Some(filter) = filter
        {
            // NOTE: technically we also might want to use ACORN for unfiltered
            // searches for segments with a lot of deleted points. But in
            // practice, such segments most likely to be picked by an optimizer
            // soon.

            let available_vector_count = self.vector_storage.available_vector_count();
            let selectivity = if available_vector_count == 0 {
                1.0
            } else {
                let query_point_cardinality = self
                    .payload_index
                    .estimate_cardinality(filter, &hw_counter)?;
                let query_cardinality = adjust_to_available_vectors(
                    query_point_cardinality,
                    available_vector_count,
                    self.id_tracker.available_point_count(),
                );
                query_cardinality.exp as f64 / available_vector_count as f64
            };
            if selectivity <= acorn_max_selectivity {
                algorithm = SearchAlgorithm::Acorn;
            }
        }

        let search_with_vectors = || -> OperationResult<Option<Vec<ScoredPointOffset>>> {
            match algorithm {
                SearchAlgorithm::Hnsw => (),
                // ACORN is not implemented for graph with vectors yet (but possible)
                SearchAlgorithm::Acorn => return Ok(None),
            }
            if !self.graph.has_inline_vectors()
                || !is_quantized_search(self.quantized_vectors, params)
            {
                return Ok(None);
            }
            let Some(quantized_vectors) = self.quantized_vectors else {
                return Ok(None);
            };

            // Quantized vectors are "link vectors"
            let link_scorer_filtered = FilteredScorer::new(
                vector.to_owned(),
                self.vector_storage,
                Some(quantized_vectors),
                filter
                    .map(|f| self.payload_index.filter_context(f, &hw_counter))
                    .transpose()?,
                deleted_points,
                vector_query_context.hardware_counter(),
            )?;
            let Some(link_scorer_filtered_bytes) = link_scorer_filtered.scorer_bytes() else {
                return Ok(None);
            };

            // Full vectors are "base vectors"
            let base_scorer = self
                .vector_storage
                .build_raw_scorer(vector.to_owned(), vector_query_context.hardware_counter())?;
            let Some(base_scorer_bytes) = base_scorer.scorer_bytes() else {
                return Ok(None);
            };

            Ok(Some(self.graph.search_with_vectors(
                top,
                std::cmp::max(ef, oversampled_top),
                &link_scorer_filtered,
                &link_scorer_filtered_bytes,
                base_scorer_bytes,
                custom_entry_points,
                &vector_query_context.is_stopped(),
            )?))
        };

        let regular_search = || -> OperationResult<Vec<ScoredPointOffset>> {
            let filter_context = filter
                .map(|f| self.payload_index.filter_context(f, &hw_counter))
                .transpose()?;
            let points_scorer = construct_search_scorer(
                vector,
                self.vector_storage,
                self.quantized_vectors,
                deleted_points,
                params,
                vector_query_context.hardware_counter(),
                filter_context,
            )?;

            let search_result = self.graph.search(
                oversampled_top,
                ef,
                algorithm,
                points_scorer,
                custom_entry_points,
                &is_stopped,
            )?;

            postprocess_search_result(
                search_result,
                self.id_tracker.deleted_point_bitslice(),
                self.vector_storage,
                self.quantized_vectors,
                vector,
                params,
                top,
                vector_query_context.hardware_counter(),
            )
        };

        // Try to use graph with vectors first.
        if let Some(search_result) = search_with_vectors()? {
            Ok(search_result)
        } else {
            // Graph with vectors is not available, fallback to regular graph search.
            regular_search()
        }
    }

    pub(super) fn search_vectors_with_graph(
        &self,
        vectors: &[&QueryVector],
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        vector_query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        vectors
            .iter()
            .map(|&vector| match vector {
                QueryVector::Discover(discover_query) => self.discover_search_with_graph(
                    discover_query.clone(),
                    filter,
                    top,
                    params,
                    vector_query_context,
                ),
                QueryVector::Nearest(_)
                | QueryVector::RecommendBestScore(_)
                | QueryVector::RecommendSumScores(_)
                | QueryVector::Context(_)
                | QueryVector::FeedbackNaive(_) => {
                    self.search_with_graph(vector, filter, top, params, None, vector_query_context)
                }
            })
            .collect()
    }

    fn search_plain_iterator_batched(
        &self,
        query_vectors: &[&QueryVector],
        points: impl Iterator<Item = PointOffsetType>,
        top: usize,
        params: Option<&SearchParams>,
        vector_query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        let deleted_points = vector_query_context
            .deleted_points()
            .unwrap_or_else(|| self.id_tracker.deleted_point_bitslice());

        let is_stopped = vector_query_context.is_stopped();
        let oversampled_top = get_oversampled_top(self.quantized_vectors, params, top);

        let batch_filtered_searcher = construct_batch_searcher(
            query_vectors,
            self.vector_storage,
            self.quantized_vectors,
            oversampled_top,
            deleted_points,
            params,
            vector_query_context.hardware_counter(),
            None,
        )?;
        let mut search_results = batch_filtered_searcher.peek_top_iter(points, &is_stopped)?;
        for (search_result, query_vector) in search_results.iter_mut().zip(query_vectors) {
            *search_result = postprocess_search_result(
                std::mem::take(search_result),
                self.id_tracker.deleted_point_bitslice(),
                self.vector_storage,
                self.quantized_vectors,
                query_vector,
                params,
                top,
                vector_query_context.hardware_counter(),
            )?;
        }
        Ok(search_results)
    }

    pub(super) fn search_plain_batched(
        &self,
        vectors: &[&QueryVector],
        filtered_points: impl Iterator<Item = PointOffsetType>,
        top: usize,
        params: Option<&SearchParams>,
        vector_query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        self.search_plain_iterator_batched(
            vectors,
            filtered_points,
            top,
            params,
            vector_query_context,
        )
    }

    pub(super) fn search_plain_unfiltered_batched(
        &self,
        vectors: &[&QueryVector],
        top: usize,
        params: Option<&SearchParams>,
        vector_query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        let ids_iterator = self.id_tracker.point_mappings().iter_internal();
        self.search_plain_iterator_batched(vectors, ids_iterator, top, params, vector_query_context)
    }

    pub(super) fn search_vectors_plain(
        &self,
        vectors: &[&QueryVector],
        filter: &Filter,
        top: usize,
        params: Option<&SearchParams>,
        vector_query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        let hw_counter = &vector_query_context.hardware_counter();
        let is_stopped = &vector_query_context.is_stopped();

        // Assume query is already estimated to be small enough so we can iterate over all matched ids
        let query_cardinality = self
            .payload_index
            .estimate_cardinality(filter, hw_counter)?;
        let filtered_points: Vec<PointOffsetType> = self
            .payload_index
            .iter_filtered_points(
                filter,
                &query_cardinality,
                hw_counter,
                is_stopped,
                // No deferred filtering here since it's HNSW index.
                DeferredBehavior::WithDeferred,
            )
            .map(|it| it.collect())?;
        self.search_plain_batched(
            vectors,
            filtered_points.into_iter(),
            top,
            params,
            vector_query_context,
        )
    }

    fn discover_search_with_graph(
        &self,
        discover_query: DiscoverQuery<VectorInternal>,
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        vector_query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        // Stage 1: Find best entry points using Context search
        let query_vector = QueryVector::Context(discover_query.pairs.clone().into());

        const DISCOVERY_ENTRY_POINT_COUNT: usize = 10;

        let custom_entry_points: Vec<_> = self
            .search_with_graph(
                &query_vector,
                filter,
                DISCOVERY_ENTRY_POINT_COUNT,
                params,
                None,
                vector_query_context,
            )
            .map(|search_result| search_result.iter().map(|x| x.idx).collect())?;

        // Stage 2: Discover search with entry points
        let query_vector = QueryVector::Discover(discover_query);

        self.search_with_graph(
            &query_vector,
            filter,
            top,
            params,
            Some(&custom_entry_points),
            vector_query_context,
        )
    }
}

fn construct_search_scorer<'a, V, Q>(
    vector: &QueryVector,
    vector_storage: &'a V,
    quantized_storage: Option<&'a Q>,
    deleted_points: &'a BitSlice,
    params: Option<&SearchParams>,
    hardware_counter: HardwareCounterCell,
    filter_context: Option<OptimizedFilter<'a>>,
) -> OperationResult<FilteredScorer<'a>>
where
    V: VectorStorageRead + RawScorerBuilder,
    Q: QuantizedVectorsRead,
{
    let quantization_enabled = is_quantized_search(quantized_storage, params);
    FilteredScorer::new(
        vector.to_owned(),
        vector_storage,
        quantization_enabled.then_some(quantized_storage).flatten(),
        filter_context,
        deleted_points,
        hardware_counter,
    )
}

#[allow(clippy::too_many_arguments)]
fn construct_batch_searcher<'a, V, Q>(
    vectors: &[&QueryVector],
    vector_storage: &'a V,
    quantized_storage: Option<&'a Q>,
    top: usize,
    deleted_points: &'a BitSlice,
    params: Option<&SearchParams>,
    hardware_counter: HardwareCounterCell,
    filter_context: Option<OptimizedFilter<'a>>,
) -> OperationResult<BatchFilteredSearcher<'a>>
where
    V: VectorStorageRead + RawScorerBuilder,
    Q: QuantizedVectorsRead,
{
    let quantization_enabled = is_quantized_search(quantized_storage, params);
    BatchFilteredSearcher::new(
        vectors,
        vector_storage,
        quantization_enabled.then_some(quantized_storage).flatten(),
        filter_context,
        top,
        deleted_points,
        hardware_counter,
    )
}
