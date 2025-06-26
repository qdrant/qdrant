use std::mem;
use std::sync::Arc;
use std::time::Duration;

use ahash::AHashSet;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::top_k::TopK;
use common::types::{ScoreType, ScoredPointOffset};
use futures::FutureExt;
use futures::future::BoxFuture;
use itertools::Itertools;
use parking_lot::Mutex;
use segment::common::operation_error::OperationResult;
use segment::common::reciprocal_rank_fusion::rrf_scoring;
use segment::common::score_fusion::{ScoreFusion, score_fusion};
use segment::data_types::vectors::{QueryVector, VectorInternal};
use segment::types::{
    Filter, HasIdCondition, ScoredPoint, VectorNameBuf, WithPayloadInterface, WithVector,
};
use segment::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
use segment::vector_storage::multi_dense::volatile_multi_dense_vector_storage::new_volatile_multi_dense_vector_storage;
use segment::vector_storage::sparse::volatile_sparse_vector_storage::new_volatile_sparse_vector_storage;
use segment::vector_storage::{VectorStorage, new_raw_scorer};
use tokio::runtime::Handle;
use tokio::time::error::Elapsed;

use super::LocalShard;
use crate::collection_manager::segments_searcher::SegmentsSearcher;
use crate::operations::types::{
    CollectionError, CollectionResult, CoreSearchRequest, CoreSearchRequestBatch,
    QueryScrollRequestInternal, ScrollOrder,
};
use crate::operations::universal_query::planned_query::{
    MergePlan, PlannedQuery, RescoreParams, RootPlan, Source,
};
use crate::operations::universal_query::shard_query::{
    FusionInternal, MmrInternal, SampleInternal, ScoringQuery, ShardQueryResponse,
};

pub enum FetchedSource {
    Search(usize),
    Scroll(usize),
}

struct PrefetchResults {
    search_results: Mutex<Vec<Vec<ScoredPoint>>>,
    scroll_results: Mutex<Vec<Vec<ScoredPoint>>>,
}

impl PrefetchResults {
    fn new(search_results: Vec<Vec<ScoredPoint>>, scroll_results: Vec<Vec<ScoredPoint>>) -> Self {
        Self {
            scroll_results: Mutex::new(scroll_results),
            search_results: Mutex::new(search_results),
        }
    }

    fn get(&self, element: FetchedSource) -> CollectionResult<Vec<ScoredPoint>> {
        match element {
            FetchedSource::Search(idx) => self.search_results.lock().get_mut(idx).map(mem::take),
            FetchedSource::Scroll(idx) => self.scroll_results.lock().get_mut(idx).map(mem::take),
        }
        .ok_or_else(|| CollectionError::service_error("Expected a prefetched source to exist"))
    }
}

impl LocalShard {
    pub async fn do_planned_query(
        &self,
        request: PlannedQuery,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_counter_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<ShardQueryResponse>> {
        let start_time = std::time::Instant::now();
        let timeout = timeout.unwrap_or(self.shared_storage_config.search_timeout);

        let searches_f = self.do_search(
            Arc::new(CoreSearchRequestBatch {
                searches: request.searches,
            }),
            search_runtime_handle,
            Some(timeout),
            hw_counter_acc.clone(),
        );

        let scrolls_f = self.query_scroll_batch(
            Arc::new(request.scrolls),
            search_runtime_handle,
            timeout,
            hw_counter_acc.clone(),
        );

        // execute both searches and scrolls concurrently
        let (search_results, scroll_results) = tokio::try_join!(searches_f, scrolls_f)?;
        let prefetch_holder = PrefetchResults::new(search_results, scroll_results);

        // decrease timeout by the time spent so far
        let timeout = timeout.saturating_sub(start_time.elapsed());

        let plans_futures = request.root_plans.into_iter().map(|root_plan| {
            self.resolve_plan(
                root_plan,
                &prefetch_holder,
                search_runtime_handle,
                timeout,
                hw_counter_acc.clone(),
            )
        });

        let batched_scored_points = futures::future::try_join_all(plans_futures).await?;

        Ok(batched_scored_points)
    }

    /// Fetches the payload and/or vector if required. This will filter out points if they are deleted between search and retrieve.
    async fn fill_with_payload_or_vectors(
        &self,
        query_response: ShardQueryResponse,
        with_payload: WithPayloadInterface,
        with_vector: WithVector,
        timeout: Duration,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<ShardQueryResponse> {
        if !with_payload.is_required() && !with_vector.is_enabled() {
            return Ok(query_response);
        }

        // ids to retrieve (deduplication happens in the searcher)
        let point_ids: Vec<_> = query_response
            .iter()
            .flatten()
            .map(|scored_point| scored_point.id)
            .collect();

        // Collect retrieved records into a hashmap for fast lookup
        let records_map = tokio::time::timeout(
            timeout,
            SegmentsSearcher::retrieve(
                self.segments.clone(),
                &point_ids,
                &(&with_payload).into(),
                &with_vector,
                &self.search_runtime,
                hw_measurement_acc,
            ),
        )
        .await
        .map_err(|_: Elapsed| CollectionError::timeout(timeout.as_secs() as usize, "retrieve"))??;

        // It might be possible, that we won't find all records,
        // so we need to re-collect the results
        let query_response: ShardQueryResponse = query_response
            .into_iter()
            .map(|points| {
                points
                    .into_iter()
                    .filter_map(|mut point| {
                        records_map.get(&point.id).map(|record| {
                            point.payload.clone_from(&record.payload);
                            point.vector.clone_from(&record.vector);
                            point
                        })
                    })
                    .collect()
            })
            .collect();

        Ok(query_response)
    }

    async fn resolve_plan<'shard, 'query>(
        &'shard self,
        root_plan: RootPlan,
        prefetch_holder: &'query PrefetchResults,
        search_runtime_handle: &'shard Handle,
        timeout: Duration,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>>
    where
        'shard: 'query,
    {
        let RootPlan {
            merge_plan,
            with_payload,
            with_vector,
        } = root_plan;

        // resolve merging plan
        let results = self
            .recurse_prefetch(
                merge_plan,
                prefetch_holder,
                search_runtime_handle,
                timeout,
                0,
                hw_measurement_acc.clone(),
            )
            .await?;

        // fetch payloads and vectors if required
        self.fill_with_payload_or_vectors(
            results,
            with_payload,
            with_vector,
            timeout,
            hw_measurement_acc,
        )
        .await
    }

    fn recurse_prefetch<'shard, 'query>(
        &'shard self,
        merge_plan: MergePlan,
        prefetch_holder: &'query PrefetchResults,
        search_runtime_handle: &'shard Handle,
        timeout: Duration,
        depth: usize,
        hw_counter_acc: HwMeasurementAcc,
    ) -> BoxFuture<'query, CollectionResult<Vec<Vec<ScoredPoint>>>>
    where
        'shard: 'query,
    {
        async move {
            let start_time = std::time::Instant::now();
            let max_len = merge_plan.sources.len();
            let mut sources = Vec::with_capacity(max_len);

            // We need to preserve the order of the sources for some fusion strategies
            for source in merge_plan.sources {
                match source {
                    Source::SearchesIdx(idx) => {
                        sources.push(prefetch_holder.get(FetchedSource::Search(idx))?)
                    }
                    Source::ScrollsIdx(idx) => {
                        sources.push(prefetch_holder.get(FetchedSource::Scroll(idx))?)
                    }
                    Source::Prefetch(prefetch) => {
                        let merged = self
                            .recurse_prefetch(
                                *prefetch,
                                prefetch_holder,
                                search_runtime_handle,
                                timeout,
                                depth + 1,
                                hw_counter_acc.clone(),
                            )
                            .await?
                            .into_iter();
                        sources.extend(merged);
                    }
                }
            }

            // decrease timeout by the time spent so far (recursive calls)
            let timeout = timeout.saturating_sub(start_time.elapsed());

            // Rescore or return plain sources
            if let Some(rescore_params) = merge_plan.rescore_params {
                let rescored = self
                    .rescore(
                        sources,
                        rescore_params,
                        search_runtime_handle,
                        timeout,
                        hw_counter_acc,
                    )
                    .await?;

                Ok(vec![rescored])
            } else {
                // The sources here are passed to the next layer without any extra processing.
                // It is either a query without prefetches, or a fusion request and the intermediate results are passed to the next layer.
                debug_assert_eq!(depth, 0);
                Ok(sources)
            }
        }
        .boxed()
    }

    /// Rescore list of scored points
    async fn rescore(
        &self,
        sources: Vec<Vec<ScoredPoint>>,
        rescore_params: RescoreParams,
        search_runtime_handle: &Handle,
        timeout: Duration,
        hw_counter_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let RescoreParams {
            rescore,
            score_threshold,
            limit,
            params,
        } = rescore_params;

        match rescore {
            ScoringQuery::Fusion(fusion) => {
                self.fusion_rescore(sources.into_iter(), fusion, score_threshold, limit)
                    .await
            }
            ScoringQuery::OrderBy(order_by) => {
                // create single scroll request for rescoring query
                let filter = filter_with_sources_ids(sources.into_iter());

                // Note: score_threshold is not used in this case, as all results will have same score,
                // but different order_value
                let scroll_request = QueryScrollRequestInternal {
                    limit,
                    filter: Some(filter),
                    with_payload: false.into(),
                    with_vector: false.into(),
                    scroll_order: ScrollOrder::ByField(order_by),
                };

                self.query_scroll_batch(
                    Arc::new(vec![scroll_request]),
                    search_runtime_handle,
                    timeout,
                    hw_counter_acc.clone(),
                )
                .await?
                .pop()
                .ok_or_else(|| {
                    CollectionError::service_error(
                        "Rescoring with order-by query didn't return expected batch of results",
                    )
                })
            }
            ScoringQuery::Vector(query_enum) => {
                // create single search request for rescoring query
                let filter = filter_with_sources_ids(sources.into_iter());

                let search_request = CoreSearchRequest {
                    query: query_enum,
                    filter: Some(filter),
                    params,
                    limit,
                    offset: 0,
                    with_payload: None,
                    with_vector: None,
                    score_threshold,
                };
                let rescoring_core_search_request = CoreSearchRequestBatch {
                    searches: vec![search_request],
                };

                self.do_search(
                    Arc::new(rescoring_core_search_request),
                    search_runtime_handle,
                    Some(timeout),
                    hw_counter_acc,
                )
                .await?
                // One search request is sent. We expect only one result
                .pop()
                .ok_or_else(|| {
                    CollectionError::service_error(
                        "Rescoring with vector(s) query didn't return expected batch of results",
                    )
                })
            }
            ScoringQuery::Formula(formula) => {
                self.rescore_with_formula(formula, sources, limit, timeout, hw_counter_acc)
                    .await
            }
            ScoringQuery::Sample(sample) => match sample {
                SampleInternal::Random => {
                    // create single scroll request for rescoring query
                    let filter = filter_with_sources_ids(sources.into_iter());

                    // Note: score_threshold is not used in this case, as all results will have same score and order_value
                    let scroll_request = QueryScrollRequestInternal {
                        limit,
                        filter: Some(filter),
                        with_payload: false.into(),
                        with_vector: false.into(),
                        scroll_order: ScrollOrder::Random,
                    };

                    self.query_scroll_batch(
                        Arc::new(vec![scroll_request]),
                        search_runtime_handle,
                        timeout,
                        hw_counter_acc.clone(),
                    )
                    .await?
                    .pop()
                    .ok_or_else(|| {
                        CollectionError::service_error(
                            "Rescoring with order-by query didn't return expected batch of results",
                        )
                    })
                }
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn fusion_rescore(
        &self,
        sources: impl Iterator<Item = Vec<ScoredPoint>>,
        fusion: FusionInternal,
        score_threshold: Option<f32>,
        limit: usize,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let fused = match fusion {
            FusionInternal::Rrf => rrf_scoring(sources),
            FusionInternal::Dbsf => score_fusion(sources, ScoreFusion::dbsf()),
        };

        let top_fused: Vec<_> = if let Some(score_threshold) = score_threshold {
            fused
                .into_iter()
                .take_while(|point| point.score >= score_threshold)
                .take(limit)
                .collect()
        } else {
            fused.into_iter().take(limit).collect()
        };

        Ok(top_fused)
    }

    /// Maximum Marginal Relevance rescoring
    async fn mmr_rescore(
        &self,
        sources: Vec<Vec<ScoredPoint>>,
        mmr: MmrInternal,
        score_threshold: Option<f32>,
        limit: usize,
        search_runtime_handle: &Handle,
        timeout: Duration,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        // Flatten all source points into a single list
        let mut points: Vec<ScoredPoint> = sources
            .into_iter()
            .flatten()
            .sorted_unstable_by_key(|p| p.id)
            .dedup()
            .collect();

        // If we have fewer points than requested, return them all
        if points.len() <= limit {
            return Ok(points);
        }

        // Take vectors out of the points
        let vectors: Vec<_> = points
            .iter_mut()
            .sorted_unstable_by_key(|p| p.id)
            .dedup_by(|a, b| a.id == b.id)
            .map(|p| {
                p.vector
                    .take()
                    // silently ignore points without this named vector
                    .and_then(|v| v.take(&mmr.vector_name))
                    .expect("all points should have the vector")
            })
            .collect();

        let max_similarities = self
            .max_similarities(vectors, mmr.vector_name, hw_measurement_acc)
            .await?;

        let mmr_results =
            self.maximum_marginal_relevance(points, max_similarities, mmr.lambda, limit);

        Ok(mmr_results)
    }

    /// Apply Maximum Marginal Relevance algorithm.
    ///
    /// # Arguments
    ///
    /// * `candidates` - the list of points to select from. Must be parallel with the `max_similarities` list.
    /// * `max_similarities` - the list of maximum similarities for each point. Must be parallel with the `candidates` list.
    /// * `lambda` - the lambda parameter for the MMR algorithm.
    /// * `limit` - the maximum number of points to select.
    fn maximum_marginal_relevance(
        &self,
        candidates: Vec<ScoredPoint>,
        max_similarities: Vec<ScoreType>,
        lambda: f32,
        limit: usize,
    ) -> Vec<ScoredPoint> {
        let mut top_k = TopK::new(limit);

        for (idx, candidate) in (0..).zip(&candidates) {
            let relevance_score = candidate.score;

            let max_similarity = max_similarities[idx];

            // Calculate MMR score: λ * relevance - (1 - λ) * max_similarity
            let mmr_score = lambda * relevance_score - (1.0 - lambda) * max_similarity;

            top_k.push(ScoredPointOffset {
                score: mmr_score,
                idx: idx as u32,
            });
        }

        top_k
            .into_vec()
            .into_iter()
            .map(|ScoredPointOffset { idx, score }| {
                let mut selected = candidates[idx as usize].clone();
                selected.score = score;
                selected
            })
            .collect()
    }

    /// Selects the maximal similarity for each point in the provided set,
    /// compared to each other within the same set.
    pub async fn max_similarities(
        &self,
        vectors: Vec<VectorInternal>,
        using: VectorNameBuf,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<ScoreType>> {
        let num_vectors = vectors.len();

        // if we have less than 2 points, we can't build a matrix
        if num_vectors < 2 {
            todo!()
        }

        // Create temporary vector storage
        let mut volatile_storage = {
            let collection_params = &self.collection_config.read().await.params;

            let distance = collection_params.get_distance(&using)?;
            match &vectors[0] {
                VectorInternal::Dense(vector) => {
                    new_volatile_dense_vector_storage(vector.len(), distance)
                }
                VectorInternal::Sparse(_sparse_vector) => new_volatile_sparse_vector_storage(),
                VectorInternal::MultiDense(typed_multi_dense_vector) => {
                    let multivec_config = collection_params
                        .vectors
                        .get_params(&using)
                        .and_then(|vector_params| vector_params.multivector_config)
                        .ok_or_else(|| CollectionError::service_error(format!("multivectors are present for {using}, but no multivector config is defined")))?;
                    new_volatile_multi_dense_vector_storage(
                        typed_multi_dense_vector.dim,
                        distance,
                        multivec_config,
                    )
                }
            }
        };

        // Populate storage with vectors
        let hw_counter = HardwareCounterCell::disposable();
        for (key, vector) in (0..).zip(&vectors) {
            volatile_storage.insert_vector(key, vector.as_vector_ref(), &hw_counter)?;
        }

        // Prepare scorers
        let raw_scorers = vectors
            .into_iter()
            .map(|vector| {
                let query = QueryVector::Nearest(vector);
                new_raw_scorer(
                    query,
                    &volatile_storage,
                    hw_measurement_acc.get_counter_cell(),
                )
            })
            .collect::<OperationResult<Vec<_>>>()?;

        // Compute all scores, retain only the top score which isn't the same vector
        let all_offsets = (0..num_vectors as u32).collect::<Vec<_>>();
        let max_scores = raw_scorers
            .into_iter()
            .enumerate()
            .map(|(tmp_id, scorer)| {
                let mut scores = vec![0.0; num_vectors];
                scorer.score_points(&all_offsets, &mut scores);
                scores
                    .into_iter()
                    .enumerate()
                    // exclude the vector score against itself
                    .filter_map(|(i, score)| (i != tmp_id).then_some(score))
                    .max_by(|a, b| a.partial_cmp(b).expect("No NaN"))
                    .expect("There should be at least two vectors")
            })
            .collect::<Vec<_>>();

        Ok(max_scores)
    }
}

/// Extracts point ids from sources, and creates a filter to only include those ids.
fn filter_with_sources_ids(sources: impl Iterator<Item = Vec<ScoredPoint>>) -> Filter {
    let mut point_ids = AHashSet::new();

    for source in sources {
        for point in source.iter() {
            point_ids.insert(point.id);
        }
    }

    // create filter for target point ids
    Filter::new_must(segment::types::Condition::HasId(HasIdCondition::from(
        point_ids,
    )))
}
