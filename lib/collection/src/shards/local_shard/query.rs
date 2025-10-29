use std::mem;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ahash::AHashSet;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use futures::FutureExt;
use futures::future::BoxFuture;
use ordered_float::OrderedFloat;
use parking_lot::Mutex;
use segment::common::reciprocal_rank_fusion::rrf_scoring;
use segment::common::score_fusion::{ScoreFusion, score_fusion};
use segment::types::{Filter, HasIdCondition, ScoredPoint, WithPayloadInterface, WithVector};
use shard::search::CoreSearchRequestBatch;
use tokio::runtime::Handle;

use super::LocalShard;
use crate::collection::mmr::mmr_from_points_with_vector;
use crate::collection_manager::segments_searcher::SegmentsSearcher;
use crate::operations::types::{
    CollectionError, CollectionResult, CoreSearchRequest, QueryScrollRequestInternal, ScrollOrder,
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
        .map_err(|_| CollectionError::timeout(timeout.as_secs() as usize, "retrieve"))??;

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

    async fn resolve_plan(
        &self,
        root_plan: RootPlan,
        prefetch_holder: &PrefetchResults,
        search_runtime_handle: &Handle,
        timeout: Duration,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
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

    fn recurse_prefetch<'a>(
        &'a self,
        merge_plan: MergePlan,
        prefetch_holder: &'a PrefetchResults,
        search_runtime_handle: &'a Handle,
        timeout: Duration,
        depth: usize,
        hw_counter_acc: HwMeasurementAcc,
    ) -> BoxFuture<'a, CollectionResult<Vec<Vec<ScoredPoint>>>> {
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
                self.fusion_rescore(
                    sources.into_iter(),
                    fusion,
                    score_threshold.map(OrderedFloat::into_inner),
                    limit,
                )
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
                    score_threshold: score_threshold.map(OrderedFloat::into_inner),
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
            ScoringQuery::Mmr(mmr) => {
                self.mmr_rescore(
                    sources,
                    mmr,
                    limit,
                    search_runtime_handle,
                    timeout,
                    hw_counter_acc,
                )
                .await
            }
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
            FusionInternal::RrfK(k) => rrf_scoring(sources, k),
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

    /// Maximal Marginal Relevance rescoring
    async fn mmr_rescore(
        &self,
        sources: Vec<Vec<ScoredPoint>>,
        mmr: MmrInternal,
        limit: usize,
        search_runtime_handle: &Handle,
        timeout: Duration,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let start = Instant::now();

        let points_with_vector = self
            .fill_with_payload_or_vectors(
                sources,
                false.into(),
                WithVector::from(mmr.using.clone()),
                timeout,
                hw_measurement_acc.clone(),
            )
            .await?
            .into_iter()
            .flatten();

        let timeout = timeout.saturating_sub(start.elapsed());

        let collection_params = &self.collection_config.read().await.params;

        // Even if we have fewer points than requested, still calculate MMR.
        let mut top_mmr = mmr_from_points_with_vector(
            collection_params,
            points_with_vector,
            mmr,
            limit,
            search_runtime_handle,
            timeout,
            hw_measurement_acc,
        )
        .await?;

        // strip mmr vector. We will handle user-requested vectors at root level of request.
        for p in &mut top_mmr {
            p.vector = None;
        }

        Ok(top_mmr)
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
