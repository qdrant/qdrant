use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use api::rest::OrderByInterface;
use futures::future::BoxFuture;
use futures::FutureExt;
use itertools::{Itertools, PeekingNext};
use segment::common::reciprocal_rank_fusion::rrf_scoring;
use segment::types::{
    Filter, HasIdCondition, PointIdType, ScoredPoint, WithPayloadInterface, WithVector,
};
use tokio::runtime::Handle;

use super::LocalShard;
use crate::collection_manager::segments_searcher::SegmentsSearcher;
use crate::operations::types::{
    CollectionError, CollectionResult, CoreSearchRequest, CoreSearchRequestBatch,
    ScrollRequestInternal,
};
use crate::operations::universal_query::planned_query::{MergeSources, RescoreParams, Source};
use crate::operations::universal_query::planned_query_batch::PlannedQueryBatch;
use crate::operations::universal_query::shard_query::{Fusion, ScoringQuery, ShardQueryResponse};

pub enum FetchedSource {
    Core(usize),
    Scroll(usize),
}

struct PrefetchHolder {
    core_results: Vec<Vec<ScoredPoint>>,
    scrolls: Vec<Vec<ScoredPoint>>,
}

impl PrefetchHolder {
    fn new(core_results: Vec<Vec<ScoredPoint>>, scrolls: Vec<Vec<ScoredPoint>>) -> Self {
        Self {
            core_results,
            scrolls,
        }
    }

    fn get(&self, element: FetchedSource) -> CollectionResult<Cow<'_, Vec<ScoredPoint>>> {
        match element {
            FetchedSource::Core(idx) => self.core_results.get(idx).map(Cow::Borrowed),
            FetchedSource::Scroll(idx) => self.scrolls.get(idx).map(Cow::Borrowed),
        }
        .ok_or_else(|| CollectionError::service_error("Expected a prefetched source to exist"))
    }
}

impl LocalShard {
    pub async fn do_planned_query_batch(
        &self,
        request: PlannedQueryBatch,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<ShardQueryResponse>> {
        let start_time = std::time::Instant::now();
        let timeout = timeout.unwrap_or(self.shared_storage_config.search_timeout);

        let core_results_f = self.do_search(
            Arc::new(CoreSearchRequestBatch {
                searches: request.searches,
            }),
            search_runtime_handle,
            Some(timeout),
        );

        let scrolls_f =
            self.query_scroll_batch(Arc::new(request.scrolls), search_runtime_handle, timeout);

        // execute both searches and scrolls concurrently
        let (core_results, scrolls) = tokio::try_join!(core_results_f, scrolls_f)?;
        let prefetch_holder = PrefetchHolder::new(core_results, scrolls);

        let (all_merge_plans, all_with_payloads_or_vectors): (Vec<_>, Vec<_>) = request
            .root_queries
            .into_iter()
            .map(|query_plan| {
                (
                    query_plan.merge_sources,
                    (query_plan.with_payload, query_plan.with_vector),
                )
            })
            .unzip();

        // decrease timeout by the time spent so far
        let timeout = timeout.saturating_sub(start_time.elapsed());

        let merge_futures = all_merge_plans.into_iter().map(|merge_plan| {
            self.recurse_prefetch(
                merge_plan,
                &prefetch_holder,
                search_runtime_handle,
                timeout,
                0,
            )
        });

        let batched_scored_points = futures::future::try_join_all(merge_futures).await?;

        // fetch payload and/or vector for scored points if necessary
        //
        // TODO(universal-query): it might make sense to change this approach to fetch payload and vector at the collection level
        // after the shard results merge, but it requires careful benchmarking
        let fill_futures = batched_scored_points
            .into_iter()
            .zip(all_with_payloads_or_vectors)
            .map(|(query_response, (with_payload, with_vector))| {
                self.fill_with_payload_or_vectors(query_response, with_payload, with_vector)
            });

        let batched_scored_points = futures::future::try_join_all(fill_futures).await?;

        Ok(batched_scored_points)
    }

    /// Fetches the payload and/or vector if required. This will filter out points if they are deleted between search and retrieve.
    async fn fill_with_payload_or_vectors(
        &self,
        query_response: ShardQueryResponse,
        with_payload: WithPayloadInterface,
        with_vector: WithVector,
    ) -> CollectionResult<ShardQueryResponse> {
        let filled = if with_payload.is_required() || with_vector.is_enabled() {
            // ids to retrieve (deduplication happens in the searcher)
            let point_ids = query_response
                .iter()
                .flatten()
                .map(|scored_point| scored_point.id)
                .collect::<Vec<PointIdType>>();

            let mut records_iter = SegmentsSearcher::retrieve(
                self.segments(),
                &point_ids,
                &(&with_payload).into(),
                &with_vector,
            )?
            .into_iter()
            .peekable();

            query_response
                .into_iter()
                .map(|intermediate| {
                    intermediate
                        .into_iter()
                        // Points might get deleted between search and retrieve.
                        // But it's not a problem, because we just filter them out.
                        .filter_map(|mut scored_point| {
                            match records_iter.peeking_next(|record| record.id == scored_point.id) {
                                Some(mut record) => {
                                    scored_point.payload = record.payload.take();
                                    scored_point.vector = record.vector.take();
                                    Some(scored_point)
                                }
                                None => {
                                    // The record was not found, skip this scored point
                                    None
                                }
                            }
                        })
                        .collect_vec()
                })
                .collect_vec()
        } else {
            query_response
        };

        Ok(filled)
    }

    fn recurse_prefetch<'shard, 'query>(
        &'shard self,
        merge_sources: MergeSources,
        prefetch_holder: &'query PrefetchHolder,
        search_runtime_handle: &'shard Handle,
        timeout: Duration,
        depth: usize,
    ) -> BoxFuture<'query, CollectionResult<Vec<Vec<ScoredPoint>>>>
    where
        'shard: 'query,
    {
        async move {
            let max_len = merge_sources.sources.len();
            let mut cow_sources = Vec::with_capacity(max_len);

            // We need to preserve the order of the sources for some fusion strategies
            for source in merge_sources.sources.into_iter() {
                match source {
                    Source::SearchesIdx(idx) => {
                        cow_sources.push(prefetch_holder.get(FetchedSource::Core(idx))?)
                    }
                    Source::ScrollsIdx(idx) => {
                        cow_sources.push(prefetch_holder.get(FetchedSource::Scroll(idx))?)
                    }
                    Source::Prefetch(prefetch) => {
                        let merged = self
                            .recurse_prefetch(
                                prefetch,
                                prefetch_holder,
                                search_runtime_handle,
                                timeout,
                                depth + 1,
                            )
                            .await?
                            .into_iter()
                            .map(Cow::Owned);
                        cow_sources.extend(merged);
                    }
                }
            }

            let root_query_needs_intermediate_results = || {
                merge_sources
                    .rescore_params
                    .as_ref()
                    .map(|params| params.rescore.needs_intermediate_results())
                    .unwrap_or(false)
            };

            if depth == 0 && root_query_needs_intermediate_results() {
                // TODO(universal-query): maybe there's a way to pass ownership of the prefetch_holder to avoid cloning with Cow::into_owned here

                // in case of top level RRF, we need to propagate intermediate results
                Ok(cow_sources.into_iter().map(Cow::into_owned).collect())
            } else {
                let merged = self
                    .merge_sources(
                        cow_sources,
                        merge_sources.rescore_params,
                        search_runtime_handle,
                        timeout,
                    )
                    .await?;
                Ok(vec![merged])
            }
        }
        .boxed()
    }

    /// Rescore list of scored points
    #[allow(clippy::too_many_arguments)]
    async fn rescore<'a>(
        &self,
        sources: impl Iterator<Item = Cow<'a, Vec<ScoredPoint>>>,
        rescore_params: RescoreParams,
        search_runtime_handle: &Handle,
        timeout: Duration,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let RescoreParams {
            rescore,
            score_threshold,
            limit,
        } = rescore_params;

        match rescore {
            ScoringQuery::Fusion(Fusion::Rrf) => {
                let sources: Vec<_> = sources.map(Cow::into_owned).collect();

                let mut top_rrf = rrf_scoring(sources);

                if let Some(score_threshold) = score_threshold {
                    top_rrf = top_rrf
                        .into_iter()
                        .take_while(|point| point.score >= score_threshold)
                        .take(limit)
                        .collect();
                } else {
                    top_rrf.truncate(limit);
                };

                Ok(top_rrf)
            }
            ScoringQuery::OrderBy(order_by) => {
                // create single scroll request for rescoring query
                let filter = filter_with_sources_ids(sources);

                // Note: score_threshold is not used in this case, as all results will have same score,
                // but different order_value
                let scroll_request = ScrollRequestInternal {
                    offset: None,
                    limit: Some(limit),
                    filter: Some(filter),
                    with_payload: Some(WithPayloadInterface::Bool(false)),
                    with_vector: WithVector::Bool(false),
                    order_by: Some(OrderByInterface::Struct(order_by)),
                };

                self.query_scroll_batch(
                    Arc::new(vec![scroll_request]),
                    search_runtime_handle,
                    timeout,
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
                let filter = filter_with_sources_ids(sources);

                let search_request = CoreSearchRequest {
                    query: query_enum,
                    filter: Some(filter),
                    params: None,
                    limit,
                    offset: 0,
                    with_payload: None, // the payload is fetched separately
                    with_vector: None,  // the vector is fetched separately
                    score_threshold,
                };

                let rescoring_core_search_request = CoreSearchRequestBatch {
                    searches: vec![search_request],
                };

                self.do_search(
                    Arc::new(rescoring_core_search_request),
                    search_runtime_handle,
                    Some(timeout),
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
        }
    }

    /// Merge multiple prefetches into a single result up to the limit.
    /// Rescores if required.
    async fn merge_sources<'a>(
        &self,
        mut sources: Vec<Cow<'a, Vec<ScoredPoint>>>,
        rescore_params: Option<RescoreParams>,
        search_runtime_handle: &Handle,
        timeout: Duration,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        if let Some(rescore_params) = rescore_params {
            self.rescore(
                sources.into_iter(),
                rescore_params,
                search_runtime_handle,
                timeout,
            )
            .await
        } else {
            // The whole query request has no prefetches, and everything comes directly from a single source
            debug_assert_eq!(sources.len(), 1, "No prefetches, but multiple sources");

            let top = sources
                .pop()
                .ok_or_else(|| {
                    CollectionError::service_error("No sources to merge in the query request")
                })?
                .into_owned();

            Ok(top)
        }
    }
}

/// Extracts point ids from sources, and creates a filter to only include those ids.
fn filter_with_sources_ids<'a>(sources: impl Iterator<Item = Cow<'a, Vec<ScoredPoint>>>) -> Filter {
    let mut point_ids = HashSet::new();

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
