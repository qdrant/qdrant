use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::FutureExt;
use itertools::Itertools as _;
use segment::common::reciprocal_rank_fusion::rrf_scoring;
use segment::types::{Filter, HasIdCondition, PointIdType, ScoredPoint, WithPayload};
use tokio::runtime::Handle;

use super::LocalShard;
use crate::collection_manager::segments_searcher::SegmentsSearcher;
use crate::operations::types::{CollectionResult, CoreSearchRequest, CoreSearchRequestBatch};
use crate::operations::universal_query::planned_query::{
    MergePlan, PlannedQuery, PrefetchSource, ResultsMerge,
};
use crate::operations::universal_query::shard_query::{Fusion, ScoringQuery, ShardQueryResponse};

impl LocalShard {
    #[allow(unreachable_code, clippy::diverging_sub_expression, unused_variables)]
    pub async fn do_planned_query(
        &self,
        request: PlannedQuery,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
    ) -> CollectionResult<ShardQueryResponse> {
        let core_results = self
            .do_search(request.searches, search_runtime_handle, timeout)
            .await?;

        let scrolls = self
            .query_scroll_batch(request.scrolls, search_runtime_handle)
            .await?;

        let mut scored_points = self
            .recurse_prefetch(
                request.merge_plan,
                &core_results,
                &scrolls,
                search_runtime_handle,
                timeout,
                0, // initial depth
            )
            .await?;

        // fetch payload and/or vector for scored points if necessary
        if request.with_payload.is_required() || request.with_vector.is_enabled() {
            // ids to retrieve (deduplication happens in the searcher)
            let point_ids = scored_points
                .iter()
                .flatten()
                .map(|scored_point| scored_point.id)
                .collect::<Vec<PointIdType>>();

            // it might make sense to change this approach to fetch payload and vector at the collection level
            // after the shard results merge, but it requires careful benchmarking
            let mut records = SegmentsSearcher::retrieve(
                self.segments(),
                &point_ids,
                &WithPayload::from(&request.with_payload),
                &request.with_vector,
            )?;

            // update scored points in place
            for (scored_point, record) in scored_points.iter_mut().flatten().zip(records.iter_mut())
            {
                scored_point.payload = record.payload.take();
                scored_point.vector = record.vector.take();
            }
        }

        Ok(scored_points)
    }

    fn recurse_prefetch<'shard, 'query>(
        &'shard self,
        prefetch: MergePlan,
        core_results: &'query Vec<Vec<ScoredPoint>>,
        scrolls: &'query Vec<Vec<ScoredPoint>>,
        search_runtime_handle: &'shard Handle,
        timeout: Option<Duration>,
        depth: usize,
    ) -> BoxFuture<'query, CollectionResult<Vec<Vec<ScoredPoint>>>>
    where
        'shard: 'query,
    {
        async move {
            let mut sources: Vec<Vec<ScoredPoint>> = Vec::with_capacity(prefetch.sources.len());

            for source in prefetch.sources.into_iter() {
                let vec: Vec<Vec<ScoredPoint>> = match source {
                    PrefetchSource::SearchesIdx(idx) => {
                        // TODO(universal-query): don't clone, by using something like a hashmap instead of a vec
                        let scored_searches = core_results.get(idx).cloned().unwrap_or_default();
                        vec![scored_searches]
                    }
                    PrefetchSource::ScrollsIdx(idx) => {
                        // TODO(universal-query): don't clone, by using something like a hashmap instead of a vec
                        let scrolled = scrolls.get(idx).cloned().unwrap_or_default();
                        vec![scrolled]
                    }
                    PrefetchSource::Prefetch(prefetch) => {
                        self.recurse_prefetch(
                            prefetch,
                            core_results,
                            scrolls,
                            search_runtime_handle,
                            timeout,
                            depth + 1,
                        )
                        .await?
                    }
                };
                sources.extend(vec);
            }

            if depth == 0 && prefetch.merge.rescore == Some(ScoringQuery::Fusion(Fusion::Rrf)) {
                // in case of top level RRF, we need to propagate intermediate results
                Ok(sources)
            } else {
                let merged = self
                    .merge_prefetches(sources, prefetch.merge, search_runtime_handle, timeout)
                    .await?;
                Ok(vec![merged])
            }
        }
        .boxed()
    }

    /// Rescore list of scored points
    async fn rescore(
        &self,
        sources: Vec<Vec<ScoredPoint>>,
        rescore_query: ScoringQuery,
        limit: usize,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        match rescore_query {
            ScoringQuery::Fusion(Fusion::Rrf) => {
                let top_rrf = rrf_scoring(sources, limit);
                Ok(top_rrf)
            }
            ScoringQuery::OrderBy(o) => {
                // TODO(universal-query): implement order by
                todo!("order by not implemented yet for {:?}", o)
            }
            ScoringQuery::Vector(query_enum) => {
                // create single search request for rescoring query
                let point_ids = sources.into_iter().fold(HashSet::new(), |mut acc, source| {
                    for scored_point in source {
                        acc.insert(scored_point.id);
                    }
                    acc
                });

                // create filter for target point ids
                let filter = Filter::new_must(segment::types::Condition::HasId(
                    HasIdCondition::from(point_ids),
                ));

                let search_request = CoreSearchRequest {
                    query: query_enum,
                    filter: Some(filter),
                    params: None,
                    limit,
                    offset: 0,
                    with_payload: None, // the payload is fetched separately
                    with_vector: None,  // the vector is fetched separately
                    score_threshold: None,
                };

                let rescoring_core_search_request = CoreSearchRequestBatch {
                    searches: vec![search_request],
                };

                let top = self
                    .do_search(
                        Arc::new(rescoring_core_search_request),
                        search_runtime_handle,
                        timeout,
                    )
                    .await?
                    // One search request is sent. We expect only one result
                    .pop()
                    .unwrap_or_default();

                Ok(top)
            }
        }
    }

    /// Merge multiple prefetches into a single result up to the limit.
    /// Rescores if required.
    async fn merge_prefetches(
        &self,
        sources: Vec<Vec<ScoredPoint>>,
        merge: ResultsMerge,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        if let Some(rescore) = merge.rescore {
            self.rescore(
                sources,
                rescore,
                merge.limit,
                search_runtime_handle,
                timeout,
            )
            .await
        } else {
            // no rescore required - just merge, sort and limit
            let top = sources
                .into_iter()
                .flatten()
                .sorted_unstable()
                .take(merge.limit)
                .collect();
            Ok(top)
        }
    }
}
