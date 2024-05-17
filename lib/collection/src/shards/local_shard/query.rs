use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::FutureExt;
use itertools::Itertools as _;
use segment::common::reciprocal_rank_fusion::rrf_scoring;
use segment::types::{Filter, HasIdCondition, ScoredPoint};
use tokio::runtime::Handle;

use super::LocalShard;
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
            .do_search(
                Arc::clone(&request.searches),
                search_runtime_handle,
                timeout,
            )
            .await?;

        let scrolls = todo!(); // TODO(universal-query): implement batch scrolling
        let merged_results = self
            .recurse_prefetch(
                &request.merge_plan,
                &core_results,
                scrolls,
                search_runtime_handle,
                timeout,
            )
            .await?;

        // TODO(universal-query): Implement with_vector and with_payload
        let results = vec![merged_results];
        Ok(results)
    }

    fn recurse_prefetch<'shard, 'query>(
        &'shard self,
        prefetch: &'query MergePlan,
        core_results: &'query Vec<Vec<ScoredPoint>>,
        scrolls: &'query Vec<Vec<ScoredPoint>>,
        search_runtime_handle: &'shard Handle,
        timeout: Option<Duration>,
    ) -> BoxFuture<'query, CollectionResult<Vec<ScoredPoint>>>
    where
        'shard: 'query,
    {
        async move {
            let mut sources = Vec::with_capacity(prefetch.sources.len());

            for source in prefetch.sources.iter() {
                let vec: Vec<ScoredPoint> = match source {
                    PrefetchSource::SearchesIdx(idx) => {
                        core_results.get(*idx).cloned().unwrap_or_default() // TODO(universal-query): don't clone, by using something like a hashmap instead of a vec
                    }
                    PrefetchSource::ScrollsIdx(idx) => {
                        scrolls.get(*idx).cloned().unwrap_or_default() // TODO(universal-query): don't clone, by using something like a hashmap instead of a vec
                    }
                    PrefetchSource::Prefetch(prefetch) => {
                        self.recurse_prefetch(
                            prefetch,
                            core_results,
                            scrolls,
                            search_runtime_handle,
                            timeout,
                        )
                        .await?
                    }
                };
                sources.push(vec);
            }

            self.merge_prefetches(sources, &prefetch.merge, search_runtime_handle, timeout)
                .await
        }
        .boxed()
    }

    /// Rescore list of scored points
    async fn rescore(
        &self,
        sources: Vec<Vec<ScoredPoint>>,
        rescore_query: &ScoringQuery,
        limit: usize,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        match rescore_query {
            ScoringQuery::Fusion(Fusion::Rrf) => {
                let top_rrf = rrf_scoring(sources, limit);
                Ok(top_rrf)
            }
            ScoringQuery::OrderBy(_) => {
                // TODO implement order by
                todo!()
            }
            ScoringQuery::Vector(query_enum) => {
                // create batch search request for rescoring query
                let search_requests = sources
                    .into_iter()
                    .map(|source| {
                        // extract target point ids to score
                        let point_ids: HashSet<_> =
                            source.iter().map(|scored_point| scored_point.id).collect();
                        // create filter for target point ids
                        let filter = Filter::new_must(segment::types::Condition::HasId(
                            HasIdCondition::from(point_ids),
                        ));
                        CoreSearchRequest {
                            query: query_enum.clone(),
                            filter: Some(filter),
                            params: None,
                            limit,
                            offset: 0,
                            with_payload: None, // the payload has already been fetched
                            with_vector: None,  // the vector has already been fetched
                            score_threshold: None,
                        }
                    })
                    .collect();
                let rescoring_core_search_request = CoreSearchRequestBatch {
                    searches: search_requests,
                };
                let core_results = self
                    .do_search(
                        Arc::new(rescoring_core_search_request),
                        search_runtime_handle,
                        timeout,
                    )
                    .await?;
                let top = core_results
                    .into_iter()
                    .flatten()
                    .sorted_unstable()
                    .take(limit)
                    .collect();
                Ok(top)
            }
        }
    }

    /// Merge multiple prefetches into a single result up to the limit.
    /// Rescores if required.
    async fn merge_prefetches(
        &self,
        sources: Vec<Vec<ScoredPoint>>,
        merge: &ResultsMerge,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        if let Some(rescore) = &merge.rescore {
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
