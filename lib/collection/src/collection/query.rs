use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::{future, TryFutureExt};
use itertools::{Either, Itertools};
use segment::common::reciprocal_rank_fusion::rrf_scoring;
use segment::common::score_fusion::{score_fusion, ScoreFusion};
use segment::types::{Order, ScoredPoint};
use segment::utils::scored_point_ties::ScoredPointTies;
use tokio::sync::RwLockReadGuard;
use tokio::time::Instant;

use super::Collection;
use crate::common::batching::batch_requests;
use crate::common::fetch_vectors::{
    build_vector_resolver_queries, resolve_referenced_vectors_batch,
};
use crate::common::retrieve_request_trait::RetrieveRequest;
use crate::common::transpose_iterator::transposed_iter;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::operations::universal_query::collection_query::CollectionQueryRequest;
use crate::operations::universal_query::shard_query::{
    Fusion, ScoringQuery, ShardQueryRequest, ShardQueryResponse,
};

struct IntermediateQueryInfo<'a> {
    scoring_query: Option<&'a ScoringQuery>,
    /// Limit + offset
    take: usize,
}

impl Collection {
    /// Returns a shape of [shard_id, batch_id, intermediate_response, points]
    async fn batch_query_shards_concurrently(
        &self,
        batch_request: Arc<Vec<ShardQueryRequest>>,
        read_consistency: Option<ReadConsistency>,
        shard_selection: &ShardSelectorInternal,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<Vec<ShardQueryResponse>>> {
        // query all shards concurrently
        let shard_holder = self.shards_holder.read().await;
        let target_shards = shard_holder.select_shards(shard_selection)?;
        let all_searches = target_shards.iter().map(|(shard, shard_key)| {
            let shard_key = shard_key.cloned();
            shard
                .query_batch(
                    Arc::clone(&batch_request),
                    read_consistency,
                    shard_selection.is_shard_id(),
                    timeout,
                )
                .and_then(move |mut shard_responses| async move {
                    if shard_key.is_none() {
                        return Ok(shard_responses);
                    }
                    shard_responses
                        .iter_mut()
                        .flatten()
                        .flatten()
                        .for_each(|point| point.shard_key.clone_from(&shard_key));

                    Ok(shard_responses)
                })
        });
        future::try_join_all(all_searches).await
    }

    /// This function is used to query the collection. It will return a list of scored points.
    async fn do_query_batch(
        &self,
        requests_batch: Vec<ShardQueryRequest>,
        read_consistency: Option<ReadConsistency>,
        shard_selection: ShardSelectorInternal,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        let instant = Instant::now();

        let requests_batch = Arc::new(requests_batch);

        let all_shards_results = self
            .batch_query_shards_concurrently(
                requests_batch.clone(),
                read_consistency,
                &shard_selection,
                timeout,
            )
            .await?;

        let results_f = transposed_iter(all_shards_results)
            .zip(requests_batch.iter())
            .map(|(shards_results, request)| async {
                // shards_results shape: [num_shards, num_intermediate_results, num_points]
                let mut merged_intermediates = self
                    .merge_intermediate_results_from_shards(request, shards_results)
                    .await?;

                let result = if let Some(ScoringQuery::Fusion(fusion)) = &request.query {
                    // If the root query is a Fusion, the returned results correspond to each the prefetches.
                    let mut fused = match fusion {
                        Fusion::Rrf => rrf_scoring(merged_intermediates),
                        Fusion::Dbsf => score_fusion(merged_intermediates, ScoreFusion::dbsf()),
                    };
                    if let Some(score_threshold) = request.score_threshold {
                        fused = fused
                            .into_iter()
                            .take_while(|point| point.score >= score_threshold)
                            .collect();
                    }
                    fused
                } else {
                    // Otherwise, it will be a list with a single list of scored points.
                    debug_assert_eq!(merged_intermediates.len(), 1);
                    merged_intermediates.pop().ok_or_else(|| {
                        CollectionError::service_error(
                            "Query response was expected to have one list of results.",
                        )
                    })?
                };

                let result: Vec<ScoredPoint> = result
                    .into_iter()
                    .skip(request.offset)
                    .take(request.limit)
                    .collect();

                let filter_refs = request.filter_refs();
                self.post_process_if_slow_request(instant.elapsed(), filter_refs);

                Ok::<_, CollectionError>(result)
            });
        let results = future::try_join_all(results_f).await?;

        Ok(results)
    }

    /// To be called on the user-responding instance. Resolves ids into vectors, and merges the results from local and remote shards.
    ///
    /// This function is used to query the collection. It will return a list of scored points.
    pub async fn query_batch<'a, F, Fut>(
        &self,
        requests_batch: Vec<(CollectionQueryRequest, ShardSelectorInternal)>,
        collection_by_name: F,
        read_consistency: Option<ReadConsistency>,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Option<RwLockReadGuard<'a, Collection>>>,
    {
        // Lift nested prefetches to root queries for vector resolution
        let resolver_requests = build_vector_resolver_queries(&requests_batch);

        // Build referenced vectors
        let ids_to_vectors = resolve_referenced_vectors_batch(
            &resolver_requests,
            self,
            collection_by_name,
            read_consistency,
        )
        .await?;

        // Check we actually fetched all referenced vectors from the resolver requests
        for (resolver_req, _) in &resolver_requests {
            for point_id in resolver_req.get_referenced_point_ids() {
                let lookup_collection = resolver_req.get_lookup_collection();
                if ids_to_vectors.get(&lookup_collection, point_id).is_none() {
                    return Err(CollectionError::PointNotFound {
                        missed_point_id: point_id,
                    });
                }
            }
        }

        let futures = batch_requests::<
            (CollectionQueryRequest, ShardSelectorInternal),
            ShardSelectorInternal,
            Vec<ShardQueryRequest>,
            Vec<_>,
        >(
            requests_batch,
            |(_req, shard)| shard,
            |(req, _), acc| {
                req.try_into_shard_request(&self.id, &ids_to_vectors)
                    .map(|shard_req| {
                        acc.push(shard_req);
                    })
            },
            |shard_selection, shard_requests, futures| {
                if shard_requests.is_empty() {
                    return Ok(());
                }

                futures.push(self.do_query_batch(
                    shard_requests,
                    read_consistency,
                    shard_selection,
                    timeout,
                ));

                Ok(())
            },
        )?;

        let results = future::try_join_all(futures)
            .await?
            .into_iter()
            .flatten()
            .collect();

        Ok(results)
    }

    /// To be called on the remote instance. Only used for the internal service.
    ///
    /// If the root query is a Fusion, the returned results correspond to each the prefetches.
    /// Otherwise, it will be a list with a single list of scored points.
    pub async fn query_batch_internal(
        &self,
        requests: Vec<ShardQueryRequest>,
        shard_selection: &ShardSelectorInternal,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<ShardQueryResponse>> {
        let requests_arc = Arc::new(requests);

        // Results from all shards
        // Shape: [num_shards, batch_size, num_intermediate_results, num_points]
        let all_shards_results = self
            .batch_query_shards_concurrently(
                Arc::clone(&requests_arc),
                None,
                shard_selection,
                timeout,
            )
            .await?;

        let merged_f = transposed_iter(all_shards_results)
            .zip(requests_arc.iter())
            .map(|(shards_results, request)| async {
                // shards_results shape: [num_shards, num_intermediate_results, num_points]
                self.merge_intermediate_results_from_shards(request, shards_results)
                    .await
            });
        let merged = futures::future::try_join_all(merged_f).await?;
        Ok(merged)
    }

    /// Merges the results in each shard for each intermediate query.
    /// ```text
    /// [ [shard1_result1, shard1_result2],
    ///          ↓               ↓
    ///   [shard2_result1, shard2_result2] ]
    ///
    /// = [merged_result1, merged_result2]
    /// ```
    async fn merge_intermediate_results_from_shards(
        &self,
        request: &ShardQueryRequest,
        all_shards_results: Vec<ShardQueryResponse>,
    ) -> CollectionResult<ShardQueryResponse> {
        let query_infos = intermediate_query_infos(request);
        let results_len = query_infos.len();
        let mut results = ShardQueryResponse::with_capacity(results_len);
        debug_assert!(all_shards_results
            .iter()
            .all(|shard_results| shard_results.len() == results_len));

        let collection_params = self.collection_config.read().await.params.clone();

        // Shape: [num_internal_queries, num_shards, num_scored_points]
        let all_shards_result_by_transposed = transposed_iter(all_shards_results);

        for (query_info, shards_results) in
            query_infos.into_iter().zip(all_shards_result_by_transposed)
        {
            // `shards_results` shape: [num_shards, num_scored_points]
            let order = ScoringQuery::order(query_info.scoring_query, &collection_params)?;

            // Equivalent to:
            //
            // shards_results
            //     .into_iter()
            //     .kmerge_by(match order {
            //         Order::LargeBetter => |a, b| ScoredPointTies(a) > ScoredPointTies(b),
            //         Order::SmallBetter => |a, b| ScoredPointTies(a) < ScoredPointTies(b),
            //     })
            //
            // if the `kmerge_by` function were able to work with reference predicates.
            // Either::Left and Either::Right are used to allow type inference to work.
            //
            let intermediate_result = match order {
                Order::LargeBetter => Either::Left(
                    shards_results
                        .into_iter()
                        .kmerge_by(|a, b| ScoredPointTies(a) > ScoredPointTies(b)),
                ),
                Order::SmallBetter => Either::Right(
                    shards_results
                        .into_iter()
                        .kmerge_by(|a, b| ScoredPointTies(a) < ScoredPointTies(b)),
                ),
            }
            .dedup()
            .take(query_info.take)
            .collect();

            results.push(intermediate_result);
        }

        Ok(results)
    }
}

/// Returns a list of the query that corresponds to each of the results in each shard.
///
/// Example: `[info1, info2, info3]` corresponds to `[result1, result2, result3]` of each shard
fn intermediate_query_infos(request: &ShardQueryRequest) -> Vec<IntermediateQueryInfo<'_>> {
    let needs_intermediate_results = request
        .query
        .as_ref()
        .map(|sq| sq.needs_intermediate_results())
        .unwrap_or(false);

    if needs_intermediate_results {
        // In case of Fusion, expect the propagated intermediate results
        request
            .prefetches
            .iter()
            .map(|prefetch| IntermediateQueryInfo {
                scoring_query: prefetch.query.as_ref(),
                take: prefetch.limit,
            })
            .collect_vec()
    } else {
        // Otherwise, we expect the root result
        vec![IntermediateQueryInfo {
            scoring_query: request.query.as_ref(),
            take: request.offset + request.limit,
        }]
    }
}
