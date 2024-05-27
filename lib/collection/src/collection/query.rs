use std::sync::Arc;
use std::{iter, mem};

use futures::{future, TryFutureExt};
use itertools::{Either, Itertools};
use segment::types::Order;
use segment::utils::scored_point_ties::ScoredPointTies;

use super::Collection;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::CollectionResult;
use crate::operations::universal_query::shard_query::{
    Fusion, ScoringQuery, ShardQueryRequest, ShardQueryResponse,
};

struct IntermediateQueryInfo<'a> {
    scoring_query: Option<&'a ScoringQuery>,
    take: usize,
}

impl Collection {
    async fn query_shards_concurrently(
        &self,
        request: Arc<ShardQueryRequest>,
        read_consistency: Option<ReadConsistency>,
        shard_selection: &ShardSelectorInternal,
    ) -> CollectionResult<Vec<ShardQueryResponse>> {
        // query all shards concurrently
        let shard_holder = self.shards_holder.read().await;
        let target_shards = shard_holder.select_shards(shard_selection)?;
        let all_searches = target_shards.iter().map(|(shard, shard_key)| {
            let shard_key = shard_key.cloned();
            shard
                .query(
                    Arc::clone(&request),
                    read_consistency,
                    shard_selection.is_shard_id(),
                )
                .and_then(move |mut records| async move {
                    if shard_key.is_none() {
                        return Ok(records);
                    }
                    for batch in &mut records {
                        for point in batch {
                            point.shard_key.clone_from(&shard_key);
                        }
                    }
                    Ok(records)
                })
        });
        future::try_join_all(all_searches).await
    }

    /// This is a remote shard implementation to querying the shards. Only used for the internal service
    pub async fn query_internal(
        &self,
        request: ShardQueryRequest,
        read_consistency: Option<ReadConsistency>,
        shard_selection: &ShardSelectorInternal,
    ) -> CollectionResult<ShardQueryResponse> {
        let request = Arc::new(request);

        let mut all_shards_results = self
            .query_shards_concurrently(Arc::clone(&request), read_consistency, shard_selection)
            .await?;

        let (num_intermediate_responses, intermediate_infos) =
            if let Some(ScoringQuery::Fusion(Fusion::Rrf)) = request.query {
                (
                    request.prefetches.len(),
                    Either::Left(
                        request
                            .prefetches
                            .iter()
                            .map(|prefetch| IntermediateQueryInfo {
                                scoring_query: prefetch.query.as_ref(),
                                take: prefetch.limit,
                            }),
                    ),
                )
            } else {
                (
                    1,
                    Either::Right(iter::once(IntermediateQueryInfo {
                        scoring_query: request.query.as_ref(),
                        take: request.offset + request.limit,
                    })),
                )
            };

        let collection_params = self.collection_config.read().await.params.clone();

        let mut result = ShardQueryResponse::with_capacity(num_intermediate_responses);

        for (idx, intermediate_info) in intermediate_infos.enumerate() {
            let intermediate_result_for_shard = all_shards_results
                .iter_mut()
                .map(|intermediates| mem::take(&mut intermediates[idx]));

            let order = collection_params.scoring_order(intermediate_info.scoring_query)?;

            let intermediate_result = match order {
                Order::LargeBetter => Either::Left(
                    intermediate_result_for_shard
                        .kmerge_by(|a, b| ScoredPointTies(a) > ScoredPointTies(b)),
                ),
                Order::SmallBetter => Either::Right(
                    intermediate_result_for_shard
                        .kmerge_by(|a, b| ScoredPointTies(a) < ScoredPointTies(b)),
                ),
            }
            .dedup()
            .take(intermediate_info.take)
            .collect();

            result.push(intermediate_result);
        }

        Ok(result)
    }
}
