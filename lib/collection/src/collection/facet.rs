use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::FuturesUnordered;
use futures::TryStreamExt;
use itertools::Itertools;
use segment::data_types::facets::{FacetParams, FacetResponse, FacetValueHit};

use super::Collection;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::CollectionResult;

impl Collection {
    pub async fn facet(
        &self,
        request: FacetParams,
        shard_selection: ShardSelectorInternal,
        read_consistency: Option<ReadConsistency>,
        timeout: Option<Duration>,
    ) -> CollectionResult<FacetResponse> {
        if request.limit == 0 {
            return Ok(FacetResponse { hits: vec![] });
        }

        let request = Arc::new(request);

        let shard_holder = self.shards_holder.read().await;
        let target_shards = shard_holder.select_shards(&shard_selection)?;

        let mut shards_reads_f = target_shards
            .iter()
            .map(|(shard, _shard_key)| {
                shard.facet(
                    request.clone(),
                    read_consistency,
                    shard_selection.is_shard_id(),
                    timeout,
                )
            })
            .collect::<FuturesUnordered<_>>();

        let mut aggregated_results = HashMap::new();
        while let Some(response) = shards_reads_f.try_next().await? {
            for hit in response.hits {
                *aggregated_results.entry(hit.value).or_insert(0) += hit.count;
            }
        }

        let hits = aggregated_results
            .into_iter()
            .map(|(value, count)| FacetValueHit { value, count })
            .k_largest(request.limit)
            .collect();

        Ok(FacetResponse { hits })
    }
}
