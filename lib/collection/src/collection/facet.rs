use std::sync::Arc;
use std::time::Duration;

use futures::future;
use itertools::Itertools;
use segment::data_types::facets::{
    aggregate_facet_hits, FacetRequestInternal, FacetResponse, FacetValueHit,
};

use super::Collection;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::CollectionResult;

impl Collection {
    pub async fn facet(
        &self,
        request: FacetRequestInternal,
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

        let shards_reads_f = target_shards.iter().map(|(shard, _shard_key)| {
            shard.facet(
                request.clone(),
                read_consistency,
                shard_selection.is_shard_id(),
                timeout,
            )
        });

        let shards_results = future::try_join_all(shards_reads_f).await?;

        let hits = aggregate_facet_hits(
            shards_results
                .into_iter()
                .flat_map(|FacetResponse { hits }| hits),
        )
        .into_iter()
        .map(|(value, count)| FacetValueHit { value, count })
        .k_largest(request.limit)
        .collect();

        Ok(FacetResponse { hits })
    }
}
