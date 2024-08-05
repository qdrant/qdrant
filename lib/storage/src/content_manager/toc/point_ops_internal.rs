//! Methods here are for distributed internal use only.

use std::time::Duration;

use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::universal_query::shard_query::{ShardQueryRequest, ShardQueryResponse};
use segment::data_types::facets::{FacetRequestInternal, FacetResponse};

use super::TableOfContent;
use crate::content_manager::errors::StorageResult;

impl TableOfContent {
    pub async fn query_batch_internal(
        &self,
        collection_name: &str,
        requests: Vec<ShardQueryRequest>,
        shard_selection: ShardSelectorInternal,
        timeout: Option<Duration>,
    ) -> StorageResult<Vec<ShardQueryResponse>> {
        let collection = self.get_collection_unchecked(collection_name).await?;

        let res = collection
            .query_batch_internal(requests, &shard_selection, timeout)
            .await?;

        Ok(res)
    }

    pub async fn facet_internal(
        &self,
        collection_name: &str,
        request: FacetRequestInternal,
        shard_selection: ShardSelectorInternal,
        timeout: Option<Duration>,
    ) -> StorageResult<FacetResponse> {
        let collection = self.get_collection_unchecked(collection_name).await?;

        let res = collection
            .facet(request, shard_selection, None, timeout)
            .await?;

        Ok(res)
    }
}
