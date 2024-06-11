//! Methods here are for distributed internal use only.

use std::time::Duration;

use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::universal_query::shard_query::ShardQueryRequest;
use segment::types::ScoredPoint;

use super::TableOfContent;
use crate::content_manager::errors::StorageError;

impl TableOfContent {
    pub async fn query_internal(
        &self,
        collection_name: &str,
        request: ShardQueryRequest,
        shard_selection: ShardSelectorInternal,
        timeout: Option<Duration>,
    ) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
        let collection = self.get_collection_unchecked(collection_name).await?;

        let res = collection
            .query_internal(request, &shard_selection, timeout)
            .await?;

        Ok(res)
    }
}
