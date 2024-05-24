//! Methods here are for distributed internal use only.

use collection::operations::{shard_selector_internal::ShardSelectorInternal, universal_query::shard_query::ShardQueryRequest};
use segment::types::ScoredPoint;

use crate::content_manager::errors::StorageError;

use super::TableOfContent;

impl TableOfContent {
    pub async fn query_internal(
        &self,
        collection_name: &str,
        _request: ShardQueryRequest,
        _shard_selection: ShardSelectorInternal, // TODO(universal-query): pass this to collection
    ) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
        let _collection = self.get_collection_unchecked(collection_name).await?;
        
        //TODO(universal-query): implement query_internal in collection
        // collection
        //     .query(request, read_consistency, &shard_selection)
        //     .await
        //     .map_err(|err| err.into())
        
        todo!()
    }
}