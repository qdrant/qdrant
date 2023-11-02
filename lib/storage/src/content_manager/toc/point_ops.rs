use std::time::Duration;

use collection::grouping::group_by::GroupRequest;
use collection::grouping::GroupBy;
use collection::operations::consistency_params::ReadConsistency;
use collection::operations::point_ops::WriteOrdering;
use collection::operations::types::*;
use collection::operations::CollectionUpdateOperations;
use collection::recommendations;
use collection::shards::shard::ShardId;
use segment::types::ScoredPoint;

use super::TableOfContent;
use crate::content_manager::errors::StorageError;

impl TableOfContent {
    /// Recommend points using positive and negative example from the request
    ///
    /// # Arguments
    ///
    /// * `collection_name` - for what collection do we recommend
    /// * `request` - [`RecommendRequest`]
    ///
    /// # Result
    ///
    /// Points with recommendation score
    pub async fn recommend(
        &self,
        collection_name: &str,
        request: RecommendRequest,
        read_consistency: Option<ReadConsistency>,
        timeout: Option<Duration>,
    ) -> Result<Vec<ScoredPoint>, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        recommendations::recommend_by(
            request,
            &collection,
            |name| self.get_collection_opt(name),
            read_consistency,
            timeout,
        )
        .await
        .map_err(|err| err.into())
    }

    /// Recommend points in a batching fashion using positive and negative example from the request
    ///
    /// # Arguments
    ///
    /// * `collection_name` - for what collection do we recommend
    /// * `request` - [`RecommendRequestBatch`]
    ///
    /// # Result
    ///
    /// Points with recommendation score
    pub async fn recommend_batch(
        &self,
        collection_name: &str,
        request: RecommendRequestBatch,
        read_consistency: Option<ReadConsistency>,
        timeout: Option<Duration>,
    ) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        recommendations::recommend_batch_by(
            request,
            &collection,
            |name| self.get_collection_opt(name),
            read_consistency,
            timeout,
        )
        .await
        .map_err(|err| err.into())
    }

    /// Search for the closest points using vector similarity with given restrictions defined
    /// in the request
    ///
    /// # Arguments
    ///
    /// * `collection_name` - in what collection do we search
    /// * `request` - [`SearchRequest`]
    /// * `shard_selection` - which local shard to use
    /// # Result
    ///
    /// Points with search score
    pub async fn search(
        &self,
        collection_name: &str,
        request: SearchRequest,
        read_consistency: Option<ReadConsistency>,
        shard_selection: Option<ShardId>,
        timeout: Option<Duration>,
    ) -> Result<Vec<ScoredPoint>, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .search(request, read_consistency, shard_selection, timeout)
            .await
            .map_err(|err| err.into())
    }

    /// Search in a batching fashion for the closest points using vector similarity with given restrictions defined
    /// in the request
    ///
    /// # Arguments
    ///
    /// * `collection_name` - in what collection do we search
    /// * `request` - [`SearchRequestBatch`]
    /// * `shard_selection` - which local shard to use
    /// # Result
    ///
    /// Points with search score
    // ! COPY-PASTE: `core_search_batch` is a copy-paste of `search_batch` with different request type
    // ! please replicate any changes to both methods
    pub async fn search_batch(
        &self,
        collection_name: &str,
        request: SearchRequestBatch,
        read_consistency: Option<ReadConsistency>,
        shard_selection: Option<ShardId>,
        timeout: Option<Duration>,
    ) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .search_batch(request, read_consistency, shard_selection, timeout)
            .await
            .map_err(|err| err.into())
    }

    // ! COPY-PASTE: `core_search_batch` is a copy-paste of `search_batch` with different request type
    // ! please replicate any changes to both methods
    pub async fn core_search_batch(
        &self,
        collection_name: &str,
        request: CoreSearchRequestBatch,
        read_consistency: Option<ReadConsistency>,
        shard_selection: Option<ShardId>,
        timeout: Option<Duration>,
    ) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .core_search_batch(request, read_consistency, shard_selection, timeout)
            .await
            .map_err(|err| err.into())
    }

    /// Count points in the collection.
    ///
    /// # Arguments
    ///
    /// * `collection_name` - in what collection do we count
    /// * `request` - [`CountRequest`]
    /// * `shard_selection` - which local shard to use
    ///
    /// # Result
    ///
    /// Number of points in the collection.
    ///
    pub async fn count(
        &self,
        collection_name: &str,
        request: CountRequest,
        shard_selection: Option<ShardId>,
    ) -> Result<CountResult, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .count(request, shard_selection)
            .await
            .map_err(|err| err.into())
    }

    /// Return specific points by IDs
    ///
    /// # Arguments
    ///
    /// * `collection_name` - select from this collection
    /// * `request` - [`PointRequest`]
    /// * `shard_selection` - which local shard to use
    ///
    /// # Result
    ///
    /// List of points with specified information included
    pub async fn retrieve(
        &self,
        collection_name: &str,
        request: PointRequest,
        read_consistency: Option<ReadConsistency>,
        shard_selection: Option<ShardId>,
    ) -> Result<Vec<Record>, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .retrieve(request, read_consistency, shard_selection)
            .await
            .map_err(|err| err.into())
    }

    pub async fn group(
        &self,
        collection_name: &str,
        request: GroupRequest,
        read_consistency: Option<ReadConsistency>,
        shard_selection: Option<ShardId>,
        timeout: Option<Duration>,
    ) -> Result<GroupsResult, StorageError> {
        let collection = self.get_collection(collection_name).await?;

        let collection_by_name = |name| self.get_collection_opt(name);

        let group_by = GroupBy::new(request, &collection, collection_by_name)
            .set_read_consistency(read_consistency)
            .set_shard_selection(shard_selection)
            .set_timeout(timeout);

        group_by
            .execute()
            .await
            .map(|groups| GroupsResult { groups })
            .map_err(|err| err.into())
    }

    /// Paginate over all stored points with given filtering conditions
    ///
    /// # Arguments
    ///
    /// * `collection_name` - which collection to use
    /// * `request` - [`ScrollRequest`]
    /// * `shard_selection` - which local shard to use
    ///
    /// # Result
    ///
    /// List of points with specified information included
    pub async fn scroll(
        &self,
        collection_name: &str,
        request: ScrollRequest,
        read_consistency: Option<ReadConsistency>,
        shard_selection: Option<ShardId>,
    ) -> Result<ScrollResult, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .scroll_by(request, read_consistency, shard_selection)
            .await
            .map_err(|err| err.into())
    }

    pub async fn update(
        &self,
        collection_name: &str,
        operation: CollectionUpdateOperations,
        shard_selection: Option<ShardId>,
        wait: bool,
        ordering: WriteOrdering,
    ) -> Result<UpdateResult, StorageError> {
        let collection = self.get_collection(collection_name).await?;

        // Ordered operation flow:
        //
        // ┌───────────────────┐
        // │ User              │
        // └┬──────────────────┘
        //  │ Shard: None
        //  │ Ordering: Strong
        // ┌▼──────────────────┐
        // │ First Node        │ <- update_from_client
        // └┬──────────────────┘
        //  │ Shard: Some(N)
        //  │ Ordering: Strong
        // ┌▼──────────────────┐
        // │ Leader node       │ <- update_from_peer
        // └┬──────────────────┘
        //  │ Shard: Some(N)
        //  │ Ordering: None(Weak)
        // ┌▼──────────────────┐
        // │ Updating node     │ <- update_from_peer
        // └───────────────────┘

        let result = match shard_selection {
            Some(shard_selection) => {
                collection
                    .update_from_peer(operation, shard_selection, wait, ordering)
                    .await
            }
            None => {
                let _rate_limit = match &self.update_rate_limiter {
                    None => None,
                    Some(rate_limiter) => Some(rate_limiter.acquire().await),
                };
                if operation.is_write_operation() {
                    self.check_write_lock()?;
                }
                collection
                    .update_from_client(operation, wait, ordering)
                    .await
            }
        };
        result.map_err(|err| err.into())
    }
}
