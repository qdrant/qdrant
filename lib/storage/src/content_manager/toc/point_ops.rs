use std::time::Duration;

use collection::collection::Collection;
use collection::grouping::group_by::GroupRequest;
use collection::grouping::GroupBy;
use collection::operations::consistency_params::ReadConsistency;
use collection::operations::point_ops::WriteOrdering;
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::*;
use collection::operations::CollectionUpdateOperations;
use collection::{discovery, recommendations};
use futures::future::try_join_all;
use segment::types::{ScoredPoint, ShardKey};

use super::TableOfContent;
use crate::content_manager::errors::StorageError;

impl TableOfContent {
    /// Recommend points using positive and negative example from the request
    ///
    /// # Arguments
    ///
    /// * `collection_name` - for what collection do we recommend
    /// * `request` - [`RecommendRequestInternal`]
    ///
    /// # Result
    ///
    /// Points with recommendation score
    pub async fn recommend(
        &self,
        collection_name: &str,
        request: RecommendRequestInternal,
        read_consistency: Option<ReadConsistency>,
        shard_selector: ShardSelectorInternal,
        timeout: Option<Duration>,
    ) -> Result<Vec<ScoredPoint>, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        recommendations::recommend_by(
            request,
            &collection,
            |name| self.get_collection_opt(name),
            read_consistency,
            shard_selector,
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
        requests: Vec<(RecommendRequestInternal, ShardSelectorInternal)>,
        read_consistency: Option<ReadConsistency>,
        timeout: Option<Duration>,
    ) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        recommendations::recommend_batch_by(
            requests,
            &collection,
            |name| self.get_collection_opt(name),
            read_consistency,
            timeout,
        )
        .await
        .map_err(|err| err.into())
    }

    /// Search in a batching fashion for the closest points using vector similarity with given restrictions defined
    /// in the request
    ///
    /// # Arguments
    ///
    /// * `collection_name` - in what collection do we search
    /// * `request` - [`CoreSearchRequestBatch`]
    /// * `shard_selection` - which local shard to use
    /// * `timeout` - how long to wait for the response
    /// * `read_consistency` - consistency level
    ///
    /// # Result
    ///
    /// Points with search score
    pub async fn core_search_batch(
        &self,
        collection_name: &str,
        request: CoreSearchRequestBatch,
        read_consistency: Option<ReadConsistency>,
        shard_selection: ShardSelectorInternal,
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
    /// * `request` - [`CountRequestInternal`]
    /// * `shard_selection` - which local shard to use
    ///
    /// # Result
    ///
    /// Number of points in the collection.
    ///
    pub async fn count(
        &self,
        collection_name: &str,
        request: CountRequestInternal,
        read_consistency: Option<ReadConsistency>,
        shard_selection: ShardSelectorInternal,
    ) -> Result<CountResult, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .count(request, read_consistency, &shard_selection)
            .await
            .map_err(|err| err.into())
    }

    /// Return specific points by IDs
    ///
    /// # Arguments
    ///
    /// * `collection_name` - select from this collection
    /// * `request` - [`PointRequestInternal`]
    /// * `shard_selection` - which local shard to use
    ///
    /// # Result
    ///
    /// List of points with specified information included
    pub async fn retrieve(
        &self,
        collection_name: &str,
        request: PointRequestInternal,
        read_consistency: Option<ReadConsistency>,
        shard_selection: ShardSelectorInternal,
    ) -> Result<Vec<Record>, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .retrieve(request, read_consistency, &shard_selection)
            .await
            .map_err(|err| err.into())
    }

    pub async fn group(
        &self,
        collection_name: &str,
        request: GroupRequest,
        read_consistency: Option<ReadConsistency>,
        shard_selection: ShardSelectorInternal,
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
            .map(|mut groups| GroupsResult {
                groups: groups.pop().unwrap_or_default(),
            })
            .map_err(|err| err.into())
    }

    pub async fn group_batch(
        &self,
        collection_name: &str,
        request: Vec<GroupRequest>,
        read_consistency: Option<ReadConsistency>,
        shard_selection: ShardSelectorInternal,
        timeout: Option<Duration>,
    ) -> Result<Vec<GroupsResult>, StorageError> {
        let collection = self.get_collection(collection_name).await?;

        let collection_by_name = |name| self.get_collection_opt(name);

        let group_by = GroupBy::batch(request, &collection, collection_by_name)
            .set_read_consistency(read_consistency)
            .set_shard_selection(shard_selection)
            .set_timeout(timeout);

        group_by
            .execute()
            .await
            .map(|groups| {
                groups
                    .into_iter()
                    .map(|groups| GroupsResult { groups })
                    .collect()
            })
            .map_err(|err| err.into())
    }

    pub async fn discover(
        &self,
        collection_name: &str,
        request: DiscoverRequestInternal,
        read_consistency: Option<ReadConsistency>,
        shard_selector: ShardSelectorInternal,
        timeout: Option<Duration>,
    ) -> Result<Vec<ScoredPoint>, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        discovery::discover(
            request,
            &collection,
            |name| self.get_collection_opt(name),
            read_consistency,
            shard_selector,
            timeout,
        )
        .await
        .map_err(|err| err.into())
    }

    pub async fn discover_batch(
        &self,
        collection_name: &str,
        requests: Vec<(DiscoverRequestInternal, ShardSelectorInternal)>,
        read_consistency: Option<ReadConsistency>,
        timeout: Option<Duration>,
    ) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
        let collection = self.get_collection(collection_name).await?;

        discovery::discover_batch(
            requests,
            &collection,
            |name| self.get_collection_opt(name),
            read_consistency,
            timeout,
        )
        .await
        .map_err(|err| err.into())
    }

    /// Paginate over all stored points with given filtering conditions
    ///
    /// # Arguments
    ///
    /// * `collection_name` - which collection to use
    /// * `request` - [`ScrollRequestInternal`]
    /// * `shard_selection` - which local shard to use
    ///
    /// # Result
    ///
    /// List of points with specified information included
    pub async fn scroll(
        &self,
        collection_name: &str,
        request: ScrollRequestInternal,
        read_consistency: Option<ReadConsistency>,
        shard_selection: ShardSelectorInternal,
    ) -> Result<ScrollResult, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .scroll_by(request, read_consistency, &shard_selection)
            .await
            .map_err(|err| err.into())
    }

    async fn _update_shard_keys(
        collection: &Collection,
        shard_keys: Vec<ShardKey>,
        operation: CollectionUpdateOperations,
        wait: bool,
        ordering: WriteOrdering,
    ) -> Result<UpdateResult, StorageError> {
        if shard_keys.is_empty() {
            return Err(StorageError::bad_input("Empty shard keys selection"));
        }

        let updates: Vec<_> = shard_keys
            .into_iter()
            .map(|shard_key| {
                collection.update_from_client(operation.clone(), wait, ordering, Some(shard_key))
            })
            .collect();

        let results = try_join_all(updates).await?;

        Ok(results.into_iter().next().unwrap())
    }

    pub async fn update(
        &self,
        collection_name: &str,
        operation: CollectionUpdateOperations,
        wait: bool,
        ordering: WriteOrdering,
        shard_selector: ShardSelectorInternal,
    ) -> Result<UpdateResult, StorageError> {
        let collection = self.get_collection(collection_name).await?;

        // Ordered operation flow:
        //
        // ┌───────────────────┐
        // │ User              │
        // └┬──────────────────┘
        //  │ Shard: None
        //  │ Ordering: Strong
        //  │ ShardKey: Some("cats")
        // ┌▼──────────────────┐
        // │ First Node        │ <- update_from_client
        // └┬──────────────────┘
        //  │ Shard: Some(N)
        //  │ Ordering: Strong
        //  │ ShardKey: None
        // ┌▼──────────────────┐
        // │ Leader node       │ <- update_from_peer
        // └┬──────────────────┘
        //  │ Shard: Some(N)
        //  │ Ordering: None(Weak)
        //  │ ShardKey: None
        // ┌▼──────────────────┐
        // │ Updating node     │ <- update_from_peer
        // └───────────────────┘

        let _rate_limit = match &self.update_rate_limiter {
            None => None,
            Some(rate_limiter) => {
                // We only want to rate limit the first node in the chain
                if !shard_selector.is_shard_id() {
                    Some(rate_limiter.acquire().await)
                } else {
                    None
                }
            }
        };
        if operation.is_write_operation() {
            self.check_write_lock()?;
        }
        let res = match shard_selector {
            ShardSelectorInternal::Empty => {
                collection
                    .update_from_client(operation, wait, ordering, None)
                    .await?
            }
            ShardSelectorInternal::All => {
                let shard_keys = collection.get_shard_keys().await;
                if shard_keys.is_empty() {
                    collection
                        .update_from_client(operation, wait, ordering, None)
                        .await?
                } else {
                    Self::_update_shard_keys(&collection, shard_keys, operation, wait, ordering)
                        .await?
                }
            }
            ShardSelectorInternal::ShardKey(shard_key) => {
                collection
                    .update_from_client(operation, wait, ordering, Some(shard_key))
                    .await?
            }
            ShardSelectorInternal::ShardKeys(shard_keys) => {
                Self::_update_shard_keys(&collection, shard_keys, operation, wait, ordering).await?
            }
            ShardSelectorInternal::ShardId(shard_selection) => {
                collection
                    .update_from_peer(operation, shard_selection, wait, ordering)
                    .await?
            }
        };
        Ok(res)
    }
}
