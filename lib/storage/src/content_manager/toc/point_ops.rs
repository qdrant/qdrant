use std::time::Duration;

use collection::collection::Collection;
use collection::grouping::group_by::GroupRequest;
use collection::grouping::GroupBy;
use collection::operations::consistency_params::ReadConsistency;
use collection::operations::point_ops::WriteOrdering;
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::*;
use collection::operations::{CollectionUpdateOperations, OperationWithClockTag};
use collection::{discovery, recommendations};
use futures::stream::FuturesUnordered;
use futures::TryStreamExt as _;
use segment::types::{ScoredPoint, ShardKey};

use super::TableOfContent;
use crate::content_manager::errors::StorageError;
use crate::rbac::Access;

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
        mut request: RecommendRequestInternal,
        read_consistency: Option<ReadConsistency>,
        shard_selector: ShardSelectorInternal,
        access: Access,
        timeout: Option<Duration>,
    ) -> Result<Vec<ScoredPoint>, StorageError> {
        let collection_pass = access.check_point_op(collection_name, &mut request)?;

        let collection = self.get_collection_by_pass(&collection_pass).await?;
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
        mut requests: Vec<(RecommendRequestInternal, ShardSelectorInternal)>,
        read_consistency: Option<ReadConsistency>,
        access: Access,
        timeout: Option<Duration>,
    ) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
        let mut collection_pass = None;
        for (request, _shard_selector) in &mut requests {
            collection_pass = Some(access.check_point_op(collection_name, request)?);
        }
        let Some(collection_pass) = collection_pass else {
            return Ok(vec![]);
        };

        let collection = self.get_collection_by_pass(&collection_pass).await?;
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
        mut request: CoreSearchRequestBatch,
        read_consistency: Option<ReadConsistency>,
        shard_selection: ShardSelectorInternal,
        access: Access,
        timeout: Option<Duration>,
    ) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
        let mut collection_pass = None;
        for request in &mut request.searches {
            collection_pass = Some(access.check_point_op(collection_name, request)?);
        }
        let Some(collection_pass) = collection_pass else {
            return Ok(vec![]);
        };

        let collection = self.get_collection_by_pass(&collection_pass).await?;
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
        mut request: CountRequestInternal,
        read_consistency: Option<ReadConsistency>,
        shard_selection: ShardSelectorInternal,
        access: Access,
    ) -> Result<CountResult, StorageError> {
        let collection_pass = access.check_point_op(collection_name, &mut request)?;

        let collection = self.get_collection_by_pass(&collection_pass).await?;
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
        mut request: PointRequestInternal,
        read_consistency: Option<ReadConsistency>,
        shard_selection: ShardSelectorInternal,
        access: Access,
    ) -> Result<Vec<Record>, StorageError> {
        let collection_pass = access.check_point_op(collection_name, &mut request)?;

        let collection = self.get_collection_by_pass(&collection_pass).await?;
        collection
            .retrieve(request, read_consistency, &shard_selection)
            .await
            .map_err(|err| err.into())
    }

    pub async fn group(
        &self,
        collection_name: &str,
        mut request: GroupRequest,
        read_consistency: Option<ReadConsistency>,
        shard_selection: ShardSelectorInternal,
        access: Access,
        timeout: Option<Duration>,
    ) -> Result<GroupsResult, StorageError> {
        let collection_pass = access.check_point_op(collection_name, &mut request)?;

        let collection = self.get_collection_by_pass(&collection_pass).await?;

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

    pub async fn discover(
        &self,
        collection_name: &str,
        mut request: DiscoverRequestInternal,
        read_consistency: Option<ReadConsistency>,
        shard_selector: ShardSelectorInternal,
        access: Access,
        timeout: Option<Duration>,
    ) -> Result<Vec<ScoredPoint>, StorageError> {
        let collection_pass = access.check_point_op(collection_name, &mut request)?;

        let collection = self.get_collection_by_pass(&collection_pass).await?;
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
        mut requests: Vec<(DiscoverRequestInternal, ShardSelectorInternal)>,
        read_consistency: Option<ReadConsistency>,
        access: Access,
        timeout: Option<Duration>,
    ) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
        let mut collection_pass = None;
        for (request, _shard_selector) in &mut requests {
            collection_pass = Some(access.check_point_op(collection_name, request)?);
        }
        let Some(collection_pass) = collection_pass else {
            return Ok(vec![]);
        };

        let collection = self.get_collection_by_pass(&collection_pass).await?;

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
        mut request: ScrollRequestInternal,
        read_consistency: Option<ReadConsistency>,
        shard_selection: ShardSelectorInternal,
        access: Access,
    ) -> Result<ScrollResult, StorageError> {
        let collection_pass = access.check_point_op(collection_name, &mut request)?;

        let collection = self.get_collection_by_pass(&collection_pass).await?;
        collection
            .scroll_by(request, read_consistency, &shard_selection)
            .await
            .map_err(|err| err.into())
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe.
    ///
    /// When it is cancelled, the operation may not be applied on some shard keys. But, all nodes
    /// are guaranteed to be consistent.
    async fn _update_shard_keys(
        collection: &Collection,
        shard_keys: Vec<ShardKey>,
        operation: CollectionUpdateOperations,
        wait: bool,
        ordering: WriteOrdering,
    ) -> Result<UpdateResult, StorageError> {
        // `Collection::update_from_client` is cancel safe, so this method is cancel safe.

        let updates: FuturesUnordered<_> = shard_keys
            .into_iter()
            .map(|shard_key| {
                collection.update_from_client(operation.clone(), wait, ordering, Some(shard_key))
            })
            .collect();

        // `Collection::update_from_client` is cancel safe, so it's safe to use `TryStreamExt::try_collect`
        let results: Vec<_> = updates.try_collect().await?;

        results
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::bad_input("Empty shard keys selection"))
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn update(
        &self,
        collection_name: &str,
        mut operation: OperationWithClockTag,
        wait: bool,
        ordering: WriteOrdering,
        shard_selector: ShardSelectorInternal,
        access: Access,
    ) -> Result<UpdateResult, StorageError> {
        let collection_pass = access.check_point_op(collection_name, &mut operation.operation)?;

        // `TableOfContent::_update_shard_keys` and `Collection::update_from_*` are cancel safe,
        // so this method is cancel safe.

        let collection = self.get_collection_by_pass(&collection_pass).await?;

        // Ordered operation flow:
        //
        // ┌───────────────────┐
        // │ User              │
        // └┬──────────────────┘
        //  │ Shard: None
        //  │ Ordering: Strong
        //  │ ShardKey: Some("cats")
        //  │ ClockTag: None
        // ┌▼──────────────────┐
        // │ First Node        │ <- update_from_client
        // └┬──────────────────┘
        //  │ Shard: Some(N)
        //  │ Ordering: Strong
        //  │ ShardKey: None
        //  │ ClockTag: None
        // ┌▼──────────────────┐
        // │ Leader node       │ <- update_from_peer
        // └┬──────────────────┘
        //  │ Shard: Some(N)
        //  │ Ordering: None(Weak)
        //  │ ShardKey: None
        //  │ ClockTag: { peer_id: IdOf(Leader node), clock_id: 1, clock_tick: 123 }
        // ┌▼──────────────────┐
        // │ Updating node     │ <- update_from_peer
        // └───────────────────┘

        let _update_rate_limiter = match &self.update_rate_limiter {
            Some(update_rate_limiter) => {
                // We only want to rate limit the first node in the chain
                if !shard_selector.is_shard_id() {
                    Some(update_rate_limiter.acquire().await)
                } else {
                    None
                }
            }

            None => None,
        };

        if operation.operation.is_write_operation() {
            self.check_write_lock()?;
        }

        // TODO: `debug_assert(operation.clock_tag.is_none())` for `_update_shard_keys`/`update_from_client`!?

        let res = match shard_selector {
            ShardSelectorInternal::Empty => {
                collection
                    .update_from_client(operation.operation, wait, ordering, None)
                    .await?
            }

            ShardSelectorInternal::All => {
                let shard_keys = collection.get_shard_keys().await;
                if shard_keys.is_empty() {
                    collection
                        .update_from_client(operation.operation, wait, ordering, None)
                        .await?
                } else {
                    Self::_update_shard_keys(
                        &collection,
                        shard_keys,
                        operation.operation,
                        wait,
                        ordering,
                    )
                    .await?
                }
            }

            ShardSelectorInternal::ShardKey(shard_key) => {
                collection
                    .update_from_client(operation.operation, wait, ordering, Some(shard_key))
                    .await?
            }

            ShardSelectorInternal::ShardKeys(shard_keys) => {
                Self::_update_shard_keys(
                    &collection,
                    shard_keys,
                    operation.operation,
                    wait,
                    ordering,
                )
                .await?
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
