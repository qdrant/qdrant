use std::time::Duration;

use collection::collection::Collection;
use collection::collection::distance_matrix::{
    CollectionSearchMatrixRequest, CollectionSearchMatrixResponse,
};
use collection::config::ShardingMethod;
use collection::grouping::GroupBy;
use collection::grouping::group_by::{GroupRequest, SourceRequest};
use collection::operations::consistency_params::ReadConsistency;
use collection::operations::point_ops::WriteOrdering;
use collection::operations::routing::RoutingToken;
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::*;
use collection::operations::universal_query::collection_query::{
    CollectionPrefetch, CollectionQueryRequest,
};
use collection::operations::{CollectionUpdateOperations, OperationWithClockTag};
use collection::shards::shard_trait::WaitUntil;
use collection::{discovery, recommendations};
use common::counter::hardware_accumulator::HwMeasurementAcc;
use futures::stream::{FuturesUnordered, StreamExt as _};
use segment::data_types::facets::{FacetParams, FacetResponse};
use segment::types::{PointIdType, ScoredPoint, ShardKey};
use shard::retrieve::record_internal::RecordInternal;
use shard::scroll::ScrollRequestInternal;
use shard::search::CoreSearchRequestBatch;

use super::TableOfContent;
use crate::content_manager::errors::{StorageError, StorageResult};
use crate::rbac::Auth;

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
    #[allow(clippy::too_many_arguments)]
    pub async fn recommend(
        &self,
        collection_name: &str,
        request: RecommendRequestInternal,
        read_consistency: Option<ReadConsistency>,
        routing_token: Option<RoutingToken>,
        shard_selector: ShardSelectorInternal,
        auth: Auth,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> StorageResult<Vec<ScoredPoint>> {
        let collection_pass = auth.check_point_op(collection_name, &request, "recommend")?;

        let collection = self.get_collection(&collection_pass).await?;
        self.validate_recommend_lookup_from(&request).await?;
        recommendations::recommend_by(
            request,
            &collection,
            |name| self.get_collection_opt(name),
            read_consistency,
            routing_token,
            shard_selector,
            timeout,
            hw_measurement_acc,
        )
        .await
        .map_err(|err| err.into())
    }

    /// Recommend points in a batching fashion using positive and negative example from the request
    ///
    /// # Arguments
    ///
    /// * `collection_name` - for what collection do we recommend
    /// * `requests` - [`RecommendRequestBatch`]
    ///
    /// # Result
    ///
    /// Points with recommendation score
    #[allow(clippy::too_many_arguments)]
    pub async fn recommend_batch(
        &self,
        collection_name: &str,
        mut requests: Vec<(RecommendRequestInternal, ShardSelectorInternal)>,
        read_consistency: Option<ReadConsistency>,
        routing_token: Option<RoutingToken>,
        auth: Auth,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> StorageResult<Vec<Vec<ScoredPoint>>> {
        let mut collection_pass = None;
        for (request, _shard_selector) in &mut requests {
            collection_pass =
                Some(auth.check_point_op(collection_name, request, "recommend_batch")?);
        }
        let Some(collection_pass) = collection_pass else {
            return Ok(vec![]);
        };

        let collection = self.get_collection(&collection_pass).await?;
        for (request, _shard_selector) in &requests {
            self.validate_recommend_lookup_from(request).await?;
        }
        recommendations::recommend_batch_by(
            requests,
            &collection,
            |name| self.get_collection_opt(name),
            read_consistency,
            routing_token,
            timeout,
            hw_measurement_acc,
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
    #[allow(clippy::too_many_arguments)]
    pub async fn core_search_batch(
        &self,
        collection_name: &str,
        mut request: CoreSearchRequestBatch,
        read_consistency: Option<ReadConsistency>,
        routing_token: Option<RoutingToken>,
        shard_selection: ShardSelectorInternal,
        auth: Auth,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> StorageResult<Vec<Vec<ScoredPoint>>> {
        let mut collection_pass = None;
        for request in &mut request.searches {
            collection_pass =
                Some(auth.check_point_op(collection_name, request, "core_search_batch")?);
        }
        let Some(collection_pass) = collection_pass else {
            return Ok(vec![]);
        };

        let collection = self.get_collection(&collection_pass).await?;
        collection
            .core_search_batch(
                request,
                read_consistency,
                routing_token,
                shard_selection,
                timeout,
                hw_measurement_acc,
            )
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
    #[allow(clippy::too_many_arguments)]
    pub async fn count(
        &self,
        collection_name: &str,
        request: CountRequestInternal,
        read_consistency: Option<ReadConsistency>,
        routing_token: Option<RoutingToken>,
        timeout: Option<Duration>,
        shard_selection: ShardSelectorInternal,
        auth: Auth,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> StorageResult<CountResult> {
        let collection_pass = auth.check_point_op(collection_name, &request, "count")?;

        let collection = self.get_collection(&collection_pass).await?;
        collection
            .count(
                request,
                read_consistency,
                routing_token,
                &shard_selection,
                timeout,
                hw_measurement_acc,
            )
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
    #[allow(clippy::too_many_arguments)]
    pub async fn retrieve(
        &self,
        collection_name: &str,
        request: PointRequestInternal,
        read_consistency: Option<ReadConsistency>,
        routing_token: Option<RoutingToken>,
        timeout: Option<Duration>,
        shard_selection: ShardSelectorInternal,
        auth: Auth,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> StorageResult<Vec<RecordInternal>> {
        let collection_pass = auth.check_point_op(collection_name, &request, "retrieve")?;

        let collection = self.get_collection(&collection_pass).await?;
        collection
            .retrieve(
                request,
                read_consistency,
                routing_token,
                &shard_selection,
                timeout,
                hw_measurement_acc,
            )
            .await
            .map_err(|err| err.into())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn group(
        &self,
        collection_name: &str,
        request: GroupRequest,
        read_consistency: Option<ReadConsistency>,
        routing_token: Option<RoutingToken>,
        shard_selection: ShardSelectorInternal,
        auth: Auth,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> StorageResult<GroupsResult> {
        let collection_pass = auth.check_point_op(collection_name, &request, "group")?;

        let collection = self.get_collection(&collection_pass).await?;
        self.validate_group_lookup_from(&request).await?;

        let collection_by_name = |name| self.get_collection_opt(name);

        let group_by = GroupBy::new(request, &collection, collection_by_name, hw_measurement_acc)
            .set_read_consistency(read_consistency)
            .set_routing_token(routing_token)
            .set_shard_selection(shard_selection)
            .set_timeout(timeout);

        group_by
            .execute()
            .await
            .map(|groups| GroupsResult { groups })
            .map_err(|err| err.into())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn discover(
        &self,
        collection_name: &str,
        request: DiscoverRequestInternal,
        read_consistency: Option<ReadConsistency>,
        routing_token: Option<RoutingToken>,
        shard_selector: ShardSelectorInternal,
        auth: Auth,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> StorageResult<Vec<ScoredPoint>> {
        let collection_pass = auth.check_point_op(collection_name, &request, "discover")?;

        let collection = self.get_collection(&collection_pass).await?;
        discovery::discover(
            request,
            &collection,
            |name| self.get_collection_opt(name),
            read_consistency,
            routing_token,
            shard_selector,
            timeout,
            hw_measurement_acc,
        )
        .await
        .map_err(|err| err.into())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn discover_batch(
        &self,
        collection_name: &str,
        mut requests: Vec<(DiscoverRequestInternal, ShardSelectorInternal)>,
        read_consistency: Option<ReadConsistency>,
        routing_token: Option<RoutingToken>,
        auth: Auth,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> StorageResult<Vec<Vec<ScoredPoint>>> {
        let mut collection_pass = None;
        for (request, _shard_selector) in &mut requests {
            collection_pass =
                Some(auth.check_point_op(collection_name, request, "discover_batch")?);
        }
        let Some(collection_pass) = collection_pass else {
            return Ok(vec![]);
        };

        let collection = self.get_collection(&collection_pass).await?;

        discovery::discover_batch(
            requests,
            &collection,
            |name| self.get_collection_opt(name),
            read_consistency,
            routing_token,
            timeout,
            hw_measurement_acc,
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
    #[allow(clippy::too_many_arguments)]
    pub async fn scroll(
        &self,
        collection_name: &str,
        request: ScrollRequestInternal,
        read_consistency: Option<ReadConsistency>,
        routing_token: Option<RoutingToken>,
        timeout: Option<Duration>,
        shard_selection: ShardSelectorInternal,
        auth: Auth,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> StorageResult<ScrollResult> {
        let collection_pass = auth.check_point_op(collection_name, &request, "scroll")?;

        let collection = self.get_collection(&collection_pass).await?;
        collection
            .scroll_by(
                request,
                read_consistency,
                routing_token,
                &shard_selection,
                timeout,
                hw_measurement_acc,
            )
            .await
            .map_err(|err| err.into())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn query_batch(
        &self,
        collection_name: &str,
        mut requests: Vec<(CollectionQueryRequest, ShardSelectorInternal)>,
        read_consistency: Option<ReadConsistency>,
        routing_token: Option<RoutingToken>,
        auth: Auth,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> StorageResult<Vec<Vec<ScoredPoint>>> {
        let mut collection_pass = None;
        for (request, _shard_selector) in &mut requests {
            collection_pass = Some(auth.check_point_op(collection_name, request, "query_batch")?);
        }
        let Some(collection_pass) = collection_pass else {
            // This can happen only if there are no requests
            return Ok(vec![]);
        };

        let collection = self.get_collection(&collection_pass).await?;
        for (request, _shard_selector) in &requests {
            self.validate_query_lookup_from(request).await?;
        }

        collection
            .query_batch(
                requests,
                |name| self.get_collection_opt(name),
                read_consistency,
                routing_token,
                timeout,
                hw_measurement_acc,
            )
            .await
            .map_err(|err| err.into())
    }

    // Return unique values for a payload key, and a count of points for each value.
    #[allow(clippy::too_many_arguments)]
    pub async fn facet(
        &self,
        collection_name: &str,
        request: FacetParams,
        shard_selection: ShardSelectorInternal,
        read_consistency: Option<ReadConsistency>,
        routing_token: Option<RoutingToken>,
        auth: Auth,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> StorageResult<FacetResponse> {
        let collection_pass = auth.check_point_op(collection_name, &request, "facet")?;

        let collection = self.get_collection(&collection_pass).await?;

        collection
            .facet(
                request,
                shard_selection,
                read_consistency,
                routing_token,
                timeout,
                hw_measurement_acc,
            )
            .await
            .map_err(StorageError::from)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn search_points_matrix(
        &self,
        collection_name: &str,
        request: CollectionSearchMatrixRequest,
        read_consistency: Option<ReadConsistency>,
        routing_token: Option<RoutingToken>,
        shard_selection: ShardSelectorInternal,
        auth: Auth,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> Result<CollectionSearchMatrixResponse, StorageError> {
        let collection_pass =
            auth.check_point_op(collection_name, &request, "search_points_matrix")?;

        let collection = self.get_collection(&collection_pass).await?;

        collection
            .search_points_matrix(
                request,
                shard_selection,
                read_consistency,
                routing_token,
                timeout,
                hw_measurement_acc,
            )
            .await
            .map_err(StorageError::from)
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
        wait: WaitUntil,
        timeout: Option<Duration>,
        ordering: WriteOrdering,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> StorageResult<UpdateResult> {
        // `Collection::update_from_client` is cancel safe, so this method is cancel safe.

        let updates: FuturesUnordered<_> = shard_keys
            .into_iter()
            .map(|shard_key| {
                collection.update_from_client(
                    operation.clone(),
                    wait,
                    timeout,
                    ordering,
                    Some(shard_key),
                    hw_measurement_acc.clone(),
                )
            })
            .collect();

        // `Collection::update_from_client` is cancel safe, so it's safe to use
        // `StreamExt::collect` to gather every per-shard result, including per-shard errors.
        let results: Vec<CollectionResult<UpdateResult>> = updates.collect().await;

        aggregate_multi_shard_update_results(results)
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe.
    #[allow(clippy::too_many_arguments)]
    pub async fn update(
        &self,
        collection_name: &str,
        operation: OperationWithClockTag,
        wait: WaitUntil,
        timeout: Option<Duration>,
        ordering: WriteOrdering,
        shard_selector: ShardSelectorInternal,
        auth: Auth,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> StorageResult<UpdateResult> {
        let collection_pass = auth.check_point_op(
            collection_name,
            &operation.operation,
            operation.operation.operation_name(),
        )?;

        // `TableOfContent::_update_shard_keys` and `Collection::update_from_*` are cancel safe,
        // so this method is cancel safe.

        let collection = self.get_collection(&collection_pass).await?;

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

        // TODO: `debug_assert(operation.clock_tag.is_none())` for `_update_shard_keys`/`update_from_client`!?

        let res = match shard_selector {
            ShardSelectorInternal::Empty => {
                collection
                    .update_from_client(
                        operation.operation,
                        wait,
                        timeout,
                        ordering,
                        None,
                        hw_measurement_acc.clone(),
                    )
                    .await?
            }

            ShardSelectorInternal::All => {
                let (sharding_method, shard_keys) = collection.get_sharding_method_and_keys().await;

                if shard_keys.is_empty() {
                    match sharding_method {
                        ShardingMethod::Custom => {
                            // No shards exist to apply the operation, but we acknowledge it
                            return Ok(UpdateResult {
                                operation_id: None,
                                status: UpdateStatus::Acknowledged,
                                clock_tag: operation.clock_tag,
                            });
                        }
                        ShardingMethod::Auto => {
                            collection
                                .update_from_client(
                                    operation.operation,
                                    wait,
                                    timeout,
                                    ordering,
                                    None,
                                    hw_measurement_acc.clone(),
                                )
                                .await?
                        }
                    }
                } else {
                    Self::_update_shard_keys(
                        &collection,
                        shard_keys,
                        operation.operation,
                        wait,
                        timeout,
                        ordering,
                        hw_measurement_acc.clone(),
                    )
                    .await?
                }
            }

            ShardSelectorInternal::ShardKey(shard_key) => {
                collection
                    .update_from_client(
                        operation.operation,
                        wait,
                        timeout,
                        ordering,
                        Some(shard_key),
                        hw_measurement_acc.clone(),
                    )
                    .await?
            }

            ShardSelectorInternal::ShardKeys(shard_keys) => {
                Self::_update_shard_keys(
                    &collection,
                    shard_keys,
                    operation.operation,
                    wait,
                    timeout,
                    ordering,
                    hw_measurement_acc.clone(),
                )
                .await?
            }

            ShardSelectorInternal::ShardKeyWithFallback(key) => {
                let shard_keys: Vec<_> = collection
                    .shards_holder()
                    .read()
                    .await
                    .route_with_fallback_for_write(key)?
                    .into_iter()
                    .map(|(_shard_ids, shard_key)| shard_key)
                    .collect();

                Self::_update_shard_keys(
                    &collection,
                    shard_keys,
                    operation.operation,
                    wait,
                    timeout,
                    ordering,
                    hw_measurement_acc.clone(),
                )
                .await?
            }
            ShardSelectorInternal::ShardId(shard_selection) => {
                collection
                    .update_from_peer(
                        operation,
                        shard_selection,
                        wait,
                        timeout,
                        ordering,
                        hw_measurement_acc.clone(),
                    )
                    .await?
            }
        };

        Ok(res)
    }

    async fn validate_lookup_from_collection_exists(
        &self,
        collection_name: &str,
    ) -> StorageResult<()> {
        match self.get_collection_unchecked(collection_name).await {
            Ok(_) => Ok(()),
            Err(StorageError::NotFound { .. }) => Err(StorageError::not_found(format!(
                "Collection {collection_name} not found"
            ))),
            Err(err) => Err(err),
        }
    }

    async fn validate_recommend_lookup_from(
        &self,
        request: &RecommendRequestInternal,
    ) -> StorageResult<()> {
        if let Some(lookup_from) = &request.lookup_from {
            self.validate_lookup_from_collection_exists(&lookup_from.collection)
                .await?;
        }
        Ok(())
    }

    async fn validate_query_lookup_from(
        &self,
        request: &CollectionQueryRequest,
    ) -> StorageResult<()> {
        if let Some(lookup_from) = &request.lookup_from {
            self.validate_lookup_from_collection_exists(&lookup_from.collection)
                .await?;
        }

        let mut prefetches: Vec<&CollectionPrefetch> = request.prefetch.iter().collect();
        while let Some(prefetch) = prefetches.pop() {
            if let Some(lookup_from) = &prefetch.lookup_from {
                self.validate_lookup_from_collection_exists(&lookup_from.collection)
                    .await?;
            }
            prefetches.extend(prefetch.prefetch.iter());
        }

        Ok(())
    }

    async fn validate_group_lookup_from(&self, request: &GroupRequest) -> StorageResult<()> {
        match &request.source {
            SourceRequest::Search(_) => {}
            SourceRequest::Recommend(request) => {
                self.validate_recommend_lookup_from(request).await?;
            }
            SourceRequest::Query(request) => {
                self.validate_query_lookup_from(request).await?;
            }
        }

        Ok(())
    }
}

/// Aggregate per-shard-key results from a fan-out dispatch in
/// [`TableOfContent::_update_shard_keys`].
///
/// The dispatcher clones the same full operation into every targeted shard key,
/// so when the operation references specific point ids, each replica set
/// legitimately reports `PointNotFound` for the points that live in *other*
/// shard keys. Naively propagating the first error makes the whole request
/// fail for requests that should obviously succeed.
///
/// The aggregation rule is intentionally conservative: we only swallow
/// `PointNotFound` when at least one shard key applied the operation
/// successfully. If *every* shard key reports `PointNotFound`, the points
/// truly do not exist anywhere in the collection, and we surface a
/// `StorageError::NotFound` that mirrors the single-shard behavior.
fn aggregate_multi_shard_update_results(
    results: Vec<CollectionResult<UpdateResult>>,
) -> StorageResult<UpdateResult> {
    if results.is_empty() {
        return Err(StorageError::bad_input("Empty shard keys selection"));
    }

    let mut first_success: Option<UpdateResult> = None;
    let mut first_non_point_not_found_error: Option<CollectionError> = None;
    let mut all_point_not_found = true;

    for result in results {
        match result {
            Ok(update_result) => {
                first_success.get_or_insert(update_result);
                all_point_not_found = false;
            }
            Err(CollectionError::PointNotFound { .. }) => {
                // Per-shard "this point isn't in my shard"; expected when the
                // caller targeted multiple shard keys.
            }
            Err(err) => {
                first_non_point_not_found_error.get_or_insert(err);
                all_point_not_found = false;
            }
        }
    }

    if let Some(update_result) = first_success {
        return Ok(update_result);
    }

    if all_point_not_found {
        // No shard owned any of the points — surface a representative
        // `PointNotFound` so the caller sees the same error as a single-shard
        // call would have produced.
        return Err(StorageError::NotFound {
            description: "No points with given ids found in any of the targeted shard keys"
                .to_string(),
        });
    }

    if let Some(err) = first_non_point_not_found_error {
        return Err(err.into());
    }

    // `first_success` is `None` and every error was `PointNotFound`, which the
    // branch above already handled. Reaching here means a non-`PointNotFound`
    // error was observed; the `if let Some(err) = ...` arm above would have
    // returned. We keep a defensive fallback so a future refactor cannot
    // accidentally return `Ok`.
    Err(StorageError::service_error(
        "Inconsistent state in `_update_shard_keys` result aggregation",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok() -> CollectionResult<UpdateResult> {
        Ok(UpdateResult {
            operation_id: None,
            clock_tag: None,
            status: UpdateStatus::Completed,
        })
    }

    fn point_not_found() -> CollectionResult<UpdateResult> {
        Err(CollectionError::PointNotFound {
            missed_point_id: PointIdType::from(9),
        })
    }

    fn service_error() -> CollectionResult<UpdateResult> {
        Err(CollectionError::ServiceError {
            error: "disk full".to_string(),
            backtrace: None,
        })
    }

    #[test]
    fn aggregator_returns_empty_input_as_bad_input() {
        let err = aggregate_multi_shard_update_results(vec![]).unwrap_err();
        assert!(matches!(err, StorageError::BadInput { .. }));
    }

    /// The exact reproducer for <https://github.com/qdrant/qdrant/issues/10064>:
    /// two shard keys, one reports success, the other reports `PointNotFound`
    /// for the foreign point. The aggregation must surface success.
    #[test]
    fn aggregator_swallows_cross_shard_point_not_found() {
        let result = aggregate_multi_shard_update_results(vec![ok(), point_not_found()]).unwrap();
        // The successful shard's `UpdateResult` is the one returned to the caller.
        assert!(matches!(result.status, UpdateStatus::Completed));
    }

    #[test]
    fn aggregator_swallows_point_not_found_when_all_other_shards_succeed() {
        let result =
            aggregate_multi_shard_update_results(vec![ok(), point_not_found(), point_not_found()])
                .unwrap();
        assert!(matches!(result.status, UpdateStatus::Completed));
    }

    /// Negative case: if every targeted shard key reports `PointNotFound`, the
    /// points really do not exist anywhere — we must not silently mask that
    /// error.
    #[test]
    fn aggregator_surfaces_not_found_when_every_shard_reports_point_not_found() {
        let err = aggregate_multi_shard_update_results(vec![point_not_found(), point_not_found()])
            .unwrap_err();
        assert!(
            matches!(err, StorageError::NotFound { .. }),
            "expected StorageError::NotFound, got {err:?}",
        );
    }

    /// A non-`PointNotFound` error must propagate even if other shards
    /// succeeded — it indicates a real problem the caller needs to react to.
    /// Ordering matters here: we only observe the service error if *no*
    /// shard succeeded.
    #[test]
    fn aggregator_surfaces_service_error_when_no_shard_succeeded() {
        let err = aggregate_multi_shard_update_results(vec![service_error(), point_not_found()])
            .unwrap_err();
        assert!(matches!(err, StorageError::ServiceError { .. }));
    }

    /// Mixed: one shard reports `PointNotFound`, another reports a
    /// non-`PointNotFound` error. The non-missing error wins, because it
    /// indicates a real problem that the caller needs to react to.
    #[test]
    fn aggregator_prefers_non_point_not_found_error_over_point_not_found() {
        let err = aggregate_multi_shard_update_results(vec![point_not_found(), service_error()])
            .unwrap_err();
        assert!(matches!(err, StorageError::ServiceError { .. }));
    }
}
