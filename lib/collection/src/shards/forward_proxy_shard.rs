use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::tar_ext;
use common::types::TelemetryDetail;
use segment::data_types::facets::{FacetParams, FacetResponse};
use segment::data_types::manifest::SnapshotManifest;
use segment::index::field_index::CardinalityEstimation;
use segment::types::{
    ExtendedPointId, Filter, PointIdType, ScoredPoint, SizeStats, SnapshotFormat, WithPayload,
    WithPayloadInterface, WithVector,
};
use shard::retrieve::record_internal::RecordInternal;
use shard::search::CoreSearchRequestBatch;
use tokio::runtime::Handle;
use tokio::sync::Mutex;

use super::shard::ShardId;
use super::update_tracker::UpdateTracker;
use crate::hash_ring::HashRingRouter;
use crate::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted, PointSyncOperation,
};
use crate::operations::types::{
    CollectionError, CollectionInfo, CollectionResult, CountRequestInternal, CountResult,
    OptimizersStatus, PointRequestInternal, ScrollRequestInternal, UpdateResult, UpdateStatus,
};
use crate::operations::universal_query::shard_query::{ShardQueryRequest, ShardQueryResponse};
use crate::operations::{
    CollectionUpdateOperations, CreateIndex, FieldIndexOperations, OperationToShard,
    OperationWithClockTag, SplitByShard as _,
};
use crate::shards::local_shard::LocalShard;
use crate::shards::remote_shard::RemoteShard;
use crate::shards::shard_trait::ShardOperation;
use crate::shards::telemetry::LocalShardTelemetry;

/// ForwardProxyShard
///
/// ForwardProxyShard is a wrapper type for a LocalShard.
///
/// It can be used to provide all read and write operations while the wrapped shard is being transferred to another node.
/// Proxy forwards all operations to remote shards.
pub struct ForwardProxyShard {
    shard_id: ShardId,
    pub(crate) wrapped_shard: LocalShard,
    pub(crate) remote_shard: RemoteShard,
    resharding_hash_ring: Option<HashRingRouter>,
    /// Lock required to protect transfer-in-progress updates.
    /// It should block data updating operations while the batch is being transferred.
    update_lock: Mutex<()>,
}

impl ForwardProxyShard {
    pub fn new(
        shard_id: ShardId,
        wrapped_shard: LocalShard,
        remote_shard: RemoteShard,
        resharding_hash_ring: Option<HashRingRouter>,
        filter: Option<Filter>,
    ) -> Self {
        // Validate that `ForwardProxyShard` initialized correctly

        debug_assert!({
            let is_regular = shard_id == remote_shard.id && resharding_hash_ring.is_none();
            let is_resharding = shard_id != remote_shard.id && resharding_hash_ring.is_some();
            let is_replicating_points = shard_id != remote_shard.id && filter.is_some();

            is_regular || is_resharding || is_replicating_points
        });

        if shard_id == remote_shard.id && resharding_hash_ring.is_some() {
            log::warn!(
                "ForwardProxyShard initialized with resharding hashring, \
                 but wrapped shard id and remote shard id are the same",
            );
        }

        Self {
            shard_id,
            wrapped_shard,
            remote_shard,
            resharding_hash_ring,
            update_lock: Mutex::new(()),
            // todo: attach filter field and use it to filter points in `update` method
        }
    }

    /// Create payload indexes in the remote shard same as in the wrapped shard.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn transfer_indexes(&self) -> CollectionResult<()> {
        let _update_lock = self.update_lock.lock().await;
        for (index_key, index_type) in self.wrapped_shard.info().await?.payload_schema {
            // TODO: Is cancelling `RemoteShard::update` safe for *receiver*?
            self.remote_shard
                .update(
                    // TODO: Assign clock tag!? 🤔
                    OperationWithClockTag::from(CollectionUpdateOperations::FieldIndexOperation(
                        FieldIndexOperations::CreateIndex(CreateIndex {
                            field_name: index_key,
                            field_schema: Some(index_type.try_into()?),
                        }),
                    )),
                    false,
                    HwMeasurementAcc::disposable(), // Internal operation
                )
                .await?;
        }
        Ok(())
    }

    /// Move batch of points to the remote shard
    ///
    /// Returns new point offset and actual number of transferred points. The new point offset can
    /// be used to start the next batch from.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn transfer_batch(
        &self,
        offset: Option<PointIdType>,
        batch_size: usize,
        hashring_filter: Option<&HashRingRouter>,
        merge_points: bool,
        runtime_handle: &Handle,
    ) -> CollectionResult<(Option<PointIdType>, usize)> {
        debug_assert!(batch_size > 0);
        let _update_lock = self.update_lock.lock().await;

        let (points, next_page_offset) = match hashring_filter {
            Some(hashring_filter) => {
                self.read_batch_with_hashring(offset, batch_size, hashring_filter, runtime_handle)
                    .await?
            }
            None => self.read_batch(offset, batch_size, runtime_handle).await?,
        };

        // Only wait on last batch
        let wait = next_page_offset.is_none();
        let count = points.len();

        // Use sync API to leverage potentially existing points
        // Normally use SyncPoints, to completely replace everything in the target shard
        // For resharding we need to merge points from multiple transfers, requiring a different operation
        let point_operation = if !merge_points {
            PointOperations::SyncPoints(PointSyncOperation {
                from_id: offset,
                to_id: next_page_offset,
                points,
            })
        } else {
            PointOperations::UpsertPoints(PointInsertOperationsInternal::PointsList(points))
        };
        let insert_points_operation = CollectionUpdateOperations::PointOperation(point_operation);

        self.remote_shard
            .update(
                OperationWithClockTag::from(insert_points_operation),
                wait,
                HwMeasurementAcc::disposable(), // Internal operation
            ) // TODO: Assign clock tag!? 🤔
            .await?;

        Ok((next_page_offset, count))
    }

    /// Read a batch of points to transfer to the remote shard
    ///
    /// This function is optimized for reading and transferring 100% of the points in this shard
    /// without filtering. If you need to filter by hash ring, use [`read_batch_with_hashring`]
    /// instead.
    ///
    /// Returns batch of points and new point offset. The new point offset can be used to start the
    /// next batch from.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    async fn read_batch(
        &self,
        offset: Option<PointIdType>,
        batch_size: usize,
        runtime_handle: &Handle,
    ) -> CollectionResult<(Vec<PointStructPersisted>, Option<PointIdType>)> {
        let limit = batch_size + 1;

        let mut batch = self
            .wrapped_shard
            .local_scroll_by_id(
                offset,
                limit,
                &WithPayloadInterface::Bool(true),
                &WithVector::Bool(true),
                None,
                runtime_handle,
                None,                           // No timeout
                HwMeasurementAcc::disposable(), // Internal operation, no need to measure hardware here.
            )
            .await?;

        let next_page_offset = (batch.len() >= limit).then(|| batch.pop().unwrap().id);

        let points = batch
            .into_iter()
            .map(PointStructPersisted::try_from)
            .collect::<Result<Vec<PointStructPersisted>, String>>()?;

        Ok((points, next_page_offset))
    }

    /// Read a batch of points using a hash ring to transfer to the remote shard
    ///
    /// Only the points that satisfy the hash ring filter will be transferred.
    ///
    /// This applies oversampling in case of resharding to account for points that will be filtered
    /// out by the hash ring. Each batch of points should therefore be roughly `batch_size`, but it
    /// may be a bit smaller or larger.
    ///
    /// It is optimized for reading and transferring only a fraction of the points in this shard by
    /// using a hash ring. If you need to read and transfer 100% of the points, use [`read_batch`]
    /// instead.
    ///
    /// Returns batch of points and new point offset. The new point offset can be used to start the
    /// next batch from.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    async fn read_batch_with_hashring(
        &self,
        offset: Option<PointIdType>,
        batch_size: usize,
        hashring_filter: &HashRingRouter,
        runtime_handle: &Handle,
    ) -> CollectionResult<(Vec<PointStructPersisted>, Option<PointIdType>)> {
        // Oversample batch size to account for points that will be filtered out by the hash ring
        let oversample_factor = match &hashring_filter {
            HashRingRouter::Single(_) => 1,
            // - resharding: 1 -> 2, transfer 50%,  factor 2
            // - resharding: 2 -> 3, transfer 33%,  factor 3
            // - resharding: 3 -> 4, transfer 25%,  factor 4
            // - resharding: 2 -> 1, transfer 100%, factor 1
            // - resharding: 3 -> 2, transfer 50%,  factor 2
            // - resharding: 4 -> 3, transfer 33%,  factor 3
            HashRingRouter::Resharding { old: _, new } => new.len().max(1),
        };
        let limit = (batch_size * oversample_factor) + 1;

        // Read only point IDs without point data
        // We first make a preselection of those point IDs by applying the hash ring filter, and
        // then we read the actual point data in a separate request. It prevents reading a lot of
        // data we immediately discard due to the hash ring. That is much more efficient,
        // especially on large deployments when only a small fraction of points needs to be
        // transferred.
        let mut batch = self
            .wrapped_shard
            .local_scroll_by_id(
                offset,
                limit,
                &WithPayloadInterface::Bool(false),
                &WithVector::Bool(false),
                None,
                runtime_handle,
                None,                           // No timeout
                HwMeasurementAcc::disposable(), // Internal operation, no need to measure hardware here.
            )
            .await?;

        let next_page_offset = (batch.len() >= limit).then(|| batch.pop().unwrap().id);

        // Make preselection of point IDs by hash ring
        let ids = batch
            .into_iter()
            .map(|point| point.id)
            .filter(|point_id| hashring_filter.is_in_shard(point_id, self.remote_shard.id))
            .collect();

        // Read actual vectors and payloads for preselection of points
        let request = PointRequestInternal {
            ids,
            with_payload: Some(WithPayloadInterface::Bool(true)),
            with_vector: WithVector::Bool(true),
        };
        let batch = self
            .wrapped_shard
            .retrieve(
                Arc::new(request),
                &WithPayload::from(true),
                &WithVector::Bool(true),
                runtime_handle,
                None,                           // No timeout
                HwMeasurementAcc::disposable(), // Internal operation, no need to measure hardware here.
            )
            .await?;

        let points = batch
            .into_iter()
            .map(PointStructPersisted::try_from)
            .collect::<Result<Vec<PointStructPersisted>, String>>()?;

        Ok((points, next_page_offset))
    }

    pub fn deconstruct(self) -> (LocalShard, RemoteShard) {
        (self.wrapped_shard, self.remote_shard)
    }

    /// Forward `create_snapshot` to `wrapped_shard`
    pub async fn create_snapshot(
        &self,
        temp_path: &Path,
        tar: &tar_ext::BuilderExt,
        format: SnapshotFormat,
        manifest: Option<SnapshotManifest>,
        save_wal: bool,
    ) -> CollectionResult<()> {
        self.wrapped_shard
            .create_snapshot(temp_path, tar, format, manifest, save_wal)
            .await
    }

    pub fn snapshot_manifest(&self) -> CollectionResult<SnapshotManifest> {
        self.wrapped_shard.snapshot_manifest()
    }

    pub async fn on_optimizer_config_update(&self) -> CollectionResult<()> {
        self.wrapped_shard.on_optimizer_config_update().await
    }

    pub async fn on_strict_mode_config_update(&mut self) {
        self.wrapped_shard.on_strict_mode_config_update().await
    }

    pub fn trigger_optimizers(&self) {
        self.wrapped_shard.trigger_optimizers();
    }

    pub async fn get_telemetry_data(&self, detail: TelemetryDetail) -> LocalShardTelemetry {
        self.wrapped_shard.get_telemetry_data(detail).await
    }

    pub async fn get_optimization_status(&self) -> OptimizersStatus {
        self.wrapped_shard.get_optimization_status().await
    }

    pub async fn get_size_stats(&self) -> SizeStats {
        self.wrapped_shard.get_size_stats().await
    }

    pub fn update_tracker(&self) -> &UpdateTracker {
        self.wrapped_shard.update_tracker()
    }

    pub async fn estimate_cardinality(
        &self,
        filter: Option<&Filter>,
        hw_measurement_acc: &HwMeasurementAcc,
    ) -> CollectionResult<CardinalityEstimation> {
        self.wrapped_shard
            .estimate_cardinality(filter, hw_measurement_acc)
            .await
    }
}

#[async_trait]
impl ShardOperation for ForwardProxyShard {
    /// Update `wrapped_shard` while keeping track of the changed points
    ///
    /// # Cancel safety
    ///
    /// This method is *not* cancel safe.
    async fn update(
        &self,
        operation: OperationWithClockTag,
        _wait: bool,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<UpdateResult> {
        // If we apply `local_shard` update, we *have to* execute `remote_shard` update to completion
        // (or we *might* introduce an inconsistency between shards?), so this method is not cancel
        // safe.

        let _update_lock = self.update_lock.lock().await;

        // Shard update is within a write lock scope, because we need a way to block the shard updates
        // during the transfer restart and finalization.

        // We always have to wait for the result of the update, cause after we release the lock,
        // the transfer needs to have access to the latest version of points.
        let mut result = self
            .wrapped_shard
            .update(operation.clone(), true, hw_measurement_acc.clone())
            .await?;

        let forward_operation = if let Some(ring) = &self.resharding_hash_ring {
            // If `ForwardProxyShard::resharding_hash_ring` is `Some`, we assume that proxy is used
            // during *resharding* shard transfer, which forwards points to a remote shard with
            // *different* shard ID.

            debug_assert_ne!(self.shard_id, self.remote_shard.id);

            // Only forward a *part* of the operation that belongs to remote shard.
            let op = match operation.operation.split_by_shard(ring) {
                OperationToShard::ToAll(op) => Some(op),
                OperationToShard::ByShard(by_shard) => by_shard
                    .into_iter()
                    .find(|&(shard_id, _)| shard_id == self.remote_shard.id)
                    .map(|(_, op)| op),
            };

            // Strip the clock tag from the operation, because clock tags are incompatible between
            // different shards.
            //
            // Even though we expect (and assert) that this whole branch is only executed when
            // forwarding to a *different* remote shard, we still handle the case when local and
            // remote shards are the same, *just in case*.
            //
            // In such case `split_by_shard` call above would be a no-op, and we can preserve the
            // clock tag.
            let tag = if self.shard_id != self.remote_shard.id {
                None
            } else {
                log::warn!(
                    "ForwardProxyShard contains resharding hashring, \
                     but wrapped shard id and remote shard id are the same",
                );

                operation.clock_tag
            };

            op.map(|op| OperationWithClockTag::new(op, tag))
        } else {
            // If `ForwardProxyShard::resharding_hash_ring` is `None`, we assume that proxy is used
            // during *regular* shard transfer, so operation can be forwarded as-is, without any
            // additional handling.

            debug_assert_eq!(self.shard_id, self.remote_shard.id);

            Some(operation)
        };

        if let Some(operation) = forward_operation {
            let remote_result = self
                .remote_shard
                .update(operation, false, hw_measurement_acc)
                .await
                .map_err(|err| {
                    CollectionError::forward_proxy_error(self.remote_shard.peer_id, err)
                })?;

            // Merge `result` and `remote_result`:
            //
            // - Pick `clock_tag` with *newer* `clock_tick`
            let tick = result.clock_tag.map(|tag| tag.clock_tick);
            let remote_tick = remote_result.clock_tag.map(|tag| tag.clock_tick);

            if remote_tick > tick || tick.is_none() {
                result.clock_tag = remote_result.clock_tag;
            }

            // - If any node *rejected* the operation, propagate `UpdateStatus::ClockRejected`
            if remote_result.status == UpdateStatus::ClockRejected {
                result.status = UpdateStatus::ClockRejected;
            }
        }

        Ok(result)
    }

    /// Forward read-only `scroll_by` to `wrapped_shard`
    async fn scroll_by(
        &self,
        request: Arc<ScrollRequestInternal>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<RecordInternal>> {
        let local_shard = &self.wrapped_shard;
        local_shard
            .scroll_by(request, search_runtime_handle, timeout, hw_measurement_acc)
            .await
    }

    async fn local_scroll_by_id(
        &self,
        offset: Option<ExtendedPointId>,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<RecordInternal>> {
        let local_shard = &self.wrapped_shard;
        local_shard
            .local_scroll_by_id(
                offset,
                limit,
                with_payload_interface,
                with_vector,
                filter,
                search_runtime_handle,
                timeout,
                hw_measurement_acc,
            )
            .await
    }

    async fn info(&self) -> CollectionResult<CollectionInfo> {
        let local_shard = &self.wrapped_shard;
        local_shard.info().await
    }
    async fn core_search(
        &self,
        request: Arc<CoreSearchRequestBatch>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        let local_shard = &self.wrapped_shard;
        local_shard
            .core_search(request, search_runtime_handle, timeout, hw_measurement_acc)
            .await
    }

    async fn count(
        &self,
        request: Arc<CountRequestInternal>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<CountResult> {
        let local_shard = &self.wrapped_shard;
        local_shard
            .count(request, search_runtime_handle, timeout, hw_measurement_acc)
            .await
    }

    async fn retrieve(
        &self,
        request: Arc<PointRequestInternal>,
        with_payload: &WithPayload,
        with_vector: &WithVector,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<RecordInternal>> {
        let local_shard = &self.wrapped_shard;
        local_shard
            .retrieve(
                request,
                with_payload,
                with_vector,
                search_runtime_handle,
                timeout,
                hw_measurement_acc,
            )
            .await
    }

    async fn query_batch(
        &self,
        requests: Arc<Vec<ShardQueryRequest>>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<ShardQueryResponse>> {
        let local_shard = &self.wrapped_shard;
        local_shard
            .query_batch(requests, search_runtime_handle, timeout, hw_measurement_acc)
            .await
    }

    async fn facet(
        &self,
        request: Arc<FacetParams>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<FacetResponse> {
        let local_shard = &self.wrapped_shard;
        local_shard
            .facet(request, search_runtime_handle, timeout, hw_measurement_acc)
            .await
    }
}
