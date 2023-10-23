use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use segment::data_types::order_by::OrderBy;
use segment::types::{
    ExtendedPointId, Filter, PointIdType, ScoredPoint, WithPayload, WithPayloadInterface,
    WithVector,
};
use tokio::runtime::Handle;
use tokio::sync::{oneshot, RwLock};
use tokio::time::timeout;

use super::update_tracker::UpdateTracker;
use crate::operations::operation_effect::{
    EstimateOperationEffectArea, OperationEffectArea, PointsOperationEffect,
};
use crate::operations::types::{
    CollectionError, CollectionInfo, CollectionResult, CoreSearchRequestBatch,
    CountRequestInternal, CountResult, PointRequestInternal, Record, UpdateResult,
};
use crate::operations::OperationWithClockTag;
use crate::shards::local_shard::LocalShard;
use crate::shards::shard_trait::ShardOperation;
use crate::shards::telemetry::LocalShardTelemetry;
use crate::update_handler::UpdateSignal;

type ChangedPointsSet = Arc<RwLock<HashSet<PointIdType>>>;

/// ProxyShard
///
/// ProxyShard is a wrapper type for a LocalShard.
///
/// It can be used to provide all read and write operations while the wrapped shard is being transferred to another node.
/// It keeps track of changed points during the shard transfer to assure consistency.
pub struct ProxyShard {
    wrapped_shard: LocalShard,
    changed_points: ChangedPointsSet,
    pub changed_alot: AtomicBool,
}

/// Max number of updates tracked to synchronize after the transfer.
const MAX_CHANGES_TRACKED_COUNT: usize = 10_000;

/// How much time can we wait for the update queue to be empty.
/// We don't want false positive here, so it should be large.
/// If the queue stuck - it means something wrong with application logic.
const UPDATE_QUEUE_CLEAR_TIMEOUT: Duration = Duration::from_secs(1);
const UPDATE_QUEUE_CLEAR_MAX_TIMEOUT: Duration = Duration::from_secs(128);

impl ProxyShard {
    #[allow(unused)]
    pub async fn new(wrapped_shard: LocalShard) -> Self {
        let res = Self {
            wrapped_shard,
            changed_points: Default::default(),
            changed_alot: Default::default(),
        };
        res.reinit_changelog().await;
        res
    }

    /// Forward `create_snapshot` to `wrapped_shard`
    pub async fn create_snapshot(
        &self,
        temp_path: &Path,
        target_path: &Path,
        save_wal: bool,
    ) -> CollectionResult<()> {
        self.wrapped_shard
            .create_snapshot(temp_path, target_path, save_wal)
            .await
    }

    pub async fn on_optimizer_config_update(&self) -> CollectionResult<()> {
        self.wrapped_shard.on_optimizer_config_update().await
    }

    pub async fn reinit_changelog(&self) -> CollectionResult<()> {
        // Blocks updates in the wrapped shard.
        let mut changed_points_guard = self.changed_points.write().await;
        // Clear the update queue
        let mut attempt = 1;
        loop {
            let (tx, rx) = oneshot::channel();
            let plunger = UpdateSignal::Plunger(tx);
            self.wrapped_shard
                .update_sender
                .load()
                .send(plunger)
                .await?;
            let attempt_timeout = UPDATE_QUEUE_CLEAR_TIMEOUT * (2_u32).pow(attempt);
            // It is possible, that the queue is recreated while we are waiting for plunger.
            // So we will timeout and try again
            if timeout(attempt_timeout, rx).await.is_err() {
                log::warn!("Timeout {} while waiting for the wrapped shard to finish the update queue, retrying", attempt_timeout.as_secs());
                attempt += 1;
                if attempt_timeout > UPDATE_QUEUE_CLEAR_MAX_TIMEOUT {
                    return Err(CollectionError::service_error(
                        "Timeout while waiting for the wrapped shard to finish the update queue"
                            .to_string(),
                    ));
                }
                continue;
            }
            break;
        }
        // Update queue is clear now
        // Clear the changed_points set
        changed_points_guard.clear();

        // Clear changed_alot flag
        self.changed_alot
            .store(false, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    pub fn get_telemetry_data(&self) -> LocalShardTelemetry {
        self.wrapped_shard.get_telemetry_data()
    }

    pub fn update_tracker(&self) -> &UpdateTracker {
        self.wrapped_shard.update_tracker()
    }
}

#[async_trait]
impl ShardOperation for ProxyShard {
    /// Update `wrapped_shard` while keeping track of the changed points
    ///
    /// # Cancel safety
    ///
    /// This method is *not* cancel safe.
    async fn update(
        &self,
        operation: OperationWithClockTag,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        // If we modify `self.changed_points`, we *have to* (?) execute `local_shard` update
        // to completion, so this method is not cancel safe.

        let local_shard = &self.wrapped_shard;
        let estimate_effect = operation.operation.estimate_effect_area();
        let points_operation_effect: PointsOperationEffect = match estimate_effect {
            OperationEffectArea::Empty => PointsOperationEffect::Empty,
            OperationEffectArea::Points(points) => PointsOperationEffect::Some(points),
            OperationEffectArea::Filter(filter) => {
                let cardinality = local_shard.estimate_cardinality(Some(&filter))?;
                // validate the size of the change set before retrieving it
                if cardinality.max > MAX_CHANGES_TRACKED_COUNT {
                    PointsOperationEffect::Many
                } else {
                    let points = local_shard.read_filtered(Some(&filter))?;
                    PointsOperationEffect::Some(points.into_iter().collect())
                }
            }
        };

        {
            let mut changed_points_guard = self.changed_points.write().await;

            match points_operation_effect {
                PointsOperationEffect::Empty => {}
                PointsOperationEffect::Some(points) => {
                    for point in points {
                        // points updates are recorded but never trigger in `changed_alot`
                        changed_points_guard.insert(point);
                    }
                }
                PointsOperationEffect::Many => {
                    self.changed_alot
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                }
            }

            // Shard update is within a write lock scope, because we need a way to block the shard updates
            // during the transfer restart and finalization.
            local_shard.update(operation, wait).await
        }
    }

    /// Forward read-only `scroll_by` to `wrapped_shard`
    async fn scroll_by(
        &self,
        offset: Option<ExtendedPointId>,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        search_runtime_handle: &Handle,
        order_by: Option<&OrderBy>,
    ) -> CollectionResult<Vec<Record>> {
        let local_shard = &self.wrapped_shard;
        local_shard
            .scroll_by(
                offset,
                limit,
                with_payload_interface,
                with_vector,
                filter,
                search_runtime_handle,
                order_by,
            )
            .await
    }

    /// Forward read-only `info` to `wrapped_shard`
    async fn info(&self) -> CollectionResult<CollectionInfo> {
        let local_shard = &self.wrapped_shard;
        local_shard.info().await
    }

    /// Forward read-only `search` to `wrapped_shard`
    async fn core_search(
        &self,
        request: Arc<CoreSearchRequestBatch>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        let local_shard = &self.wrapped_shard;
        local_shard
            .core_search(request, search_runtime_handle, timeout)
            .await
    }

    /// Forward read-only `count` to `wrapped_shard`
    async fn count(&self, request: Arc<CountRequestInternal>) -> CollectionResult<CountResult> {
        let local_shard = &self.wrapped_shard;
        local_shard.count(request).await
    }

    /// Forward read-only `retrieve` to `wrapped_shard`
    async fn retrieve(
        &self,
        request: Arc<PointRequestInternal>,
        with_payload: &WithPayload,
        with_vector: &WithVector,
    ) -> CollectionResult<Vec<Record>> {
        let local_shard = &self.wrapped_shard;
        local_shard
            .retrieve(request, with_payload, with_vector)
            .await
    }
}
