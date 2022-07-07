use crate::operations::operation_effect::{EstimateOperationEffectArea, OperationEffectArea};
use crate::{
    CollectionError, CollectionInfo, CollectionResult, CollectionUpdateOperations, CountRequest,
    CountResult, LocalShard, PointRequest, Record, SearchRequest, ShardOperation, UpdateResult,
};
use async_trait::async_trait;
use segment::types::{
    ExtendedPointId, Filter, PointIdType, ScoredPoint, WithPayload, WithPayloadInterface,
};
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::RwLock;

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
}

/// Max number of updates tracked to synchronize after the transfer.
const MAX_CHANGES_TRACKED_COUNT: usize = 10_000;

impl ProxyShard {
    // TODO: In order for the tracking system to be correct, the wrapped shard update queue operation should be empty at this moment.
    #[allow(unused)]
    fn new(wrapped_shard: LocalShard) -> Self {
        Self {
            wrapped_shard,
            changed_points: Default::default(),
        }
    }

    async fn check_changed_points_limit(&self, next: usize) -> CollectionResult<()> {
        if self.changed_points.read().await.len() + next > MAX_CHANGES_TRACKED_COUNT {
            Err(CollectionError::service_error(
                "Too many points changed during the proxy shard lifetime".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    /// Forward `create_snapshot` to `wrapped_shard`
    pub async fn create_snapshot(&self, target_path: &Path) -> CollectionResult<()> {
        self.wrapped_shard.create_snapshot(target_path).await
    }

    /// Forward `before_drop` to `wrapped_shard`
    pub async fn before_drop(&mut self) {
        self.wrapped_shard.before_drop().await
    }
}

#[async_trait]
impl ShardOperation for &ProxyShard {
    /// Update `wrapped_shard` while keeping track of the changed points
    async fn update(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        let local_shard = &self.wrapped_shard;
        let estimate_effect = operation.estimate_effect_area();
        match estimate_effect {
            OperationEffectArea::Empty => {}
            OperationEffectArea::Points(points) => {
                self.check_changed_points_limit(points.len()).await?;
                let mut changed_points_guard = self.changed_points.write().await;
                for point in points {
                    changed_points_guard.insert(point);
                }
            }
            OperationEffectArea::Filter(filter) => {
                let cardinality = local_shard.estimate_cardinality(Some(&filter)).await?;
                // validate the size of the change set before retrieving it
                self.check_changed_points_limit(cardinality.max).await?;
                let points = local_shard.read_filtered(Some(&filter)).await?;
                let mut changed_points_guard = self.changed_points.write().await;
                for point in points {
                    changed_points_guard.insert(point);
                }
            }
        }
        local_shard.update(operation, wait).await
    }

    /// Forward read-only `scroll_by` to `wrapped_shard`
    async fn scroll_by(
        &self,
        offset: Option<ExtendedPointId>,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: bool,
        filter: Option<&Filter>,
    ) -> CollectionResult<Vec<Record>> {
        let local_shard = &self.wrapped_shard;
        local_shard
            .scroll_by(offset, limit, with_payload_interface, with_vector, filter)
            .await
    }

    /// Forward read-only `info` to `wrapped_shard`
    async fn info(&self) -> CollectionResult<CollectionInfo> {
        let local_shard = &self.wrapped_shard;
        local_shard.info().await
    }

    /// Forward read-only `search` to `wrapped_shard`
    async fn search(
        &self,
        request: Arc<SearchRequest>,
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let local_shard = &self.wrapped_shard;
        local_shard.search(request, search_runtime_handle).await
    }

    /// Forward read-only `count` to `wrapped_shard`
    async fn count(&self, request: Arc<CountRequest>) -> CollectionResult<CountResult> {
        let local_shard = &self.wrapped_shard;
        local_shard.count(request).await
    }

    /// Forward read-only `retrieve` to `wrapped_shard`
    async fn retrieve(
        &self,
        request: Arc<PointRequest>,
        with_payload: &WithPayload,
        with_vector: bool,
    ) -> CollectionResult<Vec<Record>> {
        let local_shard = &self.wrapped_shard;
        local_shard
            .retrieve(request, with_payload, with_vector)
            .await
    }
}
