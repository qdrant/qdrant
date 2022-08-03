use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use segment::types::{
    ExtendedPointId, Filter, PointIdType, ScoredPoint, WithPayload, WithPayloadInterface,
};
use tokio::runtime::Handle;
use tokio::sync::Mutex;

use crate::operations::point_ops::{PointInsertOperations, PointOperations, PointStruct};
use crate::operations::types::{
    CollectionInfo, CollectionResult, CountRequest, CountResult, PointRequest, Record,
    SearchRequest, UpdateResult,
};
use crate::operations::{CollectionUpdateOperations, CreateIndex, FieldIndexOperations};
use crate::shard::local_shard::LocalShard;
use crate::shard::remote_shard::RemoteShard;
use crate::shard::ShardOperation;
use crate::telemetry::ShardTelemetry;

/// ForwardProxyShard
///
/// ForwardProxyShard is a wrapper type for a LocalShard.
///
/// It can be used to provide all read and write operations while the wrapped shard is being transferred to another node.
/// Proxy forwards all operations to remote shards.
pub struct ForwardProxyShard {
    pub(crate) wrapped_shard: LocalShard,
    pub(crate) remote_shard: RemoteShard,
    /// Lock required to protect transfer-in-progress updates.
    /// It should block data updating operations while the batch it being transferred.
    update_lock: Mutex<()>,
}

impl ForwardProxyShard {
    pub fn new(wrapped_shard: LocalShard, remote_shard: RemoteShard) -> Self {
        Self {
            wrapped_shard,
            remote_shard,
            update_lock: Mutex::new(()),
        }
    }

    /// Create payload indexes in the remote shard same as in the wrapped shard.
    pub async fn transfer_indexes(&self) -> CollectionResult<()> {
        let _update_lock = self.update_lock.lock().await;
        for (index_key, index_type) in self.wrapped_shard.info().await?.payload_schema.iter() {
            self.remote_shard
                .update(
                    CollectionUpdateOperations::FieldIndexOperation(
                        FieldIndexOperations::CreateIndex(CreateIndex {
                            field_name: index_key.clone(),
                            field_type: Some(index_type.data_type),
                        }),
                    ),
                    false,
                )
                .await?;
        }
        Ok(())
    }

    /// Move batch of points to the remote shard.
    /// Returns an offset of the next batch to be transferred.
    pub async fn transfer_batch(
        &self,
        offset: Option<PointIdType>,
        batch_size: usize,
    ) -> CollectionResult<Option<PointIdType>> {
        debug_assert!(batch_size > 0);
        let limit = batch_size + 1;
        let _update_lock = self.update_lock.lock().await;
        let mut batch = self
            .wrapped_shard
            .scroll_by(offset, limit, &WithPayloadInterface::Bool(true), true, None)
            .await?;
        let next_page_offset = if batch.len() < limit {
            // This was the last page
            None
        } else {
            // remove extra point, it would be a first point of the next page
            Some(batch.pop().unwrap().id)
        };

        if batch.is_empty() {
            return Ok(next_page_offset);
        }

        let points: Result<Vec<PointStruct>, String> =
            batch.into_iter().map(|point| point.try_into()).collect();

        let insert_points_operation = CollectionUpdateOperations::PointOperation(
            PointOperations::UpsertPoints(PointInsertOperations::PointsList(points?)),
        );

        // We only need to wait for the last batch.
        let wait = next_page_offset.is_none();
        self.remote_shard
            .update(insert_points_operation, wait)
            .await?;

        Ok(next_page_offset)
    }

    pub fn deconstruct(self) -> (LocalShard, RemoteShard) {
        (self.wrapped_shard, self.remote_shard)
    }

    /// Forward `create_snapshot` to `wrapped_shard`
    pub async fn create_snapshot(&self, target_path: &Path) -> CollectionResult<()> {
        self.wrapped_shard.create_snapshot(target_path).await
    }

    pub async fn on_optimizer_config_update(&self) -> CollectionResult<()> {
        self.wrapped_shard.on_optimizer_config_update().await
    }

    pub fn get_telemetry_data(&self) -> ShardTelemetry {
        ShardTelemetry::ForwardProxy {}
    }

    /// Forward `before_drop` to `wrapped_shard`
    pub async fn before_drop(&mut self) {
        self.wrapped_shard.before_drop().await
    }
}

#[async_trait]
impl ShardOperation for ForwardProxyShard {
    /// Update `wrapped_shard` while keeping track of the changed points
    async fn update(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        let _update_lock = self.update_lock.lock().await;
        let local_shard = &self.wrapped_shard;
        // Shard update is within a write lock scope, because we need a way to block the shard updates
        // during the transfer restart and finalization.
        local_shard.update(operation.clone(), wait).await?;

        self.remote_shard.update(operation, false).await
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

    async fn info(&self) -> CollectionResult<CollectionInfo> {
        let local_shard = &self.wrapped_shard;
        local_shard.info().await
    }

    async fn search(
        &self,
        request: Arc<SearchRequest>,
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let local_shard = &self.wrapped_shard;
        local_shard.search(request, search_runtime_handle).await
    }

    async fn count(&self, request: Arc<CountRequest>) -> CollectionResult<CountResult> {
        let local_shard = &self.wrapped_shard;
        local_shard.count(request).await
    }

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
