use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use segment::types::{
    ExtendedPointId, Filter, ScoredPoint, WithPayload, WithPayloadInterface, WithVector,
};
use tokio::runtime::Handle;
use tokio::sync::Mutex;

use super::remote_shard::RemoteShard;
use crate::operations::point_ops::WriteOrdering;
use crate::operations::types::{
    CollectionInfo, CollectionResult, CoreSearchRequestBatch, CountRequest, CountResult,
    PointRequest, Record, SearchRequestBatch, UpdateResult,
};
use crate::operations::CollectionUpdateOperations;
use crate::shards::local_shard::LocalShard;
use crate::shards::shard_trait::ShardOperation;
use crate::shards::telemetry::LocalShardTelemetry;

/// ForwardQueue shard
///
/// ForwardQueue is a wrapper type for a LocalShard.
///
/// It can be used to provide all read and write operations while the wrapped shard is being
/// snapshotted and transferred to another node. It keeps track of all collection updates since its
/// creation, and allows to transfer these updates to a remote shard at a given time to assure
/// consistency.
///
/// This keeps track of all updates through the WAL of the wrapped shard. It therefore doesn't have
/// any memory overhead while updates are accumulated. This type is called 'queue' even though it
/// doesn't use a real queue, just so it is easy to understand its purpose.
pub struct QueueProxyShard {
    pub(crate) wrapped_shard: LocalShard,
    /// ID of the last WAL operation we consider transferred.
    last_update_idx: AtomicU64,
    /// Lock required to protect transfer-in-progress updates.
    /// It should block data updating operations while the batch is being transferred.
    update_lock: Mutex<()>,
}

/// Number of operations in batch when syncing
const BATCH_SIZE: usize = 100;

/// Number of times to retry transferring updates batch
const BATCH_RETRIES: usize = 3;

impl QueueProxyShard {
    pub async fn new(wrapped_shard: LocalShard) -> Self {
        let last_idx = wrapped_shard.wal.lock().last_index();
        let shard = Self {
            wrapped_shard,
            last_update_idx: last_idx.into(),
            update_lock: Default::default(),
        };

        // Set max ack version in WAL to not truncate parts we still need to transfer later
        shard.set_max_ack_version(Some(last_idx)).await;

        shard
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

    /// Transfer all updates that the remote missed from WAL
    pub async fn transfer_all_missed_updates(
        &self,
        remote_shard: &RemoteShard,
    ) -> CollectionResult<()> {
        while !self.transfer_wal_batch(remote_shard).await? {}
        Ok(())
    }

    /// Grab and transfer single new batch of updates from the WAL
    ///
    /// Returns `true` if this was the last batch and we're now done. `false` if more batches must
    /// be sent.
    async fn transfer_wal_batch(&self, remote_shard: &RemoteShard) -> CollectionResult<bool> {
        let mut update_lock = Some(self.update_lock.lock().await);
        let start_index = self.last_update_idx.load(Ordering::Relaxed) + 1;

        // Lock wall, count pending items to transfer, grab batch
        let (pending_count, batch) = {
            let wal = self.wrapped_shard.wal.lock();
            let items_left = wal.last_index().saturating_sub(start_index);
            let batch = wal.read(start_index).take(BATCH_SIZE).collect::<Vec<_>>();
            (items_left, batch)
        };

        // Normally, we immediately release the update lock to allow new updates.
        // On the last batch we keep the lock to prevent accumulating more updates on the WAL,
        // so we can finalize the transfer after this batch, before accepting new updates.
        let last_batch = pending_count <= BATCH_SIZE as u64 || batch.is_empty();
        if !last_batch {
            drop(update_lock.take());
        }

        // Transfer batch with retries and store last transferred ID
        let last_idx = batch.last().map(|(idx, _)| *idx);
        for attempts in (0..BATCH_RETRIES).rev() {
            match Self::transfer_operations_batch(&batch, remote_shard).await {
                Ok(()) => break,
                Err(_) if attempts > 0 => continue,
                Err(err) => return Err(err),
            }
        }
        if let Some(idx) = last_idx {
            self.last_update_idx.store(idx, Ordering::Relaxed);
        }

        Ok(last_batch)
    }

    /// Transfer batch of operations without retries
    async fn transfer_operations_batch(
        batch: &[(u64, CollectionUpdateOperations)],
        remote_shard: &RemoteShard,
    ) -> CollectionResult<()> {
        // TODO: naive transfer approach, transfer batch of points instead
        for (_idx, operation) in batch {
            remote_shard
                .forward_update(operation.clone(), true, WriteOrdering::Weak)
                .await?;
        }
        Ok(())
    }

    /// Set or release maximum version to acknowledge in WAL
    ///
    /// Because this proxy shard relies on the WAL to obtain operations history, it cannot be
    /// truncated before all these update operations have been flushed.
    /// Using this function we set the WAL not to truncate past the given point.
    ///
    /// Providing `None` will release this limitation.
    pub(super) async fn set_max_ack_version(&self, max_version: Option<u64>) {
        let update_handler = self.wrapped_shard.update_handler.lock().await;
        let mut max_ack_version = update_handler.max_ack_version.lock().await;
        *max_ack_version = max_version;
    }

    pub async fn on_optimizer_config_update(&self) -> CollectionResult<()> {
        self.wrapped_shard.on_optimizer_config_update().await
    }

    pub fn get_telemetry_data(&self) -> LocalShardTelemetry {
        self.wrapped_shard.get_telemetry_data()
    }

    pub fn is_update_in_progress(&self) -> bool {
        self.wrapped_shard.is_update_in_progress()
    }
}

#[async_trait]
impl ShardOperation for QueueProxyShard {
    /// Update `wrapped_shard` while keeping track of operations
    async fn update(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        let _update_lock = self.update_lock.lock().await;
        let local_shard = &self.wrapped_shard;
        // Shard update is within a write lock scope, because we need a way to block the shard updates
        // during the transfer restart and finalization.
        local_shard.update(operation.clone(), wait).await
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
            )
            .await
    }

    /// Forward read-only `info` to `wrapped_shard`
    async fn info(&self) -> CollectionResult<CollectionInfo> {
        let local_shard = &self.wrapped_shard;
        local_shard.info().await
    }

    /// Forward read-only `search` to `wrapped_shard`
    // ! COPY-PASTE: `core_search` is a copy-paste of `search` with different request type
    // ! please replicate any changes to both methods
    async fn search(
        &self,
        request: Arc<SearchRequestBatch>,
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        let local_shard = &self.wrapped_shard;
        local_shard.search(request, search_runtime_handle).await
    }

    // ! COPY-PASTE: `core_search` is a copy-paste of `search` with different request type
    // ! please replicate any changes to both methods
    async fn core_search(
        &self,
        request: Arc<CoreSearchRequestBatch>,
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        let local_shard = &self.wrapped_shard;
        local_shard
            .core_search(request, search_runtime_handle)
            .await
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
        with_vector: &WithVector,
    ) -> CollectionResult<Vec<Record>> {
        let local_shard = &self.wrapped_shard;
        local_shard
            .retrieve(request, with_payload, with_vector)
            .await
    }
}
