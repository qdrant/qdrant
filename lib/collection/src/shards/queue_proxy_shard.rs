use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use segment::data_types::order_by::OrderBy;
use segment::types::{
    ExtendedPointId, Filter, ScoredPoint, WithPayload, WithPayloadInterface, WithVector,
};
use tokio::runtime::Handle;
use tokio::sync::Mutex;

use super::remote_shard::RemoteShard;
use super::transfer::driver::MAX_RETRY_COUNT;
use super::update_tracker::UpdateTracker;
use crate::operations::point_ops::WriteOrdering;
use crate::operations::types::{
    CollectionInfo, CollectionResult, CoreSearchRequestBatch, CountRequestInternal, CountResult,
    PointRequestInternal, Record, UpdateResult,
};
use crate::operations::OperationWithClockTag;
use crate::shards::local_shard::LocalShard;
use crate::shards::shard_trait::ShardOperation;
use crate::shards::telemetry::LocalShardTelemetry;

/// Number of operations in batch when syncing
const BATCH_SIZE: usize = 100;

/// Number of times to retry transferring updates batch
const BATCH_RETRIES: usize = MAX_RETRY_COUNT;

/// QueueProxyShard shard
///
/// QueueProxyShard is a wrapper type for a LocalShard.
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
    /// Inner queue proxy shard.
    ///
    /// This is always `Some` until `finalize()` is called. This architecture is used to allow
    /// taking out the queue proxy shard for destructing when finalizing. Destructing the current
    /// type directly is not possible because it implements `Drop`.
    inner: Option<Inner>,
}

impl QueueProxyShard {
    pub fn new(
        wrapped_shard: LocalShard,
        remote_shard: RemoteShard,
        max_ack_version: Arc<AtomicU64>,
    ) -> Self {
        Self {
            inner: Some(Inner::new(wrapped_shard, remote_shard, max_ack_version)),
        }
    }

    pub async fn create_snapshot(
        &self,
        temp_path: &Path,
        target_path: &Path,
        save_wal: bool,
    ) -> CollectionResult<()> {
        self.inner
            .as_ref()
            .expect("Queue proxy has been finalized")
            .wrapped_shard
            .create_snapshot(temp_path, target_path, save_wal)
            .await
    }

    /// Transfer all updates that the remote missed from WAL
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    ///
    /// If cancelled - none, some or all operations may be transmitted to the remote.
    ///
    /// The internal field keeping track of the last transfer and maximum acknowledged WAL version
    /// likely won't be updated. In the worst case this might cause double sending operations.
    /// This should be fine as operations are idempotent.
    pub async fn transfer_all_missed_updates(&self) -> CollectionResult<()> {
        self.inner
            .as_ref()
            .expect("Queue proxy has been finalized")
            .transfer_all_missed_updates()
            .await
    }

    pub async fn on_optimizer_config_update(&self) -> CollectionResult<()> {
        self.inner
            .as_ref()
            .expect("Queue proxy has been finalized")
            .wrapped_shard
            .on_optimizer_config_update()
            .await
    }

    pub fn get_telemetry_data(&self) -> LocalShardTelemetry {
        self.inner
            .as_ref()
            .expect("Queue proxy has been finalized")
            .wrapped_shard
            .get_telemetry_data()
    }

    pub fn update_tracker(&self) -> &UpdateTracker {
        self.inner
            .as_ref()
            .expect("Queue proxy has been finalized")
            .wrapped_shard
            .update_tracker()
    }

    /// Check if the queue proxy shard is already finalized
    #[cfg(debug_assertions)]
    fn is_finalized(&self) -> bool {
        self.inner.is_none()
    }

    /// Forget all updates and finalize.
    ///
    /// Forget all missed updates since creation of this queue proxy shard and finalize. This
    /// unwraps the inner wrapped and remote shard.
    ///
    /// It also releases the max acknowledged WAL version.
    ///
    /// # Warning
    ///
    /// This intentionally forgets and drops updates pending to be transferred to the remote shard.
    /// The remote shard is therefore left in an inconsistent state, which should be resolved
    /// separately.
    pub fn forget_updates_and_finalize(mut self) -> (LocalShard, RemoteShard) {
        // Unwrap queue proxy shards and release max acknowledged version for WAL
        let queue_proxy = self
            .inner
            .take()
            .expect("Queue proxy has already been finalized");
        queue_proxy.set_max_ack_version(None);

        (queue_proxy.wrapped_shard, queue_proxy.remote_shard)
    }
}

#[async_trait]
impl ShardOperation for QueueProxyShard {
    /// Update `wrapped_shard` while keeping track of operations
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    async fn update(
        &self,
        operation: OperationWithClockTag,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        // `Inner::update` is cancel safe, so this is also cancel safe.
        self.inner
            .as_ref()
            .expect("Queue proxy has been finalized")
            .update(operation, wait)
            .await
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
        self.inner
            .as_ref()
            .expect("Queue proxy has been finalized")
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
        self.inner
            .as_ref()
            .expect("Queue proxy has been finalized")
            .info()
            .await
    }
    async fn core_search(
        &self,
        request: Arc<CoreSearchRequestBatch>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        self.inner
            .as_ref()
            .expect("Queue proxy has been finalized")
            .core_search(request, search_runtime_handle, timeout)
            .await
    }

    /// Forward read-only `count` to `wrapped_shard`
    async fn count(&self, request: Arc<CountRequestInternal>) -> CollectionResult<CountResult> {
        self.inner
            .as_ref()
            .expect("Queue proxy has been finalized")
            .count(request)
            .await
    }

    /// Forward read-only `retrieve` to `wrapped_shard`
    async fn retrieve(
        &self,
        request: Arc<PointRequestInternal>,
        with_payload: &WithPayload,
        with_vector: &WithVector,
    ) -> CollectionResult<Vec<Record>> {
        self.inner
            .as_ref()
            .expect("Queue proxy has been finalized")
            .retrieve(request, with_payload, with_vector)
            .await
    }
}

// Safe guard in debug mode to ensure that `finalize()` is called before dropping
#[cfg(debug_assertions)]
impl Drop for QueueProxyShard {
    fn drop(&mut self) {
        if !self.is_finalized() && !std::thread::panicking() {
            panic!("To drop a queue proxy shard, finalize() must be used");
        }
    }
}

struct Inner {
    /// Wrapped local shard to operate on.
    pub(super) wrapped_shard: LocalShard,
    /// Wrapped remote shard, to transfer operations to.
    pub(super) remote_shard: RemoteShard,
    /// ID of the last WAL operation we consider transferred.
    last_update_idx: AtomicU64,
    /// Lock required to protect transfer-in-progress updates.
    /// It should block data updating operations while the batch is being transferred.
    update_lock: Mutex<()>,
    /// Maximum acknowledged WAL version of the wrapped shard.
    /// We keep it here for access in `set_max_ack_version()` without needing async locks.
    /// See `set_max_ack_version()` and `UpdateHandler::max_ack_version` for more details.
    max_ack_version: Arc<AtomicU64>,
}

impl Inner {
    pub fn new(
        wrapped_shard: LocalShard,
        remote_shard: RemoteShard,
        max_ack_version: Arc<AtomicU64>,
    ) -> Self {
        let last_idx = wrapped_shard.wal.lock().last_index();

        let shard = Self {
            wrapped_shard,
            remote_shard,
            last_update_idx: last_idx.into(),
            update_lock: Default::default(),
            max_ack_version,
        };

        // Set max acknowledged version for WAL to not truncate parts we still need to transfer later
        shard.set_max_ack_version(Some(last_idx));

        shard
    }

    /// Transfer all updates that the remote missed from WAL
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    ///
    /// If cancelled - none, some or all operations of that batch may be transmitted to the remote.
    ///
    /// The internal field keeping track of the last transfer and maximum acknowledged WAL version
    /// likely won't be updated. In the worst case this might cause double sending operations.
    /// This should be fine as operations are idempotent.
    pub async fn transfer_all_missed_updates(&self) -> CollectionResult<()> {
        while !self.transfer_wal_batch().await? {}

        // Set max acknowledged version for WAL to last item we transferred
        let last_idx = self.last_update_idx.load(Ordering::Relaxed);
        self.set_max_ack_version(Some(last_idx));

        Ok(())
    }

    /// Grab and transfer single new batch of updates from the WAL
    ///
    /// Returns `true` if this was the last batch and we're now done. `false` if more batches must
    /// be sent.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    ///
    /// If cancelled - none, some or all operations may be transmitted to the remote.
    ///
    /// The internal field keeping track of the last transfer likely won't be updated. In the worst
    /// case this might cause double sending operations. This should be fine as operations are
    /// idempotent.
    async fn transfer_wal_batch(&self) -> CollectionResult<bool> {
        let mut update_lock = Some(self.update_lock.lock().await);
        let start_index = self.last_update_idx.load(Ordering::Relaxed) + 1;

        // Lock wall, count pending items to transfer, grab batch
        let (pending_count, batch) = {
            let wal = self.wrapped_shard.wal.lock();
            let items_left = wal.last_index().saturating_sub(start_index - 1);
            let batch = wal.read(start_index).take(BATCH_SIZE).collect::<Vec<_>>();
            (items_left, batch)
        };

        log::trace!(
            "Queue proxy transferring batch of {} updates to peer {}",
            batch.len(),
            self.remote_shard.peer_id,
        );

        // Normally, we immediately release the update lock to allow new updates.
        // On the last batch we keep the lock to prevent accumulating more updates on the WAL,
        // so we can finalize the transfer after this batch, before accepting new updates.
        let last_batch = pending_count <= BATCH_SIZE as u64 || batch.is_empty();
        if !last_batch {
            drop(update_lock.take());
        }

        // Transfer batch with retries and store last transferred ID
        let last_idx = batch.last().map(|(idx, _)| *idx);
        for remaining_attempts in (0..BATCH_RETRIES).rev() {
            match transfer_operations_batch(&batch, &self.remote_shard).await {
                Ok(()) => {}
                Err(err) if remaining_attempts > 0 => {
                    log::error!(
                        "Failed to transfer batch of updates to peer {}, retrying: {err}",
                        self.remote_shard.peer_id,
                    );
                    continue;
                }
                Err(err) => return Err(err),
            }

            if let Some(idx) = last_idx {
                self.last_update_idx.store(idx, Ordering::Relaxed);
            }
        }

        Ok(last_batch)
    }

    /// Set or release maximum version to acknowledge in WAL
    ///
    /// Because this proxy shard relies on the WAL to obtain operations history, it cannot be
    /// truncated before all these update operations have been flushed.
    /// Using this function we set the WAL not to truncate past the given point.
    ///
    /// Providing `None` will release this limitation.
    fn set_max_ack_version(&self, max_version: Option<u64>) {
        let max_version = max_version.unwrap_or(u64::MAX);
        self.max_ack_version.store(max_version, Ordering::Relaxed);
    }
}

#[async_trait]
impl ShardOperation for Inner {
    /// Update `wrapped_shard` while keeping track of operations
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    async fn update(
        &self,
        operation: OperationWithClockTag,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        // `LocalShard::update` is cancel safe, so this is also cancel safe.

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

/// Transfer batch of operations without retries
///
/// # Cancel safety
///
/// This method is cancel safe.
///
/// If cancelled - none, some or all operations of the batch may be transmitted to the remote.
async fn transfer_operations_batch(
    batch: &[(u64, OperationWithClockTag)],
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
