use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use async_trait::async_trait;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::tar_ext;
use common::types::TelemetryDetail;
use parking_lot::Mutex as ParkingMutex;
use segment::data_types::facets::{FacetParams, FacetResponse};
use segment::data_types::manifest::SnapshotManifest;
use segment::data_types::order_by::OrderBy;
use segment::index::field_index::CardinalityEstimation;
use segment::types::{
    ExtendedPointId, Filter, ScoredPoint, SizeStats, SnapshotFormat, WithPayload,
    WithPayloadInterface, WithVector,
};
use semver::Version;
use shard::retrieve::record_internal::RecordInternal;
use tokio::runtime::Handle;
use tokio::sync::Mutex;

use super::remote_shard::RemoteShard;
use super::transfer::driver::MAX_RETRY_COUNT;
use super::transfer::transfer_tasks_pool::TransferTaskProgress;
use super::update_tracker::UpdateTracker;
use crate::operations::OperationWithClockTag;
use crate::operations::point_ops::WriteOrdering;
use crate::operations::types::{
    CollectionError, CollectionInfo, CollectionResult, CoreSearchRequestBatch,
    CountRequestInternal, CountResult, OptimizersStatus, PointRequestInternal, UpdateResult,
};
use crate::operations::universal_query::shard_query::{ShardQueryRequest, ShardQueryResponse};
use crate::shards::local_shard::LocalShard;
use crate::shards::shard_trait::ShardOperation;
use crate::shards::telemetry::LocalShardTelemetry;

/// Number of operations in batch when syncing
const BATCH_SIZE: usize = 10;

/// Number of times to retry transferring updates batch
const BATCH_RETRIES: usize = MAX_RETRY_COUNT;

static MINIMAL_VERSION_FOR_BATCH_WAL_TRANSFER: LazyLock<Version> =
    LazyLock::new(|| Version::parse("1.14.1-dev").unwrap());

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
    /// Queue proxy the given local shard and point to the remote shard.
    ///
    /// This starts queueing all new updates on the local shard at the point of creation.
    pub async fn new(
        wrapped_shard: LocalShard,
        remote_shard: RemoteShard,
        wal_keep_from: Arc<AtomicU64>,
        progress: Arc<ParkingMutex<TransferTaskProgress>>,
    ) -> Self {
        Self {
            inner: Some(Inner::new(wrapped_shard, remote_shard, wal_keep_from, progress).await),
        }
    }

    /// Queue proxy the given local shard and point to the remote shard, from a specific WAL version.
    ///
    /// This queues all (existing) updates from a specific WAL `version` and onwards. In other
    /// words, this will ensure we transfer updates we already have and all new updates from a
    /// specific point in our WAL. The `version` may be in the past, but must always be within
    /// range of the current WAL.
    ///
    /// # Errors
    ///
    /// This fails if the given `version` is not in bounds of our current WAL. If the given
    /// `version` is too old or too new, queue proxy creation is rejected.
    pub async fn new_from_version(
        wrapped_shard: LocalShard,
        remote_shard: RemoteShard,
        wal_keep_from: Arc<AtomicU64>,
        version: u64,
        progress: Arc<ParkingMutex<TransferTaskProgress>>,
    ) -> Result<Self, (LocalShard, CollectionError)> {
        // Lock WAL until we've successfully created the queue proxy shard
        let wal = wrapped_shard.wal.wal.clone();
        let wal_lock = wal.lock().await;

        // If start version is not in current WAL bounds [first_idx, last_idx + 1], we cannot reliably transfer WAL
        // Allow it to be one higher than the last index to only send new updates
        let (first_idx, last_idx) = (wal_lock.first_closed_index(), wal_lock.last_index());
        if !(first_idx..=last_idx + 1).contains(&version) {
            return Err((
                wrapped_shard,
                CollectionError::service_error(format!(
                    "Cannot create queue proxy shard from version {version} because it is out of WAL bounds ({first_idx}..={last_idx})",
                )),
            ));
        }

        Ok(Self {
            inner: Some(Inner::new_from_version(
                wrapped_shard,
                remote_shard,
                wal_keep_from,
                version,
                progress,
            )),
        })
    }

    /// Get inner queue proxy shard. Will panic if the queue proxy has been finalized.
    fn inner_unchecked(&self) -> &Inner {
        self.inner.as_ref().expect("Queue proxy has been finalized")
    }

    fn inner_mut_unchecked(&mut self) -> &mut Inner {
        self.inner.as_mut().expect("Queue proxy has been finalized")
    }

    pub async fn create_snapshot(
        &self,
        temp_path: &Path,
        tar: &tar_ext::BuilderExt,
        format: SnapshotFormat,
        manifest: Option<SnapshotManifest>,
        save_wal: bool,
    ) -> CollectionResult<()> {
        self.inner_unchecked()
            .wrapped_shard
            .create_snapshot(temp_path, tar, format, manifest, save_wal)
            .await
    }

    pub fn snapshot_manifest(&self) -> CollectionResult<SnapshotManifest> {
        self.inner_unchecked().wrapped_shard.snapshot_manifest()
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
        self.inner_unchecked().transfer_all_missed_updates().await
    }

    pub async fn on_optimizer_config_update(&self) -> CollectionResult<()> {
        self.inner_unchecked()
            .wrapped_shard
            .on_optimizer_config_update()
            .await
    }

    pub async fn on_strict_mode_config_update(&mut self) {
        self.inner_mut_unchecked()
            .wrapped_shard
            .on_strict_mode_config_update()
            .await
    }

    pub fn trigger_optimizers(&self) {
        self.inner_unchecked().wrapped_shard.trigger_optimizers();
    }

    pub async fn get_telemetry_data(&self, detail: TelemetryDetail) -> LocalShardTelemetry {
        self.inner_unchecked()
            .wrapped_shard
            .get_telemetry_data(detail)
            .await
    }

    pub async fn get_optimization_status(&self) -> OptimizersStatus {
        self.inner_unchecked()
            .wrapped_shard
            .get_optimization_status()
            .await
    }

    pub async fn get_size_stats(&self) -> SizeStats {
        self.inner_unchecked().wrapped_shard.get_size_stats().await
    }

    pub fn update_tracker(&self) -> &UpdateTracker {
        self.inner_unchecked().wrapped_shard.update_tracker()
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
        queue_proxy.set_wal_keep_from(None);

        (queue_proxy.wrapped_shard, queue_proxy.remote_shard)
    }

    pub async fn estimate_cardinality(
        &self,
        filter: Option<&Filter>,
        hw_measurement_acc: &HwMeasurementAcc,
    ) -> CollectionResult<CardinalityEstimation> {
        self.inner_unchecked()
            .wrapped_shard
            .estimate_cardinality(filter, hw_measurement_acc)
            .await
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
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<UpdateResult> {
        // `Inner::update` is cancel safe, so this is also cancel safe.
        self.inner_unchecked()
            .update(operation, wait, hw_measurement_acc)
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
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<RecordInternal>> {
        self.inner_unchecked()
            .scroll_by(
                offset,
                limit,
                with_payload_interface,
                with_vector,
                filter,
                search_runtime_handle,
                order_by,
                timeout,
                hw_measurement_acc,
            )
            .await
    }

    /// Forward read-only `info` to `wrapped_shard`
    async fn info(&self) -> CollectionResult<CollectionInfo> {
        self.inner_unchecked().info().await
    }
    async fn core_search(
        &self,
        request: Arc<CoreSearchRequestBatch>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        self.inner_unchecked()
            .core_search(request, search_runtime_handle, timeout, hw_measurement_acc)
            .await
    }

    /// Forward read-only `count` to `wrapped_shard`
    async fn count(
        &self,
        request: Arc<CountRequestInternal>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<CountResult> {
        self.inner_unchecked()
            .count(request, search_runtime_handle, timeout, hw_measurement_acc)
            .await
    }

    /// Forward read-only `retrieve` to `wrapped_shard`
    async fn retrieve(
        &self,
        request: Arc<PointRequestInternal>,
        with_payload: &WithPayload,
        with_vector: &WithVector,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<RecordInternal>> {
        self.inner_unchecked()
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

    /// Forward read-only `query` to `wrapped_shard`
    async fn query_batch(
        &self,
        requests: Arc<Vec<ShardQueryRequest>>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<ShardQueryResponse>> {
        self.inner_unchecked()
            .wrapped_shard
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
        self.inner_unchecked()
            .wrapped_shard
            .facet(request, search_runtime_handle, timeout, hw_measurement_acc)
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
    /// WAL record at which we started the transfer.
    started_at: u64,
    /// ID of the WAL operation we should transfer next. We consider everything before it to be
    /// transferred.
    transfer_from: AtomicU64,
    /// Lock required to protect transfer-in-progress updates.
    /// It should block data updating operations while the batch is being transferred.
    update_lock: Mutex<()>,
    /// Always keep this WAL version and later and prevent acknowledgment/truncation from the WAL.
    /// We keep it here for access in `set_wal_keep_from()` without needing async locks.
    /// See `set_wal_keep_from()` and `UpdateHandler::wal_keep_from` for more details.
    /// Defaults to `u64::MAX` to allow acknowledging all confirmed versions.
    wal_keep_from: Arc<AtomicU64>,
    /// Progression tracker.
    progress: Arc<ParkingMutex<TransferTaskProgress>>,
}

impl Inner {
    pub async fn new(
        wrapped_shard: LocalShard,
        remote_shard: RemoteShard,
        wal_keep_from: Arc<AtomicU64>,
        progress: Arc<ParkingMutex<TransferTaskProgress>>,
    ) -> Self {
        let start_from = wrapped_shard.wal.wal.lock().await.last_index() + 1;
        Self::new_from_version(
            wrapped_shard,
            remote_shard,
            wal_keep_from,
            start_from,
            progress,
        )
    }

    pub fn new_from_version(
        wrapped_shard: LocalShard,
        remote_shard: RemoteShard,
        wal_keep_from: Arc<AtomicU64>,
        version: u64,
        progress: Arc<ParkingMutex<TransferTaskProgress>>,
    ) -> Self {
        let shard = Self {
            wrapped_shard,
            remote_shard,
            transfer_from: version.into(),
            started_at: version,
            update_lock: Default::default(),
            wal_keep_from,
            progress,
        };

        // Keep all WAL entries from `version` so we don't truncate them off when we still need to transfer
        shard.set_wal_keep_from(Some(version));

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

        // Set the WAL version to keep to the next item we should transfer
        let transfer_from = self.transfer_from.load(Ordering::Relaxed);
        self.set_wal_keep_from(Some(transfer_from));

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
        let transfer_from = self.transfer_from.load(Ordering::Relaxed);

        // Lock wall, count pending items to transfer, grab batch
        let (pending_count, total, batch) = {
            let wal = self.wrapped_shard.wal.wal.lock().await;
            let items_left = (wal.last_index() + 1).saturating_sub(transfer_from);
            let items_total = (transfer_from - self.started_at) + items_left;
            let batch = wal.read(transfer_from).take(BATCH_SIZE).collect::<Vec<_>>();
            debug_assert!(
                batch.len() <= items_left as usize,
                "batch cannot be larger than items_left",
            );
            (items_left, items_total, batch)
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

        // If we are transferring the last batch, we need to wait for it to be applied.
        //  - Why can we not wait? Assuming that order of operations is still enforced by the WAL,
        //    we should end up in exactly the same state with or without waiting.
        //  - Why do we need to wait on the last batch? If we switch to ready state before
        //    updates are actually applied, we might create an inconsistency for read operations.
        let wait = last_batch;

        // Set initial progress on the first batch
        let is_first = transfer_from == self.started_at;
        if is_first {
            self.progress.lock().set(0, total as usize);
        }

        // Transfer batch with retries and store last transferred ID
        let last_idx = batch.last().map(|(idx, _)| *idx);
        for remaining_attempts in (0..BATCH_RETRIES).rev() {
            let disposed_hw = HwMeasurementAcc::disposable(); // Internal operation
            match transfer_operations_batch(&batch, &self.remote_shard, wait, disposed_hw).await {
                Ok(()) => {
                    if let Some(idx) = last_idx {
                        self.transfer_from.store(idx + 1, Ordering::Relaxed);

                        let transferred = (idx + 1 - self.started_at) as usize;
                        self.progress.lock().set(transferred, total as usize);
                    }
                    break;
                }
                Err(err) if remaining_attempts > 0 => {
                    log::error!(
                        "Failed to transfer batch of updates to peer {}, retrying: {err}",
                        self.remote_shard.peer_id,
                    );
                }
                Err(err) => return Err(err),
            }
        }

        Ok(last_batch)
    }

    /// Set or release what WAL versions to keep preventing acknowledgment/truncation.
    ///
    /// Because this proxy shard relies on the WAL to obtain operations in the past, it cannot be
    /// truncated before all these update operations have been flushed.
    /// Using this function we set the WAL not to acknowledge and truncate from a specific point.
    ///
    /// Providing `None` will release this limitation.
    fn set_wal_keep_from(&self, version: Option<u64>) {
        log::trace!("set_wal_keep_from {version:?}");
        let version = version.unwrap_or(u64::MAX);
        self.wal_keep_from.store(version, Ordering::Relaxed);
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
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<UpdateResult> {
        // `LocalShard::update` is cancel safe, so this is also cancel safe.

        let _update_lock = self.update_lock.lock().await;

        let local_shard = &self.wrapped_shard;
        // Shard update is within a write lock scope, because we need a way to block the shard updates
        // during the transfer restart and finalization.
        local_shard
            .update(operation.clone(), wait, hw_measurement_acc)
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
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<RecordInternal>> {
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
                timeout,
                hw_measurement_acc,
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
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        let local_shard = &self.wrapped_shard;
        local_shard
            .core_search(request, search_runtime_handle, timeout, hw_measurement_acc)
            .await
    }

    /// Forward read-only `count` to `wrapped_shard`
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

    /// Forward read-only `retrieve` to `wrapped_shard`
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

    /// Forward read-only `query` to `wrapped_shard`
    async fn query_batch(
        &self,
        request: Arc<Vec<ShardQueryRequest>>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<ShardQueryResponse>> {
        let local_shard = &self.wrapped_shard;
        local_shard
            .query_batch(request, search_runtime_handle, timeout, hw_measurement_acc)
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
    wait: bool,
    hw_measurement_acc: HwMeasurementAcc,
) -> CollectionResult<()> {
    if batch.is_empty() {
        return Ok(());
    }

    let supports_update_batching =
        remote_shard.check_version(&MINIMAL_VERSION_FOR_BATCH_WAL_TRANSFER);

    if supports_update_batching {
        let mut batch_upd = Vec::with_capacity(batch.len());

        for (_idx, operation) in batch {
            let mut operation = operation.clone();
            // Set force flag because operations from WAL may be unordered if another node is sending
            // new operations at the same time
            if let Some(clock_tag) = &mut operation.clock_tag {
                clock_tag.force = true;
            }
            batch_upd.push(operation);
        }

        remote_shard
            .forward_update_batch(
                batch_upd,
                wait,
                WriteOrdering::Weak,
                hw_measurement_acc.clone(),
            )
            .await?;

        return Ok(());
    }

    // Fallback to one-by-one transfer, in case the remote shard doesn't support batch updates
    for (_idx, operation) in batch {
        let mut operation = operation.clone();

        // Set force flag because operations from WAL may be unordered if another node is sending
        // new operations at the same time
        if let Some(clock_tag) = &mut operation.clock_tag {
            clock_tag.force = true;
        }

        remote_shard
            .forward_update(
                operation,
                wait,
                WriteOrdering::Weak,
                hw_measurement_acc.clone(),
            )
            .await?;
    }
    Ok(())
}
